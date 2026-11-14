package sim

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

// TestListAllFollowsNextCursorAndKeysEveryPost walks a 7-row collection served 3 per page and
// checks the guideline contract the harness relies on: filters repeat on every page, the cursor
// is sent as `cursor`, and a POST carries an Idempotency-Key.
func TestListAllFollowsNextCursorAndKeysEveryPost(t *testing.T) {
	var pages int
	var postKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			postKey = r.Header.Get("Idempotency-Key")
			w.WriteHeader(http.StatusCreated)
			return
		}
		if r.URL.Query().Get("orderRef") != "O-1" {
			http.Error(w, "filter not repeated", http.StatusBadRequest)
			return
		}
		start := 0
		if c := r.URL.Query().Get("cursor"); c != "" {
			start, _ = strconv.Atoi(c)
		}
		pages++
		end := min(start+3, 7)
		body := `{"items":[`
		for i := start; i < end; i++ {
			if i > start {
				body += ","
			}
			body += fmt.Sprintf(`{"id":"t%d"}`, i)
		}
		body += `]`
		if end < 7 {
			body += fmt.Sprintf(`,"nextCursor":"%d"`, end)
		}
		_, _ = w.Write([]byte(body + `}`))
	}))
	defer srv.Close()

	a := &Client{HTTP: srv.Client(), Base: map[string]string{"svc": srv.URL}, Stats: NewCallStats()}
	type row struct {
		ID string `json:"id"`
	}
	got, r := ListAll[row](context.Background(), a, "svc", "/tasks?orderRef", "/tasks?orderRef=O-1")
	if !r.OK() || len(got) != 7 || pages != 3 {
		t.Fatalf("got %d rows in %d pages (last %d %s), want 7 rows in 3 pages", len(got), pages, r.Status, r.Body)
	}
	for i, g := range got {
		if g.ID != fmt.Sprintf("t%d", i) {
			t.Fatalf("row %d = %s: a page was skipped or repeated", i, g.ID)
		}
	}

	if p := a.Call(context.Background(), "svc", http.MethodPost, "/x", "/x", nil); !p.OK() || postKey == "" {
		t.Fatalf("POST status %d key %q: every POST needs an Idempotency-Key", p.Status, postKey)
	}
	if p := a.Call(context.Background(), "svc", http.MethodPatch, "/x", "/x", nil); p.Status == 0 {
		t.Fatal("PATCH did not reach the server")
	}
}

// TestCallWithHeaderWinsOverTheGeneratedKey pins the retry model: a caller that re-sends a
// logical action passes its own Idempotency-Key and the client must not overwrite it, while a
// plain Call never reuses a key.
func TestCallWithHeaderWinsOverTheGeneratedKey(t *testing.T) {
	var keys []string
	var auth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		keys = append(keys, r.Header.Get("Idempotency-Key"))
		auth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()
	a := NewClient(map[string]string{"svc": srv.URL}, NewCallStats())
	ctx := context.Background()

	a.CallWith(ctx, "svc", http.MethodPost, "/x", "/x", nil, map[string]string{"Idempotency-Key": "retry-1", "Authorization": "Bearer s1"})
	a.CallWith(ctx, "svc", http.MethodPost, "/x", "/x", nil, map[string]string{"Idempotency-Key": "retry-1"})
	a.Call(ctx, "svc", http.MethodPost, "/x", "/x", nil)
	a.Call(ctx, "svc", http.MethodPost, "/x", "/x", nil)

	if keys[0] != "retry-1" || keys[1] != "retry-1" {
		t.Fatalf("explicit key was replaced: %v", keys[:2])
	}
	if keys[2] == "" || keys[3] == "" || keys[2] == keys[3] || keys[2] == "retry-1" {
		t.Fatalf("plain calls must each mint a fresh key, got %v", keys[2:])
	}
	_ = auth
}

func TestTransportErrorsAreRecordedAsStatusZeroAnd5xxCountsFail(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/boom" {
			http.Error(w, "kaput", http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	stats := NewCallStats()
	a := NewClient(map[string]string{"svc": srv.URL}, stats)
	ctx := context.Background()

	a.Call(ctx, "svc", http.MethodGet, "/ok", "/ok", nil)
	if stats.Count5xx() != 0 {
		t.Fatalf("a 200 counted as failure")
	}
	a.Call(ctx, "svc", http.MethodGet, "/boom", "/boom", nil)
	srv.Close()
	if r := a.Call(ctx, "svc", http.MethodGet, "/ok", "/ok", nil); r.Status != 0 {
		t.Fatalf("closed server answered %d", r.Status)
	}
	if got := stats.Count5xx(); got != 2 {
		t.Fatalf("Count5xx = %d, want 2 (one 502 + one transport error)", got)
	}
	if len(stats.Failures()) != 2 || len(stats.Lines()) != 2 {
		t.Fatalf("failures %d lines %d, want 2 and 2 (/ok and /boom routes)", len(stats.Failures()), len(stats.Lines()))
	}
}

func TestGatewayBaseTrimsTheSlashAndNamesEveryService(t *testing.T) {
	got := GatewayBase("http://localhost:8000/api//", []string{"a", "b"})
	if got["a"] != "http://localhost:8000/api/a" || got["b"] != "http://localhost:8000/api/b" || len(got) != 2 {
		t.Fatalf("GatewayBase = %v", got)
	}
}

func TestSuspendWatchCoversTheWindowPlusGraceOnly(t *testing.T) {
	from := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	w := &SuspendWatch{windows: []Suspension{{From: from, To: from.Add(time.Minute)}}}
	cases := []struct {
		at    time.Time
		grace time.Duration
		want  bool
	}{
		{from.Add(-time.Second), 0, false},
		{from.Add(30 * time.Second), 0, true},
		{from.Add(time.Minute + 2*time.Minute), 6 * time.Minute, true},
		{from.Add(time.Minute + 7*time.Minute), 6 * time.Minute, false},
	}
	for i, c := range cases {
		if got := w.Covers(c.at, c.grace); got != c.want {
			t.Errorf("case %d: Covers = %v, want %v", i, got, c.want)
		}
	}
	if len(w.Snapshot()) != 1 {
		t.Fatal("Snapshot lost the window")
	}
}

func TestShortTypeKeepsTheLastDottedSegment(t *testing.T) {
	for in, want := range map[string]string{
		"com.warehouse.wes.order-management.order.OrderAllocated": "OrderAllocated",
		"OrderAllocated": "OrderAllocated",
		"":               "",
	} {
		if got := ShortType(in); got != want {
			t.Errorf("ShortType(%q) = %q, want %q", in, got, want)
		}
	}
}
