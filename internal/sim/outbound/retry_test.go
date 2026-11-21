package outbound

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/sim"
)

var fast = Waits{Timeout: 2 * time.Second, Interval: time.Millisecond}

func clientFor(svc string, srv *httptest.Server) *sim.Client {
	return &sim.Client{HTTP: srv.Client(), Base: map[string]string{svc: srv.URL}, Stats: sim.NewCallStats()}
}

func problem(w http.ResponseWriter, status int, slug string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	fmt.Fprintf(w, `{"type":"https://errors.example.test/%s","title":"x","status":%d,"detail":"d","instance":"/"}`, slug, status)
}

func TestSlugReadsCodeThenTheTypeTail(t *testing.T) {
	for name, tc := range map[string]struct{ body, want string }{
		"fleet code":     {`{"type":"https://e/x/wrong-load","code":"packages-outstanding"}`, "packages-outstanding"},
		"type tail":      {`{"type":"https://errors.dock-scheduling.warehouse-systems.dev/unknown-door"}`, "unknown-door"},
		"not a problem":  {`<html>`, ""},
		"transport body": {``, ""},
	} {
		if got := Slug(sim.Resp{Status: 409, Body: []byte(tc.body)}); got != tc.want {
			t.Errorf("%s: Slug = %q, want %q", name, got, tc.want)
		}
	}
}

// The lag counter elapses: the exact transient is retried, the call then succeeds, and every
// attempt carried its OWN Idempotency-Key (the services store a 4xx under the key, so a reused key
// would replay the refusal for ever).
func TestCallTransientRetriesOnlyTheExactSlugWithAFreshKeyEachTime(t *testing.T) {
	var mu sync.Mutex
	var keys []string
	stored := map[string]int{} // key -> status stored under it, like outbound-shipping's middleware
	refusals := 3
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		key := r.Header.Get("Idempotency-Key")
		keys = append(keys, key)
		if st, ok := stored[key]; ok {
			problem(w, st, "package-not-found")
			return
		}
		if refusals > 0 {
			refusals--
			stored[key] = http.StatusNotFound
			problem(w, http.StatusNotFound, "package-not-found")
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	c := clientFor(SvcOutbound, srv)

	r, err := callTransient(context.Background(), fast, "stage", []Transient{packageNotFound}, func() sim.Resp {
		return c.Call(context.Background(), SvcOutbound, http.MethodPost, "POST /x", "/x", map[string]any{})
	})
	if err != nil || !r.OK() {
		t.Fatalf("callTransient = %d %v, want 200 after the lag", r.Status, err)
	}
	if len(keys) != 4 {
		t.Fatalf("%d attempts, want 4 (3 refusals then success)", len(keys))
	}
	seen := map[string]bool{}
	for _, k := range keys {
		if k == "" || seen[k] {
			t.Fatalf("Idempotency-Key %q is empty or reused across attempts %v", k, keys)
		}
		seen[k] = true
	}
}

func TestCallTransientFailsAtOnceOnAnyOtherRefusalAndOnTransportErrors(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		// Same status as the transient, different slug: NOT the lag.
		problem(w, http.StatusNotFound, "load-not-found")
	}))
	defer srv.Close()
	c := clientFor(SvcOutbound, srv)
	_, err := callTransient(context.Background(), fast, "stage", []Transient{packageNotFound}, func() sim.Resp {
		return c.Call(context.Background(), SvcOutbound, http.MethodPost, "POST /x", "/x", nil)
	})
	if err == nil || !strings.Contains(err.Error(), "load-not-found") || calls != 1 {
		t.Fatalf("err = %v after %d calls, want a single call and the slug in the error", err, calls)
	}

	dead := httptest.NewServer(http.NotFoundHandler())
	cd := clientFor(SvcOutbound, dead)
	dead.Close()
	calls = 0
	_, err = callTransient(context.Background(), fast, "stage", []Transient{packageNotFound}, func() sim.Resp {
		calls++
		return cd.Call(context.Background(), SvcOutbound, http.MethodPost, "POST /x", "/x", nil)
	})
	if err == nil || calls != 1 || !strings.Contains(err.Error(), "transport error") {
		t.Fatalf("err = %v after %d calls, want one attempt and a transport error", err, calls)
	}
}

func TestCallTransientTimesOutNamingWhatNeverArrived(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		problem(w, http.StatusUnprocessableEntity, "unknown-door")
	}))
	defer srv.Close()
	c := clientFor(SvcDock, srv)
	start := time.Now()
	_, err := callTransient(context.Background(), Waits{Timeout: 50 * time.Millisecond, Interval: time.Millisecond}, "book", []Transient{unknownDoor}, func() sim.Resp {
		return c.Call(context.Background(), SvcDock, http.MethodPost, "POST /x", "/x", nil)
	})
	if err == nil || !strings.Contains(err.Error(), "still waiting") || !strings.Contains(err.Error(), "unknown-door") {
		t.Fatalf("err = %v, want a bounded timeout naming 422 unknown-door", err)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("the wait was not bounded: %s", time.Since(start))
	}
}

func TestPollStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := poll(ctx, Waits{Timeout: 3 * time.Hour, Interval: time.Hour}, "wait", func() (string, error) { return "never", nil })
	if err == nil || !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("err = %v, want a cancelled wait", err)
	}
}

// outbound-shipping caps limit at 200 (400 invalid-parameter above it): the shared sim.ListAll asks
// for 500, so this package pages at 200 itself, filters repeated on every page.
func TestListAllPagesAtTheServicesMaximumAndKeepsTheFilter(t *testing.T) {
	pages := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if n, _ := strconv.Atoi(q.Get("limit")); n < 1 || n > 200 {
			problem(w, http.StatusBadRequest, "invalid-parameter")
			return
		}
		if q.Get("site_id") != "AUS3" {
			problem(w, http.StatusBadRequest, "filter-not-repeated")
			return
		}
		start, _ := strconv.Atoi(q.Get("cursor"))
		pages++
		end := min(start+2, 5)
		items := []string{}
		for i := start; i < end; i++ {
			items = append(items, fmt.Sprintf(`{"id":"load-%d"}`, i))
		}
		next := `null`
		if end < 5 {
			next = fmt.Sprintf(`"%d"`, end)
		}
		fmt.Fprintf(w, `{"items":[%s],"nextCursor":%s}`, strings.Join(items, ","), next)
	}))
	defer srv.Close()
	c := clientFor(SvcOutbound, srv)
	got, r := listAll[loadDTO](context.Background(), c, SvcOutbound, "GET /loads", "/loads?site_id=AUS3")
	if !r.OK() || len(got) != 5 || pages != 3 {
		t.Fatalf("got %d loads in %d pages (last %d %s), want 5 in 3", len(got), pages, r.Status, r.Body)
	}
}
