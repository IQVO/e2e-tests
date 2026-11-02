package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
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

	a := &api{client: srv.Client(), base: map[string]string{"svc": srv.URL}, stats: newCallStats()}
	type row struct {
		ID string `json:"id"`
	}
	got, r := listAll[row](context.Background(), a, "svc", "/tasks?orderRef", "/tasks?orderRef=O-1")
	if !r.ok() || len(got) != 7 || pages != 3 {
		t.Fatalf("got %d rows in %d pages (last %d %s), want 7 rows in 3 pages", len(got), pages, r.status, r.body)
	}
	for i, g := range got {
		if g.ID != fmt.Sprintf("t%d", i) {
			t.Fatalf("row %d = %s: a page was skipped or repeated", i, g.ID)
		}
	}

	if p := a.call(context.Background(), "svc", http.MethodPost, "/x", "/x", nil); !p.ok() || postKey == "" {
		t.Fatalf("POST status %d key %q: every POST needs an Idempotency-Key", p.status, postKey)
	}
	if p := a.call(context.Background(), "svc", http.MethodPatch, "/x", "/x", nil); p.status == 0 {
		t.Fatal("PATCH did not reach the server")
	}
}
