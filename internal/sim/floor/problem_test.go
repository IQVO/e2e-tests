package floor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/sim"
)

var t0 = time.Date(2026, 11, 14, 15, 45, 0, 0, time.UTC)

func TestSlug(t *testing.T) {
	cases := map[string]string{
		`{"type":"https://errors.wes-work-planning.warehouse-systems.dev/wip-limit-reached"}`: "wip-limit-reached",
		`{"type":"https://errors.x.dev/ignored","code":"fleet-code"}`:                         "fleet-code",
		`{"type":"bare"}`: "bare",
		`not json`:        "",
	}
	for body, want := range cases {
		if got := Slug(sim.Resp{Status: 409, Body: []byte(body)}); got != want {
			t.Errorf("Slug(%s) = %q, want %q", body, got, want)
		}
	}
}

func TestAwaitTimeoutNamesWhatItNeverSaw(t *testing.T) {
	clock := newManualClock(t0)
	looks := 0
	err := await(context.Background(), clock, Waits{Timeout: 5 * time.Second, Interval: time.Second}, "find the thing", func(context.Context) (verdict, string, error) {
		looks++
		return vWait, "the thing is still missing", nil
	})
	if !IsTimeout(err) {
		t.Fatalf("err = %v, want a timeout", err)
	}
	if !strings.Contains(err.Error(), "find the thing") || !strings.Contains(err.Error(), "the thing is still missing") {
		t.Fatalf("a timeout must say what was awaited and what was last seen, got %q", err)
	}
	if looks != 6 { // t=0..5 s inclusive at a 1 s interval
		t.Fatalf("looked %d times, want 6", looks)
	}
	if got := clock.Now().Sub(t0); got != 5*time.Second {
		t.Fatalf("the wait ran %s of clock time, want exactly its 5 s bound", got)
	}
}

func TestAwaitReturnsAtOnceOnAFatalError(t *testing.T) {
	clock := newManualClock(t0)
	err := await(context.Background(), clock, Waits{Timeout: time.Hour, Interval: time.Second}, "x", func(context.Context) (verdict, string, error) {
		return vWait, "", context.DeadlineExceeded
	})
	if err != context.DeadlineExceeded {
		t.Fatalf("err = %v", err)
	}
	if clock.Slept() != 0 {
		t.Fatalf("a fatal error must not sleep, slept %s", clock.Slept())
	}
}

func TestAwaitProgressRestartsTheTimeout(t *testing.T) {
	// The timeout bounds waiting WITHOUT progress: 20 looks that each made progress and took
	// 2 s of clock time outlast a 3 s bound; then 3 s of nothing times out.
	clock := newManualClock(t0)
	n := 0
	err := await(context.Background(), clock, Waits{Timeout: 3 * time.Second, Interval: time.Second}, "work", func(context.Context) (verdict, string, error) {
		n++
		if n <= 20 {
			clock.Advance(2 * time.Second)
			return vAgain, "", nil
		}
		return vWait, "nothing left to claim", nil
	})
	if !IsTimeout(err) || n != 24 { // 20 progress looks, then looks at 0, 1, 2 and 3 s of waiting
		t.Fatalf("err = %v after %d looks, want a timeout after 24", err, n)
	}
}

func TestAwaitStopsWhenTheContextIsDone(t *testing.T) {
	clock := newManualClock(t0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := await(ctx, clock, Waits{Timeout: time.Hour, Interval: time.Second}, "x", func(context.Context) (verdict, string, error) {
		return vWait, "waiting", nil
	})
	if err == nil || IsTimeout(err) {
		t.Fatalf("err = %v, want a context error and not a timeout", err)
	}
}

// keyServer answers every request with the next scripted status+body and records the
// Idempotency-Key of each, so a test can prove a retry sends a fresh one.
type keyServer struct {
	mu     sync.Mutex
	script []sim.Resp
	keys   []string
}

func newKeyServer(t *testing.T, script ...sim.Resp) (*keyServer, *sim.Client) {
	t.Helper()
	ks := &keyServer{script: script}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ks.mu.Lock()
		ks.keys = append(ks.keys, r.Header.Get("Idempotency-Key"))
		i := len(ks.keys) - 1
		if i >= len(ks.script) {
			i = len(ks.script) - 1
		}
		resp := ks.script[i]
		ks.mu.Unlock()
		w.WriteHeader(resp.Status)
		_, _ = w.Write(resp.Body)
	}))
	t.Cleanup(srv.Close)
	return ks, sim.NewClient(map[string]string{"svc": srv.URL}, sim.NewCallStats())
}

func problemResp(status int, slug string) sim.Resp {
	return sim.Resp{Status: status, Body: []byte(`{"type":"https://errors.svc.warehouse-systems.dev/` + slug + `","status":` + strconv.Itoa(status) + `}`)}
}

func TestCallTransientRetriesOnlyTheExactPairWithAFreshKeyEachTime(t *testing.T) {
	ks, c := newKeyServer(t,
		problemResp(409, "no-claimable-task"),
		problemResp(409, "no-claimable-task"),
		sim.Resp{Status: 200, Body: []byte(`{}`)},
	)
	clock := newManualClock(t0)
	r, err := callTransient(context.Background(), clock, Waits{Timeout: time.Minute, Interval: time.Second}, "claim", []Transient{noClaimableTask}, func() sim.Resp {
		return c.Call(context.Background(), "svc", http.MethodPost, "POST /x", "/x", nil)
	})
	if err != nil || !r.OK() {
		t.Fatalf("r=%d err=%v", r.Status, err)
	}
	if len(ks.keys) != 3 {
		t.Fatalf("%d calls, want 3", len(ks.keys))
	}
	seen := map[string]bool{}
	for _, k := range ks.keys {
		if k == "" || seen[k] {
			t.Fatalf("keys %v: every attempt needs its own non-empty Idempotency-Key", ks.keys)
		}
		seen[k] = true
	}
}

func TestCallTransientFailsAtOnceOnAnythingElse(t *testing.T) {
	for name, resp := range map[string]sim.Resp{
		"same status other slug": problemResp(409, "task-not-owner"),
		"other status same slug": problemResp(422, "no-claimable-task"),
		"server error":           problemResp(500, "internal"),
		"transport error":        {Status: 0, Body: []byte("connection refused")},
	} {
		t.Run(name, func(t *testing.T) {
			ks, c := newKeyServer(t, resp)
			clock := newManualClock(t0)
			_, err := callTransient(context.Background(), clock, Waits{Timeout: time.Minute, Interval: time.Second}, "claim", []Transient{noClaimableTask}, func() sim.Resp {
				if resp.Status == 0 {
					return resp
				}
				return c.Call(context.Background(), "svc", http.MethodPost, "POST /x", "/x", nil)
			})
			if err == nil || IsTimeout(err) {
				t.Fatalf("err = %v, want an immediate refusal", err)
			}
			if clock.Slept() != 0 || len(ks.keys) > 1 {
				t.Fatalf("slept %s over %d calls: a non-transient refusal must not be retried", clock.Slept(), len(ks.keys))
			}
		})
	}
}

func TestCallTransientGivesUpWhenTheLagNeverEnds(t *testing.T) {
	_, c := newKeyServer(t, problemResp(409, "no-claimable-task"))
	clock := newManualClock(t0)
	_, err := callTransient(context.Background(), clock, Waits{Timeout: 10 * time.Second, Interval: time.Second}, "claim a PICK task", []Transient{noClaimableTask}, func() sim.Resp {
		return c.Call(context.Background(), "svc", http.MethodPost, "POST /x", "/x", nil)
	})
	if !IsTimeout(err) || !strings.Contains(err.Error(), "409 no-claimable-task") {
		t.Fatalf("err = %v, want a timeout naming the refusal it kept getting", err)
	}
}

func TestListAllPagesAtAnExplicitLimitBelowEveryServicesCap(t *testing.T) {
	var mu sync.Mutex
	var queries []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		queries = append(queries, r.URL.RawQuery)
		mu.Unlock()
		if r.URL.Query().Get("cursor") == "" {
			_, _ = w.Write([]byte(`{"items":[{"id":"a"}],"nextCursor":"c2"}`))
			return
		}
		_, _ = w.Write([]byte(`{"items":[{"id":"b"}]}`))
	}))
	defer srv.Close()
	c := sim.NewClient(map[string]string{"svc": srv.URL}, sim.NewCallStats())
	type item struct {
		ID string `json:"id"`
	}
	got, r := listAll[item](context.Background(), c, "svc", "GET /things?ref", "/things?ref=x")
	if !r.OK() || len(got) != 2 || got[0].ID != "a" || got[1].ID != "b" {
		t.Fatalf("got %v (%d)", got, r.Status)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(queries) != 2 {
		t.Fatalf("queries %v, want two pages", queries)
	}
	for _, q := range queries {
		if !strings.Contains(q, "ref=x") || !strings.Contains(q, "limit=100") || strings.Contains(q, "limit=500") {
			t.Fatalf("query %q: want the filter repeated and limit=100", q)
		}
	}
}
