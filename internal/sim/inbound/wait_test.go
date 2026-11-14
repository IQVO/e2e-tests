package inbound

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/sim"
	"github.com/claudioed/warehouse-e2e/internal/sim/outbound"
	"github.com/claudioed/warehouse-e2e/internal/sim/transport"
)

var t0 = time.Date(2026, 11, 14, 15, 0, 0, 0, time.UTC)

func TestAwaitReturnsWhenCheckPasses(t *testing.T) {
	clock := transport.NewManualClock(t0)
	calls := 0
	err := await(context.Background(), clock, Waits{Timeout: time.Minute, Interval: time.Second}, "the thing", func() (string, error) {
		calls++
		if calls < 4 {
			return "not yet", nil
		}
		return "", nil
	})
	if err != nil {
		t.Fatalf("await: %v", err)
	}
	if calls != 4 {
		t.Errorf("check ran %d times, want 4", calls)
	}
	if got := clock.Now().Sub(t0); got != 3*time.Second {
		t.Errorf("clock advanced %s, want 3s (one Interval per failed check)", got)
	}
}

func TestAwaitTimeoutNamesWhatAndLastObservation(t *testing.T) {
	clock := transport.NewManualClock(t0)
	n := 0
	err := await(context.Background(), clock, Waits{Timeout: 5 * time.Second, Interval: time.Second}, "load the truck", func() (string, error) {
		n++
		return "saw state BOOKED", nil
	})
	if err == nil {
		t.Fatal("await returned nil, want a timeout")
	}
	if !IsTimeout(err) {
		t.Errorf("IsTimeout(%v) = false", err)
	}
	for _, want := range []string{"load the truck", "5s", "saw state BOOKED"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("timeout error %q lacks %q", err, want)
		}
	}
	// A bounded wait: the number of checks is fixed by the injected clock, never by scheduling.
	if n != 6 {
		t.Errorf("check ran %d times, want 6 (t=0..5s at 1s)", n)
	}
	if got := clock.Now().Sub(t0); got != 5*time.Second {
		t.Errorf("clock advanced %s, want exactly the 5s timeout", got)
	}
}

func TestAwaitFailureEndsTheWaitAtOnce(t *testing.T) {
	clock := transport.NewManualClock(t0)
	boom := errors.New("boom")
	n := 0
	err := await(context.Background(), clock, Waits{}, "x", func() (string, error) { n++; return "", boom })
	if !errors.Is(err, boom) || n != 1 {
		t.Fatalf("err = %v after %d checks, want boom after 1", err, n)
	}
	if IsTimeout(err) {
		t.Error("a fatal error must not read as a timeout")
	}
}

func TestAwaitStopsWhenTheContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	clock := transport.NewManualClock(t0)
	n := 0
	err := await(ctx, clock, Waits{Timeout: time.Hour, Interval: time.Second}, "x", func() (string, error) {
		n++
		cancel()
		return "waiting", nil
	})
	if !errors.Is(err, context.Canceled) || n != 1 {
		t.Fatalf("err = %v after %d checks, want context.Canceled after 1", err, n)
	}
}

func TestWaitsDefaultToTheSuitesThirtySecondsAndHalfASecond(t *testing.T) {
	w := withDefaultWaits(Waits{})
	if w.Timeout != 30*time.Second || w.Interval != 500*time.Millisecond {
		t.Errorf("defaults = %+v, want 30s / 500ms", w)
	}
	w = withDefaultWaits(Waits{Timeout: time.Second, Interval: time.Millisecond})
	if w.Timeout != time.Second || w.Interval != time.Millisecond {
		t.Errorf("explicit waits were overwritten: %+v", w)
	}
}

// ---------------------------------------------------------------------

// scripted serves a fixed sequence of answers to POST /x, then the last one for ever,
// and records the Idempotency-Key of every request.
type scripted struct {
	mu      sync.Mutex
	answers []scriptedAnswer
	keys    []string
}

type scriptedAnswer struct {
	status int
	slug   string // fleet problem `type` slug
	code   string // fleet `code` (outbound-shipping style)
}

func (s *scripted) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keys = append(s.keys, r.Header.Get("Idempotency-Key"))
	i := min(len(s.keys)-1, len(s.answers)-1)
	a := s.answers[i]
	w.WriteHeader(a.status)
	switch {
	case a.code != "":
		_, _ = w.Write([]byte(`{"type":"about:blank","code":"` + a.code + `"}`))
	case a.slug != "":
		_, _ = w.Write([]byte(`{"type":"https://errors.svc.warehouse-systems.dev/` + a.slug + `"}`))
	default:
		_, _ = w.Write([]byte(`{"ok":true}`))
	}
}

func scriptedCrew(t *testing.T, answers ...scriptedAnswer) (*Crew, *scripted, *transport.ManualClock) {
	t.Helper()
	s := &scripted{answers: answers}
	srv := httptest.NewServer(s)
	t.Cleanup(srv.Close)
	clock := transport.NewManualClock(t0)
	c := &Crew{
		C:     sim.NewClient(map[string]string{SvcInbound: srv.URL}, sim.NewCallStats()),
		Site:  Site{Code: "AUS3"},
		RunID: "T1",
		Clock: clock,
		Wait:  Waits{Timeout: 10 * time.Second, Interval: time.Second},
	}
	return c, s, clock
}

func postX(c *Crew) sim.Resp {
	return c.C.Call(context.Background(), SvcInbound, http.MethodPost, "/x", "/x", map[string]any{})
}

var lagUnknownASN = outbound.Transient{Status: http.StatusUnprocessableEntity, Slug: "unknown-asn"}

func TestCallTransientRetriesOnlyTheExactStatusAndSlugWithAFreshKeyEachTime(t *testing.T) {
	c, s, clock := scriptedCrew(t,
		scriptedAnswer{status: 422, slug: "unknown-asn"},
		scriptedAnswer{status: 422, slug: "unknown-asn"},
		scriptedAnswer{status: 201})
	r, err := c.callTransient(context.Background(), "book", []outbound.Transient{lagUnknownASN}, func() sim.Resp { return postX(c) })
	if err != nil || r.Status != 201 {
		t.Fatalf("callTransient = %d, %v; want 201", r.Status, err)
	}
	if len(s.keys) != 3 {
		t.Fatalf("%d requests, want 3", len(s.keys))
	}
	seen := map[string]bool{}
	for _, k := range s.keys {
		if k == "" || seen[k] {
			t.Errorf("Idempotency-Keys %v: want three distinct non-empty keys (a refusal is stored under its key)", s.keys)
		}
		seen[k] = true
	}
	if got := clock.Now().Sub(t0); got != 2*time.Second {
		t.Errorf("waited %s of clock time, want 2s (two lag refusals at 1s)", got)
	}
}

func TestCallTransientDoesNotRetryOtherRefusals(t *testing.T) {
	for _, tc := range []struct {
		name string
		a    scriptedAnswer
		want string
	}{
		{"same slug, other status", scriptedAnswer{status: 409, slug: "unknown-asn"}, "409 unknown-asn"},
		{"same status, other slug", scriptedAnswer{status: 422, slug: "window-in-past"}, "422 window-in-past"},
		{"a 5xx", scriptedAnswer{status: 503, slug: "dependency-unavailable"}, "503 dependency-unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, s, _ := scriptedCrew(t, tc.a)
			_, err := c.callTransient(context.Background(), "book", []outbound.Transient{lagUnknownASN}, func() sim.Resp { return postX(c) })
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to name %q", err, tc.want)
			}
			if len(s.keys) != 1 {
				t.Errorf("%d requests, want exactly 1: only the exact lag pair may be retried", len(s.keys))
			}
		})
	}
}

func TestCallTransientFailsAtOnceOnATransportError(t *testing.T) {
	c, _, _ := scriptedCrew(t, scriptedAnswer{status: 201})
	n := 0
	_, err := c.callTransient(context.Background(), "book", []outbound.Transient{lagUnknownASN}, func() sim.Resp {
		n++
		return sim.Resp{Status: 0, Body: []byte("connection refused")}
	})
	if err == nil || !strings.Contains(err.Error(), "transport error") || n != 1 {
		t.Fatalf("err = %v after %d calls, want a transport error after 1", err, n)
	}
}

func TestCallTransientTimeoutNamesTheLagThatNeverCleared(t *testing.T) {
	c, s, _ := scriptedCrew(t, scriptedAnswer{status: 422, slug: "unknown-asn"})
	_, err := c.callTransient(context.Background(), "book door", []outbound.Transient{lagUnknownASN}, func() sim.Resp { return postX(c) })
	if err == nil || !IsTimeout(err) || !strings.Contains(err.Error(), "422 unknown-asn") {
		t.Fatalf("err = %v, want a timeout naming 422 unknown-asn", err)
	}
	if len(s.keys) != 11 {
		t.Errorf("%d requests, want 11 (every 1s for 10s)", len(s.keys))
	}
}

func TestSlugIsTheFleetCodeElseTheLastSegmentOfType(t *testing.T) {
	c, _, _ := scriptedCrew(t, scriptedAnswer{status: 409, code: "package-not-found"})
	if got := outbound.Slug(postX(c)); got != "package-not-found" {
		t.Errorf("slug = %q", got)
	}
}
