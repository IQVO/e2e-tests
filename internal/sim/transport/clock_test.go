package transport

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

var t0 = time.Date(2026, 11, 14, 21, 45, 0, 0, time.UTC) // 15:45 America/Chicago

func TestManualClockSleepAdvancesInstantly(t *testing.T) {
	c := NewManualClock(t0)
	start := time.Now()
	if err := c.Sleep(context.Background(), 3*time.Hour); err != nil {
		t.Fatal(err)
	}
	if got := c.Now(); !got.Equal(t0.Add(3 * time.Hour)) {
		t.Fatalf("Now = %s, want %s", got, t0.Add(3*time.Hour))
	}
	if time.Since(start) > time.Second {
		t.Fatal("a manual clock must not really sleep")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.Sleep(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("Sleep on a cancelled context = %v, want context.Canceled", err)
	}
	if !c.Now().Equal(t0.Add(3 * time.Hour)) {
		t.Fatal("a cancelled sleep must not advance the clock")
	}
}

func TestAwaitReturnsWhenDoneAndBoundsTheWait(t *testing.T) {
	c := NewManualClock(t0)
	calls := 0
	err := await(context.Background(), c, time.Minute, 5*time.Second, func(context.Context) (bool, string, error) {
		calls++
		return calls == 3, "not yet", nil
	})
	if err != nil || calls != 3 {
		t.Fatalf("err=%v calls=%d, want nil after 3 polls", err, calls)
	}
	if got := c.Now().Sub(t0); got != 10*time.Second {
		t.Fatalf("clock advanced %s, want 10s (two sleeps)", got)
	}

	c = NewManualClock(t0)
	calls = 0
	err = await(context.Background(), c, time.Minute, 5*time.Second, func(context.Context) (bool, string, error) {
		calls++
		return false, "still empty", nil
	})
	if !IsTimeout(err) {
		t.Fatalf("err = %v, want a timeout", err)
	}
	if got := c.Now().Sub(t0); got > time.Minute+5*time.Second {
		t.Fatalf("waited %s of fake time, want a bound of 1m", got)
	}
	if calls < 2 || calls > 14 {
		t.Fatalf("polled %d times, want a bounded handful", calls)
	}
	if want := "still empty"; !strings.Contains(err.Error(), want) {
		t.Fatalf("timeout error %q must carry the last observation %q", err, want)
	}
}

func TestAwaitStopsOnAFatalError(t *testing.T) {
	c := NewManualClock(t0)
	boom := errors.New("boom")
	err := await(context.Background(), c, time.Minute, time.Second, func(context.Context) (bool, string, error) {
		return false, "", boom
	})
	if !errors.Is(err, boom) || IsTimeout(err) {
		t.Fatalf("err = %v, want the fatal error unchanged", err)
	}
}

func TestTimelineNeverGoesBackwards(t *testing.T) {
	tl := NewTimeline()
	a := t0
	if got := tl.Stamp("RN1", a); !got.Equal(a) {
		t.Fatalf("first stamp = %s, want the wanted time %s", got, a)
	}
	// An older wish is lifted just past the last time used for this package.
	if got := tl.Stamp("RN1", a.Add(-time.Hour)); !got.After(a) {
		t.Fatalf("stamp %s is not after %s", got, a)
	}
	// The same instant twice is not allowed either: scans of one package are strictly ordered.
	s1 := tl.Stamp("RN1", a.Add(time.Hour))
	s2 := tl.Stamp("RN1", a.Add(time.Hour))
	if !s2.After(s1) {
		t.Fatalf("second stamp %s must be after %s", s2, s1)
	}
	// Another package has its own line.
	if got := tl.Stamp("RN2", a.Add(-time.Hour)); !got.Equal(a.Add(-time.Hour)) {
		t.Fatalf("RN2 stamp = %s, want its own wanted time", got)
	}
	// An observed server time (a LoadDeparted stamp) lifts the line too.
	tl.Observe("RN3", a.Add(2*time.Hour))
	if got := tl.Stamp("RN3", a); !got.After(a.Add(2 * time.Hour)) {
		t.Fatalf("stamp %s must come after the observed %s", got, a.Add(2*time.Hour))
	}
	// Observing an older time never lowers it.
	tl.Observe("RN3", a)
	if !tl.Last("RN3").After(a.Add(2 * time.Hour)) {
		t.Fatal("Observe lowered the line")
	}
}
