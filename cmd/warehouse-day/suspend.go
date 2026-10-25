package main

import (
	"context"
	"sync"
	"time"
)

// suspendWatch detects host suspensions (laptop sleep) during the day.
//
// The simulator compresses a 16-hour day into minutes of wall time, but
// the services under test keep REAL clocks: claim leases (5 min), statement
// timeouts, reservation TTLs. A suspension freezes every associate mid-task
// while those real clocks keep running, so on wake the floor sees genuine
// 409 task-not-owner / reservation-already-resolved responses and one-off
// timeouts that no service caused. They are still reported, but each
// finding that falls inside a suspension window is attributed to it rather
// than counted as an operational failure.
type suspendWatch struct {
	mu      sync.Mutex
	windows []suspension
}

type suspension struct {
	From, To time.Time
}

// suspensionSlack is how far a 1s ticker may overshoot before the gap is
// treated as a host suspension rather than scheduling jitter.
const suspensionSlack = 20 * time.Second

func (w *suspendWatch) run(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	last := time.Now().Round(0) // wall clock: monotonic time stops while suspended
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			now := time.Now().Round(0)
			if gap := now.Sub(last); gap > suspensionSlack {
				w.mu.Lock()
				w.windows = append(w.windows, suspension{From: last, To: now})
				w.mu.Unlock()
				logf("host", "host was suspended for %s (laptop sleep?); real-clock leases/timeouts may have lapsed", gap.Round(time.Second))
			}
			last = now
		}
	}
}

// covers reports whether t falls inside (or within the grace period after)
// a recorded suspension. Grace covers the leases that lapsed while the host
// slept and are only discovered on the associates' next calls.
func (w *suspendWatch) covers(t time.Time, grace time.Duration) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, s := range w.windows {
		if !t.Before(s.From) && !t.After(s.To.Add(grace)) {
			return true
		}
	}
	return false
}

func (w *suspendWatch) snapshot() []suspension {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]suspension(nil), w.windows...)
}
