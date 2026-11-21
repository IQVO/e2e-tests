package sim

import (
	"context"
	"sync"
	"time"
)

// SuspendWatch detects host suspensions (laptop sleep) during a run.
//
// The simulators compress time (or at least run for tens of minutes) while the
// services under test keep REAL clocks: claim leases (5 min), statement
// timeouts, reservation TTLs, the network's PO ack window. A suspension
// freezes every actor mid-task while those real clocks keep running, so on
// wake the floor sees genuine 409 task-not-owner / reservation-already-resolved
// answers and one-off timeouts that no service caused. They are still
// reported, but each finding that falls inside a suspension window is
// attributed to it rather than counted as an operational failure.
type SuspendWatch struct {
	Log     LogFunc
	mu      sync.Mutex
	windows []Suspension
}

// Suspension is one detected host sleep.
type Suspension struct {
	From, To time.Time
}

// SuspensionSlack is how far a 1s ticker may overshoot before the gap is
// treated as a host suspension rather than scheduling jitter.
const SuspensionSlack = 20 * time.Second

// Run watches the wall clock until ctx is done.
func (w *SuspendWatch) Run(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	last := time.Now().Round(0) // wall clock: monotonic time stops while suspended
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			now := time.Now().Round(0)
			if gap := now.Sub(last); gap > SuspensionSlack {
				w.mu.Lock()
				w.windows = append(w.windows, Suspension{From: last, To: now})
				w.mu.Unlock()
				if w.Log != nil {
					w.Log("host", "host was suspended for %s (laptop sleep?); real-clock leases/timeouts may have lapsed", gap.Round(time.Second))
				}
			}
			last = now
		}
	}
}

// Covers reports whether t falls inside (or within the grace period after)
// a recorded suspension. Grace covers the leases that lapsed while the host
// slept and are only discovered on the actors' next calls.
func (w *SuspendWatch) Covers(t time.Time, grace time.Duration) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, s := range w.windows {
		if !t.Before(s.From) && !t.After(s.To.Add(grace)) {
			return true
		}
	}
	return false
}

// Snapshot returns a copy of the recorded suspensions.
func (w *SuspendWatch) Snapshot() []Suspension {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]Suspension(nil), w.windows...)
}
