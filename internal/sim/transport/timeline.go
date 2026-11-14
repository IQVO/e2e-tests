package transport

import (
	"sync"
	"time"
)

// scanStep is the gap the actors leave between two times of one package:
// shipping-and-tracking applies scans in TIME order and stores an older one
// without applying it, so two times the actor sends for one package must never
// share or reverse an instant. A millisecond is far above the services' stored
// resolution and far below the round trip to the next service-stamped event, so
// a lifted time is never ahead of what the service stamps next.
const scanStep = time.Millisecond

// Timeline is the monotonic time line of every package the actors touch. A
// time the actor sends (a delivery attempt, a carrier scan) goes through Stamp;
// a time the SERVICE stamped (a load's departedAt/arrivedAt, the newest scan of
// a journey) goes through Observe. Stamp never returns a time at or before the
// last one known for that tracking number, so a compressed or skewed clock can
// never send a scan older than the previous one.
type Timeline struct {
	mu   sync.Mutex
	last map[string]time.Time
}

// NewTimeline returns an empty timeline.
func NewTimeline() *Timeline { return &Timeline{last: map[string]time.Time{}} }

// Observe records a time the service stamped for tn. It only ever raises the line.
func (t *Timeline) Observe(tn string, at time.Time) {
	if at.IsZero() {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if at.After(t.last[tn]) {
		t.last[tn] = at
	}
}

// Stamp returns want, or the first instant after the last known time of tn when
// want is not later, and records it.
func (t *Timeline) Stamp(tn string, want time.Time) time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	if prev, ok := t.last[tn]; ok && !want.After(prev) {
		want = prev.Add(scanStep)
	}
	t.last[tn] = want
	return want
}

// Last returns the newest time known for tn (zero when none).
func (t *Timeline) Last(tn string) time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.last[tn]
}
