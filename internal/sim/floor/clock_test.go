package floor

import (
	"context"
	"sync"
	"time"
)

// manualClock is a Clock whose Sleep advances time instead of waiting, so a
// bounded wait of minutes costs nothing in a test. Safe for concurrent use: the
// contract fakes read it from their handler goroutines.
type manualClock struct {
	mu  sync.Mutex
	now time.Time
	// slept is the total time slept, for tests that assert a wait was (or was not) entered.
	slept time.Duration
}

func newManualClock(start time.Time) *manualClock { return &manualClock{now: start} }

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func (c *manualClock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.slept += d
	c.mu.Unlock()
	return nil
}

func (c *manualClock) Slept() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.slept
}
