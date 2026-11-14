// Package transport simulates the retail network's transport legs over the
// services' PUBLISHED REST contracts only: the linehaul dispatcher
// (linehaul-transportation), the delivery-station associate and the delivery
// driver (last-mile-delivery), an optional carrier (shipping-and-tracking) and
// a tracking reader (customer-ordering + shipping-and-tracking). Transport
// composes them into one run from a shipped package to PackageDelivered.
//
// Nothing here touches a database, injects an event or imports another repo's
// Go packages. A step a service refuses is a FINDING (a string in
// Delivery.Problems), never a panic, and every wait is bounded. Everything that
// reads or waits on time takes the Clock below, so tests run instantly.
package transport

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Clock is the actors' only source of time: Now stamps what the actors send
// and Sleep is every wait (polling, the pause between a departure and its
// arrival). SystemClock is the real one, ManualClock the instant test clock.
type Clock interface {
	Now() time.Time
	Sleep(ctx context.Context, d time.Duration) error
}

// SystemClock is the wall clock.
type SystemClock struct{}

// Now returns the wall-clock time.
func (SystemClock) Now() time.Time { return time.Now() }

// Sleep waits d or until ctx is done.
func (SystemClock) Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// ManualClock is a Clock whose Sleep advances time instead of waiting, so a
// bounded wait of minutes costs nothing in a test. It is safe for concurrent
// use (the httptest fakes read it from their handler goroutines).
type ManualClock struct {
	mu  sync.Mutex
	now time.Time
}

// NewManualClock starts a manual clock at start.
func NewManualClock(start time.Time) *ManualClock { return &ManualClock{now: start} }

// Now returns the current manual time.
func (c *ManualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock forward by d.
func (c *ManualClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// Sleep advances the clock by d and returns at once; a done ctx returns its
// error without advancing.
func (c *ManualClock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.Advance(d)
	return nil
}

// errTimeout is wrapped by every bounded wait that ran out.
var errTimeout = errors.New("timed out")

// IsTimeout reports whether err is a bounded wait that expired.
func IsTimeout(err error) bool { return errors.Is(err, errTimeout) }

// await polls check every interval until it reports done, fails, or timeout of
// clock time has passed. check returns (done, what it saw, fatal error); a
// fatal error ends the wait at once. The timeout error carries the last
// observation so a finding says what the actor was waiting for and what it saw.
func await(ctx context.Context, clock Clock, timeout, interval time.Duration, check func(ctx context.Context) (bool, string, error)) error {
	deadline := clock.Now().Add(timeout)
	last := ""
	for {
		done, seen, err := check(ctx)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		last = seen
		if !clock.Now().Add(interval).Before(deadline.Add(time.Nanosecond)) {
			return fmt.Errorf("%w after %s (last seen: %s)", errTimeout, timeout, last)
		}
		if err := clock.Sleep(ctx, interval); err != nil {
			return err
		}
	}
}
