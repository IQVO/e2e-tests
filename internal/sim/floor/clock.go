package floor

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Clock is the actors' only source of time: Now bounds every wait and Sleep is
// every pause between two looks at a service. SystemClock is the real one; a
// test passes a clock whose Sleep advances time instead of waiting, so a bounded
// wait of minutes costs nothing. transport.Clock has the same shape, so either
// package's clock satisfies this interface.
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

// Waits bounds an eventually-style loop: the check is repeated every Interval
// until it succeeds or Timeout of clock time has passed without progress.
type Waits struct{ Timeout, Interval time.Duration }

func (w Waits) orDefault(def Waits) Waits {
	if w.Timeout <= 0 {
		w.Timeout = def.Timeout
	}
	if w.Interval <= 0 {
		w.Interval = def.Interval
	}
	return w
}

// errTimeout is wrapped by every bounded wait that ran out.
var errTimeout = errors.New("timed out")

// IsTimeout reports whether err is a bounded wait that expired.
func IsTimeout(err error) bool { return errors.Is(err, errTimeout) }

// verdict is what one look of a wait found.
type verdict int

const (
	// vDone: the wait is over.
	vDone verdict = iota
	// vWait: not yet; sleep one interval and look again.
	vWait
	// vAgain: progress was made (a task was worked); look again at once and
	// restart the timeout, which bounds waiting without progress.
	vAgain
)

// maxAgain bounds a run of consecutive vAgain looks, so a service that reports
// progress for ever cannot keep an actor in a loop.
const maxAgain = 10000

// await repeats check until it reports vDone, fails, or w.Timeout of clock time
// passes without progress. check returns what it found, a reason that says what
// it is still waiting for (only for vWait), and a fatal error that ends the wait
// at once. A timeout error carries the last reason: a bounded wait that expires
// reads like a service bug, so it must say what never arrived.
func await(ctx context.Context, clock Clock, w Waits, what string, check func(ctx context.Context) (verdict, string, error)) error {
	deadline := clock.Now().Add(w.Timeout)
	last := ""
	again := 0
	for {
		v, why, err := check(ctx)
		if err != nil {
			return err
		}
		switch v {
		case vDone:
			return nil
		case vAgain:
			again++
			if again > maxAgain {
				return fmt.Errorf("%s: %w: still making progress after %d looks, giving up", what, errTimeout, maxAgain)
			}
			deadline = clock.Now().Add(w.Timeout)
			continue
		}
		again = 0
		last = why
		if clock.Now().Add(w.Interval).After(deadline) {
			return fmt.Errorf("%s: %w after %s (last seen: %s)", what, errTimeout, w.Timeout, last)
		}
		if err := clock.Sleep(ctx, w.Interval); err != nil {
			return fmt.Errorf("%s: %w (last seen: %s)", what, err, last)
		}
	}
}
