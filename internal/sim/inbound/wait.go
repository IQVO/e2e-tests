package inbound

import (
	"context"
	"errors"
	"fmt"

	"github.com/claudioed/warehouse-e2e/internal/sim"
	"github.com/claudioed/warehouse-e2e/internal/sim/outbound"
)

// errTimeout is wrapped by every bounded wait that ran out.
var errTimeout = errors.New("timed out")

// IsTimeout reports whether err is a bounded wait that expired.
func IsTimeout(err error) bool { return errors.Is(err, errTimeout) }

// await runs check every w.Interval of clock time until it reports done (an empty reason), fails, or
// w.Timeout of clock time has passed. check returns ("", nil) when done, (reason, nil) while it is still
// waiting (reason says for what) and ("", err) for a failure that must not be retried. A timeout is an error
// that names the last reason: a bounded wait that expires reads like a service bug, so it says what never
// arrived. The wait is counted in clock time, so a test clock makes it exact and instant.
func await(ctx context.Context, clock Clock, w Waits, what string, check func() (string, error)) error {
	w = withDefaultWaits(w)
	deadline := clock.Now().Add(w.Timeout)
	for {
		reason, err := check()
		if err != nil {
			return err
		}
		if reason == "" {
			return nil
		}
		if clock.Now().Add(w.Interval).After(deadline) {
			return fmt.Errorf("%s: %w after %s: %s", what, errTimeout, w.Timeout, reason)
		}
		if err := clock.Sleep(ctx, w.Interval); err != nil {
			return fmt.Errorf("%s: %w (last: %s)", what, err, reason)
		}
	}
}

// refusal is the error of a non-2xx answer the caller does not tolerate.
func refusal(what string, r sim.Resp) error {
	if r.Status == 0 {
		return fmt.Errorf("%s: transport error: %s", what, sim.Truncate(string(r.Body), 200))
	}
	return fmt.Errorf("%s -> %d %s: %s", what, r.Status, outbound.Slug(r), sim.Truncate(string(r.Body), 200))
}

// lagMatches reports whether r is exactly the lag refusal t: the status AND the slug.
func lagMatches(t outbound.Transient, r sim.Resp) bool {
	return r.Status == t.Status && outbound.Slug(r) == t.Slug
}

// callTransient performs do until it answers 2xx. An answer matching one of lags is a consumer-lag refusal
// (a fact another service has not consumed yet) and is retried, bounded, as a fresh request, hence a fresh
// Idempotency-Key: the services store a 4xx under the key, so re-sending the key would replay the refusal
// for ever. Any other refusal, and a transport error, is returned at once.
func (c *Crew) callTransient(ctx context.Context, what string, lags []outbound.Transient, do func() sim.Resp) (sim.Resp, error) {
	var last sim.Resp
	err := await(ctx, c.clock(), c.Wait, what, func() (string, error) {
		last = do()
		if last.OK() {
			return "", nil
		}
		for _, t := range lags {
			if lagMatches(t, last) {
				return fmt.Sprintf("%d %s", last.Status, t.Slug), nil
			}
		}
		return "", refusal(what, last)
	})
	return last, err
}
