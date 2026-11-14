package transport

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/sim"
)

// Driver is the delivery driver (last-mile-delivery): it starts a route and
// records an attempt per package.
//
//   - POST /routes/{id}/starts needs the route's wave DISPATCHED and emits
//     PackageOutForDelivery per tracking number. It takes no body: the service
//     stamps the scan with its own clock.
//   - POST /routes/{id}/attempts {trackingNumber, outcome, at}: DELIVERED,
//     FAILED (needs a failureReason; the package is re-inducted onto the next
//     wave and the route cannot take a second attempt for it) or COLLECTED.
//     The attempt's `at` is the time of the scan shipping-and-tracking applies
//     in TIME order, so it goes through the Timeline: never at or before a time
//     already known for that package.
type Driver struct {
	C        *sim.Client
	Clock    Clock
	Log      sim.LogFunc
	Settings Settings
	Timeline *Timeline
}

// NewDriver builds a driver. tl may be nil.
func NewDriver(c *sim.Client, clock Clock, log sim.LogFunc, s Settings, tl *Timeline) *Driver {
	if tl == nil {
		tl = NewTimeline()
	}
	return &Driver{C: c, Clock: clock, Log: log, Settings: s.withDefaults(), Timeline: tl}
}

func (d *Driver) logf(format string, a ...any) {
	if d.Log != nil {
		d.Log("driver", format, a...)
	}
}

// FailureReason is the free text a simulated FAILED attempt carries (required by the contract).
const FailureReason = "recipient not home (simulated)"

// StartRoute moves a PLANNED route to OUT_FOR_DELIVERY.
func (d *Driver) StartRoute(ctx context.Context, routeID string) (Route, error) {
	r := d.C.Call(ctx, SvcLastMile, http.MethodPost, "POST /routes/{routeId}/starts", "/routes/"+pathEscape(routeID)+"/starts", nil)
	var rt Route
	if err := decode("last-mile POST /routes/{id}/starts", r, &rt); err != nil {
		return Route{}, err
	}
	d.logf("route %s out for delivery", routeID)
	return rt, nil
}

// RecordAttempt records one delivery attempt for a package of a started route
// and returns it with the time it was sent. reason is used only for FAILED
// (FailureReason when empty); a FAILED attempt also says NOT_HOME.
func (d *Driver) RecordAttempt(ctx context.Context, routeID, tn, outcome, reason string) (Attempt, error) {
	at := d.Timeline.Stamp(tn, d.Clock.Now())
	body := map[string]any{"trackingNumber": tn, "outcome": outcome, "at": at.UTC()}
	if outcome == OutcomeFailed {
		if reason == "" {
			reason = FailureReason
		}
		body["failureReason"] = reason
		body["outcomeDetail"] = "NOT_HOME"
	} else {
		reason = ""
	}
	r := d.C.Call(ctx, SvcLastMile, http.MethodPost, "POST /routes/{routeId}/attempts", "/routes/"+pathEscape(routeID)+"/attempts", body)
	if !r.OK() {
		return Attempt{}, stepErr("last-mile POST /routes/{id}/attempts", r)
	}
	d.logf("route %s: %s %s at %s", routeID, tn, outcome, at.UTC().Format(time.RFC3339))
	return Attempt{RouteID: routeID, TrackingNumber: tn, Outcome: outcome, At: at, FailureReason: reason}, nil
}

// RunRoute records the same outcome for every package. A refused attempt is a
// finding (the string says which package and why) and the rest go on.
func (d *Driver) RunRoute(ctx context.Context, routeID string, tns []string, outcome string) ([]Attempt, []string) {
	var atts []Attempt
	var problems []string
	for _, tn := range tns {
		a, err := d.RecordAttempt(ctx, routeID, tn, outcome, "")
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s (package %s)", err, tn))
			continue
		}
		atts = append(atts, a)
	}
	return atts, problems
}
