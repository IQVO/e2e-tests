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
//   - POST /routes/{id}/attempts {trackingNumber, outcome, outcomeDetail, at}: DELIVERED,
//     FAILED (needs a failureReason; the package is re-inducted onto the next
//     wave and the route cannot take a second attempt for it) or COLLECTED.
//     outcomeDetail is optional and from a closed list (ADR 0014-delivery-point-profile
//     of last-mile-delivery): the driver client is what activates the delivery-point
//     profile, so this actor reports it (ReportAttempt).
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
// (FailureReason when empty); a FAILED attempt also says NOT_HOME, a DELIVERED
// one reports no detail. ReportAttempt chooses the detail.
func (d *Driver) RecordAttempt(ctx context.Context, routeID, tn, outcome, reason string) (Attempt, error) {
	return d.ReportAttempt(ctx, routeID, tn, outcome, reason, defaultDetail(outcome))
}

// defaultDetail is what RecordAttempt reports: a failed visit is "nobody home".
func defaultDetail(outcome string) string {
	if outcome == OutcomeFailed {
		return DetailNotHome
	}
	return ""
}

// ReportAttempt records one delivery attempt and reports its outcomeDetail, one of the closed list of the
// contract (NOT_HOME, ACCESS_PROBLEM, OTHER for FAILED; SAFE_PLACE_USED for DELIVERED; "" = not reported,
// which is not OTHER). A detail the contract does not accept for the outcome is refused here, before any
// call: it would be a 422 from the service and a client that sends it is a driver that is wrong, not a
// finding about the service. reason is used only for FAILED (FailureReason when empty).
func (d *Driver) ReportAttempt(ctx context.Context, routeID, tn, outcome, reason, detail string) (Attempt, error) {
	if !DetailFits(outcome, detail) {
		return Attempt{}, fmt.Errorf("last-mile POST /routes/{id}/attempts: outcomeDetail %q is not accepted for outcome %s (FAILED: %s, %s, %s; DELIVERED: %s)",
			detail, outcome, DetailNotHome, DetailAccessProblem, DetailOther, DetailSafePlaceUsed)
	}
	at := d.Timeline.Stamp(tn, d.Clock.Now())
	body := map[string]any{"trackingNumber": tn, "outcome": outcome, "at": at.UTC()}
	if outcome == OutcomeFailed {
		if reason == "" {
			reason = FailureReason
		}
		body["failureReason"] = reason
	} else {
		reason = ""
	}
	if detail != "" {
		body["outcomeDetail"] = detail
	}
	r := d.C.Call(ctx, SvcLastMile, http.MethodPost, "POST /routes/{routeId}/attempts", "/routes/"+pathEscape(routeID)+"/attempts", body)
	if !r.OK() {
		return Attempt{}, stepErr("last-mile POST /routes/{id}/attempts", r)
	}
	d.logf("route %s: %s %s %s at %s", routeID, tn, outcome, detail, at.UTC().Format(time.RFC3339))
	return Attempt{RouteID: routeID, TrackingNumber: tn, Outcome: outcome, OutcomeDetail: detail, At: at, FailureReason: reason}, nil
}

// RunRoute records the same outcome for every package (a FAILED one says NOT_HOME). A refused attempt is a
// finding (the string says which package and why) and the rest go on.
func (d *Driver) RunRoute(ctx context.Context, routeID string, tns []string, outcome string) ([]Attempt, []string) {
	return d.RunRouteDetail(ctx, routeID, tns, outcome, defaultDetail(outcome))
}

// RunRouteDetail is RunRoute reporting the given outcomeDetail on every attempt.
func (d *Driver) RunRouteDetail(ctx context.Context, routeID string, tns []string, outcome, detail string) ([]Attempt, []string) {
	var atts []Attempt
	var problems []string
	for _, tn := range tns {
		a, err := d.ReportAttempt(ctx, routeID, tn, outcome, "", detail)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s (package %s)", err, tn))
			continue
		}
		atts = append(atts, a)
	}
	return atts, problems
}
