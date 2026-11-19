package transport

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/sim"
)

// Service names: the keys of sim.Client.Base, which are also the gateway path
// segments (the gateway route is /api/<service>).
const (
	SvcLinehaul = "linehaul-transportation"
	SvcLastMile = "last-mile-delivery"
	SvcTracking = "shipping-and-tracking"
	SvcNodes    = "node-topology"
	SvcOrdering = "customer-ordering"
	SvcVendor   = "vendor-connectivity"
)

// Attempt outcomes of last-mile-delivery POST /routes/{id}/attempts.
const (
	OutcomeDelivered = "DELIVERED"
	OutcomeFailed    = "FAILED"
	OutcomeCollected = "COLLECTED"
)

// Outcome details of POST /routes/{id}/attempts (schema OutcomeDetail, last-mile-delivery ADR 0014): a
// closed list. FAILED accepts NOT_HOME, ACCESS_PROBLEM and OTHER; DELIVERED accepts SAFE_PLACE_USED; any
// other pairing is 422 validation-failed. Omitted means "not reported" and is NOT the same as OTHER.
const (
	DetailNotHome       = "NOT_HOME"
	DetailAccessProblem = "ACCESS_PROBLEM"
	DetailOther         = "OTHER"
	DetailSafePlaceUsed = "SAFE_PLACE_USED"
)

// FlagAfterStrikes is the number of counted failed visits (routes) at one delivery point after which
// last-mile-delivery flags it (ADR 0014, FlagAfterStrikes). MaxFailedAttempts is how many failed attempts a
// package can have and still be re-inducted: the third failure initiates its return instead (so a
// delivery-point journey needs exactly FlagAfterStrikes failures and then a delivery).
const (
	FlagAfterStrikes  = 2
	MaxFailedAttempts = 2
)

// DetailFits reports whether the contract accepts detail for outcome ("" is always accepted: not reported).
func DetailFits(outcome, detail string) bool {
	switch detail {
	case "":
		return true
	case DetailNotHome, DetailAccessProblem, DetailOther:
		return outcome == OutcomeFailed
	case DetailSafePlaceUsed:
		return outcome == OutcomeDelivered
	}
	return false
}

// Counted reports whether a FAILED attempt with this detail is a strike of the delivery point: only
// NOT_HOME and ACCESS_PROBLEM count; OTHER and "not reported" are tallied, never counted.
func Counted(detail string) bool { return detail == DetailNotHome || detail == DetailAccessProblem }

// FlagReason is the reason last-mile-delivery gives a flag raised by these counted details: ACCESS_PROBLEM
// when any of them is one, else NOT_HOME; "" when none is counted.
func FlagReason(details ...string) string {
	reason := ""
	for _, d := range details {
		switch d {
		case DetailAccessProblem:
			return DetailAccessProblem
		case DetailNotHome:
			reason = DetailNotHome
		}
	}
	return reason
}

// Parcel is what the warehouse side hands to the network: the tracking numbers
// of one shipped order (the ONLY link between the warehouse and the network
// side), the node the load leaves from and the delivery station it goes to.
type Parcel struct {
	TrackingNumbers   []string
	OriginNodeID      string
	DestStationNodeID string
	// OrderID is customer-ordering's order of the packages. Optional: with it, a delivery-point flag is also
	// looked for on the order's tracking view (the advisory); without it only the event tap can observe it.
	OrderID string
}

// Options tunes one Deliver run.
type Options struct {
	// FailFirstAttempt makes the first delivery attempt of every package FAILED
	// (recipient not home); the packages are re-inducted onto a later wave and
	// delivered by a second route: the redelivery.
	FailFirstAttempt bool
	// FailedAttempts makes the first N delivery attempts of every package FAILED (N = 0..MaxFailedAttempts),
	// each followed by a re-induction onto a later wave and a new route; a delivery follows the last one.
	// It wins over FailFirstAttempt (which is FailedAttempts = 1). With FlagAfterStrikes counted failures
	// (FailureDetail NOT_HOME or ACCESS_PROBLEM) the delivery point is flagged and the run waits for the flag
	// (see Transport.Tap and Parcel.OrderID).
	FailedAttempts int
	// FailureDetail is the outcomeDetail every FAILED attempt reports; empty means NOT_HOME.
	FailureDetail string
	// DeliveredDetail is the outcomeDetail of the DELIVERED attempt: empty (not reported) or SAFE_PLACE_USED.
	DeliveredDetail string
	// SkipVerify skips every read of shipping-and-tracking (journey exists
	// before dispatch, OUT_FOR_DELIVERY before an attempt, DELIVERED at the
	// end). The delivery is then judged by last-mile-delivery's answers only.
	SkipVerify bool
}

// failures is how many attempts of every package are FAILED before the delivering one.
func (o Options) failures() int {
	if o.FailedAttempts != 0 {
		return o.FailedAttempts
	}
	if o.FailFirstAttempt {
		return 1
	}
	return 0
}

// failureDetail is the outcomeDetail of a FAILED attempt: NOT_HOME unless set.
func (o Options) failureDetail() string {
	if o.FailureDetail == "" {
		return DetailNotHome
	}
	return o.FailureDetail
}

// Attempt is one recorded delivery attempt.
type Attempt struct {
	RouteID        string
	TrackingNumber string
	Outcome        string
	// OutcomeDetail is what the driver reported (one of the closed list), "" when none.
	OutcomeDetail string
	At            time.Time
	FailureReason string
}

// Delivery is the result of one Deliver run. WaveID and RouteID are those that
// DELIVERED the packages (the last round); Waves and Routes list every round.
type Delivery struct {
	LoadID      string
	DepartureID string
	WaveID      string
	RouteID     string
	Waves       []string
	Routes      []string
	Attempts    []Attempt
	// DeliveredAt is the time of the last DELIVERED attempt, set only when every
	// package of the parcel was delivered.
	DeliveredAt time.Time
	// Flagged lists the packages the delivery-point flag was observed for (the tap saw DeliveryPointFlagged
	// carrying the tracking number, or the order's tracking view showed its advisory), FlagReason the reason
	// code it carried. Both are empty when no flag was expected or none was observed (then Problems says so).
	Flagged    []string
	FlagReason string
	// Problems are the findings of the run: every step a service refused or that
	// never happened within its bound. Empty means the run went as the contracts say.
	Problems []string
}

// Delivered reports whether every package of the parcel was delivered.
func (d Delivery) Delivered() bool { return !d.DeliveredAt.IsZero() }

// OK reports a delivered parcel with no finding.
func (d Delivery) OK() bool { return d.Delivered() && len(d.Problems) == 0 }

// Settings are the actors' bounds and defaults. A zero field takes its default
// (DefaultSettings), so Settings{} is valid.
type Settings struct {
	PollInterval     time.Duration // between two reads while waiting (2s)
	InductionTimeout time.Duration // wait for LoadArrived to induct the packages (2m)
	JourneyTimeout   time.Duration // wait for a shipping-and-tracking state or journey (2m)
	TrackingTimeout  time.Duration // wait for an order's tracking numbers (3m)
	TransitPause     time.Duration // between a dispatch and its receipt, so arrival is after departure (1s)

	DepartureLookahead time.Duration // how far ahead to look for the next pickup departure (7 days)
	MaxDepartureTries  int           // departures tried when one already has a load (5)

	InductLead       time.Duration // a created wave's induct cutoff after the arrival (10m)
	WaveWindowDelay  time.Duration // its delivery window starts this long after the cutoff (30m)
	WaveWindowLength time.Duration // and lasts this long (4h)
	StopCapacity     int           // stops a created wave takes (100)
	VanCapacity      int           // stops per route of the contracted partner (50)
}

// DefaultSettings returns the defaults.
func DefaultSettings() Settings {
	return Settings{
		PollInterval:       2 * time.Second,
		InductionTimeout:   2 * time.Minute,
		JourneyTimeout:     2 * time.Minute,
		TrackingTimeout:    3 * time.Minute,
		TransitPause:       time.Second,
		DepartureLookahead: 7 * 24 * time.Hour,
		MaxDepartureTries:  5,
		InductLead:         10 * time.Minute,
		WaveWindowDelay:    30 * time.Minute,
		WaveWindowLength:   4 * time.Hour,
		StopCapacity:       100,
		VanCapacity:        50,
	}
}

// withDefaults fills every zero field. TransitPause is the one field a caller
// may switch off, with a negative value.
func (s Settings) withDefaults() Settings {
	d := DefaultSettings()
	if s.PollInterval <= 0 {
		s.PollInterval = d.PollInterval
	}
	if s.InductionTimeout <= 0 {
		s.InductionTimeout = d.InductionTimeout
	}
	if s.JourneyTimeout <= 0 {
		s.JourneyTimeout = d.JourneyTimeout
	}
	if s.TrackingTimeout <= 0 {
		s.TrackingTimeout = d.TrackingTimeout
	}
	if s.TransitPause == 0 {
		s.TransitPause = d.TransitPause
	}
	if s.DepartureLookahead <= 0 {
		s.DepartureLookahead = d.DepartureLookahead
	}
	if s.MaxDepartureTries <= 0 {
		s.MaxDepartureTries = d.MaxDepartureTries
	}
	if s.InductLead <= 0 {
		s.InductLead = d.InductLead
	}
	if s.WaveWindowDelay <= 0 {
		s.WaveWindowDelay = d.WaveWindowDelay
	}
	if s.WaveWindowLength <= 0 {
		s.WaveWindowLength = d.WaveWindowLength
	}
	if s.StopCapacity <= 0 {
		s.StopCapacity = d.StopCapacity
	}
	if s.VanCapacity <= 0 {
		s.VanCapacity = d.VanCapacity
	}
	return s
}

// StepError is a refused or failed call: which step, the HTTP status and the
// RFC 9457 problem slug and detail the service answered.
type StepError struct {
	Step   string
	Status int
	Slug   string
	Detail string
}

func (e *StepError) Error() string {
	if e.Status == 0 {
		return fmt.Sprintf("%s: transport error: %s", e.Step, e.Detail)
	}
	if e.Slug == "" {
		return fmt.Sprintf("%s: %d %s", e.Step, e.Status, e.Detail)
	}
	return fmt.Sprintf("%s: %d %s: %s", e.Step, e.Status, e.Slug, e.Detail)
}

// stepErr turns a non-2xx answer into a StepError.
func stepErr(step string, r sim.Resp) *StepError {
	e := &StepError{Step: step, Status: r.Status, Detail: sim.Truncate(strings.TrimSpace(string(r.Body)), 200)}
	var p struct {
		Type   string `json:"type"`
		Detail string `json:"detail"`
	}
	if json.Unmarshal(r.Body, &p) == nil && p.Type != "" {
		e.Slug = p.Type[strings.LastIndex(p.Type, "/")+1:]
		if p.Detail != "" {
			e.Detail = sim.Truncate(p.Detail, 200)
		}
	}
	return e
}

// hasSlug reports whether err is a StepError carrying the problem slug.
func hasSlug(err error, slug string) bool {
	var se *StepError
	return errors.As(err, &se) && se.Slug == slug
}

// decode unmarshals an OK answer into v, or returns the refusal as a StepError.
func decode(step string, r sim.Resp, v any) error {
	if !r.OK() {
		return stepErr(step, r)
	}
	if err := r.Decode(v); err != nil {
		return fmt.Errorf("%s: %w", step, err)
	}
	return nil
}

// pathEscape keeps a tracking number or id safe inside a URL path.
func pathEscape(s string) string {
	return strings.NewReplacer("%", "%25", "/", "%2F", "?", "%3F", "#", "%23", " ", "%20").Replace(s)
}
