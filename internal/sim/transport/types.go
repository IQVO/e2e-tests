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
// segments (the Kong route is /api/<service>).
const (
	SvcLinehaul = "linehaul-transportation"
	SvcLastMile = "last-mile-delivery"
	SvcTracking = "shipping-and-tracking"
	SvcNodes    = "node-topology"
	SvcOrdering = "customer-ordering"
)

// Attempt outcomes of last-mile-delivery POST /routes/{id}/attempts.
const (
	OutcomeDelivered = "DELIVERED"
	OutcomeFailed    = "FAILED"
	OutcomeCollected = "COLLECTED"
)

// Parcel is what the warehouse side hands to the network: the tracking numbers
// of one shipped order (the ONLY link between the warehouse and the network
// side), the node the load leaves from and the delivery station it goes to.
type Parcel struct {
	TrackingNumbers   []string
	OriginNodeID      string
	DestStationNodeID string
}

// Options tunes one Deliver run.
type Options struct {
	// FailFirstAttempt makes the first delivery attempt of every package FAILED
	// (recipient not home); the packages are re-inducted onto a later wave and
	// delivered by a second route: the redelivery.
	FailFirstAttempt bool
	// SkipVerify skips every read of shipping-and-tracking (journey exists
	// before dispatch, OUT_FOR_DELIVERY before an attempt, DELIVERED at the
	// end). The delivery is then judged by last-mile-delivery's answers only.
	SkipVerify bool
}

// Attempt is one recorded delivery attempt.
type Attempt struct {
	RouteID        string
	TrackingNumber string
	Outcome        string
	At             time.Time
	FailureReason  string
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
