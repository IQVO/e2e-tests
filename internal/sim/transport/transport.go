package transport

import (
	"context"
	"fmt"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/sim"
)

// Transport is the network's transport legs as one actor set: the linehaul
// dispatcher, the delivery-station associate, the delivery driver, the carrier
// and the tracking reader, sharing one client, one clock and one Timeline (the
// monotonic time line per tracking number).
type Transport struct {
	Clock    Clock
	Log      sim.LogFunc
	Settings Settings
	Timeline *Timeline

	Dispatcher *Dispatcher
	Station    *Station
	Driver     *Driver
	Carrier    *Carrier
	Tracker    *Tracker
}

// New builds the actors over c. log may be nil; a zero Settings takes the defaults.
func New(c *sim.Client, clock Clock, log sim.LogFunc, s Settings) *Transport {
	s = s.withDefaults()
	tl := NewTimeline()
	return &Transport{
		Clock: clock, Log: log, Settings: s, Timeline: tl,
		Dispatcher: NewDispatcher(c, clock, log, s, tl),
		Station:    NewStation(c, clock, log, s, tl),
		Driver:     NewDriver(c, clock, log, s, tl),
		Carrier:    NewCarrier(c, clock, log, s, tl),
		Tracker:    NewTracker(c, clock, log, s, tl),
	}
}

func (t *Transport) logf(format string, a ...any) {
	if t.Log != nil {
		t.Log("transport", format, a...)
	}
}

// DeliverOrder reads the order's tracking numbers (waiting, bounded, for the
// event-fed view to list `packages` of them) and delivers them. A parcel the
// reader could not find is a finding.
func (t *Transport) DeliverOrder(ctx context.Context, orderID string, packages int, origin, destStation string, o Options) Delivery {
	tns, err := t.Tracker.OrderTrackingNumbers(ctx, orderID, packages)
	if err != nil {
		return Delivery{Problems: []string{err.Error()}}
	}
	return t.Deliver(ctx, Parcel{TrackingNumbers: tns, OriginNodeID: origin, DestStationNodeID: destStation}, o)
}

// Deliver runs one parcel from its shipment to PackageDelivered over published
// REST only:
//
//  1. (unless SkipVerify) wait for every package's journey to exist;
//  2. linehaul: next scheduled pickup departure, load, pack by tracking number,
//     close, dispatch, receipt (before the scheduled time: no time guard);
//  3. last-mile: a wave whose induct cutoff is after the arrival (existing, or
//     created minutes ahead), wait for induction, close it, ensure a partner,
//     plan the route (the wave must be INDUCTION_CLOSED), dispatch the wave,
//     start the route (the wave must be DISPATCHED);
//  4. attempts: DELIVERED, or with FailFirstAttempt FAILED first and then, after
//     the packages are re-inducted onto a second wave, a second route that
//     delivers them (the redelivery);
//  5. (unless SkipVerify) every package's journey reaches DELIVERED and none of
//     its scans was stored without being applied.
//
// A step a service refuses, or that does not happen within its bound, is a
// finding in Problems; the run stops where it cannot go on and Delivery says
// how far it got. DeliveredAt is set only when every package was delivered.
func (t *Transport) Deliver(ctx context.Context, p Parcel, o Options) Delivery {
	var d Delivery
	add := func(format string, a ...any) { d.Problems = append(d.Problems, fmt.Sprintf(format, a...)) }

	p.TrackingNumbers = dedupe(p.TrackingNumbers)
	if len(p.TrackingNumbers) == 0 || p.OriginNodeID == "" {
		add("parcel: needs tracking numbers and an origin node (got %d tracking numbers, origin %q)", len(p.TrackingNumbers), p.OriginNodeID)
		return d
	}

	if !o.SkipVerify {
		for _, tn := range p.TrackingNumbers {
			if _, err := t.Tracker.AwaitJourney(ctx, tn); err != nil {
				add("%s (the label or shipment event has not reached shipping-and-tracking: scans of this package cannot apply)", err)
			}
		}
	}

	lr, err := t.Dispatcher.Run(ctx, p)
	d.LoadID, d.DepartureID = lr.Load.LoadID, lr.Departure.DepartureID
	d.Problems = append(d.Problems, lr.Problems...)
	if err != nil {
		add("%s", err)
		return d
	}

	dest := p.DestStationNodeID
	if dest == "" {
		dest = lr.Load.DestinationNodeID
	}
	node, err := t.Station.Node(ctx, dest)
	if err != nil {
		add("%s", err)
		return d
	}
	if node.NodeType != "DELIVERY_STATION" {
		add("node %s is a %s, not a DELIVERY_STATION: LoadArrived is not inducted into a wave there", node.NodeID, node.NodeType)
		return d
	}

	wantDelivered := len(p.TrackingNumbers)
	outcome := OutcomeDelivered
	if o.FailFirstAttempt {
		outcome = OutcomeFailed
	}
	recorded := t.round(ctx, &d, node, lr.Packed, lr.ArrivedAt, outcome, o)
	if o.FailFirstAttempt && len(recorded) > 0 {
		since := latestAttempt(d.Attempts)
		if !o.SkipVerify {
			for _, tn := range recorded {
				if _, err := t.Tracker.AwaitState(ctx, tn, "AT_STATION"); err != nil {
					add("%s", err)
					break
				}
			}
		}
		recorded = t.round(ctx, &d, node, recorded, since, OutcomeDelivered, o)
	} else if o.FailFirstAttempt {
		recorded = nil
	}

	delivered := recorded
	if len(delivered) == wantDelivered {
		d.DeliveredAt = latestDelivered(d.Attempts)
	}
	if !o.SkipVerify {
		for _, tn := range delivered {
			j, err := t.Tracker.AwaitState(ctx, tn, "DELIVERED")
			if err != nil {
				add("%s", err)
				continue
			}
			d.Problems = append(d.Problems, ScanFindings(j)...)
		}
	}
	t.logf("parcel %v: %d problem(s), delivered=%v", p.TrackingNumbers, len(d.Problems), d.Delivered())
	return d
}

// round is one pass over the station: wave, induction, dispatch, route, start,
// attempts with the given outcome. since is the instant the packages are at the
// station (the load's arrival, or a failed attempt's time); a wave must have an
// induct cutoff after it. It returns the packages whose attempt was recorded.
func (t *Transport) round(ctx context.Context, d *Delivery, node Node, tns []string, since time.Time, outcome string, o Options) []string {
	add := func(format string, a ...any) { d.Problems = append(d.Problems, fmt.Sprintf(format, a...)) }
	if _, _, err := t.Station.EnsureWave(ctx, node, since); err != nil {
		add("%s", err)
		return nil
	}
	ind, err := t.Station.AwaitInduction(ctx, node, since, tns)
	if err != nil {
		add("%s", err)
		return nil
	}
	if len(ind.Missing) > 0 {
		add("packages %v were not inducted into wave %s within %s (inducted elsewhere or not at all)", ind.Missing, ind.WaveID, t.Settings.InductionTimeout)
	}
	tns = ind.Inducted
	d.Waves = append(d.Waves, ind.WaveID)
	d.WaveID = ind.WaveID
	if _, err := t.Station.CloseInduction(ctx, ind.WaveID); err != nil {
		add("%s", err)
		return nil
	}
	// A route is planned while the wave is INDUCTION_CLOSED and can only start once the wave is
	// DISPATCHED, so: close induction, plan, dispatch the wave, start the route.
	dsp, err := t.Station.EnsurePartner(ctx, node.NodeID, 0)
	if err != nil {
		add("%s", err)
		return nil
	}
	route, err := t.Station.PlanRoute(ctx, ind.WaveID, dsp.DSPID, tns)
	if err != nil {
		add("%s", err)
		return nil
	}
	d.Routes = append(d.Routes, route.RouteID)
	d.RouteID = route.RouteID
	if _, err := t.Station.DispatchWave(ctx, ind.WaveID); err != nil {
		add("%s", err)
		return nil
	}
	if _, err := t.Driver.StartRoute(ctx, route.RouteID); err != nil {
		add("%s", err)
		return nil
	}
	if !o.SkipVerify {
		// PackageOutForDelivery is a scan stamped by the service: read it, so the attempt's time is after it.
		for _, tn := range tns {
			if _, err := t.Tracker.AwaitState(ctx, tn, "OUT_FOR_DELIVERY"); err != nil {
				add("%s", err)
				break
			}
		}
	}
	atts, problems := t.Driver.RunRoute(ctx, route.RouteID, tns, outcome)
	d.Attempts = append(d.Attempts, atts...)
	d.Problems = append(d.Problems, problems...)
	var recorded []string
	for _, a := range atts {
		recorded = append(recorded, a.TrackingNumber)
	}
	return recorded
}

func dedupe(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range in {
		if s != "" && !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

func latestAttempt(atts []Attempt) time.Time {
	var at time.Time
	for _, a := range atts {
		if a.At.After(at) {
			at = a.At
		}
	}
	return at
}

func latestDelivered(atts []Attempt) time.Time {
	var at time.Time
	for _, a := range atts {
		if a.Outcome == OutcomeDelivered && a.At.After(at) {
			at = a.At
		}
	}
	return at
}
