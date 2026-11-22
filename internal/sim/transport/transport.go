package transport

import (
	"context"
	"fmt"
	"sync"
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

	// Tap, when set, is where the delivery-point flag is observed as an event: last-mile-delivery's
	// DeliveryPointFlagged on its events topic (TopicLastMile). *sim.EventTap satisfies it and must be
	// reading that topic. Optional: the order's tracking view (Parcel.OrderID) observes the same flag
	// downstream, over REST. See flag.go.
	Tap FlagTap

	mu      sync.Mutex
	tracked map[string]bool // tracking numbers already registered on Tap: EventTap.Track appends, a second one would count twice
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
	return t.Deliver(ctx, Parcel{TrackingNumbers: tns, OriginNodeID: origin, DestStationNodeID: destStation, OrderID: orderID}, o)
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
//  4. attempts: DELIVERED, or with FailFirstAttempt / FailedAttempts FAILED first
//     (reporting FailureDetail) and then, after the packages are re-inducted onto
//     the next wave, a new route per round, the one that delivers them (the
//     redelivery). After FlagAfterStrikes counted failures (NOT_HOME or
//     ACCESS_PROBLEM) at the stop last-mile-delivery flags the delivery point:
//     (unless SkipVerify) the run waits to OBSERVE the flag, as the event on the
//     Tap and as the advisory on the order's tracking view (flag.go), before the
//     delivering round, and checks the advisory ends with the delivery;
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
	if problems := checkParcel(p, o); len(problems) > 0 {
		d.Problems = append(d.Problems, problems...)
		return d
	}
	failures, detail := o.failures(), o.failureDetail()

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

	node, problem := t.stationNode(ctx, p, lr)
	if problem != "" {
		add("%s", problem)
		return d
	}

	wantDelivered := len(p.TrackingNumbers)
	flagExpected := !o.SkipVerify && Counted(detail) && failures >= FlagAfterStrikes
	if flagExpected {
		t.trackFlags(p.TrackingNumbers)
	}

	// Each failed round re-inducts its packages onto a later wave; the last round delivers them.
	tns, since := t.failedRounds(ctx, &d, p, node, lr.Packed, lr.ArrivedAt, o, detail, flagExpected)
	var recorded []string
	if failures == 0 || len(tns) > 0 {
		recorded = t.round(ctx, &d, node, tns, since, OutcomeDelivered, o.DeliveredDetail, o)
	}

	delivered := recorded
	if len(delivered) == wantDelivered {
		d.DeliveredAt = latestDelivered(d.Attempts)
	}
	if !o.SkipVerify {
		t.verifyDelivered(ctx, &d, p, delivered)
	}
	t.logf("parcel %v: %d problem(s), delivered=%v", p.TrackingNumbers, len(d.Problems), d.Delivered())
	return d
}

// stationNode is the parcel's destination station: the parcel's own when it
// names one, the linehaul load's otherwise. It must be a DELIVERY_STATION;
// the returned string is the problem when it is not ("" when it is).
func (t *Transport) stationNode(ctx context.Context, p Parcel, lr LoadResult) (Node, string) {
	dest := p.DestStationNodeID
	if dest == "" {
		dest = lr.Load.DestinationNodeID
	}
	node, err := t.Station.Node(ctx, dest)
	if err != nil {
		return Node{}, err.Error()
	}
	if node.NodeType != "DELIVERY_STATION" {
		return node, fmt.Sprintf("node %s is a %s, not a DELIVERY_STATION: LoadArrived is not inducted into a wave there", node.NodeID, node.NodeType)
	}
	return node, ""
}

// checkParcel rejects a parcel or an option combination a service would refuse
// anyway; a refused option must cost no write.
func checkParcel(p Parcel, o Options) []string {
	var problems []string
	if len(p.TrackingNumbers) == 0 || p.OriginNodeID == "" {
		return append(problems, fmt.Sprintf("parcel: needs tracking numbers and an origin node (got %d tracking numbers, origin %q)", len(p.TrackingNumbers), p.OriginNodeID))
	}
	if failures := o.failures(); failures < 0 || failures > MaxFailedAttempts {
		return append(problems, fmt.Sprintf("options: %d failed attempts asked, but the package can fail %d times at most (the third failure initiates its return instead of a redelivery)", failures, MaxFailedAttempts))
	}
	detail := o.failureDetail()
	if !DetailFits(OutcomeFailed, detail) || !DetailFits(OutcomeDelivered, o.DeliveredDetail) {
		problems = append(problems, fmt.Sprintf("options: outcomeDetail %q on a failed and %q on a delivered attempt: not in the contract's closed list", detail, o.DeliveredDetail))
	}
	return problems
}

// failedRounds plays the parcel's failed attempts, one round each: every failed
// round re-inducts its packages onto a later wave, and the round that reaches
// FlagAfterStrikes waits for the delivery point to be flagged (and observed)
// before the next one. It returns the packages still to deliver and the instant
// they came back to the station.
func (t *Transport) failedRounds(ctx context.Context, d *Delivery, p Parcel, node Node, tns []string, since time.Time, o Options, detail string, flagExpected bool) ([]string, time.Time) {
	add := func(format string, a ...any) { d.Problems = append(d.Problems, fmt.Sprintf(format, a...)) }
	failedRounds := 0
	for i := 0; i < o.failures(); i++ {
		tns = t.round(ctx, d, node, tns, since, OutcomeFailed, detail, o)
		if len(tns) == 0 {
			break
		}
		since = latestAttempt(d.Attempts)
		failedRounds++
		if !o.SkipVerify {
			for _, tn := range tns {
				if _, err := t.Tracker.AwaitState(ctx, tn, "AT_STATION"); err != nil {
					add("%s", err)
					break
				}
			}
		}
		if flagExpected && failedRounds == FlagAfterStrikes {
			t.awaitFlag(ctx, d, p.OrderID, tns, FlagReason(detail))
		}
	}
	return tns, since
}

// verifyDelivered reads every delivered package's journey back (DELIVERED, no
// scan stored without being applied) and, when a delivery point was flagged,
// waits until the advisory on the order's tracking view ends with the delivery.
func (t *Transport) verifyDelivered(ctx context.Context, d *Delivery, p Parcel, delivered []string) {
	add := func(format string, a ...any) { d.Problems = append(d.Problems, fmt.Sprintf(format, a...)) }
	for _, tn := range delivered {
		j, err := t.Tracker.AwaitState(ctx, tn, "DELIVERED")
		if err != nil {
			add("%s", err)
			continue
		}
		d.Problems = append(d.Problems, ScanFindings(j)...)
	}
	if len(d.Flagged) > 0 && p.OrderID != "" {
		t.awaitAdvisoryEnded(ctx, d, p.OrderID, d.Flagged, delivered)
	}
}

// round is one pass over the station: wave, induction, dispatch, route, start,
// attempts with the given outcome and outcomeDetail. since is the instant the packages are at the
// station (the load's arrival, or a failed attempt's time); a wave must have an
// induct cutoff after it. It returns the packages whose attempt was recorded.
func (t *Transport) round(ctx context.Context, d *Delivery, node Node, tns []string, since time.Time, outcome, detail string, o Options) []string {
	add := func(format string, a ...any) { d.Problems = append(d.Problems, fmt.Sprintf(format, a...)) }
	waveID, inducted := t.inductAndWait(ctx, d, node, tns, since)
	if inducted == nil {
		return nil
	}
	tns = inducted
	// A route is planned while the wave is INDUCTION_CLOSED and can only start once the wave is
	// DISPATCHED, so: close induction, plan, dispatch the wave, start the route.
	dsp, err := t.Station.EnsurePartner(ctx, node.NodeID, 0)
	if err != nil {
		add("%s", err)
		return nil
	}
	route, err := t.Station.PlanRoute(ctx, waveID, dsp.DSPID, tns)
	if err != nil {
		add("%s", err)
		return nil
	}
	d.Routes = append(d.Routes, route.RouteID)
	d.RouteID = route.RouteID
	if _, err := t.Station.DispatchWave(ctx, waveID); err != nil {
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
	atts, problems := t.Driver.RunRouteDetail(ctx, route.RouteID, tns, outcome, detail)
	d.Attempts = append(d.Attempts, atts...)
	d.Problems = append(d.Problems, problems...)
	var recorded []string
	for _, a := range atts {
		recorded = append(recorded, a.TrackingNumber)
	}
	return recorded
}

// inductAndWait ensures a wave whose cutoff is after since, waits for the
// packages' induction into it, records the wave on the delivery and closes
// induction. It returns the wave id and the packages inducted; nil packages
// mean the step failed and the round cannot go on.
func (t *Transport) inductAndWait(ctx context.Context, d *Delivery, node Node, tns []string, since time.Time) (string, []string) {
	add := func(format string, a ...any) { d.Problems = append(d.Problems, fmt.Sprintf(format, a...)) }
	if _, _, err := t.Station.EnsureWave(ctx, node, since); err != nil {
		add("%s", err)
		return "", nil
	}
	ind, err := t.Station.AwaitInduction(ctx, node, since, tns)
	if err != nil {
		add("%s", err)
		return "", nil
	}
	if len(ind.Missing) > 0 {
		add("packages %v were not inducted into wave %s within %s (inducted elsewhere or not at all)", ind.Missing, ind.WaveID, t.Settings.InductionTimeout)
	}
	d.Waves = append(d.Waves, ind.WaveID)
	d.WaveID = ind.WaveID
	if _, err := t.Station.CloseInduction(ctx, ind.WaveID); err != nil {
		add("%s", err)
		return "", nil
	}
	return ind.WaveID, ind.Inducted
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
