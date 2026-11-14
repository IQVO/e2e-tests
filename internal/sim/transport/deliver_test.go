package transport

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"
)

// deliverWorld is the whole network of the retail day: a vendor node with a PICKUP departure two hours
// ahead (17:45 CT), a delivery station, and two labelled packages whose journeys exist.
func deliverWorld(t *testing.T) (*world, *Transport, Parcel, *fdeparture) {
	t.Helper()
	w := newWorld(t)
	w.addStation(station, "America/Chicago")
	w.nodes[site] = fnode{ID: site, Type: "FULFILLMENT_CENTER", Timezone: "America/Chicago", Status: "ACTIVE"}
	w.addJourney(tnA, 0)
	w.addJourney(tnB, 0)
	dep := w.addDeparture("PICKUP", site, station, t0.Add(2*time.Hour), "SCHEDULED", 50)
	return w, New(w.client(), w.clock, nil, Settings{}), parcel(tnA, tnB), dep
}

func scanTypes(j *fjourney) []string {
	var out []string
	for _, s := range j.scans {
		out = append(out, s.ScanType)
	}
	return out
}

// assertJourneyClean checks every scan of the journey was applied and that times strictly increase.
func assertJourneyClean(t *testing.T, j *fjourney, want ...string) {
	t.Helper()
	if got := scanTypes(j); !reflect.DeepEqual(got, want) {
		t.Fatalf("journey %s scans = %v, want %v", j.tn, got, want)
	}
	for i, s := range j.scans {
		if !s.Applied {
			t.Errorf("journey %s: scan %s at %s stored but not applied", j.tn, s.ScanType, s.At)
		}
		if i > 0 && !s.At.After(j.scans[i-1].At) {
			t.Errorf("journey %s: %s at %s is not after %s at %s", j.tn, s.ScanType, s.At, j.scans[i-1].ScanType, j.scans[i-1].At)
		}
	}
}

func TestDeliverFromAShippedPackageToPackageDelivered(t *testing.T) {
	w, tp, p, dep := deliverWorld(t)

	d := tp.Deliver(context.Background(), p, Options{})
	if len(d.Problems) != 0 {
		t.Fatalf("problems: %v", d.Problems)
	}
	if !d.OK() || d.LoadID != "load-0001" || d.DepartureID != dep.ID || d.WaveID != "wave-0001" || d.RouteID != "route-0001" {
		t.Fatalf("delivery = %+v", d)
	}
	if !reflect.DeepEqual(d.Waves, []string{"wave-0001"}) || !reflect.DeepEqual(d.Routes, []string{"route-0001"}) {
		t.Fatalf("waves %v routes %v", d.Waves, d.Routes)
	}
	if len(d.Attempts) != 2 || d.Attempts[0].Outcome != OutcomeDelivered || d.Attempts[1].Outcome != OutcomeDelivered {
		t.Fatalf("attempts = %+v", d.Attempts)
	}
	// the whole leg ran before the scheduled departure: the contract has no time guard
	ld := w.linehaul.loads[d.LoadID]
	if !ld.DepartedAt.Before(dep.Departs) || ld.State != "ARRIVED" {
		t.Fatalf("load %s departed %s (scheduled %s), state %s", ld.ID, ld.DepartedAt, dep.Departs, ld.State)
	}
	if !d.DeliveredAt.After(ld.ArrivedAt) {
		t.Fatalf("delivered %s, arrived %s", d.DeliveredAt, ld.ArrivedAt)
	}
	for _, tn := range []string{tnA, tnB} {
		j := w.tracking.journeys[tn]
		if j.state != "DELIVERED" {
			t.Fatalf("journey %s = %s", tn, j.state)
		}
		assertJourneyClean(t, j, "PICKUP_DEPARTURE", "STATION_ARRIVAL", "OUT_FOR_DELIVERY", "DELIVERED")
	}
	if got, want := w.sequence(SvcLinehaul), []string{
		"POST /loads", "POST /loads/load-0001/packages", "POST /loads/load-0001/packages",
		"POST /loads/load-0001/closures", "POST /loads/load-0001/dispatches", "POST /loads/load-0001/receipts",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("linehaul calls = %v", got)
	}
	if got, want := w.sequence(SvcLastMile), []string{
		"POST /waves", "POST /waves/wave-0001/induction-closures",
		"POST /partners", "POST /routes", "POST /waves/wave-0001/dispatches", "POST /routes/route-0001/starts",
		"POST /routes/route-0001/attempts", "POST /routes/route-0001/attempts",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("last-mile calls = %v", got)
	}
	w.assertEveryPostKeyed()
	if elapsed := w.clock.Now().Sub(t0); elapsed > 10*time.Minute {
		t.Fatalf("the run took %s of fake time", elapsed)
	}
}

func TestDeliverCopesWithAnInductionLag(t *testing.T) {
	w, tp, p, _ := deliverWorld(t)
	w.lastmile.lag = 25 * time.Second // the LoadArrived consumer is behind

	d := tp.Deliver(context.Background(), p, Options{})
	if !d.OK() {
		t.Fatalf("delivery = %+v", d)
	}
}

func TestDeliverRedeliveryAfterAFailedFirstAttempt(t *testing.T) {
	w, tp, p, _ := deliverWorld(t)

	d := tp.Deliver(context.Background(), p, Options{FailFirstAttempt: true})
	if len(d.Problems) != 0 || !d.OK() {
		t.Fatalf("delivery = %+v", d)
	}
	var got []string
	for _, a := range d.Attempts {
		got = append(got, a.Outcome+" "+a.RouteID)
	}
	if want := []string{"FAILED route-0001", "FAILED route-0001", "DELIVERED route-0002", "DELIVERED route-0002"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("attempts = %v, want %v", got, want)
	}
	if d.Attempts[0].FailureReason == "" {
		t.Fatal("a FAILED attempt must carry a failure reason")
	}
	if len(d.Waves) != 2 || d.Waves[0] == d.Waves[1] || len(d.Routes) != 2 || d.WaveID != d.Waves[1] || d.RouteID != d.Routes[1] {
		t.Fatalf("waves %v routes %v wave %s route %s: the redelivery needs a second wave and route", d.Waves, d.Routes, d.WaveID, d.RouteID)
	}
	if !d.DeliveredAt.Equal(d.Attempts[3].At) && !d.DeliveredAt.Equal(d.Attempts[2].At) {
		t.Fatalf("DeliveredAt %s is not the time of a DELIVERED attempt", d.DeliveredAt)
	}
	for _, tn := range []string{tnA, tnB} {
		j := w.tracking.journeys[tn]
		if j.state != "DELIVERED" || w.lastmile.failures[tn] != 1 {
			t.Fatalf("journey %s = %s, failures %d", tn, j.state, w.lastmile.failures[tn])
		}
		assertJourneyClean(t, j, "PICKUP_DEPARTURE", "STATION_ARRIVAL", "OUT_FOR_DELIVERY", "DELIVERY_FAILED", "OUT_FOR_DELIVERY", "DELIVERED")
	}
	// the second wave's induct cutoff is after the failed attempt, not after the first arrival
	second := w.lastmile.wave(d.Waves[1])
	if !second.Cutoff.After(d.Attempts[0].At) {
		t.Fatalf("second wave cutoff %s is not after the failed attempt %s", second.Cutoff, d.Attempts[0].At)
	}
	w.assertEveryPostKeyed()
}

// The induct cutoff is compared with the ARRIVAL time, not with when the consumer ran: a wave created ten
// minutes after the arrival still takes the package when the induction runs eleven minutes late. The redelivery
// round then needs a wave ahead of the FAILED attempt (eleven minutes after the arrival), not ahead of the arrival.
func TestDeliverRedeliveryAfterASlowInductionUsesAWaveAheadOfTheFailedAttempt(t *testing.T) {
	w, tp, p, _ := deliverWorld(t)
	w.lastmile.lag = 11 * time.Minute
	tp.Station.Settings.InductionTimeout = 30 * time.Minute
	tp.Station.Settings.PollInterval = 20 * time.Second // keep the number of polls small

	d := tp.Deliver(context.Background(), p, Options{FailFirstAttempt: true})
	if !d.OK() {
		t.Fatalf("delivery = %+v", d)
	}
	first, second := w.lastmile.wave(d.Waves[0]), w.lastmile.wave(d.Waves[1])
	if !d.Attempts[0].At.After(first.Cutoff) {
		t.Fatalf("setup: the failed attempt %s must come after the first wave's cutoff %s", d.Attempts[0].At, first.Cutoff)
	}
	if !second.Cutoff.After(d.Attempts[0].At) {
		t.Fatalf("second wave cutoff %s is not after the failed attempt %s", second.Cutoff, d.Attempts[0].At)
	}
}

func TestDeliverReusesAnOpenWaveAndIgnoresAStaleOne(t *testing.T) {
	w, tp, p, _ := deliverWorld(t)
	w.addWave(station, t0.Add(-10*time.Hour), 50) // this morning's induct cutoff, still OPEN
	usable := w.addWave(station, t0.Add(time.Hour), 50)

	d := tp.Deliver(context.Background(), p, Options{})
	if !d.OK() || d.WaveID != usable.ID {
		t.Fatalf("delivery = %+v, want it to use the existing wave %s", d, usable.ID)
	}
	if n := w.countCalls(SvcLastMile, "POST /waves"); n != 0 {
		t.Fatalf("%d waves created, want none", n)
	}
}

func TestDeliverCreatesAWaveWhenOnlyAStaleOneIsOpen(t *testing.T) {
	w, tp, p, _ := deliverWorld(t)
	stale := w.addWave(station, t0.Add(-10*time.Hour), 50)

	d := tp.Deliver(context.Background(), p, Options{})
	if !d.OK() || d.WaveID == stale.ID {
		t.Fatalf("delivery = %+v", d)
	}
	if n := w.countCalls(SvcLastMile, "POST /waves"); n != 1 {
		t.Fatalf("%d waves created, want 1", n)
	}
	if len(stale.Inducted) != 0 {
		t.Fatal("nothing may be inducted into a wave whose cutoff passed")
	}
}

// A wave whose induct cutoff is before the arrival never takes the package. Here the services' clocks run
// three hours ahead of the actor's and the load answers omit arrivedAt (an optional field), so the wave the
// actor creates "minutes ahead" of ITS clock is in the past for the service. Induction never happens: the
// actor must report it after a bounded wait and stop, not hang and not carry on blind.
func TestDeliverWaveInThePastReportsInductionAfterABoundedWait(t *testing.T) {
	w, tp, p, _ := deliverWorld(t)
	w.skew = 3 * time.Hour
	w.linehaul.omitTimes = true

	d := tp.Deliver(context.Background(), p, Options{})
	if d.Delivered() || len(d.Attempts) != 0 {
		t.Fatalf("delivery = %+v, nothing can be delivered without an induction", d)
	}
	if len(d.Problems) != 1 || !strings.Contains(d.Problems[0], "induction") || !strings.Contains(d.Problems[0], "timed out") {
		t.Fatalf("problems = %v, want one induction timeout finding", d.Problems)
	}
	if d.LoadID == "" {
		t.Fatal("the load part of the run happened and must be reported")
	}
	if elapsed := w.clock.Now().Sub(t0); elapsed > DefaultSettings().InductionTimeout+time.Minute {
		t.Fatalf("waited %s of fake time: the wait is not bounded by the induction timeout", elapsed)
	}
	if n := w.countCalls(SvcLastMile, "POST /waves/wave-0001/induction-closures"); n != 0 {
		t.Fatal("induction must not be closed on a wave that took nothing")
	}
}

func TestDeliverOnAServiceClockAheadOfTheActorsStillSendsOrderedTimes(t *testing.T) {
	w, tp, p, _ := deliverWorld(t)
	w.skew = 2 * time.Hour

	d := tp.Deliver(context.Background(), p, Options{FailFirstAttempt: true})
	if !d.OK() {
		t.Fatalf("delivery = %+v", d)
	}
	for _, tn := range []string{tnA, tnB} {
		assertJourneyClean(t, w.tracking.journeys[tn], "PICKUP_DEPARTURE", "STATION_ARRIVAL", "OUT_FOR_DELIVERY", "DELIVERY_FAILED", "OUT_FOR_DELIVERY", "DELIVERED")
	}
}

func TestDeliverRefusedStepsAreFindingsNotPanics(t *testing.T) {
	t.Run("no departure", func(t *testing.T) {
		w, tp, p, _ := deliverWorld(t)
		p.OriginNodeID = "site-9"
		d := tp.Deliver(context.Background(), p, Options{})
		if len(d.Problems) != 1 || !strings.Contains(d.Problems[0], "no SCHEDULED PICKUP departure") || d.Delivered() {
			t.Fatalf("delivery = %+v", d)
		}
		if n := len(w.sequence(SvcLastMile)); n != 0 {
			t.Fatalf("%d last-mile calls after the linehaul step failed", n)
		}
	})
	t.Run("destination is not a delivery station", func(t *testing.T) {
		w, tp, p, _ := deliverWorld(t)
		w.nodes["SC-1"] = fnode{ID: "SC-1", Type: "SORTATION_CENTER", Timezone: "America/Chicago", Status: "ACTIVE"}
		w.addDeparture("PICKUP", site, "SC-1", t0.Add(time.Hour), "SCHEDULED", 50)
		p.DestStationNodeID = "SC-1"
		d := tp.Deliver(context.Background(), p, Options{})
		if len(d.Problems) != 1 || !strings.Contains(d.Problems[0], "DELIVERY_STATION") || d.Delivered() {
			t.Fatalf("delivery = %+v", d)
		}
	})
	t.Run("van too small for the stops", func(t *testing.T) {
		_, tp, p, _ := deliverWorld(t)
		tp.Station.Settings.VanCapacity = 1
		d := tp.Deliver(context.Background(), p, Options{})
		if len(d.Problems) != 1 || !strings.Contains(d.Problems[0], "route-over-capacity") || d.Delivered() || len(d.Attempts) != 0 {
			t.Fatalf("delivery = %+v", d)
		}
		if d.LoadID == "" || d.WaveID == "" {
			t.Fatalf("what was done must be reported: %+v", d)
		}
	})
	t.Run("a package the load refuses", func(t *testing.T) {
		w, tp, p, _ := deliverWorld(t)
		earlier := w.addDeparture("PICKUP", site, station, t0.Add(30*time.Minute), "CLOSED", 50)
		w.linehaul.loads["load-x"] = &fload{ID: "load-x", DepartureID: earlier.ID, State: "CLOSED", TNs: []string{tnA}, Capacity: 50}
		w.linehaul.byDep[earlier.ID] = "load-x"
		d := tp.Deliver(context.Background(), p, Options{})
		if d.Delivered() {
			t.Fatalf("tnA never reached the station, the parcel cannot be delivered: %+v", d)
		}
		if len(d.Problems) != 1 || !strings.Contains(d.Problems[0], "package-already-loaded") {
			t.Fatalf("problems = %v", d.Problems)
		}
		if len(d.Attempts) != 1 || d.Attempts[0].TrackingNumber != tnB {
			t.Fatalf("attempts = %+v, want tnB delivered on its own", d.Attempts)
		}
	})
	t.Run("nothing to deliver", func(t *testing.T) {
		w, tp, _, _ := deliverWorld(t)
		d := tp.Deliver(context.Background(), Parcel{OriginNodeID: site, DestStationNodeID: station}, Options{})
		if len(d.Problems) != 1 || d.Delivered() {
			t.Fatalf("delivery = %+v", d)
		}
		if len(w.calls) != 0 {
			t.Fatalf("%d calls for an empty parcel", len(w.calls))
		}
	})
}

func TestDeliverTrackingGapsAreFindingsTheRunCarriesPast(t *testing.T) {
	w, tp, p, _ := deliverWorld(t)
	delete(w.tracking.journeys, tnB) // the label event of tnB never arrived: its journey does not exist

	d := tp.Deliver(context.Background(), p, Options{})
	if !d.Delivered() {
		t.Fatalf("the last-mile contract was kept, the parcel is delivered: %+v", d)
	}
	if len(d.Problems) < 2 {
		t.Fatalf("problems = %v, want the missing journey reported before dispatch and again at the end", d.Problems)
	}
	for _, f := range d.Problems {
		if !strings.Contains(f, tnB) {
			t.Fatalf("finding %q does not name the package that has no journey", f)
		}
	}
	if w.tracking.journeys[tnA].state != "DELIVERED" {
		t.Fatalf("tnA = %s", w.tracking.journeys[tnA].state)
	}
}

func TestDeliverSkipVerifyNeverReadsShippingAndTracking(t *testing.T) {
	w, tp, p, _ := deliverWorld(t)
	delete(w.tracking.journeys, tnB)

	d := tp.Deliver(context.Background(), p, Options{SkipVerify: true})
	if !d.OK() {
		t.Fatalf("delivery = %+v", d)
	}
	if n := len(w.callsTo(SvcTracking, "GET")); n != 0 {
		t.Fatalf("%d tracking reads, want none", n)
	}
}

func TestDeliverOrderReadsTheTrackingNumbersThenDelivers(t *testing.T) {
	w, tp, _, _ := deliverWorld(t)
	w.addOrder("ord-1", 2, tnA, tnB)

	d := tp.DeliverOrder(context.Background(), "ord-1", 2, site, station, Options{})
	if !d.OK() || len(d.Attempts) != 2 {
		t.Fatalf("delivery = %+v", d)
	}
	// an order whose numbers never appear is a finding, bounded
	w2, tp2, _, _ := deliverWorld(t)
	w2.addOrder("ord-2", 1<<30, tnA)
	d = tp2.DeliverOrder(context.Background(), "ord-2", 1, site, station, Options{})
	if len(d.Problems) != 1 || !strings.Contains(d.Problems[0], "tracking numbers of order ord-2") || d.Delivered() {
		t.Fatalf("delivery = %+v", d)
	}
	if elapsed := w2.clock.Now().Sub(t0); elapsed > DefaultSettings().TrackingTimeout+time.Minute {
		t.Fatalf("waited %s", elapsed)
	}
}

func TestDeliverStopsWhenTheContextIsCancelled(t *testing.T) {
	_, tp, p, _ := deliverWorld(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	d := tp.Deliver(ctx, p, Options{})
	if d.Delivered() || len(d.Problems) == 0 {
		t.Fatalf("delivery = %+v", d)
	}
}
