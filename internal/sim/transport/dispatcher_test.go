package transport

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"
)

const (
	site    = "site-1"
	station = "DS-SIM-1"
)

var tnA, tnB = "RN000000000000A1", "RN000000000000B2"

func newDispatcher(w *world) *Dispatcher {
	return NewDispatcher(w.client(), w.clock, nil, Settings{}, NewTimeline())
}

func parcel(tns ...string) Parcel {
	return Parcel{TrackingNumbers: tns, OriginNodeID: site, DestStationNodeID: station}
}

func TestRunOpensPacksClosesDispatchesAndReceivesBeforeTheDepartureIsDue(t *testing.T) {
	w := newWorld(t)
	w.addJourney(tnA, 0)
	w.addJourney(tnB, 0)
	dep := w.addDeparture("PICKUP", site, station, t0.Add(2*time.Hour), "SCHEDULED", 50) // 17:45 CT cutoff: two hours from now
	d := newDispatcher(w)

	res, err := d.Run(context.Background(), parcel(tnA, tnB))
	if err != nil {
		t.Fatal(err)
	}
	if res.Departure.DepartureID != dep.ID || res.Load.State != "ARRIVED" || !reflect.DeepEqual(res.Packed, []string{tnA, tnB}) || len(res.Problems) != 0 {
		t.Fatalf("result = %+v", res)
	}
	// No time guard on the service: the load left and arrived before the scheduled time.
	if !res.DepartedAt.Before(dep.Departs) || !res.ArrivedAt.Before(dep.Departs) {
		t.Fatalf("departed %s arrived %s, want both before the scheduled departure %s (no time guard)", res.DepartedAt, res.ArrivedAt, dep.Departs)
	}
	if !res.ArrivedAt.After(res.DepartedAt) {
		t.Fatalf("arrival %s must be after departure %s", res.ArrivedAt, res.DepartedAt)
	}
	want := []string{
		"POST /loads",
		"POST /loads/load-0001/packages", "POST /loads/load-0001/packages",
		"POST /loads/load-0001/closures", "POST /loads/load-0001/dispatches", "POST /loads/load-0001/receipts",
	}
	if got := w.sequence(SvcLinehaul); !reflect.DeepEqual(got, want) {
		t.Fatalf("calls = %v\nwant    %v", got, want)
	}
	// shipping-and-tracking saw both legs of the journey, in time order, all applied.
	for _, tn := range []string{tnA, tnB} {
		j := w.tracking.journeys[tn]
		if j.state != "AT_STATION" || len(j.scans) != 2 || !j.scans[0].Applied || !j.scans[1].Applied {
			t.Fatalf("journey %s = %s %+v", tn, j.state, j.scans)
		}
	}
	w.assertEveryPostKeyed()
}

func TestRunRaisesTheTimelineToTheServiceStampedTimes(t *testing.T) {
	w := newWorld(t)
	w.skew = time.Hour // the services' clocks run an hour ahead of the actors'
	w.addDeparture("PICKUP", site, station, t0.Add(2*time.Hour), "SCHEDULED", 50)
	tl := NewTimeline()
	d := NewDispatcher(w.client(), w.clock, nil, Settings{}, tl)

	res, err := d.Run(context.Background(), parcel(tnA))
	if err != nil {
		t.Fatal(err)
	}
	if !tl.Last(tnA).Equal(res.ArrivedAt) || !res.ArrivedAt.After(t0.Add(time.Hour)) {
		t.Fatalf("timeline %s, arrival %s: the actor must know the service-stamped time, not its own clock", tl.Last(tnA), res.ArrivedAt)
	}
}

func TestNextPickupDepartureSkipsWhatIsNotASchedulablePickup(t *testing.T) {
	w := newWorld(t)
	// 120 cancelled pickups come first: the walk must follow nextCursor, not stop at one page.
	for i := 0; i < 120; i++ {
		w.addDeparture("PICKUP", site, station, t0.Add(time.Duration(i+1)*time.Minute), "CANCELLED", 50)
	}
	w.addDeparture("PICKUP", site, station, t0.Add(-time.Hour), "SCHEDULED", 50)    // cutoff already passed
	w.addDeparture("LINEHAUL", site, station, t0.Add(3*time.Hour), "SCHEDULED", 50) // wrong kind
	w.addDeparture("PICKUP", "site-2", station, t0.Add(3*time.Hour), "SCHEDULED", 50)
	w.addDeparture("PICKUP", site, "DS-OTHER", t0.Add(3*time.Hour), "SCHEDULED", 50)
	w.addDeparture("PICKUP", site, station, t0.Add(3*time.Hour), "SCHEDULED", 1) // too small for two packages
	want := w.addDeparture("PICKUP", site, station, t0.Add(4*time.Hour), "SCHEDULED", 50)
	w.addDeparture("PICKUP", site, station, t0.Add(5*time.Hour), "SCHEDULED", 50)
	d := newDispatcher(w)

	got, err := d.NextPickupDeparture(context.Background(), site, station, 2)
	if err != nil {
		t.Fatal(err)
	}
	if got.DepartureID != want.ID {
		t.Fatalf("picked %s, want %s", got.DepartureID, want.ID)
	}
	if n := len(w.callsTo(SvcLinehaul, "GET /departures")); n < 2 {
		t.Fatalf("%d list calls: the walk did not follow the cursor", n)
	}
}

func TestNextPickupDepartureWithoutADestinationTakesAnyStation(t *testing.T) {
	w := newWorld(t)
	want := w.addDeparture("PICKUP", site, "DS-OTHER", t0.Add(time.Hour), "SCHEDULED", 50)
	got, err := newDispatcher(w).NextPickupDeparture(context.Background(), site, "", 1)
	if err != nil || got.DepartureID != want.ID {
		t.Fatalf("got %+v, %v; want %s", got, err, want.ID)
	}
}

func TestRunMovesOnWhenTheNextDepartureAlreadyHasALoad(t *testing.T) {
	w := newWorld(t)
	first := w.addDeparture("PICKUP", site, station, t0.Add(time.Hour), "SCHEDULED", 50)
	second := w.addDeparture("PICKUP", site, station, t0.Add(2*time.Hour), "SCHEDULED", 50)
	d := newDispatcher(w)
	// An earlier run (or another dispatcher) opened the first departure's load.
	if _, err := d.OpenLoad(context.Background(), first.ID); err != nil {
		t.Fatal(err)
	}

	res, err := d.Run(context.Background(), parcel(tnA))
	if err != nil {
		t.Fatal(err)
	}
	if res.Departure.DepartureID != second.ID {
		t.Fatalf("used %s, want the next free departure %s", res.Departure.DepartureID, second.ID)
	}
}

func TestRunGivesUpAfterTheBoundedNumberOfDepartures(t *testing.T) {
	w := newWorld(t)
	d := newDispatcher(w)
	d.Settings.MaxDepartureTries = 2
	for i := 1; i <= 3; i++ {
		dep := w.addDeparture("PICKUP", site, station, t0.Add(time.Duration(i)*time.Hour), "SCHEDULED", 50)
		if _, err := d.OpenLoad(context.Background(), dep.ID); err != nil {
			t.Fatal(err)
		}
	}
	_, err := d.Run(context.Background(), parcel(tnA))
	if err == nil || !strings.Contains(err.Error(), "load-already-exists") {
		t.Fatalf("err = %v, want the load-already-exists refusal after 2 tries", err)
	}
	if n := w.countCalls(SvcLinehaul, "POST /loads"); n != 5 { // 3 setup + 2 tries
		t.Fatalf("%d POST /loads, want 5", n)
	}
}

func TestRunReportsAMissingDepartureInsteadOfPanicking(t *testing.T) {
	w := newWorld(t)
	w.addDeparture("PICKUP", "site-2", station, t0.Add(time.Hour), "SCHEDULED", 50)
	_, err := newDispatcher(w).Run(context.Background(), parcel(tnA))
	if err == nil || !strings.Contains(err.Error(), "no SCHEDULED PICKUP departure") || !strings.Contains(err.Error(), site) {
		t.Fatalf("err = %v", err)
	}
}

// The commands have an order the service enforces; the actor must never need a refusal to learn it.
func TestLoadStateRulesOfTheContract(t *testing.T) {
	w := newWorld(t)
	dep := w.addDeparture("PICKUP", site, station, t0.Add(time.Hour), "SCHEDULED", 2)
	d := newDispatcher(w)
	ctx := context.Background()

	ld, err := d.OpenLoad(ctx, dep.ID)
	if err != nil {
		t.Fatal(err)
	}
	if ld.State != "OPEN" || ld.CapacityPackages != 2 {
		t.Fatalf("load = %+v", ld)
	}
	// a departure has at most one load
	if _, err := d.OpenLoad(ctx, dep.ID); !hasSlug(err, "load-already-exists") {
		t.Fatalf("second load on a departure: %v, want load-already-exists", err)
	}
	// dispatch needs CLOSED, receipt needs DEPARTED
	if _, err := d.Dispatch(ctx, ld.LoadID); !hasSlug(err, "illegal-transition") {
		t.Fatalf("dispatch of an OPEN load: %v, want illegal-transition", err)
	}
	if _, err := d.Receive(ctx, ld.LoadID); !hasSlug(err, "illegal-transition") {
		t.Fatalf("receipt of an OPEN load: %v, want illegal-transition", err)
	}
	// a second departure cannot take the same package, a third package is over capacity
	if _, err := d.AddPackage(ctx, ld.LoadID, tnA); err != nil {
		t.Fatal(err)
	}
	if _, err := d.AddPackage(ctx, ld.LoadID, tnA); err != nil {
		t.Fatalf("adding the same package twice changes nothing: %v", err)
	}
	other := w.addDeparture("PICKUP", site, station, t0.Add(2*time.Hour), "SCHEDULED", 5)
	l2, _ := d.OpenLoad(ctx, other.ID)
	if _, err := d.AddPackage(ctx, l2.LoadID, tnA); !hasSlug(err, "package-already-loaded") {
		t.Fatalf("same package on a second load: %v, want package-already-loaded", err)
	}
	if _, err := d.AddPackage(ctx, ld.LoadID, tnB); err != nil {
		t.Fatal(err)
	}
	if _, err := d.AddPackage(ctx, ld.LoadID, "RN000000000000C3"); !hasSlug(err, "load-over-capacity") {
		t.Fatalf("third package on a 2-package load: %v, want load-over-capacity", err)
	}
	// close once, closing again changes nothing; no package after the close
	if _, err := d.CloseLoad(ctx, ld.LoadID); err != nil {
		t.Fatal(err)
	}
	if _, err := d.CloseLoad(ctx, ld.LoadID); err != nil {
		t.Fatalf("closing a CLOSED load returns it unchanged: %v", err)
	}
	if _, err := d.AddPackage(ctx, ld.LoadID, "RN000000000000D4"); !hasSlug(err, "illegal-transition") {
		t.Fatalf("package on a CLOSED load: %v, want illegal-transition", err)
	}
	if _, err := d.Dispatch(ctx, ld.LoadID); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Receive(ctx, ld.LoadID); err != nil {
		t.Fatal(err)
	}
	// the departure left: a new load cannot be opened on it
	if _, err := d.OpenLoad(ctx, dep.ID); err == nil {
		t.Fatal("a load on an ARRIVED departure must be refused")
	}
	if _, err := d.Dispatch(ctx, "load-9999"); err == nil || !strings.Contains(err.Error(), "404") {
		t.Fatalf("unknown load: %v, want 404", err)
	}
}

func TestRunReportsAPackageTheServiceRefusesAndCarriesOn(t *testing.T) {
	w := newWorld(t)
	dep := w.addDeparture("PICKUP", site, station, t0.Add(time.Hour), "SCHEDULED", 50)
	earlier := w.addDeparture("PICKUP", site, station, t0.Add(30*time.Minute), "CLOSED", 50) // already left with tnA
	d := newDispatcher(w)
	w.linehaul.loads["load-x"] = &fload{ID: "load-x", DepartureID: earlier.ID, State: "CLOSED", TNs: []string{tnA}, Capacity: 50}
	w.linehaul.byDep[earlier.ID] = "load-x"

	res, err := d.Run(context.Background(), parcel(tnA, tnB))
	if err != nil {
		t.Fatalf("one refused package must not stop the run: %v", err)
	}
	if res.Departure.DepartureID != dep.ID || !reflect.DeepEqual(res.Packed, []string{tnB}) {
		t.Fatalf("result = %+v", res)
	}
	if len(res.Problems) != 1 || !strings.Contains(res.Problems[0], "package-already-loaded") || !strings.Contains(res.Problems[0], tnA) {
		t.Fatalf("problems = %v", res.Problems)
	}
}

func TestRunFailsWhenNoPackageCouldBeLoaded(t *testing.T) {
	w := newWorld(t)
	w.addDeparture("PICKUP", site, station, t0.Add(time.Hour), "SCHEDULED", 50)
	d := newDispatcher(w)
	_, err := d.Run(context.Background(), Parcel{OriginNodeID: site, DestStationNodeID: station})
	if err == nil || !strings.Contains(err.Error(), "no tracking numbers") {
		t.Fatalf("err = %v", err)
	}
	if n := len(w.sequence(SvcLinehaul)); n != 0 {
		t.Fatalf("%d calls for an empty parcel, want none", n)
	}
}
