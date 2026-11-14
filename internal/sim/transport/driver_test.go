package transport

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"
)

func newDriver(w *world, tl *Timeline) *Driver {
	return NewDriver(w.client(), w.clock, nil, Settings{}, tl)
}

// plannedRoute builds a DISPATCHED wave holding tns and a PLANNED route for them, all through the fake's state,
// and puts every package's journey at AT_STATION (the load arrived).
func plannedRoute(t *testing.T, w *world, dispatched bool, tns ...string) (*fwave, *froute) {
	t.Helper()
	w.addStation(station, "America/Chicago")
	for _, tn := range tns {
		w.addJourney(tn, 0)
		w.scan(tn, "PICKUP_DEPARTURE", site, t0.Add(-2*time.Hour))
		w.scan(tn, "STATION_ARRIVAL", station, t0.Add(-time.Hour))
	}
	v := w.addWave(station, t0.Add(10*time.Minute), 50)
	v.Inducted = append(v.Inducted, tns...)
	v.State = "INDUCTION_CLOSED"
	if dispatched {
		v.State = "DISPATCHED"
	}
	rt := &froute{ID: "route-0001", WaveID: v.ID, DSP: "dsp-1", Station: station, State: "PLANNED"}
	for _, tn := range tns {
		rt.Stops = append(rt.Stops, []string{tn})
	}
	w.lastmile.routes[rt.ID] = rt
	return v, rt
}

func TestStartRouteNeedsADispatchedWave(t *testing.T) {
	w := newWorld(t)
	v, rt := plannedRoute(t, w, false, tnA)
	d := newDriver(w, NewTimeline())
	ctx := context.Background()

	if _, err := d.StartRoute(ctx, rt.ID); !hasSlug(err, "illegal-transition") {
		t.Fatalf("start with the wave %s: %v, want illegal-transition", v.State, err)
	}
	v.State = "DISPATCHED"
	got, err := d.StartRoute(ctx, rt.ID)
	if err != nil || got.State != "OUT_FOR_DELIVERY" {
		t.Fatalf("start = %+v, %v", got, err)
	}
	if _, err := d.StartRoute(ctx, rt.ID); !hasSlug(err, "illegal-transition") {
		t.Fatalf("second start: %v, want illegal-transition", err)
	}
	if w.tracking.journeys[tnA].state != "OUT_FOR_DELIVERY" {
		t.Fatalf("journey = %s, want OUT_FOR_DELIVERY (PackageOutForDelivery)", w.tracking.journeys[tnA].state)
	}
	w.assertEveryPostKeyed()
}

func TestAttemptNeedsAStartedRouteAndRecordsOncePerPackage(t *testing.T) {
	w := newWorld(t)
	_, rt := plannedRoute(t, w, true, tnA, tnB)
	d := newDriver(w, NewTimeline())
	ctx := context.Background()

	if _, err := d.RecordAttempt(ctx, rt.ID, tnA, OutcomeDelivered, ""); !hasSlug(err, "route-not-started") {
		t.Fatalf("attempt before the start: %v, want route-not-started", err)
	}
	if _, err := d.StartRoute(ctx, rt.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := d.RecordAttempt(ctx, rt.ID, "RN000000000000ZZ", OutcomeDelivered, ""); !hasSlug(err, "package-not-on-route") {
		t.Fatalf("attempt for a package that is not on the route: %v, want package-not-on-route", err)
	}
	a, err := d.RecordAttempt(ctx, rt.ID, tnA, OutcomeDelivered, "")
	if err != nil || a.Outcome != OutcomeDelivered || a.RouteID != rt.ID || a.TrackingNumber != tnA || a.At.IsZero() {
		t.Fatalf("attempt = %+v, %v", a, err)
	}
	if _, err := d.RecordAttempt(ctx, rt.ID, tnA, OutcomeDelivered, ""); !hasSlug(err, "already-delivered") {
		t.Fatalf("second attempt: %v, want already-delivered", err)
	}
	j := w.tracking.journeys[tnA]
	if j.state != "DELIVERED" || j.deliveredAt.IsZero() || !j.deliveredAt.Equal(a.At) {
		t.Fatalf("journey = %s deliveredAt %s, want DELIVERED at the attempt's time %s", j.state, j.deliveredAt, a.At)
	}
	if rt.State != "OUT_FOR_DELIVERY" {
		t.Fatalf("route = %s, want OUT_FOR_DELIVERY while tnB has no attempt", rt.State)
	}
	if _, err := d.RecordAttempt(ctx, rt.ID, tnB, OutcomeDelivered, ""); err != nil {
		t.Fatal(err)
	}
	if rt.State != "COMPLETED" {
		t.Fatalf("route = %s, want COMPLETED after the last package", rt.State)
	}
}

func TestFailedAttemptCarriesAReasonAndThePackageIsReinducted(t *testing.T) {
	w := newWorld(t)
	_, rt := plannedRoute(t, w, true, tnA, tnB)
	next := w.addWave(station, t0.Add(3*time.Hour), 50) // the next OPEN wave
	d := newDriver(w, NewTimeline())
	ctx := context.Background()
	if _, err := d.StartRoute(ctx, rt.ID); err != nil {
		t.Fatal(err)
	}

	a, err := d.RecordAttempt(ctx, rt.ID, tnA, OutcomeFailed, "")
	if err != nil {
		t.Fatalf("a FAILED attempt without a caller reason must still carry one: %v", err)
	}
	if a.Outcome != OutcomeFailed || a.FailureReason == "" {
		t.Fatalf("attempt = %+v", a)
	}
	if !reflect.DeepEqual(next.Inducted, []string{tnA}) {
		t.Fatalf("next wave holds %v: RetryNextWave must put the package there", next.Inducted)
	}
	if st := w.tracking.journeys[tnA].state; st != "AT_STATION" {
		t.Fatalf("journey = %s, want AT_STATION after DELIVERY_FAILED", st)
	}
	// a second attempt on the same route is not allowed after a FAILED one
	if _, err := d.RecordAttempt(ctx, rt.ID, tnA, OutcomeDelivered, ""); !hasSlug(err, "attempt-already-recorded") {
		t.Fatalf("attempt after FAILED on the same route: %v, want attempt-already-recorded", err)
	}
}

func TestRunRouteReportsRefusalsAndGoesOn(t *testing.T) {
	w := newWorld(t)
	_, rt := plannedRoute(t, w, true, tnA, tnB)
	d := newDriver(w, NewTimeline())
	ctx := context.Background()
	if _, err := d.StartRoute(ctx, rt.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := d.RecordAttempt(ctx, rt.ID, tnA, OutcomeDelivered, ""); err != nil { // tnA is already delivered
		t.Fatal(err)
	}

	atts, problems := d.RunRoute(ctx, rt.ID, []string{tnA, tnB}, OutcomeDelivered)
	if len(atts) != 1 || atts[0].TrackingNumber != tnB {
		t.Fatalf("attempts = %+v, want only tnB recorded", atts)
	}
	if len(problems) != 1 || !strings.Contains(problems[0], "already-delivered") || !strings.Contains(problems[0], tnA) {
		t.Fatalf("problems = %v", problems)
	}
}

// Attempt times: strictly increasing per package, and never before a scan the service already stored,
// even when the service clock runs ahead of the actor's (the compressed or skewed timeline).
func TestAttemptTimesNeverGoBackwardsEvenWhenTheServiceClockRunsAhead(t *testing.T) {
	w := newWorld(t)
	w.skew = 2 * time.Hour // the services' clocks are two hours ahead of this actor's
	_, rt := plannedRoute(t, w, true, tnA)
	tl := NewTimeline()
	tr := NewTracker(w.client(), w.clock, nil, Settings{}, tl)
	d := newDriver(w, tl)
	ctx := context.Background()

	if _, err := d.StartRoute(ctx, rt.ID); err != nil { // the service stamps OUT_FOR_DELIVERY with ITS clock: now+2h
		t.Fatal(err)
	}
	if _, err := tr.AwaitState(ctx, tnA, "OUT_FOR_DELIVERY"); err != nil { // the driver learns that scan's time
		t.Fatal(err)
	}
	a, err := d.RecordAttempt(ctx, rt.ID, tnA, OutcomeDelivered, "")
	if err != nil {
		t.Fatal(err)
	}
	j := w.tracking.journeys[tnA]
	var start time.Time
	for _, s := range j.scans {
		if s.ScanType == "OUT_FOR_DELIVERY" {
			start = s.At
		}
	}
	if !a.At.After(start) {
		t.Fatalf("attempt at %s is not after the OUT_FOR_DELIVERY scan at %s", a.At, start)
	}
	for _, s := range j.scans {
		if !s.Applied {
			t.Fatalf("scan %+v was stored but not applied", s)
		}
	}
	if j.state != "DELIVERED" {
		t.Fatalf("journey = %s", j.state)
	}
}

// The control: an actor that does not learn the service's times sends an older scan and the
// service stores it WITHOUT applying it. This is the failure the timeline prevents.
func TestWithoutTheTimelineTheServiceWouldStoreTheAttemptNotApplied(t *testing.T) {
	w := newWorld(t)
	w.skew = 2 * time.Hour
	_, rt := plannedRoute(t, w, true, tnA)
	d := newDriver(w, NewTimeline()) // never told about the scans
	ctx := context.Background()
	if _, err := d.StartRoute(ctx, rt.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := d.RecordAttempt(ctx, rt.ID, tnA, OutcomeDelivered, ""); err != nil {
		t.Fatal(err)
	}
	j := w.tracking.journeys[tnA]
	if j.state == "DELIVERED" {
		t.Fatal("the fake must refuse to apply a scan older than the last applied one")
	}
	if f := ScanFindings(Journey{TrackingNumber: tnA, Scans: toScans(j.scans)}); len(f) != 1 {
		t.Fatalf("findings = %v, want the unapplied DELIVERED scan", f)
	}
}

func toScans(in []fscan) []Scan {
	out := make([]Scan, len(in))
	for i, s := range in {
		out[i] = Scan{ScanID: s.ScanID, NodeID: s.NodeID, ScanType: s.ScanType, At: s.At, Applied: s.Applied}
	}
	return out
}
