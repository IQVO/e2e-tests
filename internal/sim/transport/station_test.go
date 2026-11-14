package transport

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"
)

func newStation(w *world) *Station {
	return NewStation(w.client(), w.clock, nil, Settings{}, NewTimeline())
}

// stationWorld is a world with the delivery station and the vendor node site-1 known to node-topology.
func stationWorld(t *testing.T) (*world, *Station, Node) {
	t.Helper()
	w := newWorld(t)
	w.addStation(station, "America/Chicago")
	w.nodes[site] = fnode{ID: site, Type: "FULFILLMENT_CENTER", Timezone: "America/Chicago", Status: "ACTIVE"}
	st := newStation(w)
	n, err := st.Node(context.Background(), station)
	if err != nil {
		t.Fatal(err)
	}
	return w, st, n
}

func TestNodeReadsTheStationsTimeZoneAndType(t *testing.T) {
	_, _, n := stationWorld(t)
	if n.NodeID != station || n.NodeType != "DELIVERY_STATION" || n.Timezone != "America/Chicago" {
		t.Fatalf("node = %+v", n)
	}
}

func TestEnsureWaveCreatesOneMinutesAheadOfTheArrival(t *testing.T) {
	w, st, n := stationWorld(t)
	arrival := t0 // 15:45 CT on 2026-11-14

	wave, created, err := st.EnsureWave(context.Background(), n, arrival)
	if err != nil || !created {
		t.Fatalf("created=%v err=%v", created, err)
	}
	if !wave.InductCutoffAt.Equal(arrival.Add(10*time.Minute)) || !wave.WindowStart.Equal(wave.InductCutoffAt.Add(30*time.Minute)) || !wave.WindowEnd.Equal(wave.WindowStart.Add(4*time.Hour)) {
		t.Fatalf("wave times = %+v", wave)
	}
	if wave.ServiceDate != "2026-11-14" || wave.State != "OPEN" || wave.StopCapacity != 100 || wave.StationNodeID != station {
		t.Fatalf("wave = %+v", wave)
	}
	if n := w.countCalls(SvcLastMile, "POST /waves"); n != 1 {
		t.Fatalf("%d POST /waves, want 1", n)
	}
	w.assertEveryPostKeyed()
}

func TestEnsureWaveServiceDateIsTheStationsLocalDate(t *testing.T) {
	_, st, n := stationWorld(t)
	arrival := time.Date(2026, 11, 15, 5, 55, 0, 0, time.UTC) // 23:55 CT on the 14th: the wave window opens after midnight

	wave, _, err := st.EnsureWave(context.Background(), n, arrival)
	if err != nil {
		t.Fatal(err)
	}
	if wave.ServiceDate != "2026-11-15" {
		t.Fatalf("serviceDate = %s, want the station-local date of the window start (2026-11-15)", wave.ServiceDate)
	}
}

func TestEnsureWaveReusesTheEarliestUsableOpenWave(t *testing.T) {
	w, st, n := stationWorld(t)
	arrival := t0
	w.addWave(station, arrival.Add(-time.Hour), 50)          // induct cutoff already passed
	closed := w.addWave(station, arrival.Add(time.Hour), 50) // induction closed
	closed.State = "INDUCTION_CLOSED"
	w.addWave(station, arrival.Add(90*time.Minute), 0)  // no room
	w.addWave("DS-OTHER", arrival.Add(2*time.Hour), 50) // another station
	want := w.addWave(station, arrival.Add(2*time.Hour), 50)
	w.addWave(station, arrival.Add(3*time.Hour), 50)

	wave, created, err := st.EnsureWave(context.Background(), n, arrival)
	if err != nil || created || wave.WaveID != want.ID {
		t.Fatalf("wave=%+v created=%v err=%v, want the existing %s", wave, created, err, want.ID)
	}
	if n := w.countCalls(SvcLastMile, "POST /waves"); n != 0 {
		t.Fatalf("%d waves created, want none", n)
	}
}

func TestEnsureWaveLooksAtTomorrowsWavesToo(t *testing.T) {
	w, st, n := stationWorld(t)
	arrival := time.Date(2026, 11, 15, 5, 55, 0, 0, time.UTC)                      // 23:55 CT
	want := w.addWave(station, time.Date(2026, 11, 15, 11, 0, 0, 0, time.UTC), 50) // 05:00 CT on the 15th

	wave, created, err := st.EnsureWave(context.Background(), n, arrival)
	if err != nil || created || wave.WaveID != want.ID {
		t.Fatalf("wave=%+v created=%v err=%v, want the existing next-day wave %s", wave, created, err, want.ID)
	}
}

func TestScheduleWaveRefusalIsAFindingNotAPanic(t *testing.T) {
	_, st, _ := stationWorld(t)
	notStation := Node{NodeID: site, NodeType: "FULFILLMENT_CENTER", Timezone: "America/Chicago"}
	_, err := st.ScheduleWave(context.Background(), notStation, t0.Add(10*time.Minute))
	if !hasSlug(err, "unknown-station") {
		t.Fatalf("err = %v, want unknown-station", err)
	}
}

func TestAwaitInductionPollsUntilTheArrivalIsProcessed(t *testing.T) {
	w, st, n := stationWorld(t)
	w.lastmile.lag = 9 * time.Second // the LoadArrived consumer is a few seconds behind
	wave := w.addWave(station, t0.Add(10*time.Minute), 50)
	w.lastmile.onLoadArrived(station, []string{tnA}, w.now())

	ind, err := st.AwaitInduction(context.Background(), n, t0, []string{tnA})
	if err != nil {
		t.Fatal(err)
	}
	if ind.WaveID != wave.ID || !reflect.DeepEqual(ind.Inducted, []string{tnA}) || len(ind.Missing) != 0 {
		t.Fatalf("induction = %+v", ind)
	}
	if got := w.clock.Now().Sub(t0); got < 9*time.Second || got > 20*time.Second {
		t.Fatalf("waited %s of fake time, want about the consumer lag (9s) rounded up to polls", got)
	}
}

func TestAwaitInductionFindsThePackageInTheWaveTheServiceChose(t *testing.T) {
	w, st, n := stationWorld(t)
	first := w.addWave(station, t0.Add(10*time.Minute), 1) // earliest wave, one stop only
	second := w.addWave(station, t0.Add(20*time.Minute), 50)
	w.lastmile.onLoadArrived(station, []string{tnA, tnB}, w.now())

	ind, err := st.AwaitInduction(context.Background(), n, t0, []string{tnA, tnB})
	if err != nil {
		t.Fatal(err)
	}
	// tnA takes the earliest wave's only stop; tnB overflows to the next wave: the packages split.
	if ind.WaveID != first.ID && ind.WaveID != second.ID {
		t.Fatalf("induction = %+v", ind)
	}
	if len(ind.Inducted) != 1 || len(ind.Missing) != 1 {
		t.Fatalf("induction = %+v, want the wave with the most packages and the rest reported missing", ind)
	}
}

// A wave whose induct cutoff is before the arrival never takes the package: LoadArrived finds no
// eligible wave and the package waits AT_STATION. The actor must say so after a bounded wait.
func TestAwaitInductionWithOnlyAWaveInThePastTimesOutInsteadOfHanging(t *testing.T) {
	w, st, n := stationWorld(t)
	w.addWave(station, t0.Add(-time.Hour), 50) // the cutoff passed an hour before the load arrived
	w.lastmile.onLoadArrived(station, []string{tnA}, w.now())

	ind, err := st.AwaitInduction(context.Background(), n, t0, []string{tnA})
	if !IsTimeout(err) {
		t.Fatalf("induction = %+v, err = %v, want a timeout", ind, err)
	}
	if !strings.Contains(err.Error(), "induct cutoff after") {
		t.Fatalf("err %q must say that no wave had a cutoff after the arrival", err)
	}
	if elapsed := w.clock.Now().Sub(t0); elapsed > DefaultSettings().InductionTimeout+DefaultSettings().PollInterval {
		t.Fatalf("waited %s, want at most the induction timeout", elapsed)
	}
	if len(ind.Inducted) != 0 {
		t.Fatalf("inducted = %v", ind.Inducted)
	}
}

func TestWaveCommandsFollowTheStateRules(t *testing.T) {
	w, st, _ := stationWorld(t)
	wave := w.addWave(station, t0.Add(10*time.Minute), 50)
	ctx := context.Background()

	if _, err := st.DispatchWave(ctx, wave.ID); !hasSlug(err, "illegal-transition") {
		t.Fatalf("dispatch of an OPEN wave: %v, want illegal-transition", err)
	}
	got, err := st.CloseInduction(ctx, wave.ID)
	if err != nil || got.State != "INDUCTION_CLOSED" {
		t.Fatalf("close = %+v, %v", got, err)
	}
	if _, err := st.CloseInduction(ctx, wave.ID); !hasSlug(err, "illegal-transition") {
		t.Fatalf("closing induction twice: %v, want illegal-transition", err)
	}
	got, err = st.DispatchWave(ctx, wave.ID)
	if err != nil || got.State != "DISPATCHED" {
		t.Fatalf("dispatch = %+v, %v", got, err)
	}
	if _, err := st.CloseInduction(ctx, "wave-9999"); err == nil || !strings.Contains(err.Error(), "404") {
		t.Fatalf("unknown wave: %v, want 404", err)
	}
}

func TestEnsurePartnerIsIdempotentEvenThoughThereIsNoListRoute(t *testing.T) {
	w, st, _ := stationWorld(t)
	ctx := context.Background()

	p1, err := st.EnsurePartner(ctx, station, 0)
	if err != nil {
		t.Fatal(err)
	}
	// A cold actor (another process, another run within the key's 24 h) gets the same partner back.
	p2, err := newStation(w).EnsurePartner(ctx, station, 0)
	if err != nil {
		t.Fatal(err)
	}
	// And the same actor does not even call again.
	p3, _ := st.EnsurePartner(ctx, station, 0)
	if p1.DSPID == "" || p1.DSPID != p2.DSPID || p1.DSPID != p3.DSPID || p1.VanCapacity != DefaultSettings().VanCapacity || p1.State != "ACTIVE" {
		t.Fatalf("partners = %+v %+v %+v", p1, p2, p3)
	}
	if len(w.lastmile.partners) != 1 {
		t.Fatalf("%d partners exist, want exactly 1", len(w.lastmile.partners))
	}
	if posts := w.countCalls(SvcLastMile, "POST /partners"); posts != 2 {
		t.Fatalf("%d POST /partners, want 2 (the cold actor repeated it; the warm one did not)", posts)
	}
	if got := w.callsTo(SvcLastMile, "GET /partners"); len(got) != 0 {
		t.Fatal("the contract has no partner list route")
	}
	// A bigger van is a different partner.
	big, err := st.EnsurePartner(ctx, station, 500)
	if err != nil || big.DSPID == p1.DSPID || big.VanCapacity != 500 {
		t.Fatalf("big = %+v, %v", big, err)
	}
	w.assertEveryPostKeyed()
}

func TestEnsurePartnerRefusalForAnUnknownStation(t *testing.T) {
	_, st, _ := stationWorld(t)
	_, err := st.EnsurePartner(context.Background(), "DS-NOWHERE", 0)
	if !hasSlug(err, "unknown-station") {
		t.Fatalf("err = %v, want unknown-station", err)
	}
}

func inductedWave(t *testing.T, w *world, tns ...string) *fwave {
	t.Helper()
	v := w.addWave(station, t0.Add(10*time.Minute), 50)
	v.Inducted = append(v.Inducted, tns...)
	return v
}

func TestPlanRouteGroupsOnlyOurPackagesIntoTheWavesStops(t *testing.T) {
	w, st, _ := stationWorld(t)
	ctx := context.Background()
	wave := inductedWave(t, w, tnA, "RN00000000FOREIGN", tnB)
	w.shareStop("addr-1", tnA, "RN00000000FOREIGN") // the foreign package shares tnA's stop (same address)
	if _, err := st.CloseInduction(ctx, wave.ID); err != nil {
		t.Fatal(err)
	}
	dsp, err := st.EnsurePartner(ctx, station, 0)
	if err != nil {
		t.Fatal(err)
	}

	route, err := st.PlanRoute(ctx, wave.ID, dsp.DSPID, []string{tnA, tnB})
	if err != nil {
		t.Fatal(err)
	}
	if route.State != "PLANNED" || route.StopCount != 2 || route.WaveID != wave.ID {
		t.Fatalf("route = %+v", route)
	}
	got := w.lastmile.routes[route.RouteID].Stops
	if want := [][]string{{tnA}, {tnB}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("stops = %v, want %v (the foreign package of the shared stop must not ride our route)", got, want)
	}
}

func TestPlanRouteKeepsPackagesOfOneStopTogether(t *testing.T) {
	w, st, _ := stationWorld(t)
	ctx := context.Background()
	wave := inductedWave(t, w, tnA, tnB)
	w.shareStop("addr-1", tnA, tnB)
	_, _ = st.CloseInduction(ctx, wave.ID)
	dsp, _ := st.EnsurePartner(ctx, station, 0)

	route, err := st.PlanRoute(ctx, wave.ID, dsp.DSPID, []string{tnA, tnB})
	if err != nil || route.StopCount != 1 {
		t.Fatalf("route = %+v, %v, want one stop holding both packages", route, err)
	}
}

func TestPlanRouteRefusalsOfTheContractAreSurfaced(t *testing.T) {
	w, st, _ := stationWorld(t)
	ctx := context.Background()
	wave := inductedWave(t, w, tnA, tnB)
	dsp, _ := st.EnsurePartner(ctx, station, 0)

	// the wave must be INDUCTION_CLOSED
	if _, err := st.PlanRoute(ctx, wave.ID, dsp.DSPID, []string{tnA}); !hasSlug(err, "illegal-transition") {
		t.Fatalf("route on an OPEN wave: %v, want illegal-transition", err)
	}
	_, _ = st.CloseInduction(ctx, wave.ID)

	// stops within the van capacity
	small := &fpartner{ID: "dsp-small", Station: station, Van: 1, State: "ACTIVE"}
	w.lastmile.partners[small.ID] = small
	if _, err := st.PlanRoute(ctx, wave.ID, small.ID, []string{tnA, tnB}); !hasSlug(err, "route-over-capacity") {
		t.Fatalf("2 stops for a van of 1: %v, want route-over-capacity", err)
	}
	// the partner must work at the wave's station
	other := &fpartner{ID: "dsp-other", Station: "DS-OTHER", Van: 10, State: "ACTIVE"}
	w.lastmile.partners[other.ID] = other
	if _, err := st.PlanRoute(ctx, wave.ID, other.ID, []string{tnA}); !hasSlug(err, "partner-station-mismatch") {
		t.Fatalf("partner of another station: %v, want partner-station-mismatch", err)
	}
	// an inactive partner
	small.State = "INACTIVE"
	if _, err := st.PlanRoute(ctx, wave.ID, small.ID, []string{tnA}); !hasSlug(err, "partner-inactive") {
		t.Fatalf("inactive partner: %v, want partner-inactive", err)
	}
	// a package that is not in the wave is caught before the service has to refuse it
	before := w.countCalls(SvcLastMile, "POST /routes")
	if _, err := st.PlanRoute(ctx, wave.ID, dsp.DSPID, []string{tnA, "RN0000000000NOPE"}); err == nil || !strings.Contains(err.Error(), "RN0000000000NOPE") {
		t.Fatalf("package outside the wave: %v", err)
	}
	if after := w.countCalls(SvcLastMile, "POST /routes"); after != before {
		t.Fatal("a package outside the wave must not reach POST /routes")
	}
	// and a package already on a route of the wave is refused by the service
	if _, err := st.PlanRoute(ctx, wave.ID, dsp.DSPID, []string{tnA}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PlanRoute(ctx, wave.ID, dsp.DSPID, []string{tnA}); !hasSlug(err, "package-already-routed") {
		t.Fatalf("second route for tnA: %v, want package-already-routed", err)
	}
}
