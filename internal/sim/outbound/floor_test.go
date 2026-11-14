package outbound

import (
	"context"
	"strings"
	"testing"
	"time"
)

var cutoff1700 = Cutoff{CptID: "cpt-1700", LocalTime: "17:00", DaysOfWeek: everyDay, ShipMethod: "ground", EligiblePathIDs: []string{"PICK"}}

// rig is a site with a CPT schedule, a dock calendar on a 5-minute grid with a 30-minute arrival
// tolerance (what Provision declares), one manifested package and the floor that serves it.
type rig struct {
	w    *world
	site Site
	f    *Floor
	pkg  Package
}

func newRig(t *testing.T) *rig {
	t.Helper()
	w := newWorld(t)
	site := w.seedSite("AUS3")
	w.seedSchedule("AUS3", "America/Chicago", cutoff1700)
	w.mu.Lock()
	w.dock.settings["AUS3"] = map[string]int{"maxWindowMinutes": 240, "slotGranularityMinutes": 5, "arrivalToleranceMinutes": 30, "noShowGraceMinutes": 60}
	w.dock.version["AUS3"] = 1
	w.mu.Unlock()
	pkg := Package{ID: "PKG-1", OrderRef: "ORD-1", SourceOrderID: "SRC-1", SiteID: "AUS3", CPT: "17:00", WeightG: 900}
	w.mu.Lock()
	w.out.manifest(pkg)
	w.mu.Unlock()
	f := w.floor(site)
	f.GridMinutes = 5
	t.Cleanup(w.assertEveryPostKeyedOnce)
	return &rig{w: w, site: site, f: f, pkg: pkg}
}

func (r *rig) load() *oload {
	r.w.mu.Lock()
	defer r.w.mu.Unlock()
	return r.w.out.loads[r.w.out.packages[r.pkg.ID].Load]
}

func (r *rig) count(svc, method, template string) int {
	r.w.mu.Lock()
	defer r.w.mu.Unlock()
	return r.w.counts[svc+" "+method+" "+template]
}

func (r *rig) window() Window { return r.f.NextWindow(2*time.Minute, time.Hour) }

func TestNextWindowStartsOnTheGridAtLeastStartInAhead(t *testing.T) {
	f := &Floor{GridMinutes: 5, Now: func() time.Time { return time.Date(2026, 11, 14, 10, 3, 20, 0, time.UTC) }}
	w := f.NextWindow(2*time.Minute, 90*time.Minute)
	if want := time.Date(2026, 11, 14, 10, 10, 0, 0, time.UTC); !w.Start.Equal(want) || !w.End.Equal(want.Add(90*time.Minute)) {
		t.Fatalf("window %v-%v, want 10:10 for 90 min", w.Start, w.End)
	}
	f.Now = func() time.Time { return time.Date(2026, 11, 14, 10, 5, 0, 0, time.UTC) }
	if w := f.NextWindow(0, time.Hour); !w.Start.Equal(time.Date(2026, 11, 14, 10, 5, 0, 0, time.UTC)) {
		t.Fatalf("an exact boundary must not move: %v", w.Start)
	}
}

// ---------------------------------------------------------------------
// dock planner
// ---------------------------------------------------------------------

func TestBookDoorGivesThePlannedLoadItsDoor(t *testing.T) {
	r := newRig(t)
	ap, err := r.f.BookDoor(context.Background(), "cpt-1700", r.window())
	if err != nil {
		t.Fatal(err)
	}
	if ap.ID == "" || ap.Door != r.site.Doors[0] || ap.State != "BOOKED" {
		t.Fatalf("appointment %+v", ap)
	}
	if l := r.load(); l.Door != ap.Door || l.Appt != ap.ID {
		t.Fatalf("load has door %q from %q, want %q from %q: outbound-shipping did not take the door of the appointment", l.Door, l.Appt, ap.Door, ap.ID)
	}
}

func TestBookDoorRetriesOnlyTheConsumerLagRefusals(t *testing.T) {
	r := newRig(t)
	r.w.mu.Lock()
	r.w.dock.doorLag, r.w.dock.cutoffLag = 2, 2
	r.w.mu.Unlock()
	if _, err := r.f.BookDoor(context.Background(), "cpt-1700", r.window()); err != nil {
		t.Fatalf("after 4 lag refusals the booking must succeed: %v", err)
	}
	if n := r.count(SvcDock, "POST", "/appointments"); n != 5 {
		t.Fatalf("%d booking requests, want 5 (2 unknown-door, 2 missing-outbound-ref, 1 booked)", n)
	}
}

func TestBookDoorGivesUpOnACutoffTheScheduleNeverHad(t *testing.T) {
	r := newRig(t)
	r.f.Wait = Waits{Timeout: 40 * time.Millisecond, Interval: time.Millisecond}
	_, err := r.f.BookDoor(context.Background(), "cpt-0900", r.window())
	if err == nil || !strings.Contains(err.Error(), "missing-outbound-ref") || !strings.Contains(err.Error(), "still waiting") {
		t.Fatalf("err = %v, want a bounded wait that names missing-outbound-ref", err)
	}
}

func TestBookDoorFailsAtOnceOnAnyOtherRefusal(t *testing.T) {
	r := newRig(t)
	// An Inbound-only door cannot take an OUTBOUND appointment: 422 door-flow-mismatch, not lag.
	r.w.mu.Lock()
	r.w.facility.slots[r.site.Doors[0]].DockFlow = "Inbound"
	r.w.mu.Unlock()
	_, err := r.f.BookDoor(context.Background(), "cpt-1700", r.window())
	if err == nil || !strings.Contains(err.Error(), "door-flow-mismatch") {
		t.Fatalf("err = %v, want 422 door-flow-mismatch", err)
	}
	if n := r.count(SvcDock, "POST", "/appointments"); n != 1 {
		t.Fatalf("%d booking requests, want exactly 1: a non-lag refusal must not be retried", n)
	}
	// A window in the past is a finding too.
	r.w.mu.Lock()
	r.w.facility.slots[r.site.Doors[0]].DockFlow = "Outbound"
	r.w.mu.Unlock()
	past := Window{Start: time.Now().Add(-2 * time.Hour).Truncate(5 * time.Minute), End: time.Now().Add(-time.Hour).Truncate(5 * time.Minute)}
	if _, err = r.f.BookDoor(context.Background(), "cpt-1700", past); err == nil || !strings.Contains(err.Error(), "window-in-past") {
		t.Fatalf("err = %v, want 422 window-in-past", err)
	}
}

func TestBookDoorTakesTheNextDoorWhenTheFirstIsDoubleBooked(t *testing.T) {
	r := newRig(t)
	second := "AUS3-DOCK-DR-A01-01-02-A"
	r.w.mu.Lock()
	r.w.facility.slots[second] = &slot{Code: second, Type: "OutboundDoor", Role: "Dock", DockFlow: "Outbound", Status: "Active", Site: "AUS3"}
	r.w.mu.Unlock()
	r.f.Site.Doors = append(r.f.Site.Doors, second)
	w := r.window()
	a1, err := r.f.BookDoor(context.Background(), "cpt-1700", w)
	if err != nil {
		t.Fatal(err)
	}
	a2, err := r.f.BookDoor(context.Background(), "cpt-1700", w)
	if err != nil {
		t.Fatal(err)
	}
	if a1.Door == a2.Door || a2.Door != second {
		t.Fatalf("doors %s then %s, want the second booking on %s", a1.Door, a2.Door, second)
	}
	if _, err = r.f.BookDoor(context.Background(), "cpt-1700", w); err == nil || !strings.Contains(err.Error(), "double-booked") {
		t.Fatalf("err = %v, want every door double-booked", err)
	}
}

// ---------------------------------------------------------------------
// gate and yard clerk
// ---------------------------------------------------------------------

func (r *rig) bookedAppointment(t *testing.T, w Window) Appointment {
	t.Helper()
	ap, err := r.f.BookDoor(context.Background(), "cpt-1700", w)
	if err != nil {
		t.Fatal(err)
	}
	return ap
}

func TestGateInDocksTheTrailerAtTheDoorAndStartsDoorWork(t *testing.T) {
	r := newRig(t)
	ap := r.bookedAppointment(t, r.window())
	v, err := r.f.GateIn(context.Background(), ap)
	if err != nil {
		t.Fatal(err)
	}
	r.w.mu.Lock()
	tr := r.w.yard.trailers[v.TrailerID]
	r.w.mu.Unlock()
	if tr.State != "DOCKED" || tr.Loc == nil || tr.Loc.Code != ap.Door || tr.DoorWork != "IN_PROGRESS" {
		t.Fatalf("trailer %+v, want DOCKED at %s with door work IN_PROGRESS", tr, ap.Door)
	}
	if l := r.load(); l.State != LoadLoading || l.Trailer != v.TrailerID {
		t.Fatalf("load is %s on trailer %q, want LOADING on %s: TrailerDocked did not start the loading", l.State, l.Trailer, v.TrailerID)
	}
	if n := r.count(SvcYard, "POST", "/moves/claims"); n != 0 {
		t.Fatalf("%d hostler claims on the direct path, want 0 (the visit's own TO_DOOR move)", n)
	}
}

func TestGateInRetriesTheYardsConsumerLagOnTheAppointment(t *testing.T) {
	r := newRig(t)
	ap := r.bookedAppointment(t, r.window())
	r.w.mu.Lock()
	r.w.yard.apptLag = 3
	r.w.mu.Unlock()
	if _, err := r.f.GateIn(context.Background(), ap); err != nil {
		t.Fatal(err)
	}
	if n := r.count(SvcYard, "POST", "/visits"); n != 4 {
		t.Fatalf("%d check-ins, want 4 (3 unknown-appointment then admitted)", n)
	}
	// An appointment the dock planner never booked is not lag for ever: it fails the bounded wait.
	r.f.Wait = Waits{Timeout: 40 * time.Millisecond, Interval: time.Millisecond}
	if _, err := r.f.GateIn(context.Background(), Appointment{ID: "appt-nope", Door: ap.Door}); err == nil || !strings.Contains(err.Error(), "unknown-appointment") {
		t.Fatalf("err = %v, want a bounded wait on unknown-appointment", err)
	}
}

// An arrival earlier than the appointment's tolerance is parked at a spot; the clerk then asks for a
// hostler move to the door, claims it and completes it as the hostler.
func TestGateInEarlyArrivalGoesThroughAHostlerMove(t *testing.T) {
	r := newRig(t)
	ap := r.bookedAppointment(t, r.f.NextWindow(3*time.Hour, time.Hour))
	v, err := r.f.GateIn(context.Background(), ap)
	if err != nil {
		t.Fatal(err)
	}
	if n := r.count(SvcYard, "POST", "/moves/claims"); n != 1 {
		t.Fatalf("%d claims, want 1", n)
	}
	r.w.mu.Lock()
	tr := r.w.yard.trailers[v.TrailerID]
	r.w.mu.Unlock()
	if tr.State != "DOCKED" || tr.Loc.Code != ap.Door || tr.DoorWork != "IN_PROGRESS" {
		t.Fatalf("trailer %+v, want DOCKED at the door after the hostler move", tr)
	}
	if v.MoveID == "" {
		t.Fatal("the visit must record the move that docked the trailer")
	}
}

// Pull dispatch hands out the best waiting move of the site, which may be another trailer's: the clerk
// must not complete it.
func TestGateInDoesNotCompleteAForeignHostlerMove(t *testing.T) {
	r := newRig(t)
	ap := r.bookedAppointment(t, r.f.NextWindow(3*time.Hour, time.Hour))
	r.w.mu.Lock()
	r.w.yard.trailers["trl-foreign"] = &ytrailer{ID: "trl-foreign", Site: "AUS3", Unit: "OTHER", State: "PARKED", Loc: &yloc{"SPOT", "AUS3-YARD-PRK-A01-02-01-A"}}
	r.w.yard.moves["move-foreign"] = &ymove{ID: "move-foreign", Site: "AUS3", Trailer: "trl-foreign", Performer: "HOSTLER", Reason: "RELOCATE", State: "REQUESTED", To: yloc{"SPOT", "AUS3-YARD-PRK-A01-02-01-B"}, Band: 1, Seq: 0}
	r.w.mu.Unlock()
	_, err := r.f.GateIn(context.Background(), ap)
	if err == nil || !strings.Contains(err.Error(), "not ours") {
		t.Fatalf("err = %v, want the foreign claim reported", err)
	}
	r.w.mu.Lock()
	state := r.w.yard.moves["move-foreign"].State
	r.w.mu.Unlock()
	if state != "CLAIMED" {
		t.Fatalf("the foreign move is %s, want it left CLAIMED and untouched", state)
	}
}

func TestGateInFailsAtOnceWhenTheDoorIsOccupied(t *testing.T) {
	r := newRig(t)
	ap := r.bookedAppointment(t, r.window())
	r.w.mu.Lock()
	r.w.yard.trailers["trl-squatter"] = &ytrailer{ID: "trl-squatter", Site: "AUS3", Unit: "SQUAT", State: "DOCKED", Loc: &yloc{"DOOR", ap.Door}}
	r.w.mu.Unlock()
	_, err := r.f.GateIn(context.Background(), ap)
	// The yard sends the trailer to a spot (the door is busy), the hostler move then asks for the occupied door.
	if err == nil || !strings.Contains(err.Error(), "location-occupied") {
		t.Fatalf("err = %v, want 409 location-occupied", err)
	}
}

func TestSealAndDepartShipsALoadedClosedTrailer(t *testing.T) {
	r := newRig(t)
	ap := r.bookedAppointment(t, r.window())
	v, err := r.f.GateIn(context.Background(), ap)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := r.f.Stage(ctx, r.pkg); err != nil {
		t.Fatal(err)
	}
	if err := r.f.Load(ctx, r.pkg, r.load().ID, ap.Door); err != nil {
		t.Fatal(err)
	}
	if err := r.f.CloseLoad(ctx, r.load().ID); err != nil {
		t.Fatal(err)
	}
	if err := r.f.SealAndDepart(ctx, v, "T1-SEAL-1"); err != nil {
		t.Fatal(err)
	}
	if l := r.load(); l.State != LoadShipConfirmed {
		t.Fatalf("load is %s after the departure, want SHIP_CONFIRMED", l.State)
	}
	if n := r.w.out.Seen("SRC-1", "ShipmentConfirmed"); n != 1 {
		t.Fatalf("%d ShipmentConfirmed for SRC-1, want 1", n)
	}
}

func TestSealAndDepartRefusalsAreFindingsNotRetries(t *testing.T) {
	r := newRig(t)
	ap := r.bookedAppointment(t, r.window())
	v, err := r.f.GateIn(context.Background(), ap)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	// A LOADED live-load trailer without a seal does not leave: 409 seal-required.
	if err := r.f.SealAndDepart(ctx, v, ""); err == nil || !strings.Contains(err.Error(), "seal-required") {
		t.Fatalf("err = %v, want 409 seal-required", err)
	}
	if n := r.count(SvcYard, "POST", "/visits/{id}/check-outs"); n != 1 {
		t.Fatalf("%d check-outs, want exactly 1 (a refusal is not retried)", n)
	}
	// Door work already completed by someone else: the second completion is an illegal transition.
	if err := r.f.SealAndDepart(ctx, v, "T1-SEAL-2"); err == nil || !strings.Contains(err.Error(), "illegal-transition") {
		t.Fatalf("err = %v, want 409 illegal-transition on the second door-work completion", err)
	}
	if n := r.count(SvcYard, "POST", "/visits/{id}/check-outs"); n != 1 {
		t.Fatalf("%d check-outs after the refused completion, want still 1: nothing may depart", n)
	}
}

// A truck that leaves while the load is still LOADING: outbound-shipping never confirms the shipment; it
// raises PackageMissedLoad for every package not loaded and leaves the load for a supervisor.
func TestDepartureBeforeCloseRaisesPackageMissedLoadNotShipmentConfirmed(t *testing.T) {
	r := newRig(t)
	ap := r.bookedAppointment(t, r.window())
	v, err := r.f.GateIn(context.Background(), ap)
	if err != nil {
		t.Fatal(err)
	}
	// The package is never staged or loaded; the supervisor never closes the load.
	if err := r.f.SealAndDepart(context.Background(), v, "T1-SEAL-3"); err != nil {
		t.Fatal(err)
	}
	if n := r.w.out.Seen("SRC-1", "PackageMissedLoad"); n != 1 {
		t.Fatalf("%d PackageMissedLoad for SRC-1, want 1", n)
	}
	if n := r.w.out.Seen("SRC-1", "ShipmentConfirmed"); n != 0 {
		t.Fatalf("%d ShipmentConfirmed after an early departure, want 0", n)
	}
	if l := r.load(); l.State != LoadLoading {
		t.Fatalf("load is %s, want it left LOADING", l.State)
	}
}

// ---------------------------------------------------------------------
// stager, loader, load supervisor
// ---------------------------------------------------------------------

func TestStageRetriesPackageNotFoundWhileTheManifestIsInTheConsumer(t *testing.T) {
	w := newWorld(t)
	site := w.seedSite("AUS3")
	w.seedSchedule("AUS3", "America/Chicago", cutoff1700)
	t.Cleanup(w.assertEveryPostKeyedOnce)
	w.mu.Lock()
	w.out.lagManifest = 3
	w.out.manifest(Package{ID: "PKG-LAG", SiteID: "AUS3", CPT: "17:00", SourceOrderID: "SRC-LAG"})
	w.mu.Unlock()
	f := w.floor(site)
	loc, err := f.Stage(context.Background(), Package{ID: "PKG-LAG"})
	if err != nil || loc != site.StagingLocations[0] {
		t.Fatalf("Stage = %q, %v after the lag", loc, err)
	}
	w.mu.Lock()
	n, st := w.counts[SvcOutbound+" POST /packages/{id}/stagings"], w.out.packages["PKG-LAG"].State
	w.mu.Unlock()
	if n != 4 || st != PackageStaged {
		t.Fatalf("%d staging requests and state %s, want 4 and STAGED", n, st)
	}
}

func TestStageNamesWhyAnIgnoredManifestNeverArrives(t *testing.T) {
	w := newWorld(t)
	site := w.seedSite("AUS3") // no CPT schedule: outbound-shipping ignores the manifest
	t.Cleanup(w.assertEveryPostKeyedOnce)
	w.mu.Lock()
	w.out.manifest(Package{ID: "PKG-X", SiteID: "AUS3", CPT: "17:00"})
	w.mu.Unlock()
	f := w.floor(site)
	f.Wait = Waits{Timeout: 40 * time.Millisecond, Interval: time.Millisecond}
	_, err := f.Stage(context.Background(), Package{ID: "PKG-X"})
	if err == nil || !strings.Contains(err.Error(), "package-not-found") || !strings.Contains(err.Error(), "still waiting") {
		t.Fatalf("err = %v, want a bounded wait on package-not-found", err)
	}
}

func TestStageFailsAtOnceOnANonStagingLocation(t *testing.T) {
	r := newRig(t)
	r.f.Site.StagingLocations = []string{r.site.Doors[0]} // a Dock slot, not a Staging slot
	_, err := r.f.Stage(context.Background(), r.pkg)
	if err == nil || !strings.Contains(err.Error(), "not-staging-location") {
		t.Fatalf("err = %v, want 422 not-staging-location", err)
	}
	if n := r.count(SvcOutbound, "POST", "/packages/{id}/stagings"); n != 1 {
		t.Fatalf("%d staging requests, want 1: only the exact lag refusal is retried", n)
	}
}

func TestLoadNeedsTheLoadsOwnDoor(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	l := r.load()
	// No appointment yet: the load has no door and refuses every loading (409 wrong-load), at once.
	if err := r.f.Load(ctx, r.pkg, l.ID, r.site.Doors[0]); err == nil || !strings.Contains(err.Error(), "wrong-load") {
		t.Fatalf("err = %v, want 409 wrong-load while the load has no door", err)
	}
	if n := r.count(SvcOutbound, "POST", "/packages/{id}/loadings"); n != 1 {
		t.Fatalf("%d loadings, want 1: wrong-load is not retried", n)
	}
	ap := r.bookedAppointment(t, r.window())
	if err := r.f.Load(ctx, r.pkg, l.ID, "AUS3-DOCK-DR-A01-01-09-A"); err == nil || !strings.Contains(err.Error(), "wrong-load") {
		t.Fatalf("err = %v, want 409 wrong-load through another door", err)
	}
	if err := r.f.Load(ctx, r.pkg, l.ID, ap.Door); err != nil {
		t.Fatal(err)
	}
	if err := r.f.Load(ctx, r.pkg, l.ID, ap.Door); err == nil || !strings.Contains(err.Error(), "package-state") {
		t.Fatalf("err = %v, want 409 package-state when scanned twice", err)
	}
}

func TestCloseLoadOrderingRules(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	id := r.load().ID
	// PLANNED: not LOADING yet.
	if err := r.f.CloseLoad(ctx, id); err == nil || !strings.Contains(err.Error(), "load-state") {
		t.Fatalf("err = %v, want 409 load-state on a PLANNED load", err)
	}
	ap := r.bookedAppointment(t, r.window())
	if _, err := r.f.GateIn(ctx, ap); err != nil {
		t.Fatal(err)
	}
	// LOADING with the package still EXPECTED: outstanding.
	if err := r.f.CloseLoad(ctx, id); err == nil || !strings.Contains(err.Error(), "packages-outstanding") {
		t.Fatalf("err = %v, want 409 packages-outstanding", err)
	}
	if _, err := r.f.Stage(ctx, r.pkg); err != nil {
		t.Fatal(err)
	}
	if err := r.f.CloseLoad(ctx, id); err == nil || !strings.Contains(err.Error(), "packages-outstanding") {
		t.Fatalf("err = %v, want a STAGED package to be outstanding too", err)
	}
	if err := r.f.Load(ctx, r.pkg, id, ap.Door); err != nil {
		t.Fatal(err)
	}
	if err := r.f.CloseLoad(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := r.f.CloseLoad(ctx, id); err == nil || !strings.Contains(err.Error(), "load-state") {
		t.Fatalf("err = %v, want a CLOSED load to refuse a second close", err)
	}
	if err := r.f.CloseLoad(ctx, "load-nope"); err == nil || !strings.Contains(err.Error(), "load-not-found") {
		t.Fatalf("err = %v, want 404 load-not-found", err)
	}
}
