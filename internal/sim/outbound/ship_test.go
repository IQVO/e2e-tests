package outbound

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestShipTakesAPackageOutOfTheBuilding(t *testing.T) {
	r := newRig(t)
	s := r.f.Ship(context.Background(), r.pkg)
	if !s.OK() {
		t.Fatalf("shipment %+v", s)
	}
	if s.PackageID != "PKG-1" || s.LoadID == "" || s.Door != r.site.Doors[0] || s.VisitID == "" || s.TrailerID == "" || s.Seal == "" || !s.Departed || s.LoadState != LoadShipConfirmed {
		t.Fatalf("shipment %+v is missing facts", s)
	}
	r.w.mu.Lock()
	st := r.w.out.packages["PKG-1"].State
	r.w.mu.Unlock()
	if st != PackageShipped {
		t.Fatalf("package is %s, want SHIPPED", st)
	}
	if err := r.f.AwaitShipmentConfirmed(context.Background(), r.w.out, "SRC-1"); err != nil {
		t.Fatal(err)
	}
}

// Every event hop is slow: the manifest, the appointment, TrailerDocked and TrailerDeparted reach
// outbound-shipping late, and dock-scheduling and yard-management have not consumed their copies. The
// appointment is slower than TrailerDocked on purpose: a trailer that docks at a door the load does not
// have yet is ignored by outbound-shipping, so the floor must wait for the door before it lets the trailer in.
func TestShipSurvivesConsumerLagOnEveryHop(t *testing.T) {
	r := newRig(t)
	r.w.mu.Lock()
	r.w.out.lagAppt, r.w.out.lagDocked, r.w.out.lagDeparted = 4, 1, 2
	r.w.dock.doorLag, r.w.dock.cutoffLag, r.w.yard.apptLag = 1, 1, 2
	r.w.mu.Unlock()
	// a package manifested late: re-manifest into a fresh lagging queue
	late := Package{ID: "PKG-LATE", OrderRef: "ORD-L", SourceOrderID: "SRC-L", SiteID: "AUS3", CPT: "17:00"}
	r.w.mu.Lock()
	r.w.out.lagManifest = 3
	r.w.out.manifest(late)
	r.w.mu.Unlock()
	// The rig's own package shares the load, so the truck carries both.
	for _, s := range r.f.ShipAll(context.Background(), []Package{r.pkg, late}) {
		if !s.OK() {
			t.Fatalf("shipment %+v", s)
		}
	}
}

func TestShipRecordsAnIgnoredManifestAndTouchesNothingElse(t *testing.T) {
	w := newWorld(t)
	site := w.seedSite("AUS3") // no CPT schedule: the manifest is ignored
	t.Cleanup(w.assertEveryPostKeyedOnce)
	w.mu.Lock()
	w.out.manifest(Package{ID: "PKG-X", SiteID: "AUS3", CPT: "17:00"})
	w.mu.Unlock()
	f := w.floor(site)
	f.Wait = Waits{Timeout: 60 * time.Millisecond, Interval: time.Millisecond}
	s := f.Ship(context.Background(), Package{ID: "PKG-X", SiteID: "AUS3", CPT: "17:00"})
	if s.OK() || s.Departed || len(s.Problems) != 1 || !strings.Contains(s.Problems[0], "ignored") || !strings.Contains(s.Problems[0], "17:00") {
		t.Fatalf("shipment %+v, want one problem naming the ignored manifest", s)
	}
	if n := w.writes()[SvcDock] + w.writes()[SvcYard] + w.writes()[SvcOutbound]; n != 0 {
		t.Fatalf("%d writes for a package no load holds, want none", n)
	}
}

func (r *rig) addPackage(id, order, src string) Package {
	p := Package{ID: id, OrderRef: order, SourceOrderID: src, SiteID: "AUS3", CPT: "17:00"}
	r.w.mu.Lock()
	r.w.out.manifest(p)
	r.w.mu.Unlock()
	return p
}

func TestShipAllPutsPackagesOfOneLoadOnOneTruck(t *testing.T) {
	r := newRig(t)
	p2 := r.addPackage("PKG-2", "ORD-2", "SRC-2")
	got := r.f.ShipAll(context.Background(), []Package{r.pkg, p2})
	for _, s := range got {
		if !s.OK() {
			t.Fatalf("shipment %+v", s)
		}
	}
	if got[0].LoadID != got[1].LoadID || got[0].VisitID != got[1].VisitID || got[0].Seal != got[1].Seal || got[0].TrailerID != got[1].TrailerID {
		t.Fatalf("packages of one load must share load, visit, trailer and seal: %+v / %+v", got[0], got[1])
	}
	for _, c := range []struct {
		svc, method, tpl string
		want             int
	}{
		{SvcDock, "POST", "/appointments", 1},
		{SvcYard, "POST", "/visits", 1},
		{SvcYard, "POST", "/visits/{id}/check-outs", 1},
		{SvcOutbound, "POST", "/loads/{id}/closures", 1},
		{SvcOutbound, "POST", "/packages/{id}/stagings", 2},
		{SvcOutbound, "POST", "/packages/{id}/loadings", 2},
	} {
		if n := r.count(c.svc, c.method, c.tpl); n != c.want {
			t.Errorf("%s %s %s: %d requests, want %d", c.svc, c.method, c.tpl, n, c.want)
		}
	}
	for _, ref := range []string{"SRC-1", "SRC-2"} {
		if r.w.out.Seen(ref, "ShipmentConfirmed") != 1 {
			t.Errorf("no ShipmentConfirmed for %s", ref)
		}
	}
}

// A package of another order sits on the load and is not ours to ship: the supervisor cannot close it and
// the truck must stay at the door.
func TestShipDoesNotDepartALoadThatCannotClose(t *testing.T) {
	r := newRig(t)
	r.addPackage("PKG-OTHER", "ORD-O", "SRC-O")
	s := r.f.Ship(context.Background(), r.pkg)
	if s.OK() || s.Departed || len(s.Problems) == 0 || !strings.Contains(strings.Join(s.Problems, "|"), "packages-outstanding") {
		t.Fatalf("shipment %+v, want a packages-outstanding problem and no departure", s)
	}
	if n := r.count(SvcYard, "POST", "/visits/{id}/check-outs"); n != 0 {
		t.Fatalf("%d check-outs, want 0: a load that is not closed must not leave", n)
	}
	if l := r.load(); l.State != LoadLoading {
		t.Fatalf("load is %s, want it left LOADING for a supervisor", l.State)
	}
}

func TestShipKeepsTheTruckWhenAPackageCannotBeStaged(t *testing.T) {
	r := newRig(t)
	r.addPackage("PKG-BAD", "ORD-B", "SRC-B")
	r.w.mu.Lock()
	r.w.out.packages["PKG-BAD"].State = "SHORT_SHIPPED"
	r.w.mu.Unlock()
	got := r.f.ShipAll(context.Background(), []Package{r.pkg, {ID: "PKG-BAD", OrderRef: "ORD-B", SourceOrderID: "SRC-B", SiteID: "AUS3", CPT: "17:00"}})
	for _, s := range got {
		if s.OK() || s.Departed {
			t.Fatalf("shipment %+v: one package could not be staged, nobody may leave", s)
		}
	}
	if !strings.Contains(strings.Join(got[1].Problems, "|"), "package-state") {
		t.Fatalf("problems of the bad package: %v, want package-state", got[1].Problems)
	}
	if !strings.Contains(strings.Join(got[0].Problems, "|"), "not closed and not departed") {
		t.Fatalf("problems of the good package: %v, want the load named as not closed", got[0].Problems)
	}
	if n := r.count(SvcYard, "POST", "/visits/{id}/check-outs"); n != 0 {
		t.Fatalf("%d check-outs, want 0", n)
	}
}

// Someone else (another floor, a person with the console) already booked the door and docked the trailer: the
// floor resumes at staging, it does not book or check in again.
func TestShipResumesALoadThatAlreadyHasADoorAndATrailer(t *testing.T) {
	r := newRig(t)
	ap := r.bookedAppointment(t, r.window())
	if _, err := r.f.GateIn(context.Background(), ap); err != nil {
		t.Fatal(err)
	}
	other := r.w.floor(r.site)
	other.RunID = "T2"
	other.GridMinutes = 5
	s := other.Ship(context.Background(), r.pkg)
	if !s.OK() {
		t.Fatalf("shipment %+v", s)
	}
	if r.count(SvcDock, "POST", "/appointments") != 1 || r.count(SvcYard, "POST", "/visits") != 1 {
		t.Fatalf("the resumed load was booked or checked in again")
	}
	if s.Door != ap.Door {
		t.Fatalf("door %s, want the booked %s", s.Door, ap.Door)
	}
}

func TestShipTwoLoadsConcurrently(t *testing.T) {
	r := newRig(t)
	second := "AUS3-DOCK-DR-A01-01-02-A"
	r.w.mu.Lock()
	r.w.facility.slots[second] = &slot{Code: second, Type: "OutboundDoor", Role: "Dock", DockFlow: "Outbound", Status: "Active", Site: "AUS3"}
	c1100 := Cutoff{CptID: "cpt-1100", LocalTime: "11:00", DaysOfWeek: everyDay, ShipMethod: "ground", EligiblePathIDs: []string{"PICK"}}
	r.w.ppm.set("AUS3", "America/Chicago", []Cutoff{cutoff1700, c1100})
	r.w.mu.Unlock()
	r.f.Site.Doors = append(r.f.Site.Doors, second)
	p2 := Package{ID: "PKG-11", OrderRef: "ORD-11", SourceOrderID: "SRC-11", SiteID: "AUS3", CPT: "11:00"}
	r.w.mu.Lock()
	r.w.out.manifest(p2)
	r.w.mu.Unlock()

	var wg sync.WaitGroup
	res := make([]Shipment, 2)
	for i, p := range []Package{r.pkg, p2} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res[i] = r.f.Ship(context.Background(), p)
		}()
	}
	wg.Wait()
	for _, s := range res {
		if !s.OK() {
			t.Fatalf("shipment %+v", s)
		}
	}
	if res[0].LoadID == res[1].LoadID || res[0].Seal == res[1].Seal || res[0].VisitID == res[1].VisitID || res[0].Door == res[1].Door {
		t.Fatalf("two loads must have their own load, seal, visit and door: %+v / %+v", res[0], res[1])
	}
}

type tap struct {
	mu   sync.Mutex
	seen map[string]int
}

func (t *tap) Seen(ref, typ string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.seen[ref+"|"+typ]
}

func TestAwaitShipmentConfirmed(t *testing.T) {
	f := &Floor{Wait: Waits{Timeout: 60 * time.Millisecond, Interval: time.Millisecond}}
	ctx := context.Background()

	ok := &tap{seen: map[string]int{"O-1|ShipmentConfirmed": 1}}
	if err := f.AwaitShipmentConfirmed(ctx, ok, "O-1"); err != nil {
		t.Fatalf("a confirmed order: %v", err)
	}
	// Nothing on the tap: a bounded wait, not a pass.
	if err := f.AwaitShipmentConfirmed(ctx, &tap{seen: map[string]int{}}, "O-1"); err == nil || !strings.Contains(err.Error(), "still waiting") {
		t.Fatalf("err = %v, want a bounded wait", err)
	}
	// Another order's confirmation proves nothing about this one.
	if err := f.AwaitShipmentConfirmed(ctx, &tap{seen: map[string]int{"O-2|ShipmentConfirmed": 1}}, "O-1"); err == nil {
		t.Fatal("another order's ShipmentConfirmed must not satisfy the wait")
	}
	// PackageMissedLoad ends the wait at once with its own message.
	start := time.Now()
	f.Wait = Waits{Timeout: time.Minute, Interval: time.Millisecond}
	missed := &tap{seen: map[string]int{"O-1|PackageMissedLoad": 1}}
	if err := f.AwaitShipmentConfirmed(ctx, missed, "O-1"); err == nil || !strings.Contains(err.Error(), "PackageMissedLoad") || time.Since(start) > time.Second {
		t.Fatalf("err = %v after %s, want an immediate PackageMissedLoad error", err, time.Since(start))
	}
	// The confirmation arrives while the floor waits.
	late := &tap{seen: map[string]int{}}
	go func() {
		time.Sleep(20 * time.Millisecond)
		late.mu.Lock()
		late.seen["O-3|ShipmentConfirmed"] = 1
		late.mu.Unlock()
	}()
	if err := f.AwaitShipmentConfirmed(ctx, late, "O-3"); err != nil {
		t.Fatalf("a late confirmation: %v", err)
	}
}

// The early departure through the whole fake chain, observed the way the retail simulator will: by the tap.
func TestAnEarlyDepartureIsSeenAsPackageMissedLoadOnTheTap(t *testing.T) {
	r := newRig(t)
	ap := r.bookedAppointment(t, r.window())
	v, err := r.f.GateIn(context.Background(), ap)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.f.SealAndDepart(context.Background(), v, "T1-SEAL-9"); err != nil {
		t.Fatal(err)
	}
	err = r.f.AwaitShipmentConfirmed(context.Background(), r.w.out, "SRC-1")
	if err == nil || !strings.Contains(err.Error(), "PackageMissedLoad") {
		t.Fatalf("err = %v, want the early departure reported as PackageMissedLoad", err)
	}
}
