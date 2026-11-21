package outbound

import (
	"context"
	"strings"
	"testing"
	"time"
)

// emptyWorld is five services that know nothing about the site.
func emptyWorld(t *testing.T, code string) (*world, Site) {
	t.Helper()
	w := newWorld(t)
	t.Cleanup(w.assertEveryPostKeyedOnce)
	return w, Site{
		Code: code, Timezone: "America/Chicago",
		Doors:            []string{code + "-DOCK-DR-A01-01-01-A", code + "-DOCK-DR-A01-01-02-A"},
		StagingLocations: []string{code + "-STAG-LN-A01-01-01-A"},
	}
}

func check(t *testing.T, r Report, area, name string) Check {
	t.Helper()
	for _, c := range r.Checks {
		if c.Area == area && strings.HasPrefix(c.Name, name) {
			return c
		}
	}
	t.Fatalf("no check %s/%s in\n%s", area, name, r)
	return Check{}
}

func writesOf(w *world, svc, method, template string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.counts[svc+" "+method+" "+template]
}

// An empty world: Provision reports every gap, creates what its owners allow and leaves the schedule alone.
func TestProvisionCreatesWhatIsMissingExceptTheScheduleUnlessAllowed(t *testing.T) {
	w, site := emptyWorld(t, "SIM9")
	opt := ProvisionOptions{Cutoffs: []Cutoff{cutoff1700}}
	rep := Provision(context.Background(), w.client, site, opt)

	if rep.Ready() {
		t.Fatalf("a site without a CPT schedule is not ready:\n%s", rep)
	}
	if c := check(t, rep, SvcProcessPath, "cpt schedule"); c.Status != StatusMissing || !strings.Contains(c.Detail, "AllowWriteSchedule is false") {
		t.Fatalf("schedule check %+v, want missing and not written because AllowWriteSchedule is false", c)
	}
	if n := w.writes()[SvcProcessPath]; n != 0 {
		t.Fatalf("%d writes to process-path-management without AllowWriteSchedule, want 0", n)
	}
	for _, c := range []struct{ area, name string }{
		{SvcFacility, "site"}, {SvcFacility, "dock door " + site.Doors[0]}, {SvcFacility, "dock door " + site.Doors[1]},
		{SvcFacility, "staging slot"}, {SvcDock, "dock calendar settings"}, {SvcYard, "yard board"},
	} {
		if got := check(t, rep, c.area, c.name); got.Status != StatusCreated && got.Status != StatusOK {
			t.Errorf("%s/%s = %+v, want created or ok", c.area, c.name, got)
		}
	}
	if rep.Calendar != DefaultCalendar {
		t.Fatalf("calendar %+v, want the defaults declared", rep.Calendar)
	}
	if strings.Join(rep.Doors, ",") != strings.Join(site.Doors, ",") || len(rep.StagingLocations) != 1 {
		t.Fatalf("usable doors %v staging %v", rep.Doors, rep.StagingLocations)
	}
	// What was written, route by route: one site, the two location types, zones and aisles once each (the
	// second door shares its zone and aisle), one slot per code.
	for tpl, want := range map[string]int{
		"POST /sites": 1, "POST /location-types": 2, "POST /sites/{code}/zones": 2, "POST /zones/{zone}/aisles": 2, "POST /locations": 3,
	} {
		if n := writesOf(w, SvcFacility, tpl[:strings.Index(tpl, " ")], tpl[strings.Index(tpl, " ")+1:]); n != want {
			t.Errorf("facility %s: %d writes, want %d", tpl, n, want)
		}
	}
	if n := writesOf(w, SvcDock, "PUT", "/sites/{site}/dock-calendar/settings"); n != 1 {
		t.Errorf("%d calendar writes, want 1", n)
	}
	w.mu.Lock()
	door := w.facility.slots[site.Doors[0]]
	w.mu.Unlock()
	if door == nil || door.Role != "Dock" || door.DockFlow != "Outbound" {
		t.Fatalf("door slot %+v, want a Dock slot with OUTBOUND flow", door)
	}
}

func TestProvisionWritesTheScheduleOnlyWhenAllowedAndKeepsOtherCutoffs(t *testing.T) {
	w, site := emptyWorld(t, "SIM9")
	other := Cutoff{CptID: "cpt-0900", LocalTime: "09:00", DaysOfWeek: []string{"Mon"}, ShipMethod: "air", EligiblePathIDs: []string{"PICK"}}
	w.seedSchedule("SIM9", "America/Chicago", other)
	opt := ProvisionOptions{Cutoffs: []Cutoff{cutoff1700}}

	rep := Provision(context.Background(), w.client, site, opt)
	if c := check(t, rep, SvcProcessPath, "cpt schedule"); c.Status != StatusMismatch || w.writes()[SvcProcessPath] != 0 {
		t.Fatalf("schedule %+v with %d writes: a schedule that lacks a wanted cutoff is a mismatch, not written without permission", c, w.writes()[SvcProcessPath])
	}

	opt.AllowWriteSchedule = true
	rep = Provision(context.Background(), w.client, site, opt)
	if c := check(t, rep, SvcProcessPath, "cpt schedule"); c.Status != StatusCreated {
		t.Fatalf("schedule %+v, want created with AllowWriteSchedule", c)
	}
	w.mu.Lock()
	got := w.ppm.schedules["SIM9"]
	w.mu.Unlock()
	ids := []string{}
	for _, c := range got.Cutoffs {
		ids = append(ids, c.CptID)
	}
	if got.Timezone != "America/Chicago" || strings.Join(ids, ",") != "cpt-1700,cpt-0900" {
		t.Fatalf("schedule after the PUT: %s %v, want the wanted cutoff and the one the site already had", got.Timezone, ids)
	}
	if !rep.Ready() {
		t.Fatalf("not ready:\n%s", rep)
	}
}

func TestProvisionRefusesToPUTAScheduleWithoutItsInputs(t *testing.T) {
	w, site := emptyWorld(t, "SIM9")
	bad := cutoff1700
	bad.EligiblePathIDs = nil
	rep := Provision(context.Background(), w.client, site, ProvisionOptions{Cutoffs: []Cutoff{bad}, AllowWriteSchedule: true})
	if c := check(t, rep, SvcProcessPath, "cpt schedule"); c.Status != StatusRefused || w.writes()[SvcProcessPath] != 0 {
		t.Fatalf("schedule %+v: a cutoff without eligible paths must not be sent", c)
	}
}

func TestProvisionIsIdempotent(t *testing.T) {
	w, site := emptyWorld(t, "SIM9")
	opt := ProvisionOptions{Cutoffs: []Cutoff{cutoff1700}, AllowWriteSchedule: true}
	first := Provision(context.Background(), w.client, site, opt)
	if !first.Ready() || first.Writes == 0 {
		t.Fatalf("first run:\n%s", first)
	}
	before := w.totalWrites()
	second := Provision(context.Background(), w.client, site, opt)
	if !second.Ready() || second.Writes != 0 || w.totalWrites() != before {
		t.Fatalf("second run sent %d writes (%d on the fakes), want none:\n%s", second.Writes, w.totalWrites()-before, second)
	}
	for _, c := range second.Checks {
		if c.Status != StatusOK {
			t.Errorf("second run: %+v, want every check ok", c)
		}
	}
}

func TestProvisionReadOnlyReportsEverythingAndWritesNothing(t *testing.T) {
	w, site := emptyWorld(t, "SIM9")
	rep := Provision(context.Background(), w.client, site, ProvisionOptions{Cutoffs: []Cutoff{cutoff1700}, AllowWriteSchedule: true, ReadOnly: true})
	if rep.Ready() || rep.Writes != 0 || w.totalWrites() != 0 {
		t.Fatalf("read-only run sent %d writes:\n%s", w.totalWrites(), rep)
	}
	for _, c := range []struct{ area, name string }{
		{SvcProcessPath, "cpt schedule"}, {SvcFacility, "site"}, {SvcFacility, "dock door"}, {SvcFacility, "staging slot"}, {SvcDock, "dock calendar"}, {SvcYard, "yard board"},
	} {
		if got := check(t, rep, c.area, c.name); got.Status != StatusMissing {
			t.Errorf("%s/%s = %+v, want missing", c.area, c.name, got)
		}
	}
}

// A building that is already provisioned (the storefront's, by its seeder) is checked and not touched, and
// reuses the zones, aisles and location types it has.
func TestProvisionLeavesAReadyBuildingAlone(t *testing.T) {
	r := newRig(t)
	rep := Provision(context.Background(), r.w.client, r.site, ProvisionOptions{Cutoffs: []Cutoff{cutoff1700}})
	if !rep.Ready() || rep.Writes != 0 || r.w.totalWrites() != 0 {
		t.Fatalf("a ready building: %d writes\n%s", r.w.totalWrites(), rep)
	}
	if rep.Calendar.SlotGranularityMinutes != 5 || rep.Calendar.ArrivalToleranceMinutes != 30 {
		t.Fatalf("calendar %+v: the site's own settings must be reported, not replaced", rep.Calendar)
	}
}

func TestProvisionReusesExistingZonesAislesAndLocationTypes(t *testing.T) {
	w := newWorld(t)
	t.Cleanup(w.assertEveryPostKeyedOnce)
	seeded := w.seedSite("AUS3")
	// One more door in the existing dock zone and aisle, one more lane in the existing staging zone.
	site := seeded
	site.Doors = append(site.Doors, "AUS3-DOCK-DR-A01-01-02-A")
	site.StagingLocations = append(site.StagingLocations, "AUS3-STAG-LN-A01-01-02-A")
	rep := Provision(context.Background(), w.client, site, ProvisionOptions{})
	for tpl, svc := range map[string]string{"/sites/{code}/zones": SvcFacility, "/zones/{zone}/aisles": SvcFacility, "/location-types": SvcFacility, "/sites": SvcFacility} {
		if n := writesOf(w, svc, "POST", tpl); n != 0 {
			t.Errorf("POST %s: %d writes, want 0: the parent exists", tpl, n)
		}
	}
	if n := writesOf(w, SvcFacility, "POST", "/locations"); n != 2 {
		t.Errorf("%d slots registered, want 2", n)
	}
	if c := check(t, rep, SvcProcessPath, "cpt schedule"); c.Status != StatusMissing {
		t.Errorf("schedule %+v, want missing (none wanted written)", c)
	}
}

func TestProvisionReportsASlotItCannotFix(t *testing.T) {
	r := newRig(t)
	r.w.mu.Lock()
	r.w.facility.slots[r.site.Doors[0]].DockFlow = "Inbound"
	r.w.facility.slots[r.site.StagingLocations[0]].Status = "Decommissioned"
	r.w.mu.Unlock()
	rep := Provision(context.Background(), r.w.client, r.site, ProvisionOptions{Cutoffs: []Cutoff{cutoff1700}})
	if c := check(t, rep, SvcFacility, "dock door"); c.Status != StatusMismatch || !strings.Contains(c.Detail, "Outbound") {
		t.Errorf("door %+v, want a flow mismatch", c)
	}
	if c := check(t, rep, SvcFacility, "staging slot"); c.Status != StatusMismatch || !strings.Contains(c.Detail, "Decommissioned") {
		t.Errorf("staging %+v, want a decommissioned mismatch", c)
	}
	if rep.Ready() || r.w.totalWrites() != 0 {
		t.Fatalf("ready=%v with %d writes: a slot that exists in the wrong state must be reported, not re-registered", rep.Ready(), r.w.totalWrites())
	}
}

func TestProvisionReportsARefusedWrite(t *testing.T) {
	w, site := emptyWorld(t, "SIM9")
	w.mu.Lock()
	w.facility.failSlots = true
	w.mu.Unlock()
	rep := Provision(context.Background(), w.client, site, ProvisionOptions{})
	if c := check(t, rep, SvcFacility, "dock door "+site.Doors[0]); c.Status != StatusRefused || !strings.Contains(c.Detail, "placement-rule-violated") {
		t.Fatalf("door %+v, want refused with the slug", c)
	}
	if rep.Ready() {
		t.Fatal("a refused write is not ready")
	}
}

func TestProvisionRejectsACodeOutsideTheSite(t *testing.T) {
	w, site := emptyWorld(t, "SIM9")
	site.Doors = []string{"OTHER-DOCK-DR-A01-01-01-A", "SIM9-DOOR-1"}
	rep := Provision(context.Background(), w.client, site, ProvisionOptions{})
	for _, d := range site.Doors {
		if c := check(t, rep, SvcFacility, "dock door "+d); c.Status != StatusRefused || !strings.Contains(c.Detail, "SITE-AREA-ZONE-AISLE-BAY-LEVEL-POSITION") {
			t.Errorf("door %s: %+v, want refused locally", d, c)
		}
	}
	// Only the well-formed staging lane was registered; no parent was created for the malformed door codes.
	if n := writesOf(w, SvcFacility, "POST", "/locations"); n != 1 {
		t.Errorf("%d slot writes, want 1 (the staging lane only):\n%s", n, rep)
	}
	if n := writesOf(w, SvcFacility, "POST", "/location-types"); n != 1 {
		t.Errorf("%d location types written, want 1 (Staging only): a malformed door code must fail before any parent is created", n)
	}
}

func TestProvisionDiscoversDoorsAndLanesWhenNoneAreConfigured(t *testing.T) {
	w := newWorld(t)
	t.Cleanup(w.assertEveryPostKeyedOnce)
	seeded := w.seedSite("AUS3")
	w.seedSchedule("AUS3", "America/Chicago", cutoff1700)
	bare := Site{Code: "AUS3"}
	rep := Provision(context.Background(), w.client, bare, ProvisionOptions{})
	if !rep.Ready() {
		t.Fatalf("not ready:\n%s", rep)
	}
	got := rep.Apply(bare)
	if strings.Join(got.Doors, ",") != strings.Join(seeded.Doors, ",") || strings.Join(got.StagingLocations, ",") != strings.Join(seeded.StagingLocations, ",") || got.Timezone != "America/Chicago" {
		t.Fatalf("Apply = %+v, want the discovered doors, lanes and timezone", got)
	}
	// Nothing to discover and nothing configured: it does not invent location codes.
	w2, empty := emptyWorld(t, "SIM9")
	rep = Provision(context.Background(), w2.client, Site{Code: empty.Code}, ProvisionOptions{})
	if c := check(t, rep, SvcFacility, "dock doors"); c.Status != StatusMissing || w2.facility.slots[empty.Code+"-DOCK-DR-A01-01-01-A"] != nil {
		t.Fatalf("doors %+v: with none configured and none found it must report, not invent", c)
	}
}

func TestProvisionReportsAScheduleTimezoneMismatch(t *testing.T) {
	r := newRig(t)
	site := r.site
	site.Timezone = "UTC"
	rep := Provision(context.Background(), r.w.client, site, ProvisionOptions{Cutoffs: []Cutoff{cutoff1700}})
	if c := check(t, rep, SvcProcessPath, "cpt schedule"); c.Status != StatusMismatch || !strings.Contains(c.Detail, "timezone") {
		t.Fatalf("schedule %+v, want a timezone mismatch", c)
	}
}

// The readiness step feeds the floor: provision an empty building, then ship a package out of it.
func TestProvisionThenShip(t *testing.T) {
	w, site := emptyWorld(t, "SIM9")
	rep := Provision(context.Background(), w.client, site, ProvisionOptions{Cutoffs: []Cutoff{cutoff1700}, AllowWriteSchedule: true})
	if !rep.Ready() {
		t.Fatalf("not ready:\n%s", rep)
	}
	pkg := Package{ID: "PKG-P", OrderRef: "ORD-P", SourceOrderID: "SRC-P", SiteID: "SIM9", CPT: "17:00"}
	w.mu.Lock()
	w.out.manifest(pkg)
	w.mu.Unlock()
	f := w.floor(rep.Apply(site))
	f.GridMinutes = rep.Calendar.SlotGranularityMinutes
	f.Wait = Waits{Timeout: 3 * time.Second, Interval: time.Millisecond}
	if s := f.Ship(context.Background(), pkg); !s.OK() {
		t.Fatalf("shipment %+v", s)
	}
}
