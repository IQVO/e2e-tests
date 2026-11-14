package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// The warehouse building (ADR 0001 D1): the storefront's node is site-1 for the network, the
// warehouse building serving it is the facility-layout site AUS3, and process-path-management
// must hold the SAME CPT schedule under both ids. These tests drive the whole seeder against the
// contract fake, which keeps one schedule per site and answers facility-layout's site read.

const (
	nodeID     = "site-1"
	buildingID = "AUS3"
)

// writeSiteFixture writes a fulfilment-site fixture to a temporary file and returns its path.
func writeSiteFixture(t *testing.T, s *SiteFixture) string {
	t.Helper()
	raw, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "fulfilment-site.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// putsTo is the PUT /sites/{id}/cpt-schedule count the fake received, by site.
func putsTo(f *fakeCluster) map[string]int {
	out := map[string]int{}
	for k, v := range f.putsBySite {
		out[k] = v
	}
	return out
}

// TestFirstRunDefinesTheScheduleOnBothIdsSecondRunWritesNone: against a cluster where
// process-path-management knows nothing of the building, the first run issues ONE PUT per id
// (site-1: replacing the warehouse-day leftover; AUS3: new), both with the fixture's schedule, and
// the second run plans and sends no write at all.
func TestFirstRunDefinesTheScheduleOnBothIdsSecondRunWritesNone(t *testing.T) {
	in := loadInputs(t)
	f, _ := clusterBeforeTheBuildingSchedule(in)
	var out bytes.Buffer
	cfg := config("", in, &out)

	res, err := run(t, context.Background(), cfg, f)
	if err != nil {
		t.Fatalf("first run: %v\n%s", err, out.String())
	}
	if got := putsTo(f); !reflect.DeepEqual(got, map[string]int{nodeID: 1, buildingID: 1}) || f.puts != 2 || f.cptChanges != 2 {
		t.Fatalf("PUTs by site %v (%d total, %d changing a schedule): want exactly one each to %s and %s\n%s", got, f.puts, f.cptChanges, nodeID, buildingID, out.String())
	}
	if res.WritesByKey["cpt-schedule"] != 2 || res.Plan.BuildingSchedule == nil || res.Plan.CPTSchedule == nil {
		t.Fatalf("the plan and the counters must carry both schedules: %v, plan %+v", res.WritesByKey, res.Plan)
	}
	want := in.site.Request()
	for _, id := range []string{nodeID, buildingID} {
		if s := f.cpt[id]; !sameSchedule(s, want) || s.SiteID != id {
			t.Fatalf("%s's schedule did not converge on the fixture's: %+v", id, s)
		}
	}
	if !reflect.DeepEqual(f.cpt[nodeID].Cutoffs, f.cpt[buildingID].Cutoffs) || f.cpt[nodeID].Timezone != f.cpt[buildingID].Timezone {
		t.Fatalf("the two ids must carry the same schedule: %+v vs %+v", f.cpt[nodeID], f.cpt[buildingID])
	}
	for _, line := range []string{
		"check: warehouse building AUS3 (serves node site-1): facility-layout knows the site (Active)",
		"cpt schedule (WMS): site-1 define America/Chicago [aus-1100 11:00, aus-1700 17:00]",
		"cpt schedule (WMS building): AUS3 define America/Chicago [aus-1100 11:00, aus-1700 17:00] (current none differs or is missing)",
		"apply: wms: CPT schedule of AUS3 defined: America/Chicago [aus-1100 11:00, aus-1700 17:00]",
		"verify: process-path-management CPT schedule of AUS3 (warehouse building of site-1) equals the fixture: America/Chicago [aus-1100 11:00, aus-1700 17:00]",
	} {
		if !strings.Contains(out.String(), line) {
			t.Errorf("the output does not say %q:\n%s", line, out.String())
		}
	}

	out.Reset()
	before := putsTo(f)
	res, err = run(t, context.Background(), cfg, f)
	if err != nil {
		t.Fatalf("second run: %v\n%s", err, out.String())
	}
	if res.Writes != 0 || res.Plan.Writes() != 0 || f.puts != 2 || !reflect.DeepEqual(putsTo(f), before) {
		t.Fatalf("second run must write nothing: %d sent, %d planned, PUTs %v -> %v\n%s", res.Writes, res.Plan.Writes(), before, putsTo(f), out.String())
	}
	if !strings.Contains(out.String(), "cpt schedule (WMS building): AUS3 current America/Chicago [aus-1100 11:00, aus-1700 17:00] matches, skip") ||
		!strings.Contains(out.String(), "cpt schedule (WMS): site-1 current America/Chicago [aus-1100 11:00, aus-1700 17:00] matches, skip") {
		t.Fatalf("the second run must report both schedules as matching:\n%s", out.String())
	}
}

// TestWMSOnlyDefinesBothSchedulesAndTheStorefrontTierFindsThemEqual: scripts/10 step 2
// (-wms-only) writes the two schedules and nothing else, a rerun writes nothing, and step 5 (the
// whole seeder) skips both.
func TestWMSOnlyDefinesBothSchedulesAndTheStorefrontTierFindsThemEqual(t *testing.T) {
	in := loadInputs(t)
	f, _ := clusterBeforeTheBuildingSchedule(in)
	var out bytes.Buffer
	cfg := config("", in, &out)
	cfg.WMSOnly, cfg.DryRun = true, true
	res, err := run(t, context.Background(), cfg, f)
	if err != nil || res.Plan.Writes() != 2 || f.puts != 0 || f.posts != 0 {
		t.Fatalf("wms-only dry run: err %v, %d planned, %d PUTs, %d POSTs\n%s", err, res.Plan.Writes(), f.puts, f.posts, out.String())
	}
	cfg.DryRun = false
	out.Reset()
	res, err = run(t, context.Background(), cfg, f)
	if err != nil || res.Writes != 2 || f.posts != 0 || !reflect.DeepEqual(putsTo(f), map[string]int{nodeID: 1, buildingID: 1}) {
		t.Fatalf("wms-only: err %v, %d writes, %d POSTs, PUTs %v\n%s", err, res.Writes, f.posts, putsTo(f), out.String())
	}
	if !strings.Contains(out.String(), "verify: process-path-management CPT schedule of AUS3") {
		t.Fatalf("the WMS step must verify the building's schedule too:\n%s", out.String())
	}
	out.Reset()
	if res, err = run(t, context.Background(), cfg, f); err != nil || res.Writes != 0 || f.puts != 2 {
		t.Fatalf("wms-only rerun: err %v, %d writes, %d PUTs\n%s", err, res.Writes, f.puts, out.String())
	}
	cfg.WMSOnly = false
	out.Reset()
	if res, err = run(t, context.Background(), cfg, f); err != nil || res.Plan.CPTSchedule != nil || res.Plan.BuildingSchedule != nil || res.WritesByKey["cpt-schedule"] != 0 || f.puts != 2 {
		t.Fatalf("the storefront tier after the WMS step must skip both schedules: err %v, %v, %d PUTs\n%s", err, res.WritesByKey, f.puts, out.String())
	}
}

// TestBuildingScheduleDriftIsRestoredWithExactlyOnePut: once the cluster is converged, a schedule
// of AUS3 edited by hand (another timezone, a cutoff gone, the schedule removed) is put back with
// one PUT to AUS3 and none to site-1, whose schedule is not touched; an equal schedule in another
// order is no drift.
func TestBuildingScheduleDriftIsRestoredWithExactlyOnePut(t *testing.T) {
	in := loadInputs(t)
	want := in.site.Request()
	reordered := &CPTSchedule{SiteID: buildingID, Timezone: want.Timezone}
	for i := len(want.Cutoffs) - 1; i >= 0; i-- {
		reordered.Cutoffs = append(reordered.Cutoffs, want.Cutoffs[i])
	}
	oneCutoffGone := &CPTSchedule{SiteID: buildingID, Timezone: want.Timezone, Cutoffs: want.Cutoffs[:1]}
	utc := simLeftoverSchedule()
	utc.SiteID = buildingID
	for _, tc := range []struct {
		name     string
		edit     func(f *fakeCluster)
		wantPuts int
	}{
		{"timezone and cutoffs of a warehouse-day run", func(f *fakeCluster) { f.cpt[buildingID] = utc }, 1},
		{"one cutoff removed", func(f *fakeCluster) { f.cpt[buildingID] = oneCutoffGone }, 1},
		{"schedule removed", func(f *fakeCluster) { delete(f.cpt, buildingID) }, 1},
		{"equal, other order", func(f *fakeCluster) { f.cpt[buildingID] = reordered }, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, _ := clusterBeforeTheBuildingSchedule(in)
			var out bytes.Buffer
			cfg := config("", in, &out)
			if _, err := run(t, context.Background(), cfg, f); err != nil { // converge everything first
				t.Fatalf("converge: %v\n%s", err, out.String())
			}
			node := f.cpt[nodeID]
			before := putsTo(f)

			tc.edit(f)
			out.Reset()
			res, err := run(t, context.Background(), cfg, f)
			if err != nil {
				t.Fatalf("after the edit: %v\n%s", err, out.String())
			}
			if got := f.putsBySite[buildingID] - before[buildingID]; got != tc.wantPuts || res.WritesByKey["cpt-schedule"] != tc.wantPuts || res.Writes != tc.wantPuts || res.Plan.Writes() != tc.wantPuts {
				t.Fatalf("%d PUTs to %s, %d writes sent, %d planned; want exactly %d\n%s", got, buildingID, res.Writes, res.Plan.Writes(), tc.wantPuts, out.String())
			}
			if f.putsBySite[nodeID] != before[nodeID] || f.cpt[nodeID] != node {
				t.Fatalf("site-1 must not be touched: PUTs %v -> %v", before, putsTo(f))
			}
			if !sameSchedule(f.cpt[buildingID], want) || f.cpt[buildingID].SiteID != buildingID {
				t.Fatalf("%s did not converge: %+v", buildingID, f.cpt[buildingID])
			}
			total := f.puts
			out.Reset()
			if res, err = run(t, context.Background(), cfg, f); err != nil || res.Writes != 0 || f.puts != total {
				t.Fatalf("rerun: err %v, %d writes, %d PUTs (want still %d)\n%s", err, res.Writes, f.puts, total, out.String())
			}
		})
	}
}

// TestNodeScheduleDriftStillRestoresOnlyTheNode: site-1's behaviour is the one it always had: a
// warehouse-day leftover on site-1 is one PUT to site-1, none to the converged building.
func TestNodeScheduleDriftStillRestoresOnlyTheNode(t *testing.T) {
	in := loadInputs(t)
	f, _ := clusterLike(in) // site-1: the warehouse-day leftover; AUS3: converged
	building := f.cpt[buildingID]
	var out bytes.Buffer
	res, err := run(t, context.Background(), config("", in, &out), f)
	if err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	if !reflect.DeepEqual(putsTo(f), map[string]int{nodeID: 1}) || f.cpt[buildingID] != building || res.Plan.BuildingSchedule != nil {
		t.Fatalf("PUTs %v, building plan %v: want one PUT to site-1 and none to %s", putsTo(f), res.Plan.BuildingSchedule, buildingID)
	}
}

// TestMissingFacilityLayoutSiteFailsCheckBeforeAnyWrite: when facility-layout does not know the
// warehouse building (never registered, or decommissioned), CHECK fails naming the provisioning
// command, in every mode, with no write of any kind (not even site-1's schedule or a storefront
// POST) and the site is never created.
func TestMissingFacilityLayoutSiteFailsCheckBeforeAnyWrite(t *testing.T) {
	in := loadInputs(t)
	for _, tc := range []struct {
		name    string
		prepare func(f *fakeCluster)
		want    []string
	}{
		{"never registered", func(f *fakeCluster) { delete(f.sites, buildingID) },
			[]string{"AUS3", "not a facility-layout site", "run_all.sh --only AUS3", "never creates a site"}},
		{"decommissioned", func(f *fakeCluster) { f.sites[buildingID] = "Decommissioned" },
			[]string{"AUS3", "Decommissioned", "run_all.sh --only AUS3"}},
	} {
		for _, mode := range []string{"provision", "dry-run", "wms-only", "verify-only"} {
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				f, _ := clusterBeforeTheBuildingSchedule(in)
				tc.prepare(f)
				var out bytes.Buffer
				cfg := config("", in, &out)
				cfg.DryRun, cfg.WMSOnly, cfg.VerifyOnly = mode == "dry-run", mode == "wms-only", mode == "verify-only"
				_, err := run(t, context.Background(), cfg, f)
				if err == nil || !strings.HasPrefix(err.Error(), "check: ") {
					t.Fatalf("want a CHECK failure, got %v", err)
				}
				for _, w := range tc.want {
					if !strings.Contains(err.Error(), w) {
						t.Errorf("the failure does not say %q: %v", w, err)
					}
				}
				if f.puts != 0 || f.posts != 0 || f.mutations != 0 {
					t.Fatalf("nothing may be written: %d PUTs, %d POSTs, %d mutations", f.puts, f.posts, f.mutations)
				}
				if _, created := f.sites[buildingID]; created && tc.name == "never registered" {
					t.Fatal("the seeder must never create the facility-layout site")
				}
			})
		}
	}
}

// TestInvalidWarehouseSiteIDFailsCheckBeforeAnyWrite: a fixture whose warehouseSiteId order-
// management would reject (here a node-style code) fails CHECK before anything is read or written.
func TestInvalidWarehouseSiteIDFailsCheckBeforeAnyWrite(t *testing.T) {
	in := loadInputs(t)
	f, _ := clusterBeforeTheBuildingSchedule(in)
	var out bytes.Buffer
	cfg := config("", in, &out)
	bad := *in.site
	bad.WarehouseSiteID = "aus3"
	cfg.SitePath = writeSiteFixture(t, &bad)
	_, err := run(t, context.Background(), cfg, f)
	if err == nil || !strings.Contains(err.Error(), `warehouseSiteId "aus3" is not ^[A-Z0-9]+$`) || f.puts != 0 || f.posts != 0 {
		t.Fatalf("want a fixture CHECK failure without writes, got %v (%d PUTs, %d POSTs)", err, f.puts, f.posts)
	}
}

// TestVerifyOnlyFailsWhileTheBuildingHasNoSchedule: the read-only verify is the check that the
// storefront is still sellable; a building without the schedule is named, nothing is written.
func TestVerifyOnlyFailsWhileTheBuildingHasNoSchedule(t *testing.T) {
	in := loadInputs(t)
	f, _ := clusterBeforeTheBuildingSchedule(in)
	var out bytes.Buffer
	cfg := config("", in, &out)
	if _, err := run(t, context.Background(), cfg, f); err != nil {
		t.Fatalf("converge: %v\n%s", err, out.String())
	}
	puts := f.puts
	delete(f.cpt, buildingID)
	cfg.VerifyOnly = true
	out.Reset()
	_, err := run(t, context.Background(), cfg, f)
	if err == nil || !strings.Contains(err.Error(), "AUS3") || !strings.Contains(err.Error(), "no CPT schedule") || f.puts != puts {
		t.Fatalf("want a verify failure naming AUS3's missing schedule and no write, got %v (%d PUTs, was %d)\n%s", err, f.puts, puts, out.String())
	}
	// and once it is there, verify-only passes
	f.cpt[buildingID] = &CPTSchedule{SiteID: buildingID, Timezone: in.site.Timezone, Cutoffs: in.site.Request().Cutoffs}
	out.Reset()
	if _, err := run(t, context.Background(), cfg, f); err != nil {
		t.Fatalf("verify-only with both schedules: %v\n%s", err, out.String())
	}
}

// TestVerifyBuildingCPTNamesTheDifference pins the read-back's three outcomes.
func TestVerifyBuildingCPTNamesTheDifference(t *testing.T) {
	in := loadInputs(t)
	f, _ := clusterLike(in)
	srv := f.start()
	defer srv.Close()
	c := newClient(srv.URL+"/api", "", srv.Client(), true)
	ctx := context.Background()
	if line, problem := verifyBuildingCPT(ctx, c, in.site); problem != "" || !strings.Contains(line, "equals the fixture") {
		t.Fatalf("equal: %q %q", line, problem)
	}
	f.cpt[buildingID] = simLeftoverSchedule()
	if _, problem := verifyBuildingCPT(ctx, c, in.site); !strings.Contains(problem, "differs from the fixture") || !strings.Contains(problem, "UTC [sim-1200 09:39") {
		t.Fatalf("differs: %q", problem)
	}
	delete(f.cpt, buildingID)
	if _, problem := verifyBuildingCPT(ctx, c, in.site); !strings.Contains(problem, "no CPT schedule") {
		t.Fatalf("missing: %q", problem)
	}
}

// TestFakeKeepsOneScheduleASiteAndReadsFacilityLayoutSites pins the new slices of the fake: a PUT
// to one site never changes another site's schedule, and facility-layout answers a registered
// site (the published Site shape) or 404.
func TestFakeKeepsOneScheduleASiteAndReadsFacilityLayoutSites(t *testing.T) {
	f := newFakeCluster()
	f.sites[buildingID] = "Active"
	srv := f.start()
	defer srv.Close()
	all := []string{"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"}
	req := func(id, at string) DefineCPTScheduleRequest {
		return DefineCPTScheduleRequest{Timezone: "America/Chicago", Cutoffs: []Cutoff{{CptID: id, LocalTime: at, DaysOfWeek: all, ShipMethod: "ground", EligiblePathIDs: []string{"PICK"}}}}
	}
	base := srv.URL + "/api/process-path-management/sites/"
	if status, body := putJSON(t, base+nodeID+"/cpt-schedule", req("aus-1100", "11:00")); status != http.StatusOK {
		t.Fatalf("PUT site-1: %d %s", status, body)
	}
	if status, body := putJSON(t, base+buildingID+"/cpt-schedule", req("aus-1700", "17:00")); status != http.StatusOK {
		t.Fatalf("PUT AUS3: %d %s", status, body)
	}
	c := newClient(srv.URL+"/api", "", srv.Client(), false)
	ctx := context.Background()
	node, err := observeCPT(ctx, c, nodeID)
	if err != nil || node == nil || node.SiteID != nodeID || node.Cutoffs[0].CptID != "aus-1100" {
		t.Fatalf("site-1: %+v %v", node, err)
	}
	building, err := observeCPT(ctx, c, buildingID)
	if err != nil || building == nil || building.SiteID != buildingID || building.Cutoffs[0].CptID != "aus-1700" {
		t.Fatalf("AUS3: %+v %v", building, err)
	}
	if other, err := observeCPT(ctx, c, "SIM1"); err != nil || other != nil {
		t.Fatalf("a third site has no schedule: %+v %v", other, err)
	}
	if !reflect.DeepEqual(putsTo(f), map[string]int{nodeID: 1, buildingID: 1}) {
		t.Fatalf("PUTs by site: %v", putsTo(f))
	}

	site, err := readFacilitySite(ctx, c, buildingID)
	if err != nil || site == nil || site.SiteCode != buildingID || site.Status != "Active" {
		t.Fatalf("GET /facility-layout/sites/AUS3: %+v %v", site, err)
	}
	if site, err := readFacilitySite(ctx, c, nodeID); err != nil || site != nil {
		t.Fatalf("site-1 is the network's id, not a facility-layout site: %+v %v", site, err)
	}
	if resp, err := http.Get(srv.URL + "/api/facility-layout/sites/AUS3/zones"); err != nil || resp.StatusCode != http.StatusNotFound {
		t.Fatalf("the fake answers only the site read: %v %v", resp, err)
	}
}
