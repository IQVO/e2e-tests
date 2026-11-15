package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/claudioed/warehouse-e2e/internal/sim"
	"github.com/claudioed/warehouse-e2e/internal/sim/floor"
	"github.com/claudioed/warehouse-e2e/internal/sim/inbound"
	"github.com/claudioed/warehouse-e2e/internal/sim/outbound"
)

// ---- the readiness orchestration, with the readiness steps swapped for recorders ----

type fakeStations struct {
	provisioned []floor.Crew
	provErr     error
	shutdowns   int
	shutErr     error
	findings    []string
	stats       floor.Stats
}

func (f *fakeStations) Provision(_ context.Context, c floor.Crew) error {
	f.provisioned = append(f.provisioned, c)
	return f.provErr
}
func (f *fakeStations) Shutdown(context.Context) error { f.shutdowns++; return f.shutErr }
func (f *fakeStations) Findings() []string             { return f.findings }
func (f *fakeStations) Stats() floor.Stats             { return f.stats }

type rig struct {
	b  *building
	fs *fakeStations
	cl *fakeCluster

	outCalls []outboundCall
	inCalls  []inboundCall
	binCalls int
	logged   []string

	outReport outbound.Report
	inReady   inbound.Readiness
	bins      []string
	binsErr   error
}

type outboundCall struct {
	site outbound.Site
	opt  outbound.ProvisionOptions
}

type inboundCall struct {
	site inbound.Site
	opt  inbound.ReadyOptions
}

func okReport() outbound.Report {
	return outbound.Report{
		Site:             "AUS3",
		Checks:           []outbound.Check{{Area: "process-path-management", Name: "cpt schedule", Status: outbound.StatusOK}},
		Doors:            []string{"AUS3-OB-D1"},
		StagingLocations: []string{"AUS3-STG-1"},
		Calendar:         outbound.CalendarSettings{SlotGranularityMinutes: 15},
	}
}

func okReadiness() inbound.Readiness {
	return inbound.Readiness{
		Report: outbound.Report{Site: "AUS3", Doors: []string{"AUS3-IB-D1"}, Calendar: outbound.CalendarSettings{SlotGranularityMinutes: 15},
			Checks: []outbound.Check{{Area: "inbound-receiving", Name: "door AUS3-IB-D1", Status: outbound.StatusOK}}},
		Bins: []string{"AUS3-STO-1"},
	}
}

func newRig(t *testing.T, mod func(*wireConfig)) *rig {
	t.Helper()
	r := &rig{fs: &fakeStations{}, cl: newFakeCluster(t, nodeRoutes()), outReport: okReport(), inReady: okReadiness(), bins: []string{"AUS3-STO-1", "AUS3-STO-2"}}
	cfg := wireConfig{
		Client: r.cl.client(), RunID: "RDT1", Building: "AUS3", NetworkSite: "site-1", VendorID: "V-WS", Parallel: 6,
		Log: func(f string, a ...any) { r.logged = append(r.logged, f) },
	}
	if mod != nil {
		mod(&cfg)
	}
	r.b = &building{
		cfg: cfg, nodes: &nodeBook{c: cfg.Client, vendorID: cfg.VendorID, networkSite: cfg.NetworkSite, prefixes: &orderPrefixes{}}, fl: r.fs,
		site: siteFixture{SiteID: "site-1", WarehouseSiteID: "AUS3", Cutoffs: []outbound.Cutoff{{CptID: "aus-1700", LocalTime: "17:00"}}},
		provisionOutbound: func(_ context.Context, _ *sim.Client, s outbound.Site, o outbound.ProvisionOptions) outbound.Report {
			r.outCalls = append(r.outCalls, outboundCall{s, o})
			return r.outReport
		},
		readyInbound: func(_ context.Context, _ *sim.Client, s inbound.Site, o inbound.ReadyOptions) inbound.Readiness {
			r.inCalls = append(r.inCalls, inboundCall{s, o})
			return r.inReady
		},
		listStorageBins: func(context.Context) ([]string, error) { r.binCalls++; return r.bins, r.binsErr },
	}
	return r
}

func TestPrepareIsReadOnlyByDefault(t *testing.T) {
	r := newRig(t, nil)
	if err := r.b.prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(r.outCalls) != 1 {
		t.Fatalf("%d outbound readiness calls", len(r.outCalls))
	}
	o := r.outCalls[0]
	if !o.opt.ReadOnly {
		t.Error("the outbound readiness may write although -provision was not given")
	}
	if o.opt.AllowWriteSchedule {
		t.Error("the outbound readiness may PUT the storefront building's CPT schedule")
	}
	if o.site.Code != "AUS3" || len(o.opt.Cutoffs) != 1 || o.opt.Cutoffs[0].CptID != "aus-1700" {
		t.Errorf("outbound readiness asked about %+v with cutoffs %+v", o.site, o.opt.Cutoffs)
	}
	if len(r.inCalls) != 1 || r.inCalls[0].opt.BinCapacity != 0 {
		t.Errorf("the inbound readiness may register bins: %+v", r.inCalls)
	}
	if w := r.cl.writes(); len(w) != 0 {
		t.Errorf("prepare wrote through the client: %v", w)
	}
}

func TestProvisionOpensTheDocksWritesButNeverTheCPTScheduleNorBins(t *testing.T) {
	r := newRig(t, func(c *wireConfig) {
		c.Provision = true
		c.Doors = []string{"AUS3-OB-D9"}
		c.Staging = []string{"AUS3-STG-9"}
	})
	if err := r.b.prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	o := r.outCalls[0]
	if o.opt.ReadOnly {
		t.Error("-provision left the outbound readiness read-only")
	}
	if o.opt.AllowWriteSchedule {
		t.Error("-provision lets the readiness PUT the CPT schedule, which belongs to cmd/seed-storefront")
	}
	if !equal(o.site.Doors, "AUS3-OB-D9") || !equal(o.site.StagingLocations, "AUS3-STG-9") {
		t.Errorf("the doors and staging lanes named on the command line did not reach the readiness: %+v", o.site)
	}
	if r.inCalls[0].opt.BinCapacity != 0 {
		t.Error("-provision lets the inbound readiness register bins")
	}
}

func equal(got []string, want ...string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func TestPrepareBuildsTheCrewsFromWhatTheReadinessFound(t *testing.T) {
	r := newRig(t, nil)
	if r.b.dock() != nil || r.b.crew() != nil {
		t.Fatal("the crews exist before prepare")
	}
	if err := r.b.prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	of, ic := r.b.outFloor, r.b.inCrew
	if r.b.dock() == nil || r.b.crew() == nil || of == nil || ic == nil {
		t.Fatal("prepare did not build the crews")
	}
	if !equal(of.Site.Doors, "AUS3-OB-D1") || !equal(of.Site.StagingLocations, "AUS3-STG-1") || of.GridMinutes != 15 || of.RunID != "RDT1" || of.Site.Code != "AUS3" {
		t.Errorf("dock crew %+v grid %d", of.Site, of.GridMinutes)
	}
	if !equal(ic.Site.Doors, "AUS3-IB-D1") || !equal(ic.Site.Bins, "AUS3-STO-1") || ic.GridMinutes != 15 || ic.RunID != "RDT1" || ic.Site.Code != "AUS3" {
		t.Errorf("restock crew %+v grid %d", ic.Site, ic.GridMinutes)
	}
	if len(r.fs.provisioned) != 1 || r.fs.provisioned[0] != (floor.Crew{Pickers: 6, Packers: 3}) {
		t.Errorf("stations %+v", r.fs.provisioned)
	}
}

func TestPrepareFailsClosedAndLeavesTheBuildingUntouched(t *testing.T) {
	notReady := outbound.Check{Area: "process-path-management", Name: "cpt schedule", Status: outbound.StatusMissing, Detail: "no schedule defined for the site"}
	cases := map[string]struct {
		mod       func(*rig)
		wantErr   string
		wantInbnd int // inbound readiness calls
	}{
		"a dock check is not ready": {func(r *rig) {
			r.outReport.Checks = append(r.outReport.Checks, notReady)
		}, "no schedule defined for the site", 0},
		"the yard board lacks the doors": {func(r *rig) {
			r.outReport.Checks = append(r.outReport.Checks, outbound.Check{Area: "yard-management", Name: "yard board knows the doors", Status: outbound.StatusMissing, Detail: "retry shortly"})
		}, "yard board knows the doors", 0},
		"no bin has room": {func(r *rig) { r.inReady.Bins = nil }, "no stow bin", 1},
		"an inbound door is missing": {func(r *rig) {
			r.inReady.Checks = append(r.inReady.Checks, outbound.Check{Area: "inbound-receiving", Name: "door AUS3-IB-D2", Status: outbound.StatusMissing, Detail: "not an inbound door"})
		}, "AUS3-IB-D2", 1},
		"the stations cannot be installed": {func(r *rig) { r.fs.provErr = errors.New("POST /stations 409") }, "POST /stations 409", 1},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			r := newRig(t, nil)
			tc.mod(r)
			err := r.b.prepare(context.Background())
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err %v, want it to mention %q", err, tc.wantErr)
			}
			if r.b.dock() != nil || r.b.crew() != nil {
				t.Error("a failed prepare left crews behind")
			}
			if len(r.inCalls) != tc.wantInbnd {
				t.Errorf("%d inbound readiness calls, want %d", len(r.inCalls), tc.wantInbnd)
			}
			if name != "the stations cannot be installed" && len(r.fs.provisioned) != 0 {
				t.Errorf("stations were installed on a building that is not ready: %+v", r.fs.provisioned)
			}
		})
	}
}

func TestPrepareNamesEveryBlockerAndTheWayOut(t *testing.T) {
	r := newRig(t, nil)
	r.outReport.Checks = []outbound.Check{
		{Area: "a", Name: "n1", Status: outbound.StatusMissing, Detail: "d1"},
		{Area: "b", Name: "n2", Status: outbound.StatusMismatch, Detail: "d2"},
		{Area: "c", Name: "n3", Status: outbound.StatusOK, Detail: "fine"},
	}
	err := r.b.prepare(context.Background())
	if err == nil {
		t.Fatal("no error")
	}
	for _, want := range []string{"d1", "d2", "-provision", "make provision-storefront"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q lacks %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "fine") {
		t.Errorf("a check that passed is listed as a blocker: %v", err)
	}
}

func TestPrepareStopsAtTheNodesBeforeAnyReadinessStep(t *testing.T) {
	r := newRig(t, nil)
	r.b.nodes.vendorID = "V-NOBODY"
	r.cl.routes["GET vendor-connectivity/vendor/nodes"] = func(w http.ResponseWriter, _ *http.Request) { problem(w, 404, "vendor-not-found") }
	if err := r.b.prepare(context.Background()); err == nil || !strings.Contains(err.Error(), "origin node") {
		t.Fatalf("err %v", err)
	}
	if len(r.outCalls)+len(r.inCalls)+len(r.fs.provisioned) != 0 {
		t.Error("the building was looked at although the transport's nodes are unknown")
	}
}

func TestStowBinsNamedByTheUserAreAllRequiredAndNothingIsDiscovered(t *testing.T) {
	r := newRig(t, func(c *wireConfig) { c.StowBins = []string{"AUS3-STO-7", "AUS3-STO-8"} })
	r.inReady.Checks = append(r.inReady.Checks, outbound.Check{Area: "inventory-storage", Name: "bin AUS3-STO-8", Status: outbound.StatusMissing, Detail: "not registered"})
	err := r.b.prepare(context.Background())
	if err == nil || !strings.Contains(err.Error(), "bin AUS3-STO-8") {
		t.Fatalf("err %v", err)
	}
	if r.binCalls != 0 {
		t.Error("bins were discovered although the user named them")
	}
	if !equal(r.inCalls[0].site.Bins, "AUS3-STO-7", "AUS3-STO-8") {
		t.Errorf("bins offered %v", r.inCalls[0].site.Bins)
	}
}

func TestDiscoveredBinsMayBeUnregisteredAsLongAsOneHasRoom(t *testing.T) {
	r := newRig(t, nil)
	r.inReady.Checks = append(r.inReady.Checks, outbound.Check{Area: "inventory-storage", Name: "bin AUS3-STO-2", Status: outbound.StatusMissing, Detail: "not registered"})
	if err := r.b.prepare(context.Background()); err != nil {
		t.Fatalf("an unregistered candidate bin blocked the day: %v", err)
	}
	if !equal(r.inCalls[0].site.Bins, "AUS3-STO-1", "AUS3-STO-2") {
		t.Errorf("candidates offered %v", r.inCalls[0].site.Bins)
	}
	// ... but another kind of blocker is never excused, and neither is the absence of any usable bin
	r2 := newRig(t, nil)
	r2.inReady.Checks = append(r2.inReady.Checks, outbound.Check{Area: "inventory-storage", Name: "stow bins", Status: outbound.StatusMissing, Detail: "none"})
	if err := r2.b.prepare(context.Background()); err == nil {
		t.Error("a missing check that is not a candidate bin was excused")
	}
}

func TestPrepareRefusesWhenNoBinCanBeDiscovered(t *testing.T) {
	r := newRig(t, nil)
	r.bins = nil
	if err := r.b.prepare(context.Background()); err == nil || !strings.Contains(err.Error(), "-stow-bins") {
		t.Errorf("no candidate: %v", err)
	}
	r = newRig(t, nil)
	r.binsErr = errors.New("facility-layout GET: 503")
	if err := r.b.prepare(context.Background()); err == nil || !strings.Contains(err.Error(), "503") {
		t.Errorf("discovery failed: %v", err)
	}
	if len(r.inCalls) != 0 {
		t.Error("the inbound readiness ran without bins")
	}
}

func TestCleanupChecksTheFloorOutWhetherOrNotPrepareCompleted(t *testing.T) {
	r := newRig(t, nil)
	r.fs.shutErr = errors.New("check out RDT1-PICK-01: 500")
	r.fs.findings = []string{"a task of nobody left claimed"}
	r.fs.stats = floor.Stats{ForeignWorked: 2}
	err := r.b.cleanup(context.Background())
	if r.fs.shutdowns != 1 {
		t.Errorf("%d shutdowns", r.fs.shutdowns)
	}
	if err == nil || !strings.Contains(err.Error(), "RDT1-PICK-01") {
		t.Errorf("the shutdown error was swallowed: %v", err)
	}
	if len(r.logged) < 2 {
		t.Errorf("floor stats and findings were not logged: %v", r.logged)
	}
}

func TestCrewIsSizedByTheParallelism(t *testing.T) {
	for parallel, want := range map[int]floor.Crew{
		-3: {Pickers: 1, Packers: 1}, 0: {Pickers: 1, Packers: 1}, 1: {Pickers: 1, Packers: 1},
		2: {Pickers: 2, Packers: 1}, 4: {Pickers: 4, Packers: 2}, 7: {Pickers: 7, Packers: 3},
	} {
		if got := crewFor(parallel); got != want {
			t.Errorf("parallel %d: crew %+v, want %+v", parallel, got, want)
		}
	}
}

func TestStorageBinsAreTheActiveStorageSlotsSortedAndCapped(t *testing.T) {
	var items []any
	for i := 40; i >= 1; i-- {
		status := "Active"
		if i%10 == 0 {
			status = "Retired"
		}
		items = append(items, map[string]any{"locationCode": "AUS3-STO-" + pad(i), "status": status})
	}
	items = append(items, map[string]any{"locationCode": "", "status": "Active"})
	f := newFakeCluster(t, map[string]http.HandlerFunc{"GET facility-layout/sites/AUS3/locations": func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("role") != "Storage" {
			problem(w, 400, "bad-role")
			return
		}
		respondJSON(page(items...))(w, r)
	}})
	got, err := storageBins(context.Background(), f.client(), "AUS3")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != stowBinCandidates || got[0] != "AUS3-STO-01" || got[1] != "AUS3-STO-02" {
		t.Errorf("bins %v", got)
	}
	for _, c := range got {
		if c == "AUS3-STO-10" || c == "AUS3-STO-20" || c == "" {
			t.Errorf("an unusable slot %q was offered", c)
		}
	}
	down := newFakeCluster(t, map[string]http.HandlerFunc{"GET facility-layout/sites/AUS3/locations": func(w http.ResponseWriter, _ *http.Request) { problem(w, 500, "boom") }})
	if _, err := storageBins(context.Background(), down.client(), "AUS3"); err == nil {
		t.Error("a failed read of the slots read as no slot")
	}
}

func pad(i int) string {
	if i < 10 {
		return "0" + string(rune('0'+i))
	}
	return string(rune('0'+i/10)) + string(rune('0'+i%10))
}

// ---- newPhases against a fake gateway ----

// phasesRig is the real newPhases on a gateway that knows the dictionary and the nodes and answers
// 404 to everything else.
func phasesRig(t *testing.T, mod func(*wireConfig), routes map[string]http.HandlerFunc) (*fakeCluster, wireConfig) {
	t.Helper()
	skus, err := fixtureSKUs("../../fixtures/storefront/catalogue.json")
	if err != nil {
		t.Fatal(err)
	}
	var seed, catalog []string
	for i, sku := range skus[:3] {
		title := "title " + sku
		seed = append(seed, `{"sku":"`+sku+`","title":"`+title+`"}`)
		catalog = append(catalog, `{"catalogItemId":"ci-`+string(rune('a'+i))+`","title":"`+title+`","state":"ACTIVE"}`)
	}
	dir := t.TempDir()
	network := filepath.Join(dir, "seed.json")
	if err := os.WriteFile(network, []byte(`{"catalog":[`+strings.Join(seed, ",")+`]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	rs := nodeRoutes()
	rs["GET offer-availability/catalog"] = func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"items":[` + strings.Join(catalog, ",") + `]}`))
	}
	for k, v := range routes {
		rs[k] = v
	}
	f := newFakeCluster(t, rs)
	cfg := wireConfig{
		Client: f.client(), RunID: "RDT2", Building: "AUS3", NetworkSite: "site-1", VendorID: "V-WS",
		FixtureDir: "../../fixtures/storefront", NetworkData: network, Parallel: 2,
	}
	if mod != nil {
		mod(&cfg)
	}
	return f, cfg
}

func TestNewPhasesPutsEveryActorBehindItsInterface(t *testing.T) {
	_, cfg := phasesRig(t, nil, map[string]http.HandlerFunc{
		"GET storefront/products": respondJSON(page(
			map[string]any{"catalogItemId": "ci-a", "purchasable": true},
			map[string]any{"catalogItemId": "ci-c", "purchasable": true},
			map[string]any{"catalogItemId": "ci-other", "purchasable": true},
		)),
	})
	p, cleanup, err := newPhases(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	for name, nilIface := range map[string]bool{
		"Catalogue": p.Catalogue == nil, "Shopper": p.Shopper == nil, "Linker": p.Linker == nil, "Floor": p.Floor == nil,
		"Shipper": p.Shipper == nil, "Carrier": p.Carrier == nil, "Restocker": p.Restocker == nil, "Evidence": p.Evidence == nil,
		"Calls": p.Calls == nil, "Suspensions": p.Suspensions == nil, "Prepare": p.Prepare == nil, "Cleanup": p.Cleanup == nil,
	} {
		if nilIface {
			t.Errorf("phase %s is not wired", name)
		}
	}
	// the dictionary reached the catalogue: the storefront's two known products are candidates, the stranger is not
	items, err := p.Catalogue.Items(context.Background())
	if err != nil || len(items) != 2 || items[0].CatalogItemID != "ci-a" || items[1].CatalogItemID != "ci-c" || items[0].SKU == "" || items[1].SKU == "" {
		t.Errorf("candidates %+v (%v)", items, err)
	}
	// the day's call statistics are the client's, so every actor's calls are counted
	if p.Calls.Count5xx() != 0 || len(p.Calls.Lines()) == 0 {
		t.Errorf("calls: %d 5xx, %d route(s) recorded", p.Calls.Count5xx(), len(p.Calls.Lines()))
	}
}

func TestNewPhasesRefusesWhatItCannotBuildBeforeTouchingTheCluster(t *testing.T) {
	cases := map[string]struct {
		mod    func(*wireConfig)
		routes map[string]http.HandlerFunc
		want   string
	}{
		"no client":                {func(c *wireConfig) { c.Client = nil }, nil, "no client"},
		"fixtures missing":         {func(c *wireConfig) { c.FixtureDir = t.TempDir() }, nil, "fulfilment site fixture"},
		"the building disagrees":   {func(c *wireConfig) { c.Building = "BOS7" }, nil, "-building BOS7"},
		"the network site differs": {func(c *wireConfig) { c.NetworkSite = "site-2" }, nil, "-network-site site-2"},
		"network data missing":     {func(c *wireConfig) { c.NetworkData = filepath.Join(t.TempDir(), "gone.json") }, nil, "retail-network data file"},
		"the catalog cannot be read": {nil, map[string]http.HandlerFunc{
			"GET offer-availability/catalog": func(w http.ResponseWriter, _ *http.Request) { problem(w, 503, "unavailable") },
		}, "offer-availability GET /catalog"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			f, cfg := phasesRig(t, tc.mod, tc.routes)
			_, _, err := newPhases(context.Background(), cfg)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err %v, want it to mention %q", err, tc.want)
			}
			if w := f.writes(); len(w) != 0 {
				t.Errorf("building the phases wrote: %v", w)
			}
		})
	}
}

func TestRealPrepareWritesNothingWithoutProvisionAndNeverTheCPTSchedule(t *testing.T) {
	for _, provision := range []bool{false, true} {
		name := map[bool]string{false: "default", true: "-provision"}[provision]
		t.Run(name, func(t *testing.T) {
			f, cfg := phasesRig(t, func(c *wireConfig) { c.Provision = provision; c.Doors = []string{"AUS3-OB-DOOR-1-1-1-1"} }, nil)
			p, cleanup, err := newPhases(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer cleanup()
			err = p.Prepare(context.Background())
			if err == nil {
				t.Fatal("the building was ready on a gateway that knows nothing of it")
			}
			for _, w := range f.writes() {
				if strings.HasPrefix(w, "PUT process-path-management/") {
					t.Errorf("the CPT schedule was written: %s", w)
				}
				if strings.Contains(w, "/stations") {
					t.Errorf("stations were installed on a building that is not ready: %s", w)
				}
			}
			if !provision && len(f.writes()) != 0 {
				t.Errorf("a run without -provision wrote: %v", f.writes())
			}
			if provision && len(f.writes()) == 0 {
				t.Error("-provision wrote nothing: it did not reach the readiness step")
			}
			if err := p.Cleanup(context.Background()); err != nil {
				t.Errorf("cleanup of a floor that never opened: %v", err)
			}
		})
	}
}
