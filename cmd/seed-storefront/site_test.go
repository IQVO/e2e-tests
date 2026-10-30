package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

const siteFixturePath = "fixtures/storefront/fulfilment-site.json"

// TestSiteFixtureAgreesWithTheNetworkNodeProfile: the CPT schedule declared for site-1 in
// process-path-management is the network node profile's (warehouse-infra
// scripts/data/retail-network-seed.json): same timezone, same cutoff local times. Skipped only
// when the sibling warehouse-infra checkout is absent.
func TestSiteFixtureAgreesWithTheNetworkNodeProfile(t *testing.T) {
	site, err := LoadSiteFixture(filepath.Join(repoRoot, siteFixturePath))
	if err != nil {
		t.Fatal(err)
	}
	if problems := site.Validate(); len(problems) > 0 {
		t.Fatalf("%d problems:\n%v", len(problems), problems)
	}
	all := []string{"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"}
	want := []Cutoff{
		{CptID: "aus-1100", LocalTime: "11:00", DaysOfWeek: all, ShipMethod: "ground", EligiblePathIDs: []string{"PICK"}},
		{CptID: "aus-1700", LocalTime: "17:00", DaysOfWeek: all, ShipMethod: "ground", EligiblePathIDs: []string{"PICK"}},
	}
	if site.SiteID != "site-1" || site.Timezone != "America/Chicago" || !reflect.DeepEqual(site.Cutoffs, want) {
		t.Fatalf("fulfilment-site fixture: %+v", site)
	}

	path := siblingNetworkData()
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Skipf("sibling warehouse-infra data file %s is absent (single-repo checkout, e.g. CI): the node-profile check needs it; set STOREFRONT_NETWORK_DATA to point at it", path)
	}
	node, err := LoadNodeProfile(path, site.SiteID)
	if err != nil {
		t.Fatal(err)
	}
	if site.Timezone != node.Timezone || !reflect.DeepEqual(site.cutoffTimes(), node.CutoffTimes) {
		t.Fatalf("site-1: fixture %s %v, network node profile %s %v (is warehouse-infra up to date?)", site.Timezone, site.cutoffTimes(), node.Timezone, node.CutoffTimes)
	}
	if problems := site.AgreeWith(node); len(problems) > 0 {
		t.Fatalf("AgreeWith: %v", problems)
	}
}

func TestSiteValidateAndAgreeWithCatchInconsistencies(t *testing.T) {
	s := &SiteFixture{SiteID: "", Timezone: "Mars/Olympus", Cutoffs: []Cutoff{
		{CptID: "a", LocalTime: "24:00", DaysOfWeek: []string{"Monday"}, ShipMethod: "", EligiblePathIDs: nil},
		{CptID: "a", LocalTime: "11:00", DaysOfWeek: nil, ShipMethod: "ground", EligiblePathIDs: []string{""}},
	}}
	problems := strings.Join(s.Validate(), "\n")
	for _, w := range []string{"siteId is empty", "Mars/Olympus", "cptId \"a\" is empty or duplicate", "24:00", "\"Monday\"", "shipMethod is empty", "no eligiblePathIds", "no daysOfWeek", "empty eligiblePathIds entry"} {
		if !strings.Contains(problems, w) {
			t.Errorf("no problem mentions %q in\n%s", w, problems)
		}
	}
	ok := &SiteFixture{SiteID: "site-1", Timezone: "UTC", Cutoffs: []Cutoff{{CptID: "x", LocalTime: "09:39"}}}
	got := strings.Join(ok.AgreeWith(&NodeProfile{VendorNodeCode: "site-1", Timezone: "America/Chicago", CutoffTimes: []string{"11:00", "17:00"}}), "\n")
	if !strings.Contains(got, `timezone "UTC"`) || !strings.Contains(got, "cutoffs [09:39]") {
		t.Fatalf("AgreeWith: %s", got)
	}
}

func TestSameScheduleIgnoresOrderButNotContent(t *testing.T) {
	site, err := LoadSiteFixture(filepath.Join(repoRoot, siteFixturePath))
	if err != nil {
		t.Fatal(err)
	}
	want := site.Request()
	reordered := &CPTSchedule{Timezone: want.Timezone}
	for i := len(want.Cutoffs) - 1; i >= 0; i-- {
		c := want.Cutoffs[i]
		c.DaysOfWeek = []string{"Sun", "Sat", "Fri", "Thu", "Wed", "Tue", "Mon"}
		reordered.Cutoffs = append(reordered.Cutoffs, c)
	}
	if !sameSchedule(reordered, want) {
		t.Fatal("the same cutoffs in another order are the same schedule")
	}
	for name, mutate := range map[string]func(s *CPTSchedule){
		"timezone":    func(s *CPTSchedule) { s.Timezone = "UTC" },
		"local time":  func(s *CPTSchedule) { s.Cutoffs[0].LocalTime = "11:01" },
		"cpt id":      func(s *CPTSchedule) { s.Cutoffs[0].CptID = "sim-1200" },
		"days":        func(s *CPTSchedule) { s.Cutoffs[0].DaysOfWeek = []string{"Mon"} },
		"ship method": func(s *CPTSchedule) { s.Cutoffs[0].ShipMethod = "same-day" },
		"paths":       func(s *CPTSchedule) { s.Cutoffs[0].EligiblePathIDs = []string{"PICK", "FULL_CASE"} },
		"one fewer":   func(s *CPTSchedule) { s.Cutoffs = s.Cutoffs[:1] },
	} {
		s := &CPTSchedule{Timezone: want.Timezone, Cutoffs: normalCutoffs(want.Cutoffs)}
		mutate(s)
		if sameSchedule(s, want) {
			t.Errorf("a different %s must differ", name)
		}
	}
	if sameSchedule(nil, want) {
		t.Error("no schedule differs")
	}
}

// TestCPTSchedulePutOnlyWhenItDiffers: a different schedule (the warehouse-day leftover) or none
// at all is one PUT; an equal one (whatever its order) is none, and so is every rerun.
func TestCPTSchedulePutOnlyWhenItDiffers(t *testing.T) {
	in := loadInputs(t)
	site, err := LoadSiteFixture(filepath.Join(repoRoot, siteFixturePath))
	if err != nil {
		t.Fatal(err)
	}
	equalReordered := &CPTSchedule{SiteID: "site-1", Timezone: site.Timezone}
	for i := len(site.Cutoffs) - 1; i >= 0; i-- {
		equalReordered.Cutoffs = append(equalReordered.Cutoffs, site.Cutoffs[i])
	}
	for _, tc := range []struct {
		name     string
		current  *CPTSchedule
		wantPuts int
	}{
		{"warehouse-day leftover", simLeftoverSchedule(), 1},
		{"no schedule (404)", nil, 1},
		{"equal, other order", equalReordered, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, _ := clusterLike(in)
			delete(f.cpt, "site-1")
			if tc.current != nil {
				f.cpt["site-1"] = tc.current
			}
			var out bytes.Buffer
			res, err := run(t, context.Background(), config("", in, &out), f)
			if err != nil {
				t.Fatalf("%v\n%s", err, out.String())
			}
			if f.puts != tc.wantPuts || res.WritesByKey["cpt-schedule"] != tc.wantPuts {
				t.Fatalf("%d PUTs reached the fake, %d counted; want %d\n%s", f.puts, res.WritesByKey["cpt-schedule"], tc.wantPuts, out.String())
			}
			if s := f.cpt["site-1"]; !sameSchedule(s, site.Request()) {
				t.Fatalf("site-1's schedule did not converge: %+v", s)
			}
			out.Reset()
			if _, err := run(t, context.Background(), config("", in, &out), f); err != nil || f.puts != tc.wantPuts {
				t.Fatalf("rerun: err %v, %d PUTs (want still %d)\n%s", err, f.puts, tc.wantPuts, out.String())
			}
		})
	}
}

// TestCPTScheduleDisagreeingWithTheNetworkFailsBeforeAnyWrite: when the fixture and the network
// data file's node profile disagree, nothing is written anywhere.
func TestCPTScheduleDisagreeingWithTheNetworkFailsBeforeAnyWrite(t *testing.T) {
	in := loadInputs(t)
	raw, err := os.ReadFile(in.network)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	doc["fulfillmentNodes"] = []any{map[string]any{"vendorNodeCode": "site-1", "profile": map[string]any{
		"timezone": "America/Chicago", "cutoffs": []any{map[string]any{"localTime": "12:00"}, map[string]any{"localTime": "17:00"}},
	}}}
	raw, _ = json.Marshal(doc)
	path := filepath.Join(t.TempDir(), "drifted-network.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	f, _ := clusterLike(in)
	var out bytes.Buffer
	cfg := config("", in, &out)
	cfg.NetworkDataPath = path
	_, err = run(t, context.Background(), cfg, f)
	if err == nil || !strings.Contains(err.Error(), "disagrees with the network's node profile") || !strings.Contains(err.Error(), "[12:00 17:00]") {
		t.Fatalf("want a disagreement naming the network cutoffs, got %v", err)
	}
	if f.posts != 0 || f.puts != 0 {
		t.Fatalf("nothing may be written: %d POSTs, %d PUTs", f.posts, f.puts)
	}

	// a network data file without the node fails the same way
	delete(doc, "fulfillmentNodes")
	raw, _ = json.Marshal(doc)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := run(t, context.Background(), cfg, f); err == nil || !strings.Contains(err.Error(), "no fulfillment node") || f.posts != 0 || f.puts != 0 {
		t.Fatalf("want 'no fulfillment node' and no write, got %v (%d POSTs, %d PUTs)", err, f.posts, f.puts)
	}
}

// TestInactiveEligiblePathFailsBeforeAnyWrite: a cutoff naming a path that is not Active would be
// a 422 from the service; CHECK catches it before anything is written.
func TestInactiveEligiblePathFailsBeforeAnyWrite(t *testing.T) {
	in := loadInputs(t)
	f, _ := clusterLike(in)
	f.paths["PICK"] = "DEACTIVATED"
	var out bytes.Buffer
	_, err := run(t, context.Background(), config("", in, &out), f)
	if err == nil || !strings.Contains(err.Error(), `process path "PICK"`) || f.posts != 0 || f.puts != 0 {
		t.Fatalf("want a PICK-not-Active CHECK failure without writes, got %v (%d POSTs, %d PUTs)", err, f.posts, f.puts)
	}
}

// TestOffersStillConstrainedWarnButDoNotFail: the pre-cutoff gap (network-fulfillment's
// next-feasible-cutoff defect) keeps THROUGHPUT_CONSTRAINED 0 after a correct schedule: VERIFY
// waits for a recompute after the write, then warns and names the fix; it does not fail on it.
func TestOffersStillConstrainedWarnButDoNotFail(t *testing.T) {
	in := loadInputs(t)
	f, _ := clusterLike(in)
	f.offersStuck = true
	var out bytes.Buffer
	cfg := config("", in, &out)
	cfg.OfferRecomputeInterval = 30 * time.Millisecond
	res, err := run(t, context.Background(), cfg, f)
	if err != nil {
		t.Fatalf("constrained offers must not fail the run: %v\n%s", err, out.String())
	}
	o := res.Verify.Offers
	if f.puts != 1 || o == nil || !o.Written || len(o.Constrained) != len(in.fx.Products) || o.Advertising != 0 {
		t.Fatalf("offers: %+v (%d PUTs)", o, f.puts)
	}
	if o.Elapsed >= cfg.VerifyTimeout {
		t.Fatalf("VERIFY must stop once the offers were recomputed after the write, not wait the whole timeout: %s", o.Elapsed)
	}
	s := out.String()
	if !strings.Contains(s, "WARNING") || !strings.Contains(s, "fix/next-feasible-cutoff") || !strings.Contains(s, "THROUGHPUT_CONSTRAINED 0") {
		t.Fatalf("the warning must name the constraint and the network-fulfillment fix:\n%s", s)
	}

	// a rerun writes nothing and reads the offers once
	out.Reset()
	res, err = run(t, context.Background(), cfg, f)
	if err != nil || f.puts != 1 || res.Verify.Offers.Written || !strings.Contains(out.String(), "already matched") {
		t.Fatalf("rerun: err %v, %d PUTs, offers %+v\n%s", err, f.puts, res.Verify.Offers, out.String())
	}
}

// TestWMSOnlyWritesTheCPTScheduleAndNothingElse: scripts/10 step 2 (-wms-only, before the
// network tier) PUTs the schedule once, sends no storefront POST, and a rerun writes nothing; the
// storefront tier that follows finds the schedule equal and skips it.
func TestWMSOnlyWritesTheCPTScheduleAndNothingElse(t *testing.T) {
	in := loadInputs(t)
	f, _ := clusterLike(in)
	f.tax = nil // a storefront difference the WMS step must leave alone
	var out bytes.Buffer
	cfg := config("", in, &out)
	cfg.WMSOnly, cfg.DryRun = true, true
	res, err := run(t, context.Background(), cfg, f)
	if err != nil || res.Plan.Writes() != 1 || f.puts != 0 || f.posts != 0 {
		t.Fatalf("wms-only dry run: err %v, %d planned, %d PUTs, %d POSTs\n%s", err, res.Plan.Writes(), f.puts, f.posts, out.String())
	}
	if strings.Contains(out.String(), "tax table") {
		t.Fatalf("a wms-only plan names only the CPT schedule:\n%s", out.String())
	}
	cfg.DryRun = false
	out.Reset()
	res, err = run(t, context.Background(), cfg, f)
	if err != nil || res.Writes != 1 || f.puts != 1 || f.posts != 0 || f.tax != nil {
		t.Fatalf("wms-only: err %v, %d writes, %d PUTs, %d POSTs, tax %v\n%s", err, res.Writes, f.puts, f.posts, f.tax, out.String())
	}
	if !strings.Contains(out.String(), "81/81 storefront SKUs advertise stock") {
		t.Fatalf("the WMS step must report the capability offers:\n%s", out.String())
	}
	out.Reset()
	if res, err = run(t, context.Background(), cfg, f); err != nil || res.Writes != 0 || f.puts != 1 {
		t.Fatalf("wms-only rerun must write nothing: err %v, %d writes, %d PUTs\n%s", err, res.Writes, f.puts, out.String())
	}
	cfg.WMSOnly = false
	out.Reset()
	if res, err = run(t, context.Background(), cfg, f); err != nil || res.WritesByKey["cpt-schedule"] != 0 || f.puts != 1 || res.Plan.CPTSchedule != nil {
		t.Fatalf("the storefront tier after the WMS step must skip the schedule: err %v, %v, %d PUTs\n%s", err, res.WritesByKey, f.puts, out.String())
	}
}

func putJSON(t *testing.T, url string, body any) (int, string) {
	t.Helper()
	var raw []byte
	switch b := body.(type) {
	case string:
		raw = []byte(b)
	default:
		raw, _ = json.Marshal(body)
	}
	req, _ := http.NewRequest(http.MethodPut, url, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// TestFakeEnforcesTheCPTScheduleContract pins the process-path-management slice of the fake.
func TestFakeEnforcesTheCPTScheduleContract(t *testing.T) {
	f := newFakeCluster()
	srv := f.start()
	defer srv.Close()
	u := srv.URL + "/api/process-path-management/sites/site-1/cpt-schedule"
	all := []string{"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"}
	good := func() DefineCPTScheduleRequest {
		return DefineCPTScheduleRequest{Timezone: "America/Chicago", Cutoffs: []Cutoff{{CptID: "aus-1100", LocalTime: "11:00", DaysOfWeek: all, ShipMethod: "ground", EligiblePathIDs: []string{"PICK"}}}}
	}
	if resp, err := http.Get(u); err != nil || resp.StatusCode != 404 {
		t.Fatalf("GET before any PUT: %v %v", resp, err)
	}
	steps := []struct {
		name   string
		mutate func(*DefineCPTScheduleRequest)
		raw    string
		want   int
		slug   string
	}{
		{name: "not JSON", raw: "{", want: 400, slug: "malformed-request"},
		{name: "unknown timezone", mutate: func(r *DefineCPTScheduleRequest) { r.Timezone = "Mars/Olympus" }, want: 422, slug: "invalid-timezone"},
		{name: "no cutoff", mutate: func(r *DefineCPTScheduleRequest) { r.Cutoffs = nil }, want: 422, slug: "no-cutoffs"},
		{name: "duplicate cptId", mutate: func(r *DefineCPTScheduleRequest) { r.Cutoffs = append(r.Cutoffs, r.Cutoffs[0]) }, want: 422, slug: "invalid-cpt-id"},
		{name: "bad local time", mutate: func(r *DefineCPTScheduleRequest) { r.Cutoffs[0].LocalTime = "11h" }, want: 422, slug: "invalid-local-time"},
		{name: "bad day", mutate: func(r *DefineCPTScheduleRequest) { r.Cutoffs[0].DaysOfWeek = []string{"Monday"} }, want: 422, slug: "invalid-days-of-week"},
		{name: "no ship method", mutate: func(r *DefineCPTScheduleRequest) { r.Cutoffs[0].ShipMethod = "" }, want: 422, slug: "invalid-ship-method"},
		{name: "deactivated path", mutate: func(r *DefineCPTScheduleRequest) { r.Cutoffs[0].EligiblePathIDs = []string{"OLD_PATH"} }, want: 422, slug: "unknown-process-path"},
		{name: "define", want: 200},
		{name: "identical: no-op", want: 200},
	}
	for _, s := range steps {
		var body any = s.raw
		if s.raw == "" {
			r := good()
			if s.mutate != nil {
				s.mutate(&r)
			}
			body = r
		}
		status, resp := putJSON(t, u, body)
		if status != s.want || (s.slug != "" && !strings.Contains(resp, "/problems/"+s.slug)) {
			t.Fatalf("%s: HTTP %d %s, want %d %s", s.name, status, resp, s.want, s.slug)
		}
	}
	if f.cptChanges != 1 || f.puts != len(steps) {
		t.Fatalf("an identical revision must change nothing: %d changes, %d PUTs", f.cptChanges, f.puts)
	}
	c := newClient(srv.URL+"/api", "", srv.Client(), false)
	got, err := observeCPT(context.Background(), c, "site-1")
	if err != nil || !sameSchedule(got, good()) {
		t.Fatalf("GET after PUT: %+v %v", got, err)
	}
	paths, err := readActivePaths(context.Background(), c)
	if err != nil || !paths["PICK"] || paths["OLD_PATH"] {
		t.Fatalf("active paths: %v %v", paths, err)
	}
	ro := newClient(srv.URL+"/api", "", srv.Client(), true)
	if err := applyCPT(context.Background(), ro, "site-1", good()); err != errDryRun {
		t.Fatalf("a read-only client must refuse the PUT, got %v", err)
	}
}
