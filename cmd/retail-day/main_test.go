package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
	"github.com/claudioed/warehouse-e2e/internal/retailday/day"
	"github.com/claudioed/warehouse-e2e/internal/sim"
	"github.com/claudioed/warehouse-e2e/internal/sim/shopper"
)

// An unreachable gateway is the failure the command exists to catch: every check must
// fail closed, each with a fix, and the exit code must say so, with or without -preflight.
const dead = "http://127.0.0.1:1"

func TestPreflightFailsClosedOnAnUnreachableCluster(t *testing.T) {
	var out, errb bytes.Buffer
	code := run([]string{"-preflight", "-gateway", dead + "/api", "-storefront", dead + "/api/storefront"}, &out, &errb)
	if code != 1 {
		t.Fatalf("exit %d, want 1; stderr %s", code, errb.String())
	}
	text := out.String()
	if strings.Contains(text, "PASS  health") {
		t.Errorf("a context behind a dead gateway passed:\n%s", text)
	}
	if !strings.Contains(text, "FAIL  health order-management") || !strings.Contains(text, "fix: kubectl -n warehouse-systems get pods") {
		t.Errorf("no failing health line with its fix:\n%s", text)
	}
	if !strings.Contains(errb.String(), "preflight FAILED") {
		t.Errorf("stderr: %s", errb.String())
	}
}

func TestADayIsNeverStartedOnAFailingPreflight(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run([]string{"-gateway", dead + "/api", "-storefront", dead}, &out, &errb); code != 1 {
		t.Fatalf("exit %d, want 1 (a failed preflight), stderr %s", code, errb.String())
	}
	if strings.Contains(errb.String(), "not built yet") {
		t.Errorf("reached the day: %s", errb.String())
	}
}

func TestJSONOutputIsMachineReadable(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run([]string{"-preflight", "-json", "-gateway", dead + "/api", "-storefront", dead}, &out, &errb); code != 1 {
		t.Fatalf("exit %d", code)
	}
	var doc struct {
		Checks []struct {
			Name string `json:"name"`
			OK   bool   `json:"ok"`
		} `json:"checks"`
		Failed int `json:"failed"`
	}
	if err := json.Unmarshal(out.Bytes(), &doc); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, out.String())
	}
	if doc.Failed == 0 || doc.Failed != len(doc.Checks) {
		t.Errorf("failed %d of %d checks; a dead gateway fails every one", doc.Failed, len(doc.Checks))
	}
}

func TestUsageErrorExitsTwo(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run([]string{"-no-such-flag"}, &out, &errb); code != 2 {
		t.Fatalf("exit %d, want 2", code)
	}
}

// ---- the day, played with stub phases ----

// harness swaps the seams of main.go for stubs and restores them at the end of the test: the preflight
// and the window read the cluster, buildPhases builds the real actors.
type harness struct {
	t         *testing.T
	world     *stubWorld
	items     []day.Item
	built     int
	gotCfg    wireConfig
	preflight int
	preClient *sim.Client
	buildErr  error
	released  int

	checks []retailday.Check
	window retailday.Window
	winErr error
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{t: t, items: stubItems(30), checks: []retailday.Check{{Name: "health a", OK: true, Detail: "ok"}}, window: retailday.Window{InWindow: true}}
	h.world = newStubWorld(h.items, 20)
	oldPre, oldWin, oldBuild := runPreflight, findWindow, buildPhases
	runPreflight = func(_ context.Context, cl *sim.Client, _ retailday.Config) []retailday.Check {
		h.preflight++
		h.preClient = cl
		return h.checks
	}
	findWindow = func(context.Context, *sim.Client, retailday.Config) (retailday.Window, error) {
		return h.window, h.winErr
	}
	buildPhases = func(_ context.Context, cfg wireConfig) (day.Phases, func(), error) {
		h.built++
		h.gotCfg = cfg
		if cfg.ActorLog != nil {
			cfg.ActorLog("stub", "actor chatter %d", 1)
		}
		if h.buildErr != nil {
			return day.Phases{}, nil, h.buildErr
		}
		return stubPhases(h.world, h.items), func() { h.released++ }, nil
	}
	t.Cleanup(func() { runPreflight, findWindow, buildPhases = oldPre, oldWin, oldBuild })
	return h
}

func (h *harness) run(args ...string) (code int, stdout, stderr string) {
	h.t.Helper()
	var out, errb bytes.Buffer
	code = run(args, &out, &errb)
	return code, out.String(), errb.String()
}

func TestAPassingStubDayExitsZeroAndPrintsTheReport(t *testing.T) {
	h := newHarness(t)
	code, out, errs := h.run()
	if code != 0 {
		t.Fatalf("exit %d\nstdout:\n%s\nstderr:\n%s", code, out, errs)
	}
	for _, want := range []string{"PASS  health a", "VERDICT: PASSED", "ORDERS (8 planned, 8 accounted for)", "NOT VERIFIED"} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout lacks %q:\n%s", want, out)
		}
	}
	if h.built != 1 || h.released != 1 {
		t.Errorf("the phases were built %d time(s) and released %d time(s)", h.built, h.released)
	}
	if h.world.preparedN != 1 || h.world.cleanedN != 1 {
		t.Errorf("prepare ran %d time(s), cleanup %d", h.world.preparedN, h.world.cleanedN)
	}
	if !strings.Contains(errs, "retail-day: plan: ok") {
		t.Errorf("no progress on stderr:\n%s", errs)
	}
}

func TestAFailingVerdictExitsOne(t *testing.T) {
	h := newHarness(t)
	h.world.skipCapture = true // the money of every delivered order is never captured
	code, out, errs := h.run()
	if code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	if !strings.Contains(out, "VERDICT: FAILED") || !strings.Contains(out, "money/captured") {
		t.Errorf("the report does not say why:\n%s", out)
	}
	if !strings.Contains(errs, "the day FAILED") {
		t.Errorf("stderr: %s", errs)
	}
}

func TestAnUnreadableDeadLetterCountFailsTheDayUnlessUnverifiedIsAllowed(t *testing.T) {
	// no published route counts dead letters: the collector answers known=false, so a day with the default
	// flags is a failed day and says why; -allow-unverified lists it instead
	cannot := func() (int, bool, error) { return 0, false, nil }
	h := newHarness(t)
	h.world.deadLetters = cannot
	code, out, _ := h.run()
	if code != 1 || !strings.Contains(out, "dead letters") {
		t.Errorf("default flags: exit %d\n%s", code, out)
	}
	h = newHarness(t)
	h.world.deadLetters = cannot
	code, out, errs := h.run("-allow-unverified")
	if code != 0 {
		t.Fatalf("-allow-unverified: exit %d\n%s\n%s", code, out, errs)
	}
	if !strings.Contains(out, "dead letters") || !strings.Contains(out, "-allow-unverified: listed, not failed") {
		t.Errorf("the NOT VERIFIED line is gone:\n%s", out)
	}
	// ... and it excuses nothing else
	h = newHarness(t)
	h.world.deadLetters, h.world.skipCapture = cannot, true
	if code, _, _ := h.run("-allow-unverified"); code != 1 {
		t.Errorf("-allow-unverified excused a failed rule: exit %d", code)
	}
}

func TestAFailedPreflightNeverBuildsTheDay(t *testing.T) {
	h := newHarness(t)
	h.checks = []retailday.Check{{Name: "health a", OK: true}, {Name: "CPT schedule of the building AUS3", OK: false, Detail: "404", Fix: "make provision-storefront"}}
	code, out, errs := h.run()
	if code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	if h.built != 0 {
		t.Error("the actors were built on a failed preflight")
	}
	if !strings.Contains(out, "FAIL  CPT schedule of the building AUS3") || !strings.Contains(out, "fix: make provision-storefront") || !strings.Contains(errs, "preflight FAILED (1 of 2 checks)") {
		t.Errorf("stdout:\n%s\nstderr:\n%s", out, errs)
	}
}

func TestAPhaseThatCannotBeBuiltExitsOneAndPrintsNoReport(t *testing.T) {
	h := newHarness(t)
	h.buildErr = errors.New("retail-network data file: no such file")
	code, out, errs := h.run()
	if code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	if !strings.Contains(errs, "cannot be built") || !strings.Contains(errs, "no such file") {
		t.Errorf("stderr: %s", errs)
	}
	if strings.Contains(out, "VERDICT") {
		t.Errorf("a report for a day that never started:\n%s", out)
	}
}

func TestUsageErrorsExitTwoBeforeAnythingIsRead(t *testing.T) {
	for _, args := range [][]string{
		{"-no-such-flag"},
		{"-seed", "not-a-number"},
		{"-seed", "1.5"},
		{"-shoppers", "many"},
		{"-shoppers", "4"},
		{"-shelf", "0"},
		{"-parallel", "0"},
		{"-link-timeout", "0s"},
		{"-link-timeout", "soon"},
		{"-run-id", "lower"},
		{"-run-id", "WAYTOOLONGAID99"},
		{"stray"},
	} {
		h := newHarness(t)
		code, _, errs := h.run(args...)
		if code != 2 {
			t.Errorf("%v: exit %d, want 2 (stderr %q)", args, code, errs)
		}
		if h.preflight != 0 || h.built != 0 {
			t.Errorf("%v: the cluster was read (preflight %d) or the actors built (%d) for a usage error", args, h.preflight, h.built)
		}
	}
}

func TestPreflightOnlyNeverBuildsTheDay(t *testing.T) {
	h := newHarness(t)
	code, out, _ := h.run("-preflight")
	if code != 0 || !strings.Contains(out, "1 checks, 0 failed") || h.built != 0 {
		t.Fatalf("exit %d built %d\n%s", code, h.built, out)
	}
	// the usage rules of the day do not apply to a preflight
	if code, _, _ := newHarness(t).run("-preflight", "-shoppers", "1"); code != 0 {
		t.Errorf("-preflight refused the day's flags: exit %d", code)
	}
	code, out, _ = h.run("-preflight", "-json")
	var doc struct {
		Checks []retailday.Check `json:"checks"`
		Failed int               `json:"failed"`
	}
	if err := json.Unmarshal([]byte(out), &doc); err != nil || code != 0 || len(doc.Checks) != 1 || doc.Failed != 0 {
		t.Errorf("-preflight -json: exit %d, %v, %+v\n%s", code, err, doc, out)
	}
}

func TestJSONPrintsTheReportAndNothingElseOnStdout(t *testing.T) {
	h := newHarness(t)
	code, out, errs := h.run("-json", "-seed", "42", "-shoppers", "6", "-shelf", "5")
	if code != 0 {
		t.Fatalf("exit %d\n%s\n%s", code, out, errs)
	}
	var rep day.Report
	dec := json.NewDecoder(strings.NewReader(out))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&rep); err != nil {
		t.Fatalf("stdout is not one report: %v\n%s", err, out)
	}
	if dec.More() {
		t.Fatalf("stdout holds more than the report:\n%s", out)
	}
	if rep.Verdict != day.VerdictPassed || rep.Seed != 42 || rep.Planned != 6 || len(rep.Orders) != 6 || len(rep.Shelf) != 5 || len(rep.Phases) != 11 {
		t.Errorf("report: verdict %s seed %d planned %d orders %d shelf %d phases %d", rep.Verdict, rep.Seed, rep.Planned, len(rep.Orders), len(rep.Shelf), len(rep.Phases))
	}
	if !strings.Contains(errs, "retail-day:") {
		t.Errorf("no progress on stderr")
	}
}

func TestJSONOfAFailedPreflightIsTheChecksDocumentAsBefore(t *testing.T) {
	h := newHarness(t)
	h.checks = []retailday.Check{{Name: "x", OK: false}}
	code, out, _ := h.run("-json")
	var doc struct {
		Checks []retailday.Check `json:"checks"`
		Failed int               `json:"failed"`
	}
	if err := json.Unmarshal([]byte(out), &doc); err != nil || code != 1 || doc.Failed != 1 {
		t.Errorf("exit %d err %v doc %+v\n%s", code, err, doc, out)
	}
}

func TestTextStdoutIsTheChecksThenTheReportAndNoProgress(t *testing.T) {
	h := newHarness(t)
	code, out, _ := h.run()
	if code != 0 {
		t.Fatalf("exit %d", code)
	}
	if strings.Contains(out, "retail-day:") {
		t.Errorf("progress leaked to stdout:\n%s", out)
	}
	if !strings.HasPrefix(out, "PASS  health a: ok\n1 checks, 0 failed\nretail day ") {
		t.Errorf("stdout does not start with the checks and then the report:\n%.200s", out)
	}
}

func TestTheDayIsPlayedFromTheSeedAndTheFlags(t *testing.T) {
	a, b := newHarness(t), newHarness(t)
	_, outA, _ := a.run("-json", "-seed", "7", "-shoppers", "9", "-shelf", "6")
	_, outB, _ := b.run("-json", "-seed", "7", "-shoppers", "9", "-shelf", "6")
	var ra, rb day.Report
	if err := json.Unmarshal([]byte(outA), &ra); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(outB), &rb); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ra.Shelf, rb.Shelf) || ra.Planned != 9 || len(ra.Shelf) != 6 {
		t.Errorf("the same seed gave shelves %v and %v", ra.Shelf, rb.Shelf)
	}
	c := newHarness(t)
	_, outC, _ := c.run("-json", "-seed", "8", "-shoppers", "9", "-shelf", "6")
	var rc day.Report
	if err := json.Unmarshal([]byte(outC), &rc); err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(ra.Shelf, rc.Shelf) {
		t.Errorf("another seed gave the same shelf %v", rc.Shelf)
	}
	d := newHarness(t)
	_, outD, _ := d.run("-json")
	var rd day.Report
	if err := json.Unmarshal([]byte(outD), &rd); err != nil || rd.Seed == 0 || rd.Planned != 8 || len(rd.Shelf) != 10 {
		t.Errorf("defaults: seed %d planned %d shelf %d (%v)", rd.Seed, rd.Planned, len(rd.Shelf), err)
	}
}

func TestTheFlagsReachTheWiring(t *testing.T) {
	h := newHarness(t)
	h.run("-json", "-gateway", "http://gw.test/api/", "-storefront", "http://sf.test/api/storefront", "-building", "ZZZ9", "-network-site", "site-9", "-vendor", "V-X",
		"-run-id", "RDFLAGS", "-parallel", "7", "-link-timeout", "3h", "-fixtures", "/f", "-network-data", "/n.json", "-doors", "D1, D2,", "-staging", "S1", "-stow-bins", "B1,B2")
	c := h.gotCfg
	if c.RunID != "RDFLAGS" || c.Parallel != 7 || c.LinkTimeout != 3*time.Hour || c.FixtureDir != "/f" || c.NetworkData != "/n.json" ||
		c.Building != "ZZZ9" || c.NetworkSite != "site-9" || c.VendorID != "V-X" {
		t.Errorf("config %+v", c)
	}
	if !reflect.DeepEqual(c.Doors, []string{"D1", "D2"}) || !reflect.DeepEqual(c.Staging, []string{"S1"}) || !reflect.DeepEqual(c.StowBins, []string{"B1", "B2"}) {
		t.Errorf("lists: doors %v staging %v bins %v", c.Doors, c.Staging, c.StowBins)
	}
	if c.Provision {
		t.Error("-provision is on without being asked")
	}
	if c.Client == nil || c.Client.Base["order-management"] != "http://gw.test/api/order-management" || c.Client.Base[retailday.BFF] != "http://sf.test/api/storefront" || c.Client.Base[shopper.Service] != "http://sf.test/api/storefront" {
		t.Errorf("client bases %v", c.Client.Base)
	}
	if c.Client.HTTP.Timeout < 30*time.Second {
		t.Errorf("the day's client times out after %s: the BFF's confirm budget is 15 s", c.Client.HTTP.Timeout)
	}
	if c.Client.Stats == nil {
		t.Error("no call statistics")
	}
	if c.Client == h.preClient || c.Client.Stats == h.preClient.Stats {
		t.Error("the day shares the preflight's client or call statistics: the report's 5xx count would include the preflight's calls")
	}
}

func TestTheRunIdAndTheParallelismReachTheRunner(t *testing.T) {
	h := newHarness(t)
	_, out, _ := h.run("-json", "-run-id", "RDXYZ9", "-parallel", "7")
	var rep day.Report
	if err := json.Unmarshal([]byte(out), &rep); err != nil {
		t.Fatal(err)
	}
	if rep.RunID != "RDXYZ9" {
		t.Errorf("report run id %q", rep.RunID)
	}
	if h.world.floorParallel != 7 {
		t.Errorf("the floor was given parallel %d, want 7", h.world.floorParallel)
	}
}

func TestActorChatterIsOnStderrOnlyWithV(t *testing.T) {
	h := newHarness(t)
	_, out, errs := h.run("-v")
	if !strings.Contains(errs, "retail-day: stub: actor chatter 1") || strings.Contains(out, "chatter") {
		t.Errorf("-v: stdout %.80q stderr %.200q", out, errs)
	}
	h = newHarness(t)
	if _, _, errs := h.run(); strings.Contains(errs, "chatter") || h.gotCfg.ActorLog != nil {
		t.Errorf("the actors chatter without -v: %q", errs)
	}
}

func TestDefaultsOfTheWiring(t *testing.T) {
	h := newHarness(t)
	h.run("-json")
	c := h.gotCfg
	if c.Provision || c.LinkTimeout != 2*time.Hour || c.Parallel != 4 || c.Building != "AUS3" || c.NetworkSite != "site-1" || c.VendorID != "V-WS" {
		t.Errorf("defaults %+v", c)
	}
	if !regexp.MustCompile(`^RD[A-Z0-9]{3,10}$`).MatchString(c.RunID) {
		t.Errorf("generated run id %q", c.RunID)
	}
	h = newHarness(t)
	h.run("-json", "-provision")
	if !h.gotCfg.Provision {
		t.Error("-provision did not reach the wiring")
	}
}

func TestExitCodeNeedsTheFactsAndTheVerdictToAgree(t *testing.T) {
	passing := func() day.Report {
		r := day.Report{Verdict: day.VerdictPassed, Planned: 1, Orders: []day.OrderReport{{Order: "o", Outcome: day.OutcomePass}}}
		for _, p := range []string{day.PhasePrepare, day.PhasePlan, day.PhaseShelf, day.PhaseShoppers, day.PhaseLink, day.PhaseFloor, day.PhaseShip, day.PhaseDeliver, day.PhaseRestock, day.PhaseEvidence, day.PhaseEstate} {
			r.Phases = append(r.Phases, day.PhaseStatus{Name: p, State: day.PhaseOK})
		}
		return r
	}
	if got := exitCode(passing(), io.Discard); got != 0 {
		t.Fatalf("a passing report exits %d", got)
	}
	for name, mod := range map[string]func(*day.Report){
		"verdict says FAILED":           func(r *day.Report) { r.Verdict = day.VerdictFailed },
		"verdict empty":                 func(r *day.Report) { r.Verdict = "" },
		"an order failed":               func(r *day.Report) { r.Orders[0].Outcome = day.OutcomeFail },
		"an order is unaccounted for":   func(r *day.Report) { r.Planned = 2 },
		"nothing planned":               func(r *day.Report) { r.Planned, r.Orders = 0, nil },
		"an estate rule failed":         func(r *day.Report) { r.Estate = []retailday.Finding{{Rule: "stock/conserved"}} },
		"a phase failed":                func(r *day.Report) { r.Phases[5].State = day.PhaseFailed },
		"a phase aborted":               func(r *day.Report) { r.Phases[7].State = day.PhaseAborted },
		"a phase is missing":            func(r *day.Report) { r.Phases = r.Phases[:10] },
		"a finding fails a passing row": func(r *day.Report) { r.Orders[0].Findings = []retailday.Finding{{Rule: "x"}} },
	} {
		r := passing()
		mod(&r)
		if got := exitCode(r, io.Discard); got != 1 {
			t.Errorf("%s: exit %d, want 1", name, got)
		}
	}
	if got := exitCode(day.Report{}, io.Discard); got != 1 {
		t.Errorf("the zero report exits %d", got)
	}
}

// ---- the release window ----

func TestOutsideTheWindowTheDayRefusesToStartUnlessToldHow(t *testing.T) {
	soon := retailday.Window{Wait: 40 * time.Minute, Start: stubEpoch}
	h := newHarness(t)
	h.window = soon
	code, _, errs := h.run()
	if code != 2 || h.built != 0 || !strings.Contains(errs, "refusing to start outside the release window") {
		t.Fatalf("exit %d built %d\n%s", code, h.built, errs)
	}

	h = newHarness(t)
	h.window = soon
	code, out, errs := h.run("-start-now", "-json")
	if code != 0 {
		t.Fatalf("-start-now: exit %d\n%s\n%s", code, out, errs)
	}
	var rep day.Report
	if err := json.Unmarshal([]byte(out), &rep); err != nil {
		t.Fatal(err)
	}
	if len(rep.Notes) != 1 || !strings.Contains(rep.Notes[0], "-start-now") || !strings.Contains(rep.Notes[0], "-link-timeout 2h0m0s") {
		t.Errorf("notes %v: the report must say the run started outside the window and what bounds the wait", rep.Notes)
	}

	// a release no sooner than the link wait ends could only fail (the boundary included)
	h = newHarness(t)
	h.window = retailday.Window{Wait: 2 * time.Hour}
	if code, _, _ := h.run("-start-now"); code != 2 || h.built != 0 {
		t.Errorf("a release exactly at -link-timeout: exit %d built %d", code, h.built)
	}
	h = newHarness(t)
	h.window = retailday.Window{Wait: 3 * time.Hour}
	if code, _, errs := h.run("-start-now"); code != 2 || h.built != 0 || !strings.Contains(errs, "-link-timeout") {
		t.Errorf("exit %d built %d\n%s", code, h.built, errs)
	}
	h = newHarness(t)
	h.window = retailday.Window{Wait: 3 * time.Hour}
	if code, _, errs := h.run("-start-now", "-link-timeout", "4h"); code != 0 {
		t.Errorf("-link-timeout 4h: exit %d\n%s", code, errs)
	}
}

func TestInsideTheWindowNeedsNoFlagAndLeavesNoNote(t *testing.T) {
	h := newHarness(t)
	_, out, _ := h.run("-json")
	var rep day.Report
	if err := json.Unmarshal([]byte(out), &rep); err != nil {
		t.Fatal(err)
	}
	if len(rep.Notes) != 0 {
		t.Errorf("notes %v", rep.Notes)
	}
}

func TestAWindowThatCannotBeComputedFailsTheRun(t *testing.T) {
	h := newHarness(t)
	h.winErr = errors.New("the building's schedule is unreadable")
	code, _, errs := h.run("-start-now")
	if code != 1 || h.built != 0 || !strings.Contains(errs, "cannot be computed") {
		t.Errorf("exit %d built %d\n%s", code, h.built, errs)
	}
}

func TestWaitForWindowSleepsThenPlaysTheDay(t *testing.T) {
	h := newHarness(t)
	calls := 0
	findWindow = func(context.Context, *sim.Client, retailday.Config) (retailday.Window, error) {
		calls++
		if calls == 1 {
			return retailday.Window{Wait: 20 * time.Millisecond}, nil
		}
		return retailday.Window{InWindow: true}, nil
	}
	code, out, errs := h.run("-wait-for-window", "-json")
	if code != 0 || calls != 2 {
		t.Fatalf("exit %d after %d window reads\n%s", code, calls, errs)
	}
	if !strings.Contains(errs, "waiting") || strings.Contains(out, "waiting") {
		t.Errorf("the wait was not announced on stderr only:\nstdout %q\nstderr %q", out, errs)
	}
}

func TestWaitForWindowRefusesAWaitLongerThanMaxWait(t *testing.T) {
	h := newHarness(t)
	h.window = retailday.Window{Wait: 10 * time.Hour}
	code, _, errs := h.run("-wait-for-window", "-max-wait", "1h")
	if code != 1 || h.built != 0 || !strings.Contains(errs, "-max-wait") {
		t.Errorf("exit %d built %d\n%s", code, h.built, errs)
	}
}
