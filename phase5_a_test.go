package main

// Step definitions for features/phase5_a.feature: the Phase 5 part A scenarios
// (seller scorecard, labor standards per handling unit, FULL_CASE / CROSS_DOCK
// process paths, pack-aware charge estimate). Black-box and REST-only through
// Kong (http://localhost:8000/api/<context>). The seller scorecard scenarios reuse
// the seller_inbound step definitions for the whole shipment flow.

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/cucumber/godog"
)

var (
	laborPerformanceBaseURL = envOrDefault("LABOR_PERFORMANCE_BASE_URL", "http://localhost:8000/api/labor-performance")
	processPathMgmtBaseURL  = envOrDefault("PROCESS_PATH_BASE_URL", "http://localhost:8000/api/process-path-management")
	workPlanningBaseURL     = envOrDefault("WORK_PLANNING_BASE_URL", "http://localhost:8000/api/wes-work-planning")
)

// ---------------------------------------------------------------------
// 1. seller scorecard
// ---------------------------------------------------------------------

func (w *world) p5Scorecard(window string) (map[string]any, error) {
	if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/sellers/%s/scorecard?window=%s", sellerInboundBaseURL, w.sis().sellerID, window), nil); err != nil {
		return nil, err
	}
	if w.last.status != http.StatusOK {
		return nil, fmt.Errorf("GET scorecard window=%s = %d (%s)", window, w.last.status, w.last.body)
	}
	return w.last.json(), nil
}

func intField(m map[string]any, k string) (int, error) {
	f, ok := toFloat(m[k])
	if !ok || f != math.Trunc(f) {
		return 0, fmt.Errorf("%s = %v, want an integer", k, m[k])
	}
	return int(f), nil
}

// p5ScorecardCounts polls the 30d scorecard until the facts of the closed shipment are in.
func (w *world) p5ScorecardCounts(ships, reported, received int) error {
	return siEventually(func() error {
		sc, err := w.p5Scorecard("30d")
		if err != nil {
			return err
		}
		if sc["sellerId"] != w.sis().sellerID || sc["window"] != "30d" {
			return fmt.Errorf("scorecard identifies %v/%v: %s", sc["sellerId"], sc["window"], w.last.body)
		}
		for k, want := range map[string]int{"shipmentsReceived": ships, "cartonsReported": reported, "cartonsReceived": received} {
			got, err := intField(sc, k)
			if err != nil {
				return err
			}
			if got != want {
				return fmt.Errorf("%s = %d, want %d (%s)", k, got, want, w.last.body)
			}
		}
		return nil
	})
}

func (w *world) p5AccuracyRule() error {
	sc, err := w.p5Scorecard("30d")
	if err != nil {
		return err
	}
	rep, _ := intField(sc, "cartonsReported")
	rec, _ := intField(sc, "cartonsReceived")
	rate, ok := toFloat(sc["receiptAccuracyRate"])
	if !ok {
		return fmt.Errorf("receiptAccuracyRate absent although %d cartons were reported: %s", rep, w.last.body)
	}
	want := math.Round(float64(rec)/float64(rep)*10000) / 10000
	if math.Abs(rate-want) > 0.00005 {
		return fmt.Errorf("receiptAccuracyRate = %v, want cartonsReceived/cartonsReported = %d/%d = %v (%s)", rate, rec, rep, want, w.last.body)
	}
	return nil
}

func (w *world) p5DiscrepanciesAndTier() error {
	sc, err := w.p5Scorecard("30d")
	if err != nil {
		return err
	}
	kinds, _ := sc["discrepanciesByKind"].(map[string]any)
	if len(kinds) == 0 {
		return fmt.Errorf("discrepanciesByKind is empty although a carton was short and one missing: %s", w.last.body)
	}
	total := 0
	for k, v := range kinds {
		n, ok := toFloat(v)
		if !ok || n < 1 {
			return fmt.Errorf("discrepanciesByKind[%s] = %v, want an integer >= 1", k, v)
		}
		total += int(n)
	}
	for _, k := range []string{"Short", "MissingCarton"} {
		if _, ok := kinds[k]; !ok {
			return fmt.Errorf("discrepanciesByKind lacks %q (the short / missing carton): %s", k, w.last.body)
		}
	}
	shipments, _ := intField(sc, "shipmentsReceived")
	viol, err := intField(sc, "complianceViolations")
	if err != nil {
		return err
	}
	rate, _ := toFloat(sc["receiptAccuracyRate"])
	dr, ok := toFloat(sc["discrepancyRatePerShipment"])
	if !ok {
		return fmt.Errorf("discrepancyRatePerShipment absent although %d shipment(s) closed: %s", shipments, w.last.body)
	}
	if want := math.Round(float64(total)/float64(shipments)*10000) / 10000; math.Abs(dr-want) > 0.00005 {
		return fmt.Errorf("discrepancyRatePerShipment = %v, want discrepancies/shipmentsReceived = %d/%d = %v", dr, total, shipments, want)
	}
	// The documented rule: the worst of three signals.
	want := "GOOD"
	switch {
	case rate < 0.98 || dr >= 0.5 || viol >= 1:
		want = "WATCH"
	}
	if rate < 0.90 || dr >= 2.0 || viol >= 5 {
		want = "AT_RISK"
	}
	if sc["complianceTier"] != want {
		return fmt.Errorf("complianceTier = %v, want %s for accuracy %v, discrepancy rate %v, %d violations (%s)", sc["complianceTier"], want, rate, dr, viol, w.last.body)
	}
	return nil
}

// p5SellerCanStillConfirm: the tier is informational, the seller is not suspended nor blocked.
func (w *world) p5SellerCanStillConfirm(qty int) error {
	if err := w.planDraftEventually("CASE_PACKED", qty); err != nil {
		return fmt.Errorf("seller with a scorecard can no longer plan: %w", err)
	}
	return w.planConfirmed()
}

func (w *world) p5EmptyScorecard(window string) error {
	sc, err := w.p5Scorecard(window)
	if err != nil {
		return err
	}
	if strings.Contains(string(w.last.body), "NaN") {
		return fmt.Errorf("scorecard body contains NaN: %s", w.last.body)
	}
	if sc["window"] != window || sc["sellerId"] != w.sis().sellerID || sc["windowStart"] == nil || sc["windowEnd"] == nil {
		return fmt.Errorf("empty scorecard header is wrong: %s", w.last.body)
	}
	for _, k := range []string{"shipmentsReceived", "cartonsReported", "cartonsReceived", "complianceViolations"} {
		if n, err := intField(sc, k); err != nil || n != 0 {
			return fmt.Errorf("%s = %v, want 0 (%s)", k, sc[k], w.last.body)
		}
	}
	for _, k := range []string{"receiptAccuracyRate", "discrepancyRatePerShipment"} {
		if _, present := sc[k]; present {
			return fmt.Errorf("%s = %v must be omitted when its denominator is zero (%s)", k, sc[k], w.last.body)
		}
	}
	if kinds, ok := sc["discrepanciesByKind"].(map[string]any); !ok || len(kinds) != 0 {
		return fmt.Errorf("discrepanciesByKind = %v, want an empty object (%s)", sc["discrepanciesByKind"], w.last.body)
	}
	if sc["complianceTier"] != "GOOD" {
		return fmt.Errorf("complianceTier = %v, want GOOD (%s)", sc["complianceTier"], w.last.body)
	}
	return nil
}

func (w *world) p5ScorecardRefused(seller, window string, status int, slug string) error {
	if seller == "" {
		seller = w.sis().sellerID
	}
	if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/sellers/%s/scorecard?window=%s", sellerInboundBaseURL, w.rs(seller), window), nil); err != nil {
		return err
	}
	if ct := w.last.header.Get("Content-Type"); !strings.HasPrefix(ct, "application/problem+json") {
		return fmt.Errorf("refusal content-type = %q, want application/problem+json (%s)", ct, w.last.body)
	}
	return w.siExpectProblem(status, slug)
}

// ---------------------------------------------------------------------
// wiring
// ---------------------------------------------------------------------

func registerPhase5ASteps(sc *godog.ScenarioContext, w *world) {
	registerPhase5APathSteps(sc, w)
	registerPhase5ALaborSteps(sc, w)
	// 1. seller scorecard
	sc.Step(`^the 30d scorecard of the seller eventually reports (\d+) shipment received with (\d+) cartons reported and (\d+) received$`, w.p5ScorecardCounts)
	sc.Step(`^the scorecard receiptAccuracyRate is cartonsReceived over cartonsReported$`, w.p5AccuracyRule)
	sc.Step(`^the scorecard has discrepanciesByKind for the short and the missing carton and a complianceTier following the documented rule$`, w.p5DiscrepanciesAndTier)
	sc.Step(`^the seller can still confirm a new CASE_PACKED plan of (\d+) eaches for the node$`, w.p5SellerCanStillConfirm)
	sc.Step(`^the (7d|30d) scorecard of the seller is the empty shape: zero counts, no rates, no NaN, empty discrepanciesByKind, tier GOOD$`, w.p5EmptyScorecard)
	sc.Step(`^the scorecard of unknown seller "([^"]*)" is refused with (\d+) problem "([^"]*)"$`,
		func(s string, st int, slug string) error { return w.p5ScorecardRefused(s, "30d", st, slug) })
	sc.Step(`^the scorecard of the seller with window "([^"]*)" is refused with (\d+) problem "([^"]*)"$`,
		func(win string, st int, slug string) error { return w.p5ScorecardRefused("", win, st, slug) })
}

// ---------------------------------------------------------------------
// 2. labor standards per handling unit
// ---------------------------------------------------------------------

type laborStdState struct {
	orig          map[string]any // the task-type-only PICK standard found at the start (nil: none)
	unitlessFrom  string         // effectiveFrom of the task-type-only standard this scenario defined
	firstCaseFrom string
	lastResolved  map[string]any
}

var p5Labor *laborStdState

func (w *world) p5ls() *laborStdState {
	if p5Labor == nil {
		p5Labor = &laborStdState{}
	}
	return p5Labor
}

// p5IsProblem checks the last response is a problem+json of status/slug. Some services
// answer the bare slug as `type`, labor-performance a URL ending in "/<slug>".
func (w *world) p5IsProblem(status int, slug string) error {
	got := w.last.json()
	typ, _ := got["type"].(string)
	if w.last.status != status || (typ != slug && !strings.HasSuffix(typ, "/"+slug)) {
		return fmt.Errorf("want %d problem %q, got %d (body: %s)", status, slug, w.last.status, w.last.body)
	}
	if ct := w.last.header.Get("Content-Type"); !strings.HasPrefix(ct, "application/problem+json") {
		return fmt.Errorf("refusal content-type = %q, want application/problem+json", ct)
	}
	return nil
}

func (w *world) p5LaborHealthy() error {
	if err := w.doJSON(http.MethodGet, laborPerformanceBaseURL+"/healthz", nil); err != nil {
		return err
	}
	if w.last.status != http.StatusOK {
		return fmt.Errorf("labor-performance /healthz = %d (%s)", w.last.status, w.last.body)
	}
	return nil
}

func (w *world) p5RememberPick() error {
	st := w.p5ls()
	if err := w.doJSON(http.MethodGet, laborPerformanceBaseURL+"/standards/PICK", nil); err != nil {
		return err
	}
	switch w.last.status {
	case http.StatusOK:
		st.orig = w.last.json()
	case http.StatusNotFound:
		st.orig = nil
	default:
		return fmt.Errorf("GET /standards/PICK = %d (%s)", w.last.status, w.last.body)
	}
	return nil
}

func (w *world) p5PostStandard(body map[string]any) error {
	return w.doJSON(http.MethodPost, laborPerformanceBaseURL+"/standards", body)
}

func (w *world) p5Defined(body map[string]any, wantUnit string) error {
	if err := w.p5PostStandard(body); err != nil {
		return err
	}
	if w.last.status != http.StatusCreated {
		return fmt.Errorf("POST /standards %v = %d, want 201 (%s)", body, w.last.status, w.last.body)
	}
	got := w.last.json()
	gotUnit, _ := got["handlingUnit"].(string)
	if got["taskType"] != body["taskType"] || gotUnit != wantUnit {
		return fmt.Errorf("defined standard answers taskType=%v handlingUnit=%q, want %v/%q (%s)", got["taskType"], gotUnit, body["taskType"], wantUnit, w.last.body)
	}
	if got["effectiveFrom"] == nil || got["effectiveTo"] != nil {
		return fmt.Errorf("a freshly defined standard must be open (effectiveFrom set, no effectiveTo): %s", w.last.body)
	}
	return nil
}

func (w *world) p5DefineUnitless(secs int) error {
	if err := w.p5Defined(map[string]any{"taskType": "PICK", "expectedSeconds": secs}, ""); err != nil {
		return err
	}
	w.p5ls().unitlessFrom, _ = w.last.json()["effectiveFrom"].(string)
	return nil
}

func (w *world) p5DefineUnit(secs int, unit string) error {
	if err := w.p5Defined(map[string]any{"taskType": "PICK", "handlingUnit": unit, "expectedSeconds": secs}, unit); err != nil {
		return err
	}
	if unit == "CASE" && w.p5ls().firstCaseFrom == "" {
		w.p5ls().firstCaseFrom, _ = w.last.json()["effectiveFrom"].(string)
	}
	return nil
}

func (w *world) p5DefineTeamLift(secs int, unit string, thr, tlSecs int) error {
	return w.p5Defined(map[string]any{"taskType": "PICK", "handlingUnit": unit, "expectedSeconds": secs,
		"teamLift": map[string]any{"thresholdG": thr, "expectedSeconds": tlSecs}}, unit)
}

func (w *world) p5Resolved(query string, secs int, declared string) error {
	url := laborPerformanceBaseURL + "/standards/PICK"
	if query != "" {
		url += "?handlingUnit=" + query
	}
	if err := w.doJSON(http.MethodGet, url, nil); err != nil {
		return err
	}
	if w.last.status != http.StatusOK {
		return fmt.Errorf("GET %s = %d (%s)", url, w.last.status, w.last.body)
	}
	got := w.last.json()
	w.p5ls().lastResolved = got
	unit, _ := got["handlingUnit"].(string)
	wantUnit := declared
	if declared == "no handling unit" {
		wantUnit = ""
	}
	es, _ := toFloat(got["expectedSeconds"])
	if int(es) != secs || unit != wantUnit || got["taskType"] != "PICK" {
		return fmt.Errorf("GET %s = %v seconds declared for %q, want %d seconds declared for %q (%s)", url, got["expectedSeconds"], unit, secs, wantUnit, w.last.body)
	}
	return nil
}

func (w *world) p5ResolvedWithout(secs int, declared string) error {
	return w.p5Resolved("", secs, declared)
}

func (w *world) p5ResolvedFor(unit string, secs int, declared string) error {
	return w.p5Resolved(unit, secs, declared)
}

func (w *world) p5NoTeamLift() error {
	if tl := w.p5ls().lastResolved["teamLift"]; tl != nil {
		return fmt.Errorf("standard unexpectedly carries a team lift: %v", tl)
	}
	return nil
}

func (w *world) p5TeamLift(thr, secs int) error {
	tl, _ := w.p5ls().lastResolved["teamLift"].(map[string]any)
	t, _ := toFloat(tl["thresholdG"])
	s, _ := toFloat(tl["expectedSeconds"])
	o, _ := toFloat(tl["operators"])
	if int(t) != thr || int(s) != secs || int(o) != 2 {
		return fmt.Errorf("teamLift = %v, want thresholdG %d, expectedSeconds %d, operators 2", tl, thr, secs)
	}
	return nil
}

func (w *world) p5UnitlessKept() error {
	want := w.p5ls().unitlessFrom
	if err := w.doJSON(http.MethodGet, laborPerformanceBaseURL+"/standards/PICK", nil); err != nil {
		return err
	}
	if got, _ := w.last.json()["effectiveFrom"].(string); got != want || w.last.json()["effectiveTo"] != nil {
		return fmt.Errorf("task-type-only standard effectiveFrom = %q (effectiveTo=%v), want it still open since %q: a unit definition closed it? (%s)", got, w.last.json()["effectiveTo"], want, w.last.body)
	}
	return nil
}

func (w *world) p5CaseNotOlder() error {
	got, _ := w.p5ls().lastResolved["effectiveFrom"].(string)
	if got == "" || got < w.p5ls().firstCaseFrom {
		return fmt.Errorf("revised CASE effectiveFrom %q is before the first CASE standard's %q", got, w.p5ls().firstCaseFrom)
	}
	return nil
}

func (w *world) p5TeamLiftRefused(secs int, unitDesc string, thr, tlSecs, status int, slug string) error {
	body := map[string]any{"taskType": "PICK", "expectedSeconds": secs,
		"teamLift": map[string]any{"thresholdG": thr, "expectedSeconds": tlSecs}}
	if unit := strings.TrimPrefix(unitDesc, "handling unit "); unit != unitDesc {
		body["handlingUnit"] = unit
	}
	if err := w.p5PostStandard(body); err != nil {
		return err
	}
	return w.p5IsProblem(status, slug)
}

func (w *world) p5UnknownUnitDefineRefused(secs int, unit string, status int, slug string) error {
	if err := w.p5PostStandard(map[string]any{"taskType": "PICK", "handlingUnit": unit, "expectedSeconds": secs}); err != nil {
		return err
	}
	return w.p5IsProblem(status, slug)
}

func (w *world) p5UnknownUnitReadRefused(unit string, status int, slug string) error {
	if err := w.doJSON(http.MethodGet, laborPerformanceBaseURL+"/standards/PICK?handlingUnit="+unit, nil); err != nil {
		return err
	}
	return w.p5IsProblem(status, slug)
}

// p5RestorePick re-declares the task-type-only PICK standard found at the start.
func (w *world) p5RestorePick() {
	st := p5Labor
	if st == nil || st.orig == nil {
		return
	}
	body := map[string]any{"taskType": "PICK", "expectedSeconds": st.orig["expectedSeconds"]}
	if v, ok := st.orig["travelComponentSeconds"]; ok && v != nil {
		body["travelComponentSeconds"] = v
	}
	_ = w.p5PostStandard(body)
}

func registerPhase5ALaborSteps(sc *godog.ScenarioContext, w *world) {
	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		p5Labor = nil
		return ctx, nil
	})
	sc.After(func(ctx context.Context, _ *godog.Scenario, err error) (context.Context, error) {
		w.p5RestorePick()
		return ctx, nil
	})
	sc.Step(`^labor-performance is healthy$`, w.p5LaborHealthy)
	sc.Step(`^the task-type-only PICK standard of labor-performance is remembered$`, w.p5RememberPick)
	sc.Step(`^a PICK standard of (\d+) expected seconds with no handling unit is defined$`, w.p5DefineUnitless)
	sc.Step(`^a PICK standard of (\d+) expected seconds for handling unit (EACH|CASE|PALLET) is defined$`, w.p5DefineUnit)
	sc.Step(`^a PICK standard of (\d+) expected seconds for handling unit (CASE) with a team lift above (\d+) g of (\d+) seconds is defined$`, w.p5DefineTeamLift)
	sc.Step(`^the PICK standard resolved without a handling unit is (\d+) seconds declared for (no handling unit)$`, w.p5ResolvedWithout)
	sc.Step(`^the PICK standard resolved for handling unit (EACH|CASE|PALLET) is (\d+) seconds declared for (no handling unit|EACH|CASE|PALLET)$`, w.p5ResolvedFor)
	sc.Step(`^that standard carries no team lift$`, w.p5NoTeamLift)
	sc.Step(`^that standard carries a team lift above (\d+) g of (\d+) seconds for 2 operators$`, w.p5TeamLift)
	sc.Step(`^the task-type-only PICK standard kept its effectiveFrom$`, w.p5UnitlessKept)
	sc.Step(`^the CASE standard is not older than the first one$`, w.p5CaseNotOlder)
	sc.Step(`^defining a PICK standard of (\d+) expected seconds (with no handling unit|for handling unit \w+) and a team lift above (\d+) g of (\d+) seconds is refused with (\d+) problem "([^"]*)"$`, w.p5TeamLiftRefused)
	sc.Step(`^defining a PICK standard of (\d+) expected seconds for handling unit (\w+) is refused with (\d+) problem "([^"]*)"$`, w.p5UnknownUnitDefineRefused)
	sc.Step(`^reading the PICK standard for handling unit (\w+) is refused with (\d+) problem "([^"]*)"$`, w.p5UnknownUnitReadRefused)
}

// ---------------------------------------------------------------------
// 3. case-flow process paths
// ---------------------------------------------------------------------

// The declared catalogue entries (process-path-management catalogue/paths.json, ADR 0038).
var p5CaseFlowPaths = map[string]map[string]any{
	"FULL_CASE": {"pathId": "FULL_CASE", "matchPrefix": "full-case", "direct": true, "requiredCapabilities": []string{"pick"},
		"cycleTimeP95": "1h30m", "eligibility": map[string]any{"requiredProductAttributes": []string{"fullCase"}}, "intake": []string{"CUSTOMER_ORDER"}},
	"CROSS_DOCK": {"pathId": "CROSS_DOCK", "matchPrefix": "cross-dock", "direct": true, "requiredCapabilities": []string{"dispatch"},
		"destinationLocationRole": "Shipping", "cycleTimeP95": "1h", "eligibility": map[string]any{"requiredProductAttributes": []string{"crossDock"}},
		"intake": []string{"CUSTOMER_ORDER", "STOCK_TRANSFER"}},
	"BREAK_CASE": {"pathId": "BREAK_CASE", "matchPrefix": "break", "direct": true, "requiredCapabilities": []string{"break"},
		"cycleTimeP95": "2h", "intake": []string{}},
}

func (w *world) p5PathsHealthy() error {
	if err := w.doJSON(http.MethodGet, processPathMgmtBaseURL+"/healthz", nil); err != nil {
		return err
	}
	if w.last.status != http.StatusOK {
		return fmt.Errorf("process-path-management /healthz = %d (%s)", w.last.status, w.last.body)
	}
	return nil
}

func (w *world) p5ApplyEntry(id string) (int, error) {
	if err := w.doJSON(http.MethodPost, processPathMgmtBaseURL+"/process-paths", p5CaseFlowPaths[id]); err != nil {
		return 0, err
	}
	return w.last.status, nil
}

func (w *world) p5ApplyCatalogue() error {
	for _, id := range []string{"FULL_CASE", "CROSS_DOCK", "BREAK_CASE"} {
		st, err := w.p5ApplyEntry(id)
		if err != nil {
			return err
		}
		if st != http.StatusCreated && st != http.StatusConflict {
			return fmt.Errorf("applying %s = %d, want 201 (new) or 409 (already seeded) (%s)", id, st, w.last.body)
		}
	}
	return nil
}

func (w *world) p5ReapplyRefused(id string, status int) error {
	st, err := w.p5ApplyEntry(id)
	if err != nil {
		return err
	}
	if st != status {
		return fmt.Errorf("re-applying %s = %d, want %d (%s)", id, st, status, w.last.body)
	}
	return w.p5IsProblem(status, "path-already-exists")
}

func csvSet(s string) []string {
	if s == "" {
		return []string{}
	}
	return strings.Split(s, ",")
}

func strSlice(v any) []string {
	out := []string{}
	for _, x := range anySlice(v) {
		out = append(out, fmt.Sprint(x))
	}
	return out
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := map[string]int{}
	for _, x := range a {
		m[x]++
	}
	for _, x := range b {
		m[x]--
	}
	for _, n := range m {
		if n != 0 {
			return false
		}
	}
	return true
}

func (w *world) p5PathIs(id, prefix, caps, cycle, dest, attrs, intake string) error {
	if err := w.doJSON(http.MethodGet, processPathMgmtBaseURL+"/process-paths/"+id, nil); err != nil {
		return err
	}
	if w.last.status != http.StatusOK {
		return fmt.Errorf("GET /process-paths/%s = %d (%s)", id, w.last.status, w.last.body)
	}
	p := w.last.json()
	gotCycle, _ := p["cycleTimeP95"].(string)
	gd, e1 := time.ParseDuration(gotCycle)
	wd, e2 := time.ParseDuration(cycle)
	elig, _ := p["eligibility"].(map[string]any)
	gotDest, _ := p["destinationLocationRole"].(string)
	switch {
	case p["pathId"] != id || p["status"] != "ACTIVE" || p["direct"] != true:
		return fmt.Errorf("%s: pathId/status/direct = %v/%v/%v (%s)", id, p["pathId"], p["status"], p["direct"], w.last.body)
	case p["matchPrefix"] != prefix:
		return fmt.Errorf("%s: matchPrefix = %v, want %s", id, p["matchPrefix"], prefix)
	case !sameSet(strSlice(p["requiredCapabilities"]), csvSet(caps)):
		return fmt.Errorf("%s: requiredCapabilities = %v, want %s", id, p["requiredCapabilities"], caps)
	case e1 != nil || e2 != nil || gd != wd:
		return fmt.Errorf("%s: cycleTimeP95 = %q, want %s", id, gotCycle, cycle)
	case gotDest != dest:
		return fmt.Errorf("%s: destinationLocationRole = %q, want %q", id, gotDest, dest)
	case !sameSet(strSlice(elig["requiredProductAttributes"]), csvSet(attrs)):
		return fmt.Errorf("%s: eligibility.requiredProductAttributes = %v, want %q", id, elig["requiredProductAttributes"], attrs)
	case !sameSet(strSlice(p["intake"]), csvSet(intake)):
		return fmt.Errorf("%s: intake = %v, want %q", id, p["intake"], intake)
	}
	return nil
}

func (w *world) p5ActivePaths() ([]map[string]any, error) {
	if err := w.doJSON(http.MethodGet, processPathMgmtBaseURL+"/process-paths", nil); err != nil {
		return nil, err
	}
	if w.last.status != http.StatusOK {
		return nil, fmt.Errorf("GET /process-paths = %d (%s)", w.last.status, w.last.body)
	}
	var out []map[string]any
	for _, x := range anySlice(jsonAny(w.last.body)) {
		if m, ok := x.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out, nil
}

func (w *world) p5ListingHasCaseFlowBesideOthers() error {
	ps, err := w.p5ActivePaths()
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, p := range ps {
		id, _ := p["pathId"].(string)
		seen[id] = true
		if p["status"] != "ACTIVE" {
			return fmt.Errorf("the default listing carries non-ACTIVE path %s", id)
		}
		if _, isCaseFlow := p5CaseFlowPaths[id]; !isCaseFlow {
			// An order / WMS path: not gated on the new case-flow attributes.
			elig, _ := p["eligibility"].(map[string]any)
			for _, a := range strSlice(elig["requiredProductAttributes"]) {
				if a == "fullCase" || a == "crossDock" {
					return fmt.Errorf("path %s is gated on %s: the case-flow gate leaked onto another path", id, a)
				}
			}
		}
	}
	for id := range p5CaseFlowPaths {
		if !seen[id] {
			return fmt.Errorf("active listing lacks %s", id)
		}
	}
	if len(ps) < 4 {
		return fmt.Errorf("listing has %d paths, want the case-flow three plus the existing ones", len(ps))
	}
	return nil
}

func (w *world) p5Select(desc string, match func(map[string]any) bool, wantExact bool, want ...string) error {
	ps, err := w.p5ActivePaths()
	if err != nil {
		return err
	}
	var got []string
	for _, p := range ps {
		if match(p) {
			got = append(got, fmt.Sprint(p["pathId"]))
		}
	}
	if wantExact && !sameSet(got, want) {
		return fmt.Errorf("active paths %s = %v, want exactly %v", desc, got, want)
	}
	for _, x := range want {
		found := false
		for _, g := range got {
			found = found || g == x
		}
		if !found {
			return fmt.Errorf("active paths %s = %v, want to include %s", desc, got, x)
		}
	}
	return nil
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

func (w *world) p5ByAttribute(attr, exact string) error {
	return w.p5Select("requiring attribute "+attr, func(p map[string]any) bool {
		elig, _ := p["eligibility"].(map[string]any)
		return contains(strSlice(elig["requiredProductAttributes"]), attr)
	}, true, exact)
}

func (w *world) p5ByCapability(c, exact string) error {
	return w.p5Select("requiring capability "+c, func(p map[string]any) bool {
		return contains(strSlice(p["requiredCapabilities"]), c)
	}, true, exact)
}

func (w *world) p5ByDest(role, id string) error {
	return w.p5Select("ending at "+role, func(p map[string]any) bool { return p["destinationLocationRole"] == role }, false, id)
}

func (w *world) p5ByIntake(src, a, b string) error {
	return w.p5Select("taking intake "+src, func(p map[string]any) bool { return contains(strSlice(p["intake"]), src) }, false, a, b)
}

func (w *world) p5InvalidDefinition(id, base, field, value string, status int, slug string) error {
	body := map[string]any{}
	for k, v := range p5CaseFlowPaths[base] {
		body[k] = v
	}
	body["pathId"] = w.rs(id)
	switch field {
	case "destination role":
		body["destinationLocationRole"] = value
	case "cycleTimeP95":
		body["cycleTimeP95"] = value
	case "matchPrefix":
		body["matchPrefix"] = value
	case "intake":
		body["intake"] = []string{value}
	}
	if err := w.doJSON(http.MethodPost, processPathMgmtBaseURL+"/process-paths", body); err != nil {
		return err
	}
	return w.p5IsProblem(status, slug)
}

func (w *world) p5PathAbsent(id string) error {
	if err := w.doJSON(http.MethodGet, processPathMgmtBaseURL+"/process-paths/"+w.rs(id), nil); err != nil {
		return err
	}
	if w.last.status != http.StatusNotFound {
		return fmt.Errorf("GET %s = %d, want 404: a refused definition stored something (%s)", id, w.last.status, w.last.body)
	}
	return nil
}

func registerPhase5APathSteps(sc *godog.ScenarioContext, w *world) {
	sc.Step(`^process-path-management is healthy$`, w.p5PathsHealthy)
	sc.Step(`^the declared case-flow catalogue entries FULL_CASE, CROSS_DOCK and BREAK_CASE are applied$`, w.p5ApplyCatalogue)
	sc.Step(`^process path "([^"]*)" is ACTIVE and direct with matchPrefix "([^"]*)", capabilities "([^"]*)", cycleTimeP95 (\S+), (?:no destination role|destination role "([^"]*)"), required product attributes "([^"]*)" and intake "([^"]*)"$`, w.p5PathIs)
	sc.Step(`^applying the declared entry "([^"]*)" again is refused with (\d+) \(path-already-exists\)$`, w.p5ReapplyRefused)
	sc.Step(`^the active process path listing has FULL_CASE, CROSS_DOCK and BREAK_CASE and every other path keeps its own definition$`, w.p5ListingHasCaseFlowBesideOthers)
	sc.Step(`^the active paths requiring product attribute "([^"]*)" are exactly "([^"]*)"$`, w.p5ByAttribute)
	sc.Step(`^the active paths ending at destination role "([^"]*)" include "([^"]*)"$`, w.p5ByDest)
	sc.Step(`^the active paths requiring capability "([^"]*)" are exactly "([^"]*)"$`, w.p5ByCapability)
	sc.Step(`^the active paths taking intake "([^"]*)" include "([^"]*)" and "([^"]*)"$`, w.p5ByIntake)
	sc.Step(`^defining path "([^"]*)" like (FULL_CASE|CROSS_DOCK) but with (destination role|cycleTimeP95|matchPrefix|intake) "([^"]*)" is refused with (\d+) problem "([^"]*)"$`, w.p5InvalidDefinition)
	sc.Step(`^process path "([^"]*)" does not exist$`, w.p5PathAbsent)
}

func jsonAny(b []byte) any {
	var v any
	_ = json.Unmarshal(b, &v)
	return v
}
