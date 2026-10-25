package main

// Step definitions for features/phase5_a.feature: the Phase 5 part A scenarios
// (seller scorecard, labor standards per handling unit, FULL_CASE / CROSS_DOCK
// process paths, pack-aware charge estimate). Black-box and REST-only through
// Kong (http://localhost:8000/api/<context>). The seller scorecard scenarios reuse
// the seller_inbound step definitions for the whole shipment flow.

import (
	"fmt"
	"math"
	"net/http"
	"strings"

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
