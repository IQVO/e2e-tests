package main

// Step definitions for features/phase5_case_pick.feature (Phase 5, slotting-
// optimization ADR 0012: case-pick faces and the CASE pick unit).
//
// Black-box and REST-only through the Kong gateway, except the demand, which is
// the same injected SiteSkuDemandChanged as the other slotting scenarios
// (slotting_test.go header) and the SlotPlanApproved read from Kafka. The
// layout is built through facility-layout's REST: a zone's behavioural class IS
// its zoneCode ("a behavioural classification scoped to a Site"), so the
// case-pick zone is a zone registered with zoneCode CPK (slotting's
// CASE_PICK_ZONE_CODES) and the each-pick zone one with zoneCode FWD
// (FORWARD_ZONE_CODES); a slot's volume/weight limits are the optional
// capacityOverride of POST /locations (maxWeightKg, maxVolumeM3).
//
// The shared helpers (slots, plans, approval, velocity, the pack-level step of
// phase5_b_test.go) are reused; only what is specific to the pick unit is here.

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cucumber/godog"
)

const (
	// cpConvergeTimeout is how long slotting-optimization gets to see the
	// layout, product, pack-level and demand copies of a freshly built site
	// (four Kafka consumers, each with its own lag).
	cpConvergeTimeout = 120 * time.Second
	cpConvergePoll    = 2 * time.Second
)

// cpLimit is a slot's declared envelope.
type cpLimit struct{ kg, m3 float64 }

// cpRow is one expected assignment: slot is an exact code or "zone <CODE>".
type cpRow struct {
	sku, slot, unit string
	cap             int
}

type cpState struct {
	limits map[string]cpLimit // slot code -> declared limits
	rows   map[string][]cpRow // site|label -> the table the plan matched
}

func registerPhase5CasePickSteps(sc *godog.ScenarioContext, w *world) {
	st := &cpState{}
	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		*st = cpState{limits: map[string]cpLimit{}, rows: map[string][]cpRow{}}
		return ctx, nil
	})

	sc.Step(`^site "([^"]*)" has slot "([^"]*)" of location type "([^"]*)" limited to (\d+(?:\.\d+)?) kg and (\d+(?:\.\d+)?) m3 in facility-layout$`,
		func(site, code, typ string, kg, m3 float64) error {
			return st.slot(w, site, code, typ, &cpLimit{kg: kg, m3: m3})
		})
	sc.Step(`^site "([^"]*)" has slot "([^"]*)" of location type "([^"]*)" with its default limits in facility-layout$`,
		func(site, code, typ string) error { return st.slot(w, site, code, typ, nil) })
	sc.Step(`^order-management's demand projection states order lines of ((?:\d+)(?:(?:, | and )\d+)*) units of SKU "([^"]*)" at site "([^"]*)"$`,
		func(list, sku, site string) error {
			var units []int
			for _, f := range regexp.MustCompile(`\d+`).FindAllString(list, -1) {
				n, _ := strconv.Atoi(f)
				units = append(units, n)
			}
			return w.demandLinesAreStated(units, sku, site)
		})
	sc.Step(`^slotting-optimization eventually drafts plan "([^"]*)" for site "([^"]*)" with these pick units:$`,
		func(label, site string, tbl *godog.Table) error { return st.draftEventually(w, label, site, tbl) })
	sc.Step(`^plan "([^"]*)" of site "([^"]*)" read back by id carries the same pick units and cases capacities$`,
		func(label, site string) error { return st.planReadBack(w, label, site) })
	sc.Step(`^every CASE assignment of plan "([^"]*)" of site "([^"]*)" holds the whole cases of a (\d+)x(\d+)x(\d+) mm, (\d+) g case that its slot's volume and weight limits allow$`,
		func(label, site string, l, wd, h, g int) error {
			return st.capacityFormula(w, label, site, l, wd, h, g)
		})
	sc.Step(`^plan "([^"]*)" of site "([^"]*)" has exactly one Assign move per assigned SKU, to the slot it is assigned$`,
		func(label, site string) error { return st.assignMoves(w, label, site) })
	sc.Step(`^slotting-optimization's forward slots of site "([^"]*)" come from plan "([^"]*)" and carry its pick units and cases capacities$`,
		func(site, label string) error { return st.forwardSlots(w, site, label) })
	sc.Step(`^slotting-optimization published SlotPlanApproved for plan "([^"]*)" of site "([^"]*)" with pick_unit and cases_capacity on the CASE assignments only$`,
		func(label, site string) error { return st.approvedEvent(w, label, site) })
}

// slot registers the site, the zone named by the code's zone segment (so the
// zone's class is whatever the code says: CPK or FWD), its aisle and the slot,
// with an explicit capacityOverride when limits are given. The location type
// must be legal in the zone: facility-layout's placement rules allow only
// CartonFlow, Shelf and ToteWall in a FWD zone (a zone code without Allow
// rules, like CPK, takes any type).
func (st *cpState) slot(w *world, site, raw, locType string, limits *cpLimit) error {
	site, code := w.rs(site), w.rs(raw)
	parts := strings.Split(code, "-")
	if len(parts) != 7 || parts[0] != site {
		return fmt.Errorf("slot %q is not a 7-segment location code of site %s", code, site)
	}
	area, zone, aisle := parts[1], parts[2], parts[3]
	zoneID := strings.Join(parts[:3], "-")
	if err := w.ensureSite(site, "Case-pick E2E "+site); err != nil {
		return fmt.Errorf("site %s: %w", site, err)
	}
	if err := w.ensureZone(area, zone, site, "Ambient", false); err != nil {
		return fmt.Errorf("zone %s: %w", zoneID, err)
	}
	if err := w.ensureAisle(aisle, zoneID, 1, "TwoWay"); err != nil {
		return fmt.Errorf("aisle %s of zone %s: %w", aisle, zoneID, err)
	}
	body := map[string]any{"locationCode": code, "locationType": locType}
	if limits != nil {
		body["capacityOverride"] = map[string]any{"maxWeightKg": limits.kg, "maxVolumeM3": limits.m3}
	}
	if err := w.expectOK2xx(w.doJSON(http.MethodPost, facilityBaseURL+"/locations", body)); err != nil {
		return fmt.Errorf("slot %s (%s, limits %v): %w", code, locType, limits, err)
	}
	if limits != nil {
		st.limits[code] = *limits
	}
	return nil
}

func (st *cpState) rowsFrom(w *world, tbl *godog.Table) ([]cpRow, error) {
	var out []cpRow
	for i, row := range tbl.Rows {
		if i == 0 {
			continue
		}
		if len(row.Cells) != 4 {
			return nil, fmt.Errorf("pick-unit table row %d has %d cells, want 4 (sku, slot, pickUnit, casesCapacity)", i, len(row.Cells))
		}
		r := cpRow{sku: w.rs(row.Cells[0].Value), slot: w.rs(row.Cells[1].Value), unit: row.Cells[2].Value}
		if c := row.Cells[3].Value; c != "" {
			n, err := strconv.Atoi(c)
			if err != nil {
				return nil, fmt.Errorf("pick-unit table row %d: casesCapacity %q is not a number", i, c)
			}
			r.cap = n
		}
		if r.unit != "CASE" && r.unit != "EACH" {
			return nil, fmt.Errorf("pick-unit table row %d: pickUnit %q is neither CASE nor EACH", i, r.unit)
		}
		if (r.unit == "CASE") != (r.cap > 0) {
			return nil, fmt.Errorf("pick-unit table row %d: only a CASE row carries a casesCapacity", i)
		}
		out = append(out, r)
	}
	return out, nil
}

func zoneOf(slot string) string {
	if p := strings.Split(slot, "-"); len(p) == 7 {
		return p[2]
	}
	return ""
}

func assignmentsOf(p map[string]any) map[string]map[string]any {
	out := map[string]map[string]any{}
	as, _ := p["assignments"].([]any)
	for _, a := range as {
		m, _ := a.(map[string]any)
		sku, _ := m["sku"].(string)
		out[sku] = m
	}
	return out
}

// matchRow checks one assignment object against an expected row. Used for the
// plan, the plan read back and the forward-slot map, which share the shape.
func matchRow(a map[string]any, r cpRow, what string) error {
	slot, _ := a["slot"].(string)
	if strings.HasPrefix(r.slot, "zone ") {
		if z := strings.TrimPrefix(r.slot, "zone "); zoneOf(slot) != z {
			return fmt.Errorf("%s: %s is in slot %q (zone %q), want a slot of zone %s", what, r.sku, slot, zoneOf(slot), z)
		}
	} else if slot != r.slot {
		return fmt.Errorf("%s: %s is in slot %q, want %q", what, r.sku, slot, r.slot)
	}
	unit, hasUnit := a["pickUnit"]
	capV, hasCap := a["casesCapacity"]
	if r.unit == "CASE" {
		if unit != "CASE" {
			return fmt.Errorf("%s: %s has pickUnit %v, want CASE (assignment: %v)", what, r.sku, unit, a)
		}
		if got, ok := intOf(capV); !hasCap || !ok || got != r.cap {
			return fmt.Errorf("%s: %s has casesCapacity %v, want %d (assignment: %v)", what, r.sku, capV, r.cap, a)
		}
		return nil
	}
	if hasUnit && unit != "EACH" {
		return fmt.Errorf("%s: %s has pickUnit %v, want EACH or absent (assignment: %v)", what, r.sku, unit, a)
	}
	if hasCap {
		return fmt.Errorf("%s: %s is an each assignment but carries casesCapacity %v (assignment: %v)", what, r.sku, capV, a)
	}
	return nil
}

// planMatches requires EXACTLY the tabled SKUs, nothing unassigned, no two
// SKUs on one slot, and every row to match.
func planMatches(p map[string]any, rows []cpRow, what string) error {
	got := assignmentsOf(p)
	if us, _ := p["unassigned"].([]any); len(us) != 0 {
		return fmt.Errorf("%s leaves SKUs unassigned: %v", what, us)
	}
	if len(got) != len(rows) {
		return fmt.Errorf("%s assigns %d SKUs, want %d (%v)", what, len(got), len(rows), got)
	}
	used := map[string]string{}
	for _, r := range rows {
		a, ok := got[r.sku]
		if !ok {
			return fmt.Errorf("%s does not assign %s (%v)", what, r.sku, got)
		}
		if err := matchRow(a, r, what); err != nil {
			return err
		}
		slot, _ := a["slot"].(string)
		if other, dup := used[slot]; dup {
			return fmt.Errorf("%s puts both %s and %s in slot %s", what, other, r.sku, slot)
		}
		used[slot] = r.sku
	}
	return nil
}

// draftEventually generates plans until one matches the table: a plan reads
// four event-fed copies (layout, products, pack levels, demand) and only the
// demand one is visible over REST, so an incomplete plan is discarded and retried.
func (st *cpState) draftEventually(w *world, label, site string, tbl *godog.Table) error {
	site = w.rs(site)
	rows, err := st.rowsFrom(w, tbl)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(cpConvergeTimeout)
	var last error
	for attempt := 1; ; attempt++ {
		p, err := w.slotGenerate(site)
		if err != nil {
			return err
		}
		if last = planMatches(p, rows, "plan"); last == nil {
			w.sl().plans[slotPlanKey(site, label)] = p
			st.rows[slotPlanKey(site, label)] = rows
			return nil
		}
		w.slotDiscard(p)
		if time.Now().After(deadline) {
			return fmt.Errorf("no plan matched within %s (%d drafts); the last one: %w (body: %s)", cpConvergeTimeout, attempt, last, w.last.body)
		}
		time.Sleep(cpConvergePoll)
	}
}

func (st *cpState) expected(site, label string) ([]cpRow, error) {
	rows, ok := st.rows[slotPlanKey(site, label)]
	if !ok {
		return nil, fmt.Errorf("no plan %q has been drafted for site %s with pick units", label, site)
	}
	return rows, nil
}

// planReadBack reads GET /slot-plans/{id}: the persisted plan, not the POST answer.
func (st *cpState) planReadBack(w *world, label, site string) error {
	site = w.rs(site)
	rows, err := st.expected(site, label)
	if err != nil {
		return err
	}
	p, err := w.fetchPlan(site, label)
	if err != nil {
		return err
	}
	return planMatches(p, rows, "GET of plan "+label)
}

// capacityFormula recomputes every CASE face's capacity from ADR 0012 decision
// 4 and the slot's DECLARED limits: floor(min(volume / case cube, weight / case weight)).
func (st *cpState) capacityFormula(w *world, label, site string, l, wd, h, g int) error {
	site = w.rs(site)
	p, err := w.fetchPlan(site, label)
	if err != nil {
		return err
	}
	cases := 0
	for sku, a := range assignmentsOf(p) {
		if a["pickUnit"] != "CASE" {
			continue
		}
		cases++
		slot, _ := a["slot"].(string)
		lim, ok := st.limits[slot]
		if !ok {
			return fmt.Errorf("%s sits in slot %s, which this scenario never gave limits", sku, slot)
		}
		byVolume := int(math.Floor(math.Round(lim.m3*1e9) / float64(l*wd*h)))
		byWeight := int(math.Floor(math.Round(lim.kg*1000) / float64(g)))
		want := byVolume
		if byWeight < want {
			want = byWeight
		}
		if got, _ := intOf(a["casesCapacity"]); want < 1 || got != want {
			return fmt.Errorf("%s in %s (%g kg, %g m3): casesCapacity %v, want floor(min(%d by volume, %d by weight)) = %d",
				sku, slot, lim.kg, lim.m3, a["casesCapacity"], byVolume, byWeight, want)
		}
	}
	if cases == 0 {
		return fmt.Errorf("plan %q of %s has no CASE assignment to check (body: %s)", label, site, w.last.body)
	}
	return nil
}

// assignMoves: the site had no Approved plan, so the plan moves every assigned
// SKU in (Assign, no from) and nothing else.
func (st *cpState) assignMoves(w *world, label, site string) error {
	site = w.rs(site)
	p, err := w.fetchPlan(site, label)
	if err != nil {
		return err
	}
	var want []string
	for sku, a := range assignmentsOf(p) {
		slot, _ := a["slot"].(string)
		want = append(want, moveKey(sku, "Assign", "", slot))
	}
	got := movesOf(p)
	sort.Strings(want)
	if strings.Join(got, "; ") != strings.Join(want, "; ") {
		return fmt.Errorf("plan %q of %s has moves [%s], want exactly [%s]", label, site, strings.Join(got, "; "), strings.Join(want, "; "))
	}
	if len(got) == 0 {
		return fmt.Errorf("plan %q of %s has no moves at all", label, site)
	}
	return nil
}

func (st *cpState) forwardSlots(w *world, site, label string) error {
	site = w.rs(site)
	id, err := w.storedPlanID(site, label)
	if err != nil {
		return err
	}
	rows, err := st.expected(site, label)
	if err != nil {
		return err
	}
	q := url.Values{"siteId": {site}}
	if err := w.doJSON(http.MethodGet, slottingBaseURL+"/forward-slots?"+q.Encode(), nil); err != nil {
		return err
	}
	if w.last.status != http.StatusOK {
		return fmt.Errorf("GET /forward-slots: status %d (body: %s)", w.last.status, w.last.body)
	}
	got := w.last.json()
	if got["planId"] != id {
		return fmt.Errorf("forward slots of %s come from plan %v, want %s (body: %s)", site, got["planId"], id, w.last.body)
	}
	if err := planMatches(got, rows, "GET /forward-slots"); err != nil {
		return fmt.Errorf("%w (body: %s)", err, w.last.body)
	}
	return nil
}

func (st *cpState) approvedEvent(w *world, label, site string) error {
	site = w.rs(site)
	id, err := w.storedPlanID(site, label)
	if err != nil {
		return err
	}
	rows, err := st.expected(site, label)
	if err != nil {
		return err
	}
	p, err := expectKafkaEvent(topicSlottingEvents, typeSlotPlanApproved,
		func(p map[string]any) bool { return p["plan_id"] == id }, "plan "+id)
	if err != nil {
		return err
	}
	got := map[string]map[string]any{}
	as, _ := p["assignments"].([]any)
	for _, a := range as {
		m, _ := a.(map[string]any)
		sku, _ := m["sku"].(string)
		got[sku] = m
	}
	if len(got) != len(rows) {
		return fmt.Errorf("SlotPlanApproved carries %d assignments, want %d (payload: %v)", len(got), len(rows), p)
	}
	for _, r := range rows {
		a, ok := got[r.sku]
		if !ok {
			return fmt.Errorf("SlotPlanApproved does not assign %s (payload: %v)", r.sku, p)
		}
		slot, _ := a["slot"].(string)
		if strings.HasPrefix(r.slot, "zone ") {
			if zoneOf(slot) != strings.TrimPrefix(r.slot, "zone ") {
				return fmt.Errorf("SlotPlanApproved puts %s in %s, want a slot of %s", r.sku, slot, r.slot)
			}
		} else if slot != r.slot {
			return fmt.Errorf("SlotPlanApproved puts %s in %s, want %s", r.sku, slot, r.slot)
		}
		unit, hasUnit := a["pick_unit"]
		capV, hasCap := a["cases_capacity"]
		if r.unit == "CASE" {
			if c, ok := intOf(capV); unit != "CASE" || !hasCap || !ok || c != r.cap {
				return fmt.Errorf("SlotPlanApproved assignment of %s is pick_unit %v cases_capacity %v, want CASE / %d (assignment: %v)", r.sku, unit, capV, r.cap, a)
			}
		} else if hasUnit || hasCap {
			return fmt.Errorf("SlotPlanApproved each assignment of %s carries pick_unit/cases_capacity, ADR 0012 keeps it byte-identical to before (assignment: %v)", r.sku, a)
		}
	}
	return nil
}
