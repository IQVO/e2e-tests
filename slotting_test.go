package main

// Step definitions for features/slotting_optimization.feature:
// slotting-optimization (SlotPlan, policy abc-velocity-v1; its ADR 0001/0002)
// proposes which SKUs get a forward pick slot and which slot, a human approves
// or rejects the proposal, and the approved plan is the site's forward-slot map.
//
// Black-box over REST wherever the contract has a read: plans, the forward-slot
// map and the SKU velocity are all read from slotting-optimization's own REST
// API, and facility-layout / product-master are driven over REST. Kafka is used
// for two things only, both because no REST exists for them:
//
//   - the demand: slotting-optimization learns demand ONLY from
//     order-management's SiteSkuDemandChanged (its ADR 0003). INJECTED, NOT
//     REAL, for the same reason the transfer saga injects it
//     (transfer_test.go injectSiteDemand): order-management projects every line
//     to ONE statically configured site per deployment
//     (DEMAND_PROJECTION_SITE_ID, its ADR 0035) and carries the line's PROMISE
//     date as due_at, which with the default 48 h lead time is two days in the
//     future, while a plan counts a line only when due_at falls in
//     [now - lookback, now). Real orders therefore cannot give each scenario
//     its own site, nor demand a plan can see for two days. The facts here are
//     byte-for-byte order-management's published contract, on its own topic.
//   - SlotPlanApproved, read from warehouse.slotting-optimization.events next to
//     the REST read, so the published contract (snake_case, full type) is
//     asserted too.
//
// Every scenario plans its OWN run-scoped site ("SLOTRANK<run>"), so no scenario
// can see another's demand, slots or Approved plan, and nothing needs cleaning
// up between runs.

import (
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/cucumber/godog"
)

var slottingBaseURL = envOrDefault("SLOTTING_BASE_URL", "http://localhost:8109")

const (
	topicSlottingEvents      = "warehouse.slotting-optimization.events"
	typeSlotPlanApproved     = "com.warehouse.wms.slotting-optimization.slotplan.SlotPlanApproved"
	typeSiteSkuDemandChanged = "com.warehouse.wes.order-management.siteskudemand.SiteSkuDemandChanged"

	// slotLocationType is the facility-layout location type of every forward
	// slot the feature registers: 1000 kg / 2 m3, far above one unit of the
	// feature's SKUs (205x121x82 mm, 1720 g).
	slotLocationType = "SlotFwdRack"
	// slotLookbackDays is the demand window of every plan; the injected
	// demand is due one hour ago, well inside it.
	slotLookbackDays = 7
)

// slotGuard is "this SKU may only ever be placed in this slot".
type slotGuard struct{ sku, slot string }

// slottingState is the per-process correlation. The world is shared by every
// scenario of a process (see InitializeScenario), so each key carries the
// scenario's own run-scoped site and the maps never collide.
type slottingState struct {
	plans     map[string]map[string]any // site|label -> last known plan
	demandSeq map[string]int            // site|sku -> demand lines published so far
	guards    map[string][]slotGuard    // site -> placements that must never be violated
}

func (w *world) sl() *slottingState {
	if w.slot == nil {
		w.slot = &slottingState{
			plans:     map[string]map[string]any{},
			demandSeq: map[string]int{},
			guards:    map[string][]slotGuard{},
		}
	}
	return w.slot
}

func slotPlanKey(site, label string) string { return site + "|" + label }

// ---------------------------------------------------------------------
// Background
// ---------------------------------------------------------------------

func (w *world) slottingServicesAreHealthy() error {
	for name, base := range map[string]string{
		"slotting-optimization": slottingBaseURL,
		"facility-layout":       facilityBaseURL,
		"product-master":        productMasterBaseURL,
	} {
		if err := w.doJSON(http.MethodGet, base+"/healthz", nil); err != nil {
			return fmt.Errorf("%s not reachable: %w", name, err)
		}
		if w.last.status != http.StatusOK {
			return fmt.Errorf("%s /healthz returned %d, body=%s", name, w.last.status, w.last.body)
		}
	}
	return nil
}

// ---------------------------------------------------------------------
// facility-layout: the forward zone and its slots
// ---------------------------------------------------------------------

// slotsExist registers the site, a forward zone (zone code FWD, the default of
// slotting-optimization's FORWARD_ZONE_CODES), one aisle per slot's aisle
// segment and the slots themselves. A code is SITE-AREA-ZONE-AISLE-BAY-LEVEL-POS
// (facility-layout's LocationCode); the site segment must be the scenario's own
// run-scoped site. hazmat marks the whole zone hazmat-rated.
func (w *world) slotsExist(site string, hazmat bool, codes ...string) error {
	site = w.rs(site)
	if err := w.ensureSite(site, "Slotting E2E "+site); err != nil {
		return fmt.Errorf("site %s: %w", site, err)
	}
	if err := w.ensureLocationType(slotLocationType, 1000, 2.0); err != nil {
		return fmt.Errorf("location type %s: %w", slotLocationType, err)
	}
	for _, raw := range codes {
		code := w.rs(raw)
		parts := strings.Split(code, "-")
		if len(parts) != 7 || parts[0] != site {
			return fmt.Errorf("slot %q is not a 7-segment location code of site %s", code, site)
		}
		area, zone, aisle := parts[1], parts[2], parts[3]
		zoneID := strings.Join(parts[:3], "-")
		if err := w.ensureZone(area, zone, site, "Ambient", hazmat); err != nil {
			return fmt.Errorf("zone %s: %w", zoneID, err)
		}
		if err := w.ensureAisle(aisle, zoneID, 1, "TwoWay"); err != nil {
			return fmt.Errorf("aisle %s of zone %s: %w", aisle, zoneID, err)
		}
		if err := w.ensureLocationSlot(code, slotLocationType); err != nil {
			return fmt.Errorf("slot %s: %w", code, err)
		}
	}
	return nil
}

func (w *world) siteHasTwoForwardSlots(site, a, b string) error {
	return w.slotsExist(site, false, a, b)
}
func (w *world) siteHasForwardSlot(site, code string) error { return w.slotsExist(site, false, code) }
func (w *world) siteHasHazmatForwardSlot(site, code string) error {
	return w.slotsExist(site, true, code)
}

// ---------------------------------------------------------------------
// product-master: the SKUs a plan can place
// ---------------------------------------------------------------------

// skuReadyForSlotting registers the SKU, optionally classifies it, then gives it
// a declared and a measured physical profile (the order product-master's
// events must arrive in: classification first, so the tags are already in
// slotting-optimization's copy when the dimensions make the SKU placeable).
func (w *world) skuReadyForSlotting(sku, tags string) error {
	if err := w.registerProductInProductMaster(sku); err != nil {
		return fmt.Errorf("register %s: %w", sku, err)
	}
	if tags != "" {
		if err := w.classifyProductInProductMaster(sku, tags); err != nil {
			return fmt.Errorf("classify %s: %w", sku, err)
		}
	}
	if err := w.declareDimensionsInProductMaster(sku, 200, 120, 80, 1500); err != nil {
		return fmt.Errorf("declare %s: %w", sku, err)
	}
	if err := w.measureDimensionsInProductMaster(sku, 205, 121, 82, 1720); err != nil {
		return fmt.Errorf("measure %s: %w", sku, err)
	}
	return nil
}

func (w *world) skuRegisteredAndMeasured(sku string) error { return w.skuReadyForSlotting(sku, "") }
func (w *world) skuRegisteredClassifiedAndMeasured(sku, tags string) error {
	return w.skuReadyForSlotting(sku, tags)
}

// ---------------------------------------------------------------------
// demand: order-management's SiteSkuDemandChanged (injected, see header)
// ---------------------------------------------------------------------

func (w *world) demandIsStated(lines, units int, sku, site string) error {
	sku, site = w.rs(sku), w.rs(site)
	st := w.sl()
	key := site + "|" + sku
	due := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	for i := 0; i < lines; i++ {
		st.demandSeq[key]++
		order := fmt.Sprintf("ORD-%s-%s-%d", site, sku, st.demandSeq[key])
		subject := order + "/line/1"
		if err := publishInjectedCloudEvent(topicOrderEvents,
			"/warehouse/order-management", typeSiteSkuDemandChanged,
			"urn:warehouse:order-management:events:SiteSkuDemandChanged:v1",
			subject, time.Now(), map[string]any{
				"source_order_id":    order,
				"line_no":            1,
				"site_id":            site,
				"sku":                sku,
				"demanded_units":     units,
				"due_at":             due.Format(time.RFC3339),
				"state":              "ACTIVE",
				"assignment_version": "static-site-v1",
			}); err != nil {
			return fmt.Errorf("publish demand line %d of %s at %s: %w", i+1, sku, site, err)
		}
	}
	return nil
}

// velocityEventually waits until slotting-optimization's demand copy shows
// exactly picks lines and units units for the SKU at the site. The site is
// run-scoped, so the numbers are exact, not "at least".
func (w *world) velocityEventually(picks, units int, sku, site string) error {
	sku, site = w.rs(sku), w.rs(site)
	return eventually(func() error {
		q := url.Values{"siteId": {site}, "windowDays": {fmt.Sprint(slotLookbackDays)}, "limit": {"500"}}
		if err := w.doJSON(http.MethodGet, slottingBaseURL+"/sku-velocity?"+q.Encode(), nil); err != nil {
			return err
		}
		if w.last.status != http.StatusOK {
			return fmt.Errorf("GET /sku-velocity: status %d (body: %s)", w.last.status, w.last.body)
		}
		items, _ := w.last.json()["items"].([]any)
		for _, it := range items {
			m, _ := it.(map[string]any)
			if m["sku"] != sku {
				continue
			}
			p, _ := intOf(m["picks"])
			u, _ := intOf(m["units"])
			if p != picks || u != units {
				return fmt.Errorf("velocity of %s at %s is %d picks / %d units, want %d / %d", sku, site, p, u, picks, units)
			}
			return nil
		}
		return fmt.Errorf("slotting-optimization has no demand for %s at %s yet (body: %s)", sku, site, w.last.body)
	})
}

// ---------------------------------------------------------------------
// plans
// ---------------------------------------------------------------------

// slotRow is one row of an expected plan: an assignment (slot set) or an
// unassigned SKU (reason set).
type slotRow struct{ sku, slot, reason string }

// rowsFromTable reads | sku | slot | unassigned |; every cell may carry the
// <run> token.
func (w *world) rowsFromTable(tbl *godog.Table) ([]slotRow, error) {
	var out []slotRow
	for i, row := range tbl.Rows {
		if i == 0 {
			continue // header
		}
		if len(row.Cells) != 3 {
			return nil, fmt.Errorf("plan table row %d has %d cells, want 3 (sku, slot, unassigned)", i, len(row.Cells))
		}
		r := slotRow{sku: w.rs(row.Cells[0].Value), slot: w.rs(row.Cells[1].Value), reason: row.Cells[2].Value}
		if (r.slot == "") == (r.reason == "") {
			return nil, fmt.Errorf("plan table row %d must name exactly one of a slot or an unassigned reason", i)
		}
		out = append(out, r)
	}
	return out, nil
}

// planOutcome flattens a plan's assignments (sku -> slot) and unassigned
// SKUs (sku -> reason).
func planOutcome(p map[string]any) (assigned, unassigned map[string]string) {
	assigned, unassigned = map[string]string{}, map[string]string{}
	as, _ := p["assignments"].([]any)
	for _, a := range as {
		m, _ := a.(map[string]any)
		sku, _ := m["sku"].(string)
		slot, _ := m["slot"].(string)
		assigned[sku] = slot
	}
	us, _ := p["unassigned"].([]any)
	for _, u := range us {
		m, _ := u.(map[string]any)
		sku, _ := m["sku"].(string)
		reason, _ := m["reason"].(string)
		unassigned[sku] = reason
	}
	return assigned, unassigned
}

func sameStringMap(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

// slotPlanMatches requires the plan to assign EXACTLY the wanted SKUs to the
// wanted slots and leave EXACTLY the wanted SKUs unassigned for the wanted
// reasons: no extra assignment, no missing one.
func slotPlanMatches(p map[string]any, want []slotRow) error {
	wantAssigned, wantUnassigned := map[string]string{}, map[string]string{}
	for _, r := range want {
		if r.slot != "" {
			wantAssigned[r.sku] = r.slot
		} else {
			wantUnassigned[r.sku] = r.reason
		}
	}
	gotAssigned, gotUnassigned := planOutcome(p)
	if !sameStringMap(gotAssigned, wantAssigned) || !sameStringMap(gotUnassigned, wantUnassigned) {
		return fmt.Errorf("plan assigns %v and leaves %v unassigned, want assignments %v and unassigned %v",
			gotAssigned, gotUnassigned, wantAssigned, wantUnassigned)
	}
	return nil
}

// checkGuards fails when the plan places a guarded SKU anywhere but its slot.
func (st *slottingState) checkGuards(site string, p map[string]any) error {
	assigned, _ := planOutcome(p)
	for _, g := range st.guards[site] {
		if slot, ok := assigned[g.sku]; ok && slot != g.slot {
			return fmt.Errorf("plan %v places %s in %s, but it may only ever be placed in %s", p["planId"], g.sku, slot, g.slot)
		}
	}
	return nil
}

func (w *world) slotGuardIs(sku, slot, site string) error {
	site = w.rs(site)
	st := w.sl()
	st.guards[site] = append(st.guards[site], slotGuard{sku: w.rs(sku), slot: w.rs(slot)})
	return nil
}

// slotGenerate generates a Draft plan for the site (POST /slot-plans).
func (w *world) slotGenerate(site string) (map[string]any, error) {
	if err := w.doJSON(http.MethodPost, slottingBaseURL+"/slot-plans",
		map[string]any{"siteId": site, "lookbackDays": slotLookbackDays}); err != nil {
		return nil, err
	}
	if w.last.status != http.StatusCreated {
		return nil, fmt.Errorf("generate plan for %s: status %d, want 201 (body: %s)", site, w.last.status, w.last.body)
	}
	p := w.last.json()
	if id, _ := p["planId"].(string); id == "" || p["state"] != "Draft" {
		return nil, fmt.Errorf("generate plan for %s: planId %q state %v, want a Draft (body: %s)", site, id, p["state"], w.last.body)
	}
	return p, nil
}

// slotDiscard rejects an intermediate Draft so a retry loop leaves no stray
// Draft behind. Best effort: the outcome is irrelevant to the scenario.
func (w *world) slotDiscard(p map[string]any) {
	if id, _ := p["planId"].(string); id != "" {
		_ = w.doJSON(http.MethodPost, fmt.Sprintf("%s/slot-plans/%s/reject", slottingBaseURL, id),
			map[string]any{"reason": "e2e: intermediate draft while the local copies converge"})
	}
}

// slotDraftEventually generates plans until one assigns exactly the tabled
// rows. A plan is computed from three event-fed local copies (layout, product,
// demand) and nothing on the REST surface says the layout and product copies
// have caught up, so an incomplete plan is retried (and discarded). A guarded
// placement that is ever violated is final, not retried.
func (w *world) slotDraftEventually(label, site string, tbl *godog.Table) error {
	site = w.rs(site)
	want, err := w.rowsFromTable(tbl)
	if err != nil {
		return err
	}
	st := w.sl()
	var (
		plan  map[string]any
		fatal error
	)
	err = eventually(func() error {
		p, gerr := w.slotGenerate(site)
		if gerr != nil {
			fatal = gerr
			return nil
		}
		if cerr := st.checkGuards(site, p); cerr != nil {
			fatal = cerr
			w.slotDiscard(p)
			return nil
		}
		if merr := slotPlanMatches(p, want); merr != nil {
			w.slotDiscard(p)
			return merr
		}
		plan = p
		return nil
	})
	if fatal != nil {
		return fatal
	}
	if err != nil {
		return err
	}
	st.plans[slotPlanKey(site, label)] = plan
	return nil
}

// slotDraftOnce generates ONE plan and requires it to match: used when every
// input is already provably in place (the velocity read showed the demand, and
// the layout and products were consumed for the plan before it).
func (w *world) slotDraftOnce(label, site string, tbl *godog.Table) error {
	site = w.rs(site)
	want, err := w.rowsFromTable(tbl)
	if err != nil {
		return err
	}
	p, err := w.slotGenerate(site)
	if err != nil {
		return err
	}
	st := w.sl()
	if err := st.checkGuards(site, p); err != nil {
		return err
	}
	if err := slotPlanMatches(p, want); err != nil {
		return fmt.Errorf("%w (plan: %s)", err, w.last.body)
	}
	st.plans[slotPlanKey(site, label)] = p
	return nil
}

func (w *world) storedPlanID(site, label string) (string, error) {
	p, ok := w.sl().plans[slotPlanKey(site, label)]
	if !ok {
		return "", fmt.Errorf("no plan %q has been drafted for site %s in this process", label, site)
	}
	id, _ := p["planId"].(string)
	return id, nil
}

// fetchPlan reads the plan back (GET /slot-plans/{id}) and refreshes the stored copy.
func (w *world) fetchPlan(site, label string) (map[string]any, error) {
	id, err := w.storedPlanID(site, label)
	if err != nil {
		return nil, err
	}
	if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/slot-plans/%s", slottingBaseURL, id), nil); err != nil {
		return nil, err
	}
	if w.last.status != http.StatusOK {
		return nil, fmt.Errorf("GET plan %s: status %d (body: %s)", id, w.last.status, w.last.body)
	}
	p := w.last.json()
	w.sl().plans[slotPlanKey(site, label)] = p
	return p, nil
}

func (w *world) planIsInState(label, site, state string, version int) error {
	site = w.rs(site)
	p, err := w.fetchPlan(site, label)
	if err != nil {
		return err
	}
	v, _ := intOf(p["version"])
	if p["state"] != state || v != version {
		return fmt.Errorf("plan %q of %s is %v at version %d, want %s at version %d (body: %s)", label, site, p["state"], v, state, version, w.last.body)
	}
	return nil
}

// moveKey renders a move as "sku kind from->to" for an order-free comparison.
func moveKey(sku, kind, from, to string) string {
	return fmt.Sprintf("%s %s %s->%s", sku, kind, from, to)
}

func (w *world) planHasMoves(label, site string, tbl *godog.Table) error {
	site = w.rs(site)
	p, err := w.fetchPlan(site, label)
	if err != nil {
		return err
	}
	var want []string
	for i, row := range tbl.Rows {
		if i == 0 {
			continue
		}
		if len(row.Cells) != 4 {
			return fmt.Errorf("moves table row %d has %d cells, want 4 (sku, kind, from, to)", i, len(row.Cells))
		}
		want = append(want, moveKey(w.rs(row.Cells[0].Value), row.Cells[1].Value, w.rs(row.Cells[2].Value), w.rs(row.Cells[3].Value)))
	}
	got := movesOf(p)
	sort.Strings(want)
	if strings.Join(got, "; ") != strings.Join(want, "; ") {
		return fmt.Errorf("plan %q of %s has moves [%s], want exactly [%s]", label, site, strings.Join(got, "; "), strings.Join(want, "; "))
	}
	return nil
}

func movesOf(p map[string]any) []string {
	var got []string
	ms, _ := p["moves"].([]any)
	for _, m := range ms {
		mm, _ := m.(map[string]any)
		sku, _ := mm["sku"].(string)
		kind, _ := mm["kind"].(string)
		from, _ := mm["fromSlot"].(string)
		to, _ := mm["toSlot"].(string)
		got = append(got, moveKey(sku, kind, from, to))
	}
	sort.Strings(got)
	return got
}

func (w *world) planHasNoMoves(label, site string) error {
	site = w.rs(site)
	p, err := w.fetchPlan(site, label)
	if err != nil {
		return err
	}
	if got := movesOf(p); len(got) != 0 {
		return fmt.Errorf("plan %q of %s has moves [%s], want none (no Assign, Relocate or Vacate)", label, site, strings.Join(got, "; "))
	}
	return nil
}

func (w *world) planClassesSKU(label, site, sku, class string) error {
	site, sku = w.rs(site), w.rs(sku)
	p, err := w.fetchPlan(site, label)
	if err != nil {
		return err
	}
	as, _ := p["assignments"].([]any)
	for _, a := range as {
		m, _ := a.(map[string]any)
		if m["sku"] == sku {
			if m["abcClass"] != class {
				return fmt.Errorf("plan %q of %s puts %s in class %v, want %s (body: %s)", label, site, sku, m["abcClass"], class, w.last.body)
			}
			return nil
		}
	}
	return fmt.Errorf("plan %q of %s does not assign %s (body: %s)", label, site, sku, w.last.body)
}

// ---------------------------------------------------------------------
// the human decision
// ---------------------------------------------------------------------

func (w *world) planIsApproved(label, site string) error {
	site = w.rs(site)
	id, err := w.storedPlanID(site, label)
	if err != nil {
		return err
	}
	if err := w.expectOK2xx(w.doJSON(http.MethodPost, fmt.Sprintf("%s/slot-plans/%s/approve", slottingBaseURL, id), nil)); err != nil {
		return fmt.Errorf("approve plan %s: %w", id, err)
	}
	if got := w.last.json()["state"]; w.last.status != http.StatusOK || got != "Approved" {
		return fmt.Errorf("approve plan %s: status %d state %v, want 200 Approved (body: %s)", id, w.last.status, got, w.last.body)
	}
	w.sl().plans[slotPlanKey(site, label)] = w.last.json()
	return nil
}

func (w *world) approvingIsRefused(label, site string, status int, slug string) error {
	site = w.rs(site)
	id, err := w.storedPlanID(site, label)
	if err != nil {
		return err
	}
	if err := w.doJSON(http.MethodPost, fmt.Sprintf("%s/slot-plans/%s/approve", slottingBaseURL, id), nil); err != nil {
		return err
	}
	return w.problemIs(status, slug)
}

// assignmentsFromTable reads | sku | slot |.
func (w *world) assignmentsFromTable(tbl *godog.Table) (map[string]string, error) {
	out := map[string]string{}
	for i, row := range tbl.Rows {
		if i == 0 {
			continue
		}
		if len(row.Cells) != 2 {
			return nil, fmt.Errorf("assignments table row %d has %d cells, want 2 (sku, slot)", i, len(row.Cells))
		}
		out[w.rs(row.Cells[0].Value)] = w.rs(row.Cells[1].Value)
	}
	return out, nil
}

// noForwardSlotsYet requires the site to have no Approved plan: an empty
// assignment map and no planId (a Draft is not the forward-slot map).
func (w *world) noForwardSlotsYet(site string) error {
	site = w.rs(site)
	q := url.Values{"siteId": {site}}
	if err := w.doJSON(http.MethodGet, slottingBaseURL+"/forward-slots?"+q.Encode(), nil); err != nil {
		return err
	}
	if w.last.status != http.StatusOK {
		return fmt.Errorf("GET /forward-slots: status %d (body: %s)", w.last.status, w.last.body)
	}
	got := w.last.json()
	as, _ := got["assignments"].([]any)
	if id, _ := got["planId"].(string); id != "" || len(as) != 0 {
		return fmt.Errorf("site %s already has forward slots (plan %q, %d assignments), want none before an approval (body: %s)", site, id, len(as), w.last.body)
	}
	return nil
}

func (w *world) forwardSlotsAre(site, label string, tbl *godog.Table) error {
	site = w.rs(site)
	id, err := w.storedPlanID(site, label)
	if err != nil {
		return err
	}
	want, err := w.assignmentsFromTable(tbl)
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
	have := map[string]string{}
	as, _ := got["assignments"].([]any)
	for _, a := range as {
		m, _ := a.(map[string]any)
		sku, _ := m["sku"].(string)
		slot, _ := m["slot"].(string)
		have[sku] = slot
	}
	if !sameStringMap(have, want) {
		return fmt.Errorf("forward slots of %s are %v, want exactly %v", site, have, want)
	}
	return nil
}

func (w *world) approvedEventPublished(label, site string, tbl *godog.Table) error {
	site = w.rs(site)
	id, err := w.storedPlanID(site, label)
	if err != nil {
		return err
	}
	want, err := w.assignmentsFromTable(tbl)
	if err != nil {
		return err
	}
	p, err := expectKafkaEvent(topicSlottingEvents, typeSlotPlanApproved,
		func(p map[string]any) bool { return p["plan_id"] == id }, "plan "+id)
	if err != nil {
		return err
	}
	if p["site_id"] != site {
		return fmt.Errorf("SlotPlanApproved site_id = %v, want %s (payload: %v)", p["site_id"], site, p)
	}
	have := map[string]string{}
	as, _ := p["assignments"].([]any)
	for _, a := range as {
		m, _ := a.(map[string]any)
		sku, _ := m["sku"].(string)
		slot, _ := m["slot"].(string)
		have[sku] = slot
	}
	if !sameStringMap(have, want) {
		return fmt.Errorf("SlotPlanApproved of plan %s carries assignments %v, want exactly %v (payload: %v)", id, have, want, p)
	}
	return nil
}

// approvedEventCount requires exactly n SlotPlanApproved for the plan: the
// first scan waits for the event to be on the topic, the second pass counts.
func (w *world) approvedEventCount(n int, label, site string) error {
	site = w.rs(site)
	id, err := w.storedPlanID(site, label)
	if err != nil {
		return err
	}
	if n > 0 {
		if _, err := expectKafkaEvent(topicSlottingEvents, typeSlotPlanApproved,
			func(p map[string]any) bool { return p["plan_id"] == id }, "plan "+id); err != nil {
			return err
		}
	}
	count := 0
	if _, _, err := scanTopic(topicSlottingEvents, kafkaAbsenceWindow, func(m ceMessage) bool {
		if m.Type == typeSlotPlanApproved && m.payload()["plan_id"] == id {
			count++
		}
		return false
	}); err != nil {
		return err
	}
	if count != n {
		return fmt.Errorf("%d SlotPlanApproved for plan %s on %s, want exactly %d", count, id, topicSlottingEvents, n)
	}
	return nil
}

// ---------------------------------------------------------------------
// registration
// ---------------------------------------------------------------------

func registerSlottingSteps(sc *godog.ScenarioContext, w *world) {
	sc.Step(`^slotting-optimization, facility-layout and product-master are healthy$`, w.slottingServicesAreHealthy)

	sc.Step(`^site "([^"]*)" has forward slots "([^"]*)" and "([^"]*)" in facility-layout$`, w.siteHasTwoForwardSlots)
	sc.Step(`^site "([^"]*)" has forward slot "([^"]*)" in facility-layout$`, w.siteHasForwardSlot)
	sc.Step(`^site "([^"]*)" has hazmat forward slot "([^"]*)" in facility-layout$`, w.siteHasHazmatForwardSlot)

	sc.Step(`^SKU "([^"]*)" is registered and measured for slotting in product-master$`, w.skuRegisteredAndMeasured)
	sc.Step(`^SKU "([^"]*)" is registered, classified with handling tags "([^"]*)" and measured for slotting in product-master$`, w.skuRegisteredClassifiedAndMeasured)

	sc.Step(`^order-management's demand projection states (\d+) order lines? of (\d+) units? of SKU "([^"]*)" at site "([^"]*)"$`, w.demandIsStated)
	sc.Step(`^slotting-optimization eventually reports (\d+) picks? and (\d+) units? for SKU "([^"]*)" at site "([^"]*)"$`, w.velocityEventually)

	sc.Step(`^slotting-optimization must never place SKU "([^"]*)" anywhere but in slot "([^"]*)" at site "([^"]*)"$`, w.slotGuardIs)
	sc.Step(`^slotting-optimization eventually drafts plan "([^"]*)" for site "([^"]*)" that assigns exactly:$`, w.slotDraftEventually)
	sc.Step(`^slotting-optimization drafts plan "([^"]*)" for site "([^"]*)" that assigns exactly:$`, w.slotDraftOnce)

	sc.Step(`^plan "([^"]*)" of site "([^"]*)" is "([^"]*)" at version (\d+)$`, w.planIsInState)
	sc.Step(`^plan "([^"]*)" of site "([^"]*)" has exactly these moves:$`, w.planHasMoves)
	sc.Step(`^plan "([^"]*)" of site "([^"]*)" has no moves$`, w.planHasNoMoves)
	sc.Step(`^plan "([^"]*)" of site "([^"]*)" puts SKU "([^"]*)" in class "([^"]*)"$`, w.planClassesSKU)

	sc.Step(`^plan "([^"]*)" of site "([^"]*)" is approved in slotting-optimization$`, w.planIsApproved)
	sc.Step(`^approving plan "([^"]*)" of site "([^"]*)" in slotting-optimization is refused with (\d+) "([^"]*)"$`, w.approvingIsRefused)
	sc.Step(`^slotting-optimization has no forward slots for site "([^"]*)" yet$`, w.noForwardSlotsYet)
	sc.Step(`^slotting-optimization's forward slots of site "([^"]*)" come from plan "([^"]*)" and are exactly:$`, w.forwardSlotsAre)
	sc.Step(`^slotting-optimization published SlotPlanApproved for plan "([^"]*)" of site "([^"]*)" with exactly these assignments:$`, w.approvedEventPublished)
	sc.Step(`^slotting-optimization published exactly (\d+) SlotPlanApproved events? for plan "([^"]*)" of site "([^"]*)"$`, w.approvedEventCount)
}
