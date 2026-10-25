package main

// Step definitions for features/phase5_b.feature (Phase 5, part B):
//
//  1. network-inventory-planning ADR 0017: case-multiple transfers.
//  2. fulfillment-execution ADR 0056: ship in own container.
//
// Black-box, REST only, through the Kong gateway of the kind cluster, no
// database touched. Every fact is produced by its owning service's REST
// contract EXCEPT the approval scenarios' site-sku demand, which has no REST
// producer for an arbitrary site and is injected as the exact CloudEvent
// through the suite's existing Kafka helper (transfer_test.go); see the
// feature header and the README "Phase 5 part B" section.
//
// State lives in a closure (registerPhase5BSteps) and is reset before every
// scenario, so this file adds no field to the shared world struct.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/cucumber/godog"
)

var (
	// p5NipBase is network-inventory-planning (Kong: $P/network-inventory-planning).
	// NETWORK_INVENTORY_PLANNING_BASE_URL wins, otherwise the suite's NIP_BASE_URL.
	p5NipBase = envOrDefault("NETWORK_INVENTORY_PLANNING_BASE_URL", nipBaseURL)
	// facility-layout and fulfillment-execution reuse FACILITY_BASE_URL and
	// FULFILLMENT_BASE_URL.
)

const (
	p5GtinPrefix = "9502" // distinct from mastercase's 9501 so GTINs never collide
	p5DemandPlan = "p5b-policy-v1"
)

// p5Ordinals gives every SKU of this process a stable 1-digit ordinal so that
// its GTINs are unique per (run, sku) yet identical when a scenario re-declares
// the same SKU.
var p5Ordinals = map[string]int{}

type p5Dims struct{ l, w, h, g int }

type p5State struct {
	caseGtin map[string]string
	caseUnit map[string]int
	prop     map[string]any // proposal under test (nil = none proposed)
	propBody string
	xfer     map[string]any // approval response
	apprKey  string         // Idempotency-Key of the approval under test
	apprReq  map[string]any // its request body (fixed, so a replay is the same request)
	station  string
	taskID   string
	pkgID    string
}

func p5Gtin(w *world, sku, indicator string) string {
	ord, ok := p5Ordinals[sku]
	if !ok {
		ord = len(p5Ordinals)
		p5Ordinals[sku] = ord
	}
	p := indicator + p5GtinPrefix + w.runDigits()[:7] + fmt.Sprintf("%d", ord%10)
	return p + gs1CheckDigit(p)
}

func registerPhase5BSteps(sc *godog.ScenarioContext, w *world) {
	st := &p5State{}
	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		*st = p5State{caseGtin: map[string]string{}, caseUnit: map[string]int{}}
		return ctx, nil
	})

	// ---- health -------------------------------------------------------
	sc.Step(`^network-inventory-planning and fulfillment-execution are healthy$`, func() error {
		for name, base := range map[string]string{"network-inventory-planning": p5NipBase, "fulfillment-execution": fulfillmentBaseURL} {
			if err := w.doJSON(http.MethodGet, base+"/healthz", nil); err != nil {
				return fmt.Errorf("%s not reachable: %w", name, err)
			}
			if w.last.status < 200 || w.last.status >= 300 {
				return fmt.Errorf("%s /healthz returned %d, body=%s", name, w.last.status, w.last.body)
			}
		}
		return nil
	})
	sc.Step(`^product-master and facility-layout are healthy$`, func() error {
		for name, base := range map[string]string{"product-master": productMasterBaseURL, "facility-layout": facilityBaseURL} {
			if err := w.doJSON(http.MethodGet, base+"/healthz", nil); err != nil {
				return fmt.Errorf("%s not reachable: %w", name, err)
			}
			if w.last.status < 200 || w.last.status >= 300 {
				return fmt.Errorf("%s /healthz returned %d, body=%s", name, w.last.status, w.last.body)
			}
		}
		return nil
	})

	// ---- product-master ----------------------------------------------
	sc.Step(`^SKU "([^"]*)" declares an EACH GTIN and a CASE pack level of (\d+) eaches with case dimensions (\d+)x(\d+)x(\d+) mm and (\d+) g in product-master$`,
		func(sku string, perCase, l, wd, h, g int) error {
			return st.declare(w, sku, perCase, &p5Dims{l, wd, h, g})
		})
	sc.Step(`^SKU "([^"]*)" declares only an EACH GTIN in product-master$`, func(sku string) error {
		return st.declare(w, sku, 0, nil)
	})
	// "the CASE GTIN of SKU ... eventually resolves" is mastercase_test.go's step; declare() feeds it.

	// ---- network-inventory-planning: proposals -----------------------
	sc.Step(`^network-inventory-planning eventually plans SKU "([^"]*)" in cases of (\d+) eaches$`, func(sku string, perCase int) error {
		// Pack levels reach NIP over Kafka (its own local view, ADR 0017): no REST
		// read of that view exists, so probe the proposal itself until it carries
		// the case multiple. An unpropagated pack shows as a proposal WITHOUT
		// case fields, never as an error.
		return eventually(func() error {
			if err := st.generate(w, sku, 100000, 10000000); err != nil {
				return err
			}
			if st.prop == nil {
				return fmt.Errorf("no proposal yet: %s", st.propBody)
			}
			if m, _ := toFloat(st.prop["caseMultiple"]); int(m) != perCase {
				return fmt.Errorf("proposal has not the case multiple %d yet: %s", perCase, st.propBody)
			}
			return nil
		})
	})
	sc.Step(`^I generate a transfer proposal for SKU "([^"]*)" with a destination deficit of (\d+) eaches and an origin supply of (\d+) eaches in network-inventory-planning$`,
		func(sku string, deficit, supply int) error { return st.generate(w, sku, deficit, supply) })
	sc.Step(`^the proposal moves (\d+) cases of (\d+) eaches: requested (\d+), rounded (\d+), remainder (-?\d+)$`,
		func(cases, mult, req, rounded, rem int) error { return st.proposalIs(cases, mult, req, rounded, rem) })
	sc.Step(`^the proposal moves (\d+) eaches and carries no case fields$`, func(qty int) error {
		if st.prop == nil {
			return fmt.Errorf("no proposal: %s", st.propBody)
		}
		if q, _ := toFloat(st.prop["Quantity"]); int(q) != qty {
			return fmt.Errorf("Quantity = %v, want %d (%s)", st.prop["Quantity"], qty, st.propBody)
		}
		for _, k := range []string{"caseMultiple", "cases", "requestedQtyEaches", "roundedQtyEaches", "remainderEaches"} {
			if _, has := st.prop[k]; has {
				return fmt.Errorf("plain SKU proposal carries %q: %s", k, st.propBody)
			}
		}
		return nil
	})
	sc.Step(`^no transfer is proposed because the origin cannot supply one whole case$`, func() error {
		if st.prop != nil {
			return fmt.Errorf("expected no proposal, got %s", st.propBody)
		}
		return nil
	})

	// ---- network-inventory-planning: approval (live, ADR 0017 + ADR 0018) ----
	//
	// Facts come from their owners: the CASE level from product-master REST, the
	// transfer capability from facility-layout REST (PUT /sites/{code}/transfer-capability),
	// the capacity plan from warehouse-planning REST, and the site-sku demand from the
	// harness's injected CloudEvent (the ONE injected fact: order-management has no
	// route that states demand for an arbitrary site; see the feature header). It is
	// the existing inter_warehouse_transfer step, reused as is.
	sc.Step(`^the harness injects order-management's SiteSkuDemandChanged for site "([^"]*)": (\d+) units of SKU "([^"]*)" due in (\d+) hours?, stated (\d+) hours ago$`,
		func(site string, units int, sku string, due, ago int) error {
			return w.injectSiteDemandAt(site, units, sku, due, time.Now().Add(-time.Duration(ago)*time.Hour))
		})
	sc.Step(`^facility-layout enables site "([^"]*)" as transfer origin and destination$`, func(site string) error {
		site = w.rs(site)
		if err := w.ensureSite(site, "e2e phase5 "+site); err != nil {
			return err
		}
		return w.expectOK2xx(w.doJSON(http.MethodPut, fmt.Sprintf("%s/sites/%s/transfer-capability", facilityBaseURL, site),
			map[string]any{"transferOriginEnabled": true, "transferDestinationEnabled": true}))
	})
	// "warehouse-planning publishes a capacity plan for site ..." is transfer_test.go's step.
	sc.Step(`^an operator approves a transfer of (\d+) eaches of SKU "([^"]*)" from "([^"]*)" to "([^"]*)" in network-inventory-planning$`,
		func(qty int, sku, origin, dest string) error { return st.approve(w, qty, sku, origin, dest) })
	sc.Step(`^an operator approves a transfer of (\d+) eaches of SKU "([^"]*)" from "([^"]*)" to "([^"]*)" and network-inventory-planning eventually refuses it as facts-incomplete naming site "([^"]*)"( as stale)?$`,
		func(qty int, sku, origin, dest, site, stale string) error {
			return st.approveRefusedNaming(w, qty, sku, origin, dest, w.rs(site), stale != "")
		})
	sc.Step(`^the approval is accepted for (\d+) cases of (\d+) eaches: requested (\d+), rounded (\d+), remainder (-?\d+)$`,
		func(cases, mult, req, rounded, rem int) error {
			if w.last.status != http.StatusOK || st.xfer == nil {
				return fmt.Errorf("approval answered %d (body: %s)", w.last.status, w.last.body)
			}
			// The transfer quantity is the ROUNDED eaches, never the request.
			return caseFieldsAre(st.xfer, rounded, cases, mult, req, rounded, rem, w.last.body)
		})
	sc.Step(`^the approval is accepted for (\d+) eaches and carries no case fields$`, func(qty int) error {
		if w.last.status != http.StatusOK || st.xfer == nil {
			return fmt.Errorf("approval answered %d (body: %s)", w.last.status, w.last.body)
		}
		return noCaseFields(st.xfer, qty, w.last.body)
	})
	sc.Step(`^GET of that transfer in network-inventory-planning shows (\d+) cases of (\d+) eaches: requested (\d+), rounded (\d+), remainder (-?\d+)$`,
		func(cases, mult, req, rounded, rem int) error {
			got, raw, err := st.getTransfer(w)
			if err != nil {
				return err
			}
			return caseFieldsAre(got, rounded, cases, mult, req, rounded, rem, raw)
		})
	sc.Step(`^GET of that transfer in network-inventory-planning shows (\d+) eaches and carries no case fields$`, func(qty int) error {
		got, raw, err := st.getTransfer(w)
		if err != nil {
			return err
		}
		return noCaseFields(got, qty, raw)
	})
	sc.Step(`^replaying that approval with the same Idempotency-Key answers replayed true for the same transfer$`, func() error {
		first, _ := st.xfer["transferId"].(string)
		if first == "" || st.apprKey == "" {
			return fmt.Errorf("no approval to replay")
		}
		if err := st.postApprove(w, st.apprKey, st.apprReq); err != nil {
			return err
		}
		if w.last.status != http.StatusOK {
			return fmt.Errorf("replay answered %d, want 200 (body: %s)", w.last.status, w.last.body)
		}
		got := w.last.json()
		if got["replayed"] != true {
			return fmt.Errorf("replay: replayed = %v, want true (body: %s)", got["replayed"], w.last.body)
		}
		if id, _ := got["transferId"].(string); id != first {
			return fmt.Errorf("replay minted transfer %q, want the first %q (body: %s)", id, first, w.last.body)
		}
		return nil
	})
	sc.Step(`^the approval is refused with status (\d+) and problem "([^"]*)"$`, func(status int, slug string) error {
		return problemIs(w, status, slug)
	})

	// ---- fulfillment-execution --------------------------------------
	sc.Step(`^a PACK station "([^"]*)" is registered in fulfillment-execution$`, func(station string) error {
		st.station = w.rs(station)
		// The station's capability set is a private run-scoped capability, and the
		// task requires only that one: no other scenario's (or service's) PACK task
		// can be handed to this station, and this station's task is nobody else's.
		return w.expectOKOr409(w.doJSON(http.MethodPost, fulfillmentBaseURL+"/stations",
			map[string]any{"stationId": st.station, "capabilities": []string{st.cap(w)}}))
	})
	sc.Step(`^the PACK task of order "([^"]*)" is claimed by that station in fulfillment-execution$`, func(order string) error {
		order = w.rs(order)
		if err := w.expectOK2xx(w.doJSON(http.MethodPost, fulfillmentBaseURL+"/tasks", map[string]any{
			"type": "PACK", "cpt": time.Now().UTC().Add(2 * time.Hour).Format(time.RFC3339),
			"orderRef": order, "requiredCapabilities": []string{st.cap(w)},
		})); err != nil {
			return fmt.Errorf("create PACK task: %w", err)
		}
		created, _ := w.last.json()["id"].(string)
		err := eventually(func() error {
			if err := w.expectOK2xx(w.doJSON(http.MethodPost,
				fmt.Sprintf("%s/stations/%s/claim-next", fulfillmentBaseURL, st.station), map[string]any{"taskType": "PACK"})); err != nil {
				return err
			}
			if got := w.last.json(); got["orderRef"] != order {
				return fmt.Errorf("claimed task of order %v, want %q", got["orderRef"], order)
			}
			return nil
		})
		if err != nil {
			return err
		}
		st.taskID, _ = w.last.json()["id"].(string)
		if st.taskID != created {
			return fmt.Errorf("claimed task %s, created %s", st.taskID, created)
		}
		return nil
	})
	sc.Step(`^the station seals SKU "([^"]*)" in fulfillment-execution in its own container with line quantity (\d+)$`,
		func(sku string, qty int) error { return st.seal(w, sku, true, qty) })
	sc.Step(`^the station seals SKU "([^"]*)" in fulfillment-execution as an ordinary carton$`,
		func(sku string) error { return st.seal(w, sku, false, 0) })
	sc.Step(`^the station seals SKU "([^"]*)" in fulfillment-execution in its own container without a line quantity$`, func(sku string) error {
		return w.doJSON(http.MethodPost, fmt.Sprintf("%s/tasks/%s/seal-package", fulfillmentBaseURL, st.taskID),
			map[string]any{"stationId": st.station, "contents": []string{w.rs(sku)}, "shipInOwnContainer": true})
	})
	sc.Step(`^fulfillment-execution eventually knows the CASE level of SKU "([^"]*)"$`, func(sku string) error {
		// The pack levels reach fulfillment-execution over Kafka into its own PackView
		// (ADR 0054); no REST read of that view exists. A refused seal seals nothing
		// (ADR 0056), so the seal itself is the probe, with one each short of a case:
		// never accepted, refused no-case-level until the level has arrived and
		// quantity-mismatch afterwards.
		units := st.caseUnit[w.rs(sku)]
		if units < 2 {
			return fmt.Errorf("SKU %s declared no CASE level in this scenario", sku)
		}
		return eventually(func() error {
			if err := st.seal(w, sku, true, units-1); err != nil {
				return err
			}
			if w.last.status == http.StatusUnprocessableEntity && strings.HasSuffix(problemType(w), "/ship-in-own-container-quantity-mismatch") {
				return nil
			}
			return fmt.Errorf("CASE level not known yet: %d %s", w.last.status, w.last.body)
		})
	})
	sc.Step(`^the seal is accepted with status 201$`, func() error {
		if w.last.status != http.StatusCreated {
			return fmt.Errorf("seal answered %d (body: %s)", w.last.status, w.last.body)
		}
		st.pkgID, _ = w.last.json()["id"].(string)
		return nil
	})
	sc.Step(`^the package is sealed in its own container sourced CASE with the CASE GTIN of SKU "([^"]*)", (\d+)x(\d+)x(\d+) mm and (\d+) g$`,
		func(sku string, l, wd, h, g int) error {
			sku = w.rs(sku)
			return st.packageIsOwnContainer(w.last.json(), sku, l, wd, h, g, w.last.body)
		})
	sc.Step(`^reading that package back from fulfillment-execution shows the same own container of SKU "([^"]*)", (\d+)x(\d+)x(\d+) mm and (\d+) g$`,
		func(sku string, l, wd, h, g int) error {
			if err := w.doJSON(http.MethodGet, fulfillmentBaseURL+"/packages/"+st.pkgID, nil); err != nil {
				return err
			}
			if w.last.status != http.StatusOK {
				return fmt.Errorf("GET /packages/%s = %d (%s)", st.pkgID, w.last.status, w.last.body)
			}
			return st.packageIsOwnContainer(w.last.json(), w.rs(sku), l, wd, h, g, w.last.body)
		})
	sc.Step(`^the package is an ordinary sealed carton without own-container fields$`, func() error {
		got := w.last.json()
		if got["status"] != "SEALED" {
			return fmt.Errorf("package status = %v (%s)", got["status"], w.last.body)
		}
		for _, k := range []string{"shipInOwnContainer", "containerSource", "container"} {
			if _, has := got[k]; has {
				return fmt.Errorf("ordinary package carries %q: %s", k, w.last.body)
			}
		}
		return nil
	})
	sc.Step(`^the seal is refused with status (\d+) and fulfillment-execution problem "([^"]*)"$`, func(status int, slug string) error {
		return problemIs(w, status, slug)
	})
	sc.Step(`^nothing was sealed: the station can still seal SKU "([^"]*)" as an ordinary carton$`, func(sku string) error {
		if err := st.seal(w, sku, false, 0); err != nil {
			return err
		}
		if w.last.status != http.StatusCreated {
			return fmt.Errorf("ordinary seal after the refusal answered %d (%s)", w.last.status, w.last.body)
		}
		return nil
	})
}

// cap is the run-scoped private capability of the scenario's PACK station.
func (st *p5State) cap(w *world) string { return "p5b-" + w.runDigits() }

// declare registers the SKU and declares EACH (and CASE when perCase >= 2) with
// valid run-unique GTINs; caseDims are the CASE's declared dimensions/weight.
func (st *p5State) declare(w *world, sku string, perCase int, caseDims *p5Dims) error {
	sku = w.rs(sku)
	if err := w.registerProductInProductMaster(sku); err != nil {
		return fmt.Errorf("register %s: %w", sku, err)
	}
	each := p5Gtin(w, sku, "0")
	if err := w.expectOK2xx(w.doJSON(http.MethodPut, fmt.Sprintf("%s/products/%s/pack-levels/EACH", productMasterBaseURL, sku),
		map[string]any{"gtin": each, "isOrderable": true, "isDespatchUnit": false,
			"dims": map[string]any{"lengthMm": 100, "widthMm": 80, "heightMm": 60, "weightG": 450}})); err != nil {
		return fmt.Errorf("declare EACH %s: %w", each, err)
	}
	if perCase < 2 {
		return nil
	}
	cs := p5Gtin(w, sku, "1")
	body := map[string]any{"gtin": cs, "childGtin": each, "qtyOfChild": perCase, "isOrderable": true, "isDespatchUnit": true}
	if caseDims != nil {
		body["dims"] = map[string]any{"lengthMm": caseDims.l, "widthMm": caseDims.w, "heightMm": caseDims.h, "weightG": caseDims.g}
	}
	if err := w.expectOK2xx(w.doJSON(http.MethodPut, fmt.Sprintf("%s/products/%s/pack-levels/CASE", productMasterBaseURL, sku), body)); err != nil {
		return fmt.Errorf("declare CASE %s: %w", cs, err)
	}
	st.caseGtin[sku], st.caseUnit[sku] = cs, perCase
	w.mcs().caseGtin[sku] = cs // read by mastercase_test.go's caseGtinResolves step
	return nil
}

// generate calls POST /v1/transfer-proposals:generate over an explicit snapshot
// of one lane origin -> destination: the destination has 0 stock and a target
// of `deficit` (so its deficit is exactly that), the origin has `supply`
// available and no safety stock. The proposal for the SKU, if any, goes to st.prop.
func (st *p5State) generate(w *world, sku string, deficit, supply int) error {
	sku = w.rs(sku)
	origin, dest := w.rs("P5B-O-<run>"), w.rs("P5B-D-<run>")
	asOf := time.Now().UTC().Truncate(time.Second).Format(time.RFC3339)
	pos := func(site string, avail int) map[string]any {
		return map[string]any{"Site": site, "SKU": sku, "Available": avail, "AsOf": asOf}
	}
	pol := func(site string, target int) map[string]any {
		return map[string]any{"Version": p5DemandPlan, "Site": site, "SKU": sku, "SafetyStock": 0, "TargetStock": target, "UnitPriority": 5}
	}
	body := map[string]any{
		"asOf":      asOf,
		"positions": []any{pos(origin, supply), pos(dest, 0)},
		"policies":  []any{pol(origin, 0), pol(dest, deficit)},
		"lanes": []any{map[string]any{"Origin": origin, "Destination": dest,
			"LeadTime": int64(24 * time.Hour), "UnitHandlingCost": 1, "Enabled": true}},
	}
	if err := w.doJSON(http.MethodPost, p5NipBase+"/v1/transfer-proposals:generate", body); err != nil {
		return err
	}
	if w.last.status != http.StatusOK {
		return fmt.Errorf("generate answered %d (body: %s)", w.last.status, w.last.body)
	}
	st.propBody = string(w.last.body)
	st.prop = nil
	props, _ := w.last.json()["proposals"].([]any)
	for _, p := range props {
		if m, ok := p.(map[string]any); ok && m["SKU"] == sku {
			st.prop = m
		}
	}
	return nil
}

func (st *p5State) proposalIs(cases, mult, req, rounded, rem int) error {
	if st.prop == nil {
		return fmt.Errorf("no proposal for the SKU: %s", st.propBody)
	}
	return caseFieldsAre(st.prop, rounded, cases, mult, req, rounded, rem, st.propBody)
}

// caseFieldsAre asserts the quantity and the five ADR 0017 fields of one
// proposal / approval / transfer view. quantityKey differs per payload.
func caseFieldsAre(m map[string]any, quantity, cases, mult, req, rounded, rem int, raw any) error {
	num := func(k string) (int, bool) {
		v, ok := m[k]
		if !ok {
			return 0, false
		}
		f, ok := toFloat(v)
		return int(f), ok
	}
	q, ok := num("quantity")
	if !ok {
		q, ok = num("Quantity")
	}
	if !ok || q != quantity {
		return fmt.Errorf("quantity = %v, want %d (%v)", q, quantity, raw)
	}
	for k, want := range map[string]int{"caseMultiple": mult, "cases": cases, "requestedQtyEaches": req, "roundedQtyEaches": rounded, "remainderEaches": rem} {
		if got, ok := num(k); !ok || got != want {
			return fmt.Errorf("%s = %v (present=%v), want %d (%v)", k, got, ok, want, raw)
		}
	}
	return nil
}

func problemType(w *world) string {
	t, _ := w.last.json()["type"].(string)
	return t
}

func problemIs(w *world, status int, slug string) error {
	if w.last.status != status {
		return fmt.Errorf("status = %d, want %d (body: %s)", w.last.status, status, w.last.body)
	}
	if t := problemType(w); !strings.HasSuffix(t, "/"+slug) {
		return fmt.Errorf("problem type = %q, want slug %q (body: %s)", t, slug, w.last.body)
	}
	return nil
}

// seal posts seal-package for the claimed task; ownContainer adds the ADR 0056
// opt-in with lineQuantity.
func (st *p5State) seal(w *world, sku string, ownContainer bool, lineQty int) error {
	if st.taskID == "" {
		return fmt.Errorf("no PACK task has been claimed in this scenario")
	}
	body := map[string]any{"stationId": st.station, "contents": []string{w.rs(sku)}}
	if ownContainer {
		body["shipInOwnContainer"] = true
		body["lineQuantity"] = lineQty
	}
	return w.doJSON(http.MethodPost, fmt.Sprintf("%s/tasks/%s/seal-package", fulfillmentBaseURL, st.taskID), body)
}

func (st *p5State) packageIsOwnContainer(got map[string]any, sku string, l, wd, h, g int, raw any) error {
	if got["status"] != "SEALED" || got["shipInOwnContainer"] != true || got["containerSource"] != "CASE" {
		return fmt.Errorf("package is not a sealed own-container CASE: %v", raw)
	}
	c, _ := got["container"].(map[string]any)
	if c == nil {
		return fmt.Errorf("package has no container: %v", raw)
	}
	if c["gtin"] != st.caseGtin[sku] {
		return fmt.Errorf("container gtin = %v, want the CASE GTIN %s (%v)", c["gtin"], st.caseGtin[sku], raw)
	}
	for k, want := range map[string]int{"lengthMm": l, "widthMm": wd, "heightMm": h, "weightG": g} {
		if f, _ := toFloat(c[k]); int(f) != want {
			return fmt.Errorf("container %s = %v, want %d (%v)", k, c[k], want, raw)
		}
	}
	return nil
}

// postApprove POSTs /v1/transfers:approve with an explicit Idempotency-Key (doJSON
// mints a fresh key per call, which cannot express a replay).
func (st *p5State) postApprove(w *world, key string, body map[string]any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, p5NipBase+"/v1/transfers:approve", bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("HTTP POST approve: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	w.last = httpResult{status: resp.StatusCode, header: resp.Header, body: raw}
	return nil
}

func (st *p5State) newApproval(w *world, qty int, sku, origin, dest string) {
	st.apprKey = newIdempotencyKey()
	st.xfer = nil
	st.apprReq = map[string]any{
		"originSiteId": w.rs(origin), "destinationSiteId": w.rs(dest), "sku": w.rs(sku), "quantity": qty,
		"policyVersion": p5DemandPlan, "operatorReason": "e2e phase5 case-multiple",
		"proposalAsOf": time.Now().UTC().Truncate(time.Second).Format(time.RFC3339),
	}
}

func isFactsIncomplete(w *world) bool {
	return w.last.status == http.StatusUnprocessableEntity && strings.HasSuffix(problemType(w), "/facts-incomplete")
}

// approve drives the approval the way an operator would while the facts are still
// travelling over Kafka: 422 facts-incomplete means "NIP has not ingested them yet"
// and persists nothing, so the SAME request (same Idempotency-Key, so a retry that
// races a success can only replay) is repeated until NIP answers anything else.
func (st *p5State) approve(w *world, qty int, sku, origin, dest string) error {
	st.newApproval(w, qty, sku, origin, dest)
	err := eventually(func() error {
		if err := st.postApprove(w, st.apprKey, st.apprReq); err != nil {
			return err
		}
		if isFactsIncomplete(w) {
			return fmt.Errorf("NIP does not have the facts yet: %s", w.last.body)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if w.last.status == http.StatusOK {
		st.xfer = w.last.json()
	}
	return nil
}

// approveRefusedNaming repeats the approval until NIP refuses it 422 facts-incomplete
// with a detail that names the site (ADR 0018 ValidateApproval wording); an approval
// that is ACCEPTED is a product bug and fails at once.
func (st *p5State) approveRefusedNaming(w *world, qty int, sku, origin, dest, site string, stale bool) error {
	st.newApproval(w, qty, sku, origin, dest)
	deadline := time.Now().Add(eventualWaitTimeout)
	for {
		if err := st.postApprove(w, st.apprKey, st.apprReq); err != nil {
			return err
		}
		if w.last.status == http.StatusOK {
			return fmt.Errorf("approval from %s was ACCEPTED, want 422 facts-incomplete (body: %s)", site, w.last.body)
		}
		detail, _ := w.last.json()["detail"].(string)
		named := strings.Contains(detail, "origin site "+site+" ")
		if isFactsIncomplete(w) && named && (!stale || strings.Contains(detail, "stale") && strings.Contains(detail, "excluded from the planning snapshot")) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("no 422 facts-incomplete naming origin site %s (stale=%v) within %s; last answer %d: %s",
				site, stale, eventualWaitTimeout, w.last.status, w.last.body)
		}
		time.Sleep(eventualWaitPoll)
	}
}

func (st *p5State) getTransfer(w *world) (map[string]any, string, error) {
	id, _ := st.xfer["transferId"].(string)
	if id == "" {
		return nil, "", fmt.Errorf("no transfer approved")
	}
	if err := w.doJSON(http.MethodGet, p5NipBase+"/v1/transfers/"+id, nil); err != nil {
		return nil, "", err
	}
	if w.last.status != http.StatusOK {
		return nil, "", fmt.Errorf("GET transfer %s = %d (%s)", id, w.last.status, w.last.body)
	}
	return w.last.json(), string(w.last.body), nil
}

// noCaseFields asserts a plain SKU's transfer: the plain quantity, none of the ADR 0017 fields.
func noCaseFields(m map[string]any, quantity int, raw any) error {
	if q, _ := toFloat(m["quantity"]); int(q) != quantity {
		return fmt.Errorf("quantity = %v, want %d (%v)", m["quantity"], quantity, raw)
	}
	for _, k := range []string{"caseMultiple", "cases", "requestedQtyEaches", "roundedQtyEaches", "remainderEaches"} {
		if _, has := m[k]; has {
			return fmt.Errorf("plain SKU transfer carries %q (%v)", k, raw)
		}
	}
	return nil
}
