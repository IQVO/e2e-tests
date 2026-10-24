package main

// Step definitions for features/seller_inbound.feature: the cross-org
// seller -> network -> vendor-node inbound flow across seller-inbound,
// vendor-connectivity, node-topology, offer-availability and node-performance.
//
// Black-box and REST-only, through Kong (http://localhost:8000/api/<context>).
// The scenario is self-contained: it creates its own vendor, node, catalogue
// item and seller, so it neither depends on nor disturbs seeded data. Every
// GTIN-14 / SSCC-18 is a valid GS1 number, unique per run and per call.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/cucumber/godog"
)

var (
	sellerInboundBaseURL      = envOrDefault("SELLER_INBOUND_BASE_URL", "http://localhost:8000/api/seller-inbound")
	vendorConnectivityBaseURL = envOrDefault("VENDOR_CONNECTIVITY_BASE_URL", "http://localhost:8000/api/vendor-connectivity")
	nodeTopologyBaseURL       = envOrDefault("NODE_TOPOLOGY_BASE_URL", "http://localhost:8000/api/node-topology")
	offerAvailabilityBaseURL  = envOrDefault("OFFER_AVAILABILITY_BASE_URL", "http://localhost:8000/api/offer-availability")
	nodePerformanceBaseURL    = envOrDefault("NODE_PERFORMANCE_BASE_URL", "http://localhost:8000/api/node-performance")

	// siEventualTimeout is longer than the suite default: the local views of
	// seller-inbound / vendor-connectivity / node-performance are fed over
	// Kafka by consumer groups that may have just been created.
	siEventualTimeout = 90 * time.Second

	siSeq atomic.Int64 // process-wide: keeps every GTIN / SSCC unique inside one run
)

const (
	siGtinPrefix = "9502"    // GTIN-14 payload = indicator + 9502 + 7 run digits + 1 ordinal
	siSsccPrefix = "3401235" // SSCC payload = extension 0 + 3401235 + 6 run digits + 2 seq digits + 1 zero
	siPerCase    = 12
	siMaxWeightG = 20000
	siRegionCode = "USEAST"
)

type siCarton struct {
	sscc   string
	issued bool
}

type siState struct {
	vendorID, vendorNodeCode string
	nodeID                   string
	itemID                   string
	eachGtin, caseGtin       string
	sellerID                 string
	planID                   string
	shipmentID               string
	cartons                  []siCarton
	receiptTx                string
	problem                  map[string]any // body of the last refused call
	status                   int            // status of the last refused call
}

func (w *world) sis() *siState {
	if w.si == nil {
		w.si = &siState{}
	}
	return w.si
}

func siEventually(fn func() error) error {
	deadline := time.Now().Add(siEventualTimeout)
	var lastErr error
	for {
		if err := fn(); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("condition not met within %s: %w", siEventualTimeout, lastErr)
		}
		time.Sleep(eventualWaitPoll)
	}
}

func (w *world) siGtin(indicator string) string {
	p := indicator + siGtinPrefix + w.runDigits()[:7] + fmt.Sprintf("%d", siSeq.Add(1)%10)
	return p + gs1CheckDigit(p)
}

func (w *world) siSscc() string {
	p := fmt.Sprintf("0%s%s%02d0", siSsccPrefix, w.runDigits()[:6], siSeq.Add(1)%100)
	return p + gs1CheckDigit(p)
}

func validSscc(s string) bool {
	if len(s) != 18 {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return gs1CheckDigit(s[:17]) == s[17:]
}

// refused records the last response as a refusal and checks status + problem slug.
func (w *world) siExpectProblem(status int, slug string) error {
	got := w.last.json()
	s := w.sis()
	s.problem, s.status = got, w.last.status
	if w.last.status != status || got["type"] != slug {
		return fmt.Errorf("want %d problem %q, got %d (body: %s)", status, slug, w.last.status, w.last.body)
	}
	return nil
}

func (w *world) siServicesHealthy() error {
	for name, base := range map[string]string{
		"seller-inbound": sellerInboundBaseURL, "vendor-connectivity": vendorConnectivityBaseURL,
		"node-topology": nodeTopologyBaseURL, "offer-availability": offerAvailabilityBaseURL,
		"node-performance": nodePerformanceBaseURL,
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
// vendor + node (vendor-connectivity, node-topology)
// ---------------------------------------------------------------------

func (w *world) siVendorTransaction(txID string) (map[string]any, error) {
	q := url.Values{"vendorId": {w.sis().vendorID}}
	if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/vendor/transactions/%s?%s", vendorConnectivityBaseURL, txID, q.Encode()), nil); err != nil {
		return nil, err
	}
	if w.last.status != http.StatusOK {
		return nil, fmt.Errorf("GET transaction %s = %d (%s)", txID, w.last.status, w.last.body)
	}
	return w.last.json(), nil
}

func (w *world) siTransactionSucceeds(txID string) (map[string]any, error) {
	var tx map[string]any
	err := siEventually(func() error {
		t, err := w.siVendorTransaction(txID)
		if err != nil {
			return err
		}
		if t["status"] == "FAILURE" {
			return fmt.Errorf("transaction %s FAILED: %s", txID, w.last.body)
		}
		if t["status"] != "SUCCESS" {
			return fmt.Errorf("transaction %s is %v", txID, t["status"])
		}
		tx = t
		return nil
	})
	return tx, err
}

// vendorNodeReady onboards a vendor, registers an INBOUND_CROSS_DOCK node through
// the Vendor API, declares an operational profile with inbound limits straight on
// node-topology (the Vendor API's profile request carries no `inbound` block) and
// activates it.
func (w *world) vendorNodeReady(vendor, code string, maxWeightG int) error {
	s := w.sis()
	s.vendorID, s.vendorNodeCode = w.rs(vendor), w.rs(code)
	if err := w.expectOK2xx(w.doJSON(http.MethodPost, vendorConnectivityBaseURL+"/vendors",
		map[string]any{"vendorId": s.vendorID, "name": "E2E vendor " + s.vendorID, "ackWindowMinutes": 120})); err != nil {
		return fmt.Errorf("onboard vendor: %w", err)
	}
	// regionId is the node-topology region id (not its code): read it from the region list.
	if err := w.doJSON(http.MethodGet, nodeTopologyBaseURL+"/regions", nil); err != nil {
		return err
	}
	var regions []any
	_ = json.Unmarshal(w.last.body, &regions)
	regionID := ""
	for _, r := range regions {
		if m, ok := r.(map[string]any); ok && regionID == "" {
			regionID, _ = m["regionId"].(string)
			if m["code"] == siRegionCode {
				regionID, _ = m["regionId"].(string)
				break
			}
		}
	}
	if regionID == "" {
		return fmt.Errorf("node-topology has no region (GET /regions: %s)", w.last.body)
	}
	if err := w.doJSON(http.MethodPost, vendorConnectivityBaseURL+"/vendor/nodes", map[string]any{
		"vendorId": s.vendorID, "vendorNodeCode": s.vendorNodeCode, "nodeType": "INBOUND_CROSS_DOCK",
		"regionId": regionID, "timezone": "America/Sao_Paulo", "postalCode": "78701", "countryCode": "US",
	}); err != nil {
		return err
	}
	if w.last.status != http.StatusAccepted {
		return fmt.Errorf("POST /vendor/nodes = %d, want 202 (%s)", w.last.status, w.last.body)
	}
	tx := fmt.Sprint(w.last.json()["transactionId"])
	done, err := w.siTransactionSucceeds(tx)
	if err != nil {
		return fmt.Errorf("node registration: %w", err)
	}
	s.nodeID, _ = done["nodeId"].(string)
	if s.nodeID == "" {
		return fmt.Errorf("node registration transaction carries no nodeId: %v", done)
	}
	if err := w.expectOK2xx(w.doJSON(http.MethodPut, fmt.Sprintf("%s/nodes/%s/operational-profile", nodeTopologyBaseURL, s.nodeID), map[string]any{
		"profileRevision": 1, "timezone": "America/Sao_Paulo",
		"operatingHoursByDay": map[string]any{"monday": map[string]any{"start": "06:00", "end": "22:00"}},
		"handlingTimeMinutes": 60, "cutoffs": []map[string]any{{"localTime": "11:00"}},
		"throughputCap": map[string]any{"value": 400, "timeUnit": "HOUR", "unit": "ORDER"},
		"inbound": map[string]any{"maxCartonWeightG": maxWeightG, "maxCartonDimsMm": map[string]any{"l": 600, "w": 400, "h": 300},
			"acceptsPallets": false, "requiresSscc": false},
		"declaredBy": "VENDOR",
	})); err != nil {
		return fmt.Errorf("declare operational profile: %w", err)
	}
	if err := w.expectOK2xx(w.doJSON(http.MethodPost, fmt.Sprintf("%s/nodes/%s:activate", nodeTopologyBaseURL, s.nodeID), nil)); err != nil {
		return fmt.Errorf("activate node: %w", err)
	}
	n := w.last.json()
	caps, _ := n["capabilities"].(map[string]any)
	if n["status"] != "ACTIVE" || caps["inbound"] != true {
		return fmt.Errorf("node %s is not ACTIVE with the INBOUND capability: %s", s.nodeID, w.last.body)
	}
	return nil
}

// ---------------------------------------------------------------------
// offer-availability
// ---------------------------------------------------------------------

func (w *world) catalogItemWithCase(perCase int) error {
	s := w.sis()
	s.eachGtin, s.caseGtin = w.siGtin("0"), w.siGtin("1")
	if err := w.expectOK2xx(w.doJSON(http.MethodPost, offerAvailabilityBaseURL+"/catalog-items", map[string]any{
		"title": "E2E mastercase item " + s.eachGtin, "lengthMm": 100, "widthMm": 80, "heightMm": 60, "weightG": 300,
		"hazmat": false, "fragile": false, "identifiers": map[string]any{"gtin": s.eachGtin},
	})); err != nil {
		return fmt.Errorf("create catalogue item: %w", err)
	}
	s.itemID, _ = w.last.json()["catalogItemId"].(string)
	if s.itemID == "" {
		return fmt.Errorf("no catalogItemId in %s", w.last.body)
	}
	if err := w.expectOK2xx(w.doJSON(http.MethodPut, fmt.Sprintf("%s/catalog-items/%s/packaging/EACH", offerAvailabilityBaseURL, s.itemID),
		map[string]any{"gtin": s.eachGtin, "unitsPerPack": 1})); err != nil {
		return fmt.Errorf("declare EACH: %w", err)
	}
	if err := w.expectOK2xx(w.doJSON(http.MethodPut, fmt.Sprintf("%s/catalog-items/%s/packaging/CASE", offerAvailabilityBaseURL, s.itemID),
		map[string]any{"gtin": s.caseGtin, "childGtin": s.eachGtin, "unitsPerPack": perCase, "dimsMm": map[string]any{"lengthMm": 400, "widthMm": 300, "heightMm": 200}, "grossWeightG": 5200})); err != nil {
		return fmt.Errorf("declare CASE: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------
// seller-inbound
// ---------------------------------------------------------------------

func (w *world) sellerOnboarded(seller string) error {
	s := w.sis()
	s.sellerID = w.rs(seller)
	if err := w.doJSON(http.MethodPost, sellerInboundBaseURL+"/sellers", map[string]any{"sellerId": s.sellerID, "name": "E2E seller " + s.sellerID}); err != nil {
		return err
	}
	if w.last.status != http.StatusCreated || w.last.json()["state"] != "ACTIVE" {
		return fmt.Errorf("onboard seller = %d, want 201 ACTIVE (%s)", w.last.status, w.last.body)
	}
	return nil
}

// planDraftEventually creates the plan, retrying while seller-inbound's local
// views (fed over Kafka) have not yet learned the node or the packaging. Only
// those two refusals are treated as "not yet"; anything else fails at once.
func (w *world) planDraftEventually(packing string, qty int) error {
	s := w.sis()
	gtin := s.caseGtin
	if packing == "INDIVIDUAL" {
		gtin = s.eachGtin
	}
	return siEventually(func() error {
		if err := w.doJSON(http.MethodPost, sellerInboundBaseURL+"/inbound-plans", map[string]any{
			"sellerId": s.sellerID, "destinationNodeId": s.nodeID, "packingOption": packing,
			"lines": []map[string]any{{"catalogItemId": s.itemID, "gtin": gtin, "qtyEaches": qty}},
		}); err != nil {
			return err
		}
		if w.last.status == http.StatusCreated {
			p := w.last.json()
			if p["state"] != "DRAFT" {
				return fmt.Errorf("new plan is %v, want DRAFT", p["state"])
			}
			s.planID, _ = p["planId"].(string)
			return nil
		}
		slug := w.last.json()["type"]
		return fmt.Errorf("plan refused %d %v (%s)", w.last.status, slug, w.last.body)
	})
}

func (w *world) planConfirmed() error {
	s := w.sis()
	if err := w.doJSON(http.MethodPost, fmt.Sprintf("%s/inbound-plans/%s:confirm", sellerInboundBaseURL, s.planID), nil); err != nil {
		return err
	}
	if w.last.status != http.StatusOK || w.last.json()["state"] != "CONFIRMED" {
		return fmt.Errorf("confirm plan = %d, want 200 CONFIRMED (%s)", w.last.status, w.last.body)
	}
	return nil
}

func (w *world) planConfirmRefused(slug string) error {
	s := w.sis()
	if err := w.doJSON(http.MethodPost, fmt.Sprintf("%s/inbound-plans/%s:confirm", sellerInboundBaseURL, s.planID), nil); err != nil {
		return err
	}
	return w.siExpectProblem(http.StatusConflict, slug)
}

func (w *world) sellerSuspended() error {
	if err := w.doJSON(http.MethodPost, fmt.Sprintf("%s/sellers/%s:suspend", sellerInboundBaseURL, w.sis().sellerID),
		map[string]any{"reason": "e2e: repeated label violations"}); err != nil {
		return err
	}
	if w.last.status != http.StatusOK || w.last.json()["state"] != "SUSPENDED" {
		return fmt.Errorf("suspend seller = %d, want 200 SUSPENDED (%s)", w.last.status, w.last.body)
	}
	return nil
}

func (w *world) shipmentOpened() error {
	s := w.sis()
	if err := w.doJSON(http.MethodPost, fmt.Sprintf("%s/inbound-plans/%s/shipments", sellerInboundBaseURL, s.planID), nil); err != nil {
		return err
	}
	sh := w.last.json()
	if w.last.status != http.StatusCreated || sh["state"] != "DRAFT" || sh["nodeId"] != s.nodeID || sh["sellerId"] != s.sellerID {
		return fmt.Errorf("open shipment = %d, want 201 DRAFT for node %s seller %s (%s)", w.last.status, s.nodeID, s.sellerID, w.last.body)
	}
	s.shipmentID, _ = sh["shipmentId"].(string)
	return nil
}

func (w *world) cartonBody(qty, weightG int, sscc string) map[string]any {
	s := w.sis()
	b := map[string]any{
		"kind":         "HOMOGENEOUS_CASE",
		"contents":     []map[string]any{{"catalogItemId": s.itemID, "gtin": s.caseGtin, "qtyEaches": qty}},
		"grossWeightG": weightG, "dimsMm": map[string]any{"l": 400, "w": 300, "h": 200},
	}
	if sscc != "" {
		b["sscc"] = sscc
	}
	return b
}

func (w *world) postCarton(body map[string]any) error {
	return w.doJSON(http.MethodPost, fmt.Sprintf("%s/inbound-shipments/%s/cartons", sellerInboundBaseURL, w.sis().shipmentID), body)
}

// addCartons adds n cartons of qty eaches. supplied > 0 makes the LAST `supplied`
// cartons carry a seller-supplied valid SSCC; the others get a network-issued one.
func (w *world) addCartons(n, qty, supplied int) error {
	s := w.sis()
	for i := 0; i < n; i++ {
		sscc := ""
		if i >= n-supplied {
			sscc = w.siSscc()
		}
		if err := w.postCarton(w.cartonBody(qty, 5200, sscc)); err != nil {
			return err
		}
		if w.last.status != http.StatusCreated {
			return fmt.Errorf("add carton %d = %d, want 201 (%s)", i+1, w.last.status, w.last.body)
		}
		res := w.last.json()
		c, _ := res["carton"].(map[string]any)
		got, _ := c["sscc"].(string)
		issued, _ := res["ssccIssued"].(bool)
		if sscc != "" && (got != sscc || issued) {
			return fmt.Errorf("seller-supplied SSCC %s: got sscc=%s ssccIssued=%v, want it kept and not issued (%s)", sscc, got, issued, w.last.body)
		}
		if sscc == "" && !issued {
			return fmt.Errorf("carton %d: ssccIssued=false for a carton added without SSCC (%s)", i+1, w.last.body)
		}
		s.cartons = append(s.cartons, siCarton{sscc: got, issued: issued})
	}
	return nil
}

func (w *world) ssccsValidAndUnique(n int) error {
	cs := w.sis().cartons
	if len(cs) != n {
		return fmt.Errorf("%d cartons recorded, want %d", len(cs), n)
	}
	seen := map[string]bool{}
	for i, c := range cs {
		if !validSscc(c.sscc) {
			return fmt.Errorf("carton %d SSCC %q is not a valid 18-digit GS1 SSCC", i+1, c.sscc)
		}
		if seen[c.sscc] {
			return fmt.Errorf("SSCC %s is used twice", c.sscc)
		}
		seen[c.sscc] = true
	}
	return nil
}

func (w *world) cartonRefused(desc string, status int, slug string, qty, weightG int, dims bool) error {
	body := w.cartonBody(qty, weightG, "")
	if dims {
		body["dimsMm"] = map[string]any{"l": 900, "w": 300, "h": 200}
	}
	if err := w.postCarton(body); err != nil {
		return err
	}
	return w.siExpectProblem(status, slug)
}

func (w *world) duplicateSsccRefused(idx int) error {
	cs := w.sis().cartons
	if idx < 1 || idx > len(cs) {
		return fmt.Errorf("no carton %d", idx)
	}
	if err := w.postCarton(w.cartonBody(siPerCase, 5200, cs[idx-1].sscc)); err != nil {
		return err
	}
	return w.siExpectProblem(http.StatusConflict, "sscc-not-unique")
}

func (w *world) shipmentState(want string) (map[string]any, error) {
	if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/inbound-shipments/%s", sellerInboundBaseURL, w.sis().shipmentID), nil); err != nil {
		return nil, err
	}
	sh := w.last.json()
	if w.last.status != http.StatusOK || sh["state"] != want {
		return sh, fmt.Errorf("shipment is %d state=%v, want %s (%s)", w.last.status, sh["state"], want, w.last.body)
	}
	return sh, nil
}

func (w *world) shipRefused(slug string) error {
	if err := w.doJSON(http.MethodPost, fmt.Sprintf("%s/inbound-shipments/%s:ship", sellerInboundBaseURL, w.sis().shipmentID), nil); err != nil {
		return err
	}
	if err := w.siExpectProblem(http.StatusConflict, slug); err != nil {
		return err
	}
	_, err := w.shipmentState("DRAFT")
	return err
}

func (w *world) shipmentLabelled() error {
	if err := w.doJSON(http.MethodPost, fmt.Sprintf("%s/inbound-shipments/%s:label", sellerInboundBaseURL, w.sis().shipmentID), nil); err != nil {
		return err
	}
	if w.last.status != http.StatusOK || w.last.json()["state"] != "LABELLED" {
		return fmt.Errorf("label shipment = %d, want 200 LABELLED (%s)", w.last.status, w.last.body)
	}
	return nil
}

func (w *world) labelsOK(n, eaches int) error {
	s := w.sis()
	if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/inbound-shipments/%s/labels", sellerInboundBaseURL, s.shipmentID), nil); err != nil {
		return err
	}
	res := w.last.json()
	labels, _ := res["labels"].([]any)
	if w.last.status != http.StatusOK || res["state"] != "LABELLED" || len(labels) != n || res["destinationNodeId"] != s.nodeID {
		return fmt.Errorf("labels: %d state=%v, %d labels, node=%v; want %d labels for node %s (%s)", w.last.status, res["state"], len(labels), res["destinationNodeId"], n, s.nodeID, w.last.body)
	}
	byS := map[string]map[string]any{}
	for _, l := range labels {
		m := l.(map[string]any)
		byS[fmt.Sprint(m["sscc"])] = m
	}
	for i, c := range s.cartons {
		l, ok := byS[c.sscc]
		if !ok {
			return fmt.Errorf("no label for carton %d SSCC %s: %s", i+1, c.sscc, w.last.body)
		}
		te, _ := toFloat(l["totalEaches"])
		if l["gs1128"] != "(00)"+c.sscc || int(te) != eaches || l["destinationNodeId"] != s.nodeID || l["labelRef"] == "" {
			return fmt.Errorf("label of %s is wrong (want gs1128 (00)%s, %d eaches, node %s): %v", c.sscc, c.sscc, eaches, s.nodeID, l)
		}
	}
	return nil
}

func (w *world) shipmentShipped() error {
	if err := w.doJSON(http.MethodPost, fmt.Sprintf("%s/inbound-shipments/%s:ship", sellerInboundBaseURL, w.sis().shipmentID), nil); err != nil {
		return err
	}
	if w.last.status != http.StatusOK || w.last.json()["state"] != "SHIPPED" || w.last.json()["shippedAt"] == nil {
		return fmt.Errorf("ship = %d, want 200 SHIPPED with shippedAt (%s)", w.last.status, w.last.body)
	}
	return nil
}

// ---------------------------------------------------------------------
// vendor-connectivity: the poll and the receipt
// ---------------------------------------------------------------------

// treeCartons flattens a Vendor-API shipment tree into its cartons (pallet cartons + loose).
func treeCartons(sh map[string]any) []map[string]any {
	var out []map[string]any
	for _, p := range anySlice(sh["pallets"]) {
		if pm, ok := p.(map[string]any); ok {
			for _, c := range anySlice(pm["cartons"]) {
				if cm, ok := c.(map[string]any); ok {
					out = append(out, cm)
				}
			}
		}
	}
	for _, c := range anySlice(sh["cartons"]) {
		if cm, ok := c.(map[string]any); ok {
			out = append(out, cm)
		}
	}
	return out
}

func anySlice(v any) []any { s, _ := v.([]any); return s }

func (w *world) vendorPollsTree(eaches int) error {
	s := w.sis()
	q := url.Values{"vendorId": {s.vendorID}, "nodeId": {s.nodeID}}
	return siEventually(func() error {
		if err := w.doJSON(http.MethodGet, vendorConnectivityBaseURL+"/vendor/inbound-shipments?"+q.Encode(), nil); err != nil {
			return err
		}
		if w.last.status != http.StatusOK {
			return fmt.Errorf("poll = %d (%s)", w.last.status, w.last.body)
		}
		var sh map[string]any
		for _, x := range anySlice(w.last.json()["inboundShipments"]) {
			if m, ok := x.(map[string]any); ok && m["shipmentId"] == s.shipmentID {
				sh = m
			}
		}
		if sh == nil {
			return fmt.Errorf("shipment %s not in the poll yet: %s", s.shipmentID, w.last.body)
		}
		return w.checkTree(sh, "SHIPPED", eaches)
	})
}

func (w *world) checkTree(sh map[string]any, status string, eaches int) error {
	s := w.sis()
	if sh["status"] != status || sh["sellerId"] != s.sellerID || sh["nodeId"] != s.nodeID {
		return fmt.Errorf("tree header: status=%v sellerId=%v nodeId=%v, want %s / %s / %s", sh["status"], sh["sellerId"], sh["nodeId"], status, s.sellerID, s.nodeID)
	}
	cs := treeCartons(sh)
	if len(cs) != len(s.cartons) {
		return fmt.Errorf("tree has %d cartons, want %d: %v", len(cs), len(s.cartons), sh)
	}
	bySscc := map[string]map[string]any{}
	for _, c := range cs {
		bySscc[fmt.Sprint(c["sscc"])] = c
	}
	for i, c := range s.cartons {
		tc, ok := bySscc[c.sscc]
		if !ok {
			return fmt.Errorf("carton %d SSCC %s missing from the tree: %v", i+1, c.sscc, sh)
		}
		items := anySlice(tc["contents"])
		if len(items) != 1 {
			return fmt.Errorf("carton %s has %d content lines, want 1: %v", c.sscc, len(items), tc)
		}
		it := items[0].(map[string]any)
		q, _ := toFloat(it["qtyEaches"])
		// The seller declares the CASE GTIN on a HOMOGENEOUS_CASE carton; the tree must carry one GTIN of the item.
		if int(q) != eaches || (it["gtin"] != s.caseGtin && it["gtin"] != s.eachGtin) {
			return fmt.Errorf("carton %s content = %v, want gtin %s (case) or %s (each) x%d", c.sscc, it, s.caseGtin, s.eachGtin, eaches)
		}
	}
	return nil
}

func (w *world) vendorGetsShipment(status string) error {
	s := w.sis()
	return siEventually(func() error {
		q := url.Values{"vendorId": {s.vendorID}}
		if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/vendor/inbound-shipments/%s?%s", vendorConnectivityBaseURL, s.shipmentID, q.Encode()), nil); err != nil {
			return err
		}
		if w.last.status != http.StatusOK {
			return fmt.Errorf("GET /vendor/inbound-shipments/%s = %d (%s)", s.shipmentID, w.last.status, w.last.body)
		}
		return w.checkTree(w.last.json(), status, siPerCase)
	})
}

func (w *world) vendorPollNoLongerHasShipment() error {
	s := w.sis()
	q := url.Values{"vendorId": {s.vendorID}, "nodeId": {s.nodeID}}
	if err := w.doJSON(http.MethodGet, vendorConnectivityBaseURL+"/vendor/inbound-shipments?"+q.Encode(), nil); err != nil {
		return err
	}
	for _, x := range anySlice(w.last.json()["inboundShipments"]) {
		if m, ok := x.(map[string]any); ok && m["shipmentId"] == s.shipmentID {
			return fmt.Errorf("received shipment %s is still in the poll: %s", s.shipmentID, w.last.body)
		}
	}
	return nil
}

// fleetReceipt plays the fleet's role: the fleet ACL (network-fulfillment) runs in
// stub mode on this cluster, so the test submits the receipt itself.
func (w *world) fleetReports(recv1, counted2 int) error {
	s := w.sis()
	if len(s.cartons) != 3 {
		return fmt.Errorf("expected 3 cartons, have %d", len(s.cartons))
	}
	item := func(q int) []map[string]any { return []map[string]any{{"gtin": s.caseGtin, "qtyEaches": q}} }
	if err := w.doJSON(http.MethodPost, vendorConnectivityBaseURL+"/vendor/inbound-receipts", map[string]any{
		"vendorId": s.vendorID, "shipmentId": s.shipmentID, "nodeId": s.nodeID,
		"receivedAt": time.Now().UTC().Format(time.RFC3339),
		"cartons": []map[string]any{
			{"sscc": s.cartons[0].sscc, "status": "RECEIVED", "items": item(recv1), "complianceCodes": []string{}},
			{"sscc": s.cartons[1].sscc, "status": "RECEIVED", "items": item(counted2), "complianceCodes": []string{}},
			{"sscc": s.cartons[2].sscc, "status": "MISSING", "items": []map[string]any{}, "complianceCodes": []string{}},
		},
	}); err != nil {
		return err
	}
	if w.last.status != http.StatusAccepted {
		return fmt.Errorf("POST /vendor/inbound-receipts = %d, want 202 (%s)", w.last.status, w.last.body)
	}
	s.receiptTx, _ = w.last.json()["transactionId"].(string)
	if s.receiptTx == "" {
		return fmt.Errorf("202 without transactionId: %s", w.last.body)
	}
	return nil
}

func (w *world) receiptTransactionSucceeds() error {
	tx, err := w.siTransactionSucceeds(w.sis().receiptTx)
	if err != nil {
		return err
	}
	if tx["kind"] != "INBOUND_RECEIPT" {
		return fmt.Errorf("transaction kind = %v, want INBOUND_RECEIPT", tx["kind"])
	}
	if errs := anySlice(tx["errors"]); len(errs) != 0 {
		return fmt.Errorf("receipt transaction has errors: %v", errs)
	}
	return nil
}

// sellerSeesDiscrepancy reads the shipment the seller sees. No REST exposes the
// InboundDiscrepancyRecorded facts themselves, so the assertion is on the closed
// state, receivedAt and each carton's receiptStatus they are derived from.
func (w *world) sellerSeesDiscrepancy() error {
	s := w.sis()
	want := []string{"RECEIVED", "RECEIVED", "MISSING"}
	return siEventually(func() error {
		sh, err := w.shipmentState("CLOSED_WITH_DISCREPANCY")
		if err != nil {
			return err
		}
		if sh["receivedAt"] == nil {
			return fmt.Errorf("closed shipment has no receivedAt: %s", w.last.body)
		}
		got := map[string]string{}
		for _, c := range anySlice(sh["cartons"]) {
			m := c.(map[string]any)
			got[fmt.Sprint(m["sscc"])] = fmt.Sprint(m["receiptStatus"])
		}
		for i, c := range s.cartons {
			if got[c.sscc] != want[i] {
				return fmt.Errorf("carton %d (%s) receiptStatus = %q, want %s (%s)", i+1, c.sscc, got[c.sscc], want[i], w.last.body)
			}
		}
		return nil
	})
}

// ---------------------------------------------------------------------
// node-performance
// ---------------------------------------------------------------------

func (w *world) nodePerformanceExposes(rateNum, rateDen int) error {
	s := w.sis()
	want := float64(rateNum) / float64(rateDen)
	return siEventually(func() error {
		if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/scorecards/%s", nodePerformanceBaseURL, s.nodeID), nil); err != nil {
			return err
		}
		if w.last.status != http.StatusOK {
			return fmt.Errorf("GET scorecards/%s = %d (%s)", s.nodeID, w.last.status, w.last.body)
		}
		cards := anySlice(w.last.json()["scorecards"])
		if len(cards) == 0 {
			return fmt.Errorf("no scorecards: %s", w.last.body)
		}
		for _, c := range cards {
			m := c.(map[string]any)["metrics"].(map[string]any)
			acc, ok := toFloat(m["inboundAccuracyRate"])
			if !ok {
				return fmt.Errorf("window %v: inboundAccuracyRate absent: %s", c.(map[string]any)["window"], w.last.body)
			}
			if acc < want-0.001 || acc > want+0.001 {
				return fmt.Errorf("window %v: inboundAccuracyRate = %v, want %d/%d = %.4f (RECEIVED cartons / all cartons reported)", c.(map[string]any)["window"], acc, rateNum, rateDen, want)
			}
			t, ok := toFloat(m["inboundTimelinessP95Minutes"])
			if !ok || t < 0 {
				return fmt.Errorf("window %v: inboundTimelinessP95Minutes = %v, want a number >= 0 (%s)", c.(map[string]any)["window"], m["inboundTimelinessP95Minutes"], w.last.body)
			}
		}
		return nil
	})
}

// ---------------------------------------------------------------------
// wiring
// ---------------------------------------------------------------------

func registerSellerInboundSteps(sc *godog.ScenarioContext, w *world) {
	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		w.si = nil
		return ctx, nil
	})
	sc.Step(`^seller-inbound, vendor-connectivity, node-topology, offer-availability and node-performance are healthy$`, w.siServicesHealthy)
	sc.Step(`^vendor "([^"]*)" has an ACTIVE INBOUND_CROSS_DOCK node "([^"]*)" accepting cartons up to (\d+) g, registered through vendor-connectivity and node-topology$`,
		func(v, c string, g int) error { return w.vendorNodeReady(v, c, g) })
	sc.Step(`^a catalogue item with an each GTIN and a CASE level of (\d+) eaches exists in offer-availability$`, w.catalogItemWithCase)
	sc.Step(`^seller "([^"]*)" is onboarded in seller-inbound$`, w.sellerOnboarded)
	sc.Step(`^seller-inbound eventually accepts a (CASE_PACKED|INDIVIDUAL) plan of (\d+) eaches of the item for the node$`, w.planDraftEventually)
	sc.Step(`^the plan is confirmed$`, w.planConfirmed)
	sc.Step(`^the seller is suspended$`, w.sellerSuspended)
	sc.Step(`^confirming the plan is refused with problem "([^"]*)"$`, w.planConfirmRefused)
	sc.Step(`^a shipment is opened against the plan$`, w.shipmentOpened)
	sc.Step(`^(\d+) cartons of (\d+) eaches are added, the last (\d+) with a seller-supplied SSCC$`, w.addCartons)
	sc.Step(`^the (\d+) carton SSCCs are valid 18-digit GS1 numbers and unique$`, w.ssccsValidAndUnique)
	sc.Step(`^a carton of (\d+) eaches weighing (\d+) g is refused with (\d+) problem "([^"]*)"$`,
		func(q, g, st int, slug string) error { return w.cartonRefused("", st, slug, q, g, false) })
	sc.Step(`^a carton of (\d+) eaches with a length of 900 mm is refused with (\d+) problem "([^"]*)"$`,
		func(q, st int, slug string) error { return w.cartonRefused("", st, slug, q, 5200, true) })
	sc.Step(`^a carton reusing the SSCC of carton (\d+) is refused with problem "sscc-not-unique"$`, w.duplicateSsccRefused)
	sc.Step(`^shipping the unlabelled shipment is refused with problem "([^"]*)" and the shipment stays DRAFT$`, w.shipRefused)
	sc.Step(`^the shipment is labelled$`, w.shipmentLabelled)
	sc.Step(`^the label data has (\d+) labels of (\d+) eaches, one per carton SSCC, for the node$`, w.labelsOK)
	sc.Step(`^the shipment is shipped$`, w.shipmentShipped)
	sc.Step(`^vendor-connectivity eventually polls the shipment for the node as a SHIPPED tree of the 3 SSCCs with (\d+) eaches each$`, w.vendorPollsTree)
	sc.Step(`^the fleet role is played by the test: it reports carton 1 RECEIVED with (\d+) eaches, carton 2 RECEIVED with (\d+) eaches counted and carton 3 MISSING$`, w.fleetReports)
	sc.Step(`^the receipt transaction eventually succeeds$`, w.receiptTransactionSucceeds)
	sc.Step(`^seller-inbound eventually shows the shipment CLOSED_WITH_DISCREPANCY with cartons RECEIVED, RECEIVED and MISSING$`, w.sellerSeesDiscrepancy)
	sc.Step(`^the received shipment eventually leaves the vendor poll$`, func() error { return siEventually(w.vendorPollNoLongerHasShipment) })
	sc.Step(`^the vendor reads the shipment by id as CLOSED_WITH_DISCREPANCY$`, func() error { return w.vendorGetsShipment("CLOSED_WITH_DISCREPANCY") })
	sc.Step(`^node-performance eventually exposes for the node an inboundAccuracyRate of (\d+)/(\d+) and an inboundTimelinessP95Minutes of at least 0$`, w.nodePerformanceExposes)
}
