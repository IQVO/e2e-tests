package main

// Step definitions for features/mastercase.feature: mastercase / GTIN / SSCC
// handling (product-master pack levels, inbound-receiving cartons,
// inventory-storage containers, exception-management cases).
//
// Black-box and REST-only. Written to run against the live kind cluster through
// Kong (see the feature header for the env), where every base URL is
// http://localhost:8000/api/<context>. Nothing is injected: the GTINs and SSCCs
// are valid GS1 numbers computed per run, and every fact is produced by the
// services themselves.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/cucumber/godog"
)

const (
	typeCartonDiscrepancyRaised = "com.warehouse.wms.inbound-receiving.receipt.CartonDiscrepancyRaised"
	// Reference prefixes (GS1 company prefix stand-ins). Real check digits are
	// computed per number, so the services' own validators accept them.
	mcGtinPrefix  = "9501"    // GTIN-14 payload = indicator + 9501 + 8 run digits
	mcSsccPrefix  = "3401234" // SSCC payload = extension 0 + 3401234 + 8 run digits + 1 seq digit
	mcSettleDelay = 2 * time.Second
)

type mastercaseState struct {
	eachGtin map[string]string   // sku -> EACH GTIN
	caseGtin map[string]string   // sku -> CASE GTIN
	ssccs    map[string][]string // asn -> carton SSCCs, in scan order
	seq      int                 // next SSCC serial digit within this process
	caseBins []string            // one CASE bin per carton: seeded bulk bins hold ~22 eaches, so 36 eaches need 3
	looseBin string
	reserved map[string]int // demandRef -> eaches reserved
	scan     map[string]map[string]any
}

func (w *world) mcs() *mastercaseState {
	if w.mc == nil {
		w.mc = &mastercaseState{
			eachGtin: map[string]string{}, caseGtin: map[string]string{},
			ssccs: map[string][]string{}, reserved: map[string]int{},
			scan: map[string]map[string]any{},
		}
	}
	return w.mc
}

// gs1CheckDigit is the GS1 mod-10 check digit of payload (weights 3,1,3,... from the right).
func gs1CheckDigit(payload string) string {
	sum := 0
	for i := len(payload) - 1; i >= 0; i-- {
		d := int(payload[i] - '0')
		if (len(payload)-1-i)%2 == 0 {
			sum += 3 * d
		} else {
			sum += d
		}
	}
	return fmt.Sprintf("%d", (10-sum%10)%10)
}

// runDigits returns the run token as exactly 8 digits.
func (w *world) runDigits() string {
	s := w.rs("<run>")
	for len(s) < 8 {
		s = "0" + s
	}
	return s[len(s)-8:]
}

func (w *world) mcGtin(indicator string) string {
	p := indicator + mcGtinPrefix + w.runDigits()
	return p + gs1CheckDigit(p)
}

func (w *world) mcNextSscc() string {
	m := w.mcs()
	p := fmt.Sprintf("0%s%s%d", mcSsccPrefix, w.runDigits(), m.seq%10)
	m.seq++
	return p + gs1CheckDigit(p)
}

func (w *world) exceptionManagementIsHealthy() error {
	if err := w.doJSON(http.MethodGet, exceptionsBaseURL+"/healthz", nil); err != nil {
		return fmt.Errorf("exception-management not reachable: %w", err)
	}
	if w.last.status != http.StatusOK {
		return fmt.Errorf("exception-management /healthz returned %d, body=%s", w.last.status, w.last.body)
	}
	return nil
}

// ---------------------------------------------------------------------
// product-master
// ---------------------------------------------------------------------

// skuDeclaresPack registers the SKU and declares EACH (own GTIN) and CASE
// (qtyOfChild eaches) pack levels with valid, run-unique GS1 GTINs.
func (w *world) skuDeclaresPack(sku string, perCase int) error {
	sku = w.rs(sku)
	m := w.mcs()
	if err := w.registerProductInProductMaster(sku); err != nil {
		return fmt.Errorf("register %s: %w", sku, err)
	}
	// One SKU per scenario owns one GTIN pair; SKUs of different scenarios of the
	// same run need different GTINs (a GTIN identifies exactly one sku+level).
	each, cs := w.mcGtinFor(sku, "0"), w.mcGtinFor(sku, "1")
	m.eachGtin[sku], m.caseGtin[sku] = each, cs
	if err := w.expectOK2xx(w.doJSON(http.MethodPut, fmt.Sprintf("%s/products/%s/pack-levels/EACH", productMasterBaseURL, sku),
		map[string]any{"gtin": each, "isOrderable": true, "isDespatchUnit": false})); err != nil {
		return fmt.Errorf("declare EACH %s: %w", each, err)
	}
	if err := w.expectOK2xx(w.doJSON(http.MethodPut, fmt.Sprintf("%s/products/%s/pack-levels/CASE", productMasterBaseURL, sku),
		map[string]any{"gtin": cs, "childGtin": each, "qtyOfChild": perCase, "isOrderable": true, "isDespatchUnit": true})); err != nil {
		return fmt.Errorf("declare CASE %s: %w", cs, err)
	}
	return nil
}

// mcGtinFor makes the GTIN unique per (run, sku): the last digit of the 8 run
// digits is replaced by an ordinal of the SKUs this process has declared.
func (w *world) mcGtinFor(sku, indicator string) string {
	m := w.mcs()
	ord := 0
	for s := range m.eachGtin {
		if s != sku {
			ord++
		}
	}
	p := indicator + mcGtinPrefix + w.runDigits()[:7] + fmt.Sprintf("%d", ord%10)
	return p + gs1CheckDigit(p)
}

func (w *world) caseGtinResolves(sku string, perCase int) error {
	sku = w.rs(sku)
	g := w.mcs().caseGtin[sku]
	err := eventually(func() error {
		if err := w.doJSON(http.MethodGet, productMasterBaseURL+"/gtins/"+g, nil); err != nil {
			return err
		}
		if w.last.status != http.StatusOK {
			return fmt.Errorf("GET /gtins/%s = %d (%s)", g, w.last.status, w.last.body)
		}
		got := w.last.json()
		u, _ := toFloat(got["unitsPerPack"])
		if got["sku"] != sku || got["level"] != "CASE" || int(u) != perCase || got["active"] != true {
			return fmt.Errorf("GTIN %s resolves to %s", g, w.last.body)
		}
		return nil
	})
	if err != nil {
		return err
	}
	// product-master -> consumers is Kafka; none of the consumers' packaging
	// projections is readable over REST, so give the fan-out a moment. Anything
	// later than this surfaces as UnknownGtin/CaseQtyMismatch on the first scan.
	time.Sleep(mcSettleDelay)
	return nil
}

// ---------------------------------------------------------------------
// inbound-receiving
// ---------------------------------------------------------------------

func (w *world) cartonAsnRegistered(asn string, cartons, perCase int, sku string) error {
	asn, sku = w.rs(asn), w.rs(sku)
	m := w.mcs()
	cs, ok := m.caseGtin[sku]
	if !ok {
		return fmt.Errorf("SKU %s has no declared CASE GTIN in this process", sku)
	}
	var cc []map[string]any
	var ss []string
	for i := 0; i < cartons; i++ {
		s := w.mcNextSscc()
		ss = append(ss, s)
		cc = append(cc, map[string]any{"sscc": s, "kind": "HOMOGENEOUS_CASE",
			"contents": []map[string]any{{"lineNo": 1, "sku": sku, "gtin": cs, "qty": perCase}}})
	}
	err := eventually(func() error {
		return w.expectOK2xx(w.doJSON(http.MethodPost, inboundBaseURL+"/asns", map[string]any{
			"asnNumber": asn, "supplierRef": "ACME",
			"lines":   []map[string]any{{"lineNo": 1, "sku": sku, "expectedQty": cartons * perCase}},
			"cartons": cc,
		}))
	})
	if err != nil {
		return err
	}
	m.ssccs[asn] = ss
	return nil
}

func (w *world) postScan(asn string, idx int) (map[string]any, error) {
	asn = w.rs(asn)
	ss := w.mcs().ssccs[asn]
	if idx < 1 || idx > len(ss) {
		return nil, fmt.Errorf("ASN %s has %d cartons, no carton %d", asn, len(ss), idx)
	}
	rid, err := w.receiptIDFor(asn)
	if err != nil {
		return nil, err
	}
	if err := w.doJSON(http.MethodPost, fmt.Sprintf("%s/receipts/%s/scans", inboundBaseURL, rid),
		map[string]any{"kind": "SSCC", "id": ss[idx-1]}); err != nil {
		return nil, err
	}
	return w.last.json(), nil
}

func (w *world) allCartonsScannedAccepted(asn string) error {
	n := len(w.mcs().ssccs[w.rs(asn)])
	for i := 1; i <= n; i++ {
		res, err := w.postScan(asn, i)
		if err != nil {
			return err
		}
		if w.last.status != http.StatusCreated || res["accepted"] != true {
			return fmt.Errorf("scan of carton %d: status %d, want 201 accepted=true (body: %s)", i, w.last.status, w.last.body)
		}
		if d, _ := res["discrepancies"].([]any); len(d) != 0 {
			return fmt.Errorf("scan of carton %d raised discrepancies: %s", i, w.last.body)
		}
	}
	return nil
}

func (w *world) cartonScannedAgain(idx int, asn string) error {
	res, err := w.postScan(asn, idx)
	if err != nil {
		return err
	}
	w.mcs().scan[w.rs(asn)] = res
	return nil
}

func (w *world) scanRefusedAsDuplicate(idx int, asn string) error {
	asn = w.rs(asn)
	res := w.mcs().scan[asn]
	want := w.mcs().ssccs[asn][idx-1]
	if w.last.status != http.StatusOK || res["accepted"] != false {
		return fmt.Errorf("duplicate scan: status %d, want 200 accepted=false (body: %s)", w.last.status, w.last.body)
	}
	ds, _ := res["discrepancies"].([]any)
	if len(ds) != 1 {
		return fmt.Errorf("want exactly 1 discrepancy, got %s", w.last.body)
	}
	d, _ := ds[0].(map[string]any)
	if d["kind"] != "DuplicateSscc" || d["sscc"] != want {
		return fmt.Errorf("discrepancy = %v, want DuplicateSscc for %s", d, want)
	}
	return nil
}

func (w *world) receiptBookedOnce(asn string, eaches int) error {
	rec, err := w.readReceipt(w.rs(asn))
	if err != nil {
		return err
	}
	lines, _ := rec["lines"].([]any)
	if len(lines) != 1 {
		return fmt.Errorf("want 1 receipt line, got %v", rec["lines"])
	}
	got, _ := toFloat(lines[0].(map[string]any)["receivedGood"])
	if int(got) != eaches {
		return fmt.Errorf("receipt of %s booked %d good eaches, want %d (the duplicate must not double-count)", asn, int(got), eaches)
	}
	return nil
}

func (w *world) receiptClosesClean(asn string, eaches int) error {
	rec, err := w.readReceipt(w.rs(asn))
	if err != nil {
		return err
	}
	if rec["state"] != "Closed" {
		return fmt.Errorf("receipt state %v, want Closed", rec["state"])
	}
	for _, k := range []string{"discrepancies", "cartonDiscrepancies"} {
		if d, _ := rec[k].([]any); len(d) != 0 {
			return fmt.Errorf("receipt %s = %v, want none", k, d)
		}
	}
	if err := w.receiptBookedOnce(asn, eaches); err != nil {
		return err
	}
	if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/asns/%s/cartons", inboundBaseURL, w.rs(asn)), nil); err != nil {
		return err
	}
	cs, _ := w.last.json()["cartons"].([]any)
	if len(cs) != len(w.mcs().ssccs[w.rs(asn)]) {
		return fmt.Errorf("ASN cartons = %s", w.last.body)
	}
	for _, c := range cs {
		// GET /asns/{n}/cartons answers the lowercase "accepted" live while its openapi
		// documents "Accepted" (contract drift in inbound-receiving, reported in the PR):
		// compare case-insensitively so this scenario asserts behaviour, not casing.
		if st, _ := c.(map[string]any)["status"].(string); !strings.EqualFold(st, "Accepted") {
			return fmt.Errorf("carton %v is %v, want Accepted", c.(map[string]any)["sscc"], st)
		}
	}
	return nil
}

// ---------------------------------------------------------------------
// inventory-storage
// ---------------------------------------------------------------------

func (w *world) readContainer(lpn string) (map[string]any, error) {
	if err := w.doJSON(http.MethodGet, inventoryBaseURL+"/containers/"+lpn, nil); err != nil {
		return nil, err
	}
	if w.last.status != http.StatusOK {
		return nil, fmt.Errorf("GET /containers/%s = %d (%s)", lpn, w.last.status, w.last.body)
	}
	return w.last.json(), nil
}

func (w *world) containersSealed(n, perCase int, asn, sku string) error {
	asn, sku = w.rs(asn), w.rs(sku)
	ss := w.mcs().ssccs[asn]
	if len(ss) != n {
		return fmt.Errorf("ASN %s has %d cartons, want %d", asn, len(ss), n)
	}
	rid, _ := w.receiptIDFor(asn)
	for _, lpn := range ss {
		err := eventually(func() error {
			c, err := w.readContainer(lpn)
			if err != nil {
				return err
			}
			q, _ := toFloat(c["qtyEaches"])
			u, _ := toFloat(c["unitsPerPack"])
			if c["state"] != "SEALED" || c["sku"] != sku || int(q) != perCase || int(u) != perCase ||
				c["gtin"] != w.mcs().caseGtin[sku] || c["receiptId"] != rid {
				return fmt.Errorf("container %s = %s", lpn, w.last.body)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// seededBins picks one Active slot in a storage zone for sealed cases and one
// for loose eaches from the site layout facility-layout already serves.
func (w *world) seededBins(site string) error {
	if err := w.doJSON(http.MethodGet, facilityBaseURL+"/sites/"+site+"/layout", nil); err != nil {
		return err
	}
	if w.last.status != http.StatusOK {
		return fmt.Errorf("GET layout of %s = %d (%s)", site, w.last.status, w.last.body)
	}
	// candidates lists every Active slot of the zones whose id contains zoneSub.
	candidates := func(zoneSub string) []string {
		var out []string
		zs, _ := w.last.json()["zones"].([]any)
		for _, z := range zs {
			zm := z.(map[string]any)
			if id, _ := zm["zoneId"].(string); !strings.Contains(id, zoneSub) {
				continue
			}
			as, _ := zm["aisles"].([]any)
			for _, a := range as {
				ss, _ := a.(map[string]any)["slots"].([]any)
				for _, s := range ss {
					sm := s.(map[string]any)
					if st, ok := sm["status"].(string); ok && st != "Active" {
						continue
					}
					if c, _ := sm["locationCode"].(string); c != "" {
						out = append(out, c)
					}
				}
			}
		}
		return out
	}
	m := w.mcs()
	var err error
	if m.caseBins, err = w.binsWithRoom(candidates("STOR1-RSV"), 12, 3); err != nil {
		return err
	}
	// candidates reads w.last, which binWithRoom overwrote: re-read the layout.
	if err := w.doJSON(http.MethodGet, facilityBaseURL+"/sites/"+site+"/layout", nil); err != nil {
		return err
	}
	if m.looseBin, err = w.binWithRoom(candidates("STOR1-FWD"), 12); err != nil {
		return err
	}
	// facility-layout owns the bin's handling unit (ADR 0040): declare what each
	// seeded bin accepts, idempotently (PUT), so the scenario does not depend on
	// manual setup. inventory-storage learns it from LocationSlotHandlingUnitDeclared
	// (ADR 0047); the stow below therefore waits for that event to land.
	// A bin inventory-storage has never seen is registered with a capacity (PUT is
	// idempotent and only converges upward); known bins keep theirs.
	for _, b := range append(append([]string{}, m.caseBins...), m.looseBin) {
		if err := w.doJSON(http.MethodGet, inventoryBaseURL+"/bins/"+url.PathEscape(b), nil); err != nil {
			return err
		}
		if w.last.status != http.StatusNotFound {
			continue
		}
		if err := w.expectOK2xx(w.doJSON(http.MethodPut, inventoryBaseURL+"/bins/"+url.PathEscape(b), map[string]any{"capacity": 120})); err != nil {
			return fmt.Errorf("register bin %s: %w", b, err)
		}
	}
	units := map[string]string{m.looseBin: "EACH"}
	for _, b := range m.caseBins {
		units[b] = "CASE"
	}
	for bin, unit := range units {
		if err := w.expectOK2xx(w.doJSON(http.MethodPut, facilityBaseURL+"/locations/"+url.PathEscape(bin)+"/handling-unit",
			map[string]any{"handlingUnit": unit})); err != nil {
			return fmt.Errorf("declare handling unit %s on %s: %w", unit, bin, err)
		}
	}
	return nil
}

// binWithRoom returns the first bin of the candidates (preferring a completely
// empty one) that inventory-storage reports with at least need units free, so
// the scenario does not depend on which bins the inventory seed already filled.
func (w *world) binWithRoom(cands []string, need int) (string, error) {
	fallback := ""
	for _, c := range cands {
		if err := w.doJSON(http.MethodGet, inventoryBaseURL+"/bins/"+url.PathEscape(c), nil); err != nil {
			continue
		}
		if w.last.status == http.StatusNotFound {
			// never used: inventory-storage registers a bin on first stock, so it is empty.
			return c, nil
		}
		if w.last.status != http.StatusOK {
			continue
		}
		b := w.last.json()
		avail, _ := b["available"].(float64)
		occ, _ := b["occupied"].(float64)
		if int(avail) < need {
			continue
		}
		if occ == 0 {
			return c, nil
		}
		if fallback == "" {
			fallback = c
		}
	}
	if fallback == "" {
		return "", fmt.Errorf("no bin with %d free units among %d candidates", need, len(cands))
	}
	return fallback, nil
}

// binsWithRoom returns n distinct bins with at least need units free each.
func (w *world) binsWithRoom(cands []string, need, n int) ([]string, error) {
	var out []string
	for len(out) < n {
		b, err := w.binWithRoom(cands, need)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
		var rest []string
		for _, c := range cands {
			if c != b {
				rest = append(rest, c)
			}
		}
		cands = rest
	}
	return out, nil
}

func (w *world) stowContainer(idx int, asn string) error {
	asn = w.rs(asn)
	lpn := w.mcs().ssccs[asn][idx-1]
	bin := w.mcs().caseBins[(idx-1)%len(w.mcs().caseBins)]
	// The bin's handling unit reaches inventory-storage over Kafka: until it has
	// landed the stow fails closed with 409 bin-handling-unit-mismatch, so retry
	// that one answer (any other failure is real and returned at once).
	if err := eventually(func() error {
		if err := w.doJSON(http.MethodPost, fmt.Sprintf("%s/containers/%s:stow", inventoryBaseURL, lpn),
			map[string]any{"bin": bin}); err != nil {
			return err
		}
		if w.last.status == http.StatusConflict && strings.Contains(string(w.last.body), "bin-handling-unit-mismatch") {
			return fmt.Errorf("handling unit of %s not yet known to inventory-storage: %s", bin, w.last.body)
		}
		return w.expectOK2xx(nil)
	}); err != nil {
		return fmt.Errorf("stow %s into %s: %w", lpn, bin, err)
	}
	c := w.last.json()
	if c["binId"] != bin || c["state"] != "SEALED" {
		return fmt.Errorf("stowed container = %s", w.last.body)
	}
	return nil
}

func (w *world) allContainersStowed(asn string) error {
	for i := range w.mcs().ssccs[w.rs(asn)] {
		if err := w.stowContainer(i+1, asn); err != nil {
			return err
		}
	}
	return nil
}

func (w *world) usableIs(sku string, usable, loose, cases int) error {
	sku = w.rs(sku)
	return eventually(func() error {
		if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/inventory/%s/usable", inventoryBaseURL, url.PathEscape(sku)), nil); err != nil {
			return err
		}
		g := w.last.json()
		u, _ := toFloat(g["usable"])
		l, _ := toFloat(g["looseUsable"])
		c, _ := toFloat(g["reservableCases"])
		if w.last.status != http.StatusOK || int(u) != usable || int(l) != loose || int(c) != cases {
			return fmt.Errorf("usable of %s = %s, want usable %d loose %d reservableCases %d", sku, w.last.body, usable, loose, cases)
		}
		return nil
	})
}

func (w *world) breakContainer(idx int, asn string) error {
	asn = w.rs(asn)
	lpn := w.mcs().ssccs[asn][idx-1]
	if err := w.expectOK2xx(w.doJSON(http.MethodPost, fmt.Sprintf("%s/containers/%s:break", inventoryBaseURL, lpn),
		map[string]any{"looseBin": w.mcs().looseBin})); err != nil {
		return fmt.Errorf("break %s into %s: %w", lpn, w.mcs().looseBin, err)
	}
	return nil
}

func (w *world) containerState(idx int, asn, want string, qty int) error {
	asn = w.rs(asn)
	c, err := w.readContainer(w.mcs().ssccs[asn][idx-1])
	if err != nil {
		return err
	}
	q, _ := toFloat(c["qtyEaches"])
	if c["state"] != want || int(q) != qty {
		return fmt.Errorf("container %d = %s, want state %s qtyEaches %d", idx, w.last.body, want, qty)
	}
	return nil
}

func (w *world) reserveEaches(qty int, sku, demand string) error {
	sku, demand = w.rs(sku), w.rs(demand)
	if err := w.expectOK2xx(w.doJSON(http.MethodPost, inventoryBaseURL+"/reservations",
		map[string]any{"sku": sku, "quantity": qty, "demandRef": demand})); err != nil {
		return fmt.Errorf("reserve %d eaches of %s: %w", qty, sku, err)
	}
	w.mcs().reserved[demand] = qty
	return nil
}

func (w *world) onHandIs(sku, demand string, want int) error {
	sku, demand = w.rs(sku), w.rs(demand)
	if err := w.doJSON(http.MethodGet, inventoryBaseURL+"/reservations?demandRef="+url.QueryEscape(demand), nil); err != nil {
		return err
	}
	var rs []any
	if err := json.Unmarshal(w.last.body, &rs); err != nil {
		return fmt.Errorf("reservations body %s: %w", w.last.body, err)
	}
	reserved := 0
	for _, r := range rs {
		rm := r.(map[string]any)
		if rm["status"] == "ACTIVE" {
			q, _ := toFloat(rm["quantity"])
			reserved += int(q)
		}
	}
	if reserved != w.mcs().reserved[demand] {
		return fmt.Errorf("active reserved = %d, want %d (body %s)", reserved, w.mcs().reserved[demand], w.last.body)
	}
	if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/inventory/%s/usable", inventoryBaseURL, url.PathEscape(sku)), nil); err != nil {
		return err
	}
	u, _ := toFloat(w.last.json()["usable"])
	if int(u)+reserved != want {
		return fmt.Errorf("on hand (usable %d + reserved %d) = %d, want %d", int(u), reserved, int(u)+reserved, want)
	}
	return nil
}

// ---------------------------------------------------------------------
// exception-management
// ---------------------------------------------------------------------

func (w *world) exceptionCaseEventually(queue, kind, asn string, idx int) error {
	asn = w.rs(asn)
	rid, err := w.receiptIDFor(asn)
	if err != nil {
		return err
	}
	sscc := w.mcs().ssccs[asn][idx-1]
	subject := rid + "/" + sscc + "/" + kind
	return eventually(func() error {
		q := url.Values{"sourceType": {typeCartonDiscrepancyRaised}, "queue": {queue}, "status": {"OPEN"}}
		if err := w.doJSON(http.MethodGet, exceptionsBaseURL+"/exceptions?"+q.Encode(), nil); err != nil {
			return err
		}
		var cs []any
		if err := json.Unmarshal(w.last.body, &cs); err != nil {
			return fmt.Errorf("exceptions body %s: %w", w.last.body, err)
		}
		for _, c := range cs {
			cm := c.(map[string]any)
			dk, _ := cm["dedupKey"].(map[string]any)
			if dk["sourceSubject"] != subject {
				continue
			}
			p, _ := cm["sourcePayload"].(map[string]any)
			if cm["queue"] != queue || p["kind"] != kind || p["sscc"] != sscc || p["receipt_id"] != rid {
				return fmt.Errorf("case %v does not carry kind %s sscc %s receipt %s", cm["caseId"], kind, sscc, rid)
			}
			return nil
		}
		return fmt.Errorf("no OPEN %s case with subject %s among %d in queue %s", kind, subject, len(cs), queue)
	})
}

func registerMastercaseSteps(sc *godog.ScenarioContext, w *world) {
	sc.Step(`^exception-management is healthy$`, w.exceptionManagementIsHealthy)
	sc.Step(`^SKU "([^"]*)" declares an EACH GTIN and a CASE pack level of (\d+) eaches in product-master$`, w.skuDeclaresPack)
	sc.Step(`^the CASE GTIN of SKU "([^"]*)" eventually resolves in product-master as an active CASE of (\d+) eaches$`, w.caseGtinResolves)
	sc.Step(`^ASN "([^"]*)" announcing (\d+) homogeneous cartons of (\d+) eaches of SKU "([^"]*)" with valid SSCCs is registered in inbound-receiving$`,
		func(asn string, n, per int, sku string) error { return w.cartonAsnRegistered(asn, n, per, sku) })
	sc.Step(`^every carton SSCC of ASN "([^"]*)" is scanned on its receipt in inbound-receiving and each scan is accepted$`, w.allCartonsScannedAccepted)
	sc.Step(`^the receipt of ASN "([^"]*)" in inbound-receiving is Closed with no discrepancy, (\d+) eaches booked and every carton Accepted$`, w.receiptClosesClean)
	sc.Step(`^the SSCC of carton (\d+) of ASN "([^"]*)" is scanned again on its receipt in inbound-receiving$`, w.cartonScannedAgain)
	sc.Step(`^that scan is refused with a single DuplicateSscc finding for carton (\d+) of ASN "([^"]*)"$`, w.scanRefusedAsDuplicate)
	sc.Step(`^the receipt of ASN "([^"]*)" in inbound-receiving still books exactly (\d+) good eaches$`, w.receiptBookedOnce)
	sc.Step(`^exception-management eventually has an OPEN case in queue "([^"]*)" of kind (\w+) keyed to the receipt and SSCC of carton (\d+) of ASN "([^"]*)"$`,
		func(queue, kind string, idx int, asn string) error {
			return w.exceptionCaseEventually(queue, kind, asn, idx)
		})
	sc.Step(`^inventory-storage eventually holds (\d+) SEALED containers of (\d+) eaches each for the cartons of ASN "([^"]*)" and SKU "([^"]*)"$`, w.containersSealed)
	sc.Step(`^facility-layout has a seeded case bin and a seeded loose bin at site "([^"]*)"$`, w.seededBins)
	sc.Step(`^I stow every container of ASN "([^"]*)" into the case bin in inventory-storage$`, w.allContainersStowed)
	sc.Step(`^the usable inventory of SKU "([^"]*)" in inventory-storage is (\d+) eaches, (\d+) loose and (\d+) reservable cases$`, w.usableIs)
	sc.Step(`^I break container (\d+) of ASN "([^"]*)" into the loose bin in inventory-storage$`, w.breakContainer)
	sc.Step(`^container (\d+) of ASN "([^"]*)" is (\w+) holding (\d+) eaches$`, w.containerState)
	sc.Step(`^I reserve (\d+) eaches of SKU "([^"]*)" for demand "([^"]*)" in inventory-storage$`, w.reserveEaches)
	sc.Step(`^on hand for SKU "([^"]*)" with demand "([^"]*)" reserved is still exactly (\d+) eaches$`, w.onHandIs)
}
