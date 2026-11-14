package inbound

import (
	"regexp"
	"strings"
	"time"
)

// ---------------------------------------------------------------------
// inbound-receiving
// ---------------------------------------------------------------------

type asnRec struct {
	number, supplier, state string
	lines                   []Line
}

type rcptLine struct {
	lineNo        int
	sku           string
	expected      int
	good, damaged int
}

type rcptRec struct {
	id, asn, appt, door, state string
	lines                      []*rcptLine
	openedAt, closedAt         time.Time
	discrepancies              []map[string]any
}

// apptCopy is a service's local copy of a dock-scheduling appointment, which fills over Kafka: lag is how many
// requests that need it still miss it.
type apptCopy struct {
	asns      []string
	cancelled bool
	lag       *lag
}

type fakeReceiving struct {
	*fake
	doors    []string
	skus     map[string]bool // the known_skus local copy of product-master
	skuLag   map[string]*lag // SKUs product-master announced a moment ago
	asns     map[string]*asnRec
	receipts map[string]*rcptRec
	apptCopy map[string]*apptCopy
	closed   map[string]bool // ASNs whose receipt is closed
	// expectedDelta is added to every line's expected quantity when a receipt opens: the ASN was amended after
	// the supplier announced it, so what arrives no longer matches the receipt's snapshot.
	expectedDelta int
}

var asnNumberRE = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

func newFakeReceiving(w *world) *fakeReceiving {
	f := &fakeReceiving{
		fake: newFake(w, SvcInbound), skus: map[string]bool{}, skuLag: map[string]*lag{}, asns: map[string]*asnRec{},
		receipts: map[string]*rcptRec{}, apptCopy: map[string]*apptCopy{}, closed: map[string]bool{},
	}
	f.handle("POST", "/asns", f.registerAsn)
	f.handle("GET", "/asns/{asn}", func(c *fctx) reply {
		a := f.asns[c.params["asn"]]
		if a == nil {
			return refuse(404, "asn-not-found")
		}
		return ok(f.asnJSON(a))
	})
	f.handle("GET", "/docks", func(c *fctx) reply {
		items := make([]any, 0, len(f.doors))
		for _, d := range f.doors {
			items = append(items, map[string]any{"doorCode": d, "dockFlow": "Inbound"})
		}
		return page(c, 500, items, map[string]any{"mode": "kafka"})
	})
	f.handle("POST", "/receipts", f.openReceipt)
	f.handle("GET", "/receipts/{id}", func(c *fctx) reply {
		r := f.receipts[c.params["id"]]
		if r == nil {
			return refuse(404, "receipt-not-found")
		}
		return ok(f.receiptJSON(r))
	})
	f.handle("POST", "/receipts/{id}/lines", f.receiveLine)
	f.handle("POST", "/receipts/{id}/closures", f.closeReceipt)
	return f
}

func (f *fakeReceiving) asnJSON(a *asnRec) map[string]any {
	lines := make([]any, len(a.lines))
	for i, l := range a.lines {
		lines[i] = map[string]any{"lineNo": i + 1, "sku": l.SKU, "expectedQty": l.Qty}
	}
	return map[string]any{"asnNumber": a.number, "supplierRef": a.supplier, "state": a.state, "lines": lines, "version": 1}
}

func (f *fakeReceiving) registerAsn(c *fctx) reply {
	m, bad := c.obj()
	if bad != nil {
		return *bad
	}
	number, _ := m["asnNumber"].(string)
	supplier, _ := m["supplierRef"].(string)
	raw, _ := m["lines"].([]any)
	if !asnNumberRE.MatchString(number) || strings.TrimSpace(supplier) == "" || len(raw) == 0 {
		return refuse(400, "invalid-asn")
	}
	var lines []Line
	seen := map[string]bool{}
	for i, r := range raw {
		l, _ := r.(map[string]any)
		sku, _ := l["sku"].(string)
		qty, _ := intOf(l["expectedQty"])
		no, _ := intOf(l["lineNo"])
		if no != i+1 || sku == "" || qty < 1 || seen[sku] {
			return refuse(400, "invalid-lines")
		}
		seen[sku] = true
		lines = append(lines, Line{SKU: sku, Qty: qty})
	}
	for _, l := range lines {
		if !f.skus[l.SKU] || f.skuLag[l.SKU].refuse() {
			return refuse(422, "unknown-sku")
		}
	}
	if f.asns[number] != nil {
		return refuse(409, "asn-already-exists")
	}
	a := &asnRec{number: number, supplier: supplier, state: "Registered", lines: lines}
	f.asns[number] = a
	// ASNRegistered: dock-scheduling copies the ASN after its lag.
	f.w.dock.asnLag[number] = &lag{n: f.w.lagASNToDock}
	return created(f.asnJSON(a))
}

func intOf(v any) (int, bool) {
	switch n := v.(type) {
	case float64:
		return int(n), true
	case int:
		return n, true
	}
	return 0, false
}

func (f *fakeReceiving) receiptJSON(r *rcptRec) map[string]any {
	lines := make([]any, len(r.lines))
	for i, l := range r.lines {
		lines[i] = map[string]any{"lineNo": l.lineNo, "sku": l.sku, "expectedQty": l.expected, "receivedGood": l.good, "receivedDamaged": l.damaged}
	}
	out := map[string]any{
		"receiptId": r.id, "asnNumber": r.asn, "state": r.state, "lines": lines, "openedAt": r.openedAt.Format(time.RFC3339Nano),
		"discrepancies": append([]any{}, anySlice(r.discrepancies)...), "cartonDiscrepancies": []any{}, "version": 1,
	}
	if r.appt != "" {
		out["appointmentId"], out["doorCode"] = r.appt, r.door
	}
	if r.state == "Closed" {
		out["closedAt"] = r.closedAt.Format(time.RFC3339Nano)
	}
	return out
}

func anySlice(in []map[string]any) []any {
	out := make([]any, len(in))
	for i, v := range in {
		out[i] = v
	}
	return out
}

func (f *fakeReceiving) openReceipt(c *fctx) reply {
	m, bad := c.obj()
	if bad != nil {
		return *bad
	}
	number, _ := m["asnNumber"].(string)
	apptID, _ := m["appointmentId"].(string)
	a := f.asns[number]
	if a == nil {
		return refuse(422, "unknown-asn")
	}
	if a.state != "Registered" && a.state != "Receiving" {
		return refuse(409, "asn-not-receivable")
	}
	door := ""
	if apptID != "" {
		cp := f.apptCopy[apptID]
		if cp == nil || cp.lag.refuse() {
			return refuse(422, "unknown-appointment")
		}
		onIt := false
		for _, n := range cp.asns {
			onIt = onIt || n == number
		}
		if !onIt {
			return refuse(422, "asn-not-on-appointment")
		}
		if cp.cancelled {
			return refuse(409, "appointment-cancelled")
		}
		door = f.w.dock.appts[apptID].door
	}
	for _, r := range f.receipts {
		if r.asn == number && r.state == "Open" {
			return refuse(409, "receipt-already-open")
		}
	}
	r := &rcptRec{id: f.w.id("rcpt"), asn: number, appt: apptID, door: door, state: "Open", openedAt: f.w.clock.Now()}
	for i, l := range a.lines {
		r.lines = append(r.lines, &rcptLine{lineNo: i + 1, sku: l.SKU, expected: l.Qty + f.expectedDelta})
	}
	f.receipts[r.id] = r
	a.state = "Receiving"
	f.w.yard.receiptAppt[r.id] = apptID // ReceiptOpened
	return created(f.receiptJSON(r))
}

func (f *fakeReceiving) receiveLine(c *fctx) reply {
	r := f.receipts[c.params["id"]]
	if r == nil {
		return refuse(404, "receipt-not-found")
	}
	m, bad := c.obj()
	if bad != nil {
		return *bad
	}
	no, _ := intOf(m["lineNo"])
	qty, _ := intOf(m["quantity"])
	cond, _ := m["condition"].(string)
	if qty < 1 || (cond != "Good" && cond != "Damaged") {
		return refuse(400, "invalid-condition")
	}
	if r.state != "Open" {
		return refuse(409, "receipt-closed")
	}
	var line *rcptLine
	for _, l := range r.lines {
		if l.lineNo == no {
			line = l
		}
	}
	if line == nil {
		return refuse(422, "line-not-on-asn")
	}
	if cond == "Good" {
		line.good += qty
		// ReceiptLineReceived: inventory-storage books the Good units as staged stock.
		f.w.inv.onLineReceived(line.sku, qty)
	} else {
		line.damaged += qty
	}
	return created(f.receiptJSON(r))
}

func (f *fakeReceiving) closeReceipt(c *fctx) reply {
	r := f.receipts[c.params["id"]]
	if r == nil {
		return refuse(404, "receipt-not-found")
	}
	if r.state != "Open" {
		return refuse(409, "receipt-closed")
	}
	r.state, r.closedAt = "Closed", f.w.clock.Now()
	for _, l := range r.lines {
		recv := l.good + l.damaged
		add := func(kind string) {
			r.discrepancies = append(r.discrepancies, map[string]any{
				"lineNo": l.lineNo, "sku": l.sku, "kind": kind, "expectedQty": l.expected, "receivedQty": recv, "damagedQty": l.damaged,
			})
		}
		switch {
		case recv < l.expected:
			add("Short")
		case recv > l.expected:
			add("Over")
		}
		if l.damaged > 0 {
			add("Damaged")
		}
	}
	f.asns[r.asn].state = "Closed"
	f.closed[r.asn] = true
	f.w.yard.onReceiptClosed(r.id) // ReceiptClosed
	return ok(f.receiptJSON(r))
}
