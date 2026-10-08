package main

// Step definitions for features/inbound_receiving.feature: inbound-receiving
// (ASN, dock appointment, receipt) and its event-driven handover to
// inventory-storage (inbound-receiving ADR 0003, inventory-storage ADR 0037).
//
// Everything is black-box over the wire. REST is the main surface; Kafka is
// read (never written) where the REST contract has no read for the fact:
//
//   - inventory-storage exposes NO read of staged stock (POST /stock/receive
//     is a bare acknowledgement and /inventory/{sku}/usable only counts
//     stowed units), so "the handover booked N units" is proved by the
//     StockReceived it raises on warehouse.inventory.analytics (ADR 0037 §2),
//     which is exactly how a staged receipt becomes visible to the estate.
//   - ReceiptClosed / DockAppointmentCompleted are read from
//     warehouse.inbound-receiving.events next to the REST read, so the
//     published contract (snake_case, full type) is asserted too.
//
// Nothing here is injected: every fact is produced by a real service.

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/cucumber/godog"
)

var inboundBaseURL = envOrDefault("INBOUND_BASE_URL", "http://localhost:8108")

const (
	topicInboundEvents      = "warehouse.inbound-receiving.events"
	topicInventoryAnalytics = "warehouse.inventory.analytics"

	typeReceiptClosed        = "com.warehouse.wms.inbound-receiving.receipt.ReceiptClosed"
	typeAppointmentCompleted = "com.warehouse.wms.inbound-receiving.dockappointment.DockAppointmentCompleted"
	typeStockReceived        = "com.warehouse.wms.inventory-storage.stock.StockReceived"
)

// inboundState remembers the ids inbound-receiving minted. The world is
// shared by every scenario of a process (see InitializeScenario), so each
// scenario uses its own run-scoped ASN numbers and these maps never collide.
type inboundState struct {
	receiptByASN     map[string]string // ASN number -> receipt id
	appointmentByASN map[string]string // first ASN number -> appointment id
	doorByASN        map[string]string // first ASN number -> door code
	// anchor is the instant every appointment window of the booking
	// scenario is measured from, so "starts when the other one ends" is
	// exact instead of depending on the clock between two calls.
	anchor time.Time
}

func (w *world) ib() *inboundState {
	if w.inb == nil {
		w.inb = &inboundState{
			receiptByASN:     map[string]string{},
			appointmentByASN: map[string]string{},
			doorByASN:        map[string]string{},
		}
	}
	return w.inb
}

// ---------------------------------------------------------------------
// Background
// ---------------------------------------------------------------------

func (w *world) inboundServicesAreHealthy() error {
	for name, base := range map[string]string{
		"inbound-receiving": inboundBaseURL,
		"product-master":    productMasterBaseURL,
		"inventory-storage": inventoryBaseURL,
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
// helpers
// ---------------------------------------------------------------------

// problemIs asserts the last response is an RFC 7807 problem with this
// status and a type URL ending in "/<slug>": a bare status would also pass
// on an unrelated refusal.
func (w *world) problemIs(status int, slug string) error {
	if w.last.status != status {
		return fmt.Errorf("status %d, want %d %q (body: %s)", w.last.status, status, slug, w.last.body)
	}
	if typ, _ := w.last.json()["type"].(string); !strings.HasSuffix(typ, "/"+slug) {
		return fmt.Errorf("problem type %q, want .../%s (body: %s)", typ, slug, w.last.body)
	}
	return nil
}

func (w *world) receiptIDFor(asn string) (string, error) {
	id, ok := w.ib().receiptByASN[asn]
	if !ok {
		return "", fmt.Errorf("no receipt has been opened for ASN %s in this process", asn)
	}
	return id, nil
}

func (w *world) appointmentIDFor(asn string) (string, error) {
	id, ok := w.ib().appointmentByASN[asn]
	if !ok {
		return "", fmt.Errorf("no appointment has been booked for ASN %s in this process", asn)
	}
	return id, nil
}

// discrepancy is one row of a receipt's close-time differences, in the REST
// vocabulary (the Kafka payload is the same fact in snake_case).
type discrepancy struct {
	line                        int
	sku, kind                   string
	expected, received, damaged int
}

func (d discrepancy) String() string {
	return fmt.Sprintf("{line %d %s %s expected %d received %d damaged %d}", d.line, d.sku, d.kind, d.expected, d.received, d.damaged)
}

// discrepanciesFromTable reads | line | sku | kind | expected | received | damaged |.
func (w *world) discrepanciesFromTable(tbl *godog.Table) ([]discrepancy, error) {
	var out []discrepancy
	for i, row := range tbl.Rows {
		if i == 0 {
			continue // header
		}
		if len(row.Cells) != 6 {
			return nil, fmt.Errorf("discrepancy table row %d has %d cells, want 6", i, len(row.Cells))
		}
		var d discrepancy
		d.sku, d.kind = w.rs(row.Cells[1].Value), row.Cells[2].Value
		for cell, dst := range map[int]*int{0: &d.line, 3: &d.expected, 4: &d.received, 5: &d.damaged} {
			if _, err := fmt.Sscanf(row.Cells[cell].Value, "%d", dst); err != nil {
				return nil, fmt.Errorf("discrepancy table row %d cell %d %q is not a number", i, cell, row.Cells[cell].Value)
			}
		}
		out = append(out, d)
	}
	return out, nil
}

// discrepanciesFromJSON decodes a discrepancies array whose keys are
// line/sku/kind/expected/received/damaged under the given key names (REST is
// camelCase, the event payload snake_case).
func discrepanciesFromJSON(raw any, lineKey, expectedKey, receivedKey, damagedKey string) ([]discrepancy, error) {
	arr, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("discrepancies is %T, want an array", raw)
	}
	out := make([]discrepancy, 0, len(arr))
	for _, item := range arr {
		m, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("discrepancy %v is not an object", item)
		}
		var d discrepancy
		d.sku, _ = m["sku"].(string)
		d.kind, _ = m["kind"].(string)
		d.line, _ = intOf(m[lineKey])
		d.expected, _ = intOf(m[expectedKey])
		d.received, _ = intOf(m[receivedKey])
		d.damaged, _ = intOf(m[damagedKey])
		out = append(out, d)
	}
	return out, nil
}

func sameDiscrepancies(got, want []discrepancy) error {
	if len(got) != len(want) {
		return fmt.Errorf("discrepancies %v, want exactly %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			return fmt.Errorf("discrepancies %v, want exactly %v", got, want)
		}
	}
	return nil
}

// ---------------------------------------------------------------------
// ASN
// ---------------------------------------------------------------------

// asnEventuallyRegistered registers a one-line ASN. inbound-receiving runs
// PRODUCT_MODE=kafka, so a SKU product-master registered a moment ago is
// refused with 422 unknown-sku until ProductRegistered has reached its local
// copy: that refusal is the expected transient and is retried; anything else
// is final.
func (w *world) asnEventuallyRegistered(asn, supplier string, qty int, sku string) error {
	asn, sku = w.rs(asn), w.rs(sku)
	var final error
	err := eventually(func() error {
		if err := w.doJSON(http.MethodPost, inboundBaseURL+"/asns", map[string]any{
			"asnNumber": asn, "supplierRef": supplier,
			"lines": []map[string]any{{"lineNo": 1, "sku": sku, "expectedQty": qty}},
		}); err != nil {
			final = err
			return nil
		}
		if w.last.status == http.StatusUnprocessableEntity {
			if typ, _ := w.last.json()["type"].(string); strings.HasSuffix(typ, "/unknown-sku") {
				return fmt.Errorf("inbound-receiving does not know SKU %s yet (body: %s)", sku, w.last.body)
			}
		}
		final = w.expectOK2xx(nil)
		return nil
	})
	if err != nil {
		return err
	}
	if final != nil {
		return fmt.Errorf("register ASN %s: %w", asn, final)
	}
	got := w.last.json()
	if w.last.status != http.StatusCreated || got["state"] != "Registered" {
		return fmt.Errorf("register ASN %s: status %d state %v, want 201 Registered (body: %s)", asn, w.last.status, got["state"], w.last.body)
	}
	return nil
}

func (w *world) asnIsInState(asn, want string) error {
	asn = w.rs(asn)
	if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/asns/%s", inboundBaseURL, asn), nil); err != nil {
		return err
	}
	if w.last.status != http.StatusOK {
		return fmt.Errorf("GET ASN %s: status %d (body: %s)", asn, w.last.status, w.last.body)
	}
	if got := w.last.json()["state"]; got != want {
		return fmt.Errorf("ASN %s state = %v, want %s (body: %s)", asn, got, want, w.last.body)
	}
	return nil
}

// ---------------------------------------------------------------------
// Receipt
// ---------------------------------------------------------------------

func (w *world) openReceipt(asn string, body map[string]any) (map[string]any, error) {
	if err := w.expectOK2xx(w.doJSON(http.MethodPost, inboundBaseURL+"/receipts", body)); err != nil {
		return nil, fmt.Errorf("open receipt for ASN %s: %w", asn, err)
	}
	got := w.last.json()
	id, _ := got["receiptId"].(string)
	if w.last.status != http.StatusCreated || id == "" || got["state"] != "Open" {
		return nil, fmt.Errorf("open receipt for ASN %s: status %d state %v id %q, want 201 Open (body: %s)", asn, w.last.status, got["state"], id, w.last.body)
	}
	w.ib().receiptByASN[asn] = id
	return got, nil
}

func (w *world) receiptIsOpened(asn string) error {
	asn = w.rs(asn)
	_, err := w.openReceipt(asn, map[string]any{"asnNumber": asn})
	return err
}

// receiptIsOpenedFromAppointment opens the receipt from the ASN's checked-in
// appointment; the receipt must take the appointment's door.
func (w *world) receiptIsOpenedFromAppointment(asn string) error {
	asn = w.rs(asn)
	apptID, err := w.appointmentIDFor(asn)
	if err != nil {
		return err
	}
	got, err := w.openReceipt(asn, map[string]any{"asnNumber": asn, "appointmentId": apptID})
	if err != nil {
		return err
	}
	if got["appointmentId"] != apptID || got["doorCode"] != w.ib().doorByASN[asn] {
		return fmt.Errorf("receipt of ASN %s carries appointment %v at door %v, want %s at %s (body: %s)",
			asn, got["appointmentId"], got["doorCode"], apptID, w.ib().doorByASN[asn], w.last.body)
	}
	return nil
}

func (w *world) unitsAreReceived(qty int, condition string, lineNo int, asn string) error {
	asn = w.rs(asn)
	id, err := w.receiptIDFor(asn)
	if err != nil {
		return err
	}
	if err := w.expectOK2xx(w.doJSON(http.MethodPost, fmt.Sprintf("%s/receipts/%s/lines", inboundBaseURL, id),
		map[string]any{"lineNo": lineNo, "quantity": qty, "condition": condition})); err != nil {
		return fmt.Errorf("receive %d %s on line %d of ASN %s: %w", qty, condition, lineNo, asn, err)
	}
	if w.last.status != http.StatusCreated {
		return fmt.Errorf("receive %d %s on line %d of ASN %s: status %d, want 201 (body: %s)", qty, condition, lineNo, asn, w.last.status, w.last.body)
	}
	return nil
}

func (w *world) receiptIsClosed(asn string) error {
	asn = w.rs(asn)
	id, err := w.receiptIDFor(asn)
	if err != nil {
		return err
	}
	if err := w.expectOK2xx(w.doJSON(http.MethodPost, fmt.Sprintf("%s/receipts/%s/close", inboundBaseURL, id), nil)); err != nil {
		return fmt.Errorf("close receipt of ASN %s: %w", asn, err)
	}
	if got := w.last.json()["state"]; got != "Closed" {
		return fmt.Errorf("close receipt of ASN %s: state %v, want Closed (body: %s)", asn, got, w.last.body)
	}
	return nil
}

func (w *world) readReceipt(asn string) (map[string]any, error) {
	id, err := w.receiptIDFor(asn)
	if err != nil {
		return nil, err
	}
	if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/receipts/%s", inboundBaseURL, id), nil); err != nil {
		return nil, err
	}
	if w.last.status != http.StatusOK {
		return nil, fmt.Errorf("GET receipt %s: status %d (body: %s)", id, w.last.status, w.last.body)
	}
	return w.last.json(), nil
}

// receiptClosedWithDiscrepancies reads the receipt over REST and requires it
// Closed with EXACTLY the tabled discrepancies (no extra kind, same order).
func (w *world) receiptClosedWithDiscrepancies(asn string, tbl *godog.Table) error {
	asn = w.rs(asn)
	want, err := w.discrepanciesFromTable(tbl)
	if err != nil {
		return err
	}
	got, err := w.readReceipt(asn)
	if err != nil {
		return err
	}
	if got["state"] != "Closed" {
		return fmt.Errorf("receipt of ASN %s state = %v, want Closed (body: %s)", asn, got["state"], w.last.body)
	}
	have, err := discrepanciesFromJSON(got["discrepancies"], "lineNo", "expectedQty", "receivedQty", "damagedQty")
	if err != nil {
		return err
	}
	if err := sameDiscrepancies(have, want); err != nil {
		return fmt.Errorf("receipt of ASN %s: %w (body: %s)", asn, err, w.last.body)
	}
	return nil
}

// receiptEventPublishedWithDiscrepancies requires ReceiptClosed for the ASN
// on warehouse.inbound-receiving.events to carry the same discrepancies in
// the pinned snake_case shape.
func (w *world) receiptClosedEventPublished(asn string, tbl *godog.Table) error {
	asn = w.rs(asn)
	want, err := w.discrepanciesFromTable(tbl)
	if err != nil {
		return err
	}
	p, err := expectKafkaEvent(topicInboundEvents, typeReceiptClosed,
		func(p map[string]any) bool { return p["asn_number"] == asn }, "ASN "+asn)
	if err != nil {
		return err
	}
	have, err := discrepanciesFromJSON(p["discrepancies"], "line_no", "expected_qty", "received_qty", "damaged_qty")
	if err != nil {
		return err
	}
	if err := sameDiscrepancies(have, want); err != nil {
		return fmt.Errorf("ReceiptClosed of ASN %s: %w (payload: %v)", asn, err, p)
	}
	return nil
}

func (w *world) receiptRecordsUnits(asn string, good, damaged, lineNo int) error {
	asn = w.rs(asn)
	got, err := w.readReceipt(asn)
	if err != nil {
		return err
	}
	lines, _ := got["lines"].([]any)
	if lineNo < 1 || lineNo > len(lines) {
		return fmt.Errorf("receipt of ASN %s has %d lines, no line %d (body: %s)", asn, len(lines), lineNo, w.last.body)
	}
	line, _ := lines[lineNo-1].(map[string]any)
	g, _ := intOf(line["receivedGood"])
	d, _ := intOf(line["receivedDamaged"])
	if g != good || d != damaged {
		return fmt.Errorf("line %d of ASN %s records %d good / %d damaged, want %d / %d (body: %s)", lineNo, asn, g, d, good, damaged, w.last.body)
	}
	return nil
}

// ---------------------------------------------------------------------
// Handover to inventory-storage
// ---------------------------------------------------------------------

// inventoryStagesUnits proves the handover booked exactly `units` of the SKU
// as staged stock in exactly `events` StockReceived: first wait until the
// running total reaches the expectation, then rescan the whole topic and
// require the totals to be exact. The second pass is what makes a Damaged
// line that WAS booked (an extra 4-unit StockReceived) fail the scenario.
func (w *world) inventoryStagesUnits(units int, sku string, events int) error {
	sku = w.rs(sku)
	matches := func(m ceMessage) bool {
		return m.Type == typeStockReceived && m.payload()["sku"] == sku
	}
	total := 0
	found, seen, err := scanTopic(topicInventoryAnalytics, kafkaScanWindow, func(m ceMessage) bool {
		if !matches(m) {
			return false
		}
		q, _ := intOf(m.payload()["quantity"])
		total += q
		return total >= units
	})
	if err != nil {
		return err
	}
	if found == nil {
		return fmt.Errorf("inventory-storage staged %d of the %d expected units of %s within %s (read %d messages on %s)",
			total, units, sku, kafkaScanWindow, seen, topicInventoryAnalytics)
	}
	count, sum := 0, 0
	if _, _, err := scanTopic(topicInventoryAnalytics, kafkaAbsenceWindow, func(m ceMessage) bool {
		if matches(m) {
			count++
			q, _ := intOf(m.payload()["quantity"])
			sum += q
		}
		return false
	}); err != nil {
		return err
	}
	if count != events || sum != units {
		return fmt.Errorf("inventory-storage raised %d StockReceived totalling %d units for %s, want exactly %d totalling %d",
			count, sum, sku, events, units)
	}
	return nil
}

// ---------------------------------------------------------------------
// Dock appointments
// ---------------------------------------------------------------------

// window returns the half-open window [anchor+startMin, anchor+startMin+durMin)
// in whole seconds, anchoring on the first call of the process.
func (w *world) window(startMin, durMin int) (time.Time, time.Time) {
	in := w.ib()
	if in.anchor.IsZero() {
		in.anchor = time.Now().UTC().Truncate(time.Second)
	}
	start := in.anchor.Add(time.Duration(startMin) * time.Minute)
	return start, start.Add(time.Duration(durMin) * time.Minute)
}

func (w *world) bookAppointment(door, carrier, asn string, startMin, durMin int) error {
	start, end := w.window(startMin, durMin)
	return w.doJSON(http.MethodPost, inboundBaseURL+"/appointments", map[string]any{
		"doorCode": door, "carrier": carrier,
		"windowStart": start.Format(time.RFC3339), "windowEnd": end.Format(time.RFC3339),
		"asnNumbers": []string{asn},
	})
}

func (w *world) appointmentIsBooked(door, carrier, asn string, startMin, durMin int) error {
	door, asn = w.rs(door), w.rs(asn)
	if err := w.expectOK2xx(w.bookAppointment(door, carrier, asn, startMin, durMin)); err != nil {
		return fmt.Errorf("book %s at %s: %w", asn, door, err)
	}
	got := w.last.json()
	id, _ := got["appointmentId"].(string)
	if w.last.status != http.StatusCreated || id == "" || got["state"] != "Booked" {
		return fmt.Errorf("book %s at %s: status %d state %v id %q, want 201 Booked (body: %s)", asn, door, w.last.status, got["state"], id, w.last.body)
	}
	w.ib().appointmentByASN[asn] = id
	w.ib().doorByASN[asn] = door
	return nil
}

func (w *world) bookingIsRefused(door, asn string, startMin, durMin, status int, slug string) error {
	door, asn = w.rs(door), w.rs(asn)
	if err := w.bookAppointment(door, "ACME Freight", asn, startMin, durMin); err != nil {
		return err
	}
	return w.problemIs(status, slug)
}

func (w *world) appointmentAction(asn, action, wantState string) error {
	asn = w.rs(asn)
	id, err := w.appointmentIDFor(asn)
	if err != nil {
		return err
	}
	if err := w.expectOK2xx(w.doJSON(http.MethodPost, fmt.Sprintf("%s/appointments/%s/%s", inboundBaseURL, id, action), nil)); err != nil {
		return fmt.Errorf("%s appointment %s: %w", action, id, err)
	}
	if got := w.last.json()["state"]; got != wantState {
		return fmt.Errorf("%s appointment %s: state %v, want %s (body: %s)", action, id, got, wantState, w.last.body)
	}
	return nil
}

func (w *world) carrierChecksIn(asn string) error {
	return w.appointmentAction(asn, "check-in", "CheckedIn")
}
func (w *world) appointmentIsCancelled(asn string) error {
	return w.appointmentAction(asn, "cancel", "Cancelled")
}

func (w *world) appointmentIsInState(asn, want string) error {
	asn = w.rs(asn)
	id, err := w.appointmentIDFor(asn)
	if err != nil {
		return err
	}
	if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/appointments/%s", inboundBaseURL, id), nil); err != nil {
		return err
	}
	if w.last.status != http.StatusOK {
		return fmt.Errorf("GET appointment %s: status %d (body: %s)", id, w.last.status, w.last.body)
	}
	if got := w.last.json()["state"]; got != want {
		return fmt.Errorf("appointment %s state = %v, want %s (body: %s)", id, got, want, w.last.body)
	}
	return nil
}

func (w *world) appointmentCompletedEventPublished(asn string) error {
	asn = w.rs(asn)
	id, err := w.appointmentIDFor(asn)
	if err != nil {
		return err
	}
	door := w.ib().doorByASN[asn]
	p, err := expectKafkaEvent(topicInboundEvents, typeAppointmentCompleted,
		func(p map[string]any) bool { return p["appointment_id"] == id }, "appointment "+id)
	if err != nil {
		return err
	}
	if p["door_code"] != door {
		return fmt.Errorf("DockAppointmentCompleted door_code = %v, want %s (payload: %v)", p["door_code"], door, p)
	}
	return nil
}

// ---------------------------------------------------------------------
// registration
// ---------------------------------------------------------------------

func registerInboundSteps(sc *godog.ScenarioContext, w *world) {
	sc.Step(`^inbound-receiving, product-master and inventory-storage are healthy$`, w.inboundServicesAreHealthy)

	sc.Step(`^ASN "([^"]*)" from supplier "([^"]*)" expecting (\d+) units of SKU "([^"]*)" is eventually registered in inbound-receiving$`, w.asnEventuallyRegistered)
	sc.Step(`^inbound-receiving's ASN "([^"]*)" is "([^"]*)"$`, w.asnIsInState)

	sc.Step(`^a receipt is opened for ASN "([^"]*)" in inbound-receiving$`, w.receiptIsOpened)
	sc.Step(`^a receipt is opened for ASN "([^"]*)" from its appointment in inbound-receiving$`, w.receiptIsOpenedFromAppointment)
	sc.Step(`^(\d+) (Good|Damaged) units of line (\d+) are received on the receipt of ASN "([^"]*)" in inbound-receiving$`, w.unitsAreReceived)
	sc.Step(`^the receipt of ASN "([^"]*)" is closed in inbound-receiving$`, w.receiptIsClosed)
	sc.Step(`^the receipt of ASN "([^"]*)" in inbound-receiving is Closed with exactly these discrepancies:$`, w.receiptClosedWithDiscrepancies)
	sc.Step(`^inbound-receiving published ReceiptClosed for ASN "([^"]*)" with exactly these discrepancies:$`, w.receiptClosedEventPublished)
	sc.Step(`^the receipt of ASN "([^"]*)" in inbound-receiving records (\d+) good and (\d+) damaged units on line (\d+)$`, w.receiptRecordsUnits)

	sc.Step(`^inventory-storage eventually stages (\d+) units of SKU "([^"]*)" in exactly (\d+) StockReceived events?$`, w.inventoryStagesUnits)

	sc.Step(`^a dock appointment at door "([^"]*)" for carrier "([^"]*)" and ASN "([^"]*)" is booked to start in (\d+) minutes for (\d+) minutes in inbound-receiving$`, w.appointmentIsBooked)
	sc.Step(`^booking a dock appointment at door "([^"]*)" for ASN "([^"]*)" to start in (\d+) minutes for (\d+) minutes in inbound-receiving is refused with (\d+) "([^"]*)"$`, w.bookingIsRefused)
	sc.Step(`^the carrier of ASN "([^"]*)"'s appointment checks in at inbound-receiving$`, w.carrierChecksIn)
	sc.Step(`^the appointment of ASN "([^"]*)" in inbound-receiving is cancelled$`, w.appointmentIsCancelled)
	sc.Step(`^the appointment of ASN "([^"]*)" in inbound-receiving is "([^"]*)"$`, w.appointmentIsInState)
	sc.Step(`^inbound-receiving published DockAppointmentCompleted for the appointment of ASN "([^"]*)"$`, w.appointmentCompletedEventPublished)
}
