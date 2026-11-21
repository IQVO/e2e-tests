package main

// Step definitions for the GS1-label and receiving-advice scenarios of
// features/inbound_receiving.feature (inbound-receiving ADR 0023 + 0024,
// merged on develop as v2.0.7): the ]C1 label rules (LabelNonCompliant,
// WrongDestination), the earliest best-before of a mixed pallet, the
// receiver-assigned SSCC and the ReceivingAdviceIssued at close.
//
// Everything is black-box over the wire like the rest of this suite: REST is
// the surface, Kafka is read (never written) where only the event carries the
// fact (the CartonDiscrepancyRaised findings, the ReceiptCartonReceived
// sscc_source, the advice and its order after ReceiptClosed).
//
// The two harness literals the service is started with (scripts/services.sh,
// up_inbound_receiving — one definition, mirrored here so the scenarios can
// build labels against them; a drift fails the WrongDestination positive side
// loudly, the service would refuse the site's own GLN):
//
//	RECEIVING_SITE_GLNS=0614141000012   (13 digits, mod-10 check digit 2)
//	RECEIVER_GS1_COMPANY_PREFIX=0614141 (7 digits; SSCCs start 0+prefix = 00614141)

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/cucumber/godog"
)

const (
	typeReceivingAdviceIssued = "com.warehouse.wms.inbound-receiving.receipt.ReceivingAdviceIssued"
	typeReceiptCartonReceived = "com.warehouse.wms.inbound-receiving.receipt.ReceiptCartonReceived"

	// The harness receiving-site GLN of scripts/services.sh (see the file
	// header): the AI 410 a label may name to be accepted here.
	labelSiteGLN = "0614141000012"
	// A DIFFERENT valid GLN (check digit 9): a label naming it is refused as
	// WrongDestination because the value differs, not because it is malformed.
	labelOtherGLN = "0614141000029"
	// The receiver's GS1 company prefix of scripts/services.sh: every SSCC
	// AssignSscc allocates starts with extension digit 0 + this prefix.
	labelReceiverPrefix = "0614141"

	// SSCC reference prefix of THIS feature's cartons (a stand-in GS1 company
	// prefix like mastercase's 3401234; a different one so the label scenarios
	// never share an SSCC with the mastercase scenarios of the same run). The
	// payload is extension 0 + prefix + 8 run digits + 1 serial digit, plus the
	// mod-10 check digit.
	labelSsccPrefix = "3456789"
)

// labelsState is the GS1-label scenarios' correlation. Like every other
// per-context state it lives on the scenario's world, so ids never leak across
// scenarios; SSCC serials restart per scenario exactly like mastercase's.
type labelsState struct {
	palletByASN  map[string]string // ASN number -> pallet (parent) SSCC
	assignKey    map[string]string // ASN number -> the Idempotency-Key of its one sscc-assignment
	assignedGtin map[string]string // ASN number -> the CASE GTIN that one assignment scanned
	assignedSSCC map[string]string // ASN number -> the SSCC AssignSscc allocated
	lastLabel    string            // the raw ]C1 string the last scan step sent
	seq          int               // next SSCC serial digit of this scenario
}

func (w *world) lb() *labelsState {
	if w.lbl == nil {
		w.lbl = &labelsState{
			palletByASN:  map[string]string{},
			assignKey:    map[string]string{},
			assignedGtin: map[string]string{},
			assignedSSCC: map[string]string{},
		}
	}
	return w.lbl
}

// lbNextSscc mints a valid, run-scoped SSCC-18 of this feature's prefix.
func (w *world) lbNextSscc() string {
	m := w.lb()
	m.seq++
	p := fmt.Sprintf("0%s%s%d", labelSsccPrefix, w.runDigits(), m.seq%10)
	return p + gs1CheckDigit(p)
}

// ---------------------------------------------------------------------
// label builders (the GS1-128 element strings the scenarios scan)
// ---------------------------------------------------------------------

// The AIs used here are all fixed-length (00: 18, 15: 6, 410: 13), so the
// element string concatenates them without GS separators, exactly as a
// scanner delivers a ]C1 read.
func labelSSCC(sscc string) string                   { return "]C100" + sscc }
func labelSSCCBestBefore(sscc, yymmdd string) string { return "]C100" + sscc + "15" + yymmdd }
func labelSSCCShipTo(sscc, gln string) string        { return "]C100" + sscc + "410" + gln }

// labelConflictingAIs carries AI 01 and AI 02 together on valid GTIN-14s: the
// one combination the standard forbids outright, so the refusal can only be
// conflicting_ais (both numbers are valid, no check digit is wrong).
func labelConflictingAIs(gtin string) string { return "]C101" + gtin + "02" + gtin }

// labelBadCheckDigitSSCC takes a valid SSCC and flips its check digit: every
// other digit stays valid, so the refusal can only be bad_check_digit.
func labelBadCheckDigitSSCC(sscc string) string {
	d := int(sscc[17]-'0'+1) % 10
	return "]C100" + sscc[:17] + fmt.Sprintf("%d", d)
}

// postJSONIdempotent is doJSON with a CALLER-OWNED Idempotency-Key, the one
// way to replay the same logical request (doJSON mints a fresh key per call,
// which is right for every other step).
func (w *world) postJSONIdempotent(url, key string, body any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("HTTP POST %s: %w", url, err)
	}
	defer resp.Body.Close()
	rb, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	w.last = httpResult{status: resp.StatusCode, header: resp.Header, body: rb}
	return nil
}

// postRawScan sends one raw ]C1 scanner string to the receipt's scan
// endpoint. It asserts nothing beyond transport: the refusal steps below are
// what make a refused scan prove its exact reason.
func (w *world) postRawScan(asn, label string) error {
	rid, err := w.receiptIDFor(w.rs(asn))
	if err != nil {
		return err
	}
	w.lb().lastLabel = label
	if err := w.doJSON(http.MethodPost, fmt.Sprintf("%s/receipts/%s/scans", inboundBaseURL, rid),
		map[string]any{"scan": label}); err != nil {
		return err
	}
	return nil
}

// ---------------------------------------------------------------------
// ASN with a pallet (the earliest-best-before scenario's manifest)
// ---------------------------------------------------------------------

// palletAsnRegistered registers a one-line, carton-carrying ASN whose cartons
// all share ONE parent SSCC (the pallet), like the mastercase ASN step but
// with parentSscc set on every carton. Supplier ACME, like every inbound
// scenario, so the label findings' supplier_ref is a known constant.
func (w *world) palletAsnRegistered(asn string, cartons, perCase int, sku string) error {
	asn, sku = w.rs(asn), w.rs(sku)
	m := w.mcs()
	gtin, ok := m.caseGtin[sku]
	if !ok {
		return fmt.Errorf("SKU %s has no declared CASE GTIN in this process", sku)
	}
	pallet := w.lbNextSscc()
	var cc []map[string]any
	var ss []string
	for i := 0; i < cartons; i++ {
		s := w.lbNextSscc()
		ss = append(ss, s)
		cc = append(cc, map[string]any{"sscc": s, "parentSscc": pallet, "kind": "HOMOGENEOUS_CASE",
			"contents": []map[string]any{{"lineNo": 1, "sku": sku, "gtin": gtin, "qty": perCase}}})
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
	w.lb().palletByASN[asn] = pallet
	return nil
}

// twoLineAsnRegistered is asnEventuallyRegistered with a second line: the
// advice scenario needs a damaged line AND a clean line on one receipt, which
// the one-line step cannot express.
func (w *world) twoLineAsnRegistered(asn, supplier string, qty1 int, sku1 string, qty2 int, sku2 string) error {
	asn, sku1, sku2 = w.rs(asn), w.rs(sku1), w.rs(sku2)
	var final error
	err := eventually(func() error {
		if err := w.doJSON(http.MethodPost, inboundBaseURL+"/asns", map[string]any{
			"asnNumber": asn, "supplierRef": supplier,
			"lines": []map[string]any{
				{"lineNo": 1, "sku": sku1, "expectedQty": qty1},
				{"lineNo": 2, "sku": sku2, "expectedQty": qty2},
			},
		}); err != nil {
			final = err
			return nil
		}
		if w.last.status == http.StatusUnprocessableEntity {
			if typ, _ := w.last.json()["type"].(string); strings.HasSuffix(typ, "/unknown-sku") {
				return fmt.Errorf("inbound-receiving does not know a SKU of %s yet (body: %s)", asn, w.last.body)
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
	if w.last.status != http.StatusCreated || w.last.json()["state"] != "Registered" {
		return fmt.Errorf("register ASN %s: status %d (body: %s)", asn, w.last.status, w.last.body)
	}
	return nil
}

// ---------------------------------------------------------------------
// LabelNonCompliant (ADR 0024 section 1)
// ---------------------------------------------------------------------

func (w *world) scanConflictingAIsLabel(asn, sku string) error {
	gtin, ok := w.mcs().caseGtin[w.rs(sku)]
	if !ok {
		return fmt.Errorf("SKU %s has no declared CASE GTIN in this process", w.rs(sku))
	}
	return w.postRawScan(asn, labelConflictingAIs(gtin))
}

func (w *world) scanBadCheckDigitLabel(asn string) error {
	ss := w.mcs().ssccs[w.rs(asn)]
	if len(ss) < 1 {
		return fmt.Errorf("ASN %s has no cartons", w.rs(asn))
	}
	return w.postRawScan(asn, labelBadCheckDigitSSCC(ss[0]))
}

// scanRefusedLabelNonCompliant asserts the exact refused-scan answer of ADR
// 0024 section 1: 200 (the scan was understood and refused, not a 4xx),
// accepted false, ONE finding, the closed reason code, the supplier of the
// ASN, the CBV disposition, no W13 code (unmapped) and no SSCC (a label that
// did not parse has no trustworthy SSCC), with the raw scan recorded as
// `actual` and NOTHING booked on any line.
func (w *world) scanRefusedLabelNonCompliant(reason, supplier string) error {
	if w.last.status != http.StatusOK {
		return fmt.Errorf("refused label scan: status %d, want 200 accepted=false (body: %s)", w.last.status, w.last.body)
	}
	res := w.last.json()
	if res["accepted"] != false {
		return fmt.Errorf("refused label scan: accepted=%v, want false (body: %s)", res["accepted"], w.last.body)
	}
	ds, _ := res["discrepancies"].([]any)
	if len(ds) != 1 {
		return fmt.Errorf("refused label scan raised %d discrepancies, want exactly 1 (body: %s)", len(ds), w.last.body)
	}
	d, _ := ds[0].(map[string]any)
	if d["kind"] != "LabelNonCompliant" {
		return fmt.Errorf("finding kind = %v, want LabelNonCompliant (body: %s)", d["kind"], w.last.body)
	}
	if d["expected"] != reason {
		return fmt.Errorf("reason code = %v, want %q (body: %s)", d["expected"], reason, w.last.body)
	}
	if d["supplierRef"] != supplier {
		return fmt.Errorf("supplierRef = %v, want %q (body: %s)", d["supplierRef"], supplier, w.last.body)
	}
	if d["cbvDisposition"] != "non_conformant" {
		return fmt.Errorf("cbvDisposition = %v, want non_conformant (body: %s)", d["cbvDisposition"], w.last.body)
	}
	if _, present := d["w13Code"]; present {
		return fmt.Errorf("LabelNonCompliant is W13-unmapped, w13Code must be absent (body: %s)", w.last.body)
	}
	if sscc, _ := d["sscc"].(string); sscc != "" {
		return fmt.Errorf("LabelNonCompliant carries sscc %q, want none (body: %s)", sscc, w.last.body)
	}
	if d["actual"] != w.lb().lastLabel {
		return fmt.Errorf("finding actual = %v, want the scanned string %q (body: %s)", d["actual"], w.lb().lastLabel, w.last.body)
	}
	lines, _ := res["lines"].([]any)
	for i, l := range lines {
		line, _ := l.(map[string]any)
		g, _ := intOf(line["receivedGood"])
		dm, _ := intOf(line["receivedDamaged"])
		if g != 0 || dm != 0 {
			return fmt.Errorf("nothing may be booked by a refused scan: line %d has %d good / %d damaged", i+1, g, dm)
		}
	}
	return nil
}

// labelNonCompliantEventPublished asserts the finding reached
// warehouse.inbound-receiving.events as a CartonDiscrepancyRaised with the
// same closed vocabulary: the reason code, the supplier, the CBV disposition,
// no W13 code, no SSCC.
func (w *world) labelNonCompliantEventPublished(reason, supplier, asn string) error {
	asn = w.rs(asn)
	rid, err := w.receiptIDFor(asn)
	if err != nil {
		return err
	}
	p, err := expectKafkaEvent(topicInboundEvents, typeCartonDiscrepancyRaised,
		func(p map[string]any) bool {
			return p["receipt_id"] == rid && p["kind"] == "LabelNonCompliant" && p["expected"] == reason
		}, fmt.Sprintf("LabelNonCompliant %s of ASN %s", reason, asn))
	if err != nil {
		return err
	}
	if p["supplier_ref"] != supplier || p["cbv_disposition"] != "non_conformant" {
		return fmt.Errorf("LabelNonCompliant event: supplier_ref=%v cbv_disposition=%v, want %s / non_conformant (payload: %v)", p["supplier_ref"], p["cbv_disposition"], supplier, p)
	}
	if _, present := p["w13_code"]; present {
		return fmt.Errorf("LabelNonCompliant event is W13-unmapped, w13_code must be absent (payload: %v)", p)
	}
	if sscc, _ := p["sscc"].(string); sscc != "" {
		return fmt.Errorf("LabelNonCompliant event carries sscc %q, want none (payload: %v)", sscc, p)
	}
	return nil
}

// receiptKeepsLabelFindings asserts the receipt's persisted findings are
// EXACTLY the two refused scans, in scan order, and that no unit was ever
// booked: the receipt is not poisoned for the good scan that follows.
func (w *world) receiptKeepsLabelFindings(asn string, want int) error {
	asn = w.rs(asn)
	rec, err := w.readReceipt(asn)
	if err != nil {
		return err
	}
	ds, _ := rec["cartonDiscrepancies"].([]any)
	if len(ds) != want {
		return fmt.Errorf("receipt of ASN %s keeps %d carton discrepancies, want exactly %d (body: %s)", asn, len(ds), want, w.last.body)
	}
	wantReasons := []string{"conflicting_ais", "bad_check_digit"}
	for i, d := range ds {
		dm, _ := d.(map[string]any)
		if dm["kind"] != "LabelNonCompliant" || dm["expected"] != wantReasons[i] {
			return fmt.Errorf("carton discrepancy %d = %v, want LabelNonCompliant %s (body: %s)", i+1, dm, wantReasons[i], w.last.body)
		}
	}
	lines, _ := rec["lines"].([]any)
	for i, l := range lines {
		line, _ := l.(map[string]any)
		g, _ := intOf(line["receivedGood"])
		dm, _ := intOf(line["receivedDamaged"])
		if g != 0 || dm != 0 {
			return fmt.Errorf("refused scans booked nothing: line %d has %d good / %d damaged (body: %s)", i+1, g, dm, w.last.body)
		}
	}
	return nil
}

// scanCartonWithSSCCLabel scans an announced carton through the SAME surface
// as the refused scans (a plain ]C1 SSCC label) and must be ACCEPTED: the
// earlier refusals did not poison the receipt.
func (w *world) scanCartonWithSSCCLabel(asn string, idx int) error {
	asn = w.rs(asn)
	ss := w.mcs().ssccs[asn]
	if idx < 1 || idx > len(ss) {
		return fmt.Errorf("ASN %s has %d cartons, no carton %d", asn, len(ss), idx)
	}
	if err := w.postRawScan(asn, labelSSCC(ss[idx-1])); err != nil {
		return err
	}
	if w.last.status != http.StatusCreated || w.last.json()["accepted"] != true {
		return fmt.Errorf("scan of carton %d: status %d, want 201 accepted=true (body: %s)", idx, w.last.status, w.last.body)
	}
	if d, _ := w.last.json()["discrepancies"].([]any); len(d) != 0 {
		return fmt.Errorf("scan of carton %d raised discrepancies: %s", idx, w.last.body)
	}
	return nil
}

// ---------------------------------------------------------------------
// WrongDestination (ADR 0024 section 2)
// ---------------------------------------------------------------------

// scanCartonWithShipTo scans an announced carton's label with an AI 410
// ship-to GLN. gln is the literal 13 digits, or the phrase "the receiving
// site GLN" for the harness GLN of services.sh.
func (w *world) scanCartonWithShipTo(asn string, idx int, gln string) error {
	asn = w.rs(asn)
	if strings.Contains(gln, "receiving site") {
		gln = labelSiteGLN
	}
	ss := w.mcs().ssccs[asn]
	if idx < 1 || idx > len(ss) {
		return fmt.Errorf("ASN %s has %d cartons, no carton %d", asn, len(ss), idx)
	}
	return w.postRawScan(asn, labelSSCCShipTo(ss[idx-1], gln))
}

// scanRefusedWrongDestination asserts the exact refusal of the destination
// rule: 200 accepted=false, ONE finding, kind WrongDestination, the GLN on
// the label as `actual`, the carton's SSCC, the supplier, the CBV
// mismatch_instance, no W13 code, nothing booked.
func (w *world) scanRefusedWrongDestination(gln string, idx int, asn string) error {
	asn = w.rs(asn)
	if w.last.status != http.StatusOK || w.last.json()["accepted"] != false {
		return fmt.Errorf("wrong-destination scan: status %d, want 200 accepted=false (body: %s)", w.last.status, w.last.body)
	}
	ds, _ := w.last.json()["discrepancies"].([]any)
	if len(ds) != 1 {
		return fmt.Errorf("wrong-destination scan raised %d discrepancies, want exactly 1 (body: %s)", len(ds), w.last.body)
	}
	d, _ := ds[0].(map[string]any)
	wantSSCC := w.mcs().ssccs[asn][idx-1]
	if d["kind"] != "WrongDestination" || d["actual"] != gln || d["sscc"] != wantSSCC {
		return fmt.Errorf("finding = %v, want WrongDestination actual=%s sscc=%s (body: %s)", d, gln, wantSSCC, w.last.body)
	}
	if d["supplierRef"] != "ACME" || d["cbvDisposition"] != "mismatch_instance" {
		return fmt.Errorf("finding supplierRef=%v cbvDisposition=%v, want ACME / mismatch_instance (body: %s)", d["supplierRef"], d["cbvDisposition"], w.last.body)
	}
	if _, present := d["w13Code"]; present {
		return fmt.Errorf("WrongDestination is W13-unmapped, w13Code must be absent (body: %s)", w.last.body)
	}
	if expected, _ := d["expected"].(string); expected != "" {
		return fmt.Errorf("WrongDestination carries expected %q, want none (body: %s)", expected, w.last.body)
	}
	return nil
}

// wrongDestinationEventPublished asserts the finding on the topic with the
// same exactness: the GLN, the SSCC, the supplier, mismatch_instance, no
// W13 code.
func (w *world) wrongDestinationEventPublished(gln string, idx int, asn string) error {
	asn = w.rs(asn)
	rid, err := w.receiptIDFor(asn)
	if err != nil {
		return err
	}
	sscc := w.mcs().ssccs[asn][idx-1]
	p, err := expectKafkaEvent(topicInboundEvents, typeCartonDiscrepancyRaised,
		func(p map[string]any) bool {
			return p["receipt_id"] == rid && p["kind"] == "WrongDestination" && p["sscc"] == sscc
		}, fmt.Sprintf("WrongDestination of carton %d of ASN %s", idx, asn))
	if err != nil {
		return err
	}
	if p["actual"] != gln || p["supplier_ref"] != "ACME" || p["cbv_disposition"] != "mismatch_instance" {
		return fmt.Errorf("WrongDestination event: actual=%v supplier_ref=%v cbv=%v, want %s / ACME / mismatch_instance (payload: %v)", p["actual"], p["supplier_ref"], p["cbv_disposition"], gln, p)
	}
	if _, present := p["w13_code"]; present {
		return fmt.Errorf("WrongDestination event is W13-unmapped, w13_code must be absent (payload: %v)", p)
	}
	return nil
}

// scanAcceptedBooksEaches asserts the positive side of the rule: the scan of
// an announced carton whose AI 410 names THE receiving site is accepted
// (201, no findings), books exactly qty eaches on the line, and the carton
// read shows who labelled it (ssccSource supplier) and where it ships
// (shipToGln): the label's data was kept, not just tolerated.
func (w *world) scanAcceptedBooksEaches(qty, lineNo int, gln string) error {
	if strings.Contains(gln, "receiving site") {
		gln = labelSiteGLN
	}
	res := w.last.json()
	if w.last.status != http.StatusCreated || res["accepted"] != true {
		return fmt.Errorf("site-GLN scan: status %d, want 201 accepted=true (body: %s)", w.last.status, w.last.body)
	}
	if d, _ := res["discrepancies"].([]any); len(d) != 0 {
		return fmt.Errorf("site-GLN scan raised discrepancies: %s", w.last.body)
	}
	lines, _ := res["lines"].([]any)
	if lineNo < 1 || lineNo > len(lines) {
		return fmt.Errorf("scan answer has %d lines, no line %d (body: %s)", len(lines), lineNo, w.last.body)
	}
	g, _ := intOf(lines[lineNo-1].(map[string]any)["receivedGood"])
	if g != qty {
		return fmt.Errorf("line %d booked %d good eaches, want exactly %d (the refused carton must not have booked) (body: %s)", lineNo, g, qty, w.last.body)
	}
	cs, _ := res["cartons"].([]any)
	if len(cs) != 1 {
		return fmt.Errorf("scan answer carries %d cartons, want 1 (body: %s)", len(cs), w.last.body)
	}
	c, _ := cs[0].(map[string]any)
	if c["ssccSource"] != "supplier" || c["shipToGln"] != gln {
		return fmt.Errorf("carton ssccSource=%v shipToGln=%v, want supplier / %s (body: %s)", c["ssccSource"], c["shipToGln"], gln, w.last.body)
	}
	return nil
}

// cartonReceivedEventPublished asserts the ReceiptCartonReceived of the scan:
// sscc_source supplier (a supplier label was scanned) and the label's AI 410
// kept as ship_to_gln — the additive fields of ADR 0024 section 3 on the
// published contract, not only on the REST answer.
func (w *world) cartonReceivedEventPublished(idx int, asn, source, gln string) error {
	asn = w.rs(asn)
	if strings.Contains(gln, "receiving site") {
		gln = labelSiteGLN
	}
	rid, err := w.receiptIDFor(asn)
	if err != nil {
		return err
	}
	sscc := w.mcs().ssccs[asn][idx-1]
	p, err := expectKafkaEvent(topicInboundEvents, typeReceiptCartonReceived,
		func(p map[string]any) bool {
			return p["receipt_id"] == rid && p["sscc"] == sscc
		}, fmt.Sprintf("ReceiptCartonReceived of carton %d of ASN %s", idx, asn))
	if err != nil {
		return err
	}
	if p["sscc_source"] != source || p["ship_to_gln"] != gln {
		return fmt.Errorf("ReceiptCartonReceived: sscc_source=%v ship_to_gln=%v, want %s / %s (payload: %v)", p["sscc_source"], p["ship_to_gln"], source, gln, p)
	}
	return nil
}

// ---------------------------------------------------------------------
// Earliest best-before (ADR 0024 section 3)
// ---------------------------------------------------------------------

// scanCartonBestBefore scans an announced carton with a ]C1 label carrying
// AI 15 yymmdd and must be accepted: a label with a valid date never trips
// the label rules.
func (w *world) scanCartonBestBefore(asn string, idx int, yymmdd string) error {
	asn = w.rs(asn)
	ss := w.mcs().ssccs[asn]
	if idx < 1 || idx > len(ss) {
		return fmt.Errorf("ASN %s has %d cartons, no carton %d", asn, len(ss), idx)
	}
	if err := w.postRawScan(asn, labelSSCCBestBefore(ss[idx-1], yymmdd)); err != nil {
		return err
	}
	if w.last.status != http.StatusCreated || w.last.json()["accepted"] != true {
		return fmt.Errorf("AI 15 scan of carton %d: status %d, want 201 accepted=true (body: %s)", idx, w.last.status, w.last.body)
	}
	if d, _ := w.last.json()["discrepancies"].([]any); len(d) != 0 {
		return fmt.Errorf("AI 15 scan of carton %d raised discrepancies: %s", idx, w.last.body)
	}
	return nil
}

// palletEarliestBestBefore asserts the receipt's pallets[] block EXACTLY: one
// pallet (this ASN's), the count of accepted cartons, the earliest AI 15 as
// an ISO date and NO expiry (no carton carried AI 17 — the value can only
// come from the best-before rule, never from a mix-up with AI 17).
func (w *world) palletEarliestBestBefore(asn string, cartons int, bestBefore string) error {
	asn = w.rs(asn)
	rec, err := w.readReceipt(asn)
	if err != nil {
		return err
	}
	ps, _ := rec["pallets"].([]any)
	if len(ps) != 1 {
		return fmt.Errorf("receipt of ASN %s has %d pallets, want exactly 1 (body: %s)", asn, len(ps), w.last.body)
	}
	p, _ := ps[0].(map[string]any)
	if p["parentSscc"] != w.lb().palletByASN[asn] {
		return fmt.Errorf("pallet parentSscc = %v, want %s (body: %s)", p["parentSscc"], w.lb().palletByASN[asn], w.last.body)
	}
	cc, _ := intOf(p["cartonCount"])
	if cc != cartons {
		return fmt.Errorf("pallet cartonCount = %d, want %d (body: %s)", cc, cartons, w.last.body)
	}
	if p["earliestBestBefore"] != bestBefore {
		return fmt.Errorf("pallet earliestBestBefore = %v, want %q (body: %s)", p["earliestBestBefore"], bestBefore, w.last.body)
	}
	if _, present := p["earliestExpiry"]; present {
		return fmt.Errorf("pallet carries earliestExpiry=%v, want none (no AI 17 was scanned) (body: %s)", p["earliestExpiry"], w.last.body)
	}
	return nil
}

// lineEarliestBestBefore asserts the same derivation on the line read.
func (w *world) lineEarliestBestBefore(asn string, lineNo int, bestBefore string) error {
	asn = w.rs(asn)
	rec, err := w.readReceipt(asn)
	if err != nil {
		return err
	}
	lines, _ := rec["lines"].([]any)
	if lineNo < 1 || lineNo > len(lines) {
		return fmt.Errorf("receipt of ASN %s has %d lines, no line %d (body: %s)", asn, len(lines), lineNo, w.last.body)
	}
	if got := lines[lineNo-1].(map[string]any)["earliestBestBefore"]; got != bestBefore {
		return fmt.Errorf("line %d earliestBestBefore = %v, want %q (body: %s)", lineNo, got, bestBefore, w.last.body)
	}
	return nil
}

// ---------------------------------------------------------------------
// AssignSscc (ADR 0024 section 4)
// ---------------------------------------------------------------------

// assignSsccForCase POSTs the one sscc-assignment of the scenario with a
// key THIS scenario owns (kept for the replay step) and asserts the whole
// 201 answer: an 18-digit SSCC under extension 0 + the receiver prefix, a
// mod-10 check digit that validates, ssccSource receiver, the case's SKU and
// units, and NO finding — against a flat ASN nobody promised a label, so no
// UnlabelledCarton may be raised.
func (w *world) assignSsccForCase(asn, sku string, perCase int) error {
	asn, sku = w.rs(asn), w.rs(sku)
	m := w.mcs()
	gtin, ok := m.caseGtin[sku]
	if !ok {
		return fmt.Errorf("SKU %s has no declared CASE GTIN in this process", sku)
	}
	rid, err := w.receiptIDFor(asn)
	if err != nil {
		return err
	}
	key := newIdempotencyKey()
	w.lb().assignKey[asn] = key
	w.lb().assignedGtin[asn] = gtin
	if err := w.postJSONIdempotent(fmt.Sprintf("%s/receipts/%s/sscc-assignments", inboundBaseURL, rid), key,
		map[string]any{"gtin": gtin}); err != nil {
		return err
	}
	if w.last.status != http.StatusCreated {
		return fmt.Errorf("sscc-assignment: status %d, want 201 (body: %s)", w.last.status, w.last.body)
	}
	res := w.last.json()
	sscc, _ := res["sscc"].(string)
	if len(sscc) != 18 || !strings.HasPrefix(sscc, "0"+labelReceiverPrefix) {
		return fmt.Errorf("assigned SSCC %q, want 18 digits starting with extension 0 + %s (body: %s)", sscc, labelReceiverPrefix, w.last.body)
	}
	if want := sscc[:17] + gs1CheckDigit(sscc[:17]); sscc != want {
		return fmt.Errorf("assigned SSCC %s has check digit %q, want %q (mod-10)", sscc, sscc[17:], want[17:])
	}
	if res["ssccSource"] != "receiver" || res["lpn"] != sscc {
		return fmt.Errorf("assignment ssccSource=%v lpn=%v, want receiver / the SSCC (body: %s)", res["ssccSource"], res["lpn"], w.last.body)
	}
	if res["sku"] != sku {
		return fmt.Errorf("assignment sku=%v, want %s (body: %s)", res["sku"], sku, w.last.body)
	}
	units, _ := intOf(res["units"])
	if units != perCase {
		return fmt.Errorf("assignment units=%d, want %d (body: %s)", units, perCase, w.last.body)
	}
	if d, _ := res["discrepancies"].([]any); len(d) != 0 {
		return fmt.Errorf("assignment against a flat ASN raised %d findings, want none (body: %s)", len(d), w.last.body)
	}
	w.lb().assignedSSCC[asn] = sscc
	return nil
}

// replaySsccAssignment replays the SAME request under the SAME
// Idempotency-Key: the service must answer the stored 201 with the SAME SSCC
// and the Idempotency-Replayed header, and receive nothing twice.
func (w *world) replaySsccAssignment(asn string) error {
	asn = w.rs(asn)
	gtin := w.lb().assignedGtin[asn]
	if gtin == "" {
		return fmt.Errorf("no sscc-assignment has been made for ASN %s in this scenario", asn)
	}
	rid, err := w.receiptIDFor(asn)
	if err != nil {
		return err
	}
	key := w.lb().assignKey[asn]
	if key == "" {
		return fmt.Errorf("no sscc-assignment has been made for ASN %s in this scenario", asn)
	}
	if err := w.postJSONIdempotent(fmt.Sprintf("%s/receipts/%s/sscc-assignments", inboundBaseURL, rid), key,
		map[string]any{"gtin": gtin}); err != nil {
		return err
	}
	if w.last.status != http.StatusCreated {
		return fmt.Errorf("replayed sscc-assignment: status %d, want the stored 201 (body: %s)", w.last.status, w.last.body)
	}
	if got := w.last.json()["sscc"]; got != w.lb().assignedSSCC[asn] {
		return fmt.Errorf("replayed SSCC = %v, want the same %s (body: %s)", got, w.lb().assignedSSCC[asn], w.last.body)
	}
	if v := w.last.header.Get("Idempotency-Replayed"); v != "true" {
		return fmt.Errorf("replayed sscc-assignment: Idempotency-Replayed=%q, want true (body: %s)", v, w.last.body)
	}
	return nil
}

// assignedCartonEventPublished asserts the ReceiptCartonReceived of the
// ASSIGNED carton carries sscc_source receiver on the published contract.
func (w *world) assignedCartonEventPublished(asn string) error {
	asn = w.rs(asn)
	rid, err := w.receiptIDFor(asn)
	if err != nil {
		return err
	}
	sscc := w.lb().assignedSSCC[asn]
	p, err := expectKafkaEvent(topicInboundEvents, typeReceiptCartonReceived,
		func(p map[string]any) bool {
			return p["receipt_id"] == rid && p["sscc"] == sscc
		}, "ReceiptCartonReceived of the assigned SSCC of ASN "+asn)
	if err != nil {
		return err
	}
	if p["sscc_source"] != "receiver" {
		return fmt.Errorf("assigned carton event sscc_source=%v, want receiver (payload: %v)", p["sscc_source"], p)
	}
	if p["lpn"] != sscc {
		return fmt.Errorf("assigned carton event lpn=%v, want the SSCC %s (payload: %v)", p["lpn"], sscc, p)
	}
	return nil
}

// ---------------------------------------------------------------------
// ReceivingAdviceIssued (ADR 0023 section 5)
// ---------------------------------------------------------------------

// noAdviceWhileOpen proves the negative half of the advice contract: while
// the receipt is still Open, no ReceivingAdviceIssued for its ASN exists on
// warehouse.inbound-receiving.events (the advice is a commitment made at
// close, never before).
func (w *world) noAdviceWhileOpen(asn string) error {
	asn = w.rs(asn)
	return expectNoKafkaEvent(topicInboundEvents, typeReceivingAdviceIssued,
		func(p map[string]any) bool { return p["asn_number"] == asn },
		"no advice may exist for open ASN "+asn)
}

// adviceIssuedAfterClose asserts the ReceivingAdviceIssued of the closed
// receipt: the references (asn_number, supplier_ref, despatch_advice_ref =
// the ASN number, the RECADV "reference to the despatch advice"), the SLA
// block (within_sla true under the 24h default) and the per-line quantities
// of the table (accepted = Good - held, rejected = Damaged), with the W13
// code on the damaged line's Damaged finding and NO finding on the clean
// line. It also proves the ORDER: ReceiptClosed is published first (the
// outbox keeps unit-of-work order) and the advice right after.
func (w *world) adviceIssuedAfterClose(asn string, supplier string, tbl *godog.Table) error {
	asn = w.rs(asn)
	rid, err := w.receiptIDFor(asn)
	if err != nil {
		return err
	}
	type wantLine struct {
		lineNo                   int
		sku                      string
		expected, received       int
		accepted, rejected, held int
		w13                      string // "-" = the line has NO discrepancy at all
	}
	var want []wantLine
	for i, row := range tbl.Rows {
		if i == 0 {
			continue
		}
		if len(row.Cells) != 8 {
			return fmt.Errorf("advice line table row %d has %d cells, want 8", i, len(row.Cells))
		}
		var l wantLine
		l.sku, l.w13 = w.rs(row.Cells[1].Value), row.Cells[7].Value
		for cell, dst := range map[int]*int{0: &l.lineNo, 2: &l.expected, 3: &l.received, 4: &l.accepted, 5: &l.rejected, 6: &l.held} {
			if _, err := fmt.Sscanf(row.Cells[cell].Value, "%d", dst); err != nil {
				return fmt.Errorf("advice line table row %d cell %d %q is not a number", i, cell, row.Cells[cell].Value)
			}
		}
		want = append(want, l)
	}

	// ReceiptClosed first, the advice right after: both scans read the topic
	// from its first offset, so the message counts order them.
	closed, seenClosed, err := scanTopic(topicInboundEvents, kafkaScanWindow, func(m ceMessage) bool {
		return m.Type == typeReceiptClosed && m.Subject == rid
	})
	if err != nil {
		return err
	}
	if closed == nil {
		return fmt.Errorf("no ReceiptClosed of receipt %s on %s within %s", rid, topicInboundEvents, kafkaScanWindow)
	}
	adv, seenAdvice, err := scanTopic(topicInboundEvents, kafkaScanWindow, func(m ceMessage) bool {
		return m.Type == typeReceivingAdviceIssued && m.Subject == rid
	})
	if err != nil {
		return err
	}
	if adv == nil {
		return fmt.Errorf("no ReceivingAdviceIssued of receipt %s on %s within %s", rid, topicInboundEvents, kafkaScanWindow)
	}
	if seenClosed >= seenAdvice {
		return fmt.Errorf("ReceivingAdviceIssued (message %d) is not after ReceiptClosed (message %d) on %s", seenAdvice, seenClosed, topicInboundEvents)
	}
	p := adv.payload()
	if p["receipt_id"] != rid || p["asn_number"] != asn {
		return fmt.Errorf("advice receipt_id=%v asn_number=%v, want %s / %s (payload: %v)", p["receipt_id"], p["asn_number"], rid, asn, p)
	}
	if p["despatch_advice_ref"] != asn {
		return fmt.Errorf("advice despatch_advice_ref=%v, want the ASN number %s (payload: %v)", p["despatch_advice_ref"], asn, p)
	}
	if p["supplier_ref"] != supplier {
		return fmt.Errorf("advice supplier_ref=%v, want %q (payload: %v)", p["supplier_ref"], supplier, p)
	}
	if p["within_sla"] != true {
		return fmt.Errorf("advice within_sla=%v, want true (payload: %v)", p["within_sla"], p)
	}
	sla, _ := intOf(p["sla_seconds"])
	if sla != 86400 {
		return fmt.Errorf("advice sla_seconds=%d, want the 24h default 86400 (payload: %v)", sla, p)
	}
	lines, _ := p["lines"].([]any)
	total, _ := intOf(p["lines_total"])
	if len(lines) != len(want) || total != len(want) {
		return fmt.Errorf("advice carries %d of %d lines (lines_total %d), want exactly %d (payload: %v)", len(lines), total, len(want), len(want), p)
	}
	for i, wl := range want {
		l, _ := lines[i].(map[string]any)
		got := func(k string) int { v, _ := intOf(l[k]); return v }
		if got("line_no") != wl.lineNo || l["sku"] != wl.sku ||
			got("expected_qty") != wl.expected || got("received_qty") != wl.received ||
			got("accepted_qty") != wl.accepted || got("rejected_qty") != wl.rejected || got("held_qty") != wl.held {
			return fmt.Errorf("advice line %d = %v, want line %d %s %d/%d received, accepted %d rejected %d held %d (payload: %v)",
				i+1, l, wl.lineNo, wl.sku, wl.expected, wl.received, wl.accepted, wl.rejected, wl.held, p)
		}
		ds, _ := l["discrepancies"].([]any)
		if wl.w13 == "-" {
			if len(ds) != 0 {
				return fmt.Errorf("clean line %d carries %d discrepancies, want none (payload: %v)", wl.lineNo, len(ds), p)
			}
			continue
		}
		if len(ds) != 1 {
			return fmt.Errorf("line %d carries %d discrepancies, want exactly the Damaged one (payload: %v)", wl.lineNo, len(ds), p)
		}
		d, _ := ds[0].(map[string]any)
		qty, _ := intOf(d["quantity"])
		if d["kind"] != "Damaged" || d["w13_code"] != wl.w13 || d["cbv_disposition"] != "damaged" || qty != wl.rejected {
			return fmt.Errorf("line %d discrepancy = %v, want Damaged quantity %d w13_code %q cbv damaged (payload: %v)",
				wl.lineNo, d, wl.rejected, wl.w13, p)
		}
	}
	return nil
}

// ---------------------------------------------------------------------
// registration
// ---------------------------------------------------------------------

func registerLabelSteps(sc *godog.ScenarioContext, w *world) {
	// LabelNonCompliant (ADR 0024 section 1)
	sc.Step(`^a ]C1 label naming both AI 01 and AI 02 of SKU "([^"]*)" is scanned on the receipt of ASN "([^"]*)" in inbound-receiving$`,
		func(sku, asn string) error { return w.scanConflictingAIsLabel(asn, sku) })
	sc.Step(`^a ]C1 label carrying an SSCC with a wrong check digit is scanned on the receipt of ASN "([^"]*)" in inbound-receiving$`,
		w.scanBadCheckDigitLabel)
	sc.Step(`^that scan is refused as LabelNonCompliant with reason "([a-z_]+)" and supplier "([^"]*)", nothing booked$`, w.scanRefusedLabelNonCompliant)
	sc.Step(`^inbound-receiving published CartonDiscrepancyRaised kind LabelNonCompliant with reason "([a-z_]+)" and supplier "([^"]*)" for the receipt of ASN "([^"]*)"$`,
		w.labelNonCompliantEventPublished)
	sc.Step(`^the receipt of ASN "([^"]*)" in inbound-receiving keeps exactly the (\d+) LabelNonCompliant findings and no booked unit$`, w.receiptKeepsLabelFindings)
	sc.Step(`^carton (\d+) of ASN "([^"]*)" is scanned on its receipt with a valid ]C1 SSCC label in inbound-receiving$`,
		func(idx int, asn string) error { return w.scanCartonWithSSCCLabel(asn, idx) })

	// WrongDestination (ADR 0024 section 2)
	sc.Step(`^carton (\d+) of ASN "([^"]*)" is scanned with a ]C1 label whose AI 410 names GLN "([^"]*)" on its receipt in inbound-receiving$`,
		func(idx int, asn, gln string) error { return w.scanCartonWithShipTo(asn, idx, gln) })
	sc.Step(`^carton (\d+) of ASN "([^"]*)" is scanned with a ]C1 label whose AI 410 names the receiving site GLN on its receipt in inbound-receiving$`,
		func(idx int, asn string) error { return w.scanCartonWithShipTo(asn, idx, "the receiving site GLN") })
	sc.Step(`^that scan is refused as WrongDestination for GLN "([^"]*)" and the SSCC of carton (\d+) of ASN "([^"]*)", nothing booked$`, w.scanRefusedWrongDestination)
	sc.Step(`^inbound-receiving published CartonDiscrepancyRaised kind WrongDestination for GLN "([^"]*)" and the SSCC of carton (\d+) of ASN "([^"]*)" with supplier "([^"]*)"$`,
		func(gln string, idx int, asn, supplier string) error {
			if supplier != "ACME" {
				return fmt.Errorf("supplier %q: every inbound scenario's ASN is ACME", supplier)
			}
			return w.wrongDestinationEventPublished(gln, idx, asn)
		})
	sc.Step(`^that scan is accepted, books (\d+) eaches on line (\d+) and the carton shows ssccSource "([^"]*)" and shipToGln "([^"]*)"$`,
		func(qty, lineNo int, source, gln string) error {
			if source != "supplier" {
				return fmt.Errorf("ssccSource %q: a scanned supplier label is the positive case", source)
			}
			return w.scanAcceptedBooksEaches(qty, lineNo, gln)
		})
	sc.Step(`^inbound-receiving published ReceiptCartonReceived for carton (\d+) of ASN "([^"]*)" with sscc_source "([^"]*)" and ship_to_gln "([^"]*)"$`, w.cartonReceivedEventPublished)

	// Earliest best-before (ADR 0024 section 3)
	sc.Step(`^ASN "([^"]*)" announcing (\d+) homogeneous cartons of (\d+) eaches of SKU "([^"]*)" on one pallet with valid SSCCs is registered in inbound-receiving$`, w.palletAsnRegistered)
	sc.Step(`^carton (\d+) of ASN "([^"]*)" is scanned with a ]C1 label carrying AI 15 "(\d+)" on its receipt in inbound-receiving$`,
		func(idx int, asn, yymmdd string) error { return w.scanCartonBestBefore(asn, idx, yymmdd) })
	sc.Step(`^the pallet of ASN "([^"]*)" in inbound-receiving carries (\d+) accepted cartons with earliest best-before "([^"]*)" and no expiry$`, w.palletEarliestBestBefore)
	sc.Step(`^line (\d+) of the receipt of ASN "([^"]*)" in inbound-receiving has earliest best-before "([^"]*)"$`,
		func(lineNo int, asn, bestBefore string) error {
			return w.lineEarliestBestBefore(asn, lineNo, bestBefore)
		})

	// AssignSscc (ADR 0024 section 4)
	sc.Step(`^I assign an SSCC to one unlabelled case of SKU "([^"]*)" of (\d+) eaches on the receipt of ASN "([^"]*)" in inbound-receiving$`,
		func(sku string, perCase int, asn string) error { return w.assignSsccForCase(asn, sku, perCase) })
	sc.Step(`^the same sscc-assignment is replayed with the same Idempotency-Key for ASN "([^"]*)"$`, w.replaySsccAssignment)
	sc.Step(`^inbound-receiving published ReceiptCartonReceived for the assigned SSCC of ASN "([^"]*)" with sscc_source "receiver"$`, w.assignedCartonEventPublished)

	// ReceivingAdviceIssued (ADR 0023 section 5)
	sc.Step(`^ASN "([^"]*)" from supplier "([^"]*)" with (\d+) units of SKU "([^"]*)" on line 1 and (\d+) units of SKU "([^"]*)" on line 2 is eventually registered in inbound-receiving$`, w.twoLineAsnRegistered)
	sc.Step(`^no ReceivingAdviceIssued for ASN "([^"]*)" is on warehouse.inbound-receiving.events while its receipt is open$`, w.noAdviceWhileOpen)
	sc.Step(`^inbound-receiving published ReceivingAdviceIssued for ASN "([^"]*)" from supplier "([^"]*)" right after ReceiptClosed with:$`, w.adviceIssuedAfterClose)
}
