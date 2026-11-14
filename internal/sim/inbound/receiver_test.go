package inbound

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// docked books, checks in and docks a delivery of the lines: everything up to the receiver's first call.
func docked(t *testing.T, c *Crew, lines ...Line) (ASN, Appointment, Visit) {
	t.Helper()
	asn, a := booked(t, c, lines...)
	v, err := c.GateIn(context.Background(), a)
	if err != nil {
		t.Fatalf("GateIn: %v", err)
	}
	return asn, a, v
}

func TestOpenReceiptOnTheAppointmentTakesItsDoor(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	asn, a, _ := docked(t, c, Line{skuA, 10}, Line{skuB, 5})
	r, err := c.OpenReceipt(context.Background(), asn, a)
	if err != nil {
		t.Fatalf("OpenReceipt: %v", err)
	}
	if r.ID == "" || r.State != "Open" || r.ASN != asn.Number || r.AppointmentID != a.ID || r.Door != a.Door || len(r.Lines) != 2 {
		t.Fatalf("receipt = %+v", r)
	}
	var body map[string]any
	bodies := w.bodies("inbound-receiving POST /receipts")
	if err := json.Unmarshal(bodies[0], &body); err != nil {
		t.Fatal(err)
	}
	if body["asnNumber"] != asn.Number || body["appointmentId"] != a.ID {
		t.Errorf("body = %v, want the ASN and the appointment", body)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if st := w.recv.asns[asn.Number].state; st != "Receiving" {
		t.Errorf("ASN is %s, want Receiving", st)
	}
}

func TestOpenReceiptRetriesTheLagOfTheAppointmentCopy(t *testing.T) {
	w := newWorld(t)
	w.lagApptToReceiving = 3
	c := w.crew(w.seedSite())
	asn, a, _ := docked(t, c, Line{skuA, 10})
	if _, err := c.OpenReceipt(context.Background(), asn, a); err != nil {
		t.Fatalf("OpenReceipt: %v", err)
	}
	if n := w.served("inbound-receiving POST /receipts"); n != 4 {
		t.Errorf("%d POST /receipts, want 4 (3 unknown-appointment then the receipt)", n)
	}
}

func TestOpenReceiptOfACopyThatNeverArrivesTimesOut(t *testing.T) {
	w := newWorld(t)
	w.lagApptToReceiving = 1 << 20
	c := w.crew(w.seedSite())
	asn, a, _ := docked(t, c, Line{skuA, 10})
	_, err := c.OpenReceipt(context.Background(), asn, a)
	if err == nil || !IsTimeout(err) || !strings.Contains(err.Error(), "unknown-appointment") {
		t.Fatalf("err = %v, want a timeout naming unknown-appointment", err)
	}
}

func TestOpenReceiptOfAnASNNotOnTheAppointmentIsRefusedNotRetried(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	_, a, _ := docked(t, c, Line{skuA, 10})
	other := registered(t, c, Line{skuB, 3})
	before := w.served("inbound-receiving POST /receipts")
	_, err := c.OpenReceipt(context.Background(), other, a)
	// Same status as the lag refusal, another slug: it must not be mistaken for lag.
	if err == nil || !strings.Contains(err.Error(), "422 asn-not-on-appointment") {
		t.Fatalf("err = %v, want 422 asn-not-on-appointment", err)
	}
	if n := w.served("inbound-receiving POST /receipts") - before; n != 1 {
		t.Errorf("%d requests, want exactly 1", n)
	}
}

func TestReceiveCountsEveryLineGoodExactlyOnceAndCloses(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	asn, a, _ := docked(t, c, Line{skuA, 10}, Line{skuB, 5})
	r, err := c.OpenReceipt(context.Background(), asn, a)
	if err != nil {
		t.Fatal(err)
	}
	r, err = c.Receive(context.Background(), asn, r)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if r.State != "Closed" || len(r.Discrepancies) != 0 {
		t.Fatalf("receipt = %+v, want Closed with no discrepancy", r)
	}
	if got := []int{r.Lines[0].ReceivedGood, r.Lines[1].ReceivedGood}; got[0] != 10 || got[1] != 5 {
		t.Errorf("received good = %v, want [10 5]", got)
	}
	if n := w.served("inbound-receiving POST /receipts/{id}/lines"); n != 2 {
		t.Errorf("%d line receipts, want exactly one per line (receiving is not idempotent: never counted twice)", n)
	}
	if n := w.served("inbound-receiving POST /receipts/{id}/closures"); n != 1 {
		t.Errorf("%d closures, want 1", n)
	}
	for i, b := range w.bodies("inbound-receiving POST /receipts/{id}/lines") {
		var body map[string]any
		_ = json.Unmarshal(b, &body)
		want := []float64{10, 5}[i]
		if body["lineNo"] != float64(i+1) || body["quantity"] != want || body["condition"] != "Good" {
			t.Errorf("line body %d = %v, want line %d, %v Good", i, body, i+1, want)
		}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.inv.staged[skuA] != 10 || w.inv.staged[skuB] != 5 {
		t.Errorf("inventory-storage staged %v, want exactly the received units", w.inv.staged)
	}
}

func TestReceiveStopsAtTheFirstRefusalAndNeverCloses(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	asn, a, _ := docked(t, c, Line{skuA, 10})
	r, err := c.OpenReceipt(context.Background(), asn, a)
	if err != nil {
		t.Fatal(err)
	}
	// The caller's ASN has a line the receipt (its snapshot of the ASN) does not.
	asn.Lines = append(asn.Lines, Line{skuB, 4})
	got, err := c.Receive(context.Background(), asn, r)
	if err == nil || !strings.Contains(err.Error(), "422 line-not-on-asn") {
		t.Fatalf("err = %v, want 422 line-not-on-asn", err)
	}
	if got.Lines[0].ReceivedGood != 10 {
		t.Errorf("the receipt returned shows %d good on line 1, want the 10 counted before the refusal", got.Lines[0].ReceivedGood)
	}
	if n := w.served("inbound-receiving POST /receipts/{id}/closures"); n != 0 {
		t.Errorf("%d closures after a refusal, want 0: a receipt that was not fully counted is not closed", n)
	}
}

func TestCloseReceiptReportsTheDiscrepanciesTheServiceFound(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	asn, a, _ := docked(t, c, Line{skuA, 10}, Line{skuB, 5})
	r, err := c.OpenReceipt(context.Background(), asn, a)
	if err != nil {
		t.Fatal(err)
	}
	if r, err = c.ReceiveLine(context.Background(), r, 1, 10); err != nil {
		t.Fatal(err)
	}
	r, err = c.CloseReceipt(context.Background(), r)
	if err != nil {
		t.Fatalf("CloseReceipt: %v", err)
	}
	if len(r.Discrepancies) != 1 || !strings.Contains(r.Discrepancies[0], "Short") || !strings.Contains(r.Discrepancies[0], skuB) || !strings.Contains(r.Discrepancies[0], "expected 5, received 0") {
		t.Errorf("discrepancies = %v, want the Short of line 2", r.Discrepancies)
	}
}

func TestClosingAClosedReceiptIsRefusedNotRetried(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	asn, a, _ := docked(t, c, Line{skuA, 10})
	r, _ := c.OpenReceipt(context.Background(), asn, a)
	r, err := c.Receive(context.Background(), asn, r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.CloseReceipt(context.Background(), r); err == nil || !strings.Contains(err.Error(), "409 receipt-closed") {
		t.Fatalf("err = %v, want 409 receipt-closed", err)
	}
	if n := w.served("inbound-receiving POST /receipts/{id}/closures"); n != 2 {
		t.Errorf("%d closures, want 2 (Receive's, then the refused one, not retried)", n)
	}
}
