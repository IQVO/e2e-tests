package inbound

import (
	"context"
	"fmt"
	"net/http"

	"github.com/claudioed/warehouse-e2e/internal/sim"
	"github.com/claudioed/warehouse-e2e/internal/sim/outbound"
)

// ---------------------------------------------------------------------
// receiver (inbound-receiving receipts)
// ---------------------------------------------------------------------

type receiptDTO struct {
	ReceiptID     string `json:"receiptId"`
	ASNNumber     string `json:"asnNumber"`
	AppointmentID string `json:"appointmentId"`
	DoorCode      string `json:"doorCode"`
	State         string `json:"state"`
	Lines         []struct {
		LineNo          int    `json:"lineNo"`
		SKU             string `json:"sku"`
		ExpectedQty     int    `json:"expectedQty"`
		ReceivedGood    int    `json:"receivedGood"`
		ReceivedDamaged int    `json:"receivedDamaged"`
	} `json:"lines"`
	Discrepancies []struct {
		LineNo      int    `json:"lineNo"`
		SKU         string `json:"sku"`
		Kind        string `json:"kind"`
		ExpectedQty int    `json:"expectedQty"`
		ReceivedQty int    `json:"receivedQty"`
		DamagedQty  int    `json:"damagedQty"`
	} `json:"discrepancies"`
}

func (d receiptDTO) receipt() Receipt {
	r := Receipt{ID: d.ReceiptID, ASN: d.ASNNumber, AppointmentID: d.AppointmentID, Door: d.DoorCode, State: d.State}
	for _, l := range d.Lines {
		r.Lines = append(r.Lines, ReceiptLine{LineNo: l.LineNo, SKU: l.SKU, Expected: l.ExpectedQty, ReceivedGood: l.ReceivedGood, ReceivedDamaged: l.ReceivedDamaged})
	}
	for _, x := range d.Discrepancies {
		r.Discrepancies = append(r.Discrepancies, fmt.Sprintf("%s line %d %s: expected %d, received %d, damaged %d", x.Kind, x.LineNo, x.SKU, x.ExpectedQty, x.ReceivedQty, x.DamagedQty))
	}
	return r
}

// OpenReceipt opens the receipt of the ASN on its appointment: the receipt takes the appointment's door and
// moves the ASN to Receiving. inbound-receiving looks the appointment up in its LOCAL COPY of dock-scheduling's,
// which fills over Kafka, so 422 unknown-appointment is consumer lag and is retried in a bounded loop. A
// refusal that has the same status but another slug (422 asn-not-on-appointment, 409 appointment-cancelled,
// 409 receipt-already-open) fails at once.
func (c *Crew) OpenReceipt(ctx context.Context, asn ASN, a Appointment) (Receipt, error) {
	what := fmt.Sprintf("open the receipt of ASN %s on appointment %s", asn.Number, a.ID)
	r, err := c.callTransient(ctx, what, []outbound.Transient{receivingUnknownAppointment}, func() sim.Resp {
		return c.call(ctx, SvcInbound, http.MethodPost, "/receipts", "/receipts", map[string]any{"asnNumber": asn.Number, "appointmentId": a.ID})
	})
	if err != nil {
		return Receipt{}, err
	}
	var dto receiptDTO
	if err := r.Decode(&dto); err != nil {
		return Receipt{}, fmt.Errorf("%s: %w", what, err)
	}
	got := dto.receipt()
	if r.Status != http.StatusCreated || got.ID == "" || got.State != "Open" || got.ASN != asn.Number || got.Door != a.Door {
		return got, fmt.Errorf("%s: status %d receipt %q state %q ASN %q door %q, want 201 Open on %s at door %s (body: %s)",
			what, r.Status, got.ID, got.State, got.ASN, got.Door, asn.Number, a.Door, sim.Truncate(string(r.Body), 200))
	}
	c.logf("receiver", "opened %s for %s at door %s", got.ID, asn.Number, got.Door)
	return got, nil
}

// ReceiveLine counts qty Good units against a line of the receipt and publishes ReceiptLineReceived, the
// handover inventory-storage books as staged stock. It is NOT idempotent by nature, so a line is counted once
// and a refusal or a transport error is returned at once, never retried: a retry under a fresh key would count the
// units twice.
func (c *Crew) ReceiveLine(ctx context.Context, rc Receipt, lineNo, qty int) (Receipt, error) {
	what := fmt.Sprintf("receive %d Good units of line %d on receipt %s", qty, lineNo, rc.ID)
	var before int
	for _, l := range rc.Lines {
		if l.LineNo == lineNo {
			before = l.ReceivedGood
		}
	}
	r := c.call(ctx, SvcInbound, http.MethodPost, "/receipts/{id}/lines", "/receipts/"+rc.ID+"/lines",
		map[string]any{"lineNo": lineNo, "quantity": qty, "condition": "Good"})
	if !r.OK() {
		return rc, refusal(what, r)
	}
	var dto receiptDTO
	if err := r.Decode(&dto); err != nil {
		return rc, fmt.Errorf("%s: %w", what, err)
	}
	got := dto.receipt()
	for _, l := range got.Lines {
		if l.LineNo == lineNo && l.ReceivedGood != before+qty {
			return got, fmt.Errorf("%s: the receipt shows %d Good on the line, want %d", what, l.ReceivedGood, before+qty)
		}
	}
	return got, nil
}

// CloseReceipt ends receiving: inbound-receiving computes the discrepancies (Short, Over, Damaged) and completes
// the ASN. The discrepancies come back on the Receipt for the caller to judge; closing is not refused for them.
// 409 receipt-closed (a second close) is a refusal, not retried.
func (c *Crew) CloseReceipt(ctx context.Context, rc Receipt) (Receipt, error) {
	what := fmt.Sprintf("close receipt %s", rc.ID)
	r := c.call(ctx, SvcInbound, http.MethodPost, "/receipts/{id}/closures", "/receipts/"+rc.ID+"/closures", map[string]any{})
	if !r.OK() {
		return rc, refusal(what, r)
	}
	var dto receiptDTO
	if err := r.Decode(&dto); err != nil {
		return rc, fmt.Errorf("%s: %w", what, err)
	}
	got := dto.receipt()
	if got.State != "Closed" {
		return got, fmt.Errorf("%s: receipt is %s, want Closed (body: %s)", what, got.State, sim.Truncate(string(r.Body), 200))
	}
	c.logf("receiver", "closed %s: %d discrepancies", got.ID, len(got.Discrepancies))
	return got, nil
}

// Receive counts every line of the ASN at its announced quantity as Good and closes the receipt. It stops at the
// first refusal and returns the receipt as far as it got; a receipt that was not fully counted is never closed.
func (c *Crew) Receive(ctx context.Context, asn ASN, rc Receipt) (Receipt, error) {
	for i, l := range asn.Lines {
		var err error
		if rc, err = c.ReceiveLine(ctx, rc, i+1, l.Qty); err != nil {
			return rc, err
		}
	}
	return c.CloseReceipt(ctx, rc)
}
