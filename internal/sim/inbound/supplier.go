package inbound

import (
	"context"
	"fmt"
	"net/http"

	"github.com/claudioed/warehouse-e2e/internal/sim"
	"github.com/claudioed/warehouse-e2e/internal/sim/outbound"
)

// The consumer-lag refusals of the inbound leg, each one read from the owning service's contract.
var (
	// inbound-receiving has not yet seen the SKU in product-master's feed (its known_skus copy).
	unknownSKU = outbound.Transient{Status: http.StatusUnprocessableEntity, Slug: "unknown-sku"}
	// dock-scheduling has not consumed facility-layout's dock slot yet.
	unknownDoor = outbound.Transient{Status: http.StatusUnprocessableEntity, Slug: "unknown-door"}
	// dock-scheduling has not consumed inbound-receiving's ASNRegistered yet.
	unknownASN = outbound.Transient{Status: http.StatusUnprocessableEntity, Slug: "unknown-asn"}
	// yard-management has not consumed DockAppointmentBooked yet.
	yardUnknownAppointment = outbound.Transient{Status: http.StatusUnprocessableEntity, Slug: "unknown-appointment"}
	// yard-management has not consumed facility-layout's door slot yet.
	yardUnknownLocation = outbound.Transient{Status: http.StatusUnprocessableEntity, Slug: "unknown-location"}
	// inbound-receiving has not consumed DockAppointmentBooked yet (its local copy of the appointment).
	receivingUnknownAppointment = outbound.Transient{Status: http.StatusUnprocessableEntity, Slug: "unknown-appointment"}
)

// ---------------------------------------------------------------------
// supplier (inbound-receiving ASNs)
// ---------------------------------------------------------------------

// RegisterASN is the supplier announcing a delivery: one ASN with a line per SKU (duplicates merged, sorted by
// SKU, numbered 1..n), registered in inbound-receiving. The ASN number is run-scoped (ASN-<RunID>-IB-nnn) and
// never reused.
//
// 422 unknown-sku is the consumer lag of product-master's SKU feed in inbound-receiving's local copy and is
// retried in a bounded loop; a SKU that never arrives times out saying product-master must know it. Any other
// refusal (409 asn-already-exists, a 400 domain rule) fails at once.
func (c *Crew) RegisterASN(ctx context.Context, lines []Line) (ASN, error) {
	lines = normalise(lines)
	if len(lines) == 0 {
		return ASN{}, fmt.Errorf("register an ASN: no line with a SKU and a positive quantity to announce")
	}
	asn := ASN{Number: fmt.Sprintf("ASN-%s-IB-%03d", c.RunID, c.next()), Lines: lines}
	wire := make([]map[string]any, len(lines))
	for i, l := range lines {
		wire[i] = map[string]any{"lineNo": i + 1, "sku": l.SKU, "expectedQty": l.Qty}
	}
	body := map[string]any{"asnNumber": asn.Number, "supplierRef": c.supplier(), "lines": wire}
	what := fmt.Sprintf("register ASN %s (%s)", asn.Number, describeLines(lines))
	r, err := c.callTransient(ctx, what, []outbound.Transient{unknownSKU}, func() sim.Resp {
		return c.call(ctx, SvcInbound, http.MethodPost, "/asns", "/asns", body)
	})
	if err != nil {
		if IsTimeout(err) {
			err = fmt.Errorf("%w: a SKU of the ASN is not in inbound-receiving's copy of product-master: is every SKU registered in product-master?", err)
		}
		return asn, err
	}
	var got struct {
		ASNNumber string `json:"asnNumber"`
		State     string `json:"state"`
		Lines     []struct {
			LineNo      int    `json:"lineNo"`
			SKU         string `json:"sku"`
			ExpectedQty int    `json:"expectedQty"`
		} `json:"lines"`
	}
	if err := r.Decode(&got); err != nil {
		return asn, fmt.Errorf("%s: %w", what, err)
	}
	if r.Status != http.StatusCreated || got.ASNNumber != asn.Number || got.State != "Registered" || len(got.Lines) != len(lines) {
		return asn, fmt.Errorf("%s: status %d, ASN %q state %q with %d lines, want 201 Registered with %d (body: %s)",
			what, r.Status, got.ASNNumber, got.State, len(got.Lines), len(lines), sim.Truncate(string(r.Body), 200))
	}
	c.logf("supplier", "announced %s: %s", asn.Number, describeLines(lines))
	return asn, nil
}
