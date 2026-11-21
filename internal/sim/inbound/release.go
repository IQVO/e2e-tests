package inbound

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/claudioed/warehouse-e2e/internal/sim"
)

// typeStockReceived is the event inventory-storage raises on its analytics topic when it books a Good receipt line
// as staged stock (inventory-storage ADR 0037).
const typeStockReceived = "StockReceived"

// ---------------------------------------------------------------------
// gate: the truck leaves
// ---------------------------------------------------------------------

// Release lets the empty trailer leave: it waits, by READING the trailer, until yard-management has completed the
// unload (door work DONE, load EMPTY) in reaction to the receipt's close, then checks the visit out with its trailer.
//
// The clerk never completes the door work itself: the unload is released by the receipt, not by the clerk, and a
// check-out tried before that is refused 409 trailer-not-released. Retrying that refusal would hide a ReceiptClosed
// that never arrived, so the clerk waits on the trailer instead and a timeout says what it saw.
func (c *Crew) Release(ctx context.Context, v Visit) error {
	what := fmt.Sprintf("release trailer %s of visit %s", v.TrailerID, v.ID)
	err := await(ctx, c.clock(), c.Wait, "the unload of trailer "+v.TrailerID+" released by the receipt's close", func() (string, error) {
		r := c.call(ctx, SvcYard, http.MethodGet, "/trailers/{id}", "/trailers/"+v.TrailerID, nil)
		if !r.OK() {
			return "", refusal("read trailer "+v.TrailerID, r)
		}
		var tr trailerDTO
		if err := r.Decode(&tr); err != nil {
			return "", fmt.Errorf("read trailer %s: %w", v.TrailerID, err)
		}
		switch {
		case tr.State == "DEPARTED":
			return "", fmt.Errorf("%s: the trailer already DEPARTED", what)
		case tr.State == trailerDocked && tr.DoorWork == doorWorkDone && tr.LoadState == loadEmpty:
			return "", nil
		}
		return fmt.Sprintf("trailer is %s with door work %s and load %s (ReceiptClosed not consumed by yard-management yet, or the receipt does not cover every ASN of the appointment)",
			tr.State, tr.DoorWork, tr.LoadState), nil
	})
	if err != nil {
		return err
	}
	r := c.call(ctx, SvcYard, http.MethodPost, "/visits/{id}/check-outs", "/visits/"+v.ID+"/check-outs",
		map[string]any{"trailersOut": []map[string]any{{"trailerId": v.TrailerID}}})
	if !r.OK() {
		return refusal("check out visit "+v.ID, r)
	}
	var out struct {
		State       string   `json:"state"`
		TrailersOut []string `json:"trailersOut"`
	}
	if err := r.Decode(&out); err != nil {
		return fmt.Errorf("check out visit %s: %w", v.ID, err)
	}
	if out.State != "CHECKED_OUT" || len(out.TrailersOut) != 1 {
		return fmt.Errorf("check out visit %s: visit is %s with %d trailers out, want CHECKED_OUT with 1 (body: %s)", v.ID, out.State, len(out.TrailersOut), sim.Truncate(string(r.Body), 200))
	}
	c.logf("gate", "visit %s left with trailer %s", v.ID, v.TrailerID)
	return nil
}

// AwaitCompleted waits, by reading, until dock-scheduling shows the appointment COMPLETED: the door release the
// yard recorded reached it over Kafka. An appointment that was cancelled or marked no-show can never complete and
// ends the wait at once.
func (c *Crew) AwaitCompleted(ctx context.Context, a Appointment) error {
	return await(ctx, c.clock(), c.Wait, "dock-scheduling completes appointment "+a.ID, func() (string, error) {
		r := c.call(ctx, SvcDock, http.MethodGet, "/appointments/{id}", "/appointments/"+a.ID, nil)
		if !r.OK() {
			return "", refusal("read appointment "+a.ID, r)
		}
		var got struct {
			State string `json:"state"`
		}
		if err := r.Decode(&got); err != nil {
			return "", fmt.Errorf("read appointment %s: %w", a.ID, err)
		}
		switch got.State {
		case "COMPLETED":
			return "", nil
		case "CANCELLED", "NO_SHOW":
			return "", fmt.Errorf("appointment %s is %s: it will never be COMPLETED", a.ID, got.State)
		}
		return fmt.Sprintf("appointment is %s (the yard's departure was not consumed by dock-scheduling yet)", got.State), nil
	})
}

// ---------------------------------------------------------------------
// the staged handover, seen on the Kafka tap
// ---------------------------------------------------------------------

// stagedRef is the correlation id of a SKU on the tap: the SKU as a quoted JSON string, because the tap matches
// by substring and a bare SKU-001 would also match every event of SKU-0010.
func stagedRef(sku string) string { return `"` + sku + `"` }

// trackStaged registers the SKUs on the tap (once per SKU: EventTap.Track appends, so a second call would count
// every event twice) and returns how many StockReceived events each had already carried: what the wait must see
// exceeded. It returns nil without a tap. Call it BEFORE the lines are received: the tap indexes only later messages.
func (c *Crew) trackStaged(lines []Line) map[string]int {
	if c.Staged == nil {
		return nil
	}
	c.trackMu.Lock()
	defer c.trackMu.Unlock()
	if c.tracked == nil {
		c.tracked = map[string]bool{}
	}
	base := map[string]int{}
	for _, l := range lines {
		ref := stagedRef(l.SKU)
		if !c.tracked[ref] {
			c.Staged.Track(ref)
			c.tracked[ref] = true
		}
		base[l.SKU] = c.Staged.Seen(ref, typeStockReceived)
	}
	return base
}

// awaitStaged waits (bounded) until inventory-storage has staged every line: one more StockReceived for each SKU than
// the baseline. The units are staged, not yet usable, until the stower stows them. A ReceiptLineReceived does not
// count: that is inbound-receiving's side of the handover, StockReceived is inventory-storage's. A no-op without a tap.
func (c *Crew) awaitStaged(ctx context.Context, base map[string]int, lines []Line) error {
	if c.Staged == nil {
		return nil
	}
	return await(ctx, c.clock(), c.Wait, "inventory-storage stages the received units", func() (string, error) {
		var missing []string
		for _, l := range lines {
			if c.Staged.Seen(stagedRef(l.SKU), typeStockReceived) <= base[l.SKU] {
				missing = append(missing, l.SKU)
			}
		}
		if len(missing) == 0 {
			return "", nil
		}
		return fmt.Sprintf("no StockReceived seen for %s since the lines were counted (inventory-storage's INBOUND_RECEIPT_CONSUMER_GROUP unset or the consumer lagging, or the tap is not reading warehouse.inventory.analytics)",
			strings.Join(missing, ", ")), nil
	})
}
