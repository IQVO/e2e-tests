package inbound

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sort"
)

// Stock is usable inventory per SKU, in eaches: what inventory-storage's GET /inventory/{sku}/usable answers
// (on hand, minus reservations, minus held stock). Staged stock is not usable until it is stowed.
type Stock map[string]int

// ReadStock reads the usable units of every SKU (GET /inventory/{sku}/usable, no owner: fleet-owned stock). A SKU
// inventory-storage does not know reads as 0. A refusal or a transport error stops the read and names the SKU.
func (c *Crew) ReadStock(ctx context.Context, skus []string) (Stock, error) {
	out := Stock{}
	for _, sku := range skus {
		what := "read the usable stock of " + sku
		r := c.call(ctx, SvcInventory, http.MethodGet, "/inventory/{sku}/usable", "/inventory/"+url.PathEscape(sku)+"/usable", nil)
		if !r.OK() {
			return out, refusal(what, r)
		}
		var got struct {
			Usable int `json:"usable"`
		}
		if err := r.Decode(&got); err != nil {
			return out, fmt.Errorf("%s: %w", what, err)
		}
		out[sku] = got.Usable
	}
	return out, nil
}

// Deficit is what must come back to reach the start: for every SKU whose current usable stock is below its start,
// the difference, sorted by SKU. A SKU at or above its start is not listed; one missing from now reads as 0.
func Deficit(start, now Stock) []Line {
	var out []Line
	for sku, want := range start {
		if short := want - now[sku]; short > 0 {
			out = append(out, Line{SKU: sku, Qty: short})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SKU < out[j].SKU })
	return out
}

// RestockOptions tune Restock.
type RestockOptions struct {
	// LinesPerASN caps the lines of one truck; a larger shortfall goes in several trucks, one after the other
	// (default 25).
	LinesPerASN int
}

func (o RestockOptions) linesPerASN() int {
	if o.LinesPerASN > 0 {
		return o.LinesPerASN
	}
	return 25
}

// Result is what a restock did. Start is the stock the day began with, Before what was usable when the restock
// began, After what is usable now; Lines are the shortfall that was ordered and Deliveries the trucks that brought
// it. Short lists every SKU that still ends below its start, by how many units. Problems are the findings of the
// restock itself (a stock that could not be read); every delivery has its own.
type Result struct {
	Start, Before, After Stock
	Lines                []Line
	Deliveries           []Delivery
	Short                map[string]int
	Problems             []string
}

// OK reports a restock that ended every SKU at or above its start with no finding anywhere.
func (r Result) OK() bool {
	if len(r.Problems) > 0 || len(r.Short) > 0 {
		return false
	}
	for _, d := range r.Deliveries {
		if !d.OK() {
			return false
		}
	}
	return true
}

// Findings is every finding of the restock and of its deliveries, in order.
func (r Result) Findings() []string {
	out := append([]string(nil), r.Problems...)
	for _, d := range r.Deliveries {
		for _, p := range d.Problems {
			out = append(out, fmt.Sprintf("delivery %s: %s", d.ASN, p))
		}
	}
	return out
}

func (r *Result) problem(format string, args ...any) {
	r.Problems = append(r.Problems, fmt.Sprintf(format, args...))
}

// Restock ends the day at or above the stock it started with (ADR 0001 D4): it reads what is usable now, orders
// the shortfall against start through ASN, dock appointment, gate, receipt and stow (Deliver), and reads the stock
// again. A SKU that still ends below its start is a finding (Result.Short), not an error: reservations made by orders
// still in flight lower the usable figure, so run it once the day's orders have shipped.
//
// It is idempotent: when nothing is short it sends no POST or PUT, so a second run against the stock it left adds
// nothing. Trucks go one after the other (each waits for its door to be released before the next books one).
// A truck that fails leaves the units it did receive staged or stowed and the shortfall is read again at the end,
// so a rerun orders only what is still missing.
func (c *Crew) Restock(ctx context.Context, start Stock, opt RestockOptions) Result {
	res := Result{Start: start}
	skus := make([]string, 0, len(start))
	for sku := range start {
		skus = append(skus, sku)
	}
	sort.Strings(skus)

	before, err := c.ReadStock(ctx, skus)
	res.Before = before
	if err != nil {
		res.problem("restock: %v", err)
		return res
	}
	res.Lines = Deficit(start, before)
	if len(res.Lines) == 0 {
		res.After = before
		return res
	}
	c.logf("restock", "ordering %d SKUs back to their start: %s", len(res.Lines), describeLines(res.Lines))

	per := opt.linesPerASN()
	for i := 0; i < len(res.Lines); i += per {
		res.Deliveries = append(res.Deliveries, c.Deliver(ctx, res.Lines[i:min(i+per, len(res.Lines))]))
	}

	after, err := c.ReadStock(ctx, skus)
	res.After = after
	if err != nil {
		res.problem("restock: %v", err)
		return res
	}
	for _, sku := range skus {
		if after[sku] < start[sku] {
			if res.Short == nil {
				res.Short = map[string]int{}
			}
			res.Short[sku] = start[sku] - after[sku]
			res.problem("%s ends at %d usable units, below the %d it started with", sku, after[sku], start[sku])
		}
	}
	return res
}
