package main

import (
	"context"
	"slices"

	"github.com/claudioed/warehouse-e2e/internal/retailday/day"
	"github.com/claudioed/warehouse-e2e/internal/sim/inbound"
)

// The Restocker phase: inbound.Crew.Restock, the shelf back to the stock the day started with.

// crew is the one method of *inbound.Crew the adapter uses.
type crew interface {
	Restock(ctx context.Context, start inbound.Stock, opt inbound.RestockOptions) inbound.Result
}

// restockAdapter satisfies day.Restocker. The restock crew needs the doors and bins the readiness
// step found, so it is built in Prepare: until then get returns nil and nothing is restocked.
type restockAdapter struct{ get func() crew }

var _ day.Restocker = restockAdapter{}

func (a restockAdapter) Restock(ctx context.Context, start map[string]int) (day.RestockResult, error) {
	var c crew
	if a.get != nil {
		c = a.get()
	}
	if c == nil {
		return day.RestockResult{}, errNotReadied("restock crew")
	}
	res := c.Restock(ctx, inbound.Stock(start), inbound.RestockOptions{})
	if err := ctx.Err(); err != nil {
		return toRestockResult(res), err
	}
	return toRestockResult(res), nil
}

// toRestockResult: Restocked is what the trucks received AND stowed (a unit that is staged but not
// stowed is not usable and does not count), Short is what still ends below its start, and Problems
// are the findings of the restock and of every truck.
func toRestockResult(r inbound.Result) day.RestockResult {
	out := day.RestockResult{Restocked: map[string]int{}, Problems: slices.Clone(r.Findings())}
	for _, d := range r.Deliveries {
		for _, s := range d.Stowed {
			out.Restocked[s.SKU] += s.Qty
		}
	}
	if len(r.Short) > 0 {
		out.Short = make(map[string]int, len(r.Short))
		for sku, n := range r.Short {
			out.Short[sku] = n
		}
	}
	return out
}
