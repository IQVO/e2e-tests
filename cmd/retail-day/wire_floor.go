package main

import (
	"context"
	"slices"

	"github.com/claudioed/warehouse-e2e/internal/retailday/day"
	"github.com/claudioed/warehouse-e2e/internal/sim/floor"
)

// The Floor phase: floor.Floor.ProcessAll, order-management ids in, one result per id out.

// processor is the one method of *floor.Floor the adapter uses.
type processor interface {
	ProcessAll(ctx context.Context, ids []string, parallel int) []floor.Result
}

type floorAdapter struct{ p processor }

var _ day.Floor = floorAdapter{}

// ProcessAll keeps the actor's order and length: the runner matches result i to id i.
func (a floorAdapter) ProcessAll(ctx context.Context, ids []string, parallel int) []day.FloorResult {
	rs := a.p.ProcessAll(ctx, ids, parallel)
	out := make([]day.FloorResult, len(rs))
	for i, r := range rs {
		out[i] = toFloorResult(r)
	}
	return out
}

// toFloorResult copies what became of one order on the floor. Status is passed through as the floor
// reported it (LABELED, DIVERTED, SEALED...): a diverted package must never read as labelled.
func toFloorResult(r floor.Result) day.FloorResult {
	return day.FloorResult{
		OrderID:       r.OrderID,
		PackageID:     r.PackageID,
		Status:        r.Status,
		OrderRef:      r.OrderRef,
		SourceOrderID: r.SourceOrderID,
		SiteID:        r.SiteID,
		CPT:           r.CPT,
		WeightG:       r.WeightG,
		Problems:      slices.Clone(r.Problems),
	}
}
