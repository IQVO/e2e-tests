package main

import (
	"context"
	"slices"

	"github.com/claudioed/warehouse-e2e/internal/retailday/day"
	"github.com/claudioed/warehouse-e2e/internal/sim/outbound"
)

// The Shipper phase: outbound.Floor.ShipAll, one call per load.

// dock is the one method of *outbound.Floor the adapter uses.
type dock interface {
	ShipAll(ctx context.Context, pkgs []outbound.Package) []outbound.Shipment
}

// shipperAdapter satisfies day.Shipper. The outbound crew needs the doors and staging lanes the
// readiness step found, so it is built in Prepare: until then get returns nil and nothing is shipped.
type shipperAdapter struct{ get func() dock }

var _ day.Shipper = shipperAdapter{}

// ShipAll returns one shipment per package, in order, whatever happens.
func (a shipperAdapter) ShipAll(ctx context.Context, pkgs []day.Package) []day.Shipment {
	var d dock
	if a.get != nil {
		d = a.get()
	}
	if d == nil {
		out := make([]day.Shipment, len(pkgs))
		for i, p := range pkgs {
			out[i] = day.Shipment{PackageID: p.PackageID, OrderRef: p.OrderRef, Problems: []string{"the dock crew was never readied: the prepare phase did not complete"}}
		}
		return out
	}
	in := make([]outbound.Package, len(pkgs))
	for i, p := range pkgs {
		in[i] = toOutboundPackage(p)
	}
	res := d.ShipAll(ctx, in)
	out := make([]day.Shipment, len(res))
	for i, s := range res {
		out[i] = toShipment(s)
	}
	return out
}

// toOutboundPackage carries the facts of PackageManifested the dock crew finds the package by.
func toOutboundPackage(p day.Package) outbound.Package {
	return outbound.Package{
		ID:            p.PackageID,
		OrderRef:      p.OrderRef,
		SourceOrderID: p.SourceOrderID,
		SiteID:        p.SiteID,
		CPT:           p.CPT,
		WeightG:       p.WeightG,
	}
}

// toShipment: Confirmed is outbound-shipping's own SHIP_CONFIRMED (the departure was consumed),
// not "the truck left": a trailer that departs a load that is not CLOSED raises PackageMissedLoad.
func toShipment(s outbound.Shipment) day.Shipment {
	return day.Shipment{
		PackageID: s.PackageID,
		OrderRef:  s.OrderRef,
		LoadID:    s.LoadID,
		Departed:  s.Departed,
		Confirmed: s.LoadState == outbound.LoadShipConfirmed,
		Problems:  slices.Clone(s.Problems),
	}
}
