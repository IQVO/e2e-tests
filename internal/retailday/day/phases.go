package day

import (
	"context"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
	"github.com/claudioed/warehouse-e2e/internal/sim"
)

// The phase interfaces: one per actor family, each shaped like the real actor's method so the
// real type satisfies it through a thin adapter (cmd/retail-day/wire.go). The runner knows
// nothing else about an actor, which is what lets every phase be a stub in the tests.

// Catalogue lists the purchasable products with the SKU behind each: the day's candidate shelf.
type Catalogue interface {
	Items(ctx context.Context) ([]Item, error)
}

// Status is how one purchase ended, in the shopper actor's words (internal/sim/shopper.Status).
type Status string

// The ways a purchase ends.
const (
	StatusPlaced           Status = "placed"
	StatusDeclined         Status = "declined"
	StatusInDoubt          Status = "in-doubt"
	StatusRequoteExhausted Status = "requote-exhausted"
	StatusRefused          Status = "refused"
	StatusCancelled        Status = "cancelled"
	StatusNotServed        Status = "not-served"
)

// Bought is what one shopper learned.
type Bought struct {
	Status          Status
	OrderID         string // customer-ordering's id; empty when nothing was placed
	TotalMinor      int64
	AuthorizationID string
	PromiseEnd      time.Time // the latest delivery instant the order promised; zero = unknown
	OrderStatus     string
	CancelSettled   bool
	CancelRefused   string // the problem slug of a refused cancellation (cancel-after-lock)
	// Problems are the refusals and contract disagreements met on the way, retried ones included.
	Problems []string
}

// Shopper plays one buyer's purchase end to end. An error is a setup bug or the end of ctx;
// everything the storefront refused is in Bought.Problems.
type Shopper interface {
	Buy(ctx context.Context, b Buyer) (Bought, error)
}

// Linker follows a placed customer order to the order-management order raised for its purchase
// order. The floor, the dock and inventory work on THAT id: the storefront's order id is another
// context's and no floor route knows it. No published route links the two ids by itself (the
// link is network-fulfillment's PO -> local order mapping), so this is a phase of its own: it
// waits, bounded, for the purchase order to be released, acknowledged and raised, and returns
// every order id found (more than one is a defect the runner reports).
type Linker interface {
	OrderIDs(ctx context.Context, p retailday.Placed) ([]string, error)
}

// Floor packages and labels. ProcessAll is floor.Floor.ProcessAll: it takes ORDER-MANAGEMENT ids (Linker's),
// waits by itself for order-management to release the orders and returns one result per id, in order.
type Floor interface {
	ProcessAll(ctx context.Context, orderIDs []string, parallel int) []FloorResult
}

// FloorResult is what became of one order on the floor.
type FloorResult struct {
	OrderID       string
	PackageID     string
	Status        string // LABELED, DIVERTED, SEALED, ...
	OrderRef      string
	SourceOrderID string
	SiteID        string
	CPT           string
	WeightG       int
	Problems      []string
}

// Labeled reports a package SLAM labelled.
func (r FloorResult) Labeled() bool { return r.PackageID != "" && r.Status == "LABELED" }

// Package is a labelled package ready to leave the building: the facts of PackageManifested the
// outbound crew needs (outbound.Package), and the order it belongs to.
type Package struct {
	OrderID       string
	PackageID     string
	OrderRef      string
	SourceOrderID string
	SiteID        string
	CPT           string
	WeightG       int
}

// Shipment is what became of one package at the dock and the gate.
type Shipment struct {
	PackageID string
	OrderRef  string
	LoadID    string
	Departed  bool
	// Confirmed is outbound-shipping's SHIP_CONFIRMED: the departure was consumed.
	Confirmed bool
	Problems  []string
}

// Shipper stages, books, gates in, loads, closes and departs. ShipAll is outbound.Floor.ShipAll:
// packages that share a load must be handed over in ONE call, because they share a door, a
// trailer and a departure and two concurrent calls on one load would book it twice. It returns
// one shipment per package, in order.
type Shipper interface {
	ShipAll(ctx context.Context, pkgs []Package) []Shipment
}

// Delivery is what the network's transport legs did with one shipped order.
type Delivery struct {
	DeliveredAt time.Time // zero unless every package was delivered
	Problems    []string
}

// Delivered reports a parcel whose every package was delivered.
func (d Delivery) Delivered() bool { return !d.DeliveredAt.IsZero() }

// Carrier takes a shipped order to the doorstep: transport.Transport.DeliverOrder with the
// origin and the delivery station the adapter knows.
type Carrier interface {
	DeliverOrder(ctx context.Context, orderID string, packages int) Delivery
}

// RestockResult is what the restock leg did.
type RestockResult struct {
	// Restocked is the units of each SKU the leg received and stowed.
	Restocked map[string]int
	// Short is the SKUs that still end below their start, by how many units.
	Short    map[string]int
	Problems []string
}

// Restocker orders the shelf back to its start level through ASN, dock, gate, receipt and stow
// (inbound.Crew.Restock). start is the usable stock the day began with.
type Restocker interface {
	Restock(ctx context.Context, start map[string]int) (RestockResult, error)
}

// Calls is what the day knows of its own HTTP traffic: *sim.CallStats.
type Calls interface {
	Count5xx() int
	Failures() []string
	Lines() []sim.RouteLine
}

// Suspensions reports host sleeps: *sim.SuspendWatch.
type Suspensions interface {
	Snapshot() []sim.Suspension
}

// Phases is every actor family the day needs plus the evidence it is judged by. The two hooks
// are optional: Prepare readies the building before anything is bought (floor stations, dock and
// yard readiness), Cleanup undoes what Prepare did and always runs, on a context that outlives
// the run's.
type Phases struct {
	Catalogue Catalogue
	Shopper   Shopper
	Linker    Linker
	Floor     Floor
	Shipper   Shipper
	Carrier   Carrier
	Restocker Restocker
	Evidence  retailday.Evidence

	Calls       Calls
	Suspensions Suspensions

	Prepare func(ctx context.Context) error
	Cleanup func(ctx context.Context) error
}
