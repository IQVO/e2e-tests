package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
	"github.com/claudioed/warehouse-e2e/internal/retailday/day"
	"github.com/claudioed/warehouse-e2e/internal/sim"
)

// A stub day: every phase interface answered from one small WORLD, so a day that nothing breaks is
// judged PASSED by the real runner and the real audit, and a break injected in one phase shows up
// in the verdict. It is the stand-in for the cluster in main_test.go; nothing here talks to a network.

var stubEpoch = time.Date(2026, 11, 14, 19, 0, 0, 0, time.UTC)

type stubStage int

const (
	stageNone stubStage = iota
	stagePlaced
	stageCancelled
	stageLabeled
	stageShipped
	stageDelivered
)

type stubWorld struct {
	mu     sync.Mutex
	stock  map[string]int
	buyers map[string]day.Buyer // by customer order id
	stage  map[string]stubStage

	// breakage knobs
	skipCapture bool // the evidence reports no capture for delivered orders
	deadLetters func() (int, bool, error)

	preparedN, cleanedN int
	floorParallel       int
}

func newStubWorld(items []day.Item, usable int) *stubWorld {
	w := &stubWorld{stock: map[string]int{}, buyers: map[string]day.Buyer{}, stage: map[string]stubStage{}}
	for _, it := range items {
		w.stock[it.SKU] = usable
	}
	return w
}

func stubItems(n int) []day.Item {
	out := make([]day.Item, n)
	for i := range out {
		out[i] = day.Item{CatalogItemID: fmt.Sprintf("cat-%02d", i+1), SKU: fmt.Sprintf("SKU-%02d", i+1)}
	}
	return out
}

func orderOf(b day.Buyer) string { return fmt.Sprintf("ORD-%02d", b.Index) }
func omOf(order string) string   { return "OM-" + order }

type stubCatalogue struct{ items []day.Item }

func (c stubCatalogue) Items(context.Context) ([]day.Item, error) { return c.items, nil }

type stubShopper struct{ w *stubWorld }

func (s stubShopper) Buy(_ context.Context, b day.Buyer) (day.Bought, error) {
	s.w.mu.Lock()
	defer s.w.mu.Unlock()
	id := orderOf(b)
	switch b.Expect {
	case retailday.Delivered:
		s.w.buyers[id], s.w.stage[id] = b, stagePlaced
		return day.Bought{Status: day.StatusPlaced, OrderID: id, TotalMinor: 5000, AuthorizationID: "auth-" + id, PromiseEnd: stubEpoch.Add(30 * time.Hour), OrderStatus: "PLACED"}, nil
	case retailday.Cancelled:
		s.w.buyers[id], s.w.stage[id] = b, stageCancelled
		return day.Bought{Status: day.StatusCancelled, OrderID: id, TotalMinor: 5000, AuthorizationID: "auth-" + id, OrderStatus: "CANCELLED", CancelSettled: true}, nil
	case retailday.Declined:
		return day.Bought{Status: day.StatusDeclined}, nil
	case retailday.NotServed:
		return day.Bought{Status: day.StatusNotServed}, nil
	default:
		return day.Bought{Status: day.StatusInDoubt, Problems: []string{"outcome in doubt: session still AUTHORIZING"}}, nil
	}
}

type stubLinker struct{}

func (stubLinker) OrderIDs(_ context.Context, p retailday.Placed) ([]string, error) {
	return []string{omOf(p.OrderID)}, nil
}

type stubFloor struct{ w *stubWorld }

func (f stubFloor) ProcessAll(_ context.Context, ids []string, parallel int) []day.FloorResult {
	f.w.mu.Lock()
	defer f.w.mu.Unlock()
	f.w.floorParallel = parallel
	out := make([]day.FloorResult, len(ids))
	for i, omID := range ids {
		id := omID[len("OM-"):]
		b := f.w.buyers[id]
		for sku, n := range b.Placed.SKUs {
			f.w.stock[sku] -= n
		}
		f.w.stage[id] = stageLabeled
		out[i] = day.FloorResult{OrderID: omID, PackageID: "PKG-" + id, Status: "LABELED", OrderRef: omID, SourceOrderID: omID, SiteID: "AUS3", CPT: "2026-11-14T23:00:00Z"}
	}
	return out
}

type stubShipper struct{ w *stubWorld }

func (s stubShipper) ShipAll(_ context.Context, pkgs []day.Package) []day.Shipment {
	s.w.mu.Lock()
	defer s.w.mu.Unlock()
	out := make([]day.Shipment, len(pkgs))
	for i, p := range pkgs {
		s.w.stage[p.OrderID] = stageShipped
		out[i] = day.Shipment{PackageID: p.PackageID, OrderRef: p.OrderRef, LoadID: "LOAD-1", Departed: true, Confirmed: true}
	}
	return out
}

type stubCarrier struct{ w *stubWorld }

func (c stubCarrier) DeliverOrder(_ context.Context, orderID string, _ int) day.Delivery {
	c.w.mu.Lock()
	defer c.w.mu.Unlock()
	c.w.stage[orderID] = stageDelivered
	return day.Delivery{DeliveredAt: stubEpoch.Add(2 * time.Hour)}
}

type stubRestocker struct{ w *stubWorld }

func (r stubRestocker) Restock(_ context.Context, start map[string]int) (day.RestockResult, error) {
	r.w.mu.Lock()
	defer r.w.mu.Unlock()
	res := day.RestockResult{Restocked: map[string]int{}}
	for sku, want := range start {
		if short := want - r.w.stock[sku]; short > 0 {
			res.Restocked[sku] = short
			r.w.stock[sku] = want
		}
	}
	return res, nil
}

type stubEvidence struct{ w *stubWorld }

func (e stubEvidence) Order(_ context.Context, p retailday.Placed) (retailday.OrderFacts, []string) {
	e.w.mu.Lock()
	defer e.w.mu.Unlock()
	f := retailday.OrderFacts{OrderID: p.OrderID, Expect: p.Expect, Lines: p.Lines()}
	if p.OrderID == "" {
		return f, nil
	}
	st := e.w.stage[p.OrderID]
	f.TotalMinor, f.AuthorizedMinor, f.Status = 5000, 5000, "PLACED"
	switch {
	case st == stageCancelled:
		f.Status, f.VoidedMinor = "CANCELLED", 5000
	case st >= stageLabeled:
		f.POs = []retailday.PO{{Number: "PO-" + p.OrderID, State: "SHIPPED"}}
		f.OMOrders = []retailday.OMOrder{{ID: omOf(p.OrderID), Status: "Released"}}
		f.WorkUnits, f.PickTasksDone, f.PackTasks = p.Lines(), p.Lines(), 1
		f.Packages = []retailday.Package{{ID: "PKG-" + p.OrderID, Outcome: "LABELED"}}
		f.Status = "IN_FULFILLMENT"
		if st >= stageShipped {
			f.OMOrders[0].Status, f.ShipmentConfirmations, f.TrackingNumbers, f.Status = "Shipped", 1, []string{"TN-" + p.OrderID}, "SHIPPED"
			if !e.w.skipCapture {
				f.CapturedMinor = 5000
			}
		}
		if st >= stageDelivered {
			f.Status, f.DeliveredAt, f.PromiseEnd = "DELIVERED", stubEpoch.Add(2*time.Hour), stubEpoch.Add(30*time.Hour)
		}
	}
	return f, nil
}

func (e stubEvidence) Shelf(_ context.Context, skus []string) (map[string]retailday.Shelf, error) {
	e.w.mu.Lock()
	defer e.w.mu.Unlock()
	out := map[string]retailday.Shelf{}
	for _, sku := range skus {
		out[sku] = retailday.Shelf{Usable: e.w.stock[sku], Purchasable: e.w.stock[sku] > 0}
	}
	return out, nil
}

func (e stubEvidence) DeadLetters(context.Context, string) (int, bool, error) {
	if e.w.deadLetters != nil {
		return e.w.deadLetters()
	}
	return 0, true, nil
}

type stubCalls struct{}

func (stubCalls) Count5xx() int      { return 0 }
func (stubCalls) Failures() []string { return nil }
func (stubCalls) Lines() []sim.RouteLine {
	return []sim.RouteLine{{Route: "storefront POST /checkout", Calls: 3, Statuses: map[int]int{200: 3}}}
}

// stubPhases is the whole day over one world.
func stubPhases(w *stubWorld, items []day.Item) day.Phases {
	return day.Phases{
		Catalogue: stubCatalogue{items}, Shopper: stubShopper{w}, Linker: stubLinker{}, Floor: stubFloor{w},
		Shipper: stubShipper{w}, Carrier: stubCarrier{w}, Restocker: stubRestocker{w}, Evidence: stubEvidence{w},
		Calls:   stubCalls{},
		Prepare: func(context.Context) error { w.mu.Lock(); w.preparedN++; w.mu.Unlock(); return nil },
		Cleanup: func(context.Context) error { w.mu.Lock(); w.cleanedN++; w.mu.Unlock(); return nil },
	}
}
