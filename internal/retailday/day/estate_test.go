package day

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
)

// The shelf after the day is read once more; if that read fails the day cannot say the stock was
// conserved, and silence would read as a pass.
func TestAShelfThatCannotBeReadAfterTheDayFailsIt(t *testing.T) {
	g := newRig()
	calls := 0
	g.evidence.onShelf = func(skus []string) (map[string]retailday.Shelf, error) {
		calls++
		if calls > 1 {
			return nil, errors.New("inventory-storage: HTTP 503")
		}
		out := map[string]retailday.Shelf{}
		for _, s := range skus {
			out[s] = retailday.Shelf{Usable: 20, Purchasable: true}
		}
		return out, nil
	}
	rep := g.run(context.Background())
	if rep.Passed() {
		t.Fatalf("a day whose stock could not be checked passed:\n%s", rep.dump())
	}
	if !rep.hasRule("stock/unreadable") {
		t.Errorf("no stock/unreadable finding:\n%s", rep.dump())
	}
	if !containsSub(rep.NotVerified, "conservation of stock was not checked") {
		t.Errorf("not verified: %v", rep.NotVerified)
	}
}

// An order that was labelled consumed its units, even if the dock then failed it: that order fails by
// itself and the stock rule must stay quiet about it (its units are in a trailer, not lost).
func TestAnOrderThatFailedAfterTheFloorDoesNotMakeTheStockLookLost(t *testing.T) {
	g := newRig()
	victim := firstDelivered(g)
	g.shipper.onShip = func(p Package) Shipment {
		if p.OrderID == orderID(victim) {
			return Shipment{PackageID: p.PackageID, Problems: []string{"load 1 not closed: nothing was loaded"}}
		}
		return g.w.ship(p)
	}
	rep := g.run(context.Background())
	if rep.Passed() {
		t.Fatalf("a refused shipment passed:\n%s", rep.dump())
	}
	if rep.hasRule("stock/conserved") {
		t.Errorf("the stock rule blamed the shelf for an order the dock failed:\n%s", rep.dump())
	}
	if row := rep.row(orderID(victim)); row.Outcome != OutcomeFail || !contains(row.Rules, "ship/not-confirmed") {
		t.Errorf("victim row %+v", row)
	}
}

// ...and an order the floor never took did not consume anything.
func TestAnOrderTheFloorNeverTookDoesNotCountAsConsumed(t *testing.T) {
	g := newRig()
	victim := firstDelivered(g)
	g.floor.onProcess = func(_ context.Context, id string) FloorResult {
		if id == omIDOf(orderID(victim)) {
			return FloorResult{OrderID: id, Problems: []string{"order is Held"}} // never picked: the stub leaves its stock alone
		}
		return g.w.label(id)
	}
	rep := g.run(context.Background())
	if rep.Passed() {
		t.Fatalf("an order that never reached the floor passed:\n%s", rep.dump())
	}
	if rep.hasRule("stock/conserved") {
		t.Errorf("units that never left the shelf were counted as consumed:\n%s", rep.dump())
	}
}

// A day with a hole in it is not played: every phase it needs is named, and nothing is bought.
func TestADayMissingAnyPhaseIsNotPlayed(t *testing.T) {
	for name, drop := range map[string]func(p *Phases){
		"Catalogue": func(p *Phases) { p.Catalogue = nil },
		"Shopper":   func(p *Phases) { p.Shopper = nil },
		"Linker":    func(p *Phases) { p.Linker = nil },
		"Floor":     func(p *Phases) { p.Floor = nil },
		"Shipper":   func(p *Phases) { p.Shipper = nil },
		"Carrier":   func(p *Phases) { p.Carrier = nil },
		"Restocker": func(p *Phases) { p.Restocker = nil },
		"Evidence":  func(p *Phases) { p.Evidence = nil },
	} {
		t.Run(name, func(t *testing.T) {
			g := newRig()
			drop(&g.runner.Phases)
			rep := g.run(context.Background())
			if rep.Passed() {
				t.Fatalf("a day without %s passed:\n%s", name, rep.dump())
			}
			if st := rep.phase(PhasePrepare); !strings.Contains(st.Detail, name) {
				t.Errorf("prepare phase %+v: want it to name %s", st, name)
			}
			if len(g.shopper.called()) != 0 {
				t.Errorf("%d buyer(s) shopped on an unwired day", len(g.shopper.called()))
			}
		})
	}
}
