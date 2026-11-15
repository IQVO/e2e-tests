package day

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
	"github.com/claudioed/warehouse-e2e/internal/sim"
)

func TestTheHappyDayPassesAndEveryOrderIsAccountedFor(t *testing.T) {
	g := newRig()
	rep := g.run(context.Background())
	if !rep.Passed() || rep.Verdict != VerdictPassed {
		t.Fatalf("the happy day did not pass: %v\n%s", rep.Reasons, rep.dump())
	}
	if rep.Planned != 12 || len(rep.Orders) != 12 {
		t.Fatalf("%d planned, %d rows", rep.Planned, len(rep.Orders))
	}
	for _, o := range rep.Orders {
		want := OutcomePass
		if o.Expect == retailday.InDoubt {
			want = OutcomeWarn // tok_timeout is recorded, never green (ADR 0001 D6)
		}
		if o.Outcome != want || len(o.Rules) != 0 {
			t.Errorf("%s (%s): %s rules %v, want %s", o.Order, o.Expect, o.Outcome, o.Rules, want)
		}
	}
	for _, p := range rep.Phases {
		if p.State != PhaseOK && p.State != PhaseIdle {
			t.Errorf("phase %s is %s", p.Name, p.State)
		}
	}
	if len(rep.Phases) != len(requiredPhases) {
		t.Errorf("%d phases recorded, want %d: %+v", len(rep.Phases), len(requiredPhases), rep.Phases)
	}
	for _, s := range rep.Stock {
		if s.After != s.Before {
			t.Errorf("%s ends at %d, started at %d", s.SKU, s.After, s.Before)
		}
	}
	if len(rep.Stock) != 8 {
		t.Errorf("%d stock rows, want the 8 shelf SKUs", len(rep.Stock))
	}
	if g.prepared != 1 || g.cleaned != 1 {
		t.Errorf("prepare %d, cleanup %d: each runs once", g.prepared, g.cleaned)
	}
}

// What each actor was asked is part of the contract: the floor, the dock and the carrier see only
// the orders that were placed and expected delivered, nothing for the declined, the cancelled
// and the not-served; the evidence judges every buyer.
func TestEachPhaseSeesExactlyTheOrdersItShouldAndEveryBuyerIsJudged(t *testing.T) {
	g := newRig()
	rep := g.run(context.Background())
	var delivered []string
	for _, b := range g.plan().Buyers {
		if b.Expect == retailday.Delivered {
			delivered = append(delivered, orderID(b))
		}
	}
	sort.Strings(delivered)
	if got := g.floor.ids(); !reflect.DeepEqual(got, delivered) {
		t.Errorf("the floor processed %v, want %v", got, delivered)
	}
	if got := g.carrier.delivered(); !reflect.DeepEqual(got, delivered) {
		t.Errorf("the carrier delivered %v, want %v", got, delivered)
	}
	var shipped []string
	for _, batch := range g.shipper.batches() {
		for _, p := range batch {
			shipped = append(shipped, p.OrderID)
		}
	}
	sort.Strings(shipped)
	if !reflect.DeepEqual(shipped, delivered) {
		t.Errorf("the dock shipped %v, want %v", shipped, delivered)
	}
	for _, n := range g.carrier.pkgs {
		if n != 1 {
			t.Errorf("a parcel of %d packages, the floor makes one per order", n)
		}
	}
	if len(g.restocker.calls) != 1 {
		t.Fatalf("restock called %d times", len(g.restocker.calls))
	}
	for sku, n := range g.restocker.calls[0] {
		if n != 20 {
			t.Errorf("restock start for %s is %d, the shelf began at 20", sku, n)
		}
	}
	if len(g.restocker.calls[0]) != 8 {
		t.Errorf("restock start covers %d SKUs, want the shelf's 8", len(g.restocker.calls[0]))
	}

	// the evidence saw all 12 buyers with the plan's expectation and run id
	seen := g.evidence.seen()
	if len(seen) != 12 {
		t.Fatalf("the evidence was asked about %d buyers, want 12", len(seen))
	}
	byExpect := map[retailday.Outcome]int{}
	for _, p := range seen {
		byExpect[p.Expect]++
		if p.RunID != "RD1" || len(p.SKUs) == 0 || p.PlacedAt.IsZero() {
			t.Errorf("a Placed lacks its run id, SKUs or placement time: %+v", p)
		}
	}
	if byExpect[retailday.Declined] != 1 || byExpect[retailday.NotServed] != 1 || byExpect[retailday.Cancelled] != 1 || byExpect[retailday.InDoubt] != 1 || byExpect[retailday.Delivered] != 8 {
		t.Errorf("the evidence saw %v", byExpect)
	}
	for _, p := range seen {
		switch p.Expect {
		case retailday.Declined, retailday.NotServed, retailday.InDoubt:
			if p.OrderID != "" {
				t.Errorf("a %s buyer has an order id %q", p.Expect, p.OrderID)
			}
		default:
			if p.OrderID == "" || p.TotalMinor != 5000 || p.AuthorizationID == "" {
				t.Errorf("a %s order lacks what the shopper answered: %+v", p.Expect, p)
			}
		}
	}
	// the rows carry the plan's expectation, not the evidence's
	for _, o := range rep.Orders {
		if o.Expect != g.plan().Buyers[o.Buyer-1].Expect {
			t.Errorf("row %s expects %s, the plan said %s", o.Order, o.Expect, g.plan().Buyers[o.Buyer-1].Expect)
		}
	}
}

// ---- each break makes the verdict FAIL, with the right rule on the right order ----

func firstDelivered(g *rig) Buyer { return g.buyer(retailday.Delivered, 0) }

func TestAnOrderThatNeverReachesTheFloorFailsThatOrderOnly(t *testing.T) {
	g := newRig()
	victim := firstDelivered(g)
	g.floor.onProcess = func(ctx context.Context, id string) FloorResult {
		if id == orderID(victim) {
			return FloorResult{OrderID: id, Problems: []string{"order " + id + ": wait for its lines to be Released: the order is Held (the release was not made, or not consumed)"}}
		}
		return g.w.label(id)
	}
	rep := g.run(context.Background())
	if rep.Passed() {
		t.Fatalf("an order that never reached the floor passed:\n%s", rep.dump())
	}
	row := rep.row(orderID(victim))
	if row.Outcome != OutcomeFail || !contains(row.Rules, "phase/floor") || !contains(row.Rules, "floor/not-labeled") {
		t.Errorf("victim row: %+v", row)
	}
	for _, b := range g.plan().Buyers {
		if b.Expect == retailday.Delivered && b.Index != victim.Index && rep.row(orderID(b)).Outcome != OutcomePass {
			t.Errorf("another order was dragged down: %+v", rep.row(orderID(b)))
		}
	}
	for _, id := range g.carrier.delivered() {
		if id == orderID(victim) {
			t.Error("the carrier was asked to deliver an order with no package")
		}
	}
	for _, batch := range g.shipper.batches() {
		for _, p := range batch {
			if p.OrderID == orderID(victim) {
				t.Error("the dock was asked to ship an order with no package")
			}
		}
	}
	if !rep.hasRule("stock/conserved") && rep.phase(PhaseFloor).State != PhaseOK {
		t.Errorf("floor phase %+v", rep.phase(PhaseFloor))
	}
}

// The runner's own bookkeeping must fail the order even when the evidence says all is well: a
// verdict that rests only on the collector would pass a phase that failed.
func TestAPhaseFailureAloneFailsTheOrderWhateverTheEvidenceSays(t *testing.T) {
	victim := func(g *rig) string { return orderID(firstDelivered(g)) }
	buyerIdx := func(g *rig) int { return firstDelivered(g).Index }
	cases := []struct {
		name string
		arm  func(g *rig, id string)
		rule string
	}{
		{"the floor reports a problem and no package", func(g *rig, id string) {
			g.floor.onProcess = func(_ context.Context, i string) FloorResult {
				if i == id {
					return FloorResult{OrderID: i, Problems: []string{"timed out"}}
				}
				return g.w.label(i)
			}
		}, "floor/not-labeled"},
		{"the package is DIVERTED", func(g *rig, id string) {
			g.floor.onProcess = func(_ context.Context, i string) FloorResult {
				r := g.w.label(i)
				if i == id {
					r.Status = "DIVERTED"
				}
				return r
			}
		}, "floor/diverted"},
		{"the dock refuses", func(g *rig, id string) {
			g.shipper.onShip = func(p Package) Shipment {
				if p.OrderID == id {
					return Shipment{PackageID: p.PackageID, Problems: []string{"load 1 not closed and not departed: 0 of 1 packages were loaded"}}
				}
				return g.w.ship(p)
			}
		}, "ship/not-confirmed"},
		{"the trailer left but the departure was never consumed", func(g *rig, id string) {
			g.shipper.onShip = func(p Package) Shipment {
				if p.OrderID == id {
					return Shipment{PackageID: p.PackageID, Departed: true, Confirmed: false}
				}
				return g.w.ship(p)
			}
		}, "ship/not-confirmed"},
		{"the carrier never delivers", func(g *rig, id string) {
			g.carrier.onDeliver = func(i string) Delivery {
				if i == id {
					return Delivery{Problems: []string{"the load was never inducted"}}
				}
				return g.w.deliver(i)
			}
		}, "carrier/not-delivered"},
		{"the shopper ends declined on a paying order", func(g *rig, id string) {
			g.shopper.onBuy = func(_ context.Context, b Buyer) (Bought, error) {
				if orderID(b) == id {
					return Bought{Status: StatusDeclined}, nil
				}
				return g.w.buy(b), nil
			}
		}, "shopper/outcome"},
		{"the shopper placed it but returned no id", func(g *rig, id string) {
			g.shopper.onBuy = func(_ context.Context, b Buyer) (Bought, error) {
				if orderID(b) == id {
					return Bought{Status: StatusPlaced}, nil
				}
				return g.w.buy(b), nil
			}
		}, "shopper/order-id"},
		{"the shopper met refusals on the way", func(g *rig, id string) {
			g.shopper.onBuy = func(_ context.Context, b Buyer) (Bought, error) {
				r := g.w.buy(b)
				if orderID(b) == id {
					r.Problems = []string{"POST /carts/{id}/lines 503 unavailable"}
				}
				return r, nil
			}
		}, "shopper/problems"},
		{"the shopper cannot run", func(g *rig, id string) {
			g.shopper.onBuy = func(_ context.Context, b Buyer) (Bought, error) {
				if orderID(b) == id {
					return Bought{}, errors.New("no storefront base URL")
				}
				return g.w.buy(b), nil
			}
		}, "phase/shopper"},
		{"an actor panics", func(g *rig, id string) {
			g.carrier.onDeliver = func(i string) Delivery {
				if i == id {
					panic("nil map")
				}
				return g.w.deliver(i)
			}
		}, "phase/carrier"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			g := newRig()
			// the evidence always says: delivered, fine. Only the runner's own findings can fail.
			g.evidence.onOrder = func(p retailday.Placed) (retailday.OrderFacts, []string) {
				f := happyFacts(p)
				return f, nil
			}
			g.evidence.onShelf = func(skus []string) (map[string]retailday.Shelf, error) {
				out := map[string]retailday.Shelf{}
				for _, s := range skus {
					out[s] = retailday.Shelf{Usable: 20, Purchasable: true}
				}
				return out, nil
			}
			id := victim(g)
			c.arm(g, id)
			rep := g.run(context.Background())
			if rep.Passed() {
				t.Fatalf("a day with %q passed:\n%s", c.name, rep.dump())
			}
			row := rep.rowOf(buyerIdx(g))
			if row.Outcome != OutcomeFail || !contains(row.Rules, c.rule) {
				t.Errorf("row %s: %s rules %v, want %s among them", id, row.Outcome, row.Rules, c.rule)
			}
		})
	}
}

// happyFacts is the evidence of an order that went perfectly, whatever the stubs did.
func happyFacts(p retailday.Placed) retailday.OrderFacts {
	f := retailday.OrderFacts{OrderID: p.OrderID, Expect: p.Expect, Lines: p.Lines()}
	switch p.Expect {
	case retailday.Delivered:
		f.TotalMinor, f.AuthorizedMinor, f.CapturedMinor = 5000, 5000, 5000
		f.POs = []retailday.PO{{Number: "PO", State: "SHIPPED"}}
		f.OMOrders = []retailday.OMOrder{{ID: "OM", Status: "Shipped"}}
		f.WorkUnits, f.PickTasksDone, f.PackTasks = p.Lines(), p.Lines(), 1
		f.Packages = []retailday.Package{{ID: "PKG", Outcome: "LABELED"}}
		f.ShipmentConfirmations = 1
		f.TrackingNumbers = []string{"TN"}
		f.Status = "DELIVERED"
		f.DeliveredAt = epoch
		f.PromiseEnd = epoch.Add(24 * time.Hour)
	case retailday.Cancelled:
		f.Status, f.AuthorizedMinor, f.VoidedMinor = "CANCELLED", 5000, 5000
	}
	return f
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

func TestStockThatIsNotRestockedFailsTheEstate(t *testing.T) {
	g := newRig()
	g.restocker.onRestock = func(map[string]int) (RestockResult, error) { return RestockResult{}, nil } // a leg that does nothing
	rep := g.run(context.Background())
	if rep.Passed() || !rep.hasRule("stock/restocked") {
		t.Fatalf("stock that came back short passed:\n%s", rep.dump())
	}
	// every order is fine: it is the ESTATE that fails
	for _, o := range rep.Orders {
		if o.Outcome == OutcomeFail {
			t.Errorf("order %s failed: %v", o.Order, o.Rules)
		}
	}
}

func TestARestockThatClaimsUnitsItDidNotBringFailsTheEstate(t *testing.T) {
	g := newRig()
	g.restocker.onRestock = func(start map[string]int) (RestockResult, error) {
		r := g.w.restock(start)
		for sku := range r.Restocked {
			r.Restocked[sku]++ // the result says more than inventory shows
		}
		return r, nil
	}
	rep := g.run(context.Background())
	if rep.Passed() || !rep.hasRule("stock/conserved") {
		t.Fatalf("a restock result that disagrees with the stock passed:\n%s", rep.dump())
	}
}

func TestARestockThatCannotRunFailsTheDayAndItsProblemsAreReported(t *testing.T) {
	g := newRig()
	g.restocker.onRestock = func(map[string]int) (RestockResult, error) {
		return RestockResult{}, errors.New("inbound-receiving 503")
	}
	rep := g.run(context.Background())
	if rep.Passed() || rep.phase(PhaseRestock).State != PhaseFailed || !rep.hasRule("phase/restock") {
		t.Fatalf("restock error:\n%s", rep.dump())
	}

	g = newRig()
	g.restocker.onRestock = func(start map[string]int) (RestockResult, error) {
		r := g.w.restock(start)
		r.Problems = []string{"delivery ASN-1: receipt discrepancy"}
		return r, nil
	}
	rep = g.run(context.Background())
	if rep.Passed() || !rep.hasRule("phase/restock") {
		t.Fatalf("restock problems were not a failure:\n%s", rep.dump())
	}
}

// ---- not verified = fail closed ----

func TestEvidenceThatCouldNotReadAFactOfADeliveredOrderFailsClosed(t *testing.T) {
	g := newRig()
	victim := firstDelivered(g)
	g.evidence.onOrder = func(p retailday.Placed) (retailday.OrderFacts, []string) {
		f := g.w.facts(p)
		if p.OrderID == orderID(victim) {
			// the facts are perfect, but the collector says it could not read one of them
			return f, []string{"shipping-and-tracking: GET /journeys -> 503"}
		}
		return f, nil
	}
	rep := g.run(context.Background())
	if rep.Passed() {
		t.Fatalf("a note on a delivered order passed:\n%s", rep.dump())
	}
	row := rep.row(orderID(victim))
	if row.Outcome != OutcomeFail || !contains(row.Rules, "evidence/not-verified") {
		t.Errorf("row %+v", row)
	}
	if !containsSub(rep.NotVerified, "shipping-and-tracking: GET /journeys -> 503") || !containsSub(rep.NotVerified, orderID(victim)) {
		t.Errorf("the note is not in NOT VERIFIED with its order: %v", rep.NotVerified)
	}
	if !strings.Contains(rep.dump(), "NOT VERIFIED") {
		t.Error("the text report has no NOT VERIFIED section")
	}
}

func TestANoteOnAnOrderThatMustHaveNothingDownstreamIsAlsoNotGreen(t *testing.T) {
	g := newRig()
	g.evidence.onOrder = func(p retailday.Placed) (retailday.OrderFacts, []string) {
		f := g.w.facts(p)
		if p.Expect == retailday.Declined {
			return f, []string{"order-management: GET /orders?reference -> 503"}
		}
		return f, nil
	}
	rep := g.run(context.Background())
	if rep.Passed() {
		t.Fatalf("\"nothing downstream\" that could not be looked at passed:\n%s", rep.dump())
	}
	if !containsSub(rep.NotVerified, "order-management: GET /orders?reference -> 503") {
		t.Errorf("NOT VERIFIED %v", rep.NotVerified)
	}
}

func TestDeadLettersThatCannotBeCountedAreNotVerifiedAndFailUnlessAllowed(t *testing.T) {
	for name, dlq := range map[string]func() (int, bool, error){
		"unknown":  func() (int, bool, error) { return 0, false, nil },
		"an error": func() (int, bool, error) { return 0, false, errors.New("kafka tap not reading") },
	} {
		t.Run(name, func(t *testing.T) {
			g := newRig()
			g.evidence.dlq = dlq
			rep := g.run(context.Background())
			if rep.Passed() || !rep.hasRule("kafka/dead-letters-unverified") {
				t.Fatalf("an unknown dead-letter count passed:\n%s", rep.dump())
			}
			if !containsSub(rep.NotVerified, "dead letters") {
				t.Errorf("NOT VERIFIED %v", rep.NotVerified)
			}

			g = newRig()
			g.evidence.dlq = dlq
			g.runner.Config.AllowUnverified = true
			rep = g.run(context.Background())
			if !rep.Passed() {
				t.Fatalf("-allow-unverified did not let an unknown dead-letter count through:\n%s", rep.dump())
			}
			if !containsSub(rep.NotVerified, "dead letters") || !rep.AllowUnverified {
				t.Errorf("an allowed unverified item must still be LISTED: %v", rep.NotVerified)
			}
			if !strings.Contains(rep.dump(), "-allow-unverified") {
				t.Error("the report does not say the policy was relaxed")
			}
		})
	}
}

func TestAllowUnverifiedDoesNotHideARuleThatFailedBecauseAFactWasUnread(t *testing.T) {
	g := newRig()
	g.runner.Config.AllowUnverified = true
	victim := firstDelivered(g)
	g.evidence.onOrder = func(p retailday.Placed) (retailday.OrderFacts, []string) {
		if p.OrderID == orderID(victim) {
			return retailday.OrderFacts{OrderID: p.OrderID, Expect: p.Expect}, []string{"nothing could be read"}
		}
		return g.w.facts(p), nil
	}
	rep := g.run(context.Background())
	if rep.Passed() {
		t.Fatalf("an order whose facts are all zero passed under -allow-unverified:\n%s", rep.dump())
	}
}

func TestDeadLettersThatCarryTheRunFailTheEstate(t *testing.T) {
	g := newRig()
	g.evidence.dlq = func() (int, bool, error) { return 2, true, nil }
	rep := g.run(context.Background())
	if rep.Passed() || !rep.hasRule("kafka/dead-letters") {
		t.Fatalf("dead letters carrying the run id passed:\n%s", rep.dump())
	}
}

func TestServerErrorsFailTheEstateAndTheStatisticsAreReported(t *testing.T) {
	g := newRig()
	g.calls.n5xx = 3
	g.calls.failures = []string{"checkout POST /checkout -> 503"}
	g.runner.Calls = g.calls
	rep := g.run(context.Background())
	if rep.Passed() || !rep.hasRule("http/5xx") || rep.ServerErrors != 3 || len(rep.Failures) != 1 || len(rep.Calls) != 1 {
		t.Fatalf("5xx not judged or reported:\n%s", rep.dump())
	}
}

func TestMissingCallStatisticsAreNotVerified(t *testing.T) {
	g := newRig()
	g.runner.Calls = nil
	rep := g.run(context.Background())
	if rep.Passed() || !rep.hasRule("http/5xx-unverified") || !containsSub(rep.NotVerified, "call statistics") {
		t.Fatalf("a day with no call statistics passed:\n%s", rep.dump())
	}
}

func TestHostSuspensionsAreReportedAndNeverExcuseA5xx(t *testing.T) {
	g := newRig()
	g.calls.n5xx = 1
	g.runner.Calls = g.calls
	g.runner.Suspensions = stubSleeps{windows: []sim.Suspension{{From: epoch, To: epoch.Add(time.Minute)}}}
	rep := g.run(context.Background())
	if rep.Passed() || rep.Suspensions != 1 || !containsSub(rep.NotVerified, "suspended") {
		t.Fatalf("suspension handling:\n%s", rep.dump())
	}
}

func containsSub(ss []string, sub string) bool {
	for _, s := range ss {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// ---- the orders that must leave nothing behind ----

func TestDeclinedCancelledAndNotServedOrdersAreJudgedWithNothingDownstream(t *testing.T) {
	for _, e := range []retailday.Outcome{retailday.Declined, retailday.Cancelled, retailday.NotServed} {
		t.Run(string(e), func(t *testing.T) {
			g := newRig()
			victim := g.buyer(e, 0)
			g.evidence.onOrder = func(p retailday.Placed) (retailday.OrderFacts, []string) {
				f := g.w.facts(p)
				if p.Expect == e {
					// a purchase order and a work unit exist for an order that must have none
					f.POs = []retailday.PO{{Number: "PO-LEAK", State: "ISSUED"}}
					f.WorkUnits = 1
				}
				return f, nil
			}
			rep := g.run(context.Background())
			if rep.Passed() {
				t.Fatalf("a %s order with a purchase order passed:\n%s", e, rep.dump())
			}
			label := orderID(victim)
			if e != retailday.Cancelled {
				label = victim.Label()
			}
			row := rep.row(label)
			if !contains(row.Rules, "network/no-po") || !contains(row.Rules, "warehouse/no-work") {
				t.Errorf("%s row %q: %+v", e, label, row)
			}
			// and nothing downstream was ever played for it
			for _, id := range append(g.floor.ids(), g.carrier.delivered()...) {
				if id == orderID(victim) {
					t.Errorf("the %s order reached a downstream phase", e)
				}
			}
		})
	}
}

func TestACancelledOrderWhoseCancellationWasRefusedFailsAndIsNotShipped(t *testing.T) {
	g := newRig()
	victim := g.buyer(retailday.Cancelled, 0)
	g.shopper.onBuy = func(_ context.Context, b Buyer) (Bought, error) {
		if b.Index == victim.Index {
			return Bought{Status: StatusPlaced, OrderID: orderID(b), TotalMinor: 5000, CancelRefused: "cancel-after-lock"}, nil
		}
		return g.w.buy(b), nil
	}
	rep := g.run(context.Background())
	row := rep.row(orderID(victim))
	if rep.Passed() || !contains(row.Rules, "shopper/cancel-refused") || !contains(row.Rules, "shopper/outcome") {
		t.Fatalf("a refused cancellation:\n%s", rep.dump())
	}
}

func TestAnInDoubtOrderIsAWarningAndNeverGreen(t *testing.T) {
	g := newRig()
	rep := g.run(context.Background())
	v := g.buyer(retailday.InDoubt, 0)
	row := rep.row(v.Label())
	if row.Outcome != OutcomeWarn || len(row.Findings) < 1 {
		t.Fatalf("row %+v", row)
	}
	warned := false
	for _, f := range row.Findings {
		warned = warned || (f.Warn && f.Rule == "in-doubt")
		if !f.Warn {
			t.Errorf("an in-doubt order has a failing finding: %+v", f)
		}
	}
	if !warned {
		t.Error("no in-doubt warning from AuditOrder")
	}
	if !rep.Passed() {
		t.Error("a warning alone must not fail the day")
	}

	// money moving on an in-doubt order IS a failure
	g = newRig()
	g.evidence.onOrder = func(p retailday.Placed) (retailday.OrderFacts, []string) {
		f := g.w.facts(p)
		if p.Expect == retailday.InDoubt {
			f.CapturedMinor = 5000
		}
		return f, nil
	}
	if rep = g.run(context.Background()); rep.Passed() {
		t.Fatalf("captured money on an in-doubt order passed:\n%s", rep.dump())
	}

	// and so is a shopper that did NOT end in doubt: the wedge was fixed and the plan is stale
	g = newRig()
	g.shopper.onBuy = func(_ context.Context, b Buyer) (Bought, error) {
		if b.Expect == retailday.InDoubt {
			return Bought{Status: StatusPlaced, OrderID: "ORD-X", TotalMinor: 5000}, nil
		}
		return g.w.buy(b), nil
	}
	if rep = g.run(context.Background()); rep.Passed() || !rep.hasRule("shopper/outcome") {
		t.Fatalf("an in-doubt buyer that placed an order passed:\n%s", rep.dump())
	}
}

func TestTheJudgementUsesThePlansExpectationEvenIfTheEvidenceDisagrees(t *testing.T) {
	g := newRig()
	g.evidence.onOrder = func(p retailday.Placed) (retailday.OrderFacts, []string) {
		f := g.w.facts(p)
		f.Expect = retailday.Delivered // a collector that guesses
		f.OrderID = "something-else"
		return f, nil
	}
	rep := g.run(context.Background())
	for _, o := range rep.Orders {
		if o.Expect == retailday.Declined && (o.Outcome != OutcomePass) {
			t.Errorf("a declined order was judged as delivered: %+v", o)
		}
		for _, f := range o.Findings {
			if f.Order != o.Order {
				t.Errorf("finding %+v is filed under %q, row %q", f, f.Order, o.Order)
			}
		}
	}
}

// ---- a phase that cannot run at all ----

func TestAPhaseThatCannotRunFailsTheWholeDay(t *testing.T) {
	cases := []struct {
		name  string
		arm   func(g *rig)
		phase string
	}{
		{"the floor answers the wrong number of results", func(g *rig) {
			g.runner.Floor = shortFloor{}
		}, PhaseFloor},
		{"the dock answers the wrong number of shipments", func(g *rig) {
			g.runner.Shipper = shortShipper{}
		}, PhaseShip},
		{"the restock cannot run", func(g *rig) {
			g.restocker.onRestock = func(map[string]int) (RestockResult, error) { return RestockResult{}, errors.New("down") }
		}, PhaseRestock},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			g := newRig()
			c.arm(g)
			rep := g.run(context.Background())
			if rep.Passed() || rep.phase(c.phase).State != PhaseFailed || rep.Verdict != VerdictFailed {
				t.Fatalf("phase %s: %+v\n%s", c.phase, rep.phase(c.phase), rep.dump())
			}
			// the other orders were not silently dropped: every buyer still has a row
			if len(rep.Orders) != rep.Planned {
				t.Errorf("%d rows for %d buyers", len(rep.Orders), rep.Planned)
			}
		})
	}
}

type shortFloor struct{}

func (shortFloor) ProcessAll(_ context.Context, ids []string, _ int) []FloorResult { return nil }

type shortShipper struct{}

func (shortShipper) ShipAll(_ context.Context, p []Package) []Shipment { return nil }

func TestADayThatCannotStartBuysNothing(t *testing.T) {
	cases := []struct {
		name  string
		arm   func(g *rig)
		phase string
	}{
		{"the building cannot be prepared", func(g *rig) { g.prepareErr = errors.New("a station would not check in") }, PhasePrepare},
		{"the catalogue cannot be read", func(g *rig) { g.catalogue.err = errors.New("storefront 503") }, PhasePlan},
		{"the catalogue is too small for the shelf", func(g *rig) { g.catalogue.items = candidates(3) }, PhasePlan},
		{"the shelf cannot be read", func(g *rig) {
			g.evidence.onShelf = func([]string) (map[string]retailday.Shelf, error) { return nil, errors.New("collector not wired") }
		}, PhasePlan},
		{"the shelf has no stock for the day's demand", func(g *rig) {
			g.evidence.onShelf = func(skus []string) (map[string]retailday.Shelf, error) {
				out := map[string]retailday.Shelf{}
				for _, s := range skus {
					out[s] = retailday.Shelf{Usable: 1, Purchasable: true}
				}
				return out, nil
			}
		}, PhasePlan},
		{"a shelf SKU has no reading", func(g *rig) {
			g.evidence.onShelf = func(skus []string) (map[string]retailday.Shelf, error) {
				out := map[string]retailday.Shelf{}
				for _, s := range skus[1:] {
					out[s] = retailday.Shelf{Usable: 20, Purchasable: true}
				}
				return out, nil
			}
		}, PhasePlan},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			g := newRig()
			c.arm(g)
			rep := g.run(context.Background())
			if rep.Passed() {
				t.Fatalf("passed:\n%s", rep.dump())
			}
			if n := len(g.shopper.called()); n != 0 {
				t.Errorf("%d purchases were made on a day that could not start", n)
			}
			if got := rep.phase(c.phase).State; got != PhaseFailed {
				t.Errorf("phase %s is %q", c.phase, got)
			}
			if g.cleaned != 1 {
				t.Errorf("cleanup ran %d times", g.cleaned)
			}
			for _, o := range rep.Orders {
				if o.Outcome != OutcomeFail {
					t.Errorf("order %s %s: an order that never ran must not look fine", o.Order, o.Outcome)
				}
			}
			for _, p := range requiredPhases {
				if rep.phase(p).Name == "" {
					t.Errorf("phase %s missing from the report", p)
				}
			}
		})
	}
}

func TestARunnerMissingAPhaseFailsInsteadOfPanicking(t *testing.T) {
	for _, drop := range []func(*Runner){
		func(r *Runner) { r.Shopper = nil }, func(r *Runner) { r.Floor = nil }, func(r *Runner) { r.Shipper = nil },
		func(r *Runner) { r.Carrier = nil }, func(r *Runner) { r.Restocker = nil }, func(r *Runner) { r.Evidence = nil },
		func(r *Runner) { r.Catalogue = nil },
	} {
		g := newRig()
		drop(g.runner)
		rep := g.run(context.Background())
		if rep.Passed() || rep.Verdict != VerdictFailed {
			t.Fatalf("a day with a missing phase passed:\n%s", rep.dump())
		}
	}
}

func TestAnOverdrawnShelfIsRefusedBeforeAnyPurchase(t *testing.T) {
	g := newRig()
	plan := g.plan()
	var sku string
	for s, n := range plan.Demand() {
		if n >= 2 {
			sku = s
			break
		}
	}
	if sku == "" {
		t.Skip("the seed's demand is 1 unit of every SKU")
	}
	g.evidence.onShelf = func(skus []string) (map[string]retailday.Shelf, error) {
		out := map[string]retailday.Shelf{}
		for _, s := range skus {
			out[s] = retailday.Shelf{Usable: 20, Purchasable: true}
		}
		out[sku] = retailday.Shelf{Usable: 1, Purchasable: true}
		return out, nil
	}
	rep := g.run(context.Background())
	if rep.Passed() || len(g.shopper.called()) != 0 || !strings.Contains(rep.phase(PhasePlan).Detail, sku) {
		t.Fatalf("overdrawn %s:\n%s", sku, rep.dump())
	}
}

// ---- concurrency is bounded ----

func TestShoppersRunWithBoundedParallelism(t *testing.T) {
	const parallel = 3
	g := newRig()
	g.runner.Config.Parallel = parallel
	var inFlight, peak atomic.Int64
	var arrived atomic.Int64
	release := make(chan struct{})
	var once sync.Once
	g.shopper.onBuy = func(ctx context.Context, b Buyer) (Bought, error) {
		n := inFlight.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		defer inFlight.Add(-1)
		// The first `parallel` shoppers wait for each other: if the runner allowed fewer than
		// `parallel` at once the barrier would never fill. (Shoppers beyond them pass at once.)
		if arrived.Add(1) <= parallel {
			if arrived.Load() == parallel {
				once.Do(func() { close(release) })
			}
			select {
			case <-release:
			case <-time.After(10 * time.Second):
				return Bought{}, errors.New("the runner never ran " + fmt.Sprint(parallel) + " shoppers at once")
			}
		}
		return g.w.buy(b), nil
	}
	rep := g.run(context.Background())
	if !rep.Passed() {
		t.Fatalf("%v\n%s", rep.Reasons, rep.dump())
	}
	if got := peak.Load(); got > parallel {
		t.Fatalf("%d shoppers at once, the bound is %d", got, parallel)
	}
}

func TestEveryBoundedPhaseHonoursParallel(t *testing.T) {
	const parallel = 2
	g := newRig()
	g.runner.Config.Parallel = parallel
	// give the day enough loads and parcels to exceed the bound: a distinct CPT per order
	g.floor.onProcess = func(_ context.Context, id string) FloorResult {
		r := g.w.label(id)
		r.CPT = "cpt-" + id
		return r
	}
	slow := func() { time.Sleep(3 * time.Millisecond) }
	g.shipper.hold, g.carrier.hold = slow, slow
	rep := g.run(context.Background())
	if !rep.Passed() {
		t.Fatalf("%v\n%s", rep.Reasons, rep.dump())
	}
	if g.shipper.maxFlt > parallel || g.carrier.maxFlt > parallel {
		t.Fatalf("dock %d and carrier %d in flight, the bound is %d", g.shipper.maxFlt, g.carrier.maxFlt, parallel)
	}
	for _, p := range g.floor.parallels {
		if p != parallel {
			t.Errorf("the floor was told %d in parallel, want %d", p, parallel)
		}
	}
}

// Packages that share a load must reach the dock in ONE call: they share a door, a trailer and a
// departure, and two concurrent calls on one load book it twice.
func TestPackagesOfOneLoadAreShippedTogetherAndOtherLoadsApart(t *testing.T) {
	g := newRig()
	rep := g.run(context.Background())
	if !rep.Passed() {
		t.Fatalf("%v", rep.Reasons)
	}
	plan := g.plan()
	cpts := map[string]int{}
	for _, b := range plan.Buyers {
		if b.Expect == retailday.Delivered {
			cpts[cptFor(b)]++
		}
	}
	batches := g.shipper.batches()
	if len(batches) != len(cpts) {
		t.Fatalf("%d ShipAll calls for %d distinct loads (%v)", len(batches), len(cpts), cpts)
	}
	for _, batch := range batches {
		for _, p := range batch {
			if p.CPT != batch[0].CPT || p.SiteID != batch[0].SiteID {
				t.Errorf("a batch mixes loads: %+v", batch)
			}
		}
		if len(batch) != cpts[batch[0].CPT] {
			t.Errorf("a load's %d packages were split: batch of %d", cpts[batch[0].CPT], len(batch))
		}
	}
}

// ---- a cancelled context ends every phase promptly ----

func TestACancelledContextEndsEveryPhasePromptly(t *testing.T) {
	stages := []string{PhaseShoppers, PhaseFloor, PhaseShip, PhaseDeliver, PhaseRestock}
	for _, at := range stages {
		t.Run(at, func(t *testing.T) {
			g := newRig()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			block := func(c context.Context) { <-c.Done() }
			switch at {
			case PhaseShoppers:
				g.shopper.onBuy = func(c context.Context, b Buyer) (Bought, error) {
					cancel()
					block(c)
					return Bought{}, c.Err()
				}
			case PhaseFloor:
				g.floor.onProcess = func(c context.Context, id string) FloorResult {
					cancel()
					block(c)
					return FloorResult{OrderID: id, Problems: []string{c.Err().Error()}}
				}
			case PhaseShip:
				g.shipper.onShip = func(p Package) Shipment {
					cancel()
					block(ctx)
					return Shipment{PackageID: p.PackageID, Problems: []string{ctx.Err().Error()}}
				}
			case PhaseDeliver:
				g.carrier.onDeliver = func(id string) Delivery {
					cancel()
					block(ctx)
					return Delivery{Problems: []string{ctx.Err().Error()}}
				}
			case PhaseRestock:
				g.restocker.onRestock = func(map[string]int) (RestockResult, error) {
					cancel()
					block(ctx)
					return RestockResult{}, ctx.Err()
				}
			}
			done := make(chan Report, 1)
			go func() { done <- g.run(ctx) }()
			var rep Report
			select {
			case rep = <-done:
			case <-time.After(20 * time.Second):
				t.Fatal("the run did not end after its context was cancelled")
			}
			if rep.Passed() || rep.Verdict != VerdictFailed {
				t.Fatalf("a cancelled day passed:\n%s", rep.dump())
			}
			if len(rep.Orders) != rep.Planned || rep.Planned == 0 {
				t.Errorf("%d rows for %d buyers", len(rep.Orders), rep.Planned)
			}
			for _, o := range rep.Orders {
				if o.Outcome == OutcomePass {
					t.Errorf("order %s reads PASS on an interrupted day", o.Order)
				}
			}
			// the phases after the interruption did not run, and say so
			after := false
			for _, p := range requiredPhases {
				if p == at {
					after = true
					continue
				}
				if after && rep.phase(p).State != PhaseAborted {
					t.Errorf("phase %s after the interruption is %q, want aborted", p, rep.phase(p).State)
				}
			}
			if len(g.evidence.seen()) != 0 {
				t.Errorf("the evidence was read after the context ended")
			}
			// cleanup still ran, on a context that was not cancelled
			if g.cleaned != 1 || g.cleanCtxErr != nil {
				t.Errorf("cleanup ran %d times with ctx error %v", g.cleaned, g.cleanCtxErr)
			}
		})
	}
}

func TestACancelledContextBeforeTheRunStartsNothing(t *testing.T) {
	g := newRig()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rep := g.run(ctx)
	if rep.Passed() || len(g.shopper.called()) != 0 || g.catalogue.calls != 0 {
		t.Fatalf("work was done on a cancelled context:\n%s", rep.dump())
	}
}

// ---- the report ----

func TestTheReportRoundTripsThroughJSON(t *testing.T) {
	g := newRig()
	g.evidence.dlq = func() (int, bool, error) { return 0, false, nil } // so NotVerified and Estate are populated
	g.runner.Config.Notes = []string{"started outside the release window"}
	rep := g.run(context.Background())
	var buf bytes.Buffer
	if err := rep.JSON(&buf); err != nil {
		t.Fatal(err)
	}
	var back Report
	if err := json.Unmarshal(buf.Bytes(), &back); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, buf.String())
	}
	if !reflect.DeepEqual(rep, back) {
		var again bytes.Buffer
		_ = back.JSON(&again)
		t.Fatalf("the report changed through JSON:\n%s\n---\n%s", buf.String(), again.String())
	}
	if back.Passed() != rep.Passed() || back.Verdict != VerdictFailed || len(back.NotVerified) == 0 || len(back.Orders) != 12 {
		t.Errorf("decoded: passed %v verdict %s notVerified %v orders %d", back.Passed(), back.Verdict, back.NotVerified, len(back.Orders))
	}
}

func TestTheTextReportHasTheOrderTableTheEstateTheStockAndTheStatistics(t *testing.T) {
	g := newRig()
	g.evidence.dlq = func() (int, bool, error) { return 0, false, nil }
	rep := g.run(context.Background())
	text := rep.dump()
	for _, want := range []string{"retail day RD1", "PHASES", "ORDERS (12 planned, 12 accounted for)", "EXPECTED", "ESTATE", "SKU", "NOT VERIFIED", "dead letters", "CALLS:", "storefront POST /checkout", "VERDICT: FAILED", "in-doubt", "WARN"} {
		if !strings.Contains(text, want) {
			t.Errorf("the text report lacks %q:\n%s", want, text)
		}
	}
}

func TestTheSameSeedPlaysTheSameDay(t *testing.T) {
	a, b := newRig().run(context.Background()), newRig().run(context.Background())
	if !reflect.DeepEqual(a.Orders, b.Orders) || !reflect.DeepEqual(a.Stock, b.Stock) || !reflect.DeepEqual(a.Shelf, b.Shelf) {
		t.Fatalf("two runs of one seed differ:\n%s\n%s", a.dump(), b.dump())
	}
}

func TestPassedIsDerivedFromTheFactsNotFromTheVerdictString(t *testing.T) {
	g := newRig()
	rep := g.run(context.Background())
	if !rep.Passed() {
		t.Fatal("setup")
	}
	cases := map[string]func(*Report){
		"an order is missing":        func(r *Report) { r.Orders = r.Orders[1:] },
		"nothing planned":            func(r *Report) { r.Planned, r.Orders = 0, nil },
		"a row failed":               func(r *Report) { r.Orders[0].Outcome = OutcomeFail },
		"a failing finding on a row": func(r *Report) { r.Orders[0].Findings = []retailday.Finding{{Rule: "x"}} },
		"an estate finding":          func(r *Report) { r.Estate = []retailday.Finding{{Rule: "x"}} },
		"a phase failed":             func(r *Report) { r.Phases[3].State = PhaseFailed },
		"a phase aborted":            func(r *Report) { r.Phases[5].State = PhaseAborted },
		"a phase is missing":         func(r *Report) { r.Phases = r.Phases[:len(r.Phases)-1] },
	}
	for name, mut := range cases {
		c := rep
		c.Orders = append([]OrderReport(nil), rep.Orders...)
		c.Phases = append([]PhaseStatus(nil), rep.Phases...)
		for i := range c.Orders {
			c.Orders[i].Findings = append([]retailday.Finding(nil), rep.Orders[i].Findings...)
		}
		mut(&c)
		if c.Passed() {
			t.Errorf("%s: still passes", name)
		}
	}
	warn := rep
	warn.Estate = []retailday.Finding{{Rule: "x", Warn: true}}
	if !warn.Passed() {
		t.Error("a warning alone must not fail the day")
	}
}
