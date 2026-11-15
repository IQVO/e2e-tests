package day

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
)

// The floor works on order-management's id, not the storefront's. No published route maps one to
// the other by itself, so the runner asks a Linker, and everything after it uses that answer.

func TestTheFloorIsHandedTheLinkedOrderNotTheStorefrontsOrder(t *testing.T) {
	g := newRig()
	// an id scheme that has nothing in common with the storefront's, like the estate's
	g.linker.onLink = func(p retailday.Placed) ([]string, error) { return []string{omIDOf(p.OrderID)}, nil }
	rep := g.run(context.Background())
	if !rep.Passed() {
		t.Fatalf("%v\n%s", rep.Reasons, rep.dump())
	}
	for _, id := range g.floor.ids() {
		if _, ok := customerOf(id); !ok {
			t.Errorf("the floor was handed %q, which is not an order-management id", id)
		}
	}
	// the packages the dock ships carry the floor's ids, not the storefront's
	for _, batch := range g.shipper.batches() {
		for _, p := range batch {
			if !strings.HasPrefix(p.OrderRef, "OM-") || !strings.HasPrefix(p.SourceOrderID, "OM-") {
				t.Errorf("package %s: order ref %q, source order %q: want order-management ids", p.PackageID, p.OrderRef, p.SourceOrderID)
			}
		}
	}
	// the carrier, in contrast, follows the storefront's order: that is the id its tracking view has
	for _, id := range g.carrier.delivered() {
		if strings.HasPrefix(id, "OM-") {
			t.Errorf("the carrier was handed %q: shipping-and-tracking's view is keyed by the storefront's order", id)
		}
	}
}

// A floor that is handed the storefront's id is refused by order-management (the stub says what the
// real one does): this is the defect the link phase exists to prevent, so it must fail LOUDLY.
func TestALinkerThatReturnsTheStorefrontIdFailsTheOrder(t *testing.T) {
	g := newRig()
	victim := firstDelivered(g)
	g.linker.onLink = func(p retailday.Placed) ([]string, error) {
		if p.OrderID == orderID(victim) {
			return []string{p.OrderID}, nil // not an order-management id
		}
		return []string{omIDOf(p.OrderID)}, nil
	}
	rep := g.run(context.Background())
	if rep.Passed() {
		t.Fatalf("a floor handed a storefront id passed:\n%s", rep.dump())
	}
	row := rep.row(orderID(victim))
	if row.Outcome != OutcomeFail || !contains(row.Rules, "floor/not-labeled") {
		t.Errorf("victim row: %+v", row)
	}
}

func TestAnOrderThatCannotBeLinkedFailsThatOrderOnly(t *testing.T) {
	for name, link := range map[string]func(retailday.Placed) ([]string, error){
		"the linker errors": func(retailday.Placed) ([]string, error) {
			return nil, errors.New("purchase order never acknowledged within 30m")
		},
		"the linker is empty": func(retailday.Placed) ([]string, error) { return nil, nil },
		"the linker panics":   func(retailday.Placed) ([]string, error) { panic("index out of range") },
	} {
		t.Run(name, func(t *testing.T) {
			g := newRig()
			victim := firstDelivered(g)
			g.linker.onLink = func(p retailday.Placed) ([]string, error) {
				if p.OrderID == orderID(victim) {
					return link(p)
				}
				return []string{omIDOf(p.OrderID)}, nil
			}
			rep := g.run(context.Background())
			if rep.Passed() {
				t.Fatalf("an order that could not be linked passed:\n%s", rep.dump())
			}
			row := rep.row(orderID(victim))
			if row.Outcome != OutcomeFail || !contains(row.Rules, "link/order") {
				t.Errorf("victim row: %+v", row)
			}
			for _, b := range g.plan().Buyers {
				if b.Expect == retailday.Delivered && b.Index != victim.Index && rep.row(orderID(b)).Outcome != OutcomePass {
					t.Errorf("another order was dragged down: %+v", rep.row(orderID(b)))
				}
			}
			// nothing downstream was played for it
			for _, id := range g.floor.ids() {
				if id == omIDOf(orderID(victim)) || id == orderID(victim) {
					t.Errorf("the floor was handed the unlinked order as %q", id)
				}
			}
			for _, id := range g.carrier.delivered() {
				if id == orderID(victim) {
					t.Error("the carrier delivered an order that never reached the floor")
				}
			}
			if st := rep.phase(PhaseLink); st.State != PhaseOK {
				t.Errorf("link phase %+v: one of many orders unlinked is that order's failure, not the phase's", st)
			}
		})
	}
}

func TestADuplicateOrderIsAFindingAndTheFloorWorksTheFirst(t *testing.T) {
	g := newRig()
	victim := firstDelivered(g)
	g.linker.onLink = func(p retailday.Placed) ([]string, error) {
		if p.OrderID == orderID(victim) {
			return []string{omIDOf(p.OrderID) + "-B", omIDOf(p.OrderID), omIDOf(p.OrderID)}, nil // unsorted, one repeated
		}
		return []string{omIDOf(p.OrderID)}, nil
	}
	rep := g.run(context.Background())
	if rep.Passed() {
		t.Fatalf("two order-management orders for one order passed:\n%s", rep.dump())
	}
	row := rep.row(orderID(victim))
	if !contains(row.Rules, "link/one-order") {
		t.Errorf("victim row: %+v", row)
	}
	var n int
	for _, id := range g.floor.ids() {
		if strings.HasPrefix(id, omIDOf(orderID(victim))) {
			n++
			if id != omIDOf(orderID(victim)) {
				t.Errorf("the floor worked %q: the first in id order is %q", id, omIDOf(orderID(victim)))
			}
		}
	}
	if n != 1 {
		t.Errorf("the floor worked %d order(s) for the one order, want 1", n)
	}
}

func TestWhenNothingCanBeLinkedTheLinkPhaseFailsAndTheFloorStaysIdle(t *testing.T) {
	g := newRig()
	g.linker.onLink = func(retailday.Placed) ([]string, error) { return nil, errors.New("network-fulfillment unreachable") }
	rep := g.run(context.Background())
	if rep.Passed() {
		t.Fatalf("a day with no order in order-management passed:\n%s", rep.dump())
	}
	if st := rep.phase(PhaseLink); st.State != PhaseFailed {
		t.Errorf("link phase %+v, want failed", st)
	}
	if st := rep.phase(PhaseFloor); st.State != PhaseIdle {
		t.Errorf("floor phase %+v, want idle: nothing reached it", st)
	}
	if ids := g.floor.ids(); len(ids) != 0 {
		t.Errorf("the floor was handed %v", ids)
	}
}

// Only the orders placed for delivery are linked: a declined, cancelled or not-served buyer has
// no order-management order to find, and looking would turn "nothing may exist" into a wait.
func TestOnlyOrdersPlacedForDeliveryAreLinked(t *testing.T) {
	g := newRig()
	g.run(context.Background())
	var want []string
	for _, b := range g.plan().Buyers {
		if b.Expect == retailday.Delivered {
			want = append(want, orderID(b))
		}
	}
	sort.Strings(want)
	if got := g.linker.linked(); !reflect.DeepEqual(got, want) {
		t.Errorf("linked %v, want %v", got, want)
	}
	for _, p := range g.linker.calls {
		if p.RunID == "" || p.Expect != retailday.Delivered || p.OrderID == "" {
			t.Errorf("the linker was given %+v: it needs the run id, the expectation and the storefront's order id", p)
		}
	}
}

func TestLinkingIsBoundedByParallel(t *testing.T) {
	g := newRig()
	g.runner.Config.Parallel = 2
	var cur, peak int
	g.linker.onLink = func(p retailday.Placed) ([]string, error) {
		g.linker.mu.Lock() // calls is guarded by the same lock; the counters ride on it
		cur++
		peak = max(peak, cur)
		g.linker.mu.Unlock()
		time.Sleep(3 * time.Millisecond)
		g.linker.mu.Lock()
		cur--
		g.linker.mu.Unlock()
		return []string{omIDOf(p.OrderID)}, nil
	}
	if rep := g.run(context.Background()); !rep.Passed() {
		t.Fatalf("%v\n%s", rep.Reasons, rep.dump())
	}
	if peak < 1 || peak > 2 {
		t.Errorf("peak %d concurrent links with Parallel 2", peak)
	}
}

func TestACancelledContextEndsTheLinkPromptly(t *testing.T) {
	g := newRig()
	ctx, cancel := context.WithCancel(context.Background())
	g.linker.onLink = func(retailday.Placed) ([]string, error) {
		cancel()
		return nil, ctx.Err()
	}
	done := make(chan Report, 1)
	go func() { done <- g.run(ctx) }()
	select {
	case rep := <-done:
		if rep.Passed() {
			t.Fatalf("an interrupted day passed:\n%s", rep.dump())
		}
		if ids := g.floor.ids(); len(ids) != 0 {
			t.Errorf("the floor ran after the context ended: %v", ids)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the run did not end with its context")
	}
}

func TestADayWithoutALinkerIsNotPlayed(t *testing.T) {
	g := newRig()
	g.runner.Linker = nil
	rep := g.run(context.Background())
	if rep.Passed() {
		t.Fatalf("a day with a hole in it passed:\n%s", rep.dump())
	}
	if st := rep.phase(PhasePrepare); !strings.Contains(st.Detail, "Linker") {
		t.Errorf("prepare phase %+v: want it to name the missing Linker", st)
	}
	if ids := g.floor.ids(); len(ids) != 0 {
		t.Errorf("the floor ran on an unwired day: %v", ids)
	}
}

// A floor result that leaves the order refs empty must not make the dock fall back on the
// storefront's id: the package belongs to the order-management order it was worked under.
func TestAPackageWithoutRefsFallsBackToTheLinkedOrder(t *testing.T) {
	g := newRig()
	g.floor.onProcess = func(_ context.Context, id string) FloorResult {
		r := g.w.label(id)
		r.OrderRef, r.SourceOrderID = "", ""
		return r
	}
	rep := g.run(context.Background())
	if !rep.Passed() {
		t.Fatalf("%v\n%s", rep.Reasons, rep.dump())
	}
	n := 0
	for _, batch := range g.shipper.batches() {
		for _, p := range batch {
			n++
			if !strings.HasPrefix(p.OrderRef, "OM-") || !strings.HasPrefix(p.SourceOrderID, "OM-") {
				t.Errorf("package %s: order ref %q, source order %q: the fallback must be the linked order-management id", p.PackageID, p.OrderRef, p.SourceOrderID)
			}
		}
	}
	if n == 0 {
		t.Fatal("no package reached the dock")
	}
}
