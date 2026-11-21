package collect

import (
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
)

// wantNotes asserts the notes exactly: the evidence says what it could not read, no more, no less.
func wantNotes(t *testing.T, got []string, want ...string) {
	t.Helper()
	if len(got) == 0 && len(want) == 0 {
		return
	}
	if !slices.Equal(got, want) {
		t.Fatalf("notes:\n got %q\nwant %q", got, want)
	}
}

// hasNote asserts that exactly one note contains every fragment.
func hasNote(t *testing.T, notes []string, fragments ...string) {
	t.Helper()
	n := 0
	for _, note := range notes {
		all := true
		for _, f := range fragments {
			all = all && strings.Contains(note, f)
		}
		if all {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("want one note containing %q, got %d in %q", fragments, n, notes)
	}
}

func noGets(t *testing.T, w *world) {
	t.Helper()
	if bad := w.notGets(); len(bad) != 0 {
		t.Fatalf("the collector issued a request that was not a GET: %+v", bad)
	}
}

func TestShopperSideOfADeliveredOrder(t *testing.T) {
	w := newWorld(t)
	fx := seedDelivered(w, "1")
	f, notes := fx.collect(t, newCollector(w))
	wantNotes(t, notes)
	if f.OrderID != "ord-1" || f.Expect != retailday.Delivered || f.Lines != 2 {
		t.Fatalf("identity: %+v", f)
	}
	if f.Status != "DELIVERED" {
		t.Fatalf("status %q", f.Status)
	}
	if f.TotalMinor != 4500 || f.AuthorizedMinor != 4500 || f.CapturedMinor != 4500 || f.VoidedMinor != 0 {
		t.Fatalf("money: total %d authorised %d captured %d voided %d", f.TotalMinor, f.AuthorizedMinor, f.CapturedMinor, f.VoidedMinor)
	}
	if want := []string{fx.tn}; !reflect.DeepEqual(f.TrackingNumbers, want) {
		t.Fatalf("tracking %v want %v", f.TrackingNumbers, want)
	}
	if want := t0.Add(72 * time.Hour); !f.PromiseEnd.Equal(want) {
		t.Fatalf("promise end %s want %s", f.PromiseEnd, want)
	}
	noGets(t, w)
}

// Authorised, captured and released are three different allocation states: reading the wrong
// field would make a cancelled order look captured or a delivered one look voided.
func TestMoneyIsReadFromTheAllocations(t *testing.T) {
	w := newWorld(t)
	seedDelivered(w, "1")
	c := seedCancelled(w, "2")
	f, notes := c.collect(t, newCollector(w))
	wantNotes(t, notes)
	if f.Status != "CANCELLED" {
		t.Fatalf("status %q", f.Status)
	}
	if f.AuthorizedMinor != 1200 || f.VoidedMinor != 1200 || f.CapturedMinor != 0 || f.TotalMinor != 1200 {
		t.Fatalf("cancelled: authorised %d voided %d captured %d total %d", f.AuthorizedMinor, f.VoidedMinor, f.CapturedMinor, f.TotalMinor)
	}
	if len(f.TrackingNumbers) != 0 {
		t.Fatalf("a cancelled order has no tracking: %v", f.TrackingNumbers)
	}

	// A hold captured for part of the order and released for the rest.
	w.mu.Lock()
	a := w.payments.auths[0]
	a.State = "PARTIALLY_CAPTURED"
	a.Allocations = []cpAllocation{{GroupNo: 1, AmountMinor: 3000, State: "CAPTURED"}, {GroupNo: 2, AmountMinor: 1500, State: "RELEASED"}}
	w.mu.Unlock()
	d, _ := seedDeliveredFacts(t, w)
	if d.CapturedMinor != 3000 || d.VoidedMinor != 1500 || d.AuthorizedMinor != 4500 {
		t.Fatalf("split: captured %d voided %d authorised %d", d.CapturedMinor, d.VoidedMinor, d.AuthorizedMinor)
	}
}

func seedDeliveredFacts(t *testing.T, w *world) (retailday.OrderFacts, []string) {
	t.Helper()
	fx := &fixture{placed: retailday.Placed{RunID: "RUN1", Expect: retailday.Delivered, OrderID: "ord-1",
		SKUs: map[string]int{"SKU-A": 2, "SKU-B": 1}, TotalMinor: 4500, AuthorizationID: "pay-1", PlacedAt: t0}}
	return fx.collect(t, newCollector(w))
}

// A declined payment places no order: the provider's record is found by the shopper's
// authorization id, held nothing, and nothing downstream is looked for.
func TestADeclinedOrderIsReadFromItsAuthorizationAlone(t *testing.T) {
	w := newWorld(t)
	seedDelivered(w, "1")
	d := seedDeclined(w, "3")
	w.reset()
	f, notes := d.collect(t, newCollector(w))
	wantNotes(t, notes)
	if f.OrderID != "" || f.Expect != retailday.Declined {
		t.Fatalf("identity: %+v", f)
	}
	if f.AuthorizedMinor != 0 || f.CapturedMinor != 0 || f.VoidedMinor != 0 {
		t.Fatalf("a declined hold moved money: %+v", f)
	}
	if f.TotalMinor != 1200 {
		t.Fatalf("total %d: the checkout session still says what the order would have cost", f.TotalMinor)
	}
	if fs := retailday.AuditOrder(f); len(fs) != 0 {
		t.Fatalf("a clean decline has findings: %+v", fs)
	}
	for _, rd := range w.reads() {
		if !strings.HasPrefix(rd, "customer-payments ") && !strings.HasPrefix(rd, "checkout ") {
			t.Fatalf("a declined order has nothing downstream to look for, but %q was read", rd)
		}
	}
	noGets(t, w)
}

// A payment record that names an order for a payment the shopper phase saw declined is a
// defect: the collector follows the order it names, so the audit can see what exists for it.
func TestAPaymentRecordThatNamesAnOrderIsFollowed(t *testing.T) {
	w := newWorld(t)
	seedDelivered(w, "1")
	d := seedDeclined(w, "3")
	w.mu.Lock()
	w.payments.auths[len(w.payments.auths)-1].OrderID = "ord-1"
	w.mu.Unlock()
	f, notes := d.collect(t, newCollector(w))
	hasNote(t, notes, "disagree OrderID", "ord-1")
	if f.Status != "DELIVERED" || len(f.TrackingNumbers) == 0 {
		t.Fatalf("the named order was not followed: %+v", f)
	}
	// The link is asked about the order that is followed, not about the empty id the shopper phase had.
	if len(f.OMOrders) != 1 || f.OMOrders[0].ID != "om-1" || f.WorkUnits != 2 {
		t.Fatalf("the warehouse side of the named order was not followed: %+v", f)
	}
}

func TestNothingToLookUpIsSaid(t *testing.T) {
	w := newWorld(t)
	c := newCollector(w)
	for _, tc := range []struct {
		expect       retailday.Outcome
		order, money bool
	}{
		// An order that was expected has no id to follow, and its money cannot be looked up either.
		{retailday.Delivered, true, true}, {retailday.Cancelled, true, true},
		// No order is expected, but a zero here would pass "no money moved" without looking.
		{retailday.Declined, false, true}, {retailday.InDoubt, false, true},
		// Out of area: nothing was placed and no payment exists to look at.
		{retailday.NotServed, false, false},
	} {
		_, notes := c.Order(t.Context(), retailday.Placed{RunID: "RUN1", Expect: tc.expect, SKUs: map[string]int{"SKU-C": 1}})
		if want := b2i(tc.order) + b2i(tc.money); len(notes) != want {
			t.Fatalf("%s: %d notes, want %d: %q", tc.expect, len(notes), want, notes)
		}
		if tc.order {
			hasNote(t, notes, "unread order", "no order id")
		}
		if tc.money {
			hasNote(t, notes, "unread AuthorizedMinor, CapturedMinor, VoidedMinor, TotalMinor", "neither an order id nor an authorization id")
		}
	}
	if n := len(w.snapshot()); n != 0 {
		t.Fatalf("%d requests with nothing to look up", n)
	}
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

// A 404 is an answer: nothing exists. It is not a failure and is not noted.
func TestAnUnknownOrderIsNoneNotAFailure(t *testing.T) {
	w := newWorld(t)
	f, notes := newCollector(w).Order(t.Context(), retailday.Placed{RunID: "RUN1", Expect: retailday.Delivered, OrderID: "ord-nobody", SKUs: map[string]int{"SKU-A": 1}})
	wantNotes(t, notes)
	if f.Status != "" || f.AuthorizedMinor != 0 || len(f.TrackingNumbers) != 0 {
		t.Fatalf("facts out of nothing: %+v", f)
	}
	// What the audit makes of it is the audit's business: the order is not DELIVERED.
	if got := rules(retailday.AuditOrder(f)); !slices.Contains(got, "status") {
		t.Fatalf("the audit did not see an order that is not there: %v", got)
	}
}
