package collect

import (
	"testing"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
)

// "Could not look" is not "none exists": a context that fails leaves its facts at zero AND says so,
// while the contexts that answer are still read.
func TestAFailingContextIsNotedAndTheOthersAreStillRead(t *testing.T) {
	w := newWorld(t)
	fx := seedDelivered(w, "1")
	w.failRoute("customer-ordering", "/orders/{orderId}", 500)
	f, notes := fx.collect(t, newCollector(w))
	wantNotes(t, notes, "unread Status, PromiseEnd: customer-ordering /orders/{orderId}: HTTP 500 internal-error")
	if f.Status != "" || !f.PromiseEnd.IsZero() {
		t.Fatalf("a fact that could not be read must be zero: status %q promise %s", f.Status, f.PromiseEnd)
	}
	if f.AuthorizedMinor != 4500 || f.CapturedMinor != 4500 || len(f.TrackingNumbers) != 1 {
		t.Fatalf("the other contexts were not read: %+v", f)
	}
	// The audit sees an order that is not DELIVERED: never a pass.
	if got := rules(retailday.AuditOrder(f)); !contains(got, "status") {
		t.Fatalf("findings %v lack status", got)
	}
	noGets(t, w)
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func TestAFailureIsNotAnAbsence(t *testing.T) {
	for _, tc := range []struct {
		name string
		do   func(w *world)
		note string
	}{
		{"payments answer 500", func(w *world) { w.fail("customer-payments", 500) },
			"unread AuthorizedMinor, CapturedMinor, VoidedMinor, TotalMinor: customer-payments /payment-authorizations?orderId: HTTP 500 internal-error"},
		{"payments refuse with a 503", func(w *world) { w.failRoute("customer-payments", "/payment-authorizations", 503) },
			"customer-payments /payment-authorizations?orderId: HTTP 503 internal-error"},
		{"payments drop the connection", func(w *world) { w.drop("customer-payments") },
			"customer-payments /payment-authorizations?orderId: no usable answer"},
		{"checkout answers 500", func(w *world) { w.fail("checkout", 500) },
			"unread TotalMinor: checkout /checkout-sessions/{sessionId}: HTTP 500 internal-error"},
		{"tracking answers 500", func(w *world) { w.failRoute("customer-ordering", "/orders/{orderId}/tracking", 500) },
			"unread TrackingNumbers: customer-ordering /orders/{orderId}/tracking: HTTP 500 internal-error"},
		{"a refusal that is not a 404", func(w *world) { w.failRoute("customer-ordering", "/orders/{orderId}", 403) },
			"unread Status, PromiseEnd: customer-ordering /orders/{orderId}: HTTP 403 internal-error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			fx := seedDelivered(w, "1")
			tc.do(w)
			f, notes := fx.collect(t, newCollector(w))
			hasNote(t, notes, tc.note)
			if len(notes) != 1 {
				t.Fatalf("one failure, one note: %q", notes)
			}
			switch tc.name[:4] {
			case "paym":
				if f.AuthorizedMinor != 0 || f.CapturedMinor != 0 || f.TotalMinor != 0 {
					t.Fatalf("money that could not be read must be zero: %+v", f)
				}
				if f.Status != "DELIVERED" {
					t.Fatalf("the shopper side was not read: %+v", f)
				}
			case "chec":
				if f.TotalMinor != 0 || f.AuthorizedMinor != 4500 {
					t.Fatalf("total %d authorised %d", f.TotalMinor, f.AuthorizedMinor)
				}
			case "trac":
				if len(f.TrackingNumbers) != 0 || f.Status != "DELIVERED" {
					t.Fatalf("tracking %v status %q", f.TrackingNumbers, f.Status)
				}
			}
			noGets(t, w)
		})
	}
}

// The checkout total is how the order total is known; the shopper's own figure is compared with it
// and never trusted in its place.
func TestTheShoppersFiguresAreComparedNotTrusted(t *testing.T) {
	w := newWorld(t)
	fx := seedDelivered(w, "1")
	c := newCollector(w)

	fx.placed.TotalMinor = 4600
	f, notes := fx.collect(t, c)
	hasNote(t, notes, "disagree TotalMinor", "quoted 4600", "totals 4500")
	if f.TotalMinor != 4500 {
		t.Fatalf("total %d: the checkout's figure, not the shopper's", f.TotalMinor)
	}
	fx.placed.TotalMinor = 4500

	fx.placed.SKUs = map[string]int{"SKU-A": 3, "SKU-B": 1}
	_, notes = fx.collect(t, c)
	hasNote(t, notes, "disagree Lines", "SKU-A x3", "customer-ordering holds {SKU-A x2")
	fx.placed.SKUs = map[string]int{"SKU-A": 2, "SKU-B": 1, "SKU-C": 1}
	_, notes = fx.collect(t, c)
	hasNote(t, notes, "disagree Lines: the shopper bought 3 lines, customer-ordering holds 2")
	fx.placed.SKUs = map[string]int{"SKU-A": 2, "SKU-B": 1}

	fx.placed.AuthorizationID = "pay-other"
	_, notes = fx.collect(t, c)
	hasNote(t, notes, "disagree AuthorizedMinor", "pay-other")
	fx.placed.AuthorizationID = fx.auth

	fx.placed.PromiseEnd = t0.Add(48 * time.Hour)
	_, notes = fx.collect(t, c)
	hasNote(t, notes, "disagree PromiseEnd")
	fx.placed.PromiseEnd = t0.Add(72 * time.Hour)

	_, notes = fx.collect(t, newCollector(w, func(s *Settings) { s.Catalogue = nil }))
	hasNote(t, notes, "unread Lines", "no catalogue")
	_, notes = fx.collect(t, newCollector(w, func(s *Settings) { s.Catalogue = map[string]string{"SKU-A": "CAT-A"} }))
	hasNote(t, notes, "disagree Lines", "CAT-B")
	_, notes = fx.collect(t, c)
	wantNotes(t, notes)
}

// Two authorizations for one order is a double hold: the money is counted, not hidden, and said.
func TestTwoAuthorizationsAreBothCounted(t *testing.T) {
	w := newWorld(t)
	fx := seedDelivered(w, "1")
	w.mu.Lock()
	w.payments.auths = append(w.payments.auths, &cpAuth{AuthorizationID: "pay-zz", CheckoutSessionID: "cs-1", OrderID: "ord-1", State: "AUTHORIZED", Currency: "USD", TotalMinor: 4500,
		Allocations: []cpAllocation{{GroupNo: 1, AmountMinor: 4500, State: "PENDING", TrackingNumbers: []string{}}}})
	w.mu.Unlock()
	f, notes := fx.collect(t, newCollector(w))
	hasNote(t, notes, "disagree AuthorizedMinor", "2 payment authorizations")
	if f.AuthorizedMinor != 9000 || f.CapturedMinor != 4500 {
		t.Fatalf("authorised %d captured %d", f.AuthorizedMinor, f.CapturedMinor)
	}
	if !contains(rules(retailday.AuditOrder(f)), "money/authorised") {
		t.Fatal("the audit did not see the double hold")
	}
}

func TestACheckoutSessionOfAnotherOrderIsNoted(t *testing.T) {
	w := newWorld(t)
	fx := seedDelivered(w, "1")
	w.mu.Lock()
	w.checkout.sessions["cs-1"].OrderID = "ord-other"
	w.checkout.sessions["cs-1"].TotalMinor = 4400
	w.mu.Unlock()
	f, notes := fx.collect(t, newCollector(w))
	hasNote(t, notes, "disagree OrderID", "ord-other")
	hasNote(t, notes, "disagree TotalMinor", "checkout session cs-1 totals 4400, authorization pay-1 4500")
	if f.TotalMinor != 4400 {
		t.Fatalf("total %d", f.TotalMinor)
	}
}
