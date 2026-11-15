package collect

import (
	"slices"
	"testing"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
)

// The derivation is vendor-connectivity's (ADR 0002 A1, purchaseorder.NumberFor). The literals were
// computed with `printf asg-1:1 | shasum -a 256`, independently of this package: a derivation that
// drifted would read purchase orders that do not exist.
func TestPONumberDerivationIsPinned(t *testing.T) {
	for _, tc := range []struct {
		asg  string
		no   int
		want string
	}{
		{"asg-1", 1, "PO-415a91a42a8b75f0"},
		{"asg-1", 2, "PO-1744e20df4b40fbd"},
		{"asg-2", 1, "PO-772eeb1ba5bfe23b"},
		{"a-9f31", 1, "PO-cbf2f06cc203a85e"},
	} {
		if got := derivePONumber(tc.asg, tc.no); got != tc.want {
			t.Errorf("derivePONumber(%q, %d) = %s, want %s", tc.asg, tc.no, got, tc.want)
		}
		if got := poNumberOf(tc.asg, tc.no); got != tc.want {
			t.Errorf("the fixture's derivation drifted: %s, want %s", got, tc.want)
		}
	}
}

// addRelease gives the order's assignment one more release, and vendor-connectivity the purchase
// order that release implies (state po; "" = vendor-connectivity never issued it).
func (fx *fixture) addRelease(no int, outcome, po string) string {
	fx.w.mu.Lock()
	defer fx.w.mu.Unlock()
	for _, a := range fx.w.sourcing.assignments {
		if a.AssignmentID == fx.asg {
			a.Releases = append(a.Releases, osRelease{ReleaseNo: no, NodeID: "node-2", ReleasedAt: ts(t0.Add(3 * time.Hour)), RequiredShipBy: ts(t0.Add(20 * time.Hour)), Outcome: outcome})
		}
	}
	number := poNumberOf(fx.asg, no)
	if po != "" {
		fx.w.vendor.pos[number] = &vcPO{PONumber: number, VendorID: vendorID, NodeID: "node-2", State: po, IssuedAt: ts(t0), RequiredShipBy: ts(t0.Add(20 * time.Hour)), AckBy: ts(t0.Add(time.Hour)),
			Lines: []vcLine{}, Labels: []any{}, TrackingNumbers: []string{}}
	}
	return number
}

// addAssignment is another group's assignment for the order, with a release (and its PO).
func (fx *fixture) addAssignment(asg string, po string) string {
	fx.w.mu.Lock()
	defer fx.w.mu.Unlock()
	fx.w.sourcing.assignments = append(fx.w.sourcing.assignments, &osAssignment{AssignmentID: asg, OrderID: fx.placed.OrderID, GroupNo: 2, DestinationPostalPrefix: "787", State: "RELEASED", AssignmentVersion: 1,
		Releases: []osRelease{{ReleaseNo: 1, NodeID: "node-1", ReleasedAt: ts(t0), RequiredShipBy: ts(t0.Add(20 * time.Hour)), Outcome: "ACCEPTED"}}, Lines: []foLine{}, ExcludedNodes: []string{}, TrackingNumbers: []string{}})
	number := poNumberOf(asg, 1)
	if po != "" {
		fx.w.vendor.pos[number] = &vcPO{PONumber: number, VendorID: vendorID, NodeID: "node-1", State: po, IssuedAt: ts(t0), RequiredShipBy: ts(t0.Add(20 * time.Hour)), AckBy: ts(t0.Add(time.Hour)),
			Lines: []vcLine{}, Labels: []any{}, TrackingNumbers: []string{}}
	}
	return number
}

func po(number, state string) retailday.PO { return retailday.PO{Number: number, State: state} }

func TestThePurchaseOrderAndTheDeliveryOfAHealthyOrder(t *testing.T) {
	w := newWorld(t)
	fx := seedDelivered(w, "1")
	seedDelivered(w, "2") // somebody else's order: its purchase order is not ours
	f, notes := fx.collect(t, newCollector(w))
	wantNotes(t, notes)
	if want := []retailday.PO{{Number: fx.po, State: "SHIPPED"}}; !slices.Equal(f.POs, want) {
		t.Fatalf("POs %v want %v", f.POs, want)
	}
	if want := t0.Add(30 * time.Hour); !f.DeliveredAt.Equal(want) {
		t.Fatalf("delivered at %s want %s", f.DeliveredAt, want)
	}
	noGets(t, w)
}

// The one-po rule needs to SEE a second purchase order: every release's is read, whichever
// assignment or release carried it, across pages.
func TestEveryPurchaseOrderOfTheOrderIsReported(t *testing.T) {
	w := newWorld(t)
	fx := seedDelivered(w, "1")
	second := fx.addRelease(2, "EXPIRED", "ISSUED")
	third := fx.addAssignment("asg-1b", "ACCEPTED")
	fourth := fx.addAssignment("asg-1c", "REJECTED") // three assignments: the list crosses two pages
	f, notes := fx.collect(t, newCollector(w))
	wantNotes(t, notes)
	want := []retailday.PO{po(fx.po, "SHIPPED"), po(second, "ISSUED"), po(third, "ACCEPTED"), po(fourth, "REJECTED")}
	slices.SortFunc(want, func(a, b retailday.PO) int {
		switch {
		case a.Number < b.Number:
			return -1
		case a.Number > b.Number:
			return 1
		}
		return 0
	})
	if !slices.Equal(f.POs, want) {
		t.Fatalf("POs %v want %v", f.POs, want)
	}
	if got := rules(retailday.AuditOrder(f)); !contains(got, "network/one-po") {
		t.Fatalf("a duplicated purchase order was not seen by the audit: %v", got)
	}
}

func TestAPurchaseOrderIsReadWithTheStorefrontsVendor(t *testing.T) {
	w := newWorld(t)
	fx := seedDelivered(w, "1")
	f, notes := fx.collect(t, newCollector(w, func(s *Settings) { s.VendorID = "V-OTHER" }))
	// The route answers 404 for a purchase order of another vendor, and order-sourcing says the node
	// accepted that release: that contradiction is named, and the purchase order is not invented.
	hasNote(t, notes, "disagree POs", "accepted release 1", fx.po, "V-OTHER")
	if len(f.POs) != 0 {
		t.Fatalf("POs %v", f.POs)
	}
}

// A 404 is "none exists". It is a disagreement only when another context said the thing exists:
// a release the node accepted has a purchase order, a release that was never accepted may not.
func TestA404ForAPurchaseOrderIsNoneUnlessAnotherContextSaysItExists(t *testing.T) {
	w := newWorld(t)
	fx := seedDelivered(w, "1")
	fx.addRelease(2, "WITHDRAWN", "") // never issued: nothing to find, nothing to say
	f, notes := fx.collect(t, newCollector(w))
	wantNotes(t, notes)
	if len(f.POs) != 1 {
		t.Fatalf("POs %v", f.POs)
	}

	w.mu.Lock()
	delete(w.vendor.pos, fx.po)
	w.mu.Unlock()
	f, notes = fx.collect(t, newCollector(w))
	hasNote(t, notes, "disagree POs", "accepted release 1", fx.po, "has no purchase order")
	if len(f.POs) != 0 {
		t.Fatalf("POs %v", f.POs)
	}
	if got := rules(retailday.AuditOrder(f)); !contains(got, "network/one-po") {
		t.Fatalf("audit: %v", got)
	}
}

// A purchase order the derivation missed, found through the journey's own poNumber.
func TestAPurchaseOrderOnlyAJourneyNamesIsAdded(t *testing.T) {
	w := newWorld(t)
	fx := seedDelivered(w, "1")
	w.mu.Lock()
	w.vendor.pos["PO-drifted"] = &vcPO{PONumber: "PO-drifted", VendorID: vendorID, NodeID: "node-1", State: "SHIPPED", IssuedAt: ts(t0), RequiredShipBy: ts(t0), AckBy: ts(t0), Lines: []vcLine{}, Labels: []any{}, TrackingNumbers: []string{}}
	w.tracking.journeys[fx.tn].PONumber = "PO-drifted"
	w.mu.Unlock()
	f, notes := fx.collect(t, newCollector(w))
	wantNotes(t, notes)
	want := []retailday.PO{po(fx.po, "SHIPPED"), po("PO-drifted", "SHIPPED")}
	if !slices.Equal(f.POs, want) {
		t.Fatalf("POs %v want %v", f.POs, want)
	}

	// A journey that names a purchase order vendor-connectivity does not have.
	w.mu.Lock()
	w.tracking.journeys[fx.tn].PONumber = "PO-ghost"
	w.mu.Unlock()
	f, notes = fx.collect(t, newCollector(w))
	hasNote(t, notes, "disagree POs", "the journey of "+fx.tn+" names it", "PO-ghost")
	if len(f.POs) != 1 || f.POs[0].Number != fx.po {
		t.Fatalf("POs %v", f.POs)
	}
}

func TestAPurchaseOrderThatCannotBeReadIsNotCountedAsAbsent(t *testing.T) {
	for _, tc := range []struct {
		name string
		do   func(w *world, fx *fixture)
		note string
	}{
		{"vendor-connectivity answers 500", func(w *world, _ *fixture) { w.fail("vendor-connectivity", 500) },
			"unread POs: vendor-connectivity /vendor/purchase-orders/{poNumber}: HTTP 500 internal-error"},
		{"order-sourcing answers 500", func(w *world, _ *fixture) { w.fail("order-sourcing", 500) },
			"unread POs: order-sourcing /assignments?orderId: HTTP 500 internal-error"},
		{"the vendor answers 429 for one of two", func(w *world, fx *fixture) {
			fx.addRelease(2, "EXPIRED", "ISSUED")
			w.failRoute("vendor-connectivity", "/vendor/purchase-orders/{poNumber}", 429)
		}, "unread POs: vendor-connectivity /vendor/purchase-orders/{poNumber}: HTTP 429"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			fx := seedDelivered(w, "1")
			tc.do(w, fx)
			f, notes := fx.collect(t, newCollector(w))
			hasNote(t, notes, tc.note)
			if len(notes) != 1 {
				t.Fatalf("one failure, one note: %q", notes)
			}
			if len(f.POs) != 0 {
				t.Fatalf("a purchase order that could not be read was counted: %v", f.POs)
			}
			// the rest of the order is still read
			if f.Status != "DELIVERED" || f.CapturedMinor != 4500 || f.DeliveredAt.IsZero() {
				t.Fatalf("the other contexts were not read: %+v", f)
			}
			noGets(t, w)
		})
	}
}

func TestTheDeliveryIsTheLatestAndOnlyWhenEveryPackageArrived(t *testing.T) {
	w := newWorld(t)
	fx := seedDelivered(w, "1")
	// A second package of the order, delivered later.
	tn2 := "RN0000000000000092"
	later := t0.Add(31 * time.Hour)
	w.mu.Lock()
	g := &w.ordering.tracking[fx.placed.OrderID].Groups[0]
	g.Packages = append(g.Packages, foPackage{TrackingNumber: tn2, DestinationKind: "SHIP_TO", State: "DELIVERED"})
	w.sourcing.assignments[0].TrackingNumbers = []string{fx.tn, tn2}
	w.tracking.journeys[tn2] = &stJourney{TrackingNumber: tn2, State: "DELIVERED", PONumber: fx.po, AssignmentID: fx.asg, OrderID: fx.placed.OrderID, Tier: "TWO_DAY", DeliveredAt: ts(later), Scans: []stScan{}}
	w.mu.Unlock()
	f, notes := fx.collect(t, newCollector(w))
	wantNotes(t, notes)
	if !f.DeliveredAt.Equal(later) {
		t.Fatalf("delivered at %s, want the later package's %s", f.DeliveredAt, later)
	}
	if want := []string{fx.tn, tn2}; !slices.Equal(f.TrackingNumbers, want) {
		t.Fatalf("tracking %v", f.TrackingNumbers)
	}

	// One package still moving: the order is not delivered.
	w.mu.Lock()
	w.tracking.journeys[tn2].State = "OUT_FOR_DELIVERY"
	w.tracking.journeys[tn2].DeliveredAt = ""
	w.mu.Unlock()
	f, notes = fx.collect(t, newCollector(w))
	wantNotes(t, notes)
	if !f.DeliveredAt.IsZero() {
		t.Fatalf("delivered at %s with a package out for delivery", f.DeliveredAt)
	}

	// A journey that does not exist yet is "not delivered", not a failure.
	w.mu.Lock()
	delete(w.tracking.journeys, tn2)
	w.mu.Unlock()
	f, notes = fx.collect(t, newCollector(w))
	wantNotes(t, notes)
	if !f.DeliveredAt.IsZero() {
		t.Fatalf("delivered at %s with a package that has no journey", f.DeliveredAt)
	}
}

func TestAJourneyThatCannotBeReadOrContradictsIsNoted(t *testing.T) {
	w := newWorld(t)
	fx := seedDelivered(w, "1")

	w.failRoute("shipping-and-tracking", "/journeys/{trackingNumber}", 500)
	f, notes := fx.collect(t, newCollector(w))
	wantNotes(t, notes, "unread DeliveredAt: shipping-and-tracking /journeys/{trackingNumber}: HTTP 500 internal-error")
	if !f.DeliveredAt.IsZero() || len(f.POs) != 1 {
		t.Fatalf("facts: delivered %s POs %v", f.DeliveredAt, f.POs)
	}
	w.mu.Lock()
	delete(w.routeFails, "shipping-and-tracking GET /journeys/{trackingNumber}")
	w.mu.Unlock()

	w.mu.Lock()
	w.tracking.journeys[fx.tn].OrderID = "ord-other"
	w.mu.Unlock()
	_, notes = fx.collect(t, newCollector(w))
	hasNote(t, notes, "disagree OrderID", "ord-other")
	w.mu.Lock()
	w.tracking.journeys[fx.tn].OrderID = fx.placed.OrderID
	w.tracking.journeys[fx.tn].DeliveredAt = ""
	w.mu.Unlock()
	f, notes = fx.collect(t, newCollector(w))
	hasNote(t, notes, "disagree DeliveredAt", "carries no deliveredAt")
	if !f.DeliveredAt.IsZero() {
		t.Fatalf("delivered at %s", f.DeliveredAt)
	}
}

func TestATrackingNumberOnlySourcingHoldsIsReadAndNoted(t *testing.T) {
	w := newWorld(t)
	fx := seedDelivered(w, "1")
	tn2 := "RN0000000000000093"
	late := t0.Add(40 * time.Hour)
	w.mu.Lock()
	w.sourcing.assignments[0].TrackingNumbers = []string{fx.tn, tn2}
	w.tracking.journeys[tn2] = &stJourney{TrackingNumber: tn2, State: "DELIVERED", PONumber: fx.po, OrderID: fx.placed.OrderID, DeliveredAt: ts(late), Scans: []stScan{}}
	w.mu.Unlock()
	f, notes := fx.collect(t, newCollector(w))
	hasNote(t, notes, "disagree TrackingNumbers", tn2)
	if want := []string{fx.tn}; !slices.Equal(f.TrackingNumbers, want) {
		t.Fatalf("tracking %v: the shopper's own list", f.TrackingNumbers)
	}
	if !f.DeliveredAt.Equal(late) {
		t.Fatalf("the package only sourcing knew was not read: delivered %s", f.DeliveredAt)
	}
}

// A cancelled order has no release, so no purchase order is looked for, and no journey either.
func TestACancelledOrderHasNothingInTheNetwork(t *testing.T) {
	w := newWorld(t)
	seedDelivered(w, "1")
	c := seedCancelled(w, "2")
	w.reset()
	f, notes := c.collect(t, newCollector(w))
	wantNotes(t, notes)
	if len(f.POs) != 0 || !f.DeliveredAt.IsZero() {
		t.Fatalf("facts: %+v", f)
	}
	if n := w.requests("vendor-connectivity", ""); n != 0 {
		t.Fatalf("%d purchase-order reads for an order with no release", n)
	}
	if n := w.requests("shipping-and-tracking", ""); n != 0 {
		t.Fatalf("%d journey reads for an order with no tracking number", n)
	}
}
