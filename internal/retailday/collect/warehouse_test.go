package collect

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
)

// healthy is what the collector must return for the fixture's delivered order: every fact, read.
func (fx *fixture) healthy() retailday.OrderFacts {
	return retailday.OrderFacts{
		OrderID: fx.placed.OrderID, Expect: retailday.Delivered, Lines: 2, Status: "DELIVERED",
		TotalMinor: 4500, AuthorizedMinor: 4500, CapturedMinor: 4500,
		POs:      []retailday.PO{{Number: fx.po, State: "SHIPPED"}},
		OMOrders: []retailday.OMOrder{{ID: fx.om, Status: "Shipped"}}, WorkUnits: 2, PickTasksDone: 2, PackTasks: 1,
		Packages: []retailday.Package{{ID: fx.pkg, Outcome: "LABELED"}}, ShipmentConfirmations: 1,
		TrackingNumbers: []string{fx.tn}, DeliveredAt: t0.Add(30 * time.Hour), PromiseEnd: t0.Add(72 * time.Hour),
	}
}

func sameFacts(t *testing.T, got, want retailday.OrderFacts) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("facts:\n got %+v\nwant %+v", got, want)
	}
}

// addOM gives the customer order one more order-management order (linked), with its own units,
// tasks and package.
func (fx *fixture) addOM(n string) string {
	om := "om-" + n
	w := fx.w
	w.mu.Lock()
	defer w.mu.Unlock()
	w.oms.orders[om] = &omOrder{ID: om, Status: "Released", FulfillmentSiteID: site,
		Lines: []omLine{{1, "SKU-A", 2, "pick", false, "Released", "r1"}, {2, "SKU-B", 1, "pick", false, "Released", "r2"}}}
	u1, u2 := om+"-line-1", om+"-line-2"
	w.wes.units = append(w.wes.units,
		&wesUnit{ID: u1, PathID: "PICK", Reference: om, State: "Completed", SKU: "SKU-A", LineNo: 1},
		&wesUnit{ID: u2, PathID: "PICK", Reference: om, State: "Completed", SKU: "SKU-B", LineNo: 2})
	w.fe.tasks = append(w.fe.tasks,
		&feTask{ID: "tk-pick-1-" + n, Type: "PICK", Status: "COMPLETED", OrderRef: u1},
		&feTask{ID: "tk-pick-2-" + n, Type: "PICK", Status: "COMPLETED", OrderRef: u2},
		&feTask{ID: "tk-pack-" + n, Type: "PACK", Status: "COMPLETED", OrderRef: om})
	w.fe.packages = append(w.fe.packages, &fePackage{ID: "pkg-" + n, OrderRef: om, Status: "LABELED", ScannedContents: []string{}, SortLane: "STANDARD"})
	w.links[fx.placed.OrderID] = append(w.links[fx.placed.OrderID], om)
	return om
}

// addLoad adds a load (the newest) with its packages.
func (fx *fixture) addLoad(id, state, siteID string, version int, pkgs ...*obPackage) {
	fx.w.mu.Lock()
	defer fx.w.mu.Unlock()
	fx.w.outbound.loads = append(fx.w.outbound.loads, &obLoad{ID: id, SiteID: siteID, CPTID: "aus-1700", OperatingDate: "2026-11-14", ShipMethod: "GROUND", Cutoff: ts(t0), State: state, PackageCount: len(pkgs), Version: version})
	fx.w.outbound.packages[id] = pkgs
}

func TestAHealthyDeliveredOrderIsReadWholeAndTheAuditFindsNothing(t *testing.T) {
	w := newWorld(t)
	fx := seedDelivered(w, "1")
	seedDelivered(w, "2")
	seedCancelled(w, "3")
	f, notes := fx.collect(t, newCollector(w))
	wantNotes(t, notes)
	sameFacts(t, f, fx.healthy())
	if fs := retailday.AuditOrder(f); len(fs) != 0 {
		t.Fatalf("a healthy order has findings: %+v", fs)
	}
	noGets(t, w)
}

func TestACancelledOrderLeavesNothingDownstreamAndItsHoldIsReleased(t *testing.T) {
	w := newWorld(t)
	seedDelivered(w, "1")
	c := seedCancelled(w, "2")
	w.reset()
	f, notes := c.collect(t, newCollector(w))
	wantNotes(t, notes)
	want := retailday.OrderFacts{OrderID: "ord-2", Expect: retailday.Cancelled, Lines: 1, Status: "CANCELLED", TotalMinor: 1200, AuthorizedMinor: 1200, VoidedMinor: 1200}
	sameFacts(t, f, want)
	if fs := retailday.AuditOrder(f); len(fs) != 0 {
		t.Fatalf("a clean cancellation has findings: %+v", fs)
	}
	// The link answered with no order-management order: nothing is read by its id.
	for _, svc := range []string{"order-management", "wes-work-planning", "fulfillment-execution", "outbound-shipping"} {
		if n := w.requests(svc, ""); n != 0 {
			t.Fatalf("%d requests to %s for an order that has no warehouse order", n, svc)
		}
	}
	noGets(t, w)

	// A cancelled order whose hold was not released is a finding, not a pass.
	w.mu.Lock()
	w.payments.auths[len(w.payments.auths)-1].Allocations[0].State = "PENDING"
	w.mu.Unlock()
	f, _ = c.collect(t, newCollector(w))
	if got := rules(retailday.AuditOrder(f)); !contains(got, "money/released") {
		t.Fatalf("a hold that stayed: %v", got)
	}
}

// Every package the order has is reported with the outcome fulfillment-execution holds.
func TestADivertedPackageIsReportedAsDiverted(t *testing.T) {
	w := newWorld(t)
	fx := seedDelivered(w, "1")
	w.mu.Lock()
	w.fe.packages[0].Status = "DIVERTED"
	w.fe.packages = append(w.fe.packages, &fePackage{ID: "pkg-1b", OrderRef: fx.om, Status: "SEALED", ScannedContents: []string{}, SortLane: "STANDARD"})
	w.mu.Unlock()
	f, notes := fx.collect(t, newCollector(w))
	wantNotes(t, notes)
	want := []retailday.Package{{ID: "pkg-1", Outcome: "DIVERTED"}, {ID: "pkg-1b", Outcome: "SEALED"}}
	if !slices.Equal(f.Packages, want) {
		t.Fatalf("packages %v want %v", f.Packages, want)
	}
	if got := rules(retailday.AuditOrder(f)); !contains(got, "warehouse/labeled") || contains(got, "warehouse/package") {
		t.Fatalf("findings %v", got)
	}
}

func TestTwoOrderManagementOrdersAreBothReportedAndEveryCountAddsUp(t *testing.T) {
	w := newWorld(t)
	fx := seedDelivered(w, "1")
	second := fx.addOM("1b")
	f, notes := fx.collect(t, newCollector(w))
	wantNotes(t, notes)
	want := []retailday.OMOrder{{ID: fx.om, Status: "Shipped"}, {ID: second, Status: "Released"}}
	if !slices.Equal(f.OMOrders, want) {
		t.Fatalf("orders %v want %v", f.OMOrders, want)
	}
	if f.WorkUnits != 4 || f.PickTasksDone != 4 || f.PackTasks != 2 || len(f.Packages) != 2 {
		t.Fatalf("totals over both orders: units %d picks %d packs %d packages %v", f.WorkUnits, f.PickTasksDone, f.PackTasks, f.Packages)
	}
	got := rules(retailday.AuditOrder(f))
	for _, r := range []string{"warehouse/one-order", "warehouse/work-units", "warehouse/one-pack"} {
		if !contains(got, r) {
			t.Fatalf("findings %v lack %s", got, r)
		}
	}
}

// An id the link returns twice is one order, and the empty id is no order.
func TestALinkThatRepeatsAnIdNamesOneOrder(t *testing.T) {
	w := newWorld(t)
	fx := seedDelivered(w, "1")
	w.linkOrders(fx.placed.OrderID, fx.om, "", fx.om)
	f, notes := fx.collect(t, newCollector(w))
	wantNotes(t, notes)
	sameFacts(t, f, fx.healthy())
}

func TestWithoutTheLinkEveryWarehouseFactIsZeroAndNamed(t *testing.T) {
	w := newWorld(t)
	fx := seedDelivered(w, "1")
	w.reset()
	f, notes := fx.collect(t, newCollector(w, func(s *Settings) { s.OrderIDs = nil }))
	hasNote(t, notes, "unread OMOrders, WorkUnits, PickTasksDone, PackTasks, Packages, ShipmentConfirmations", "Settings.OrderIDs is not set", "ord-1")
	if len(notes) != 1 {
		t.Fatalf("notes %q", notes)
	}
	want := fx.healthy()
	want.OMOrders, want.WorkUnits, want.PickTasksDone, want.PackTasks, want.Packages, want.ShipmentConfirmations = nil, 0, 0, 0, nil, 0
	sameFacts(t, f, want)
	for _, svc := range []string{"order-management", "wes-work-planning", "fulfillment-execution", "outbound-shipping"} {
		if n := w.requests(svc, ""); n != 0 {
			t.Fatalf("%d requests to %s without a link", n, svc)
		}
	}
	// What was readable without it was read.
	if len(f.POs) != 1 || f.Status != "DELIVERED" || f.DeliveredAt.IsZero() {
		t.Fatalf("facts %+v", f)
	}
}

func TestALinkThatFailsOrPanicsIsNotedAndNeverPropagates(t *testing.T) {
	for _, tc := range []struct {
		name string
		link OrderIDs
		note string
	}{
		{"an error", func(_ context.Context, _ retailday.Placed, _ []string) ([]string, error) {
			return nil, errors.New("tap offline")
		}, "the link to order-management failed: tap offline"},
		{"a panic", func(_ context.Context, _ retailday.Placed, _ []string) ([]string, error) { panic("boom") }, "the link to order-management panicked: boom"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			fx := seedDelivered(w, "1")
			f, notes := fx.collect(t, newCollector(w, func(s *Settings) { s.OrderIDs = tc.link }))
			hasNote(t, notes, "unread OMOrders, WorkUnits, PickTasksDone, PackTasks, Packages, ShipmentConfirmations", tc.note)
			if f.OMOrders != nil || f.WorkUnits != 0 || f.ShipmentConfirmations != 0 || f.Status != "DELIVERED" {
				t.Fatalf("facts %+v", f)
			}
		})
	}
}

// The link is asked about the order's own purchase orders, in a fixed order, even when
// vendor-connectivity could not be read: the numbers are known, only their state is not.
func TestTheLinkIsGivenTheOrdersPurchaseOrderNumbers(t *testing.T) {
	w := newWorld(t)
	fx := seedDelivered(w, "1")
	second := fx.addRelease(2, "EXPIRED", "ISSUED")
	var gotP retailday.Placed
	var gotNumbers []string
	link := func(_ context.Context, p retailday.Placed, numbers []string) ([]string, error) {
		gotP, gotNumbers = p, numbers
		return []string{fx.om}, nil
	}
	want := []string{fx.po, second}
	slices.Sort(want)

	fx.collect(t, newCollector(w, func(s *Settings) { s.OrderIDs = link }))
	if !slices.Equal(gotNumbers, want) || gotP.OrderID != fx.placed.OrderID || gotP.RunID != "RUN1" {
		t.Fatalf("link got %+v %v, want %v", gotP, gotNumbers, want)
	}
	w.fail("vendor-connectivity", 500)
	fx.collect(t, newCollector(w, func(s *Settings) { s.OrderIDs = link }))
	if !slices.Equal(gotNumbers, want) {
		t.Fatalf("with vendor-connectivity down the link got %v, want %v", gotNumbers, want)
	}
}

// A 404 is "none exists", everywhere downstream: no note, and the fact is zero. The one
// exception, by design, is a 404 that contradicts another context (tested where it happens).
func TestA404DownstreamIsNoneAndNotANote(t *testing.T) {
	for _, tc := range []struct {
		svc, route string
		zero       func(*retailday.OrderFacts)
	}{
		{"customer-ordering", "/orders/{orderId}", func(f *retailday.OrderFacts) { f.Status = ""; f.PromiseEnd = time.Time{} }},
		{"customer-ordering", "/orders/{orderId}/tracking", func(f *retailday.OrderFacts) { f.TrackingNumbers = nil }},
		{"customer-payments", "/payment-authorizations", func(f *retailday.OrderFacts) { f.TotalMinor, f.AuthorizedMinor, f.CapturedMinor = 0, 0, 0 }},
		{"checkout", "/checkout-sessions/{sessionId}", func(f *retailday.OrderFacts) { f.TotalMinor = 0 }},
		// order-sourcing knows nothing of the order, yet the journey names the purchase order: it is found.
		{"order-sourcing", "/assignments", func(f *retailday.OrderFacts) {}},
		{"shipping-and-tracking", "/journeys/{trackingNumber}", func(f *retailday.OrderFacts) { f.DeliveredAt = time.Time{} }},
		{"wes-work-planning", "/work-units", func(f *retailday.OrderFacts) { f.WorkUnits, f.PickTasksDone = 0, 0 }},
		{"fulfillment-execution", "/tasks", func(f *retailday.OrderFacts) { f.PickTasksDone, f.PackTasks = 0, 0 }},
		{"fulfillment-execution", "/packages", func(f *retailday.OrderFacts) { f.Packages = nil }},
		{"outbound-shipping", "/loads", func(f *retailday.OrderFacts) { f.ShipmentConfirmations = 0 }},
		{"outbound-shipping", "/loads/{loadId}/packages", func(f *retailday.OrderFacts) { f.ShipmentConfirmations = 0 }},
	} {
		t.Run(tc.svc+tc.route, func(t *testing.T) {
			w := newWorld(t)
			fx := seedDelivered(w, "1")
			w.failRoute(tc.svc, tc.route, 404)
			f, notes := fx.collect(t, newCollector(w))
			wantNotes(t, notes)
			want := fx.healthy()
			tc.zero(&want)
			sameFacts(t, f, want)
		})
	}
}

// A 404 for an order-management order the link itself named contradicts the link.
func TestAnOrderTheLinkNamesButOrderManagementDoesNotHaveIsADisagreement(t *testing.T) {
	w := newWorld(t)
	fx := seedDelivered(w, "1")
	w.failRoute("order-management", "/orders/{id}", 404)
	f, notes := fx.collect(t, newCollector(w))
	hasNote(t, notes, "disagree OMOrders", "order-management order "+fx.om)
	if len(notes) != 1 || f.OMOrders != nil || f.WorkUnits != 2 {
		t.Fatalf("notes %q facts %+v", notes, f)
	}
}

// One context failing leaves its facts zero and says so; every other context is still read.
func TestAFailureInOneContextLeavesTheOthersRead(t *testing.T) {
	for _, tc := range []struct {
		name  string
		do    func(w *world)
		zero  func(*retailday.OrderFacts)
		notes []string
	}{
		{"order-management", func(w *world) { w.fail("order-management", 500) },
			func(f *retailday.OrderFacts) { f.OMOrders = nil },
			[]string{"unread OMOrders: order-management /orders/{id}: HTTP 500 internal-error"}},
		{"wes-work-planning", func(w *world) { w.fail("wes-work-planning", 503) },
			func(f *retailday.OrderFacts) { f.WorkUnits, f.PickTasksDone = 0, 0 },
			[]string{"unread WorkUnits, PickTasksDone: wes-work-planning /work-units?reference: HTTP 503 internal-error"}},
		{"fulfillment-execution tasks", func(w *world) { w.failRoute("fulfillment-execution", "/tasks", 500) },
			func(f *retailday.OrderFacts) { f.PickTasksDone, f.PackTasks = 0, 0 },
			[]string{"unread PackTasks: fulfillment-execution /tasks?orderRef: HTTP 500 internal-error", "unread PickTasksDone: fulfillment-execution /tasks?orderRef: HTTP 500 internal-error"}},
		{"fulfillment-execution packages", func(w *world) { w.failRoute("fulfillment-execution", "/packages", 500) },
			func(f *retailday.OrderFacts) { f.Packages = nil },
			[]string{"unread Packages: fulfillment-execution /packages?orderRef: HTTP 500 internal-error"}},
		{"outbound-shipping loads", func(w *world) { w.failRoute("outbound-shipping", "/loads", 500) },
			func(f *retailday.OrderFacts) { f.ShipmentConfirmations = 0 },
			[]string{"unread ShipmentConfirmations: outbound-shipping /loads: HTTP 500 internal-error"}},
		{"outbound-shipping load packages", func(w *world) { w.failRoute("outbound-shipping", "/loads/{loadId}/packages", 500) },
			func(f *retailday.OrderFacts) { f.ShipmentConfirmations = 0 },
			[]string{"unread ShipmentConfirmations: outbound-shipping /loads/{loadId}/packages: HTTP 500 internal-error"}},
		{"a dropped connection", func(w *world) { w.drop("wes-work-planning") },
			func(f *retailday.OrderFacts) { f.WorkUnits, f.PickTasksDone = 0, 0 },
			[]string{"unread WorkUnits, PickTasksDone: wes-work-planning /work-units?reference: no usable answer"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			fx := seedDelivered(w, "1")
			tc.do(w)
			f, notes := fx.collect(t, newCollector(w))
			if len(notes) != len(tc.notes) {
				t.Fatalf("notes %q want %q", notes, tc.notes)
			}
			for i := range notes {
				if !hasPrefixFold(notes[i], tc.notes[i]) {
					t.Fatalf("note %d: %q want prefix %q", i, notes[i], tc.notes[i])
				}
			}
			want := fx.healthy()
			tc.zero(&want)
			sameFacts(t, f, want)
			noGets(t, w)
		})
	}
}

func hasPrefixFold(s, prefix string) bool { return len(s) >= len(prefix) && s[:len(prefix)] == prefix }

// A failure of one part of one order-management order makes the fact zero as a whole: a total that
// is missing a leg would pass for a smaller one.
func TestAPartThatCannotBeReadZeroesTheWholeFact(t *testing.T) {
	w := newWorld(t)
	fx := seedDelivered(w, "1")
	fx.addOM("1b")
	w.failWhen("fulfillment-execution", func(r *http.Request) bool { return r.URL.Query().Get("orderRef") == "om-1b-line-2" }, 500)
	f, notes := fx.collect(t, newCollector(w))
	hasNote(t, notes, "unread PickTasksDone", "fulfillment-execution /tasks?orderRef: HTTP 500")
	if len(notes) != 1 {
		t.Fatalf("notes %q", notes)
	}
	if f.PickTasksDone != 0 {
		t.Fatalf("picks %d: three of the four picks were read, and that count passes for a smaller order", f.PickTasksDone)
	}
	if f.WorkUnits != 4 || f.PackTasks != 2 || len(f.Packages) != 2 || len(f.OMOrders) != 2 {
		t.Fatalf("what was readable was not read: %+v", f)
	}
}

func TestOnlyCompletedPicksCountAndACancelledUnitIsNotWork(t *testing.T) {
	w := newWorld(t)
	fx := seedDelivered(w, "1")
	w.mu.Lock()
	for _, tk := range w.fe.tasks {
		if tk.ID == "tk-pick-2-1" {
			tk.Status = "CLAIMED"
		}
	}
	// A third unit that was cancelled, a RELOCATE task on a unit's ref, a PACK task of another order.
	w.wes.units = append(w.wes.units, &wesUnit{ID: "om-1-line-3", PathID: "PICK", Reference: "om-1", State: "Cancelled", SKU: "SKU-C", LineNo: 3})
	w.fe.tasks = append(w.fe.tasks,
		&feTask{ID: "tk-reloc", Type: "RELOCATE", Status: "COMPLETED", OrderRef: "om-1-line-1"},
		&feTask{ID: "tk-pick-3", Type: "PICK", Status: "COMPLETED", OrderRef: "om-1-line-3"},
		&feTask{ID: "tk-pack-other", Type: "PACK", Status: "COMPLETED", OrderRef: "om-other"},
		// the order's own SLAM task shares its ref with the PACK task and is not a pack
		&feTask{ID: "tk-slam", Type: "SLAM", Status: "COMPLETED", OrderRef: "om-1"})
	w.mu.Unlock()
	f, notes := fx.collect(t, newCollector(w))
	wantNotes(t, notes)
	if f.WorkUnits != 2 {
		t.Fatalf("work units %d: a cancelled unit is not work", f.WorkUnits)
	}
	if f.PickTasksDone != 1 {
		t.Fatalf("picks done %d: a CLAIMED pick is not done, a RELOCATE is not a pick", f.PickTasksDone)
	}
	if f.PackTasks != 1 {
		t.Fatalf("pack tasks %d", f.PackTasks)
	}
	if got := rules(retailday.AuditOrder(f)); !contains(got, "warehouse/picks") {
		t.Fatalf("findings %v", got)
	}

	// A second PACK task for the order is reported, so the one-pack rule can fire.
	w.mu.Lock()
	w.fe.tasks = append(w.fe.tasks, &feTask{ID: "tk-pack-dup", Type: "PACK", Status: "PENDING", OrderRef: "om-1"})
	w.mu.Unlock()
	f, _ = fx.collect(t, newCollector(w))
	if f.PackTasks != 2 {
		t.Fatalf("pack tasks %d", f.PackTasks)
	}
}

func TestSiteOfTheOrderManagementOrderIsComparedWithTheStorefrontsBuilding(t *testing.T) {
	w := newWorld(t)
	fx := seedDelivered(w, "1")
	w.mu.Lock()
	w.oms.orders["om-1"].FulfillmentSiteID = "SIM1"
	w.mu.Unlock()
	_, notes := fx.collect(t, newCollector(w))
	hasNote(t, notes, "disagree OMOrders", "fulfilled at SIM1", "served from AUS3")
	w.mu.Lock()
	w.oms.orders["om-1"].FulfillmentSiteID = ""
	w.mu.Unlock()
	_, notes = fx.collect(t, newCollector(w))
	hasNote(t, notes, "disagree OMOrders", "no fulfillmentSiteId")
	// Without a building configured the site is not judged.
	_, notes = fx.collect(t, newCollector(w, func(s *Settings) { s.BuildingSite = "" }))
	wantNotes(t, notes)
	// The lines of the warehouse order are compared with what the shopper bought.
	w.mu.Lock()
	w.oms.orders["om-1"].FulfillmentSiteID = site
	w.oms.orders["om-1"].Lines[0].Quantity = 1
	w.mu.Unlock()
	_, notes = fx.collect(t, newCollector(w))
	hasNote(t, notes, "disagree Lines", "order-management order om-1 holds {SKU-A x1, SKU-B x1}")
}

func TestShipmentConfirmationsAreLoadsThatLeftCarryingTheOrder(t *testing.T) {
	w := newWorld(t)
	fx := seedDelivered(w, "1")
	// A fresh collector per count: a load that left is remembered, and this test changes loads.
	count := func() int {
		f, notes := fx.collect(t, newCollector(w))
		wantNotes(t, notes)
		return f.ShipmentConfirmations
	}
	if n := count(); n != 1 {
		t.Fatalf("one load left with the order: %d", n)
	}
	// Two packages of the order on one load is still one ShipmentConfirmed.
	w.mu.Lock()
	w.outbound.packages[fx.load] = append(w.outbound.packages[fx.load], &obPackage{ID: "pkg-1b", SiteID: site, SourceOrderID: fx.om, LoadID: fx.load, State: "SHIPPED", Version: 2})
	w.mu.Unlock()
	if n := count(); n != 1 {
		t.Fatalf("two packages on one load are one confirmation: %d", n)
	}
	// A package that did not ship (or was short-shipped) is not a confirmation.
	w.mu.Lock()
	for _, p := range w.outbound.packages[fx.load] {
		if p.SourceOrderID == fx.om {
			p.State = "SHORT_SHIPPED"
		}
	}
	w.mu.Unlock()
	if n := count(); n != 0 {
		t.Fatalf("a short-shipped package confirmed the order: %d", n)
	}
	w.mu.Lock()
	for _, p := range w.outbound.packages[fx.load] {
		p.State = "SHIPPED"
	}
	w.mu.Unlock()
	// A load that has not left, or one at another site, does not count.
	// The load state is the guard (ADR 0002): even a SHIPPED package on a load that is not SHIP_CONFIRMED proves nothing.
	fx.addLoad("load-closed", "CLOSED", site, 3, &obPackage{ID: "p-c", SiteID: site, SourceOrderID: fx.om, State: "SHIPPED"})
	fx.addLoad("load-elsewhere", "SHIP_CONFIRMED", "SIM1", 3, &obPackage{ID: "p-e", SiteID: "SIM1", SourceOrderID: fx.om, State: "SHIPPED"})
	if n := count(); n != 1 {
		t.Fatalf("a load that has not left, or at another site, counted: %d", n)
	}
	// The same order on a second load that left is two confirmations: the rule must see it.
	fx.addLoad("load-again", "SHIP_CONFIRMED", site, 3, &obPackage{ID: "p-a", SiteID: site, SourceOrderID: fx.om, State: "SHIPPED"})
	f, _ := fx.collect(t, newCollector(w))
	if f.ShipmentConfirmations != 2 {
		t.Fatalf("two loads carried the order: %d", f.ShipmentConfirmations)
	}
	if got := rules(retailday.AuditOrder(f)); !contains(got, "warehouse/shipment-confirmed") {
		t.Fatalf("findings %v", got)
	}
}

func TestTheShipmentScanIsBoundedAndSaysWhenItWasCutShort(t *testing.T) {
	w := newWorld(t)
	fx := seedDelivered(w, "1") // its load is the second oldest of the site's two
	for _, id := range []string{"l-a", "l-b", "l-c", "l-d"} {
		fx.addLoad(id, "SHIP_CONFIRMED", site, 3, &obPackage{ID: "p-" + id, SiteID: site, SourceOrderID: "om-x-" + id, State: "SHIPPED"})
	}
	f, notes := fx.collect(t, newCollector(w, func(s *Settings) { s.MaxLoads = 3 }))
	hasNote(t, notes, "unread ShipmentConfirmations", "more than 3 SHIP_CONFIRMED loads", "the newest 3")
	if f.ShipmentConfirmations != 0 {
		t.Fatalf("a scan that did not reach the order's load counted %d", f.ShipmentConfirmations)
	}
	if n := w.requests("outbound-shipping", "/loads/load-1/packages"); n != 0 {
		t.Fatalf("read a load beyond the bound")
	}
	// A bound that covers every load is not "cut short", even when it is exact.
	f, notes = fx.collect(t, newCollector(w, func(s *Settings) { s.MaxLoads = 6 }))
	wantNotes(t, notes)
	if f.ShipmentConfirmations != 1 {
		t.Fatalf("confirmations %d", f.ShipmentConfirmations)
	}
}

// A load that left cannot change, so a run of many orders reads its packages once; a load whose
// version moved is read again.
func TestPackagesOfALoadThatLeftAreReadOncePerVersion(t *testing.T) {
	w := newWorld(t)
	fx := seedDelivered(w, "1")
	other := seedDelivered(w, "2")
	c := newCollector(w)
	fx.collect(t, c)
	other.collect(t, c)
	if n := w.requests("outbound-shipping", "/loads/load-1/packages"); n != 1 {
		t.Fatalf("load-1 packages read %d times by two orders", n)
	}
	w.mu.Lock()
	for _, l := range w.outbound.loads {
		if l.ID == "load-1" {
			l.Version++
		}
	}
	w.mu.Unlock()
	fx.collect(t, c)
	if n := w.requests("outbound-shipping", "/loads/load-1/packages"); n != 2 {
		t.Fatalf("a load whose version moved was not read again: %d", n)
	}
	// Each collector has its own memory.
	fx.collect(t, newCollector(w))
	if n := w.requests("outbound-shipping", "/loads/load-1/packages"); n != 3 {
		t.Fatalf("%d", n)
	}
}

func TestAnOrderIsFollowedThroughAllItsContextsWithOnlyGETs(t *testing.T) {
	w := newWorld(t)
	fx := seedDelivered(w, "1")
	w.reset()
	fx.collect(t, newCollector(w))
	want := []string{
		"checkout /checkout-sessions/cs-1",
		"customer-ordering /orders/ord-1",
		"customer-ordering /orders/ord-1/tracking",
		"customer-payments /payment-authorizations",
		"fulfillment-execution /packages",
		"fulfillment-execution /tasks",
		"order-management /orders/om-1",
		"order-sourcing /assignments",
		"outbound-shipping /loads",
		"outbound-shipping /loads/load-1/packages",
		"outbound-shipping /loads/load-old-1/packages",
		"shipping-and-tracking /journeys/" + fx.tn,
		"vendor-connectivity /vendor/purchase-orders/" + fx.po,
		"wes-work-planning /work-units",
	}
	if got := w.reads(); !slices.Equal(got, want) {
		t.Fatalf("reads:\n got %q\nwant %q", got, want)
	}
	for _, c := range w.snapshot() {
		if c.Status >= 500 || c.Status == 405 {
			t.Fatalf("a request the fakes refused or failed: %+v", c)
		}
	}
	noGets(t, w)
}
