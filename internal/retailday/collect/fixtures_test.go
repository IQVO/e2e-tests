package collect

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
)

// t0 is when the fixture orders were placed (21:00 UTC, 15:00 America/Chicago).
var t0 = time.Date(2026, 11, 14, 21, 0, 0, 0, time.UTC)

const (
	vendorID = "V-WS"
	site     = "AUS3"
)

func ts(t time.Time) string { return t.UTC().Format(time.RFC3339) }

// poNumberOf is vendor-connectivity's NumberFor (ADR 0002 A1): "PO-" + the first 16 hex digits
// of sha256(assignmentId + ":" + releaseNo). TestPONumberDerivationIsPinned holds it to literals.
func poNumberOf(assignmentID string, releaseNo int) string {
	sum := sha256.Sum256([]byte(assignmentID + ":" + strconv.Itoa(releaseNo)))
	return "PO-" + hex.EncodeToString(sum[:])[:16]
}

// fixture is one seeded order and the ids the seeding minted for it.
type fixture struct {
	w      *world
	n      string
	placed retailday.Placed

	om, tn, po, asg, session, auth, pkg, load string
	units                                     []string
}

// seedLinks is the link the tests give the collector in place of the runner's: customer order id ->
// order-management ids. It lives on the world so a test can break it.
func (w *world) linkOrders(orderID string, ids ...string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.links == nil {
		w.links = map[string][]string{}
	}
	w.links[orderID] = ids
}

func (w *world) orderIDs(_ context.Context, p retailday.Placed, _ []string) ([]string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.links[p.OrderID]...), nil
}

func catalogue() map[string]string {
	return map[string]string{"SKU-A": "CAT-A", "SKU-B": "CAT-B", "SKU-C": "CAT-C"}
}

// newCollector is a collector over the world's fakes with the order link wired.
func newCollector(w *world, mod ...func(*Settings)) *Collector {
	s := Settings{Catalogue: catalogue(), BuildingSite: site, OrderIDs: w.orderIDs}
	for _, m := range mod {
		m(&s)
	}
	return New(w.client(), s)
}

// seedDelivered seeds a healthy delivered order: two lines (SKU-A x2, SKU-B x1), authorised and
// captured once for 4500, one purchase order, one order-management order Shipped, two work
// units, two PICK tasks done, one PACK task, one LABELED package on a SHIP_CONFIRMED load that
// also carries somebody else's package, one tracking number DELIVERED before the promise ended.
// n is the suffix that keeps ids of different orders apart.
func seedDelivered(w *world, n string) *fixture {
	fx := &fixture{w: w, n: n,
		om: "om-" + n, tn: "RN" + fmt.Sprintf("%016s", n), asg: "asg-" + n, session: "cs-" + n,
		auth: "pay-" + n, pkg: "pkg-" + n, load: "load-" + n,
	}
	orderID := "ord-" + n
	fx.po = poNumberOf(fx.asg, 1)
	fx.units = []string{fx.om + "-line-1", fx.om + "-line-2"}
	promiseEnd := t0.Add(72 * time.Hour)
	delivered := t0.Add(30 * time.Hour)

	fx.placed = retailday.Placed{
		RunID: "RUN1", Expect: retailday.Delivered, OrderID: orderID,
		SKUs: map[string]int{"SKU-A": 2, "SKU-B": 1}, TotalMinor: 4500, AuthorizationID: fx.auth,
		PromiseEnd: promiseEnd, PlacedAt: t0,
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	w.ordering.orders[orderID] = &foOrder{OrderID: orderID, ShopperID: "shopper-" + n, Status: "DELIVERED", DestinationPostalPrefix: "787", PlacedAt: ts(t0),
		Lines: []foLine{{1, "CAT-A", 2}, {2, "CAT-B", 1}},
		Groups: []foGroup{{GroupNo: 1, PromiseID: "prm-" + n, Tier: "TWO_DAY", ShipBy: ts(t0.Add(20 * time.Hour)), DeliverByEarliest: ts(t0.Add(48 * time.Hour)),
			DeliverByLatest: ts(promiseEnd), TimeZone: "America/Chicago", LineNos: []int{1, 2}, Status: "DELIVERED"}}}
	w.ordering.tracking[orderID] = &foTracking{OrderID: orderID, Status: "DELIVERED", Groups: []foTrackGroup{{GroupNo: 1, PromiseID: "prm-" + n, Status: "DELIVERED",
		DeliverByEarliest: ts(t0.Add(48 * time.Hour)), DeliverByLatest: ts(promiseEnd),
		Packages: []foPackage{{TrackingNumber: fx.tn, DestinationKind: "SHIP_TO", Carrier: "RN-NET", State: "DELIVERED"}}}}}

	w.payments.auths = append(w.payments.auths, &cpAuth{AuthorizationID: fx.auth, CheckoutSessionID: fx.session, OrderID: orderID, State: "CAPTURED", Currency: "USD", TotalMinor: 4500,
		Allocations:             []cpAllocation{{GroupNo: 1, AmountMinor: 4500, State: "CAPTURED", TrackingNumber: fx.tn, TrackingNumbers: []string{fx.tn}}},
		ProviderAuthorizationID: "hold_" + n})
	w.checkout.sessions[fx.session] = &coSession{SessionID: fx.session, State: "COMPLETED", CartID: "cart-" + n, ShopperID: "shopper-" + n, Currency: "USD", TotalMinor: 4500, AuthorizationID: fx.auth, OrderID: orderID}

	w.sourcing.assignments = append(w.sourcing.assignments, &osAssignment{AssignmentID: fx.asg, OrderID: orderID, GroupNo: 1, DestinationPostalPrefix: "787", State: "DELIVERED", AssignmentVersion: 3,
		Promise:         osPromise{PromiseID: "prm-" + n, Tier: "TWO_DAY", DeliverByLatest: ts(promiseEnd), WitnessNodeID: "node-1"},
		Lines:           []foLine{{1, "CAT-A", 2}, {2, "CAT-B", 1}},
		Releases:        []osRelease{{ReleaseNo: 1, NodeID: "node-1", ReleasedAt: ts(t0.Add(time.Hour)), RequiredShipBy: ts(t0.Add(20 * time.Hour)), Outcome: "ACCEPTED"}},
		ExcludedNodes:   []string{},
		TrackingNumbers: []string{fx.tn}})
	w.vendor.pos[fx.po] = &vcPO{PONumber: fx.po, VendorID: vendorID, NodeID: "node-1", State: "SHIPPED", IssuedAt: ts(t0.Add(time.Hour)), RequiredShipBy: ts(t0.Add(20 * time.Hour)), AckBy: ts(t0.Add(2 * time.Hour)),
		Lines: []vcLine{{1, "lst-a", 2, []any{}}, {2, "lst-b", 1, []any{}}}, Labels: []any{}, TrackingNumbers: []string{fx.tn}}

	w.tracking.journeys[fx.tn] = &stJourney{TrackingNumber: fx.tn, State: "DELIVERED", PONumber: fx.po, AssignmentID: fx.asg, OrderID: orderID, Tier: "TWO_DAY", DeliveredAt: ts(delivered),
		Scans: []stScan{{"scan-1", "node-1", "SHIPMENT_CONFIRMED", ts(t0.Add(5 * time.Hour)), true}, {"scan-2", "ds-1", "DELIVERED", ts(delivered), true}}}

	w.oms.orders[fx.om] = &omOrder{ID: fx.om, Status: "Shipped", RequiredShipBy: ts(t0.Add(20 * time.Hour)), FulfillmentSiteID: site, PromiseDate: ts(promiseEnd),
		Lines: []omLine{{1, "SKU-A", 2, "pick", false, "Shipped", "res-1"}, {2, "SKU-B", 1, "pick", false, "Shipped", "res-2"}}}
	w.wes.units = append(w.wes.units,
		&wesUnit{ID: fx.units[0], PathID: "PICK", CPT: ts(t0.Add(20 * time.Hour)), Reference: fx.om, State: "Completed", SKU: "SKU-A", LineNo: 1},
		&wesUnit{ID: fx.units[1], PathID: "PICK", CPT: ts(t0.Add(20 * time.Hour)), Reference: fx.om, State: "Completed", SKU: "SKU-B", LineNo: 2})
	w.fe.tasks = append(w.fe.tasks,
		&feTask{ID: "tk-pick-1-" + n, Type: "PICK", Status: "COMPLETED", CPT: ts(t0.Add(20 * time.Hour)), OrderRef: fx.units[0], RequiredCapabilities: []string{"pick"}},
		&feTask{ID: "tk-pick-2-" + n, Type: "PICK", Status: "COMPLETED", CPT: ts(t0.Add(20 * time.Hour)), OrderRef: fx.units[1], RequiredCapabilities: []string{"pick"}},
		&feTask{ID: "tk-pack-" + n, Type: "PACK", Status: "COMPLETED", CPT: ts(t0.Add(20 * time.Hour)), OrderRef: fx.om, RequiredCapabilities: []string{"pack"}})
	w.fe.packages = append(w.fe.packages, &fePackage{ID: fx.pkg, OrderRef: fx.om, Status: "LABELED", ScannedContents: []string{"SKU-A", "SKU-A", "SKU-B"}, SortLane: "STANDARD"})

	// The load is shared with somebody else's package, and an older load carries only theirs.
	w.outbound.loads = append(w.outbound.loads,
		&obLoad{ID: "load-old-" + n, SiteID: site, CPTID: "aus-1100", OperatingDate: "2026-11-14", ShipMethod: "GROUND", Cutoff: ts(t0), State: "SHIP_CONFIRMED", PackageCount: 1, Version: 4},
		&obLoad{ID: fx.load, SiteID: site, CPTID: "aus-1700", OperatingDate: "2026-11-14", ShipMethod: "GROUND", Cutoff: ts(t0.Add(2 * time.Hour)), State: "SHIP_CONFIRMED", PackageCount: 2, Version: 5})
	w.outbound.packages["load-old-"+n] = []*obPackage{{ID: "pkg-other-old-" + n, SiteID: site, SourceOrderID: "om-other-old-" + n, LoadID: "load-old-" + n, State: "SHIPPED", Version: 2}}
	w.outbound.packages[fx.load] = []*obPackage{
		{ID: "pkg-other-" + n, SiteID: site, SourceOrderID: "om-other-" + n, LoadID: fx.load, State: "SHIPPED", Version: 2},
		{ID: fx.pkg, SiteID: site, SourceOrderID: fx.om, WorkUnitRef: fx.units[0], LoadID: fx.load, StagingLocation: "STG-1", State: "SHIPPED", Version: 3},
	}

	w.inv.usable["SKU-A"], w.inv.usable["SKU-B"] = 10, 5
	w.bff.products["CAT-A"] = &bffProduct{CatalogItemID: "CAT-A", Title: "A", InStock: true, Purchasable: true}
	w.bff.products["CAT-B"] = &bffProduct{CatalogItemID: "CAT-B", Title: "B", InStock: true, Purchasable: true}

	if w.links == nil {
		w.links = map[string][]string{}
	}
	w.links[orderID] = []string{fx.om}
	return fx
}

// seedCancelled seeds an order cancelled before its purchase order was accepted: the assignment
// was never released, the hold was released in full, nothing exists downstream.
func seedCancelled(w *world, n string) *fixture {
	fx := &fixture{w: w, n: n, asg: "asg-" + n, session: "cs-" + n, auth: "pay-" + n}
	orderID := "ord-" + n
	fx.placed = retailday.Placed{RunID: "RUN1", Expect: retailday.Cancelled, OrderID: orderID,
		SKUs: map[string]int{"SKU-C": 1}, TotalMinor: 1200, AuthorizationID: fx.auth, PlacedAt: t0}

	w.mu.Lock()
	defer w.mu.Unlock()
	w.ordering.orders[orderID] = &foOrder{OrderID: orderID, ShopperID: "shopper-" + n, Status: "CANCELLED", DestinationPostalPrefix: "787", PlacedAt: ts(t0),
		Lines: []foLine{{1, "CAT-C", 1}},
		Groups: []foGroup{{GroupNo: 1, PromiseID: "prm-" + n, Tier: "STANDARD", ShipBy: ts(t0.Add(30 * time.Hour)), DeliverByEarliest: ts(t0.Add(72 * time.Hour)),
			DeliverByLatest: ts(t0.Add(96 * time.Hour)), LineNos: []int{1}, Status: "CANCELLED"}}}
	w.ordering.tracking[orderID] = &foTracking{OrderID: orderID, Status: "CANCELLED", Groups: []foTrackGroup{{GroupNo: 1, PromiseID: "prm-" + n, Status: "CANCELLED",
		DeliverByEarliest: ts(t0.Add(72 * time.Hour)), DeliverByLatest: ts(t0.Add(96 * time.Hour)), Packages: []foPackage{}}}}
	w.payments.auths = append(w.payments.auths, &cpAuth{AuthorizationID: fx.auth, CheckoutSessionID: fx.session, OrderID: orderID, State: "RELEASED", Currency: "USD", TotalMinor: 1200,
		Allocations:             []cpAllocation{{GroupNo: 1, AmountMinor: 1200, State: "RELEASED", TrackingNumbers: []string{}}},
		ProviderAuthorizationID: "hold_" + n})
	w.checkout.sessions[fx.session] = &coSession{SessionID: fx.session, State: "COMPLETED", ShopperID: "shopper-" + n, Currency: "USD", TotalMinor: 1200, AuthorizationID: fx.auth, OrderID: orderID}
	w.sourcing.assignments = append(w.sourcing.assignments, &osAssignment{AssignmentID: fx.asg, OrderID: orderID, GroupNo: 1, DestinationPostalPrefix: "787", State: "CANCELLED", AssignmentVersion: 2,
		Promise: osPromise{PromiseID: "prm-" + n, Tier: "STANDARD", DeliverByLatest: ts(t0.Add(96 * time.Hour)), WitnessNodeID: "node-1"},
		Lines:   []foLine{{1, "CAT-C", 1}}, Releases: []osRelease{}, ExcludedNodes: []string{}, TrackingNumbers: []string{}})
	return fx
}

// seedDeclined seeds an order whose payment token was declined: nothing was placed, so the
// shopper has no order id; the provider's DECLINED record is all that exists.
func seedDeclined(w *world, n string) *fixture {
	fx := &fixture{w: w, n: n, session: "cs-" + n, auth: "pay-" + n}
	fx.placed = retailday.Placed{RunID: "RUN1", Expect: retailday.Declined, SKUs: map[string]int{"SKU-C": 1}, TotalMinor: 1200, AuthorizationID: fx.auth, PlacedAt: t0}

	w.mu.Lock()
	defer w.mu.Unlock()
	w.payments.auths = append(w.payments.auths, &cpAuth{AuthorizationID: fx.auth, CheckoutSessionID: fx.session, State: "DECLINED", Currency: "USD", TotalMinor: 1200,
		Allocations: []cpAllocation{{GroupNo: 1, AmountMinor: 1200, State: "PENDING", TrackingNumbers: []string{}}}, ProviderAuthorizationID: "", DeclineReason: "card_declined"})
	w.checkout.sessions[fx.session] = &coSession{SessionID: fx.session, State: "DECLINED", ShopperID: "shopper-" + n, Currency: "USD", TotalMinor: 1200, AuthorizationID: fx.auth}
	return fx
}

// collect runs the collector on the fixture's placed order.
func (fx *fixture) collect(t *testing.T, c *Collector) (retailday.OrderFacts, []string) {
	t.Helper()
	return c.Order(context.Background(), fx.placed)
}

// rules lists the failing rules AuditOrder reports, sorted; warnings are listed too, prefixed "warn:".
func rules(fs []retailday.Finding) []string {
	var out []string
	for _, f := range fs {
		r := f.Rule
		if f.Warn {
			r = "warn:" + r
		}
		out = append(out, r)
	}
	sort.Strings(out)
	return out
}
