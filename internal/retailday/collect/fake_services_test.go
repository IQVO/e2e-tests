package collect

// One tiny state machine per service. The shapes are the published response shapes, with the
// JSON names of each repo's apis/openapi.yaml (origin/develop, 2026-11-14); the comment above
// each fake says which spec it follows. State is keyed by ids, so one world holds many orders
// (and orders of nobody under test) and a read must pick its own.

import (
	"net/http"
	"slices"
	"strings"
)

// ---- customer-ordering: GET /orders/{orderId}, GET /orders/{orderId}/tracking ----

type foLine struct {
	LineNo        int    `json:"lineNo"`
	CatalogItemID string `json:"catalogItemId"`
	Quantity      int    `json:"quantity"`
}

type foGroup struct {
	GroupNo           int    `json:"groupNo"`
	PromiseID         string `json:"promiseId"`
	Tier              string `json:"tier"`
	ShipBy            string `json:"shipBy"`
	DeliverByEarliest string `json:"deliverByEarliest"`
	DeliverByLatest   string `json:"deliverByLatest"`
	TimeZone          string `json:"timeZone,omitempty"`
	LineNos           []int  `json:"lineNos"`
	Status            string `json:"status"`
}

type foOrder struct {
	OrderID                 string    `json:"orderId"`
	ShopperID               string    `json:"shopperId"`
	Status                  string    `json:"status"`
	DestinationPostalPrefix string    `json:"destinationPostalPrefix"`
	PlacedAt                string    `json:"placedAt"`
	Lines                   []foLine  `json:"lines"`
	Groups                  []foGroup `json:"groups"`
}

type foPackage struct {
	TrackingNumber  string `json:"trackingNumber"`
	DestinationKind string `json:"destinationKind"`
	Carrier         string `json:"carrier,omitempty"`
	State           string `json:"state,omitempty"`
}

type foTrackGroup struct {
	GroupNo           int         `json:"groupNo"`
	PromiseID         string      `json:"promiseId"`
	Status            string      `json:"status"`
	DeliverByEarliest string      `json:"deliverByEarliest"`
	DeliverByLatest   string      `json:"deliverByLatest"`
	Packages          []foPackage `json:"packages"`
}

type foTracking struct {
	OrderID string         `json:"orderId"`
	Status  string         `json:"status"`
	Groups  []foTrackGroup `json:"groups"`
}

type orderingFake struct {
	orders   map[string]*foOrder
	tracking map[string]*foTracking
}

func newOrderingFake(w *world) *orderingFake {
	f := &orderingFake{orders: map[string]*foOrder{}, tracking: map[string]*foTracking{}}
	w.get("customer-ordering", "/orders/{orderId}", func(r *http.Request) reply {
		if bad := checkQuery(r); bad != nil {
			return *bad
		}
		o, found := f.orders[r.PathValue("orderId")]
		if !found {
			return notFound("no order with this id")
		}
		return ok(o)
	})
	w.get("customer-ordering", "/orders/{orderId}/tracking", func(r *http.Request) reply {
		if bad := checkQuery(r); bad != nil {
			return *bad
		}
		t, found := f.tracking[r.PathValue("orderId")]
		if !found {
			return notFound("no order with this id")
		}
		return ok(t)
	})
	return f
}

// ---- customer-payments: GET /payment-authorizations, GET /payment-authorizations/{id} ----

type cpAllocation struct {
	GroupNo         int      `json:"groupNo"`
	AmountMinor     int64    `json:"amountMinor"`
	State           string   `json:"state"`
	TrackingNumber  string   `json:"trackingNumber"`
	TrackingNumbers []string `json:"trackingNumbers"`
}

type cpAuth struct {
	AuthorizationID         string         `json:"authorizationId"`
	CheckoutSessionID       string         `json:"checkoutSessionId"`
	OrderID                 string         `json:"orderId"`
	State                   string         `json:"state"`
	Currency                string         `json:"currency"`
	TotalMinor              int64          `json:"totalMinor"`
	Allocations             []cpAllocation `json:"allocations"`
	ProviderAuthorizationID string         `json:"providerAuthorizationId"`
	DeclineReason           string         `json:"declineReason,omitempty"`
}

type paymentsFake struct{ auths []*cpAuth }

func newPaymentsFake(w *world) *paymentsFake {
	f := &paymentsFake{}
	w.get("customer-payments", "/payment-authorizations", func(r *http.Request) reply {
		if bad := checkQuery(r, "orderId", "checkoutSessionId", "limit", "cursor"); bad != nil {
			return *bad
		}
		oid, sid := q(r, "orderId"), q(r, "checkoutSessionId")
		if (oid == "") == (sid == "") {
			return problem(http.StatusUnprocessableEntity, "validation-failed", "exactly one of orderId or checkoutSessionId is required")
		}
		var hit []*cpAuth
		for _, a := range f.auths {
			if (oid != "" && a.OrderID == oid) || (sid != "" && a.CheckoutSessionID == sid) {
				hit = append(hit, a)
			}
		}
		return pageOf(r, 500, len(hit), func(i int) any { return hit[i] })
	})
	w.get("customer-payments", "/payment-authorizations/{authorizationId}", func(r *http.Request) reply {
		for _, a := range f.auths {
			if a.AuthorizationID == r.PathValue("authorizationId") {
				return ok(a)
			}
		}
		return notFound("no such authorization")
	})
	return f
}

// ---- checkout: GET /checkout-sessions/{sessionId} ----

type coSession struct {
	SessionID       string `json:"sessionId"`
	State           string `json:"state"`
	CartID          string `json:"cartId"`
	ShopperID       string `json:"shopperId"`
	Currency        string `json:"currency"`
	TotalMinor      int64  `json:"totalMinor"`
	AuthorizationID string `json:"authorizationId"`
	OrderID         string `json:"orderId"`
}

type checkoutFake struct{ sessions map[string]*coSession }

func newCheckoutFake(w *world) *checkoutFake {
	f := &checkoutFake{sessions: map[string]*coSession{}}
	w.get("checkout", "/checkout-sessions/{sessionId}", func(r *http.Request) reply {
		s, found := f.sessions[r.PathValue("sessionId")]
		if !found {
			return notFound("unknown session")
		}
		return ok(s)
	})
	return f
}

// ---- order-sourcing: GET /assignments?orderId= ----

type osRelease struct {
	ReleaseNo      int    `json:"releaseNo"`
	NodeID         string `json:"nodeId"`
	ReleasedAt     string `json:"releasedAt"`
	RequiredShipBy string `json:"requiredShipBy"`
	Outcome        string `json:"outcome"`
}

type osAssignment struct {
	AssignmentID            string      `json:"assignmentId"`
	OrderID                 string      `json:"orderId"`
	GroupNo                 int         `json:"groupNo"`
	DestinationPostalPrefix string      `json:"destinationPostalPrefix"`
	State                   string      `json:"state"`
	AssignmentVersion       int         `json:"assignmentVersion"`
	Promise                 osPromise   `json:"promise"`
	Lines                   []foLine    `json:"lines"`
	Releases                []osRelease `json:"releases"`
	ExcludedNodes           []string    `json:"excludedNodes"`
	TrackingNumbers         []string    `json:"trackingNumbers"`
}

type osPromise struct {
	PromiseID       string `json:"promiseId"`
	Tier            string `json:"tier"`
	DeliverByLatest string `json:"deliverByLatest"`
	WitnessNodeID   string `json:"witnessNodeId"`
	RevisionNo      int    `json:"revisionNo"`
}

type sourcingFake struct{ assignments []*osAssignment }

func newSourcingFake(w *world) *sourcingFake {
	f := &sourcingFake{}
	w.get("order-sourcing", "/assignments", func(r *http.Request) reply {
		if bad := checkQuery(r, "state", "nodeId", "orderId", "limit", "cursor"); bad != nil {
			return *bad
		}
		var hit []*osAssignment
		for _, a := range f.assignments {
			if q(r, "orderId") == "" || a.OrderID == q(r, "orderId") {
				hit = append(hit, a)
			}
		}
		slices.SortFunc(hit, func(a, b *osAssignment) int { return strings.Compare(a.AssignmentID, b.AssignmentID) })
		return pageOf(r, 500, len(hit), func(i int) any { return hit[i] })
	})
	return f
}

// ---- vendor-connectivity: GET /vendor/purchase-orders/{poNumber}?vendorId= ----

type vcPO struct {
	PONumber        string   `json:"poNumber"`
	VendorID        string   `json:"vendorId"`
	NodeID          string   `json:"nodeId"`
	State           string   `json:"state"`
	IssuedAt        string   `json:"issuedAt"`
	RequiredShipBy  string   `json:"requiredShipBy"`
	AckBy           string   `json:"ackBy"`
	Lines           []vcLine `json:"lines"`
	Labels          []any    `json:"labels"`
	TrackingNumbers []string `json:"trackingNumbers"`
}

type vcLine struct {
	LineNo    int    `json:"lineNo"`
	ListingID string `json:"listingId"`
	Quantity  int    `json:"quantity"`
	Packaging []any  `json:"packaging"`
}

type vendorFake struct{ pos map[string]*vcPO }

func newVendorFake(w *world) *vendorFake {
	f := &vendorFake{pos: map[string]*vcPO{}}
	w.get("vendor-connectivity", "/vendor/purchase-orders/{poNumber}", func(r *http.Request) reply {
		if bad := checkQuery(r, "vendorId"); bad != nil {
			return *bad
		}
		v := q(r, "vendorId")
		if v == "" {
			return problem(http.StatusUnprocessableEntity, "validation-failed", "a missing or invalid vendorId")
		}
		po, found := f.pos[r.PathValue("poNumber")]
		if !found || po.VendorID != v {
			// A purchase order of another vendor is 404 too (node-not-owned-by-vendor).
			return notFound("no such purchase order")
		}
		return ok(po)
	})
	return f
}

// ---- shipping-and-tracking: GET /journeys/{trackingNumber} ----

type stScan struct {
	ScanID   string `json:"scanId"`
	NodeID   string `json:"nodeId"`
	ScanType string `json:"scanType"`
	At       string `json:"at"`
	Applied  bool   `json:"applied"`
}

type stJourney struct {
	TrackingNumber string   `json:"trackingNumber"`
	State          string   `json:"state"`
	PONumber       string   `json:"poNumber"`
	AssignmentID   string   `json:"assignmentId"`
	OrderID        string   `json:"orderId"`
	Tier           string   `json:"tier"`
	DeliveredAt    string   `json:"deliveredAt,omitempty"`
	Scans          []stScan `json:"scans"`
}

type trackingFake struct{ journeys map[string]*stJourney }

func newTrackingFake(w *world) *trackingFake {
	f := &trackingFake{journeys: map[string]*stJourney{}}
	w.get("shipping-and-tracking", "/journeys/{trackingNumber}", func(r *http.Request) reply {
		j, found := f.journeys[r.PathValue("trackingNumber")]
		if !found {
			return notFound("the addressed label or journey does not exist")
		}
		return ok(j)
	})
	return f
}

// ---- order-management: GET /orders/{id} ----

type omLine struct {
	LineNo        int    `json:"lineNo"`
	SKU           string `json:"sku"`
	Quantity      int    `json:"quantity"`
	PathID        string `json:"pathId"`
	GiftWrap      bool   `json:"giftWrap"`
	Status        string `json:"status"`
	ReservationID string `json:"reservationId,omitempty"`
}

type omOrder struct {
	ID                   string   `json:"id"`
	Status               string   `json:"status"`
	AllowPartialShipment bool     `json:"allowPartialShipment"`
	RequiredShipBy       string   `json:"requiredShipBy,omitempty"`
	FulfillmentSiteID    string   `json:"fulfillmentSiteId,omitempty"`
	ReleaseOnAllocation  bool     `json:"releaseOnAllocation"`
	PromiseDate          string   `json:"promiseDate,omitempty"`
	Lines                []omLine `json:"lines"`
}

type omFake struct{ orders map[string]*omOrder }

func newOMFake(w *world) *omFake {
	f := &omFake{orders: map[string]*omOrder{}}
	w.get("order-management", "/orders/{id}", func(r *http.Request) reply {
		if bad := checkQuery(r); bad != nil {
			return *bad
		}
		o, found := f.orders[r.PathValue("id")]
		if !found {
			return notFound("no order has this id")
		}
		return ok(o)
	})
	return f
}

// ---- wes-work-planning: GET /work-units?reference= ----

type wesUnit struct {
	ID        string `json:"id"`
	PathID    string `json:"pathId"`
	CPT       string `json:"cpt"`
	Reference string `json:"reference"`
	State     string `json:"state"`
	GiftWrap  bool   `json:"giftWrap"`
	SKU       string `json:"sku"`
	LineNo    int    `json:"lineNo"`
}

type wesFake struct{ units []*wesUnit }

func newWESFake(w *world) *wesFake {
	f := &wesFake{}
	w.get("wes-work-planning", "/work-units", func(r *http.Request) reply {
		if bad := checkQuery(r, "reference", "limit", "cursor"); bad != nil {
			return *bad
		}
		ref := q(r, "reference")
		if ref == "" {
			return problem(http.StatusBadRequest, "invalid-request", "the reference query parameter is required")
		}
		var hit []*wesUnit
		for _, u := range f.units {
			if u.Reference == ref {
				hit = append(hit, u)
			}
		}
		return pageOf(r, 500, len(hit), func(i int) any { return hit[i] })
	})
	return f
}

// ---- fulfillment-execution: GET /tasks?orderRef=, GET /packages?orderRef= ----

type feTask struct {
	ID                   string   `json:"id"`
	Type                 string   `json:"type"`
	Status               string   `json:"status"`
	CPT                  string   `json:"cpt"`
	OrderRef             string   `json:"orderRef"`
	RequiredCapabilities []string `json:"requiredCapabilities"`
	Fragile              bool     `json:"fragile"`
	GiftWrap             bool     `json:"giftWrap"`
}

type fePackage struct {
	ID                string   `json:"id"`
	OrderRef          string   `json:"orderRef"`
	Status            string   `json:"status"`
	ScannedContents   []string `json:"scannedContents"`
	FragileHandling   bool     `json:"fragileHandling"`
	GiftWrapRequested bool     `json:"giftWrapRequested"`
	SortLane          string   `json:"sortLane"`
}

type feFake struct {
	tasks    []*feTask
	packages []*fePackage
}

func newFEFake(w *world) *feFake {
	f := &feFake{}
	w.get("fulfillment-execution", "/tasks", func(r *http.Request) reply {
		if bad := checkQuery(r, "orderRef", "limit", "cursor"); bad != nil {
			return *bad
		}
		ref := q(r, "orderRef")
		if ref == "" {
			return problem(http.StatusBadRequest, "invalid-request", "the orderRef query parameter is required")
		}
		var hit []*feTask
		for _, t := range f.tasks {
			if t.OrderRef == ref {
				hit = append(hit, t)
			}
		}
		slices.SortFunc(hit, func(a, b *feTask) int { return strings.Compare(a.ID, b.ID) })
		return pageOf(r, 500, len(hit), func(i int) any { return hit[i] })
	})
	w.get("fulfillment-execution", "/packages", func(r *http.Request) reply {
		if bad := checkQuery(r, "orderRef", "limit", "cursor"); bad != nil {
			return *bad
		}
		ref := q(r, "orderRef")
		if ref == "" {
			return problem(http.StatusBadRequest, "invalid-request", "the orderRef query parameter is required")
		}
		var hit []*fePackage
		for _, p := range f.packages {
			if p.OrderRef == ref {
				hit = append(hit, p)
			}
		}
		slices.SortFunc(hit, func(a, b *fePackage) int { return strings.Compare(a.ID, b.ID) })
		return pageOf(r, 500, len(hit), func(i int) any { return hit[i] })
	})
	return f
}

// ---- outbound-shipping: GET /loads, GET /loads/{loadId}/packages (limit capped at 200) ----

type obLoad struct {
	ID            string `json:"id"`
	SiteID        string `json:"site_id"`
	CPTID         string `json:"cpt_id"`
	OperatingDate string `json:"operating_date"`
	ShipMethod    string `json:"ship_method"`
	Cutoff        string `json:"cutoff"`
	State         string `json:"state"`
	DoorCode      string `json:"door_code,omitempty"`
	TrailerID     string `json:"trailer_id,omitempty"`
	PackageCount  int    `json:"package_count"`
	Version       int    `json:"version"`
}

type obPackage struct {
	ID              string `json:"id"`
	SiteID          string `json:"site_id"`
	SourceOrderID   string `json:"source_order_id,omitempty"`
	WorkUnitRef     string `json:"work_unit_ref,omitempty"`
	LoadID          string `json:"load_id,omitempty"`
	StagingLocation string `json:"staging_location,omitempty"`
	State           string `json:"state"`
	Version         int    `json:"version"`
}

type outboundFake struct {
	loads    []*obLoad               // in creation order; the list answers newest first
	packages map[string][]*obPackage // by load id
}

func newOutboundFake(w *world) *outboundFake {
	f := &outboundFake{packages: map[string][]*obPackage{}}
	w.get("outbound-shipping", "/loads", func(r *http.Request) reply {
		if bad := checkQuery(r, "site_id", "state", "operating_date", "limit", "cursor"); bad != nil {
			return *bad
		}
		var hit []*obLoad
		for i := len(f.loads) - 1; i >= 0; i-- {
			l := f.loads[i]
			if (q(r, "site_id") == "" || l.SiteID == q(r, "site_id")) && (q(r, "state") == "" || l.State == q(r, "state")) &&
				(q(r, "operating_date") == "" || l.OperatingDate == q(r, "operating_date")) {
				hit = append(hit, l)
			}
		}
		rep := pageOf(r, 200, len(hit), func(i int) any { return hit[i] })
		if m, isMap := rep.body.(map[string]any); isMap {
			if _, has := m["nextCursor"]; !has {
				m["nextCursor"] = nil // the contract requires the key, null on the last page
			}
		}
		return rep
	})
	w.get("outbound-shipping", "/loads/{loadId}/packages", func(r *http.Request) reply {
		if bad := checkQuery(r, "state", "limit", "cursor"); bad != nil {
			return *bad
		}
		id := r.PathValue("loadId")
		known := false
		for _, l := range f.loads {
			known = known || l.ID == id
		}
		if !known {
			return problem(http.StatusNotFound, "load-not-found", "no such load")
		}
		ps := f.packages[id]
		return pageOf(r, 200, len(ps), func(i int) any { return ps[i] })
	})
	return f
}

// ---- inventory-storage: GET /inventory/{sku}/usable (fleet-owned stock when ownerRef is omitted) ----

type inventoryFake struct{ usable map[string]int }

func newInventoryFake(w *world) *inventoryFake {
	f := &inventoryFake{usable: map[string]int{}}
	w.get("inventory-storage", "/inventory/{sku}/usable", func(r *http.Request) reply {
		if bad := checkQuery(r, "ownerRef"); bad != nil {
			return *bad
		}
		sku := r.PathValue("sku")
		// "0 if the SKU is unknown": an unknown SKU is a 200, never a 404.
		return ok(map[string]any{"sku": sku, "usable": f.usable[sku], "looseUsable": f.usable[sku] / 2, "reservableCases": 0})
	})
	return f
}

// ---- storefront-bff: GET /products/{id} (product-discovery's ProductView, relayed) ----

type bffProduct struct {
	CatalogItemID string `json:"catalogItemId"`
	Title         string `json:"title"`
	Hazmat        bool   `json:"hazmat"`
	Fragile       bool   `json:"fragile"`
	InStock       bool   `json:"inStock"`
	Purchasable   bool   `json:"purchasable"`
}

type bffFake struct{ products map[string]*bffProduct }

func newBFFFake(w *world) *bffFake {
	f := &bffFake{products: map[string]*bffProduct{}}
	w.get("storefront-bff", "/products/{id}", func(r *http.Request) reply {
		p, found := f.products[r.PathValue("id")]
		if !found {
			// a stub row is invisible: GET answers 404 (product-discovery).
			return notFound("no such product")
		}
		return ok(p)
	})
	return f
}
