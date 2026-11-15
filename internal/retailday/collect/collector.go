// Package collect is the REST collector of the retail day: a retailday.Evidence that follows
// one placed order across every context over their PUBLISHED REST and fills
// retailday.OrderFacts and the shelf the verdict judges (ADR 0001 D5, D6; docs/retail-day-journey.md).
// The run's dead letters are not on any REST route: see Collector.DeadLetters.
//
// It only reads. Every request goes through Collector.get, which can only issue a GET, and the
// tests prove the contract fakes never received another verb. It is bounded (Settings.Parallel
// requests in flight, Settings.Timeout per call, Settings.PageSize and Settings.MaxLoads per
// collection), reads in parallel what does not depend on each other, and gives the same facts and
// the same notes, in the same order, for the same state of the estate. It does not panic: a reader
// that fails unexpectedly is a note.
//
// # What a fact is when it cannot be read
//
// A fact that cannot be read is left at its zero value AND named in the notes Order returns, so
// "none exists" differs from "could not look":
//
//	404                                  none exists: the fact is zero, nothing is noted
//	5xx, 429, any other refusal,         could not look: the fact is zero, and
//	a transport error, a timeout,        unread <facts>: <service> <route>: HTTP <status> <slug>
//	a body that is not the contract,     unread <facts>: <service> <route>: no usable answer: ...
//	a service with no base URL, a        unread <facts>: Settings.OrderIDs is not set ...
//	missing or failing link
//	two reads of one fact differ         disagree <fact>: ...  (the facts are what each context said)
//
// A fact that is a total over several reads (purchase orders, work units, picks, packages) is zero
// as a whole when any read of it failed: a count missing a leg would pass for a smaller order. The
// one 404 that is not silent is a 404 for something another context said exists (a release the node
// accepted has a purchase order; a journey's poNumber; the order-management order the link named):
// that is a disagreement and is noted, and the fact is still zero. Shelf is stricter, because its
// zero is a number the estate rules accept: see Collector.Shelf.
//
// # The id chain
//
// The order id is customer-ordering's. Every later context has its own id for the same order,
// and the collector follows them in this order:
//
//	customer-ordering   GET /orders/{orderId}                  status, lines, promise (deliverByLatest)
//	                    GET /orders/{orderId}/tracking         the tracking numbers the shopper sees
//	customer-payments   GET /payment-authorizations?orderId=   authorised (totalMinor, unless DECLINED),
//	                                                           captured and released (allocations[].state)
//	                    GET /payment-authorizations/{id}       when nothing was placed (a declined
//	                                                           payment), by the shopper's authorizationId
//	checkout            GET /checkout-sessions/{sessionId}     the order total (sessionId from the
//	                                                           authorization)
//	order-sourcing      GET /assignments?orderId=              assignmentId + releases[].releaseNo
//	vendor-connectivity GET /vendor/purchase-orders/{poNumber} the purchase order, poNumber derived
//	                                                           from (assignmentId, releaseNo), see below
//	shipping-and-tracking GET /journeys/{trackingNumber}       state, deliveredAt, poNumber, orderId
//	order-management    GET /orders/{id}                       the order raised for the PO (see below)
//	wes-work-planning   GET /work-units?reference=<omOrderId>  one unit per line
//	fulfillment-execution GET /tasks?orderRef=<unitId>         PICK tasks (orderRef = work unit id)
//	                    GET /tasks?orderRef=<omOrderId>        the PACK task (orderRef = order id)
//	                    GET /packages?orderRef=<omOrderId>     the package and its SLAM outcome
//	outbound-shipping   GET /loads?state=SHIP_CONFIRMED, GET /loads/{id}/packages
//	                                                           packages with source_order_id = omOrderId
//	inventory-storage   GET /inventory/{sku}/usable            Shelf: usable stock
//	storefront-bff      GET /products/{catalogItemId}          Shelf: purchasable
//
// When the shopper phase placed no order (a declined payment) the money records are read first by
// the authorization id; if they name an order anyway, that order is followed and the disagreement
// is noted, so "nothing downstream" is checked and not assumed.
//
// # Two links no published route gives
//
// A purchase order's number is derived, not read: vendor-connectivity ADR 0002 A1 states
// poNumber = "PO-" + the first 16 hex digits of sha256(assignmentId + ":" + releaseNo), and
// order-sourcing publishes both parts (assignmentId, releases[].releaseNo) at
// GET /assignments?orderId=. No route lists purchase orders by order. A journey's poNumber
// (shipping-and-tracking) is read as a cross-check and adds a purchase order the derivation
// missed. A derived number that answers 404 is "none" unless order-sourcing says the node
// accepted that release, when it is noted, because that is also what a drifted derivation
// looks like.
//
// The order-management order raised for a purchase order is NOT readable from the customer order
// over any route that is published today. network-fulfillment mints an opaque NetworkRef per
// purchase order (ADR 0026: "never derived from the network's purchase order number") and keeps
// the PO -> localOrderId mapping in its own tables, and order-management has no list route. The
// runner supplies the link: Settings.OrderIDs, which is given the order and the purchase order
// numbers the collector found. The implementation is the link package
// (internal/retailday/link): (*link.Linker).ForPurchaseOrders has this signature and reads once,
// over network-fulfillment's GET /purchase-orders/{poNumber} (the NetworkOrder of a purchase
// order: state, siteId and localOrderId once acknowledged; 404 purchase-order-not-found), so it
// works once that route is deployed. Without a link every order-management-side fact (OMOrders, WorkUnits,
// PickTasksDone, PackTasks, Packages, ShipmentConfirmations) stays zero and is named in the
// notes, whatever the order's expected outcome: a zero read without looking would pass the rules
// of a cancelled or declined order.
//
// # ShipmentConfirmed
//
// The event cannot be counted over REST. outbound-shipping raises it when TrailerDeparted
// arrives for a CLOSED load, and the load state itself is the guard (ADR 0002): a load is
// SHIP_CONFIRMED at most once. ShipmentConfirmations is therefore the number of SHIP_CONFIRMED
// loads (of the building's site) that carry a SHIPPED package whose source_order_id is the order's
// order-management id (ADR 0003: ShipmentConfirmed.orders[].order_id comes from source_order_id).
// It proves the load left and was confirmed; it cannot prove that the event reached Kafka or how
// many times it was published. The scan reads the newest Settings.MaxLoads loads and says so when
// there are more; the packages of a load that left are remembered per load version, so a run of
// many orders reads each load once.
package collect

import (
	"context"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
	"github.com/claudioed/warehouse-e2e/internal/sim"
)

// OrderIDs links a placed order to the order-management orders raised for its purchase orders.
// No route published today does (see the package comment); (*link.Linker).ForPurchaseOrders does,
// over network-fulfillment's GET /purchase-orders/{poNumber}, once that route is deployed, and a
// runner can supply its own meanwhile (the Kafka tap sees network-fulfillment's and
// order-management's events). poNumbers are the
// purchase orders the collector found for the order: the ones order-sourcing's releases imply and
// the ones a tracking journey names, whether or not vendor-connectivity could be read; p.OrderID
// is the order being followed. Return every distinct order id found: two orders for one purchase
// order is the audit's one-order finding, so a duplicate must reach it. An id returned twice is
// one order. An error means "could not look": every warehouse fact is zero and named.
type OrderIDs func(ctx context.Context, p retailday.Placed, poNumbers []string) ([]string, error)

// Settings tunes the collector. Catalogue and BuildingSite are the day's; the rest has defaults.
type Settings struct {
	// Catalogue maps a warehouse SKU to its storefront catalogItemId (supplied by the caller:
	// the listing -> SKU dictionary is GitOps, ADR 0001 D4). Shelf needs it; Order compares the
	// order's lines with it.
	Catalogue map[string]string
	// BuildingSite is the facility-layout site that serves the storefront (AUS3). An
	// order-management order that fulfils anywhere else is noted (journey gap G2), and the
	// shipment scan reads only that site's loads.
	BuildingSite string
	// VendorID is the vendor that owns the storefront's node. Default "V-WS".
	VendorID string
	// PageSize is the limit sent on every collection read. Default 200: the largest value every
	// service accepts (outbound-shipping caps limit at 200 and answers 400 above it).
	PageSize int
	// MaxLoads bounds the shipment scan, in loads read per order. Default 500.
	MaxLoads int
	// Parallel bounds the requests in flight at once across all calls. Default 8.
	Parallel int
	// Timeout bounds one Order or Shelf call. Default 90 s.
	Timeout time.Duration
	// OrderIDs links an order to its order-management orders; nil means it cannot be read.
	OrderIDs OrderIDs
}

func (s Settings) withDefaults() Settings {
	if s.VendorID == "" {
		s.VendorID = "V-WS"
	}
	if s.PageSize <= 0 {
		s.PageSize = 200
	}
	if s.MaxLoads <= 0 {
		s.MaxLoads = 500
	}
	if s.Parallel <= 0 {
		s.Parallel = 8
	}
	if s.Timeout <= 0 {
		s.Timeout = 90 * time.Second
	}
	return s
}

// Collector is a retailday.Evidence over published REST.
type Collector struct {
	c   *sim.Client
	set Settings
	sem chan struct{}
	// memo holds the packages of loads that already left, so a run of many orders reads each once.
	memo loadMemo
}

var _ retailday.Evidence = (*Collector)(nil)

// New returns a collector reading through client. The client needs a Base entry for every
// service the chain touches (retailday.AllGateway plus retailday.BFF); a missing one is a note,
// never a request. The caller's client is not modified.
func New(client *sim.Client, s Settings) *Collector {
	s = s.withDefaults()
	cc := *client
	if cc.Stats == nil {
		cc.Stats = sim.NewCallStats()
	}
	return &Collector{c: &cc, set: s, sem: make(chan struct{}, s.Parallel)}
}

// DeadLetters says it cannot know: the collector reads published REST only. A dead letter lives on
// a Kafka <topic>.dlq topic (warehouse-infra docs/dlq-runbook.md, read with scripts/dlq-redrive.sh
// over kubectl); no service publishes a route that lists them, and the services' /metrics counters
// carry no run id. known is false, so a Collector used alone shows "not verified", never 0.
//
// cmd/retail-day does not use it alone: its composite Evidence forwards Order and Shelf here and
// answers DeadLetters from internal/retailday/dlq, a read-only snapshot-and-count of the `*.dlq`
// topics.
func (c *Collector) DeadLetters(context.Context, string) (int, bool, error) { return 0, false, nil }
