// Package collect is the REST collector of the retail day: a retailday.Evidence that follows
// one placed order across every context over their PUBLISHED REST and fills
// retailday.OrderFacts, the shelf and the dead-letter evidence the verdict judges (ADR 0001 D5,
// D6; docs/retail-day-journey.md).
//
// It only reads. Every request goes through Collector.get, which can only issue a GET, and the
// tests prove the contract fakes never received another verb. A fact that cannot be read is
// left at its zero value AND named in the notes Order returns, so "none exists" (a 404, no note)
// differs from "could not look" (a 5xx, a transport error, a refusal, a missing link: a note):
//
//	unread <fact>: <service> <route>: HTTP <status> <problem slug>   could not look; the fact is zero
//	disagree <fact>: ...                                             two reads of one fact differ
//
// # The id chain
//
// The order id is customer-ordering's. Every later context has its own id for the same order,
// and the collector follows them in this order:
//
//	customer-ordering   GET /orders/{orderId}                  status, lines, promise (deliverByLatest)
//	                    GET /orders/{orderId}/tracking         the tracking numbers the shopper sees
//	customer-payments   GET /payment-authorizations?orderId=   authorised (totalMinor), captured and
//	                                                           voided/released (allocations[].state)
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
//
// # Two links no published route gives
//
// A purchase order's number is derived, not read: vendor-connectivity ADR 0002 A1 states
// poNumber = "PO-" + the first 16 hex digits of sha256(assignmentId + ":" + releaseNo), and
// order-sourcing publishes both parts (assignmentId, releases[].releaseNo) at
// GET /assignments?orderId=. No route lists purchase orders by order. A journey's poNumber
// (shipping-and-tracking) is read as a cross-check and adds a purchase order the derivation
// missed; a derived number that answers 404 is noted, because that is also what a drifted
// derivation looks like.
//
// The order-management order raised for a purchase order is NOT readable over REST at all.
// network-fulfillment mints an opaque NetworkRef per purchase order (ADR 0026: "never derived
// from the network's purchase order number") and keeps the PO -> localOrderId mapping in its
// own tables; GET /network-orders/{networkRef} exposes localOrderId but nothing maps a PO or a
// customer order to a networkRef, GET /network-orders lists only NEW orders, and
// order-management has no list route. The runner supplies the link: Settings.OrderIDs. Without
// it every order-management-side fact (OMOrders, WorkUnits, PickTasksDone, PackTasks,
// Packages, ShipmentConfirmations) stays zero and is named in the notes.
//
// # ShipmentConfirmed
//
// The event cannot be counted over REST. outbound-shipping raises it when TrailerDeparted
// arrives for a CLOSED load, and the load state itself is the guard (ADR 0002): a load is
// SHIP_CONFIRMED at most once. ShipmentConfirmations is therefore the number of SHIP_CONFIRMED
// loads that carry a SHIPPED package whose source_order_id is the order's order-management id
// (ADR 0003: ShipmentConfirmed.orders[].order_id comes from source_order_id). It proves the
// load left and was confirmed; it cannot prove that the event reached Kafka or how many times it
// was published.
package collect

import (
	"context"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
	"github.com/claudioed/warehouse-e2e/internal/sim"
)

// OrderIDs links a placed order to the order-management orders raised for its purchase orders.
// No published route does (see the package comment); the runner knows how it can (the Kafka tap
// sees network-fulfillment's and order-management's events, the operator can read a
// network order). poNumbers are the purchase orders the collector found for the order. Return
// every order id found, including a duplicate: the audit's one-order rule needs to see it.
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

// DeadLetters says it cannot know. A dead letter lives on a Kafka <topic>.dlq topic
// (warehouse-infra docs/dlq-runbook.md, read with scripts/dlq-redrive.sh over kubectl); no
// service publishes a route that lists them, and the services' /metrics counters carry no run
// id. known is false, so the report shows "not verified", never 0.
func (c *Collector) DeadLetters(context.Context, string) (int, bool, error) { return 0, false, nil }
