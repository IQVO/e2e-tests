package retailday

import (
	"fmt"
	"sort"
	"time"
)

// The verdict of the retail day (ADR 0001 D6). Nothing here reads a service: a
// collector fills OrderFacts from the published REST of each context and this file
// only judges them, so every rule is testable without a cluster and a rule can be
// shown to FAIL (a test per rule breaks the facts it guards).

// Outcome is what the shopper's plan said must happen to an order.
type Outcome string

const (
	// Delivered: placed, paid, fulfilled, shipped, delivered.
	Delivered Outcome = "delivered"
	// Cancelled: placed, then cancelled before the purchase order was accepted.
	Cancelled Outcome = "cancelled"
	// Declined: the payment token was declined; nothing may exist downstream.
	Declined Outcome = "declined"
	// NotServed: the destination is outside the service areas; nothing was placed.
	NotServed Outcome = "not-served"
	// InDoubt: tok_timeout. Today the checkout session stays AUTHORIZING with no way
	// out (journey G8): the run records it and only requires that no money moved.
	InDoubt Outcome = "in-doubt"
)

// PO is a vendor purchase order of the order.
type PO struct{ Number, State string }

// OMOrder is an order-management order raised for the purchase order.
type OMOrder struct {
	ID, Status string
}

// Package is a package fulfillment-execution sealed for the order.
type Package struct{ ID, Outcome string } // Outcome: LABELED | DIVERTED

// OrderFacts is everything the verdict needs about one order, as read from the
// contexts. Zero values mean "not found / not seen".
type OrderFacts struct {
	OrderID string
	Expect  Outcome
	Lines   int // order lines the shopper bought

	// storefront and money
	Status          string // customer-ordering status: PLACED, IN_FULFILLMENT, SHIPPED, DELIVERED, CANCELLED, ...
	TotalMinor      int64
	AuthorizedMinor int64
	CapturedMinor   int64
	VoidedMinor     int64

	// network
	POs []PO

	// warehouse
	OMOrders              []OMOrder
	WorkUnits             int
	PickTasksDone         int
	PackTasks             int
	Packages              []Package
	ShipmentConfirmations int // ShipmentConfirmed events carrying this order's ref

	// transport
	TrackingNumbers []string
	DeliveredAt     time.Time // zero = not delivered
	PromiseEnd      time.Time // end of the delivery window the shopper was promised
}

// Finding is one broken rule. Every finding is a failure of the run; Warn is for
// things worth a human's eye that a run may still pass with.
type Finding struct {
	Order  string `json:"order"`
	Rule   string `json:"rule"`
	Warn   bool   `json:"warn,omitempty"`
	Detail string `json:"detail"`
}

// poStatesThatDie are purchase-order states that mean the node never took the order.
var poStatesThatDie = map[string]bool{"EXPIRED": true, "REJECTED": true, "CANCELLED": true, "FAILED": true}

// AuditOrder judges one order against what its plan required.
func AuditOrder(f OrderFacts) []Finding {
	var out []Finding
	bad := func(rule, format string, args ...any) {
		out = append(out, Finding{Order: f.OrderID, Rule: rule, Detail: fmt.Sprintf(format, args...)})
	}
	warn := func(rule, format string, args ...any) {
		out = append(out, Finding{Order: f.OrderID, Rule: rule, Warn: true, Detail: fmt.Sprintf(format, args...)})
	}

	switch f.Expect {
	case Delivered:
		auditDelivered(f, bad, warn)
	case Cancelled:
		noDownstream(f, bad)
		if f.AuthorizedMinor > 0 && f.VoidedMinor != f.AuthorizedMinor {
			bad("money/released", "authorised %d but voided %d: a cancelled order must release its hold in full", f.AuthorizedMinor, f.VoidedMinor)
		}
		if f.CapturedMinor != 0 {
			bad("money/not-captured", "captured %d on a cancelled order", f.CapturedMinor)
		}
		if f.Status != "CANCELLED" {
			bad("status", "status %q, want CANCELLED", f.Status)
		}
	case Declined, NotServed:
		noDownstream(f, bad)
		if f.CapturedMinor != 0 || (f.AuthorizedMinor != 0 && f.VoidedMinor != f.AuthorizedMinor) {
			bad("money/untouched", "a %s order moved money: authorised %d captured %d voided %d", f.Expect, f.AuthorizedMinor, f.CapturedMinor, f.VoidedMinor)
		}
	case InDoubt:
		if f.CapturedMinor != 0 {
			bad("money/not-captured", "captured %d on an in-doubt order", f.CapturedMinor)
		}
		warn("in-doubt", "tok_timeout leaves the checkout session AUTHORIZING with no way out (journey G8): recorded as a finding, not asserted green")
	default:
		bad("expectation", "unknown expected outcome %q", f.Expect)
	}
	return out
}

// noDownstream asserts that a refused or cancelled order left nothing behind in
// the network or the warehouse.
func noDownstream(f OrderFacts, bad func(string, string, ...any)) {
	if len(f.POs) != 0 {
		bad("network/no-po", "%d purchase order(s) exist for an order that %s", len(f.POs), f.Expect)
	}
	if len(f.OMOrders) != 0 {
		bad("warehouse/no-order", "%d order-management order(s) exist for an order that %s", len(f.OMOrders), f.Expect)
	}
	if f.WorkUnits != 0 || f.PickTasksDone != 0 || f.PackTasks != 0 || len(f.Packages) != 0 {
		bad("warehouse/no-work", "work exists for an order that %s: %d work units, %d picks done, %d pack tasks, %d packages", f.Expect, f.WorkUnits, f.PickTasksDone, f.PackTasks, len(f.Packages))
	}
}

func auditDelivered(f OrderFacts, bad, warn func(string, string, ...any)) {
	// money: authorised once for the order total, captured once for the same, never voided
	switch {
	case f.TotalMinor <= 0:
		bad("money/total", "order total %d is not positive", f.TotalMinor)
	case f.AuthorizedMinor != f.TotalMinor:
		bad("money/authorised", "authorised %d, order total %d", f.AuthorizedMinor, f.TotalMinor)
	case f.CapturedMinor != f.TotalMinor:
		bad("money/captured", "captured %d, order total %d (capture follows FulfillmentShipped)", f.CapturedMinor, f.TotalMinor)
	}
	if f.VoidedMinor != 0 {
		bad("money/voided", "%d voided on an order that was delivered", f.VoidedMinor)
	}

	// network: exactly one purchase order, and the node took it
	switch {
	case len(f.POs) != 1:
		bad("network/one-po", "%d purchase orders, want exactly 1 (a retried poll or a redelivered event must not duplicate it)", len(f.POs))
	case poStatesThatDie[f.POs[0].State]:
		bad("network/po-state", "purchase order %s is %s", f.POs[0].Number, f.POs[0].State)
	}

	// warehouse: one held-then-released order, a unit per line, picked, one pack task, labelled
	switch {
	case len(f.OMOrders) != 1:
		bad("warehouse/one-order", "%d order-management orders, want exactly 1 per purchase order", len(f.OMOrders))
	case f.OMOrders[0].Status != "Shipped":
		bad("warehouse/order-shipped", "order-management order %s is %s, want Shipped (ShipmentConfirmed moves it)", f.OMOrders[0].ID, f.OMOrders[0].Status)
	}
	if f.Lines > 0 && f.WorkUnits != f.Lines {
		bad("warehouse/work-units", "%d work units for %d lines", f.WorkUnits, f.Lines)
	}
	if f.PickTasksDone < f.Lines {
		bad("warehouse/picks", "%d pick tasks completed for %d lines", f.PickTasksDone, f.Lines)
	}
	if f.PackTasks != 1 {
		bad("warehouse/one-pack", "%d pack tasks, want exactly 1", f.PackTasks)
	}
	if len(f.Packages) == 0 {
		bad("warehouse/package", "no package was sealed")
	}
	for _, p := range f.Packages {
		if p.Outcome != "LABELED" {
			bad("warehouse/labeled", "package %s is %s: a day that injects no mispick must not divert", p.ID, p.Outcome)
		}
	}
	if f.ShipmentConfirmations != 1 {
		bad("warehouse/shipment-confirmed", "%d ShipmentConfirmed events for the order, want exactly 1", f.ShipmentConfirmations)
	}

	// transport and the shopper's view
	if len(f.TrackingNumbers) == 0 || len(f.TrackingNumbers) != len(f.Packages) {
		bad("network/tracking", "%d tracking numbers for %d packages", len(f.TrackingNumbers), len(f.Packages))
	}
	if f.Status != "DELIVERED" {
		bad("status", "the shopper sees %q, want DELIVERED", f.Status)
	}
	switch {
	case f.DeliveredAt.IsZero():
		bad("delivered", "no delivery was recorded")
	case !f.PromiseEnd.IsZero() && f.DeliveredAt.After(f.PromiseEnd):
		// A compressed run delivers minutes after checkout, long before the window opens, so
		// "inside the window" cannot be required; "not after its end" can.
		bad("promise", "delivered %s, after the promised window ended %s", f.DeliveredAt.UTC().Format(time.RFC3339), f.PromiseEnd.UTC().Format(time.RFC3339))
	case f.PromiseEnd.IsZero():
		warn("promise", "the promised window was not captured, so lateness could not be judged")
	}
}

// StockFacts is what the estate-level checks compare: units of one SKU on the
// day's shelf before and after, and what the run shipped.
type StockFacts struct {
	SKU           string
	UsableBefore  int
	UsableAfter   int
	Shipped       int // units delivered orders carried
	Restocked     int // units the restock leg received and stowed
	AdvertisedNow int // network-fulfillment capability offer, -1 = not read
	Purchasable   bool
}

// AuditEstate judges the run as a whole: stock moved by exactly what shipped and
// arrived, and the storefront followed the stock.
func AuditEstate(stock []StockFacts, deadLettersForRun, serverErrors int, suspendedFindings int) []Finding {
	var out []Finding
	add := func(rule, format string, args ...any) {
		out = append(out, Finding{Order: "-", Rule: rule, Detail: fmt.Sprintf(format, args...)})
	}
	skus := append([]StockFacts(nil), stock...)
	sort.Slice(skus, func(i, j int) bool { return skus[i].SKU < skus[j].SKU })
	for _, s := range skus {
		if want := s.UsableBefore - s.Shipped + s.Restocked; s.UsableAfter != want {
			add("stock/conserved", "%s: usable %d -> %d, want %d (%d before - %d shipped + %d restocked)", s.SKU, s.UsableBefore, s.UsableAfter, want, s.UsableBefore, s.Shipped, s.Restocked)
		}
		if s.UsableAfter > 0 && !s.Purchasable {
			add("storefront/follows-stock", "%s has %d usable but the storefront no longer sells it", s.SKU, s.UsableAfter)
		}
		if s.UsableAfter == 0 && s.Purchasable {
			add("storefront/follows-stock", "%s has no usable stock but the storefront still sells it", s.SKU)
		}
	}
	if deadLettersForRun > 0 {
		add("kafka/dead-letters", "%d dead-lettered message(s) carry this run's id", deadLettersForRun)
	}
	if serverErrors > 0 {
		add("http/5xx", "%d 5xx or transport errors outside a recorded host suspension (%d more inside one, not counted)", serverErrors, suspendedFindings)
	}
	return out
}

// Failed counts the findings that are not warnings.
func FailedFindings(fs []Finding) int {
	n := 0
	for _, f := range fs {
		if !f.Warn {
			n++
		}
	}
	return n
}
