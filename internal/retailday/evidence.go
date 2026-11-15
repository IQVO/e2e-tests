package retailday

import (
	"context"
	"time"
)

// The seam between the day's runner (which plays the actors) and the evidence it is
// judged by (which only reads). The runner never reads a service to decide a verdict and
// the collector never writes: a collector is an Evidence over published REST, a test
// double is an Evidence over a map, and AuditOrder / AuditEstate judge what either returns.

// Placed is what the shopper phase knows about one order of the day when it hands over to
// the rest of the run. Every field except RunID and Expect may be zero: an order that was
// declined or never placed has no OrderID, and nothing downstream may exist for it.
type Placed struct {
	RunID   string  // the run's id: stamped on every actor's references and on the shopper's emails
	Expect  Outcome // what the shopper's plan said must happen
	OrderID string  // customer-ordering's order id; empty when nothing was placed

	// SKUs is the quantity of each warehouse SKU the order bought. The storefront buys by
	// catalogItemId; the dictionary that turns it into a SKU is the day's catalogue. A key
	// count is the order's line count.
	SKUs map[string]int

	// What the shopper's own answers said, so the collector can compare instead of trust.
	TotalMinor      int64
	AuthorizationID string
	PromiseEnd      time.Time // the latest delivery instant the order promised; zero = unknown
	PlacedAt        time.Time
}

// Lines is the order's line count.
func (p Placed) Lines() int { return len(p.SKUs) }

// Shelf is one SKU as the warehouse and the storefront see it at one instant.
type Shelf struct {
	Usable      int  // inventory-storage's usable units (on hand minus reservations)
	Purchasable bool // the storefront still sells it
}

// Evidence reads the estate. Implementations only read; a read they cannot make is an
// error or an unknown, never a zero that would pass a rule.
type Evidence interface {
	// Order follows one placed order across every context and returns what it found.
	// A fact that could not be read is left at its zero value AND named in the
	// returned notes, so the runner can tell "none exists" from "could not look".
	Order(ctx context.Context, p Placed) (OrderFacts, []string)

	// Shelf reads each SKU's usable stock and whether the storefront sells it.
	Shelf(ctx context.Context, skus []string) (map[string]Shelf, error)

	// DeadLetters counts the dead-lettered messages that carry the run id. known is
	// false when no published route can say, which the report must show as not verified,
	// not as zero.
	DeadLetters(ctx context.Context, runID string) (n int, known bool, err error)
}
