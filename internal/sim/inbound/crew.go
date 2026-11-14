package inbound

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/sim"
	"github.com/claudioed/warehouse-e2e/internal/sim/outbound"
	"github.com/claudioed/warehouse-e2e/internal/sim/transport"
)

// Crew is the restock crew of one building: supplier (registers the ASN), dock planner (books an inbound
// door), gate and yard clerk (checks the carrier in, docks the trailer, starts door work, checks the visit out),
// receiver (opens, counts and closes the receipt) and stower (puts the received units into bins). Every method is
// one role doing one thing over REST and returns an error naming the call and the answer when the service
// refuses; none panics. It is safe for concurrent use.
type Crew struct {
	C     *sim.Client
	Site  Site
	RunID string // namespaces ASNs, plates, unit numbers and seals: short, upper-case, run-unique

	Carrier  Carrier // the trucks' carrier
	Supplier string  // supplierRef of the ASNs; default "SIM-SUPPLIER"
	Wait     Waits   // bound of every eventually-style wait
	Clock    Clock   // time source of every wait and window; default the wall clock
	Log      sim.LogFunc

	// GridMinutes is the dock calendar's slot granularity (Readiness.Calendar.SlotGranularityMinutes);
	// appointment windows start and end on it. Default 5, what the outbound Provision declares.
	GridMinutes int
	// Lead is how far ahead of now the appointment window starts (default 2 minutes). A window must not start
	// in the past, and a carrier that arrives earlier than the calendar's arrival tolerance is sent to a
	// holding spot instead of the door.
	Lead time.Duration
	// WindowLength is how long an appointment window lasts (default 1 hour; at most the calendar's 4 hours).
	WindowLength time.Duration
	// Hostler is the ref the clerk claims yard moves under. Default RunID + "-IB-HOSTLER".
	Hostler string
	// Staged, when set, is the Kafka tap the receiver and stower use to see inventory-storage stage the
	// received units (StockReceived on warehouse.inventory.analytics): the stower waits for it before it stows.
	// *sim.EventTap satisfies it; the tap must be reading that topic. Nil skips the wait.
	Staged StagedTap

	n atomic.Int64
}

// StagedTap is what the crew needs from the Kafka tap to see the handover from inbound-receiving to
// inventory-storage, which has no REST read: Track registers a correlation id, Seen counts the events of a
// short type that carried it. *sim.EventTap satisfies it.
type StagedTap interface {
	outbound.Confirmations
	Track(ref string)
}

var _ StagedTap = (*sim.EventTap)(nil)

func (c *Crew) logf(who, format string, args ...any) {
	if c.Log != nil {
		c.Log(who, format, args...)
	}
}

func (c *Crew) clock() Clock {
	if c.Clock != nil {
		return c.Clock
	}
	return transport.SystemClock{}
}

func (c *Crew) grid() int {
	if c.GridMinutes > 0 {
		return c.GridMinutes
	}
	return 5
}

func (c *Crew) lead() time.Duration {
	if c.Lead > 0 {
		return c.Lead
	}
	return 2 * time.Minute
}

func (c *Crew) windowLength() time.Duration {
	if c.WindowLength > 0 {
		return c.WindowLength
	}
	return time.Hour
}

func (c *Crew) supplier() string {
	if c.Supplier != "" {
		return c.Supplier
	}
	return "SIM-SUPPLIER"
}

func (c *Crew) hostler() string {
	if c.Hostler != "" {
		return c.Hostler
	}
	return c.RunID + "-IB-HOSTLER"
}

func (c *Crew) next() int64 { return c.n.Add(1) }

func (c *Crew) call(ctx context.Context, svc, method, route, path string, body any) sim.Resp {
	return c.C.Call(ctx, svc, method, route, path, body)
}

// NextWindow is the appointment window starting on the next slot boundary at least Lead from now (by the
// crew's clock), WindowLength long. The slot arithmetic is the outbound crew's (Floor.NextWindow).
func (c *Crew) NextWindow() Window {
	f := &outbound.Floor{Now: c.clock().Now, GridMinutes: c.grid()}
	return f.NextWindow(c.lead(), c.windowLength())
}
