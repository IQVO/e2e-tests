// Package floor simulates the people of a warehouse floor who take one RELEASED customer order from
// wes-work-planning to a LABELED package, over the services' PUBLISHED REST contracts only:
//
//   - the release operator, who admits the order's work units into the PICK pool
//     (wes-work-planning POST /paths/{path}/work-releases; nothing in the fleet calls it);
//   - the pickers, who pull PICK tasks at their stations (fulfillment-execution POST /stations,
//     /check-ins, /task-claims, /tasks/{id}/completions) and, optionally, confirm the reservation
//     (inventory-storage POST /reservations/{id}/pick-confirmations);
//   - the rebin associate, who scans each picked line into the order's consolidation slot
//     (POST /rebin/arrivals), which creates the PACK task once every line has arrived;
//   - the packers, who pull the PACK task, scan the contents and seal the package
//     (POST /tasks/{id}/package-seals, /completions);
//   - the SLAM operator, who weighs the sealed package (POST /packages/{id}/slam-runs) and reads
//     whether it was LABELED or DIVERTED.
//
// LABELED is where this package stops. SLAM is also what raises PackageManifested, the event the
// outbound crew (internal/sim/outbound) and outbound-shipping take over from; this package never
// produces it by hand, it only OBSERVES it when given an event tap.
//
// What it does NOT do is just as deliberate: no database, no injected Kafka event, no workforce
// shifts (fulfillment-execution never calls workforce-management; the station's occupant is what
// TaskCompleted carries as the associate).
//
// The simulators' rules apply: every creating POST carries a fresh Idempotency-Key (sim.Client does
// it; a retry builds a new request, because the services store a 4xx under the key), a refusal is a
// finding in Result.Problems and never a panic, the one tolerated failure is an exact (status, slug)
// "not yet" pair retried inside a bounded loop, and every wait takes the injected Clock and says what
// it never saw when it expires.
//
// The floor is shared and pull-dispatched. wes-work-planning releases the earliest-CPT unit of a
// pool, the caller never chooses; fulfillment-execution hands a station the earliest-CPT task, with
// no site and no order predicate. So the actors never assume that what they released or claimed is
// theirs: they release until THEIR units are released, claim until THEIR tasks are done, and work
// whatever task they were handed, for another order of the same Floor or, by default, for somebody
// else (see Settings.LeaveForeign).
//
// It was written against the services' published contracts (apis/openapi.yaml, the ADRs) and
// exercised against contract fakes (fake_test.go); it has never run against the cluster.
package floor

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/sim"
)

// Service names: the keys of sim.Client.Base (and the Kong path segment).
const (
	SvcWES       = "wes-work-planning"
	SvcExecution = "fulfillment-execution"
	SvcInventory = "inventory-storage"
	SvcOrders    = "order-management"
)

// States the actors branch on.
const (
	UnitPending   = "Pending"
	UnitReleased  = "Released"
	UnitCompleted = "Completed"
	UnitCancelled = "Cancelled"

	TypePick = "PICK"
	TypePack = "PACK"

	TaskPending   = "PENDING"
	TaskClaimed   = "CLAIMED"
	TaskCompleted = "COMPLETED"

	PackageSealed   = "SEALED"
	PackageLabeled  = "LABELED"
	PackageDiverted = "DIVERTED"

	LineReleased    = "Released"
	LineBackordered = "Backordered"
	LineCancelled   = "Cancelled"

	ReservationActive    = "ACTIVE"
	ReservationConfirmed = "CONFIRMED"
)

// TypePackageManifested is the event SLAM raises; Settings.Events looks for it.
const TypePackageManifested = "PackageManifested"

// slamToleranceKg is fulfillment-execution's SLAM weight tolerance (0.05, the unit of the two weights).
const slamToleranceKg = 0.05

// EventSeer is what the floor needs from the Kafka tap to see the one thing it never produces:
// PackageManifested. *sim.EventTap satisfies it (the tap must be tracking the ref: EventTap.Track).
type EventSeer interface {
	// Seen is how many integration events of the short type carried ref.
	Seen(ref, typ string) int
}

var _ EventSeer = (*sim.EventTap)(nil)

// ConfirmMode is how a picked line's reservation is confirmed in inventory-storage.
type ConfirmMode int

const (
	// ConfirmByEvent (the default) completes the PICK task and lets inventory-storage consume
	// TaskCompleted (its ADR 0035/0036), which is what the fleet does today. The floor then
	// READS the reservations after SLAM and reports one that is not CONFIRMED.
	ConfirmByEvent ConfirmMode = iota
	// ConfirmExplicit also calls POST /reservations/{id}/pick-confirmations before completing the
	// task. That route is superseded by the consumer ("removal planned with the next breaking
	// window"), so it is opt-in; it reads the reservation first and only confirms an ACTIVE one.
	ConfirmExplicit
)

// Scale is the SLAM scale: what the package weighs and what the system expects it to weigh, in
// kilograms. fulfillment-execution diverts the package when they differ by more than 0.05.
type Scale interface {
	Weigh(orderID string, contents []string) (actualKg, expectedKg float64)
}

// StandardScale weighs every unit at UnitKg and the carton at TareKg; the package weighs what it
// should.
type StandardScale struct{ UnitKg, TareKg float64 }

// Weigh implements Scale.
func (s StandardScale) Weigh(_ string, contents []string) (float64, float64) {
	w := s.TareKg + s.UnitKg*float64(len(contents))
	return w, w
}

// Settings tune the floor. Every field is optional.
type Settings struct {
	// Site is the building the floor works in (a facility-layout site code, e.g. "AUS3"). When set,
	// an order whose fulfillmentSiteId is empty or another building is a finding: an order without
	// its site yields a PackageManifested outbound-shipping ignores (retail-day-journey G2).
	Site string
	// Lag bounds the waits that depend on a consumer in another service (an order released,
	// the work units created, the WorkReleased task created, the reservation confirmed).
	// Default 60 s polled every 500 ms.
	Lag Waits
	// Work bounds the waits that depend on other people's pace (a free station, room under the
	// WIP limit, a task another station holds). It is a bound on waiting WITHOUT progress: every
	// task worked restarts it. Default 5 min polled every 500 ms.
	Work Waits
	// PickCapability and PackCapability are the capabilities the stations hold and the PACK
	// task requires. Default "pick" and "pack", what the path catalogue requires.
	PickCapability, PackCapability string
	// WorkCentres are facility-layout WorkCenter location codes the stations are installed on,
	// round robin. Empty installs them without a location (the service then checks nothing).
	WorkCentres []string
	// ConfirmPicks: see ConfirmMode.
	ConfirmPicks ConfirmMode
	// LeaveForeign leaves a PICK task that belongs to nobody this floor knows CLAIMED (its lease
	// expires on its own, after fulfillment-execution's sweep) and reports it. The default WORKS it
	// instead: pull dispatch hands a station the earliest-CPT task whoever it belongs to, a lease
	// cannot be handed back, and a picker who is given a task works it (cmd/warehouse-day clears
	// the queue the same way). Completion is what makes inventory-storage confirm the pick.
	LeaveForeign bool
	// MaxForeignReleases bounds how many work units of orders this floor does not know the release
	// operator admits ahead of its own before it stops and reports. Default 200.
	MaxForeignReleases int
	// Scale is the SLAM scale. Default StandardScale{UnitKg: 0.5, TareKg: 0.25}.
	Scale Scale
	// Events, when set, makes Process wait until PackageManifested of the order was seen. The tap
	// must track the order id (the manifest's source_order_id).
	Events EventSeer
}

func (s Settings) withDefaults() Settings {
	s.Lag = s.Lag.orDefault(Waits{Timeout: 60 * time.Second, Interval: 500 * time.Millisecond})
	s.Work = s.Work.orDefault(Waits{Timeout: 5 * time.Minute, Interval: 500 * time.Millisecond})
	if s.PickCapability == "" {
		s.PickCapability = "pick"
	}
	if s.PackCapability == "" {
		s.PackCapability = "pack"
	}
	if s.MaxForeignReleases <= 0 {
		s.MaxForeignReleases = 200
	}
	if s.Scale == nil {
		s.Scale = StandardScale{UnitKg: 0.5, TareKg: 0.25}
	}
	return s
}

// Stats counts what the floor did that was not for the order that asked for it.
type Stats struct {
	// ForeignReleased: work units of other orders the release operator admitted because pull
	// release gave it no choice.
	ForeignReleased int
	// ForeignWorked: PICK tasks of nobody this floor knows that a picker completed.
	ForeignWorked int
	// ForeignLeft: tasks of nobody this floor knows that were left CLAIMED.
	ForeignLeft int
}

// Floor is the crew of one building's pick-to-SLAM floor. Provision it once, then Process orders;
// Process is safe for concurrent use (that is how a shared queue is exercised) and shares the
// stations: a picker works one task at a time.
type Floor struct {
	c     *sim.Client
	clock Clock
	log   sim.LogFunc
	runID string
	set   Settings

	mu       sync.Mutex
	pickers  []*station
	packers  []*station
	orders   map[string]*orderState // by order id
	units    map[string]*lineState  // by WES work unit id
	findings []string
	suppress int

	foreignReleased, foreignWorked, foreignLeft atomic.Int64
}

// maxFindings caps the floor-level findings kept; the rest are only counted.
const maxFindings = 50

// New returns a floor over c. runID namespaces stations and occupants: short, upper-case,
// run-unique. clock may be nil (the wall clock); log may be nil.
func New(c *sim.Client, clock Clock, log sim.LogFunc, runID string, s Settings) *Floor {
	if clock == nil {
		clock = SystemClock{}
	}
	return &Floor{c: c, clock: clock, log: log, runID: runID, set: s.withDefaults(),
		orders: map[string]*orderState{}, units: map[string]*lineState{}}
}

func (f *Floor) logf(who, format string, args ...any) {
	if f.log != nil {
		f.log(who, format, args...)
	}
}

// Stats returns what the floor did for nobody in particular.
func (f *Floor) Stats() Stats {
	return Stats{ForeignReleased: int(f.foreignReleased.Load()), ForeignWorked: int(f.foreignWorked.Load()), ForeignLeft: int(f.foreignLeft.Load())}
}

// Findings are the floor-level findings: things that happened to no single order (a task of
// somebody else left CLAIMED, a station that would not check out).
func (f *Floor) Findings() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.findings...)
}

func (f *Floor) finding(format string, args ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.findings) >= maxFindings {
		f.suppress++
		return
	}
	f.findings = append(f.findings, fmt.Sprintf(format, args...))
}

func (f *Floor) call(ctx context.Context, svc, method, route, path string, body any) sim.Resp {
	return f.c.Call(ctx, svc, method, route, path, body)
}
