package day

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
	"github.com/claudioed/warehouse-e2e/internal/sim"
)

// The test kit: a tiny warehouse-and-network WORLD and one stub per phase interface, all reading
// and writing it. A failure injected into one stub therefore changes what the evidence stub
// reports downstream, exactly as it would on the cluster; and a test that wants the verdict to
// rest on the runner's own bookkeeping swaps the evidence for one that always says "fine".

var epoch = time.Date(2026, 11, 14, 19, 0, 0, 0, time.UTC)

type stage int

const (
	stageNone stage = iota
	stagePlaced
	stageCancelled
	stageLabeled
	stageShipped
	stageDelivered
)

type world struct {
	mu     sync.Mutex
	runID  string
	stock  map[string]int
	buyers map[string]Buyer // by order id
	stage  map[string]stage
	// downstream is what a buyer with no order of its own nevertheless left behind (a bug to catch).
	downstream map[string]bool // by buyer label
}

func newWorld(items []Item, usable int) *world {
	w := &world{runID: "RD1", stock: map[string]int{}, buyers: map[string]Buyer{}, stage: map[string]stage{}, downstream: map[string]bool{}}
	for _, it := range items {
		w.stock[it.SKU] = usable
	}
	return w
}

func (w *world) usable(sku string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stock[sku]
}

func orderID(b Buyer) string { return fmt.Sprintf("ORD-%02d", b.Index) }

// ---- shopper ----

type stubShopper struct {
	w     *world
	calls []Buyer
	mu    sync.Mutex
	// onBuy, when set, answers instead of the default; it may call w.buy itself.
	onBuy func(ctx context.Context, b Buyer) (Bought, error)
}

func (s *stubShopper) Buy(ctx context.Context, b Buyer) (Bought, error) {
	s.mu.Lock()
	s.calls = append(s.calls, b)
	s.mu.Unlock()
	if s.onBuy != nil {
		return s.onBuy(ctx, b)
	}
	return s.w.buy(b), nil
}

func (s *stubShopper) called() []Buyer {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Buyer(nil), s.calls...)
}

func (w *world) buy(b Buyer) Bought {
	w.mu.Lock()
	defer w.mu.Unlock()
	id := orderID(b)
	switch b.Expect {
	case retailday.Delivered:
		w.buyers[id], w.stage[id] = b, stagePlaced
		return Bought{Status: StatusPlaced, OrderID: id, TotalMinor: 5000, AuthorizationID: "auth-" + id, PromiseEnd: epoch.Add(30 * time.Hour), OrderStatus: "PLACED"}
	case retailday.Cancelled:
		w.buyers[id], w.stage[id] = b, stageCancelled
		return Bought{Status: StatusCancelled, OrderID: id, TotalMinor: 5000, AuthorizationID: "auth-" + id, OrderStatus: "CANCELLED", CancelSettled: true}
	case retailday.Declined:
		return Bought{Status: StatusDeclined}
	case retailday.NotServed:
		return Bought{Status: StatusNotServed}
	default:
		return Bought{Status: StatusInDoubt, Problems: []string{"outcome in doubt: session still AUTHORIZING after 45s"}}
	}
}

// ---- floor ----

type stubFloor struct {
	w         *world
	mu        sync.Mutex
	calls     [][]string
	parallels []int
	onProcess func(ctx context.Context, id string) FloorResult
}

func (f *stubFloor) ProcessAll(ctx context.Context, ids []string, parallel int) []FloorResult {
	f.mu.Lock()
	f.calls = append(f.calls, append([]string(nil), ids...))
	f.parallels = append(f.parallels, parallel)
	f.mu.Unlock()
	out := make([]FloorResult, len(ids))
	for i, id := range ids {
		if f.onProcess != nil {
			out[i] = f.onProcess(ctx, id)
			continue
		}
		out[i] = f.w.label(id)
	}
	return out
}

func (f *stubFloor) ids() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, c := range f.calls {
		out = append(out, c...)
	}
	sort.Strings(out)
	return out
}

// label picks and packs: the stock leaves the shelf when the pick is confirmed.
func (w *world) label(id string) FloorResult {
	w.mu.Lock()
	defer w.mu.Unlock()
	b := w.buyers[id]
	for sku, n := range b.Placed.SKUs {
		w.stock[sku] -= n
	}
	w.stage[id] = stageLabeled
	return FloorResult{OrderID: id, PackageID: "PKG-" + id, Status: "LABELED", OrderRef: id, SourceOrderID: "OM-" + id, SiteID: "AUS3", CPT: cptFor(b)}
}

func cptFor(b Buyer) string {
	if b.Tier == TierTwoDay {
		return "2026-11-16T23:00:00Z"
	}
	return "2026-11-14T23:00:00Z"
}

// ---- shipper ----

type stubShipper struct {
	w      *world
	mu     sync.Mutex
	calls  [][]Package
	inFlt  int
	maxFlt int
	// onShip, when set, answers one package instead of the default.
	onShip func(p Package) Shipment
	hold   func() // called inside every ShipAll, to hold calls in flight
}

func (s *stubShipper) ShipAll(ctx context.Context, pkgs []Package) []Shipment {
	s.mu.Lock()
	s.calls = append(s.calls, append([]Package(nil), pkgs...))
	s.inFlt++
	s.maxFlt = max(s.maxFlt, s.inFlt)
	s.mu.Unlock()
	if s.hold != nil {
		s.hold()
	}
	defer func() { s.mu.Lock(); s.inFlt--; s.mu.Unlock() }()
	out := make([]Shipment, len(pkgs))
	for i, p := range pkgs {
		if s.onShip != nil {
			out[i] = s.onShip(p)
			continue
		}
		out[i] = s.w.ship(p)
	}
	return out
}

func (w *world) ship(p Package) Shipment {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.stage[p.OrderID] = stageShipped
	return Shipment{PackageID: p.PackageID, OrderRef: p.OrderRef, LoadID: "LOAD-" + p.CPT, Departed: true, Confirmed: true}
}

func (s *stubShipper) batches() [][]Package {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]Package(nil), s.calls...)
}

// ---- carrier ----

type stubCarrier struct {
	w         *world
	mu        sync.Mutex
	calls     []string
	pkgs      []int
	inFlt     int
	maxFlt    int
	onDeliver func(id string) Delivery
	hold      func()
}

func (c *stubCarrier) DeliverOrder(ctx context.Context, id string, packages int) Delivery {
	c.mu.Lock()
	c.calls = append(c.calls, id)
	c.pkgs = append(c.pkgs, packages)
	c.inFlt++
	c.maxFlt = max(c.maxFlt, c.inFlt)
	c.mu.Unlock()
	if c.hold != nil {
		c.hold()
	}
	defer func() { c.mu.Lock(); c.inFlt--; c.mu.Unlock() }()
	if c.onDeliver != nil {
		return c.onDeliver(id)
	}
	return c.w.deliver(id)
}

func (w *world) deliver(id string) Delivery {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.stage[id] = stageDelivered
	return Delivery{DeliveredAt: epoch.Add(2 * time.Hour)}
}

func (c *stubCarrier) delivered() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := append([]string(nil), c.calls...)
	sort.Strings(out)
	return out
}

// ---- restock ----

type stubRestocker struct {
	w         *world
	mu        sync.Mutex
	calls     []map[string]int
	onRestock func(start map[string]int) (RestockResult, error)
}

func (r *stubRestocker) Restock(ctx context.Context, start map[string]int) (RestockResult, error) {
	r.mu.Lock()
	r.calls = append(r.calls, copyMap(start))
	r.mu.Unlock()
	if r.onRestock != nil {
		return r.onRestock(start)
	}
	return r.w.restock(start), nil
}

func (w *world) restock(start map[string]int) RestockResult {
	w.mu.Lock()
	defer w.mu.Unlock()
	res := RestockResult{Restocked: map[string]int{}}
	for sku, want := range start {
		if short := want - w.stock[sku]; short > 0 {
			res.Restocked[sku] = short
			w.stock[sku] = want
		}
	}
	return res
}

func copyMap(in map[string]int) map[string]int {
	out := make(map[string]int, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// ---- evidence ----

type stubEvidence struct {
	w *world

	mu     sync.Mutex
	placed []retailday.Placed

	// onOrder, when set, answers instead of the world.
	onOrder    func(p retailday.Placed) (retailday.OrderFacts, []string)
	onShelf    func(skus []string) (map[string]retailday.Shelf, error)
	shelfCalls int
	dlq        func() (int, bool, error)
}

func (e *stubEvidence) Order(ctx context.Context, p retailday.Placed) (retailday.OrderFacts, []string) {
	e.mu.Lock()
	e.placed = append(e.placed, p)
	e.mu.Unlock()
	if e.onOrder != nil {
		return e.onOrder(p)
	}
	return e.w.facts(p), nil
}

func (e *stubEvidence) Shelf(ctx context.Context, skus []string) (map[string]retailday.Shelf, error) {
	e.mu.Lock()
	e.shelfCalls++
	e.mu.Unlock()
	if e.onShelf != nil {
		return e.onShelf(skus)
	}
	out := map[string]retailday.Shelf{}
	for _, sku := range skus {
		n := e.w.usable(sku)
		out[sku] = retailday.Shelf{Usable: n, Purchasable: n > 0}
	}
	return out, nil
}

func (e *stubEvidence) DeadLetters(context.Context, string) (int, bool, error) {
	if e.dlq != nil {
		return e.dlq()
	}
	return 0, true, nil
}

func (e *stubEvidence) seen() []retailday.Placed {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := append([]retailday.Placed(nil), e.placed...)
	sort.Slice(out, func(i, j int) bool {
		return out[i].OrderID+fmt.Sprint(out[i].Expect) < out[j].OrderID+fmt.Sprint(out[j].Expect)
	})
	return out
}

// facts is what the services would say about the order, given how far it really got.
func (w *world) facts(p retailday.Placed) retailday.OrderFacts {
	w.mu.Lock()
	defer w.mu.Unlock()
	f := retailday.OrderFacts{OrderID: p.OrderID, Expect: p.Expect, Lines: p.Lines()}
	if p.OrderID == "" {
		return f
	}
	st := w.stage[p.OrderID]
	f.TotalMinor, f.AuthorizedMinor = 5000, 5000
	f.Status = "PLACED"
	switch {
	case st == stageCancelled:
		f.Status, f.VoidedMinor = "CANCELLED", 5000
	case st >= stageLabeled:
		f.POs = []retailday.PO{{Number: "PO-" + p.OrderID, State: "SHIPPED"}}
		f.OMOrders = []retailday.OMOrder{{ID: "OM-" + p.OrderID, Status: "Released"}}
		f.WorkUnits, f.PickTasksDone, f.PackTasks = p.Lines(), p.Lines(), 1
		f.Packages = []retailday.Package{{ID: "PKG-" + p.OrderID, Outcome: "LABELED"}}
		f.Status = "IN_FULFILLMENT"
		if st >= stageShipped {
			f.OMOrders[0].Status = "Shipped"
			f.ShipmentConfirmations = 1
			f.TrackingNumbers = []string{"TN-" + p.OrderID}
			f.CapturedMinor = 5000
			f.Status = "SHIPPED"
		}
		if st >= stageDelivered {
			f.Status = "DELIVERED"
			f.DeliveredAt = epoch.Add(2 * time.Hour)
			f.PromiseEnd = epoch.Add(30 * time.Hour)
		}
	}
	return f
}

// ---- calls ----

type stubCalls struct {
	n5xx     int
	failures []string
	lines    []sim.RouteLine
}

func (c stubCalls) Count5xx() int          { return c.n5xx }
func (c stubCalls) Failures() []string     { return c.failures }
func (c stubCalls) Lines() []sim.RouteLine { return c.lines }

type stubSleeps struct{ windows []sim.Suspension }

func (s stubSleeps) Snapshot() []sim.Suspension { return s.windows }

// ---- catalogue ----

type stubCatalogue struct {
	items []Item
	err   error
	calls int
}

func (c *stubCatalogue) Items(context.Context) ([]Item, error) {
	c.calls++
	return c.items, c.err
}

// ---- the rig ----

type rig struct {
	w         *world
	items     []Item
	catalogue *stubCatalogue
	shopper   *stubShopper
	floor     *stubFloor
	shipper   *stubShipper
	carrier   *stubCarrier
	restocker *stubRestocker
	evidence  *stubEvidence
	calls     stubCalls

	prepared, cleaned int
	cleanCtxErr       error
	prepareErr        error
	runner            *Runner
}

func newRig() *rig {
	items := candidates(30)
	w := newWorld(items, 20)
	g := &rig{w: w, items: items}
	g.catalogue = &stubCatalogue{items: items}
	g.shopper = &stubShopper{w: w}
	g.floor = &stubFloor{w: w}
	g.shipper = &stubShipper{w: w}
	g.carrier = &stubCarrier{w: w}
	g.restocker = &stubRestocker{w: w}
	g.evidence = &stubEvidence{w: w}
	g.calls = stubCalls{lines: []sim.RouteLine{{Route: "storefront POST /checkout", Calls: 12, Statuses: map[int]int{200: 12}, AvgMs: 8}}}
	g.runner = &Runner{
		Phases: Phases{
			Catalogue: g.catalogue, Shopper: g.shopper, Floor: g.floor, Shipper: g.shipper,
			Carrier: g.carrier, Restocker: g.restocker, Evidence: g.evidence,
			Calls:   g.calls,
			Prepare: func(context.Context) error { g.prepared++; return g.prepareErr },
			Cleanup: func(ctx context.Context) error { g.cleaned++; g.cleanCtxErr = ctx.Err(); return nil },
		},
		Config: Config{
			Plan:     PlanConfig{RunID: "RD1", Seed: 1, Shoppers: 12, ShelfSize: 8},
			Parallel: 3,
			Now:      func() time.Time { return epoch },
		},
	}
	return g
}

// run plays the day on a context that ends the test's wait rather than hanging it.
func (g *rig) run(ctx context.Context) Report { return g.runner.Run(ctx) }

func (g *rig) plan() Plan {
	p, err := BuildPlan(g.runner.Config.Plan, g.items)
	if err != nil {
		panic(err)
	}
	return p
}

// buyer returns the n-th buyer of an outcome in plan order.
func (g *rig) buyer(e retailday.Outcome, n int) Buyer {
	for _, b := range g.plan().Buyers {
		if b.Expect == e {
			if n == 0 {
				return b
			}
			n--
		}
	}
	panic("no such buyer")
}

func (r Report) row(order string) OrderReport {
	for _, o := range r.Orders {
		if o.Order == order {
			return o
		}
	}
	return OrderReport{}
}

func (r Report) rowOf(buyer int) OrderReport {
	for _, o := range r.Orders {
		if o.Buyer == buyer {
			return o
		}
	}
	return OrderReport{}
}

func (r Report) hasRule(rule string) bool {
	for _, o := range r.Orders {
		for _, f := range o.Findings {
			if f.Rule == rule && !f.Warn {
				return true
			}
		}
	}
	for _, f := range r.Estate {
		if f.Rule == rule && !f.Warn {
			return true
		}
	}
	return false
}

func (r Report) phase(name string) PhaseStatus {
	for _, p := range r.Phases {
		if p.Name == name {
			return p
		}
	}
	return PhaseStatus{}
}

func (r Report) dump() string {
	var b strings.Builder
	r.Text(&b)
	return b.String()
}
