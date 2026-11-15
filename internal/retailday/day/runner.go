package day

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
)

// Config is how one day is played.
type Config struct {
	Plan PlanConfig
	// Parallel bounds every concurrent phase: shoppers, the floor's orders, the loads at the dock,
	// the parcels in transit and the evidence reads. Default 4.
	Parallel int
	// AllowUnverified turns the findings "the evidence could not establish this" from failures
	// into warnings. Default off: a day judged by evidence that cannot read is a failed day.
	// It does not hide a rule that failed because a fact could not be read.
	AllowUnverified bool
	// Notes are printed in the report as they are (for example that the run started outside the
	// release window and what that means for the wait).
	Notes []string
	// Now is the clock of the report and of Placed.PlacedAt. Default time.Now.
	Now func() time.Time
	// Log receives progress lines; it must be safe for concurrent use. Nil is silent.
	Log func(format string, args ...any)
}

func (c Config) withDefaults() Config {
	if c.Parallel < 1 {
		c.Parallel = 4
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	if c.Log == nil {
		c.Log = func(string, ...any) {}
	}
	return c
}

// Runner plays the retail day against its phase interfaces and judges it with
// retailday.AuditOrder and retailday.AuditEstate.
type Runner struct {
	Phases
	Config Config
}

// journey is one buyer's way through the day. Exactly one goroutine owns it at a time.
type journey struct {
	buyer  Buyer
	label  string // the order id, or buyer-NN until there is one
	placed retailday.Placed
	bought Bought
	ran    bool // Buy returned an answer

	omID        string // the order-management order raised for the PO (Linker): the floor's id
	pkg         *Package
	deliveredAt time.Time

	stopped bool // a phase failed: the journey goes no further
	judged  bool // AuditOrder ran on facts the evidence returned

	facts    retailday.OrderFacts
	gotFacts bool

	findings []retailday.Finding
	notes    []string
	evNotes  []string
}

func (j *journey) fail(rule, format string, a ...any) {
	j.findings = append(j.findings, retailday.Finding{Order: j.label, Rule: rule, Detail: fmt.Sprintf(format, a...)})
}

func (j *journey) warn(rule, format string, a ...any) {
	j.findings = append(j.findings, retailday.Finding{Order: j.label, Rule: rule, Warn: true, Detail: fmt.Sprintf(format, a...)})
}

func (j *journey) note(format string, a ...any) { j.notes = append(j.notes, fmt.Sprintf(format, a...)) }

func (j *journey) stop() { j.stopped = true }

type run struct {
	r   *Runner
	ctx context.Context
	cfg Config
	rep Report

	plan       Plan
	planned    bool
	js         []*journey
	before     map[string]retailday.Shelf
	restocked  map[string]int
	estate     []retailday.Finding // findings of the day's own phases, before the audit's
	phaseOrder map[string]int
}

// guard runs one call into an actor and turns a panic into an error: an adapter that panics ends
// that call's work with a finding, never the run.
func guard(what string, fn func() error) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("%s panicked: %v", what, p)
		}
	}()
	return fn()
}

// Run plays the day and returns its report. It never panics and never returns early without a
// report: a phase that cannot run is a failed phase, an order that cannot go on ends with its
// findings, and the other orders go on. A cancelled context ends every phase promptly and fails
// the day.
func (r *Runner) Run(ctx context.Context) (rep Report) {
	s := &run{r: r, ctx: ctx, cfg: r.Config.withDefaults(), phaseOrder: map[string]int{}}
	for i, p := range requiredPhases {
		s.phaseOrder[p] = i
	}
	s.rep = Report{RunID: s.cfg.Plan.RunID, Seed: s.cfg.Plan.Seed, AllowUnverified: s.cfg.AllowUnverified, Notes: slices.Clone(s.cfg.Notes), StartedAt: s.cfg.Now()}
	defer func() {
		s.cleanup()
		rep = s.finish()
	}()

	if missing := r.missing(); len(missing) > 0 {
		s.setPhase(PhasePrepare, PhaseFailed, "not wired: "+strings.Join(missing, ", "))
		s.abort(PhasePlan, "the day is not wired")
		return
	}
	steps := []struct {
		name  string
		fatal bool
		fn    func() (string, string)
	}{
		{PhasePrepare, true, s.prepare},
		{PhasePlan, true, s.makePlan},
		{PhaseShelf, true, s.readShelf},
		{PhaseShoppers, false, s.shoppers},
		{PhaseLink, false, s.link},
		{PhaseFloor, false, s.floor},
		{PhaseShip, false, s.ship},
		{PhaseDeliver, false, s.deliver},
		{PhaseRestock, false, s.restock},
		{PhaseEvidence, false, s.evidence},
		{PhaseEstate, false, s.audit},
	}
	for _, st := range steps {
		if err := ctx.Err(); err != nil {
			s.abort(st.name, "the context ended: "+err.Error())
			return
		}
		state, detail := st.fn()
		s.setPhase(st.name, state, detail)
		s.cfg.Log("%s: %s %s", st.name, state, detail)
		if state == PhaseFailed && st.fatal {
			s.abort(requiredPhases[s.phaseOrder[st.name]+1], st.name+" failed: "+detail)
			return
		}
	}
	return
}

// missing names the phases the runner was not given: a day with a hole in it is not played.
func (r *Runner) missing() []string {
	var out []string
	add := func(nilIface bool, name string) {
		if nilIface {
			out = append(out, name)
		}
	}
	add(r.Catalogue == nil, "Catalogue")
	add(r.Shopper == nil, "Shopper")
	add(r.Linker == nil, "Linker")
	add(r.Floor == nil, "Floor")
	add(r.Shipper == nil, "Shipper")
	add(r.Carrier == nil, "Carrier")
	add(r.Restocker == nil, "Restocker")
	add(r.Evidence == nil, "Evidence")
	return out
}

func (s *run) setPhase(name, state, detail string) {
	ps := PhaseStatus{Name: name, State: state, Detail: detail}
	for i := range s.rep.Phases {
		if s.rep.Phases[i].Name == name {
			s.rep.Phases[i] = ps
			return
		}
	}
	s.rep.Phases = append(s.rep.Phases, ps)
}

func (s *run) hasPhase(name string) bool {
	for _, p := range s.rep.Phases {
		if p.Name == name {
			return true
		}
	}
	return false
}

// abort ends the day at a phase: it and every later phase that has not run are aborted, and every
// order the plan made is told why nothing was done for it.
func (s *run) abort(from, why string) {
	for _, name := range requiredPhases[s.phaseOrder[from]:] {
		if !s.hasPhase(name) {
			s.setPhase(name, PhaseAborted, why)
		}
	}
	for _, j := range s.js {
		j.stop()
		if !j.judged && retailday.FailedFindings(j.findings) == 0 {
			j.fail("phase/not-run", "no work was done for this order: %s", why)
		}
	}
}

// cleanup runs the Cleanup hook on a context that outlives a cancelled run.
func (s *run) cleanup() {
	if s.r.Cleanup == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(s.ctx), 30*time.Second)
	defer cancel()
	if err := guard("cleanup", func() error { return s.r.Cleanup(ctx) }); err != nil {
		s.estate = append(s.estate, retailday.Finding{Order: "-", Rule: "phase/cleanup", Warn: true, Detail: "cleanup did not complete (stations may stay checked in): " + err.Error()})
	}
}

// ---- 0. prepare ----

func (s *run) prepare() (string, string) {
	if s.r.Prepare == nil {
		return PhaseIdle, "nothing to prepare"
	}
	if err := guard("prepare", func() error { return s.r.Prepare(s.ctx) }); err != nil {
		return PhaseFailed, "the building was not prepared: " + err.Error()
	}
	return PhaseOK, "the building is ready"
}

// ---- 1. the plan ----

func (s *run) makePlan() (string, string) {
	var items []Item
	if err := guard("catalogue", func() (e error) { items, e = s.r.Catalogue.Items(s.ctx); return }); err != nil {
		return PhaseFailed, "the catalogue could not be read: " + err.Error()
	}
	plan, err := BuildPlan(s.cfg.Plan, items)
	if err != nil {
		return PhaseFailed, err.Error()
	}
	s.plan, s.planned = plan, true
	s.rep.Shelf = plan.ShelfSKUs()
	s.rep.Planned = len(plan.Buyers)
	for _, b := range plan.Buyers {
		j := &journey{buyer: b, label: b.Label(), placed: b.Placed}
		j.placed.SKUs = copyMap(b.Placed.SKUs)
		s.js = append(s.js, j)
	}
	return PhaseOK, fmt.Sprintf("%d buyers, a shelf of %d SKUs, seed %d", len(plan.Buyers), len(plan.Shelf), plan.Seed)
}

// ---- 2. the shelf before ----

func (s *run) readShelf() (string, string) {
	skus := s.plan.ShelfSKUs()
	var got map[string]retailday.Shelf
	if err := guard("evidence", func() (e error) { got, e = s.r.Evidence.Shelf(s.ctx, skus); return }); err != nil {
		s.setPhase(PhasePlan, PhaseFailed, "the shelf's stock could not be read, so the day's demand cannot be checked: "+err.Error())
		return PhaseFailed, err.Error()
	}
	usable := map[string]int{}
	for _, sku := range skus {
		if sh, ok := got[sku]; ok {
			usable[sku] = sh.Usable
		}
	}
	if err := s.plan.CheckDemand(usable); err != nil {
		s.setPhase(PhasePlan, PhaseFailed, err.Error())
		return PhaseFailed, "the plan was refused"
	}
	s.before = got
	return PhaseOK, fmt.Sprintf("%d SKUs read", len(skus))
}

// ---- 3. the shoppers ----

func (s *run) shoppers() (string, string) {
	sem := make(chan struct{}, s.cfg.Parallel)
	var wg sync.WaitGroup
	for _, j := range s.js {
		select {
		case sem <- struct{}{}:
		case <-s.ctx.Done():
			j.stop()
			j.fail("phase/shoppers", "not run: the context ended before this shopper started")
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			s.buy(j)
		}()
	}
	wg.Wait()
	if err := s.ctx.Err(); err != nil {
		return PhaseAborted, "interrupted: " + err.Error()
	}
	placed, refused := 0, 0
	for _, j := range s.js {
		if j.bought.OrderID != "" {
			placed++
		}
		if j.stopped {
			refused++
		}
	}
	return PhaseOK, fmt.Sprintf("%d buyers, %d orders placed, %d ended off plan", len(s.js), placed, refused)
}

// wantStatus is what a shopper must end with for each outcome the plan expects.
var wantStatus = map[retailday.Outcome]Status{
	retailday.Delivered: StatusPlaced,
	retailday.Cancelled: StatusCancelled,
	retailday.Declined:  StatusDeclined,
	retailday.NotServed: StatusNotServed,
	retailday.InDoubt:   StatusInDoubt,
}

func (s *run) buy(j *journey) {
	var b Bought
	if err := guard("shopper", func() (e error) { b, e = s.r.Shopper.Buy(s.ctx, j.buyer); return }); err != nil {
		j.stop()
		j.fail("phase/shopper", "the purchase did not run: %v", err)
		return
	}
	j.ran, j.bought = true, b
	j.placed.OrderID = b.OrderID
	j.placed.TotalMinor, j.placed.AuthorizationID, j.placed.PromiseEnd = b.TotalMinor, b.AuthorizationID, b.PromiseEnd
	j.placed.PlacedAt = s.cfg.Now()
	if b.OrderID != "" {
		j.label = b.OrderID
	}
	s.cfg.Log("%s: %s %s", j.label, j.buyer.Expect, b.Status)

	want := wantStatus[j.buyer.Expect]
	switch {
	case b.Status != want:
		j.stop()
		j.fail("shopper/outcome", "the shopper ended %q, the plan needs %q for an order expected %s%s", b.Status, want, j.buyer.Expect, problemTail(b.Problems))
	case j.buyer.Expect == retailday.Delivered || j.buyer.Expect == retailday.Cancelled:
		if b.OrderID == "" {
			j.stop()
			j.fail("shopper/order-id", "the shopper reports %q but no order id came back", b.Status)
		}
	case b.OrderID != "":
		j.stop()
		j.fail("shopper/outcome", "an order %s was placed for a buyer expected %s: nothing may exist for it", b.OrderID, j.buyer.Expect)
	}
	if b.CancelRefused != "" {
		j.stop()
		j.fail("shopper/cancel-refused", "the cancellation was refused (%s): the purchase order was already accepted, so the order goes on and the plan's cancellation cannot be judged", b.CancelRefused)
	}
	switch {
	case len(b.Problems) == 0:
	case b.Status != want:
		// the outcome finding above carries them
	case j.buyer.Expect == retailday.InDoubt:
		j.warn("shopper/in-doubt", "%s", strings.Join(b.Problems, "; "))
	default:
		j.fail("shopper/problems", "%d request(s) were refused or disagreed: %s", len(b.Problems), strings.Join(firstN(b.Problems, 3), "; "))
	}
	if j.buyer.Expect == retailday.Cancelled && b.Status == StatusCancelled {
		j.note("cancellation accepted; the order read %q (settled: %v)", b.OrderStatus, b.CancelSettled)
	}
	if !j.stopped && j.buyer.Expect == retailday.Delivered {
		j.note("placed order %s for %d minor units", b.OrderID, b.TotalMinor)
	}
}

func problemTail(ps []string) string {
	if len(ps) == 0 {
		return ""
	}
	return ": " + strings.Join(firstN(ps, 3), "; ")
}

func firstN(in []string, n int) []string {
	if len(in) <= n {
		return in
	}
	return append(slices.Clone(in[:n]), fmt.Sprintf("(+%d more)", len(in)-n))
}

// ---- 4. the link ----

// link finds, for each order placed for delivery, the order-management order raised for its
// purchase order. Without it the floor would be handed the storefront's id, which order-management
// has never heard of.
func (s *run) link() (string, string) {
	var in []*journey
	for _, j := range s.js {
		if j.buyer.Expect == retailday.Delivered && !j.stopped && j.bought.Status == StatusPlaced && j.bought.OrderID != "" {
			in = append(in, j)
		}
	}
	if len(in) == 0 {
		return PhaseIdle, "no order was placed for delivery"
	}
	sem := make(chan struct{}, s.cfg.Parallel)
	var wg sync.WaitGroup
	for _, j := range in {
		select {
		case sem <- struct{}{}:
		case <-s.ctx.Done():
			j.stop()
			j.fail("phase/link", "not run: the context ended before this order was linked")
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			s.linkOne(j)
		}()
	}
	wg.Wait()
	if err := s.ctx.Err(); err != nil {
		return PhaseAborted, "interrupted: " + err.Error()
	}
	linked := 0
	for _, j := range in {
		if j.omID != "" {
			linked++
		}
	}
	if linked == 0 {
		return PhaseFailed, fmt.Sprintf("none of %d orders reached order-management", len(in))
	}
	return PhaseOK, fmt.Sprintf("%d of %d orders found in order-management", linked, len(in))
}

func (s *run) linkOne(j *journey) {
	var ids []string
	if err := guard("linker", func() (e error) { ids, e = s.r.Linker.OrderIDs(s.ctx, j.placed); return }); err != nil {
		j.stop()
		j.fail("link/order", "no order-management order was found for the order: %v", err)
		return
	}
	ids = slices.Compact(slices.Sorted(slices.Values(ids)))
	if len(ids) == 0 {
		j.stop()
		j.fail("link/order", "no order-management order was raised for the order")
		return
	}
	if len(ids) > 1 {
		// the floor works the first so the rest of the chain is still exercised; the evidence
		// reports the duplicate on its own
		j.fail("link/one-order", "%d order-management orders were raised for one order (%s), want 1; the floor works %s", len(ids), strings.Join(ids, ", "), ids[0])
	}
	j.omID = ids[0]
	j.note("order-management order %s", j.omID)
}

// ---- 5. the floor ----

func (s *run) floor() (string, string) {
	var in []*journey
	var ids []string
	for _, j := range s.js {
		if j.buyer.Expect == retailday.Delivered && !j.stopped && j.bought.Status == StatusPlaced && j.omID != "" {
			in, ids = append(in, j), append(ids, j.omID)
		}
	}
	if len(in) == 0 {
		return PhaseIdle, "no order reached the floor"
	}
	var res []FloorResult
	err := guard("floor", func() error { res = s.r.Floor.ProcessAll(s.ctx, slices.Clone(ids), s.cfg.Parallel); return nil })
	if err == nil && len(res) != len(ids) {
		err = fmt.Errorf("the floor answered %d results for %d orders", len(res), len(ids))
	}
	if err != nil {
		for _, j := range in {
			j.stop()
			j.fail("phase/floor", "the floor could not run: %v", err)
		}
		return PhaseFailed, err.Error()
	}
	labelled := 0
	for i, j := range in {
		r := res[i]
		if r.OrderID != "" && r.OrderID != ids[i] {
			j.stop()
			j.fail("phase/floor", "the floor answered order %s in the place of %s", r.OrderID, ids[i])
			continue
		}
		for _, p := range r.Problems {
			j.fail("phase/floor", "%s", p)
		}
		if !r.Labeled() {
			j.stop()
			rule := "floor/not-labeled"
			if r.Status == "DIVERTED" {
				rule = "floor/diverted"
			}
			j.fail(rule, "no labelled package came out of the floor: package %q is %q", r.PackageID, r.Status)
			continue
		}
		j.pkg = &Package{OrderID: j.bought.OrderID, PackageID: r.PackageID, OrderRef: orElse(r.OrderRef, j.omID), SourceOrderID: orElse(r.SourceOrderID, j.omID), SiteID: r.SiteID, CPT: r.CPT, WeightG: r.WeightG}
		j.note("package %s labelled (cpt %s)", r.PackageID, r.CPT)
		labelled++
	}
	if err := s.ctx.Err(); err != nil {
		return PhaseAborted, "interrupted: " + err.Error()
	}
	return PhaseOK, fmt.Sprintf("%d of %d orders labelled", labelled, len(in))
}

// copyMap hands a phase its own copy: no phase may change what the run later compares against.
func copyMap(in map[string]int) map[string]int {
	out := make(map[string]int, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func orElse(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// ---- 5. the dock ----

func (s *run) ship() (string, string) {
	groups := map[string][]*journey{}
	for _, j := range s.js {
		if !j.stopped && j.pkg != nil {
			key := j.pkg.SiteID + "|" + j.pkg.CPT
			groups[key] = append(groups[key], j)
		}
	}
	if len(groups) == 0 {
		return PhaseIdle, "no labelled package to ship"
	}
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	sem := make(chan struct{}, s.cfg.Parallel)
	var wg sync.WaitGroup
	var mu sync.Mutex
	unusable := 0
	for _, k := range keys {
		select {
		case sem <- struct{}{}:
		case <-s.ctx.Done():
			for _, j := range groups[k] {
				j.stop()
				j.fail("phase/ship", "not run: the context ended before this load was handled")
			}
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if !s.shipGroup(groups[k]) {
				mu.Lock()
				unusable++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if err := s.ctx.Err(); err != nil {
		return PhaseAborted, "interrupted: " + err.Error()
	}
	shipped := 0
	for _, j := range s.js {
		if j.pkg != nil && !j.stopped {
			shipped++
		}
	}
	if unusable > 0 {
		return PhaseFailed, fmt.Sprintf("%d of %d load(s) got no usable answer from the dock", unusable, len(keys))
	}
	return PhaseOK, fmt.Sprintf("%d package(s) in %d load(s) shipped", shipped, len(keys))
}

// shipGroup ships the packages that share a load in one call; it reports whether the dock gave a
// usable answer.
func (s *run) shipGroup(js []*journey) bool {
	pkgs := make([]Package, len(js))
	for i, j := range js {
		pkgs[i] = *j.pkg
	}
	var out []Shipment
	err := guard("shipper", func() error { out = s.r.Shipper.ShipAll(s.ctx, pkgs); return nil })
	if err == nil && len(out) != len(js) {
		err = fmt.Errorf("the dock answered %d shipments for %d packages", len(out), len(js))
	}
	if err != nil {
		for _, j := range js {
			j.stop()
			j.fail("phase/ship", "the dock could not run: %v", err)
		}
		return false
	}
	for i, j := range js {
		sh := out[i]
		for _, p := range sh.Problems {
			j.fail("phase/ship", "%s", p)
		}
		if !sh.Departed || !sh.Confirmed {
			j.stop()
			j.fail("ship/not-confirmed", "package %s did not leave the building confirmed (departed %v, confirmed %v)", j.pkg.PackageID, sh.Departed, sh.Confirmed)
			continue
		}
		j.note("shipped on load %s", sh.LoadID)
	}
	return true
}

// ---- 6. the doorstep ----

func (s *run) deliver() (string, string) {
	var in []*journey
	for _, j := range s.js {
		if !j.stopped && j.pkg != nil {
			in = append(in, j)
		}
	}
	if len(in) == 0 {
		return PhaseIdle, "no shipped order to deliver"
	}
	sem := make(chan struct{}, s.cfg.Parallel)
	var wg sync.WaitGroup
	for _, j := range in {
		select {
		case sem <- struct{}{}:
		case <-s.ctx.Done():
			j.stop()
			j.fail("phase/carrier", "not run: the context ended before this order was delivered")
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			s.deliverOne(j)
		}()
	}
	wg.Wait()
	if err := s.ctx.Err(); err != nil {
		return PhaseAborted, "interrupted: " + err.Error()
	}
	delivered := 0
	for _, j := range in {
		if !j.deliveredAt.IsZero() {
			delivered++
		}
	}
	return PhaseOK, fmt.Sprintf("%d of %d orders delivered", delivered, len(in))
}

func (s *run) deliverOne(j *journey) {
	var d Delivery
	// the floor makes one package per order (one pack task): that is the parcel's size
	if err := guard("carrier", func() error { d = s.r.Carrier.DeliverOrder(s.ctx, j.bought.OrderID, 1); return nil }); err != nil {
		j.stop()
		j.fail("phase/carrier", "the carrier could not run: %v", err)
		return
	}
	for _, p := range d.Problems {
		j.fail("phase/carrier", "%s", p)
	}
	if !d.Delivered() {
		j.stop()
		j.fail("carrier/not-delivered", "the carrier never delivered the parcel")
		return
	}
	j.deliveredAt = d.DeliveredAt
	j.note("delivered at %s", d.DeliveredAt.UTC().Format(time.RFC3339))
}

// ---- 7. the restock ----

func (s *run) restock() (string, string) {
	start := map[string]int{}
	for _, sku := range s.plan.ShelfSKUs() {
		start[sku] = s.before[sku].Usable
	}
	var res RestockResult
	if err := guard("restocker", func() (e error) { res, e = s.r.Restocker.Restock(s.ctx, copyMap(start)); return }); err != nil {
		s.estate = append(s.estate, retailday.Finding{Order: "-", Rule: "phase/restock", Detail: "the restock leg could not run: " + err.Error()})
		return PhaseFailed, err.Error()
	}
	s.restocked = res.Restocked
	for _, p := range res.Problems {
		s.estate = append(s.estate, retailday.Finding{Order: "-", Rule: "phase/restock", Detail: p})
	}
	units := 0
	for _, n := range res.Restocked {
		units += n
	}
	if err := s.ctx.Err(); err != nil {
		return PhaseAborted, "interrupted: " + err.Error()
	}
	return PhaseOK, fmt.Sprintf("%d unit(s) of %d SKU(s) received and stowed, %d SKU(s) still short", units, len(res.Restocked), len(res.Short))
}

// ---- 8. the evidence, order by order ----

func (s *run) evidence() (string, string) {
	sem := make(chan struct{}, s.cfg.Parallel)
	var wg sync.WaitGroup
	read := 0
	for _, j := range s.js {
		// A buyer that never got an order cannot have one to look up, unless the plan said
		// nothing may exist: then "nothing exists" is exactly what must be read.
		if !j.ran || ((j.buyer.Expect == retailday.Delivered || j.buyer.Expect == retailday.Cancelled) && j.bought.OrderID == "") {
			continue
		}
		select {
		case sem <- struct{}{}:
		case <-s.ctx.Done():
			continue
		}
		read++
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			s.judge(j)
		}()
	}
	wg.Wait()
	if err := s.ctx.Err(); err != nil {
		return PhaseAborted, "interrupted: " + err.Error()
	}
	unread := 0
	for _, j := range s.js {
		s.rep.NotVerified = append(s.rep.NotVerified, prefixed(j.label, j.evNotes)...)
		unread += len(j.evNotes)
	}
	return PhaseOK, fmt.Sprintf("%d order(s) read, %d note(s) of facts that could not be read", read, unread)
}

func prefixed(label string, notes []string) []string {
	out := make([]string, len(notes))
	for i, n := range notes {
		out[i] = label + ": " + n
	}
	return out
}

func (s *run) judge(j *journey) {
	var facts retailday.OrderFacts
	var notes []string
	if err := guard("evidence", func() error { facts, notes = s.r.Evidence.Order(s.ctx, j.placed); return nil }); err != nil {
		j.fail("phase/evidence", "the evidence could not be read: %v", err)
		return
	}
	// The plan is the authority on what the order must become and the shopper on its id.
	facts.Expect = j.placed.Expect
	facts.OrderID = j.label
	if facts.Lines == 0 {
		facts.Lines = j.placed.Lines()
	}
	j.facts, j.gotFacts, j.judged = facts, true, true
	j.findings = append(j.findings, retailday.AuditOrder(facts)...)
	if len(notes) > 0 {
		j.evNotes = notes
		detail := fmt.Sprintf("%d fact(s) could not be read: %s", len(notes), strings.Join(firstN(notes, 3), "; "))
		if s.cfg.AllowUnverified {
			j.warn("evidence/not-verified", "%s", detail)
		} else {
			j.fail("evidence/not-verified", "%s", detail)
		}
	}
}

// ---- 9. the estate ----

func (s *run) audit() (string, string) {
	skus := s.plan.ShelfSKUs()
	var after map[string]retailday.Shelf
	var stock []retailday.StockFacts
	shelfErr := guard("evidence", func() (e error) { after, e = s.r.Evidence.Shelf(s.ctx, skus); return })
	if shelfErr != nil {
		s.estate = append(s.estate, retailday.Finding{Order: "-", Rule: "stock/unreadable", Detail: "the shelf could not be read after the day: " + shelfErr.Error()})
		s.rep.NotVerified = append(s.rep.NotVerified, "stock: the shelf could not be read after the day, so conservation of stock was not checked")
	}
	shipped := map[string]int{}
	for _, j := range s.js {
		if j.buyer.Expect == retailday.Delivered && j.gotFacts && j.facts.Status == "DELIVERED" {
			for sku, n := range j.placed.SKUs {
				shipped[sku] += n
			}
		}
	}
	for _, sku := range skus {
		sh, ok := after[sku]
		if shelfErr == nil && !ok {
			s.estate = append(s.estate, retailday.Finding{Order: "-", Rule: "stock/unreadable", Detail: sku + ": no reading after the day"})
			continue
		}
		if !ok {
			continue
		}
		b := s.before[sku]
		if sh.Usable < b.Usable {
			// Conservation (before - shipped + restocked = after) holds for a restock that did
			// nothing and said so; the day's own rule is that it ends at or above where it began.
			s.estate = append(s.estate, retailday.Finding{Order: "-", Rule: "stock/restocked", Detail: fmt.Sprintf("%s ends at %d usable units, below the %d it started with: the restock leg did not bring the shelf back (ADR 0001 D4)", sku, sh.Usable, b.Usable)})
		}
		stock = append(stock, retailday.StockFacts{SKU: sku, UsableBefore: b.Usable, UsableAfter: sh.Usable, Shipped: shipped[sku], Restocked: s.restocked[sku], AdvertisedNow: -1, Purchasable: sh.Purchasable})
		s.rep.Stock = append(s.rep.Stock, StockRow{SKU: sku, Before: b.Usable, After: sh.Usable, Shipped: shipped[sku], Restocked: s.restocked[sku], Purchasable: sh.Purchasable})
	}

	dead := 0
	var n int
	var known bool
	dlqErr := guard("evidence", func() (e error) { n, known, e = s.r.Evidence.DeadLetters(s.ctx, s.cfg.Plan.RunID); return })
	switch {
	case dlqErr != nil:
		s.unverified("kafka/dead-letters-unverified", "dead letters: the count failed: "+dlqErr.Error())
	case !known:
		s.unverified("kafka/dead-letters-unverified", "dead letters: no published route can count them for run "+s.cfg.Plan.RunID+", so \"none\" is not established")
	default:
		dead = n
	}

	serverErrors := 0
	if s.r.Calls == nil {
		s.unverified("http/5xx-unverified", "call statistics are not wired: 5xx and transport errors were not counted")
	} else {
		serverErrors = s.r.Calls.Count5xx()
		s.rep.ServerErrors = serverErrors
		s.rep.Failures = s.r.Calls.Failures()
		s.rep.Calls = s.r.Calls.Lines()
	}
	if s.r.Suspensions != nil {
		if k := len(s.r.Suspensions.Snapshot()); k > 0 {
			s.rep.Suspensions = k
			s.rep.NotVerified = append(s.rep.NotVerified, fmt.Sprintf("5xx: the host was suspended %d time(s); the call statistics keep no timestamps, so no 5xx can be attributed to a suspension and every one counts", k))
		}
	}
	s.estate = append(s.estate, retailday.AuditEstate(stock, dead, serverErrors, 0)...)
	return PhaseOK, fmt.Sprintf("%d SKU(s) accounted, %d dead letter(s), %d 5xx", len(stock), dead, serverErrors)
}

// unverified records something the day could not establish: a NOT VERIFIED line and a finding
// that fails the day unless the run allows unverified evidence.
func (s *run) unverified(rule, detail string) {
	s.rep.NotVerified = append(s.rep.NotVerified, detail)
	s.estate = append(s.estate, retailday.Finding{Order: "-", Rule: rule, Warn: s.cfg.AllowUnverified, Detail: detail})
}

// ---- the report ----

func (s *run) finish() Report {
	rep := s.rep
	rep.FinishedAt = s.cfg.Now()
	for _, j := range s.js {
		// An order nobody judged and that carries no failure would read as a pass: it never ran
		// the whole way, so it is a failure. (Every deliberate skip of the evidence comes with
		// its own failing finding.)
		if !j.judged && retailday.FailedFindings(j.findings) == 0 {
			j.fail("phase/evidence", "this order was never judged: the run did not reach the evidence for it")
		}
		rep.Orders = append(rep.Orders, row(j))
	}
	rep.Estate = s.estate
	sort.SliceStable(rep.Phases, func(a, b int) bool { return s.phaseOrder[rep.Phases[a].Name] < s.phaseOrder[rep.Phases[b].Name] })
	rep.Reasons = rep.failures()
	rep.Verdict = VerdictPassed
	if len(rep.Reasons) > 0 {
		rep.Verdict = VerdictFailed
	}
	return rep
}

func row(j *journey) OrderReport {
	o := OrderReport{Order: j.label, Buyer: j.buyer.Index, Expect: j.buyer.Expect, Outcome: OutcomePass, Findings: j.findings, Notes: slices.Clone(j.notes)}
	seen := map[string]bool{}
	for _, f := range j.findings {
		if f.Warn {
			if o.Outcome == OutcomePass {
				o.Outcome = OutcomeWarn
			}
			continue
		}
		o.Outcome = OutcomeFail
		if !seen[f.Rule] {
			seen[f.Rule] = true
			o.Rules = append(o.Rules, f.Rule)
		}
	}
	for _, n := range j.evNotes {
		o.Notes = append(o.Notes, "not verified: "+n)
	}
	return o
}
