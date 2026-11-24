package day

import (
	"context"
	"fmt"
	"slices"
	"strings"
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
