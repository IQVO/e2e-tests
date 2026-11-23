package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------
// Kafka event tap
// ---------------------------------------------------------------------

var tappedTopics = []string{
	"warehouse.facility.events",
	"warehouse.product-master.events",
	"warehouse.inventory.events",
	"warehouse.process-path-management.events",
	"warehouse.work-planning.events",
	"warehouse.fulfillment.events",
	"warehouse.order-management.events",
	"warehouse.workforce.events",
	"warehouse.labor-performance.events",
	// The planner's morning (warehouse-planning ADR 0023):
	// WorkloadForecastReceived and LabourPlanCommitted.
	planningEventsTopic,
	// Analytics topics: most domain facts (StockReceived, TaskClaimed,
	// PackageSealed, AssociateShiftStarted, ...) are published ONLY on a
	// context's analytics stream, never on its integration topic (see each
	// repo's apis/asyncapi.yaml). Auditing the day must read both.
	"warehouse.facility.analytics",
	"warehouse.inventory.analytics",
	"warehouse.fulfillment.analytics",
	"warehouse.workforce.analytics",
	"warehouse.labor-performance.analytics",
	"warehouse.wes.analytics",
	"warehouse.order-management.analytics",
	"warehouse.process-path-management.analytics",
}

// ---------------------------------------------------------------------
// the day
// ---------------------------------------------------------------------

func (s *sim) run(ctx context.Context) bool {
	cfg := s.cfg
	fmt.Printf("\n=== warehouse day %s — %d orders, %d pickers, %d packers, %d rebin, %d SLAM, %d flex; day=%s ===\n\n",
		cfg.runID, cfg.orders, cfg.pickers, cfg.packers, cfg.rebinners, cfg.slammers, cfg.flex, cfg.day)

	if err := s.healthCheck(ctx); err != nil {
		return s.abortRun("", err)
	}
	tapCtx, stopTap := context.WithCancel(context.Background())
	defer stopTap()
	s.openTap(tapCtx)
	if s.tap != nil {
		defer s.tap.Close()
	}
	if !s.openTheBuilding(ctx) || !s.startTheShift(ctx) {
		return false
	}
	floorCtx, stopFloor := context.WithCancel(ctx)
	defer stopFloor()
	floor, office := s.launchTheFloor(floorCtx)
	s.trackOrdersInTap(floorCtx)
	s.drainTheBuilding(ctx)
	stopFloor()
	floor.Wait()
	office.Wait()
	logf("supervisor", "all associates checked out and shifts ended")

	// let the last events propagate before auditing
	sleepCtx(ctx, 8*time.Second)
	return s.audit(context.Background())
}

// abortRun logs why the day cannot go on, writes the report and reports a
// failed day. stage names the phase ("during opening", ...; "": none).
func (s *sim) abortRun(stage string, err error) bool {
	if stage == "" {
		logf("control-tower", "ABORT: %v", err)
	} else {
		logf("control-tower", "ABORT %s: %v", stage, err)
	}
	s.writeReport(false)
	return false
}

// openTap starts the Kafka event tap when brokers are configured; a tap that
// cannot open only skips the event audit.
func (s *sim) openTap(ctx context.Context) {
	if len(s.cfg.brokers) == 0 {
		return
	}
	tap, err := startTap(ctx, s.cfg.brokers, s.cfg.runID)
	if err != nil {
		s.finding("Kafka event tap unavailable (%v) — event audit skipped", err)
		return
	}
	s.tap = tap
}

// openTheBuilding runs the 05:30 opening (staffing, engineering setup, the
// building map, the product catalogue, inbound) and the planner's morning.
func (s *sim) openTheBuilding(ctx context.Context) bool {
	s.staffTheFloor()
	setup := []func(context.Context) error{
		s.ensureProcessPaths,
		s.defineLaborStandards,
		func(ctx context.Context) error { return s.mapBuilding(ctx, s.stationCount()) },
		s.registerStations,
		s.catalogueProducts,
		s.inbound,
	}
	for _, step := range setup {
		if err := step(ctx); err != nil {
			return s.abortRun("during opening", err)
		}
	}
	s.clock = newSimClock(s.cfg.day)
	theClock.Store(s.clock)
	s.planOrders()
	if s.tap != nil {
		s.tap.Track(s.cfg.runID)
	}
	for _, step := range []func(context.Context) error{s.publishCPTSchedule, s.planTheDay} {
		if err := step(ctx); err != nil {
			return s.abortRun("during planning", err)
		}
	}
	return true
}

// startTheShift clocks every associate in at 06:00.
func (s *sim) startTheShift(ctx context.Context) bool {
	for _, a := range s.associates {
		if err := s.startShift(ctx, a); err != nil {
			return s.abortRun("at shift start", err)
		}
	}
	logf("supervisor", "shift %s started: %d associates clocked in and assigned", s.shiftID(), len(s.associates))
	return true
}

// launchTheFloor starts the associates' floor work and the office loops; it
// returns the floor's and the office's wait groups.
func (s *sim) launchTheFloor(floorCtx context.Context) (floor, office *sync.WaitGroup) {
	go s.suspend.Run(floorCtx)
	floor, office = &sync.WaitGroup{}, &sync.WaitGroup{}
	for _, a := range s.associates {
		floor.Add(1)
		go s.work(floorCtx, a, floor)
	}
	floor.Add(2)
	go s.releaseScheduler(floorCtx, floor)
	go s.sweeps(floorCtx, floor)
	office.Add(4)
	go s.customerChannel(floorCtx, office)
	go s.customerService(floorCtx, office)
	go s.inventoryControl(floorCtx, office)
	go s.supervisor(floorCtx, office)
	return floor, office
}

// trackOrdersInTap feeds every order id to the event tap as soon as it exists.
func (s *sim) trackOrdersInTap(floorCtx context.Context) {
	if s.tap == nil {
		return
	}
	go func() {
		seen := map[string]bool{}
		for floorCtx.Err() == nil {
			s.mu.Lock()
			for id := range s.byID {
				if !seen[id] {
					seen[id] = true
					s.tap.Track(id)
				}
			}
			s.mu.Unlock()
			sleepCtx(floorCtx, 200*time.Millisecond)
		}
	}()
}

// drainTheBuilding waits from 22:00 for the floor to clear, with overtime up to
// drainTimeout.
func (s *sim) drainTheBuilding(ctx context.Context) {
	sleepCtx(ctx, time.Until(s.clock.wallAt(dayCloseHour, 0)))
	deadline := time.Now().Add(s.cfg.drainTimeout)
	for ctx.Err() == nil && time.Now().Before(deadline) {
		if s.intakeDone.Load() && s.outstanding() == 0 {
			break
		}
		sleepCtx(ctx, time.Second)
	}
	if n := s.outstanding(); n > 0 {
		logf("supervisor", "closing with %d orders still not through SLAM after %s overtime", n, s.cfg.drainTimeout)
	} else {
		logf("supervisor", "floor clear — every shippable order went through SLAM")
	}
}

// outstanding counts orders that should ship but have not passed SLAM.
func (s *sim) outstanding() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, o := range s.orders {
		if o.shippable() && !o.cancelled && o.packageStatus == "" {
			n++
		}
	}
	return n
}

// ---------------------------------------------------------------------
// end-of-day audit
// ---------------------------------------------------------------------

type check struct {
	Name   string `json:"name"`
	Pass   bool   `json:"pass"`
	Detail string `json:"detail"`
}

type orderAudit struct {
	Seq          int      `json:"seq"`
	ID           string   `json:"id"`
	Kind         string   `json:"kind"`
	ArrivedSim   string   `json:"arrivedSim"`
	Lines        int      `json:"lines"`
	IntakeStatus string   `json:"intakeStatus"`
	FinalStatus  string   `json:"finalStatus"`
	Promise      string   `json:"promise"`
	WorkUnits    string   `json:"workUnits"`
	Reservations string   `json:"reservations"`
	PickTasks    string   `json:"pickTasks"`
	PackTask     string   `json:"packTask"`
	Package      string   `json:"package"`
	SortLane     string   `json:"sortLane"`
	CycleSeconds float64  `json:"cycleWallSeconds"`
	Events       string   `json:"events"`
	Problems     []string `json:"problems"`
}

// dayTally is the end-of-day count the aggregate checks are judged against.
type dayTally struct {
	placed, shipped, diverted, expectedShip, cancelled, broken int
	cycle                                                      []float64
	pickedUnits                                                map[string]int
}

func (s *sim) audit(ctx context.Context) bool {
	logf("auditor", "end-of-day audit across every bounded context")
	var checks []check
	add := func(name string, pass bool, format string, args ...any) {
		checks = append(checks, check{name, pass, fmt.Sprintf(format, args...)})
	}

	audits, t := s.auditOrders(ctx)
	add("orders placed", t.placed == len(s.orders), "%d of %d planned orders accepted by order-management", t.placed, len(s.orders))
	add("orders shipped", t.shipped+t.diverted == t.expectedShip, "%d labeled + %d diverted of %d shippable (%d cancelled by customers)", t.shipped, t.diverted, t.expectedShip, t.cancelled)
	mispicks := 0
	for _, o := range s.orders {
		if o.kind == kindMispickInjct && o.id != "" {
			mispicks++
		}
	}
	add("SLAM caught every mispick", t.diverted == mispicks, "%d diverted, %d mispicks injected", t.diverted, mispicks)
	add("per-order cross-context consistency", t.broken == 0, "%d of %d orders have at least one inconsistency (see orders[].problems)", t.broken, t.placed)
	add("no server errors", s.stats.Count5xx() == 0, "%d transport errors / 5xx responses over %d routes", s.stats.Count5xx(), len(s.stats.Lines()))

	// inventory conservation: usable == received - picked (- unlocated)
	invOK, invDetail := s.auditInventory(ctx, t.pickedUnits)
	add("inventory conservation", invOK, "%s", invDetail)

	// labor: every associate that did floor work has a scorecard
	laborOK, laborDetail := s.auditLabor(ctx)
	add("labor performance recorded", laborOK, "%s", laborDetail)

	s.auditEvents(add)

	s.mu.Lock()
	findings := append([]string(nil), s.findings...)
	suspended := len(s.findingsDuringSuspend)
	s.mu.Unlock()
	detail := fmt.Sprintf("%d findings (see findings[])", len(findings))
	if w := s.suspend.Snapshot(); len(w) > 0 {
		detail += fmt.Sprintf("; %d more raised during %d host suspension(s), not counted (see findingsDuringSuspend[])", suspended, len(w))
	}
	add("no operational findings", len(findings) == 0, "%s", detail)

	pass := true
	for _, c := range checks {
		if !c.Pass {
			pass = false
		}
	}
	s.printSummary(checks, audits, t.cycle, findings, pass)
	s.writeReportFull(pass, checks, audits, findings)
	return pass
}

// auditOrders audits every placed order and tallies the day's outcomes.
func (s *sim) auditOrders(ctx context.Context) (audits []orderAudit, t dayTally) {
	t.pickedUnits = map[string]int{}
	for _, o := range s.orders {
		if o.id == "" {
			continue
		}
		a := s.auditOrder(ctx, o)
		audits = append(audits, a)
		if o.kind == kindHeldCancel {
			t.cancelled++
		} else {
			t.expectedShip++
		}
		if len(a.Problems) > 0 {
			t.broken++
		}
		switch strings.TrimSuffix(o.packageStatus, "?") {
		case "LABELED":
			t.shipped++
			t.cycle = append(t.cycle, a.CycleSeconds)
		case "DIVERTED":
			t.diverted++
		}
		for n, l := range o.lines {
			if o.pickedLines[n+1] {
				t.pickedUnits[l.sku] += l.qty
			}
		}
	}
	t.placed = len(audits)
	return audits, t
}

// auditInventory checks usable == received - picked (- unlocated) for every SKU
// of the catalogue.
func (s *sim) auditInventory(ctx context.Context, pickedUnits map[string]int) (bool, string) {
	var detail []string
	ok := true
	for _, sd := range skuCatalogue {
		received := sd.stock
		if sd.key == "CAMERA" {
			received += 30
		}
		want := received - pickedUnits[sd.key]
		if sd.key == "SAMPLE" {
			// Found 3 short at the 10:00 cycle count. inventory-storage's
			// reconciliation is deliberately coarse (its ADR-0002): a
			// shortfall marks whole StockUnits UNLOCATED rather than
			// splitting located from lost portions, so the overflow bin's
			// single SAMPLE unit goes entirely unusable.
			want = 0
		}
		var u struct {
			Usable int `json:"usable"`
		}
		r := s.api.Call(ctx, svcInventory, http.MethodGet, "/inventory/{sku}/usable", "/inventory/"+url.PathEscape(s.sku(sd.key))+"/usable", nil)
		_ = r.Decode(&u)
		if u.Usable != want {
			ok = false
			detail = append(detail, fmt.Sprintf("%s usable %d want %d", sd.key, u.Usable, want))
		}
	}
	if ok {
		detail = append(detail, fmt.Sprintf("all %d SKUs reconcile (received - picked - unlocated == usable)", len(skuCatalogue)))
	}
	return ok, strings.Join(detail, "; ")
}

// auditEvents checks the Kafka tap's day: every required event type observed,
// nothing dead-lettered, the tap itself healthy.
func (s *sim) auditEvents(add func(string, bool, string, ...any)) {
	if s.tap == nil {
		return
	}
	missing, required := s.missingEvents()
	add("integration events published", len(missing) == 0, "%d/%d required event types observed on Kafka; missing: %v", required-len(missing), required, missing)
	s.tap.Mu.Lock()
	dlq := 0
	for _, n := range s.tap.Dlq {
		dlq += n
	}
	legacy := 0
	for _, n := range s.tap.DlqLegacy {
		legacy += n
	}
	dlqDetail := fmt.Sprintf("%v (plus %d legacy/unrelated messages dead-lettered by consumer replay)", s.tap.Dlq, legacy)
	tapErrs := append([]string(nil), s.tap.Errs...)
	s.tap.Mu.Unlock()
	add("no dead-lettered events", dlq == 0, "%d messages dead-lettered during the day %s", dlq, dlqDetail)
	if len(tapErrs) > 0 {
		add("event tap healthy", false, "%v", tapErrs)
	}
}

func (s *sim) auditOrder(ctx context.Context, o *orderRec) orderAudit {
	a := orderAudit{Seq: o.seq, ID: o.id, Kind: string(o.kind), ArrivedSim: o.simAt, Lines: len(o.lines),
		IntakeStatus: o.intakeStatus, Promise: o.promiseDate, Package: o.packageStatus, SortLane: o.sortLane}
	a.Problems = append(a.Problems, o.notes...)
	prob := func(f string, args ...any) { a.Problems = append(a.Problems, fmt.Sprintf(f, args...)) }

	om, st := s.getOrder(ctx, o.id)
	a.FinalStatus = om.Status
	if st != http.StatusOK {
		prob("GET /orders/{id} -> %d", st)
	}
	if !o.labeledWall.IsZero() {
		a.CycleSeconds = o.labeledWall.Sub(o.placedWall).Seconds()
	}
	wantStatus := "Released"
	if o.kind == kindHeldCancel {
		wantStatus = "Cancelled"
	}
	if om.Status != wantStatus {
		prob("order-management status %s, want %s", om.Status, wantStatus)
	}
	if o.kind == kindHeldCancel {
		res := s.reservationsFor(ctx, o.id)
		for _, r := range res {
			if r.Status != "REVOKED" {
				prob("cancelled order reservation %s is %s, want REVOKED", r.ID, r.Status)
			}
		}
		a.Reservations = fmt.Sprintf("%d revoked", len(res))
		return a
	}

	s.auditWorkUnits(ctx, o, &a, prob)
	s.auditReservations(ctx, o, &a, prob)
	s.auditTasks(ctx, o, &a, prob)
	s.auditPackages(ctx, o, &a, prob)
	s.auditOrderEvents(o, &a, prob)
	return a
}

// auditWorkUnits checks wes-work-planning's work units of the order.
func (s *sim) auditWorkUnits(ctx context.Context, o *orderRec, a *orderAudit, prob func(string, ...any)) {
	type workUnit struct {
		ID    string `json:"id"`
		State string `json:"state"`
	}
	wus, _ := listAll[workUnit](ctx, s.api, svcWES, "/work-units?reference", "/work-units?reference="+url.QueryEscape(o.id))
	states := map[string]int{}
	for _, w := range wus {
		states[w.State]++
	}
	a.WorkUnits = fmt.Sprint(states)
	if len(wus) != len(o.lines) {
		prob("wes has %d work units, order has %d lines", len(wus), len(o.lines))
	}
	if states["Completed"] != len(o.lines) {
		prob("wes work units not all Completed: %v", states)
	}
}

// auditReservations checks inventory-storage's reservations of the order.
func (s *sim) auditReservations(ctx context.Context, o *orderRec, a *orderAudit, prob func(string, ...any)) {
	res := s.reservationsFor(ctx, o.id)
	rs := map[string]int{}
	for _, r := range res {
		rs[r.Status]++
	}
	a.Reservations = fmt.Sprint(rs)
	if rs["CONFIRMED"] < len(o.lines) {
		prob("reservations %v, want %d CONFIRMED", rs, len(o.lines))
	}
}

// auditTasks checks fulfillment-execution's PICK task per line and the single
// PACK task of the order.
func (s *sim) auditTasks(ctx context.Context, o *orderRec, a *orderAudit, prob func(string, ...any)) {
	pickDone := 0
	for i := range o.lines {
		ref := fmt.Sprintf("%s-line-%d", o.id, i+1)
		ts, _ := listAll[task](ctx, s.api, svcFulfillment, "/tasks?orderRef", "/tasks?orderRef="+url.QueryEscape(ref))
		for _, t := range ts {
			if t.Type == "PICK" && t.Status == "COMPLETED" {
				pickDone++
				break
			}
		}
	}
	a.PickTasks = fmt.Sprintf("%d/%d completed", pickDone, len(o.lines))
	if pickDone != len(o.lines) {
		prob("PICK tasks completed %d/%d", pickDone, len(o.lines))
	}
	packs, _ := listAll[task](ctx, s.api, svcFulfillment, "/tasks?orderRef", "/tasks?orderRef="+url.QueryEscape(o.id))
	packStates := []string{}
	for _, t := range packs {
		if t.Type == "PACK" {
			packStates = append(packStates, t.Status)
		}
	}
	a.PackTask = strings.Join(packStates, ",")
	if len(packStates) != 1 || packStates[0] != "COMPLETED" {
		prob("PACK tasks for order: %v, want exactly one COMPLETED", packStates)
	}
}

// auditPackages checks the order's SLAM package.
func (s *sim) auditPackages(ctx context.Context, o *orderRec, a *orderAudit, prob func(string, ...any)) {
	pk, r := listAll[pkg](ctx, s.api, svcFulfillment, "/packages?orderRef", "/packages?orderRef="+url.QueryEscape(o.id))
	if r.OK() {
		wantPkg := "LABELED"
		if o.kind == kindMispickInjct {
			wantPkg = "DIVERTED"
		}
		if len(pk) != 1 || pk[0].Status != wantPkg {
			got := []string{}
			for _, p := range pk {
				got = append(got, p.Status)
			}
			prob("packages %v, want one %s", got, wantPkg)
		} else {
			a.Package = pk[0].Status
		}
	} else if o.packageStatus == "" {
		prob("never reached SLAM")
	}
}

// auditOrderEvents checks the Kafka events of the order.
func (s *sim) auditOrderEvents(o *orderRec, a *orderAudit, prob func(string, ...any)) {
	if s.tap == nil {
		return
	}
	ev := []string{}
	for _, ty := range []string{"OrderAllocated", "WorkUnitCreated", "WorkReleased", "PackageManifested", "PackageDiverted", "OrderRepromised", "TaskCPTMissed"} {
		if n := s.tap.Seen(o.id, ty); n > 0 {
			ev = append(ev, fmt.Sprintf("%s×%d", ty, n))
		}
	}
	a.Events = strings.Join(ev, " ")
	if s.tap.Seen(o.id, "WorkReleased") < len(o.lines) {
		prob("WorkReleased seen %d times for %d lines", s.tap.Seen(o.id, "WorkReleased"), len(o.lines))
	}
	final := "PackageManifested"
	if o.kind == kindMispickInjct {
		final = "PackageDiverted"
	}
	if final == "PackageManifested" && s.tap.Seen(o.id, final) == 0 {
		prob("no PackageManifested event for the order")
	}
}

// planningEventsTopic is warehouse-planning's integration topic: the
// planner's WorkloadForecastReceived and LabourPlanCommitted.
const planningEventsTopic = "warehouse.warehouse-planning.events"

// plannerEvents are the planner's required event types, each counted on its
// producer's topics only. The tap keys on the SHORT type (the last dotted
// segment of the CloudEvents type), summed over every tapped topic, and
// wes-work-planning publishes its own
// com.warehouse.wes.work-planning.plan.ShiftPlanCommitted (retiring with
// POST /paths/{pathId}/plan, its ADR 0049) on warehouse.work-planning.events
// and warehouse.wes.analytics: counted fleet-wide it would stand in for
// workforce-management's ShiftPlanCommitted.
var plannerEvents = []struct {
	typ    string
	topics []string
}{
	{"WorkloadForecastReceived", []string{planningEventsTopic}},
	{"LabourPlanCommitted", []string{planningEventsTopic}},
	{"ShiftPlanCommitted", []string{"warehouse.workforce.events", "warehouse.workforce.analytics"}},
}

// missingEvents lists the required event types the tap did not observe and
// says how many types are required.
func (s *sim) missingEvents() (missing []string, required int) {
	types := []string{
		"StockReceived", "ItemStowed", "StockReserved", "StockPicked", "CycleCountCompleted", "DiscrepancyDetected",
		"OrderAllocated", "WorkUnitCreated", "WorkReleased", "TaskCreated", "TaskClaimed", "TaskCompleted",
		"PackageSealed", "LabelApplied", "PackageManifested", "PackageDiverted", "WeightDiscrepancyDetected",
		"AssociateShiftStarted", "LaborAssigned", "AssociateBreakStarted", "AssociateBreakEnded", "AssociateShiftEnded",
		"TaskPerformanceRecorded", "CPTScheduleChanged", "LocationSlotRegistered",
	}
	if s.slotsImported == 0 {
		// The site was already mapped on an earlier day: no slot is new,
		// so facility-layout correctly registers (and publishes) nothing.
		types = removeString(types, "LocationSlotRegistered")
	}
	for _, ty := range types {
		if s.tap.Total(ty) == 0 {
			missing = append(missing, ty)
		}
	}
	for _, e := range plannerEvents {
		if s.tap.TotalOn(e.typ, e.topics...) == 0 {
			missing = append(missing, e.typ+" on "+strings.Join(e.topics, "|"))
		}
	}
	return missing, len(types) + len(plannerEvents)
}

func (s *sim) auditLabor(ctx context.Context) (bool, string) {
	deadline := time.Now().Add(30 * time.Second)
	var missing []string
	for {
		missing = missing[:0]
		for _, a := range s.associates {
			if a.role == roleRebin || a.role == roleSlam || a.tasks == 0 {
				continue // rebin/SLAM produce no TaskCompleted
			}
			var sc struct {
				TaskCount int `json:"taskCount"`
			}
			r := s.api.Call(ctx, svcLabor, http.MethodGet, "/associates/{id}/scorecard", "/associates/"+url.PathEscape(a.id)+"/scorecard", nil)
			if !r.OK() || r.Decode(&sc) != nil || sc.TaskCount == 0 {
				missing = append(missing, fmt.Sprintf("%s(%d)", a.id, r.Status))
			}
		}
		if len(missing) == 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(3 * time.Second)
	}
	if len(missing) > 0 {
		return false, "no scorecard for " + strings.Join(missing, ", ")
	}
	return true, "every picker/packer has a labor-performance scorecard"
}

// ---------------------------------------------------------------------
// reporting
// ---------------------------------------------------------------------

func percentile(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	c := append([]float64(nil), xs...)
	sort.Float64s(c)
	i := int(p * float64(len(c)-1))
	return c[i]
}

func (s *sim) printSummary(checks []check, audits []orderAudit, cycle []float64, findings []string, pass bool) {
	simPerWall := 1 / s.clock.ratio()
	fmt.Printf("\n================ WAREHOUSE DAY %s — END OF DAY REPORT ================\n", s.cfg.runID)
	fmt.Println("hourly supervisor snapshots:")
	for _, l := range s.snapshots {
		fmt.Println("  " + l)
	}
	fmt.Printf("\nfloor activity: released %d (rejected %d) | picks %d | rebin scans %d | packs %d | SLAM %d -> labeled %d, diverted %d\n",
		s.m.released.Load(), s.m.releaseRejected.Load(), s.m.picks.Load(), s.m.rebinScans.Load(), s.m.packs.Load(), s.m.slams.Load(), s.m.labeled.Load(), s.m.diverted.Load())
	fmt.Printf("                breaks %d | flex reassignments %d | lease renewals %d | cycle counts %d (%d discrepancies) | picker travel %dm | carried-over tasks worked %d\n",
		s.m.breaks.Load(), s.m.reassignments.Load(), s.m.leaseRenewals.Load(), s.m.cycleCounts.Load(), s.m.discrepancies.Load(), s.m.travelMetres.Load(), s.m.foreignTasks.Load())
	if len(cycle) > 0 {
		fmt.Printf("order-to-label cycle time (simulated): p50 %s, p90 %s, max %s\n",
			time.Duration(percentile(cycle, 0.5)*simPerWall*float64(time.Second)).Round(time.Minute),
			time.Duration(percentile(cycle, 0.9)*simPerWall*float64(time.Second)).Round(time.Minute),
			time.Duration(percentile(cycle, 1)*simPerWall*float64(time.Second)).Round(time.Minute))
	}
	fmt.Println("\nassociates:")
	for _, a := range s.associates {
		fmt.Printf("  %-22s %-7s tasks %4d units %4d speed %.2f\n", a.id, a.role, a.tasks, a.units, a.speed)
	}
	s.printTapEvents()
	printBrokenOrders(audits)
	if len(findings) > 0 {
		fmt.Println("\nfindings:")
		for _, f := range findings {
			fmt.Println("  - " + f)
		}
	}
	fmt.Println("\nchecks:")
	for _, c := range checks {
		mark := "PASS"
		if !c.Pass {
			mark = "FAIL"
		}
		fmt.Printf("  [%s] %-38s %s\n", mark, c.Name, c.Detail)
	}
	verdict := "THE WAREHOUSE WORKED"
	if !pass {
		verdict = "THE WAREHOUSE DID NOT WORK END TO END"
	}
	fmt.Printf("\nVERDICT: %s\n", verdict)
}

// printTapEvents prints the Kafka events the day produced, per topic.
func (s *sim) printTapEvents() {
	if s.tap == nil {
		return
	}
	fmt.Println("\nKafka events produced today (topic: type×n):")
	s.tap.Mu.Lock()
	defer s.tap.Mu.Unlock()
	topics := make([]string, 0, len(s.tap.Counts))
	for t := range s.tap.Counts {
		topics = append(topics, t)
	}
	sort.Strings(topics)
	for _, t := range topics {
		var parts []string
		for ty, n := range s.tap.Counts[t] {
			parts = append(parts, ty+"×"+strconv.Itoa(n))
		}
		sort.Strings(parts)
		fmt.Printf("  %-42s %s\n", t, strings.Join(parts, " "))
	}
}

// printBrokenOrders prints the first broken orders and the total count.
func printBrokenOrders(audits []orderAudit) {
	broken := 0
	for _, a := range audits {
		if len(a.Problems) > 0 {
			broken++
			if broken <= 15 {
				fmt.Printf("  order #%03d %s %-19s %v\n", a.Seq, short(a.ID), a.Kind, a.Problems)
			}
		}
	}
	if broken > 0 {
		fmt.Printf("  (%d orders with problems; full list in the JSON report)\n", broken)
	}
}

func (s *sim) writeReport(pass bool) { s.writeReportFull(pass, nil, nil, s.findings) }

func (s *sim) writeReportFull(pass bool, checks []check, audits []orderAudit, findings []string) {
	rep := map[string]any{
		"runId": s.cfg.runID, "pass": pass, "generatedAt": time.Now().UTC().Format(time.RFC3339),
		"config": map[string]any{
			"day": s.cfg.day.String(), "orders": s.cfg.orders, "pickers": s.cfg.pickers, "packers": s.cfg.packers,
			"rebin": s.cfg.rebinners, "slam": s.cfg.slammers, "flex": s.cfg.flex, "seed": s.cfg.seed,
		},
		"checks": checks, "orders": audits, "findings": findings, "findingsDuringSuspend": s.findingsDuringSuspend, "suspensions": s.suspend.Snapshot(), "snapshots": s.snapshots, "routes": s.stats.Lines(),
	}
	if s.tap != nil {
		s.tap.Mu.Lock()
		rep["events"], rep["dlq"], rep["dlqLegacy"] = s.tap.Counts, s.tap.Dlq, s.tap.DlqLegacy
		b, _ := json.MarshalIndent(rep, "", "  ")
		s.tap.Mu.Unlock()
		s.saveReport(b)
		return
	}
	b, _ := json.MarshalIndent(rep, "", "  ")
	s.saveReport(b)
}

func (s *sim) saveReport(b []byte) {
	if err := os.MkdirAll(s.cfg.reportDir, 0o755); err != nil {
		return
	}
	p := filepath.Join(s.cfg.reportDir, "warehouse-day-"+s.cfg.runID+".json")
	if err := os.WriteFile(p, b, 0o644); err == nil {
		abs, _ := filepath.Abs(p)
		fmt.Printf("report: %s\n", abs)
	}
}

func removeString(xs []string, drop string) []string {
	out := xs[:0:0]
	for _, x := range xs {
		if x != drop {
			out = append(out, x)
		}
	}
	return out
}
