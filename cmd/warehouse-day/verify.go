package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// ---------------------------------------------------------------------
// Kafka event tap
// ---------------------------------------------------------------------

// eventTap reads every warehouse integration topic from the offsets they
// were at when the day started (no consumer group — it never moves a
// service's offsets) and indexes what the day produced. Accepts the
// CloudEvents 1.0 structured envelope ("type"), CloudEvents binary mode
// ("ce_type" header) and the legacy flat envelope ("event_type"), so it
// audits whichever envelope the deployed images speak.
type eventTap struct {
	mu      sync.Mutex
	counts  map[string]map[string]int // topic -> short type -> n
	byRef   map[string]map[string]int // correlation id -> short type -> n
	refs    []string
	dlq     map[string]int
	readers []*kafkago.Reader
	errs    []string
}

var tappedTopics = []string{
	"warehouse.facility.events",
	"warehouse.inventory.events",
	"warehouse.process-path-management.events",
	"warehouse.work-planning.events",
	"warehouse.fulfillment.events",
	"warehouse.order-management.events",
	"warehouse.workforce.events",
	"warehouse.labor-performance.events",
}

func startTap(ctx context.Context, brokers []string) (*eventTap, error) {
	t := &eventTap{counts: map[string]map[string]int{}, byRef: map[string]map[string]int{}, dlq: map[string]int{}}
	conn, err := ipv4Dialer().DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	all, err := conn.ReadPartitions()
	if err != nil {
		return nil, err
	}
	want := map[string]bool{}
	for _, tp := range tappedTopics {
		want[tp] = true
	}
	n := 0
	for _, p := range all {
		if !want[p.Topic] && !strings.HasSuffix(p.Topic, ".dlq") {
			continue
		}
		r := kafkago.NewReader(kafkago.ReaderConfig{
			Brokers: brokers, Topic: p.Topic, Partition: p.ID, MinBytes: 1, MaxBytes: 10 << 20, MaxWait: 500 * time.Millisecond,
			Dialer: ipv4Dialer(),
		})
		if err := r.SetOffset(kafkago.LastOffset); err != nil {
			return nil, err
		}
		t.readers = append(t.readers, r)
		n++
		go t.consume(ctx, r, p.Topic)
	}
	logf("event-tap", "tapping %d partitions across %d integration topics + DLQs", n, len(tappedTopics))
	return t, nil
}

func (t *eventTap) track(ref string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.refs = append(t.refs, ref)
}

func shortType(s string) string {
	if i := strings.LastIndex(s, "."); i >= 0 {
		return s[i+1:]
	}
	return s
}

func (t *eventTap) consume(ctx context.Context, r *kafkago.Reader, topic string) {
	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() == nil && !errors.Is(err, context.Canceled) {
				t.mu.Lock()
				t.errs = append(t.errs, fmt.Sprintf("%s: %v", topic, err))
				t.mu.Unlock()
			}
			return
		}
		var env struct {
			Type      string `json:"type"`
			EventType string `json:"event_type"`
		}
		_ = json.Unmarshal(m.Value, &env)
		typ := env.Type
		if typ == "" {
			typ = env.EventType
		}
		for _, h := range m.Headers {
			if typ == "" && (h.Key == "ce_type" || h.Key == "ce-type") {
				typ = string(h.Value)
			}
		}
		typ = shortType(typ)
		if typ == "" {
			typ = "?"
		}
		raw := string(m.Value)
		t.mu.Lock()
		if strings.HasSuffix(topic, ".dlq") {
			t.dlq[topic]++
		} else {
			if t.counts[topic] == nil {
				t.counts[topic] = map[string]int{}
			}
			t.counts[topic][typ]++
			for _, ref := range t.refs {
				if strings.Contains(raw, ref) {
					if t.byRef[ref] == nil {
						t.byRef[ref] = map[string]int{}
					}
					t.byRef[ref][typ]++
				}
			}
		}
		t.mu.Unlock()
	}
}

func (t *eventTap) seen(ref, typ string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.byRef[ref][typ]
}

func (t *eventTap) total(typ string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for _, byType := range t.counts {
		n += byType[typ]
	}
	return n
}

func (t *eventTap) close() {
	for _, r := range t.readers {
		_ = r.Close()
	}
}

// ---------------------------------------------------------------------
// the day
// ---------------------------------------------------------------------

func (s *sim) run(ctx context.Context) bool {
	cfg := s.cfg
	fmt.Printf("\n=== warehouse day %s — %d orders, %d pickers, %d packers, %d rebin, %d SLAM, %d flex; day=%s ===\n\n",
		cfg.runID, cfg.orders, cfg.pickers, cfg.packers, cfg.rebinners, cfg.slammers, cfg.flex, cfg.day)

	if err := s.healthCheck(ctx); err != nil {
		logf("control-tower", "ABORT: %v", err)
		return false
	}
	tapCtx, stopTap := context.WithCancel(context.Background())
	defer stopTap()
	if len(cfg.brokers) > 0 {
		tap, err := startTap(tapCtx, cfg.brokers)
		if err != nil {
			s.finding("Kafka event tap unavailable (%v) — event audit skipped", err)
		} else {
			s.tap = tap
			defer tap.close()
		}
	}

	// 05:30 — opening
	s.staffTheFloor()
	setup := []func(context.Context) error{
		s.ensureProcessPaths,
		s.defineLaborStandards,
		func(ctx context.Context) error { return s.mapBuilding(ctx, s.stationCount()) },
		s.registerStations,
		s.inbound,
	}
	for _, step := range setup {
		if err := step(ctx); err != nil {
			logf("control-tower", "ABORT during opening: %v", err)
			s.writeReport(false)
			return false
		}
	}
	s.clock = newSimClock(cfg.day)
	theClock.Store(s.clock)
	s.planOrders()
	if s.tap != nil {
		s.tap.track(cfg.runID)
	}
	for _, step := range []func(context.Context) error{s.publishCPTSchedule, s.planTheDay} {
		if err := step(ctx); err != nil {
			logf("control-tower", "ABORT during planning: %v", err)
			s.writeReport(false)
			return false
		}
	}

	// 06:00 — shift start
	for _, a := range s.associates {
		if err := s.startShift(ctx, a); err != nil {
			logf("control-tower", "ABORT at shift start: %v", err)
			s.writeReport(false)
			return false
		}
	}
	logf("supervisor", "shift %s started: %d associates clocked in and assigned", s.shiftID(), len(s.associates))

	floorCtx, stopFloor := context.WithCancel(ctx)
	defer stopFloor()
	var floor, office sync.WaitGroup
	for _, a := range s.associates {
		floor.Add(1)
		go s.work(floorCtx, a, &floor)
	}
	floor.Add(2)
	go s.releaseScheduler(floorCtx, &floor)
	go s.sweeps(floorCtx, &floor)
	office.Add(4)
	go s.customerChannel(floorCtx, &office)
	go s.customerService(floorCtx, &office)
	go s.inventoryControl(floorCtx, &office)
	go s.supervisor(floorCtx, &office)

	// track every order id in the event tap as soon as it exists
	if s.tap != nil {
		go func() {
			seen := map[string]bool{}
			for floorCtx.Err() == nil {
				s.mu.Lock()
				for id := range s.byID {
					if !seen[id] {
						seen[id] = true
						s.tap.track(id)
					}
				}
				s.mu.Unlock()
				sleepCtx(floorCtx, 200*time.Millisecond)
			}
		}()
	}

	// 22:00 — the building closes once the floor is clear (overtime up to
	// drainTimeout if it is not).
	sleepCtx(ctx, time.Until(s.clock.wallAt(dayCloseHour, 0)))
	deadline := time.Now().Add(cfg.drainTimeout)
	for ctx.Err() == nil && time.Now().Before(deadline) {
		if s.intakeDone.Load() && s.outstanding() == 0 {
			break
		}
		sleepCtx(ctx, time.Second)
	}
	if n := s.outstanding(); n > 0 {
		logf("supervisor", "closing with %d orders still not through SLAM after %s overtime", n, cfg.drainTimeout)
	} else {
		logf("supervisor", "floor clear — every shippable order went through SLAM")
	}
	stopFloor()
	floor.Wait()
	office.Wait()
	logf("supervisor", "all associates checked out and shifts ended")

	// let the last events propagate before auditing
	sleepCtx(ctx, 8*time.Second)
	ok := s.audit(context.Background())
	return ok
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

func (s *sim) audit(ctx context.Context) bool {
	logf("auditor", "end-of-day audit across every bounded context")
	var checks []check
	add := func(name string, pass bool, format string, args ...any) {
		checks = append(checks, check{name, pass, fmt.Sprintf(format, args...)})
	}

	var audits []orderAudit
	shipped, diverted, expectedShip, cancelled, broken := 0, 0, 0, 0, 0
	var cycle []float64
	pickedUnits := map[string]int{}
	for _, o := range s.orders {
		if o.id == "" {
			continue
		}
		a := s.auditOrder(ctx, o)
		audits = append(audits, a)
		if o.kind == kindHeldCancel {
			cancelled++
		} else {
			expectedShip++
		}
		if len(a.Problems) > 0 {
			broken++
		}
		switch strings.TrimSuffix(o.packageStatus, "?") {
		case "LABELED":
			shipped++
			cycle = append(cycle, a.CycleSeconds)
		case "DIVERTED":
			diverted++
		}
		for n, l := range o.lines {
			if o.pickedLines[n+1] {
				pickedUnits[l.sku] += l.qty
			}
		}
	}

	placed := len(audits)
	add("orders placed", placed == len(s.orders), "%d of %d planned orders accepted by order-management", placed, len(s.orders))
	add("orders shipped", shipped+diverted == expectedShip, "%d labeled + %d diverted of %d shippable (%d cancelled by customers)", shipped, diverted, expectedShip, cancelled)
	mispicks := 0
	for _, o := range s.orders {
		if o.kind == kindMispickInjct && o.id != "" {
			mispicks++
		}
	}
	add("SLAM caught every mispick", diverted == mispicks, "%d diverted, %d mispicks injected", diverted, mispicks)
	add("per-order cross-context consistency", broken == 0, "%d of %d orders have at least one inconsistency (see orders[].problems)", broken, placed)
	add("no server errors", s.stats.count5xx() == 0, "%d transport errors / 5xx responses over %d routes", s.stats.count5xx(), len(s.stats.lines()))

	// inventory conservation: usable == received - picked (- unlocated)
	var invDetail []string
	invOK := true
	for _, sd := range skuCatalogue {
		received := sd.stock
		if sd.key == "CAMERA" {
			received += 30
		}
		want := received - pickedUnits[sd.key]
		if sd.key == "SAMPLE" {
			want -= 3 // found short at the 10:00 cycle count -> unlocated
		}
		var u struct {
			Usable int `json:"usable"`
		}
		r := s.api.call(ctx, svcInventory, http.MethodGet, "/inventory/{sku}/usable", "/inventory/"+url.PathEscape(s.sku(sd.key))+"/usable", nil)
		_ = r.decode(&u)
		if u.Usable != want {
			invOK = false
			invDetail = append(invDetail, fmt.Sprintf("%s usable %d want %d", sd.key, u.Usable, want))
		}
	}
	if invOK {
		invDetail = append(invDetail, fmt.Sprintf("all %d SKUs reconcile (received - picked - unlocated == usable)", len(skuCatalogue)))
	}
	add("inventory conservation", invOK, "%s", strings.Join(invDetail, "; "))

	// labor: every associate that did floor work has a scorecard
	laborOK, laborDetail := s.auditLabor(ctx)
	add("labor performance recorded", laborOK, "%s", laborDetail)

	// events
	if s.tap != nil {
		required := []string{
			"StockReceived", "ItemStowed", "StockReserved", "StockPicked", "CycleCountCompleted", "DiscrepancyDetected",
			"OrderAllocated", "WorkUnitCreated", "WorkReleased", "TaskCreated", "TaskClaimed", "TaskCompleted",
			"PackageSealed", "LabelApplied", "PackageManifested", "PackageDiverted", "WeightDiscrepancyDetected",
			"AssociateShiftStarted", "LaborAssigned", "AssociateBreakStarted", "AssociateBreakEnded", "AssociateShiftEnded",
			"ShiftPlanCommitted", "TaskPerformanceRecorded", "CPTScheduleChanged", "LocationSlotRegistered",
		}
		var missing []string
		for _, ty := range required {
			if s.tap.total(ty) == 0 {
				missing = append(missing, ty)
			}
		}
		add("integration events published", len(missing) == 0, "%d/%d required event types observed on Kafka; missing: %v", len(required)-len(missing), len(required), missing)
		s.tap.mu.Lock()
		dlq := 0
		for _, n := range s.tap.dlq {
			dlq += n
		}
		dlqDetail := fmt.Sprintf("%v", s.tap.dlq)
		tapErrs := append([]string(nil), s.tap.errs...)
		s.tap.mu.Unlock()
		add("no dead-lettered events", dlq == 0, "%d messages dead-lettered during the day %s", dlq, dlqDetail)
		if len(tapErrs) > 0 {
			add("event tap healthy", false, "%v", tapErrs)
		}
	}

	s.mu.Lock()
	findings := append([]string(nil), s.findings...)
	s.mu.Unlock()
	add("no operational findings", len(findings) == 0, "%d findings (see findings[])", len(findings))

	pass := true
	for _, c := range checks {
		if !c.Pass {
			pass = false
		}
	}
	s.printSummary(checks, audits, cycle, findings, pass)
	s.writeReportFull(pass, checks, audits, findings)
	return pass
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

	// wes-work-planning: one work unit per line, all Completed
	var wus []struct {
		ID    string `json:"id"`
		State string `json:"state"`
	}
	if r := s.api.call(ctx, svcWES, http.MethodGet, "/work-units?reference", "/work-units?reference="+url.QueryEscape(o.id), nil); r.ok() {
		_ = r.decode(&wus)
	}
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

	// inventory-storage: every reservation consumed by a confirmed pick
	res := s.reservationsFor(ctx, o.id)
	rs := map[string]int{}
	for _, r := range res {
		rs[r.Status]++
	}
	a.Reservations = fmt.Sprint(rs)
	if rs["CONFIRMED"] < len(o.lines) {
		prob("reservations %v, want %d CONFIRMED", rs, len(o.lines))
	}

	// fulfillment-execution: PICK task per line COMPLETED, one PACK task
	pickDone := 0
	for i := range o.lines {
		var ts []task
		ref := fmt.Sprintf("%s-line-%d", o.id, i+1)
		if r := s.api.call(ctx, svcFulfillment, http.MethodGet, "/tasks?orderRef", "/tasks?orderRef="+url.QueryEscape(ref), nil); r.ok() {
			_ = r.decode(&ts)
		}
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
	var packs []task
	if r := s.api.call(ctx, svcFulfillment, http.MethodGet, "/tasks?orderRef", "/tasks?orderRef="+url.QueryEscape(o.id), nil); r.ok() {
		_ = r.decode(&packs)
	}
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

	var pk []pkg
	r := s.api.call(ctx, svcFulfillment, http.MethodGet, "/packages?orderRef", "/packages?orderRef="+url.QueryEscape(o.id), nil)
	if r.ok() {
		_ = r.decode(&pk)
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

	// events about this order
	if s.tap != nil {
		ev := []string{}
		for _, ty := range []string{"OrderAllocated", "WorkUnitCreated", "WorkReleased", "PackageManifested", "PackageDiverted", "OrderRepromised", "TaskCPTMissed"} {
			if n := s.tap.seen(o.id, ty); n > 0 {
				ev = append(ev, fmt.Sprintf("%s×%d", ty, n))
			}
		}
		a.Events = strings.Join(ev, " ")
		if s.tap.seen(o.id, "WorkReleased") < len(o.lines) {
			prob("WorkReleased seen %d times for %d lines", s.tap.seen(o.id, "WorkReleased"), len(o.lines))
		}
		final := "PackageManifested"
		if o.kind == kindMispickInjct {
			final = "PackageDiverted"
		}
		if final == "PackageManifested" && s.tap.seen(o.id, final) == 0 {
			prob("no PackageManifested event for the order")
		}
	}
	return a
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
			r := s.api.call(ctx, svcLabor, http.MethodGet, "/associates/{id}/scorecard", "/associates/"+url.PathEscape(a.id)+"/scorecard", nil)
			if !r.ok() || r.decode(&sc) != nil || sc.TaskCount == 0 {
				missing = append(missing, fmt.Sprintf("%s(%d)", a.id, r.status))
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
	if s.tap != nil {
		fmt.Println("\nKafka events produced today (topic: type×n):")
		s.tap.mu.Lock()
		topics := make([]string, 0, len(s.tap.counts))
		for t := range s.tap.counts {
			topics = append(topics, t)
		}
		sort.Strings(topics)
		for _, t := range topics {
			var parts []string
			for ty, n := range s.tap.counts[t] {
				parts = append(parts, ty+"×"+strconv.Itoa(n))
			}
			sort.Strings(parts)
			fmt.Printf("  %-42s %s\n", t, strings.Join(parts, " "))
		}
		s.tap.mu.Unlock()
	}
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

func (s *sim) writeReport(pass bool) { s.writeReportFull(pass, nil, nil, s.findings) }

func (s *sim) writeReportFull(pass bool, checks []check, audits []orderAudit, findings []string) {
	rep := map[string]any{
		"runId": s.cfg.runID, "pass": pass, "generatedAt": time.Now().UTC().Format(time.RFC3339),
		"config": map[string]any{
			"day": s.cfg.day.String(), "orders": s.cfg.orders, "pickers": s.cfg.pickers, "packers": s.cfg.packers,
			"rebin": s.cfg.rebinners, "slam": s.cfg.slammers, "flex": s.cfg.flex, "seed": s.cfg.seed,
		},
		"checks": checks, "orders": audits, "findings": findings, "snapshots": s.snapshots, "routes": s.stats.lines(),
	}
	if s.tap != nil {
		s.tap.mu.Lock()
		rep["events"], rep["dlq"] = s.tap.counts, s.tap.dlq
		b, _ := json.MarshalIndent(rep, "", "  ")
		s.tap.mu.Unlock()
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

// ipv4Dialer forces tcp4. The kind broker's EXTERNAL listener is published
// on 127.0.0.1 only, and advertises "localhost:9092"; Go resolves that to
// [::1] first, so every partition reader failed with "connection refused".
func ipv4Dialer() *kafkago.Dialer {
	nd := &net.Dialer{Timeout: 10 * time.Second}
	return &kafkago.Dialer{
		Timeout: 10 * time.Second,
		DialFunc: func(ctx context.Context, _ string, addr string) (net.Conn, error) {
			return nd.DialContext(ctx, "tcp4", addr)
		},
	}
}
