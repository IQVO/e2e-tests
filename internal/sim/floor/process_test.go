package floor

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// std is the order most tests process: two lines, three units, for the building the floor works in.
func std(id string) orderSpec {
	return orderSpec{ID: id, Site: "AUS3", HeldFor: 3, UnitLag: 3, Lines: []lineSpec{{SKU: "SKU-A", Qty: 2}, {SKU: "SKU-B", Qty: 1}}}
}

func provisioned(t *testing.T, w *world, s Settings, c Crew) *Floor {
	t.Helper()
	f := newTestFloor(w, s)
	if err := f.Provision(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	return f
}

func hasProblem(r Result, parts ...string) bool {
	for _, p := range r.Problems {
		all := true
		for _, part := range parts {
			all = all && strings.Contains(p, part)
		}
		if all {
			return true
		}
	}
	return false
}

func TestOneOrderGoesFromReleasedToLabeled(t *testing.T) {
	w := newWorld(t)
	w.addOrder(std("OM-1"))
	f := provisioned(t, w, Settings{Site: "AUS3"}, Crew{Pickers: 2, Packers: 1})

	r := f.Process(context.Background(), "OM-1")
	if !r.OK() {
		t.Fatalf("not OK: status %q package %q problems %v", r.Status, r.PackageID, r.Problems)
	}
	if r.Status != PackageLabeled || r.SortLane != "STANDARD" {
		t.Fatalf("status %q lane %q", r.Status, r.SortLane)
	}
	if r.OrderRef != "OM-1" || r.SourceOrderID != "OM-1" || r.SiteID != "AUS3" || r.CPT != cptEvening {
		t.Fatalf("manifest facts: order_ref %q source_order_id %q site %q cpt %q", r.OrderRef, r.SourceOrderID, r.SiteID, r.CPT)
	}
	if strings.Join(r.Units, ",") != "OM-1-line-1,OM-1-line-2" {
		t.Fatalf("units %v", r.Units)
	}
	if strings.Join(r.Contents, ",") != "SKU-A,SKU-A,SKU-B" {
		t.Fatalf("contents %v: one SKU per unit", r.Contents)
	}
	if r.WeightG != 1750 { // 250 g carton + 3 units at 500 g
		t.Fatalf("weight %d g", r.WeightG)
	}
	if r.PackTaskID == "" {
		t.Fatal("no PACK task id")
	}

	// the world agrees with the result
	if pk := w.packagesOf("OM-1"); len(pk) != 1 || pk[0].Status != PackageLabeled || pk[0].ID != r.PackageID {
		t.Fatalf("packages %+v", pk)
	}
	for _, u := range r.Units {
		tk := w.tasksOf(u)
		if len(tk) != 1 || tk[0].Type != TypePick || tk[0].Status != TaskCompleted {
			t.Fatalf("PICK tasks of %s: %+v", u, tk)
		}
		if s := w.unitState(u); s != UnitReleased && s != UnitCompleted {
			t.Fatalf("unit %s is %s: it was never released", u, s)
		}
	}
	if tk := w.tasksOf("OM-1"); len(tk) != 1 || tk[0].Type != TypePack || tk[0].Status != TaskCompleted {
		t.Fatalf("PACK tasks: %+v", tk)
	}
	if w.fe.rebinPackCPT["OM-1"] != cptEvening {
		t.Fatalf("PACK task cpt %q: it must be the order's promise date, the PICK task's cpt carried through", w.fe.rebinPackCPT["OM-1"])
	}
	if got := strings.Join(w.fe.sealContents[r.PackageID], ","); got != "SKU-A,SKU-A,SKU-B" {
		t.Fatalf("sealed %q", got)
	}
	for _, id := range []string{"res-OM-1-1", "res-OM-1-2"} {
		if s := w.reservationStatus(id); s != ReservationConfirmed {
			t.Fatalf("reservation %s is %s", id, s)
		}
	}

	// exact route counts: nothing done twice, nothing done for others
	for route, want := range map[string]int{
		"POST /wes-work-planning/paths/{pathId}/work-releases":         2,
		"POST /fulfillment-execution/rebin/arrivals":                   2,
		"POST /fulfillment-execution/tasks/{id}/package-seals":         1,
		"POST /fulfillment-execution/packages/{id}/slam-runs":          1,
		"POST /fulfillment-execution/tasks/{id}/completions":           3, // two PICK, one PACK
		"POST /inventory-storage/reservations/{id}/pick-confirmations": 0,
	} {
		if got := w.callsTo(route); got != want {
			t.Errorf("%s: %d calls, want %d", route, got, want)
		}
	}
	if st := f.Stats(); st != (Stats{}) {
		t.Fatalf("stats %+v: no foreign work in this run", st)
	}
	if fi := f.Findings(); len(fi) != 0 {
		t.Fatalf("findings %v", fi)
	}

	// the order of the day: release, pick, rebin, pack, SLAM
	log := w.callLog()
	idx := func(route string) int { return firstIndex(log, route) }
	order := []string{
		"GET /order-management/orders/{id}",
		"GET /wes-work-planning/work-units",
		"POST /wes-work-planning/paths/{pathId}/work-releases",
		"POST /fulfillment-execution/stations/{id}/task-claims",
		"POST /fulfillment-execution/rebin/arrivals",
		"POST /fulfillment-execution/tasks/{id}/package-seals",
		"POST /fulfillment-execution/packages/{id}/slam-runs",
	}
	for i := 1; i < len(order); i++ {
		if idx(order[i-1]) < 0 || idx(order[i]) < 0 || idx(order[i-1]) >= idx(order[i]) {
			t.Fatalf("%s (at %d) must come before %s (at %d)", order[i-1], idx(order[i-1]), order[i], idx(order[i]))
		}
	}
}

func TestOnlyCrewStationsWorkAndEachTaskIsCompletedByItsClaimant(t *testing.T) {
	w := newWorld(t)
	w.addOrder(std("OM-1"))
	f := provisioned(t, w, Settings{}, Crew{Pickers: 2, Packers: 1})
	if r := f.Process(context.Background(), "OM-1"); !r.OK() {
		t.Fatalf("%v", r.Problems)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	total := 0
	for id, tasks := range w.fe.completedBy {
		if !strings.HasPrefix(id, "RD01-PICK-") && !strings.HasPrefix(id, "RD01-PACK-") {
			t.Fatalf("task completed by %q, not a station of this floor", id)
		}
		total += len(tasks)
	}
	if total != 3 {
		t.Fatalf("%d completions, want 3", total)
	}
	if len(w.fe.completedBy["RD01-PACK-01"]) != 1 {
		t.Fatalf("the packer completed %v, want exactly the PACK task", w.fe.completedBy["RD01-PACK-01"])
	}
}

func TestConsumerLagIsWaitedOutNotRetriedBlindly(t *testing.T) {
	w := newWorld(t)
	o := std("OM-1")
	o.HeldFor, o.UnitLag = 40, 25 // the release and OrderAllocated are slow
	w.addOrder(o)
	w.fe.lag = 15 // so is WorkReleased
	w.inv.lag = 12
	f := provisioned(t, w, Settings{}, Crew{Pickers: 1, Packers: 1})
	r := f.Process(context.Background(), "OM-1")
	if !r.OK() {
		t.Fatalf("problems: %v", r.Problems)
	}
	if w.clock.Slept() == 0 {
		t.Fatal("the lag was never waited out")
	}
	// the tasks did not exist at the first claim: that is a 409 no-claimable-task, not a failure
	claims := w.callsTo("POST /fulfillment-execution/stations/{id}/task-claims")
	if claims < 3 {
		t.Fatalf("%d claims for 3 tasks", claims)
	}
}

func TestEveryWaitIsBoundedAndSaysWhatItNeverSaw(t *testing.T) {
	cases := map[string]struct {
		setup func(w *world, o *orderSpec)
		set   Settings
		parts []string
	}{
		"order never released": {
			setup: func(w *world, o *orderSpec) { o.HeldFor = 1 << 30 },
			set:   Settings{Lag: Waits{Timeout: 5 * time.Second, Interval: time.Second}},
			parts: []string{"Released", "Allocated", "held"},
		},
		"work units never created": {
			setup: func(w *world, o *orderSpec) { o.UnitLag = 1 << 30 },
			set:   Settings{Lag: Waits{Timeout: 5 * time.Second, Interval: time.Second}},
			parts: []string{"work units", "OM-1", "OrderAllocated"},
		},
		"task never created": {
			setup: func(w *world, o *orderSpec) { w.fe.lag = 1 << 30 },
			set:   Settings{Work: Waits{Timeout: 5 * time.Second, Interval: time.Second}},
			parts: []string{"pick", "no-claimable-task", "OM-1-line-1", "no PICK task exists"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			w := newWorld(t)
			o := std("OM-1")
			tc.setup(w, &o)
			w.addOrder(o)
			f := provisioned(t, w, tc.set, Crew{Pickers: 1, Packers: 1})
			start := w.clock.Now()
			r := f.Process(context.Background(), "OM-1")
			if r.OK() || r.PackageID != "" || len(r.Problems) == 0 {
				t.Fatalf("result %+v: must be an unfinished order with a finding", r)
			}
			if !hasProblem(r, tc.parts...) {
				t.Fatalf("problems %v: want one naming %v", r.Problems, tc.parts)
			}
			if got := w.clock.Now().Sub(start); got < 5*time.Second || got > 8*time.Second {
				t.Fatalf("the wait lasted %s of clock time, want about its 5 s bound", got)
			}
			if r.PackageID != "" || w.callsTo("POST /fulfillment-execution/packages/{id}/slam-runs") != 0 {
				t.Fatal("went on after a wait expired")
			}
		})
	}
}

func TestAnOrderWithoutStockIsAFindingAndNothingIsWritten(t *testing.T) {
	w := newWorld(t)
	o := std("OM-1")
	o.Lines[1].Backordered = true
	w.addOrder(o)
	f := provisioned(t, w, Settings{}, Crew{Pickers: 1, Packers: 1})
	before := w.writes()
	r := f.Process(context.Background(), "OM-1")
	if !hasProblem(r, "line 2", "Backordered") {
		t.Fatalf("problems %v", r.Problems)
	}
	if w.writes() != before {
		t.Fatalf("%d writes for an order the floor cannot work: it must not release part of it", w.writes()-before)
	}
}

func TestAnUnknownOrderIsAFindingAndNotAPanic(t *testing.T) {
	w := newWorld(t)
	f := provisioned(t, w, Settings{}, Crew{Pickers: 1, Packers: 1})
	before := w.writes()
	r := f.Process(context.Background(), "NOPE")
	if r.OK() || !hasProblem(r, "NOPE", "404", "order-not-found") {
		t.Fatalf("problems %v", r.Problems)
	}
	if w.writes() != before {
		t.Fatal("wrote for an order that does not exist")
	}
}

func TestNothingIsDoneBeforeTheCrewIsProvisioned(t *testing.T) {
	w := newWorld(t)
	w.addOrder(std("OM-1"))
	f := newTestFloor(w, Settings{})
	r := f.Process(context.Background(), "OM-1")
	if !hasProblem(r, "Provision") || len(w.callLog()) != 0 {
		t.Fatalf("problems %v, calls %v", r.Problems, w.callLog())
	}
}

func TestTheSiteOfTheOrderIsCheckedWhenTheFloorHasOne(t *testing.T) {
	for name, site := range map[string]string{"no site (retail-day-journey G2)": "", "another building": "SIM1"} {
		t.Run(name, func(t *testing.T) {
			w := newWorld(t)
			o := std("OM-1")
			o.Site = site
			w.addOrder(o)
			f := provisioned(t, w, Settings{Site: "AUS3"}, Crew{Pickers: 1, Packers: 1})
			r := f.Process(context.Background(), "OM-1")
			if r.OK() || !hasProblem(r, "AUS3", "fulfillmentSiteId") {
				t.Fatalf("problems %v", r.Problems)
			}
			if !r.Labeled() {
				t.Fatalf("a site finding is reported, it does not stop the floor from working: %+v", r)
			}
		})
	}
	// a floor with no site of its own checks nothing
	w := newWorld(t)
	o := std("OM-1")
	o.Site = ""
	w.addOrder(o)
	f := provisioned(t, w, Settings{}, Crew{Pickers: 1, Packers: 1})
	if r := f.Process(context.Background(), "OM-1"); !r.OK() {
		t.Fatalf("problems %v", r.Problems)
	}
}

func TestAPromiseDateThatIsNotThePickTasksCptIsAFinding(t *testing.T) {
	w := newWorld(t)
	w.addOrder(std("OM-1"))
	w.mu.Lock()
	w.om.orders["OM-1"].CPT = "2026-11-15T17:00:00Z" // order-management promises another instant than the work units carry
	w.mu.Unlock()
	f := provisioned(t, w, Settings{}, Crew{Pickers: 1, Packers: 1})
	r := f.Process(context.Background(), "OM-1")
	if !hasProblem(r, "promise", "2026-11-15T17:00:00Z", cptEvening) {
		t.Fatalf("problems %v", r.Problems)
	}
}

func TestADivertedPackageIsReportedAndNeverCalledLabeled(t *testing.T) {
	w := newWorld(t)
	w.addOrder(std("OM-1"))
	f := provisioned(t, w, Settings{Scale: offScale{extraKg: 0.2}}, Crew{Pickers: 1, Packers: 1})
	r := f.Process(context.Background(), "OM-1")
	if r.Labeled() || r.OK() || r.Status != PackageDiverted || r.PackageID == "" {
		t.Fatalf("result %+v", r)
	}
	if !hasProblem(r, "DIVERTED", "problem-solve") {
		t.Fatalf("problems %v", r.Problems)
	}
	if w.fe.manifested["OM-1"] != 0 {
		t.Fatal("a diverted package was manifested")
	}
}

type offScale struct{ extraKg float64 }

func (s offScale) Weigh(_ string, contents []string) (float64, float64) {
	w := 0.25 + 0.5*float64(len(contents))
	return w + s.extraKg, w
}

func TestARefusedSealIsAFindingAndNothingIsSlammed(t *testing.T) {
	w := newWorld(t)
	w.addOrder(std("OM-1"))
	w.fe.sealRefusal = &[2]any{422, "package-no-scanned-contents"}
	f := provisioned(t, w, Settings{}, Crew{Pickers: 1, Packers: 1})
	r := f.Process(context.Background(), "OM-1")
	if r.OK() || r.PackageID != "" || !hasProblem(r, "package-no-scanned-contents") {
		t.Fatalf("result %+v", r)
	}
	if w.callsTo("POST /fulfillment-execution/packages/{id}/slam-runs") != 0 {
		t.Fatal("SLAM ran without a package")
	}
	if got := w.callsTo("POST /fulfillment-execution/tasks/{id}/package-seals"); got != 1 {
		t.Fatalf("%d seal attempts: a refusal that is not a lag must not be retried", got)
	}
}

func TestEveryLineOnItsOwnPathIsReleasedOnThatPath(t *testing.T) {
	w := newWorld(t)
	o := std("OM-1")
	o.Lines[0].Path, o.Lines[1].Path = "PICK-A", "PICK-B"
	w.addOrder(o)
	f := provisioned(t, w, Settings{}, Crew{Pickers: 1, Packers: 1})
	if r := f.Process(context.Background(), "OM-1"); !r.OK() {
		t.Fatalf("%v", r.Problems)
	}
	var paths []string
	w.mu.Lock()
	for _, c := range w.calls {
		if c.Route == "POST /wes-work-planning/paths/{pathId}/work-releases" {
			paths = append(paths, c.Path)
		}
	}
	w.mu.Unlock()
	if joinSorted(paths) != "/wes-work-planning/paths/PICK-A/work-releases,/wes-work-planning/paths/PICK-B/work-releases" {
		t.Fatalf("releases %v", paths)
	}
}

func TestTheWholeCollectionIsReadNotJustTheFirstPage(t *testing.T) {
	// the fakes cap a page at 2; this order has 5 lines, so its work units need 3 pages
	w := newWorld(t)
	o := orderSpec{ID: "OM-5", Site: "AUS3"}
	for i := 0; i < 5; i++ {
		o.Lines = append(o.Lines, lineSpec{SKU: fmt.Sprintf("SKU-%d", i), Qty: 1})
	}
	w.addOrder(o)
	f := provisioned(t, w, Settings{}, Crew{Pickers: 2, Packers: 1})
	r := f.Process(context.Background(), "OM-5")
	if !r.OK() || len(r.Units) != 5 || len(r.Contents) != 5 {
		t.Fatalf("units %v contents %v problems %v", r.Units, r.Contents, r.Problems)
	}
}
