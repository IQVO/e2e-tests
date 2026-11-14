package floor

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

const cptEarly = "2026-11-14T22:00:00Z" // 16:00 America/Chicago: ahead of cptEvening in every queue

func threeLines(id string) orderSpec {
	o := std(id)
	o.Lines = append(o.Lines, lineSpec{SKU: "SKU-C", Qty: 1})
	return o
}

func TestAPoolAtItsWIPLimitIsWaitedOutWhilePickersMakeRoom(t *testing.T) {
	// One unit at a time: the next release is refused 409 wip-limit-reached until the picker has
	// completed the previous one and wes-work-planning consumed TaskCompleted. Releasing everything
	// before picking would never finish.
	w := newWorld(t)
	w.wes.wipMax = 1
	w.addOrder(threeLines("OM-1"))
	f := provisioned(t, w, Settings{}, Crew{Pickers: 1, Packers: 1})
	r := f.Process(context.Background(), "OM-1")
	if !r.OK() {
		t.Fatalf("problems %v", r.Problems)
	}
	if got := w.countStatus("POST /wes-work-planning/paths/{pathId}/work-releases", 409); got < 1 {
		t.Fatalf("the WIP limit was never hit: this test proves nothing")
	}
	if got := w.countStatus("POST /wes-work-planning/paths/{pathId}/work-releases", 200); got != 3 {
		t.Fatalf("%d units released, want 3", got)
	}
}

func TestAReleaseRacedByAnotherReleaseIsNotAFailure(t *testing.T) {
	w := newWorld(t)
	w.wes.concurrentOnce = 3
	w.addOrder(std("OM-1"))
	f := provisioned(t, w, Settings{}, Crew{Pickers: 1, Packers: 1})
	if r := f.Process(context.Background(), "OM-1"); !r.OK() {
		t.Fatalf("problems %v", r.Problems)
	}
	if got := w.countStatus("POST /wes-work-planning/paths/{pathId}/work-releases", 409); got != 3 {
		t.Fatalf("%d refused releases, want the 3 scripted ones", got)
	}
}

func TestPullReleaseMayAdmitOthersWorkFirstAndThePickerWorksItByDefault(t *testing.T) {
	w := newWorld(t)
	w.addOrder(std("OM-1"))
	w.addForeignUnit("OTHER-line-1", "OTHER", "PICK", cptEarly)
	f := provisioned(t, w, Settings{}, Crew{Pickers: 1, Packers: 1})
	r := f.Process(context.Background(), "OM-1")
	if !r.OK() {
		t.Fatalf("problems %v", r.Problems)
	}
	if st := f.Stats(); st != (Stats{ForeignReleased: 1, ForeignWorked: 1}) {
		t.Fatalf("stats %+v, want the one foreign release and the one foreign pick", st)
	}
	if got := w.tasksOf("OTHER-line-1"); len(got) != 1 || got[0].Status != TaskCompleted {
		t.Fatalf("the foreign task: %+v", got)
	}
}

func TestLeaveForeignKeepsHandsOffOtherPeoplesTasks(t *testing.T) {
	w := newWorld(t)
	w.addOrder(std("OM-1"))
	w.addForeignUnit("OTHER-line-1", "OTHER", "PICK", cptEarly)
	f := provisioned(t, w, Settings{LeaveForeign: true}, Crew{Pickers: 1, Packers: 1})
	r := f.Process(context.Background(), "OM-1")
	if !r.OK() {
		t.Fatalf("problems %v", r.Problems)
	}
	if st := f.Stats(); st != (Stats{ForeignReleased: 1, ForeignLeft: 1}) {
		t.Fatalf("stats %+v", st)
	}
	status, by := w.taskState(w.tasksOf("OTHER-line-1")[0].ID)
	if status != TaskClaimed || !strings.HasPrefix(by, "RD01-PICK-") {
		t.Fatalf("the foreign task is %s by %q: it must be left claimed, not completed", status, by)
	}
	if fi := f.Findings(); len(fi) != 1 || !strings.Contains(fi[0], "OTHER-line-1") || !strings.Contains(fi[0], "left CLAIMED") {
		t.Fatalf("findings %v", fi)
	}
}

func TestAForeignTaskAlreadyInTheQueueIsHandledTheSameWay(t *testing.T) {
	for name, leave := range map[string]bool{"worked": false, "left": true} {
		t.Run(name, func(t *testing.T) {
			w := newWorld(t)
			w.addOrder(std("OM-1"))
			w.mu.Lock()
			w.fe.addForeignPick("SIM1-order-9-line-1", cptEarly)
			w.mu.Unlock()
			f := provisioned(t, w, Settings{LeaveForeign: leave}, Crew{Pickers: 1, Packers: 1})
			if r := f.Process(context.Background(), "OM-1"); !r.OK() {
				t.Fatalf("problems %v", r.Problems)
			}
			st := f.Stats()
			if leave != (st.ForeignLeft == 1) || leave == (st.ForeignWorked == 1) || st.ForeignReleased != 0 {
				t.Fatalf("stats %+v", st)
			}
		})
	}
}

func TestTooMuchOtherWorkAheadStopsTheReleaseOperatorAndSaysSo(t *testing.T) {
	w := newWorld(t)
	w.addOrder(std("OM-1"))
	for i := 0; i < 5; i++ {
		w.addForeignUnit(fmt.Sprintf("OTHER-%d-line-1", i), fmt.Sprintf("OTHER-%d", i), "PICK", cptEarly)
	}
	f := provisioned(t, w, Settings{MaxForeignReleases: 2}, Crew{Pickers: 1, Packers: 1})
	r := f.Process(context.Background(), "OM-1")
	if r.OK() || !hasProblem(r, "released 3 work units of other orders", "budget 2") {
		t.Fatalf("problems %v", r.Problems)
	}
	if got := w.countStatus("POST /wes-work-planning/paths/{pathId}/work-releases", 200); got != 3 {
		t.Fatalf("%d releases: the operator must stop at the first one over the budget", got)
	}
}

func TestAForeignPackTaskIsLeftClaimedAndTheOrdersOwnIsStillPacked(t *testing.T) {
	w := newWorld(t)
	w.addOrder(std("OM-1"))
	w.mu.Lock()
	foreign := w.fe.addForeignPack("OTHER", cptEarly)
	w.mu.Unlock()
	f := provisioned(t, w, Settings{}, Crew{Pickers: 1, Packers: 1})
	r := f.Process(context.Background(), "OM-1")
	if !r.OK() {
		t.Fatalf("problems %v", r.Problems)
	}
	if status, by := w.taskState(foreign.ID); status != TaskClaimed || by != "RD01-PACK-01" {
		t.Fatalf("foreign PACK task is %s by %q: the contents are unknown, it can only be left", status, by)
	}
	if fi := f.Findings(); len(fi) != 1 || !strings.Contains(fi[0], "PACK") || !strings.Contains(fi[0], "OTHER") {
		t.Fatalf("findings %v", fi)
	}
	if f.Stats().ForeignLeft != 1 {
		t.Fatalf("stats %+v", f.Stats())
	}
}

func TestExplicitConfirmationReadsFirstAndConfirmsBeforeCompleting(t *testing.T) {
	w := newWorld(t)
	w.addOrder(std("OM-1"))
	w.mu.Lock()
	w.inv.concurrentOnce["res-OM-1-1"] = 2 // two pickers emptied the bin at once
	w.mu.Unlock()
	f := provisioned(t, w, Settings{ConfirmPicks: ConfirmExplicit}, Crew{Pickers: 1, Packers: 1})
	r := f.Process(context.Background(), "OM-1")
	if !r.OK() {
		t.Fatalf("problems %v", r.Problems)
	}
	if got := w.callsTo("POST /inventory-storage/reservations/{id}/pick-confirmations"); got != 4 { // 2 refused + 1 + 1
		t.Fatalf("%d confirmations, want 4 (two refused for concurrent-modification, then one per line)", got)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, id := range []string{"res-OM-1-1", "res-OM-1-2"} {
		if by := w.inv.confirmedBy[id]; by != "pick-confirmations" {
			t.Fatalf("reservation %s was confirmed by %q", id, by)
		}
	}
}

func TestExplicitConfirmationLeavesAReservationTheConsumerAlreadyConfirmed(t *testing.T) {
	w := newWorld(t)
	w.addOrder(std("OM-1"))
	w.mu.Lock()
	w.inv.find("res-OM-1-1").Status = ReservationConfirmed
	w.mu.Unlock()
	f := provisioned(t, w, Settings{ConfirmPicks: ConfirmExplicit}, Crew{Pickers: 1, Packers: 1})
	if r := f.Process(context.Background(), "OM-1"); !r.OK() {
		t.Fatalf("problems %v", r.Problems)
	}
	if got := w.callsTo("POST /inventory-storage/reservations/{id}/pick-confirmations"); got != 1 {
		t.Fatalf("%d confirmations, want only the ACTIVE line's", got)
	}
}

func TestTheDefaultNeverCallsTheSupersededConfirmRoute(t *testing.T) {
	w := newWorld(t)
	w.addOrder(std("OM-1"))
	f := provisioned(t, w, Settings{}, Crew{Pickers: 1, Packers: 1})
	if r := f.Process(context.Background(), "OM-1"); !r.OK() {
		t.Fatalf("problems %v", r.Problems)
	}
	if got := w.callsTo("POST /inventory-storage/reservations/{id}/pick-confirmations"); got != 0 {
		t.Fatalf("%d calls to the superseded route", got)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.inv.confirmedBy["res-OM-1-1"] != "TaskCompleted" {
		t.Fatalf("confirmed by %q", w.inv.confirmedBy["res-OM-1-1"])
	}
}

func TestAReservationNobodyConfirmsIsAFindingOnAnOtherwiseLabeledOrder(t *testing.T) {
	w := newWorld(t)
	w.addOrder(std("OM-1"))
	w.inv.consumerOff = true
	f := provisioned(t, w, Settings{Lag: Waits{Timeout: 5e9, Interval: 1e9}}, Crew{Pickers: 1, Packers: 1})
	r := f.Process(context.Background(), "OM-1")
	if !r.Labeled() || r.OK() {
		t.Fatalf("labeled=%v ok=%v problems %v", r.Labeled(), r.OK(), r.Problems)
	}
	if !hasProblem(r, "res-OM-1-1", "ACTIVE", "TaskCompleted consumer") {
		t.Fatalf("problems %v", r.Problems)
	}
}

func TestAReservationReleasedBeforeItsPickIsAFinding(t *testing.T) {
	w := newWorld(t)
	w.addOrder(std("OM-1"))
	w.mu.Lock()
	w.inv.find("res-OM-1-2").Status = "REVOKED"
	w.mu.Unlock()
	f := provisioned(t, w, Settings{}, Crew{Pickers: 1, Packers: 1})
	r := f.Process(context.Background(), "OM-1")
	if r.OK() || !hasProblem(r, "res-OM-1-2", "REVOKED", "released before") {
		t.Fatalf("problems %v", r.Problems)
	}
}

func TestPackageManifestedIsAwaitedOnTheTapWhenThereIsOne(t *testing.T) {
	w := newWorld(t)
	w.addOrder(std("OM-1"))
	f := provisioned(t, w, Settings{Events: tap{w}}, Crew{Pickers: 1, Packers: 1})
	r := f.Process(context.Background(), "OM-1")
	if !r.OK() {
		t.Fatalf("problems %v", r.Problems)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.fe.manifested["OM-1"] != 1 {
		t.Fatalf("manifested %d times", w.fe.manifested["OM-1"])
	}
}

func TestAManifestThatNeverComesIsAFindingNamingTheTap(t *testing.T) {
	w := newWorld(t)
	w.addOrder(std("OM-1"))
	w.fe.tapLag = 1 << 30
	f := provisioned(t, w, Settings{Events: tap{w}, Lag: Waits{Timeout: 5e9, Interval: 1e9}}, Crew{Pickers: 1, Packers: 1})
	r := f.Process(context.Background(), "OM-1")
	if !r.Labeled() || r.OK() || !hasProblem(r, "PackageManifested", "tap") {
		t.Fatalf("labeled=%v problems %v", r.Labeled(), r.Problems)
	}
}

func TestProcessingAnOrderAgainAddsNoWrites(t *testing.T) {
	w := newWorld(t)
	w.addOrder(std("OM-1"))
	f := provisioned(t, w, Settings{}, Crew{Pickers: 1, Packers: 1})
	first := f.Process(context.Background(), "OM-1")
	if !first.OK() {
		t.Fatalf("problems %v", first.Problems)
	}
	before := w.writes()
	second := f.Process(context.Background(), "OM-1")
	if !second.OK() || second.PackageID != first.PackageID || second.Status != PackageLabeled {
		t.Fatalf("second run %+v vs first %+v", second, first)
	}
	if after := w.writes(); after != before {
		t.Fatalf("the second run wrote %d times: a floor that finds the order labeled has nothing to do", after-before)
	}
}

func TestAnOrderWorkedByOthersHalfWayIsTakenOverFromWhereItStands(t *testing.T) {
	// The first line was picked (and its task completed) by somebody else before this floor met
	// the order: nobody will ever claim that task again, so waiting for it would never end.
	w := newWorld(t)
	w.addOrder(std("OM-1"))
	f := provisioned(t, w, Settings{}, Crew{Pickers: 1, Packers: 1})
	w.mu.Lock()
	u := w.wes.units[0]
	u.State = UnitCompleted
	w.fe.createPick(u)
	t1 := w.fe.tasks[len(w.fe.tasks)-1]
	t1.Status = TaskCompleted
	w.inv.onPickCompleted(u.Reference, u.LineNo) // inventory-storage consumed that TaskCompleted
	w.mu.Unlock()
	r := f.Process(context.Background(), "OM-1")
	if !r.OK() {
		t.Fatalf("problems %v", r.Problems)
	}
	if got := w.callsTo("POST /fulfillment-execution/rebin/arrivals"); got != 2 {
		t.Fatalf("%d rebin arrivals, want one per line, the taken-over one included", got)
	}
}

func TestOrdersInFlightTogetherShareTheStationsAndAllGetLabeled(t *testing.T) {
	// Four orders, two pickers, two packers: a station is handed another order's task and works it
	// for that order. Nothing here depends on WHICH station worked WHICH task, only on the totals.
	w := newWorld(t)
	var ids []string
	lines := 0
	for i := 0; i < 4; i++ {
		id := fmt.Sprintf("OM-%d", i)
		ids = append(ids, id)
		o := std(id)
		if i%2 == 1 {
			o.Lines = append(o.Lines, lineSpec{SKU: "SKU-C", Qty: 3})
		}
		o.CPT = fmt.Sprintf("2026-11-14T2%d:00:00Z", i)
		lines += len(o.Lines)
		w.addOrder(o)
	}
	f := provisioned(t, w, Settings{
		Site: "AUS3",
		// a bound no run of this test can reach: sleeps of OTHER goroutines advance the shared test clock
		Work: Waits{Timeout: 24 * 3600e9, Interval: 500e6}, Lag: Waits{Timeout: 24 * 3600e9, Interval: 500e6},
	}, Crew{Pickers: 2, Packers: 2})
	results := f.ProcessAll(context.Background(), ids, 0)
	for i, r := range results {
		if !r.OK() {
			t.Fatalf("order %s: %v", ids[i], r.Problems)
		}
		pk := w.packagesOf(ids[i])
		if len(pk) != 1 || pk[0].Status != PackageLabeled {
			t.Fatalf("packages of %s: %+v", ids[i], pk)
		}
		if got := len(w.fe.sealContents[r.PackageID]); got != len(r.Contents) || got == 0 {
			t.Fatalf("package %s: sealed %d items, result says %d", r.PackageID, got, len(r.Contents))
		}
	}
	w.mu.Lock()
	picks, packs := 0, 0
	for _, tk := range w.fe.tasks {
		if tk.Status != TaskCompleted {
			t.Errorf("task %s of %s is %s", tk.ID, tk.OrderRef, tk.Status)
		}
		switch tk.Type {
		case TypePick:
			picks++
		case TypePack:
			packs++
		}
	}
	w.mu.Unlock()
	if picks != lines || packs != 4 {
		t.Fatalf("%d PICK and %d PACK tasks, want %d and 4", picks, packs, lines)
	}
	if got := w.callsTo("POST /fulfillment-execution/rebin/arrivals"); got != lines {
		t.Fatalf("%d rebin arrivals, want one per line (%d)", got, lines)
	}
	if got := w.callsTo("POST /fulfillment-execution/packages/{id}/slam-runs"); got != 4 {
		t.Fatalf("%d SLAM runs, want 4", got)
	}
}

func TestTheSameOrderTwiceInOneBatchIsProcessedOnce(t *testing.T) {
	w := newWorld(t)
	w.addOrder(std("OM-1"))
	f := provisioned(t, w, Settings{}, Crew{Pickers: 1, Packers: 1})
	rs := f.ProcessAll(context.Background(), []string{"OM-1", "OM-1"}, 2)
	if !rs[0].OK() || rs[1].OK() || !hasProblem(rs[1], "given twice") {
		t.Fatalf("results %+v", rs)
	}
}

func TestALineTheOrderAlreadyTookOverIsNotScannedIntoRebinTwice(t *testing.T) {
	// Two parties can notice one completed PICK task at the same time: the picker who completed it and
	// the order's own look at fulfillment-execution while it waits. Whoever marks the line picked first
	// scans it in; the other must not.
	w := newWorld(t)
	w.addOrder(std("OM-1"))
	f := provisioned(t, w, Settings{}, Crew{Pickers: 1, Packers: 1})
	ctx := context.Background()
	st := &orderState{id: "OM-1"}
	f.register(st)
	if err := f.awaitOrder(ctx, st); err != nil {
		t.Fatal(err)
	}
	if err := f.awaitUnits(ctx, st); err != nil {
		t.Fatal(err)
	}
	f.registerUnits(st)
	w.mu.Lock()
	u := w.wes.units[0]
	u.State = UnitReleased
	w.fe.createPick(u)
	w.mu.Unlock()
	st.markPicked(st.line(1), cptEvening, false) // the order's own look took line 1 over first

	for i := 0; i < 5 && w.pendingPicks() > 0; i++ {
		if _, _, err := f.pickStep(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if w.pendingPicks() != 0 {
		t.Fatal("the PICK task was never claimed")
	}
	if got := w.callsTo("POST /fulfillment-execution/rebin/arrivals"); got != 0 {
		t.Fatalf("%d rebin arrivals for a line somebody else had already scanned in", got)
	}
	if err := st.failed(); err != nil {
		t.Fatal(err)
	}
}

func TestACancelledContextEndsTheRunWithAFindingAndNoWrites(t *testing.T) {
	w := newWorld(t)
	w.addOrder(std("OM-1"))
	f := provisioned(t, w, Settings{}, Crew{Pickers: 1, Packers: 1})
	before := w.writes()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := f.Process(ctx, "OM-1")
	if r.OK() || len(r.Problems) == 0 {
		t.Fatalf("result %+v", r)
	}
	if w.writes() != before {
		t.Fatalf("%d writes after the context was cancelled", w.writes()-before)
	}
}
