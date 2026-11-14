package floor

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/claudioed/warehouse-e2e/internal/sim"
)

// pickLines runs the release operator and the pickers until every line of the order is picked. They
// are ONE loop on purpose: a release-fed pool admits work only while it is under its WIP limit, and
// room appears when pickers complete work, so releasing everything first and picking after would
// wait for room that only picking makes.
func (f *Floor) pickLines(ctx context.Context, st *orderState) error {
	what := "order " + st.id + ": pick its lines"
	err := await(ctx, f.clock, f.set.Work, what, func(ctx context.Context) (verdict, string, error) {
		if err := st.failed(); err != nil {
			return 0, "", err
		}
		if st.allPicked() {
			return vDone, "", nil
		}
		released, whyRelease, err := f.releaseStep(ctx, st)
		if err != nil {
			return 0, "", err
		}
		worked, whyClaim, err := f.pickStep(ctx)
		if err != nil {
			return 0, "", err
		}
		if err := st.failed(); err != nil {
			return 0, "", err
		}
		if released || worked {
			return vAgain, "", nil
		}
		return vWait, fmt.Sprintf("lines %v unpicked; release: %s; claim: %s", st.unpicked(), orDash(whyRelease), orDash(whyClaim)), nil
	})
	if err != nil && IsTimeout(err) {
		err = fmt.Errorf("%w; %s", err, f.diagnosePicks(ctx, st))
	}
	return err
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// releaseStep is the release operator: if the order still has a Pending unit it admits the next unit
// of that unit's path. wes-work-planning picks the unit (earliest cpt), not the caller, so what it
// admits may belong to another order. It reports whether a unit was admitted.
func (f *Floor) releaseStep(ctx context.Context, st *orderState) (bool, string, error) {
	if st.releasedAll() {
		return false, "", nil
	}
	units, err := f.readUnits(ctx, st)
	if err != nil {
		return false, "", err
	}
	state := map[string]string{}
	for _, u := range units {
		state[u.ID] = u.State
	}
	pending := st.syncUnitStates(state)
	if len(pending) == 0 {
		st.markReleasedAll()
		return false, "", nil
	}
	path := pending[0].PathID
	route, p := "/paths/{pathId}/work-releases", "/paths/"+url.PathEscape(path)+"/work-releases"
	r := f.call(ctx, SvcWES, http.MethodPost, route, p, nil)
	switch {
	case r.OK():
		var u workUnit
		if err := r.Decode(&u); err != nil {
			return false, "", fmt.Errorf("release the next unit of path %s: %w", path, err)
		}
		if u.Reference == st.id || f.knowsUnit(u.ID) {
			f.logf("releaser", "released %s (path %s, cpt %s)", u.ID, path, u.CPT)
			return true, "", nil
		}
		f.foreignReleased.Add(1)
		if n := st.addForeignRelease(); n > f.set.MaxForeignReleases {
			return false, "", fmt.Errorf("order %s: released %d work units of other orders on the way to its own (budget %d, last %s, cpt %s): the pool is deep with work that is not ours and release is pull, so the earliest cpt goes first",
				st.id, n, f.set.MaxForeignReleases, u.ID, u.CPT)
		}
		f.logf("releaser", "released %s (path %s, cpt %s): not an order of this floor", u.ID, path, u.CPT)
		return true, "", nil
	case wipLimitReached.matches(r), poolConcurrent.matches(r), workPoolEmpty.matches(r):
		return false, fmt.Sprintf("%d %s on path %s", r.Status, Slug(r), path), nil
	}
	return false, "", refusal("release the next unit of path "+path, r)
}

func (f *Floor) knowsUnit(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, known := f.units[id]
	return known
}

// pickStep is one picker's turn: take a free station, pull the next PICK task and work it. Pull
// dispatch hands out the earliest-CPT task whoever it belongs to, so the task is looked up: a line of
// an order in flight on this floor is worked for THAT order (its state is updated, its rebin done),
// anything else is foreign (see Settings.LeaveForeign). It reports whether a task was claimed.
func (f *Floor) pickStep(ctx context.Context) (bool, string, error) {
	s := f.acquire(&f.pickers)
	if s == nil {
		return false, "every picker is busy", nil
	}
	defer f.free(s)
	r := f.call(ctx, SvcExecution, http.MethodPost, "/stations/{id}/task-claims", "/stations/"+url.PathEscape(s.ID)+"/task-claims", map[string]any{"taskType": TypePick})
	if noClaimableTask.matches(r) {
		return false, "409 no-claimable-task (no PICK task to claim: WorkReleased not consumed yet, or the pending ones are held by other stations)", nil
	}
	if !r.OK() {
		return false, "", refusal("claim the next PICK task at "+s.ID, r)
	}
	var t task
	if err := r.Decode(&t); err != nil {
		return false, "", fmt.Errorf("claim the next PICK task at %s: %w", s.ID, err)
	}
	if t.Type != TypePick || t.ID == "" {
		return false, "", fmt.Errorf("claim the next PICK task at %s: got task %q of type %q", s.ID, t.ID, t.Type)
	}
	o, ln := f.lookupUnit(t.OrderRef)
	if o == nil {
		f.foreignPick(ctx, s, t)
		return true, "", nil
	}
	if err := f.workPick(ctx, s, o, ln, t); err != nil {
		o.fail(err)
	}
	return true, "", nil
}

// foreignPick handles a PICK task that no order of this floor owns.
func (f *Floor) foreignPick(ctx context.Context, s *station, t task) {
	if f.set.LeaveForeign {
		f.foreignLeft.Add(1)
		f.finding("claimed PICK task %s of %q, which no order of this floor owns: left CLAIMED under %s, its lease expires on its own", t.ID, t.OrderRef, s.ID)
		return
	}
	if err := f.completeTask(ctx, s, t); err != nil {
		f.finding("could not complete PICK task %s of %q, which no order of this floor owns: %v", t.ID, t.OrderRef, err)
		return
	}
	f.foreignWorked.Add(1)
	f.logf(s.Occupant, "picked task %s of %q: not an order of this floor", t.ID, t.OrderRef)
}

// workPick is the picker working one line of an order of this floor, then the rebin associate
// scanning it in.
func (f *Floor) workPick(ctx context.Context, s *station, o *orderState, ln *lineState, t task) error {
	if f.set.ConfirmPicks == ConfirmExplicit {
		if err := f.confirmReservation(ctx, o, ln); err != nil {
			return err
		}
	}
	if err := f.completeTask(ctx, s, t); err != nil {
		return fmt.Errorf("pick line %d of order %s: %w", ln.No, o.id, err)
	}
	o.markPicked(ln, t.CPT, t.Fragile)
	f.logf(s.Occupant, "picked %dx %s for line %d of %s (task %s)", ln.Qty, ln.SKU, ln.No, o.id, t.ID)
	return f.rebin(ctx, o, ln)
}

// completeTask finishes a claimed task: only the claiming station may.
func (f *Floor) completeTask(ctx context.Context, s *station, t task) error {
	what := fmt.Sprintf("complete %s task %s at %s", t.Type, t.ID, s.ID)
	r := f.call(ctx, SvcExecution, http.MethodPost, "/tasks/{id}/completions", "/tasks/"+url.PathEscape(t.ID)+"/completions", map[string]any{"stationId": s.ID})
	if !r.OK() {
		return refusal(what, r)
	}
	return nil
}

// rebin is the rebin associate scanning one picked line into the order's consolidation slot. The
// first arrival of an order fixes its required line set; the arrival that completes it creates the
// PACK task, which takes packCpt and the hints from THAT call: the order's cpt (the picked tasks'),
// fragile when any picked line was, gift wrap when any line asks.
func (f *Floor) rebin(ctx context.Context, o *orderState, ln *lineState) error {
	cpt, fragile, gift, required := o.packHints()
	r := f.call(ctx, SvcExecution, http.MethodPost, "/rebin/arrivals", "/rebin/arrivals", map[string]any{
		"orderRef": o.id, "lineId": ln.lineID(), "requiredLineIds": required,
		"packCpt": cpt, "packRequiredCapabilities": []string{f.set.PackCapability}, "packFragile": fragile, "packGiftWrap": gift,
	})
	if r.Status != http.StatusNoContent {
		return refusal(fmt.Sprintf("scan line %d of order %s into rebin", ln.No, o.id), r)
	}
	return nil
}

// confirmReservation is the explicit confirm-pick (ConfirmExplicit): it reads the order's
// reservations, takes the line's and confirms it when it is ACTIVE. A CONFIRMED one (the consumer was
// first) is left alone. 409 concurrent-modification, two pickers emptying one bin, is retried in the
// bounded loop with a fresh key; any other refusal is a finding.
func (f *Floor) confirmReservation(ctx context.Context, o *orderState, ln *lineState) error {
	what := fmt.Sprintf("confirm the pick of line %d of order %s", ln.No, o.id)
	res, err := f.readReservations(ctx, o.id)
	if err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	x := findReservation(res, ln)
	if x == nil {
		return fmt.Errorf("%s: inventory-storage holds no reservation for it (demandRef %s)", what, o.id)
	}
	if x.Status != ReservationActive {
		if x.Status == ReservationConfirmed {
			return nil
		}
		return fmt.Errorf("%s: reservation %s is %s, not ACTIVE", what, x.ID, x.Status)
	}
	_, err = callTransient(ctx, f.clock, f.set.Lag, what, []Transient{stockConcurrent}, func() sim.Resp {
		return f.call(ctx, SvcInventory, http.MethodPost, "/reservations/{id}/pick-confirmations", "/reservations/"+url.PathEscape(x.ID)+"/pick-confirmations", nil)
	})
	return err
}

// diagnosePicks says, for a timeout, what fulfillment-execution holds for each unpicked line: no task
// at all means WorkReleased was not consumed, a CLAIMED one is another station's.
func (f *Floor) diagnosePicks(ctx context.Context, st *orderState) string {
	var parts []string
	for _, no := range st.unpicked() {
		ln := st.line(no)
		if ln == nil {
			continue
		}
		tasks, r := listAll[task](ctx, f.c, SvcExecution, "/tasks?orderRef", "/tasks?orderRef="+url.QueryEscape(ln.Unit.ID))
		switch {
		case !r.OK():
			parts = append(parts, fmt.Sprintf("line %d: could not read its tasks: %v", no, refusal("read tasks of "+ln.Unit.ID, r)))
		case len(tasks) == 0:
			parts = append(parts, fmt.Sprintf("line %d: no PICK task exists for unit %s (unit is %s): WorkReleased was not released or not consumed by fulfillment-execution", no, ln.Unit.ID, st.unitState(no)))
		default:
			var seen []string
			for _, t := range tasks {
				seen = append(seen, fmt.Sprintf("%s %s %s", t.ID, t.Type, t.Status+orBy(t.LeaseStationID)))
			}
			parts = append(parts, fmt.Sprintf("line %d: tasks %s", no, strings.Join(seen, ", ")))
		}
	}
	return strings.Join(parts, "; ")
}

func orBy(station string) string {
	if station == "" {
		return ""
	}
	return " by " + station
}
