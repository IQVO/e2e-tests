package floor

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/url"
)

// packOrder runs the packers until the order's package is sealed. The PACK task does not exist until
// the rebin arrival that completes the order's line set created it.
func (f *Floor) packOrder(ctx context.Context, st *orderState) error {
	what := "order " + st.id + ": pack it"
	err := await(ctx, f.clock, f.set.Work, what, func(ctx context.Context) (verdict, string, error) {
		if err := st.failed(); err != nil {
			return 0, "", err
		}
		if st.sealed() != nil {
			return vDone, "", nil
		}
		worked, why, err := f.packStep(ctx)
		if err != nil {
			return 0, "", err
		}
		if err := st.failed(); err != nil {
			return 0, "", err
		}
		if worked {
			return vAgain, "", nil
		}
		return vWait, why, nil
	})
	if err != nil && IsTimeout(err) {
		err = fmt.Errorf("%w; %s", err, f.diagnosePack(ctx, st))
	}
	return err
}

// packStep is one packer's turn: take a free station, pull the next PACK task and, when it is the
// PACK task of an order of this floor, scan its contents, seal the package and complete the task.
// A PACK task of nobody this floor knows cannot be packed (the contents are unknown): it is left
// CLAIMED and reported, and the packer goes on to the next.
func (f *Floor) packStep(ctx context.Context) (bool, string, error) {
	s := f.acquire(&f.packers)
	if s == nil {
		return false, "every packer is busy", nil
	}
	defer f.free(s)
	r := f.call(ctx, SvcExecution, http.MethodPost, "/stations/{id}/task-claims", "/stations/"+url.PathEscape(s.ID)+"/task-claims", map[string]any{"taskType": TypePack})
	if noClaimableTask.matches(r) {
		return false, "409 no-claimable-task (no PACK task to claim: rebin has not completed the order's line set, or the pending ones are held by other stations)", nil
	}
	if !r.OK() {
		return false, "", refusal("claim the next PACK task at "+s.ID, r)
	}
	var t task
	if err := r.Decode(&t); err != nil {
		return false, "", fmt.Errorf("claim the next PACK task at %s: %w", s.ID, err)
	}
	if t.Type != TypePack || t.ID == "" {
		return false, "", fmt.Errorf("claim the next PACK task at %s: got task %q of type %q", s.ID, t.ID, t.Type)
	}
	o := f.lookupOrder(t.OrderRef)
	if o == nil {
		f.foreignLeft.Add(1)
		f.finding("claimed PACK task %s of %q, which no order of this floor owns and whose contents are unknown: left CLAIMED under %s, its lease expires on its own", t.ID, t.OrderRef, s.ID)
		return true, "", nil
	}
	if err := f.sealAndComplete(ctx, s, o, t); err != nil {
		o.fail(err)
	}
	return true, "", nil
}

// sealAndComplete is the packer scanning the order's contents into a carton and sealing it
// (POST /tasks/{id}/package-seals creates the package; the task stays claimed), then finishing the task.
func (f *Floor) sealAndComplete(ctx context.Context, s *station, o *orderState, t task) error {
	contents := o.contents()
	what := fmt.Sprintf("seal the package of order %s at %s", o.id, s.ID)
	r := f.call(ctx, SvcExecution, http.MethodPost, "/tasks/{id}/package-seals", "/tasks/"+url.PathEscape(t.ID)+"/package-seals", map[string]any{"stationId": s.ID, "contents": contents})
	if r.Status != http.StatusCreated {
		return refusal(what, r)
	}
	var p pkgDTO
	if err := r.Decode(&p); err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	if p.ID == "" || p.Status != PackageSealed {
		return fmt.Errorf("%s: package %q is %q, want a SEALED package with an id (body: %s)", what, p.ID, p.Status, truncate(r.Body))
	}
	o.setPackage(&p, t.ID)
	f.logf(s.Occupant, "sealed %d item(s) of %s into package %s (lane %s)", len(contents), o.id, p.ID, p.SortLane)
	if err := f.completeTask(ctx, s, t); err != nil {
		return fmt.Errorf("package %s sealed but %w", p.ID, err)
	}
	return nil
}

// slam is the SLAM operator: weigh the sealed package and read what became of it. The call answers
// 204 whether the label was applied or the package diverted, so the outcome is READ (GET /packages/{id}).
// A diverted package goes to problem-solve and is never manifested: it is a finding, and the floor
// does not re-pack it.
func (f *Floor) slam(ctx context.Context, st *orderState, r *Result) error {
	p := st.sealed()
	actual, expected := f.set.Scale.Weigh(st.id, p.ScannedContents)
	actual, expected = round3(actual), round3(expected)
	what := fmt.Sprintf("SLAM package %s of order %s", p.ID, st.id)
	res := f.call(ctx, SvcExecution, http.MethodPost, "/packages/{id}/slam-runs", "/packages/"+url.PathEscape(p.ID)+"/slam-runs", map[string]any{"actualWeight": actual, "expectedWeight": expected})
	if res.Status != http.StatusNoContent {
		return refusal(what, res)
	}
	r.WeightG = int(math.Round(actual * 1000))
	g := f.call(ctx, SvcExecution, http.MethodGet, "/packages/{id}", "/packages/"+url.PathEscape(p.ID), nil)
	if !g.OK() {
		return refusal("read "+what, g)
	}
	var got pkgDTO
	if err := g.Decode(&got); err != nil {
		return fmt.Errorf("read %s: %w", what, err)
	}
	r.Status = got.Status
	st.setPackage(&got, st.packTaskID())
	switch got.Status {
	case PackageLabeled:
		f.logf("slam", "package %s of %s LABELED at %.3f kg", p.ID, st.id, actual)
		return nil
	case PackageDiverted:
		return fmt.Errorf("%s: DIVERTED to problem-solve (the scale read %.3f kg, %.3f kg expected, tolerance %.2f): it is never manifested and the floor does not re-pack it", what, actual, expected, slamToleranceKg)
	}
	return fmt.Errorf("%s: the package is %q after the weigh-check, want LABELED or DIVERTED", what, got.Status)
}

func round3(f float64) float64 { return math.Round(f*1000) / 1000 }

func (o *orderState) packTaskID() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.packTask
}

// diagnosePack says, for a timeout, what fulfillment-execution holds for the order's PACK task.
func (f *Floor) diagnosePack(ctx context.Context, st *orderState) string {
	tasks, r := listAll[task](ctx, f.c, SvcExecution, "/tasks?orderRef", "/tasks?orderRef="+url.QueryEscape(st.id))
	switch {
	case !r.OK():
		return fmt.Sprintf("could not read the order's tasks: %v", refusal("read tasks of "+st.id, r))
	case len(tasks) == 0:
		return fmt.Sprintf("no PACK task exists for order %s: the rebin arrivals did not complete its line set (lines %v unpicked)", st.id, st.unpicked())
	}
	var seen []string
	for _, t := range tasks {
		seen = append(seen, fmt.Sprintf("%s %s %s", t.ID, t.Type, t.Status+orBy(t.LeaseStationID)))
	}
	return fmt.Sprintf("tasks of the order: %v", seen)
}
