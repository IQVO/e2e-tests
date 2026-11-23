package main

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// The planner's morning plans the catalogue path PICK: warehouse-planning's
// path_id and workforce-management's pathId (the WES pool PICK work lands in
// is cfg.wesPickPath, a different identifier).
const (
	planningPath = "PICK"
	// plannedRate is the planned PICK rate, units per head per hour, and
	// plannedShiftHours the length of the shift the labour plan covers.
	plannedRate       = 60.0
	plannedShiftHours = 8.0
)

// planTheDay runs the planner's morning (warehouse-planning ADR 0023): the
// workload forecast of the PICK path at the sim's site, the shift's labour
// plan with the heads DERIVED from that forecast, workforce-management's
// proposal read from its copy of the published labour plan (ADR 0043), then
// the committed shift plan. wes-work-planning is not called: its
// POST /paths/{pathId}/charge and /plan moved to warehouse-planning.
func (s *sim) planTheDay(ctx context.Context) error {
	units := 0
	for _, o := range s.orders {
		for _, l := range o.lines {
			units += l.qty
		}
	}
	buckets := s.forecastBuckets(units)
	heads := map[string]int{
		"PICK": s.cfg.pickers + s.cfg.flex, "PACK": s.cfg.packers + s.cfg.flex,
		"SLAM": s.cfg.slammers, "REBIN": s.cfg.rebinners,
	}
	site, shift := s.cfg.siteCode, s.shiftID()

	// PUT is naturally idempotent (no Idempotency-Key): a receipt replaces
	// the (site, path) forecast as a whole, so a second run only revises it.
	r := s.api.Call(ctx, svcPlanning, http.MethodPut, "/workload-forecasts/{site_id}/{path_id}",
		"/workload-forecasts/"+url.PathEscape(site)+"/"+planningPath, map[string]any{"buckets": buckets})
	if !r.OK() {
		return fail("PUT /workload-forecasts/%s/%s -> %d %s", site, planningPath, r.Status, truncate(string(r.Body), 200))
	}
	var forecast struct {
		TotalQuantity int64 `json:"total_quantity"`
		Version       int64 `json:"version"`
	}
	if err := r.Decode(&forecast); err != nil {
		return fail("PUT /workload-forecasts/%s/%s: %v", site, planningPath, err)
	}
	logf("planner", "workload forecast %s/%s: %d units across %d trucks (-> %d, version %d)", site, planningPath, forecast.TotalQuantity, len(buckets), r.Status, forecast.Version)

	planned, err := s.commitLabourPlan(ctx, site, shift, heads["PICK"])
	if err != nil {
		return err
	}
	s.proposeFromLabourPlan(ctx, site, shift, planned)

	r = s.api.Call(ctx, svcWorkforce, http.MethodPost, "/shift-plans", "/shift-plans",
		map[string]any{"buildingId": s.cfg.buildingID, "shiftId": shift, "lines": shiftPlanLines(heads)})
	if !r.OK() {
		// Not fatal for the floor (associates are assigned individually),
		// but the supervisor loses the planned-vs-active staffing view.
		s.finding("workforce-management rejected the shift plan: %d %s", r.Status, truncate(string(r.Body), 220))
	} else {
		s.shiftPlanned = true
	}
	logf("planner", "shift plan %s (committed=%v; PICK %d, PACK %d, SLAM %d, REBIN %d heads; labour plan PICK %d)", shift, s.shiftPlanned, heads["PICK"], heads["PACK"], heads["SLAM"], heads["REBIN"], planned)
	return nil
}

// forecastBuckets splits the day's pick units across the truck cutoffs.
func (s *sim) forecastBuckets(units int) []map[string]any {
	var buckets []map[string]any
	for i, t := range truckCutoffs {
		share := []float64{0.35, 0.40, 0.25}[i]
		buckets = append(buckets, map[string]any{
			"cpt":      s.clock.wallAt(t.h, t.m).UTC().Truncate(time.Minute).Add(time.Minute).Format(time.RFC3339),
			"quantity": int(float64(units) * share),
		})
	}
	return buckets
}

// shiftPlanLines is the shift plan's staffing lines, one per staffed path.
func shiftPlanLines(heads map[string]int) []map[string]any {
	var lines []map[string]any
	for _, p := range []string{"PICK", "PACK", "SLAM", "REBIN"} {
		if heads[p] == 0 {
			continue
		}
		lines = append(lines, map[string]any{
			"pathId": p, "plannedHeads": heads[p], "plannedRate": plannedRate,
			"plannedHours": float64(heads[p]) * plannedShiftHours, "installedStations": heads[p],
		})
	}
	return lines
}

// shiftStart is the wall instant the simulated 06:00 shift starts, the
// labour plan's shift_start (whole seconds, UTC).
func (s *sim) shiftStart() time.Time {
	return s.clock.wallAt(dayOpenHour, 0).UTC().Truncate(time.Second)
}

// commitLabourPlan PUTs the shift's PICK labour plan WITHOUT planned_heads,
// so warehouse-planning derives them from the stored forecast:
// ceil(units with a cpt in [shift_start, shift_start + hours) / (rate x
// hours)). installed_stations is the PICK heads the sim staffs. Derived heads
// above that are 422 heads-exceed-stations: the planner then commits the
// staffed heads explicitly and records a finding, the day goes on. It returns
// the heads warehouse-planning stored.
func (s *sim) commitLabourPlan(ctx context.Context, site, shift string, staffed int) (int, error) {
	const route = "/labour-plans/{site_id}/{shift_id}/{path_id}"
	path := "/labour-plans/" + url.PathEscape(site) + "/" + url.PathEscape(shift) + "/" + planningPath
	body := map[string]any{
		"rate_units_per_hour": plannedRate, "hours": plannedShiftHours,
		"installed_stations": staffed, "shift_start": s.shiftStart().Format(time.RFC3339),
	}
	r := s.api.Call(ctx, svcPlanning, http.MethodPut, route, path, body)
	if r.Status == http.StatusUnprocessableEntity && strings.Contains(string(r.Body), "heads-exceed-stations") {
		s.finding("warehouse-planning derived more %s heads than the %d stations the sim staffs (%s): committed the staffed heads explicitly", planningPath, staffed, truncate(string(r.Body), 200))
		body["planned_heads"] = staffed
		r = s.api.Call(ctx, svcPlanning, http.MethodPut, route, path, body)
	}
	if !r.OK() {
		return 0, fail("PUT /labour-plans/%s/%s/%s -> %d %s", site, shift, planningPath, r.Status, truncate(string(r.Body), 200))
	}
	var plan struct {
		PlannedHeads int      `json:"planned_heads"`
		HeadsSource  string   `json:"heads_source"`
		Version      int64    `json:"version"`
		Warnings     []string `json:"warnings"`
	}
	if err := r.Decode(&plan); err != nil {
		return 0, fail("PUT /labour-plans/%s/%s/%s: %v", site, shift, planningPath, err)
	}
	logf("planner", "labour plan %s/%s/%s: %d heads (heads_source %s; %d stations, %.0f u/h x %.0f h; -> %d, version %d)",
		site, shift, planningPath, plan.PlannedHeads, plan.HeadsSource, staffed, plannedRate, plannedShiftHours, r.Status, plan.Version)
	for _, w := range plan.Warnings {
		logf("planner", "labour plan warning: %s", w)
	}
	return plan.PlannedHeads, nil
}

// proposeFromLabourPlan asks workforce-management for the PICK proposal of
// the shift WITHOUT a charge, so it answers the planned_heads warehouse-planning
// published (rateSource labour-plan, ADR 0043). Its copy of LabourPlanCommitted
// arrives over Kafka: 404 labour-plan-not-found (or a copy of an earlier
// commit) is polled for at most proposalWait, then recorded as a finding; the
// day never stops on it.
func (s *sim) proposeFromLabourPlan(ctx context.Context, site, shift string, want int) {
	type proposal struct {
		ProposedHeads int     `json:"proposedHeads"`
		ResolvedRate  float64 `json:"resolvedRate"`
		RateSource    string  `json:"rateSource"`
	}
	deadline := time.Now().Add(s.proposalWait)
	asks := 0
	var last resp
	for {
		asks++
		last = s.api.Call(ctx, svcWorkforce, http.MethodPost, "/paths/{pathId}/plan-proposals", "/paths/"+planningPath+"/plan-proposals",
			map[string]any{"siteCode": site, "shiftId": shift})
		switch {
		case last.OK():
			var p proposal
			if err := last.Decode(&p); err != nil {
				s.finding("workforce-management proposal for %s/%s: %v", shift, planningPath, err)
				return
			}
			if p.RateSource != "labour-plan" {
				s.finding("workforce-management proposed %d %s heads from rateSource %q, want labour-plan (ADR 0043)", p.ProposedHeads, planningPath, p.RateSource)
				return
			}
			if p.ProposedHeads == want {
				logf("planner", "workforce proposal from the labour plan: %d %s heads at %.0f u/h (after %d ask(s))", p.ProposedHeads, planningPath, p.ResolvedRate, asks)
				return
			}
			// A copy of an earlier commit of this shift: wait for the new one.
		case last.Status == http.StatusNotFound:
			// labour-plan-not-found: the copy has not arrived yet.
		default:
			s.finding("workforce-management proposal from the labour plan for %s/%s -> %d %s", shift, planningPath, last.Status, truncate(string(last.Body), 200))
			return
		}
		if !time.Now().Add(s.proposalPoll).Before(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.proposalPoll):
		}
	}
	s.finding("workforce-management had no proposal of %d %s heads from the labour plan of %s/%s within %s (%d asks; last %d %s)",
		want, planningPath, site, shift, s.proposalWait, asks, last.Status, truncate(strings.TrimSpace(string(last.Body)), 200))
}
