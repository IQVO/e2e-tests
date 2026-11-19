package main

import (
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestWarehousePlanningIsAHarnessService: the planner's morning calls warehouse-planning, so it is
// in the health sweep and routed like every other context (Kong /api/<service> in gateway mode,
// WAREHOUSE_PLANNING_BASE_URL from env.sh on the local harness).
func TestWarehousePlanningIsAHarnessService(t *testing.T) {
	if !slices.Contains(allServices, svcPlanning) {
		t.Fatalf("allServices %v does not list %s: the health sweep would not check it", allServices, svcPlanning)
	}
	if got := newAPI(config{gateway: "http://edge:8000/api/"}, nil).Base[svcPlanning]; got != "http://edge:8000/api/warehouse-planning" {
		t.Fatalf("gateway base = %q, want http://edge:8000/api/warehouse-planning", got)
	}
	t.Setenv("WAREHOUSE_PLANNING_BASE_URL", "")
	if got := newAPI(config{}, nil).Base[svcPlanning]; got != "http://localhost:8100" {
		t.Fatalf("local default = %q, want env.sh's http://localhost:8100", got)
	}
	t.Setenv("WAREHOUSE_PLANNING_BASE_URL", "http://planning:9000")
	if got := newAPI(config{}, nil).Base[svcPlanning]; got != "http://planning:9000" {
		t.Fatalf("local base = %q, want the WAREHOUSE_PLANNING_BASE_URL override", got)
	}
}

type answer struct {
	status int
	body   string
}

const (
	forecastPath  = "/warehouse-planning/workload-forecasts/SIM1/PICK"
	proposalPath  = "/workforce-management/paths/PICK/plan-proposals"
	shiftPlanPath = "/workforce-management/shift-plans"
	forecast201   = `{"site_id":"SIM1","path_id":"PICK","buckets":[],"total_quantity":9,"received_at":"2026-11-19T05:00:00Z","version":1}`
	notPublished  = `{"type":"https://errors.workforce-management.warehouse-systems.dev/labour-plan-not-found","title":"No labour plan has been published for this site, shift and path","status":404}`
)

func labourPlan(status, heads int, source string) answer {
	return answer{status, fmt.Sprintf(`{"site_id":"SIM1","shift_id":"S","path_id":"PICK","planned_heads":%d,"rate_units_per_hour":60,"hours":8,"installed_stations":6,"heads_source":%q,"version":1,"warnings":[]}`, heads, source)}
}

func proposalOf(heads int, rateSource string) answer {
	return answer{http.StatusOK, fmt.Sprintf(`{"pathId":"PICK","proposedHeads":%d,"resolvedRate":60,"rateSource":%q}`, heads, rateSource)}
}

// plannerFake answers the planner's morning: the forecast PUT, the labour plan PUT (one answer
// per call, the last one repeated), workforce-management's proposal (likewise) and the shift-plan
// commit. Anything else, wes-work-planning included, is an unrouted 404.
func plannerFake(labourPlans, proposals []answer) *fakeServices {
	var mu sync.Mutex
	lp, pr := 0, 0
	next := func(list []answer, i *int) (int, string) {
		a := list[min(*i, len(list)-1)]
		*i++
		return a.status, a.body
	}
	return &fakeServices{reply: func(r *http.Request) (int, string) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.Method == http.MethodPut && r.URL.Path == forecastPath:
			return http.StatusCreated, forecast201
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/warehouse-planning/labour-plans/"):
			return next(labourPlans, &lp)
		case r.Method == http.MethodPost && r.URL.Path == proposalPath:
			return next(proposals, &pr)
		case r.Method == http.MethodPost && r.URL.Path == shiftPlanPath:
			return http.StatusCreated, `{}`
		}
		return http.StatusNotFound, `{"type":"unrouted"}`
	}}
}

// plannerSim is a day of 10 ordered units staffed with 5 pickers + 1 flex (6 PICK heads), whose
// proposal poll is bounded to a fraction of a second.
func plannerSim(t *testing.T, f *fakeServices) *sim {
	t.Helper()
	s := simAgainst(t, f)
	s.cfg.pickers, s.cfg.flex, s.cfg.packers, s.cfg.slammers, s.cfg.rebinners = 5, 1, 3, 1, 1
	s.cfg.buildingID = "SIM1"
	s.clock = newSimClock(8 * time.Minute)
	s.orders = []*orderRec{{lines: []orderLinePlan{{sku: "MUG", qty: 6}}}, {lines: []orderLinePlan{{sku: "BOOK", qty: 4}}}}
	s.proposalWait, s.proposalPoll = 250*time.Millisecond, 5*time.Millisecond
	return s
}

// requestsTo lists the bodies of the requests sent to method + path.
func requestsTo(f *fakeServices, method, path string) []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []map[string]any
	for i := range f.path {
		if f.method[i] == method && f.path[i] == path {
			out = append(out, f.body[i])
		}
	}
	return out
}

// TestPlanTheDayPlansThroughWarehousePlanning: the forecast and the labour plan are PUT on
// warehouse-planning (heads left to be derived from the forecast, over a shift window that covers
// every forecast bucket), the proposal comes from the published labour plan (no charge), the shift
// plan is committed, and wes-work-planning's retired /charge and /plan are never called.
func TestPlanTheDayPlansThroughWarehousePlanning(t *testing.T) {
	f := plannerFake([]answer{labourPlan(http.StatusCreated, 1, "forecast")}, []answer{proposalOf(1, "labour-plan")})
	s := plannerSim(t, f)
	if err := s.planTheDay(t.Context()); err != nil {
		t.Fatal(err)
	}
	labourPlanPath := "/warehouse-planning/labour-plans/SIM1/" + s.shiftID() + "/PICK"
	want := []string{"PUT " + forecastPath, "PUT " + labourPlanPath, "POST " + proposalPath, "POST " + shiftPlanPath}
	var got []string
	for i := range f.path {
		got = append(got, f.method[i]+" "+f.path[i])
		if strings.HasPrefix(f.path[i], "/"+svcWES+"/") {
			t.Errorf("the planner called wes-work-planning: %s %s", f.method[i], f.path[i])
		}
	}
	if !slices.Equal(got, want) {
		t.Fatalf("requests\n %v\nwant\n %v", got, want)
	}

	start := s.shiftStart()
	buckets, _ := f.body[0]["buckets"].([]any)
	if len(buckets) != len(truckCutoffs) {
		t.Fatalf("forecast buckets %v, want one per truck", f.body[0])
	}
	total := 0.0
	for _, b := range buckets {
		bucket := b.(map[string]any)
		cpt, err := time.Parse(time.RFC3339, bucket["cpt"].(string))
		if err != nil {
			t.Fatalf("bucket cpt: %v", err)
		}
		if cpt.Before(start) || !cpt.Before(start.Add(plannedShiftHours*time.Hour)) {
			t.Errorf("bucket cpt %s is outside the labour plan's shift [%s, +8h): the heads could not be derived from it", cpt, start)
		}
		total += bucket["quantity"].(float64)
	}
	if total != 9 { // int(10 x 0.35) + int(10 x 0.40) + int(10 x 0.25)
		t.Errorf("forecast units %v, want 9 (the shares of the 10 ordered units)", total)
	}

	plan := f.body[1]
	if _, sent := plan["planned_heads"]; sent {
		t.Errorf("labour plan %v sends planned_heads: warehouse-planning must derive them from the forecast", plan)
	}
	if plan["rate_units_per_hour"] != 60.0 || plan["hours"] != 8.0 || plan["installed_stations"] != 6.0 || plan["shift_start"] != start.Format(time.RFC3339) {
		t.Errorf("labour plan %v, want rate 60, hours 8, 6 installed stations (the staffed PICK heads), shift_start %s", plan, start.Format(time.RFC3339))
	}

	proposal := f.body[2]
	if proposal["siteCode"] != "SIM1" || proposal["shiftId"] != s.shiftID() || len(proposal) != 2 {
		t.Errorf("proposal %v, want exactly siteCode SIM1 + shiftId %s (no deprecated charge)", proposal, s.shiftID())
	}
	if len(s.findings) != 0 || !s.shiftPlanned {
		t.Errorf("findings %v, shift planned %v: want none and a committed plan", s.findings, s.shiftPlanned)
	}
}

// TestPlanTheDayCommitsTheStaffedHeadsWhenTheDerivedOnesExceedTheStations: 422
// heads-exceed-stations on the derived plan is a finding and a second PUT with the staffed heads;
// the day goes on. Any other refusal of the labour plan aborts the planning, as before.
func TestPlanTheDayCommitsTheStaffedHeadsWhenTheDerivedOnesExceedTheStations(t *testing.T) {
	exceed := answer{http.StatusUnprocessableEntity, `{"type":"https://problems.warehouse-planning/heads-exceed-stations","status":422,"detail":"planned_heads 9 exceeds installed_stations 6"}`}
	f := plannerFake([]answer{exceed, labourPlan(http.StatusCreated, 6, "request")}, []answer{proposalOf(6, "labour-plan")})
	s := plannerSim(t, f)
	if err := s.planTheDay(t.Context()); err != nil {
		t.Fatalf("heads-exceed-stations must not abort the day: %v", err)
	}
	plans := requestsTo(f, http.MethodPut, "/warehouse-planning/labour-plans/SIM1/"+s.shiftID()+"/PICK")
	if len(plans) != 2 {
		t.Fatalf("labour plan PUTs %v, want the derived one and the explicit retry", plans)
	}
	if _, sent := plans[0]["planned_heads"]; sent || plans[1]["planned_heads"] != 6.0 {
		t.Fatalf("labour plans %v, want no planned_heads then planned_heads 6 (the staffed PICK heads)", plans)
	}
	if len(s.findings) != 1 || !strings.Contains(s.findings[0], "heads-exceed-stations") {
		t.Fatalf("findings %v, want exactly one naming heads-exceed-stations", s.findings)
	}
	if len(requestsTo(f, http.MethodPost, shiftPlanPath)) != 1 || !s.shiftPlanned {
		t.Fatal("the shift plan must still be committed")
	}

	missing := answer{http.StatusUnprocessableEntity, `{"type":"https://problems.warehouse-planning/missing-planned-heads","status":422}`}
	f = plannerFake([]answer{missing}, []answer{proposalOf(1, "labour-plan")})
	s = plannerSim(t, f)
	if err := s.planTheDay(t.Context()); err == nil || !strings.Contains(err.Error(), "missing-planned-heads") {
		t.Fatalf("an underivable labour plan must abort the planning, got %v", err)
	}
	if n := len(requestsTo(f, http.MethodPost, proposalPath)); n != 0 {
		t.Fatalf("%d proposals asked after a refused labour plan, want none", n)
	}
}

// TestProposalPollStopsOnTheLabourPlanAndGivesAFindingOnTimeout: workforce-management's copy of
// LabourPlanCommitted arrives over Kafka, so 404 is polled for; the poll stops on the first 200
// with the committed heads and is a finding (never an abort) when the copy never shows up.
func TestProposalPollStopsOnTheLabourPlanAndGivesAFindingOnTimeout(t *testing.T) {
	notFound := answer{http.StatusNotFound, notPublished}
	for _, tc := range []struct {
		name        string
		proposals   []answer
		asks        int // exact asks; 0 = several (the poll ran to its bound)
		findingWith string
	}{
		{name: "first ask", proposals: []answer{proposalOf(1, "labour-plan")}, asks: 1},
		{name: "stops on 200 after 404s", proposals: []answer{notFound, notFound, proposalOf(1, "labour-plan")}, asks: 3},
		{name: "a copy of an earlier commit is waited out", proposals: []answer{proposalOf(4, "labour-plan"), proposalOf(1, "labour-plan")}, asks: 2},
		{name: "never published", proposals: []answer{notFound}, findingWith: "within"},
		{name: "never the committed heads", proposals: []answer{proposalOf(4, "labour-plan")}, findingWith: "no proposal of 1 PICK heads"},
		{name: "not from the labour plan", proposals: []answer{proposalOf(1, "caller")}, asks: 1, findingWith: "want labour-plan"},
		{name: "refused", proposals: []answer{{http.StatusBadRequest, `{"type":"missing-charge"}`}}, asks: 1, findingWith: "-> 400"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := plannerFake([]answer{labourPlan(http.StatusOK, 1, "forecast")}, tc.proposals)
			s := plannerSim(t, f)
			if err := s.planTheDay(t.Context()); err != nil {
				t.Fatalf("the proposal must never abort the day: %v", err)
			}
			asks := len(requestsTo(f, http.MethodPost, proposalPath))
			if tc.asks > 0 && asks != tc.asks {
				t.Errorf("%d proposal asks, want %d", asks, tc.asks)
			}
			if tc.asks == 0 && asks < 2 {
				t.Errorf("%d proposal asks, want the poll to retry until its bound", asks)
			}
			switch {
			case tc.findingWith == "" && len(s.findings) != 0:
				t.Errorf("findings %v, want none", s.findings)
			case tc.findingWith != "" && (len(s.findings) != 1 || !strings.Contains(s.findings[0], tc.findingWith)):
				t.Errorf("findings %v, want one saying %q", s.findings, tc.findingWith)
			}
			if len(requestsTo(f, http.MethodPost, shiftPlanPath)) != 1 {
				t.Error("the shift plan must be committed whatever the proposal answered")
			}
		})
	}
}
