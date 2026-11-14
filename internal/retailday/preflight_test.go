package retailday

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/sim"
)

// fakeCluster answers the reads of the preflight the way the deployed contexts do
// (shapes from each repo's apis/openapi.yaml) and lets a test break one thing.
type fakeCluster struct {
	down         map[string]bool // contexts whose healthz answers 503
	mode         string
	nodeStatus   string
	handling     int
	netSchedule  string // body of GET /sites/site-1/cpt-schedule ("" = 404)
	bldSchedule  string
	noBuilding   bool
	productCount int
	writes       int
}

const sched1100and1700 = `{"siteId":"%s","timezone":"America/Chicago","cutoffs":[
 {"cptId":"aus-1100","localTime":"11:00","daysOfWeek":["Mon","Tue","Wed","Thu","Fri","Sat","Sun"],"shipMethod":"ground","eligiblePathIds":["PICK"]},
 {"cptId":"aus-1700","localTime":"17:00","daysOfWeek":["Sun","Mon","Tue","Wed","Thu","Fri","Sat"],"shipMethod":"ground","eligiblePathIds":["PICK"]}]}`

func newFake() *fakeCluster {
	return &fakeCluster{
		down: map[string]bool{}, mode: "live", nodeStatus: "ACTIVE", handling: 60, productCount: 12,
		netSchedule: strings.Replace(sched1100and1700, "%s", "site-1", 1),
		bldSchedule: strings.Replace(sched1100and1700, "%s", "AUS3", 1),
	}
}

func (f *fakeCluster) handler(t *testing.T) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			f.writes++
			t.Errorf("preflight must be read-only, got %s %s", r.Method, r.URL)
		}
		path := r.URL.Path
		parts := strings.SplitN(strings.TrimPrefix(path, "/"), "/", 2)
		svc, rest := parts[0], "/"
		if len(parts) == 2 {
			rest += parts[1]
		}
		switch {
		case svc == "storefront-bff" && rest == "/products":
			items := make([]map[string]any, f.productCount)
			for i := range items {
				items[i] = map[string]any{"id": i, "purchasable": true}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"items": items})
		case rest == "/healthz":
			if f.down[svc] {
				http.Error(w, "down", http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case svc == "network-fulfillment" && rest == "/inbound-status":
			_, _ = w.Write([]byte(`{"networkMode":"` + f.mode + `","polls":3,"received":0,"failed":0,"overdue":0}`))
		case svc == "vendor-connectivity" && rest == "/vendor/nodes":
			if r.URL.Query().Get("vendorId") != "V-WS" {
				http.Error(w, "vendor", http.StatusNotFound)
				return
			}
			_, _ = w.Write([]byte(`{"items":[{"nodeId":"node-1","vendorNodeCode":"site-1","status":"` + f.nodeStatus + `","timezone":"America/Chicago","handlingTimeMinutes":` + itoa(f.handling) + `,"profileRevision":1}]}`))
		case svc == "process-path-management" && rest == "/sites/site-1/cpt-schedule":
			f.serve(w, f.netSchedule)
		case svc == "process-path-management" && rest == "/sites/AUS3/cpt-schedule":
			f.serve(w, f.bldSchedule)
		case svc == "facility-layout" && rest == "/sites/AUS3":
			if f.noBuilding {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write([]byte(`{"siteCode":"AUS3"}`))
		default:
			http.NotFound(w, r)
		}
	})
	return mux
}

func (f *fakeCluster) serve(w http.ResponseWriter, body string) {
	if body == "" {
		http.NotFound(w, nil)
		return
	}
	_, _ = w.Write([]byte(body))
}

func itoa(n int) string { b, _ := json.Marshal(n); return string(b) }

func run(t *testing.T, f *fakeCluster, now time.Time, require bool) []Check {
	t.Helper()
	srv := httptest.NewServer(f.handler(t))
	t.Cleanup(srv.Close)
	cl := sim.NewClient(sim.GatewayBase(srv.URL, AllGateway()), sim.NewCallStats())
	cl.Base[BFF] = srv.URL + "/" + BFF
	return Preflight(context.Background(), cl, Config{
		NetworkSite: "site-1", BuildingSite: "AUS3", RequireWindow: require,
		Now: func() time.Time { return now },
	})
}

func find(t *testing.T, cs []Check, prefix string) Check {
	t.Helper()
	for _, c := range cs {
		if strings.HasPrefix(c.Name, prefix) {
			return c
		}
	}
	t.Fatalf("no check starting %q in %d checks", prefix, len(cs))
	return Check{}
}

func at(t *testing.T, hh, mm int) time.Time {
	t.Helper()
	return time.Date(2026, 11, 14, hh, mm, 0, 0, chicago(t))
}

func TestPreflightPassesOnAHealthyClusterInsideTheWindowAndNeverWrites(t *testing.T) {
	f := newFake()
	cs := run(t, f, at(t, 15, 40), true)
	if n := Failed(cs); n != 0 {
		for _, c := range cs {
			if !c.OK {
				t.Errorf("FAIL %s: %s (fix: %s)", c.Name, c.Detail, c.Fix)
			}
		}
		t.Fatalf("%d checks failed on a healthy cluster", n)
	}
	if want := len(AllGateway()) + 8; len(cs) != want {
		t.Errorf("%d checks, want %d (one health per context + live + node + 2 schedules + agree + building + window + shelf)", len(cs), want)
	}
	if f.writes != 0 {
		t.Fatalf("%d writes", f.writes)
	}
	if w := find(t, cs, "release window"); !strings.Contains(w.Detail, "in the release window 15:30-16:00 CST, 20m0s left") || !strings.Contains(w.Detail, "handling 1h0m0s from the node profile") {
		t.Errorf("window detail: %s", w.Detail)
	}
}

func TestPreflightNamesTheContextThatIsDownAndHowToLook(t *testing.T) {
	f := newFake()
	f.down["order-management"] = true
	cs := run(t, f, at(t, 15, 40), false)
	c := find(t, cs, "health order-management")
	if c.OK || !strings.Contains(c.Detail, "503") || !strings.Contains(c.Fix, "kafka-controller-0") {
		t.Fatalf("got %+v", c)
	}
	if n := Failed(cs); n != 1 {
		t.Fatalf("%d failures, want exactly the one down context", n)
	}
}

func TestPreflightRefusesAStubNetworkAnInactiveNodeAndMissingSchedules(t *testing.T) {
	f := newFake()
	f.mode, f.nodeStatus, f.netSchedule, f.noBuilding = "stub", "PROPOSED", "", true
	cs := run(t, f, at(t, 15, 40), false)
	for _, prefix := range []string{"network-fulfillment is live", "fulfilment node site-1", "CPT schedule of the network site", "CPT schedules of site-1 and AUS3 agree", "facility-layout knows the building"} {
		if c := find(t, cs, prefix); c.OK || c.Fix == "" {
			t.Errorf("%q should fail with a fix: %+v", prefix, c)
		}
	}
	if c := find(t, cs, "CPT schedule of the building site"); !c.OK {
		t.Errorf("the building's schedule is fine and must pass: %+v", c)
	}
}

func TestPreflightCatchesTwoSchedulesThatDrifted(t *testing.T) {
	f := newFake()
	f.bldSchedule = strings.Replace(f.bldSchedule, `"17:00"`, `"17:30"`, 1)
	c := find(t, run(t, f, at(t, 15, 40), false), "CPT schedules of site-1 and AUS3 agree")
	if c.OK || !strings.Contains(c.Detail, "17:30") || !strings.Contains(c.Fix, "ADR 0001 D1") {
		t.Fatalf("got %+v", c)
	}
}

func TestScheduleComparisonIgnoresOrder(t *testing.T) {
	f := newFake()
	// same cutoffs, listed the other way round and with days shuffled
	f.bldSchedule = `{"siteId":"AUS3","timezone":"America/Chicago","cutoffs":[
 {"cptId":"aus-1700","localTime":"17:00","daysOfWeek":["Sat","Fri","Thu","Wed","Tue","Mon","Sun"],"shipMethod":"ground","eligiblePathIds":["PICK"]},
 {"cptId":"aus-1100","localTime":"11:00","daysOfWeek":["Sun","Sat","Fri","Thu","Wed","Tue","Mon"],"shipMethod":"ground","eligiblePathIds":["PICK"]}]}`
	if c := find(t, run(t, f, at(t, 15, 40), false), "CPT schedules of site-1 and AUS3 agree"); !c.OK {
		t.Fatalf("%+v", c)
	}
}

func TestWindowIsReportedAndOnlyFailsWhenRequired(t *testing.T) {
	f := newFake()
	noon := at(t, 12, 0)
	soft := find(t, run(t, f, noon, false), "release window")
	if !soft.OK || !strings.Contains(soft.Detail, "next release window 15:30-16:00 CST on Sat 14 Nov, in 3h30m0s") {
		t.Fatalf("not required: %+v", soft)
	}
	hard := find(t, run(t, f, noon, true), "release window")
	if hard.OK || !strings.Contains(hard.Fix, "-wait-for-window") {
		t.Fatalf("required outside the window must fail with the way out: %+v", hard)
	}
}

func TestShelfCheckCountsPurchasableProducts(t *testing.T) {
	f := newFake()
	f.productCount = 3
	c := find(t, run(t, f, at(t, 15, 40), false), "the storefront lists at least 10")
	if c.OK || !strings.Contains(c.Detail, "3 purchasable") {
		t.Fatalf("got %+v", c)
	}
}

func TestNextWindowReadsTheNodeAndTheBuildingSchedule(t *testing.T) {
	f := newFake()
	srv := httptest.NewServer(f.handler(t))
	t.Cleanup(srv.Close)
	cl := sim.NewClient(sim.GatewayBase(srv.URL, AllGateway()), sim.NewCallStats())
	w, err := NextWindow(context.Background(), cl, Config{NetworkSite: "site-1", BuildingSite: "AUS3", Now: func() time.Time { return at(t, 9, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	if w.InWindow || w.Wait != 6*time.Hour+30*time.Minute || w.Start.Hour() != 15 || w.Start.Minute() != 30 {
		t.Fatalf("got %+v", w)
	}
	f.handling = 90 // a slower node moves the window earlier: 17:00 - 90m - 30m = 15:00
	w, err = NextWindow(context.Background(), cl, Config{NetworkSite: "site-1", BuildingSite: "AUS3", Now: func() time.Time { return at(t, 9, 0) }})
	if err != nil || w.Start.Hour() != 15 || w.Start.Minute() != 0 {
		t.Fatalf("handling 90: got %+v err %v", w, err)
	}
	f.bldSchedule = ""
	if _, err := NextWindow(context.Background(), cl, Config{NetworkSite: "site-1", BuildingSite: "AUS3", Now: func() time.Time { return at(t, 9, 0) }}); err == nil {
		t.Fatal("an unreadable schedule must be an error, not a guessed window")
	}
}
