package link

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
	"github.com/claudioed/warehouse-e2e/internal/sim"
	"github.com/claudioed/warehouse-e2e/internal/sim/transport"
)

// ---- a fake of the two services, shaped from their published contracts ----

type step struct {
	status int    // 0 = 200 with the order below
	state  string // NetworkOrder.state
	local  string // NetworkOrder.localOrderId ("" = absent)
	slug   string // problem slug for an error status
}

type fake struct {
	mu sync.Mutex
	// assignments by storefront order id; a script per order: each read of the assignments takes the next
	assignments map[string][][]assignmentJSON
	reads       map[string]int
	// network-fulfillment: a script per poNumber, the last step repeats
	nf     map[string][]step
	nfRead map[string]int
	// failure modes of the fake
	sourcingStatus int  // answers every assignments read with this status and no problem
	nfGateway404   bool // network-fulfillment answers 404 the way Kong does for an unknown route
	// every request seen
	methods []string
	paths   []string
}

type assignmentJSON struct {
	ID       string        `json:"assignmentId"`
	OrderID  string        `json:"orderId"`
	GroupNo  int           `json:"groupNo"`
	State    string        `json:"state"`
	Releases []releaseJSON `json:"releases,omitempty"`
}
type releaseJSON struct {
	ReleaseNo int    `json:"releaseNo"`
	Outcome   string `json:"outcome"`
}

func newFake() *fake {
	return &fake{assignments: map[string][][]assignmentJSON{}, reads: map[string]int{}, nf: map[string][]step{}, nfRead: map[string]int{}}
}

func (f *fake) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/order-sourcing/assignments", func(w http.ResponseWriter, r *http.Request) {
		f.record(r)
		if f.sourcingStatus != 0 {
			w.WriteHeader(f.sourcingStatus)
			_, _ = w.Write([]byte(`{"message":"no Route matched with those values"}`))
			return
		}
		if q := r.URL.Query().Get("limit"); q != "200" {
			problem(w, 400, "invalid-parameter")
			return
		}
		id := r.URL.Query().Get("orderId")
		f.mu.Lock()
		script := f.assignments[id]
		n := f.reads[id]
		f.reads[id]++
		f.mu.Unlock()
		var items []assignmentJSON
		if len(script) > 0 {
			items = script[min(n, len(script)-1)]
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"items": items})
	})
	mux.HandleFunc("/network-fulfillment/purchase-orders/", func(w http.ResponseWriter, r *http.Request) {
		f.record(r)
		po := strings.TrimPrefix(r.URL.Path, "/network-fulfillment/purchase-orders/")
		f.mu.Lock()
		script, ok := f.nf[po]
		n := f.nfRead[po]
		f.nfRead[po]++
		f.mu.Unlock()
		if f.nfGateway404 {
			w.WriteHeader(404)
			_, _ = w.Write([]byte(`{"message":"no Route matched with those values"}`))
			return
		}
		if !ok {
			problem(w, 404, "purchase-order-not-found")
			return
		}
		s := script[min(n, len(script)-1)]
		if s.status != 0 {
			problem(w, s.status, s.slug)
			return
		}
		out := map[string]any{"networkRef": "NR-" + po, "siteId": "site-1", "state": s.state}
		if s.local != "" {
			out["localOrderId"] = s.local
		}
		_ = json.NewEncoder(w).Encode(out)
	})
	return mux
}

func (f *fake) record(r *http.Request) {
	f.mu.Lock()
	f.methods = append(f.methods, r.Method)
	f.paths = append(f.paths, r.URL.Path)
	f.mu.Unlock()
}

func problem(w http.ResponseWriter, status int, slug string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"type": "https://example.test/problems/" + slug, "status": status})
}

func (f *fake) nfReads(po string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.nfRead[po]
}

type rig struct {
	f     *fake
	clock *transport.ManualClock
	l     *Linker
	start time.Time
}

func newRig(t *testing.T, s Settings) *rig {
	t.Helper()
	f := newFake()
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)
	start := time.Date(2026, 11, 14, 15, 0, 0, 0, time.UTC)
	clock := transport.NewManualClock(start)
	s.Clock = clock
	c := sim.NewClient(map[string]string{SvcSourcing: srv.URL + "/order-sourcing", SvcNetwork: srv.URL + "/network-fulfillment"}, sim.NewCallStats())
	return &rig{f: f, clock: clock, l: New(c, s), start: start}
}

func placed(id string) retailday.Placed {
	return retailday.Placed{RunID: "RD1", Expect: retailday.Delivered, OrderID: id}
}

func asg(id string, group int, state string, rel ...releaseJSON) assignmentJSON {
	return assignmentJSON{ID: id, OrderID: "ord-1", GroupNo: group, State: state, Releases: rel}
}

func accepted(n int) releaseJSON { return releaseJSON{ReleaseNo: n, Outcome: "ACCEPTED"} }

// ---- the derivation ----

// The vectors were computed independently with sha256 (python hashlib), so a change of the formula
// in either direction fails here and not at the first live run.
func TestPurchaseOrderNumberIsVendorConnectivitysDerivation(t *testing.T) {
	for _, c := range []struct {
		asg  string
		rel  int
		want string
	}{
		{"asg-1", 1, "PO-415a91a42a8b75f0"},
		{"asg-1", 2, "PO-1744e20df4b40fbd"},
		{"0f9c1b52-7d3e-4a8e-9c61-2b5f4d0a77aa", 1, "PO-c275c4544c9c759c"},
	} {
		if got := PurchaseOrderNumber(c.asg, c.rel); got != c.want {
			t.Errorf("PurchaseOrderNumber(%q, %d) = %s, want %s", c.asg, c.rel, got, c.want)
		}
	}
}

// ---- OrderIDs ----

func TestOrderIDsFollowsTheChainToTheOrderManagementOrder(t *testing.T) {
	g := newRig(t, Settings{})
	po := PurchaseOrderNumber("asg-1", 1)
	g.f.assignments["ord-1"] = [][]assignmentJSON{{asg("asg-1", 1, "RELEASED", accepted(1))}}
	g.f.nf[po] = []step{{state: "ACKNOWLEDGED", local: "ord-om-7"}}
	ids, err := g.l.OrderIDs(context.Background(), placed("ord-1"))
	if err != nil || len(ids) != 1 || ids[0] != "ord-om-7" {
		t.Fatalf("ids %v err %v", ids, err)
	}
	if !g.clock.Now().Equal(g.start) {
		t.Errorf("an order that is already acknowledged must not wait: the clock moved %s", g.clock.Now().Sub(g.start))
	}
}

// The purchase order is released late, received by network-fulfillment on its next poll, answered,
// then reconciled: each of these is a wait, not a failure.
func TestOrderIDsWaitsForEveryStageOfTheNetworksLoop(t *testing.T) {
	g := newRig(t, Settings{Interval: 15 * time.Second, Timeout: time.Hour})
	po := PurchaseOrderNumber("asg-1", 1)
	g.f.assignments["ord-1"] = [][]assignmentJSON{
		{},                          // not consumed by order-sourcing yet
		{asg("asg-1", 1, "QUEUED")}, // queued, release not due
		{asg("asg-1", 1, "QUEUED")}, //
		{asg("asg-1", 1, "RELEASED", accepted(1))}, // released
	}
	g.f.nf[po] = []step{
		{status: 404, slug: "purchase-order-not-found"}, // not polled yet
		{status: 503, slug: "unavailable"},              // a blip is not a refusal
		{state: "NEW"},
		{state: "SUBMITTED"},
		{state: "ACKNOWLEDGED", local: "ord-om-9"},
	}
	ids, err := g.l.OrderIDs(context.Background(), placed("ord-1"))
	if err != nil || len(ids) != 1 || ids[0] != "ord-om-9" {
		t.Fatalf("ids %v err %v", ids, err)
	}
	waited := g.clock.Now().Sub(g.start)
	if waited < 5*15*time.Second || waited > 9*15*time.Second {
		t.Errorf("waited %s: the loop has 3 sourcing waits then 4 network waits at 15s", waited)
	}
	if n := g.f.nfReads(po); n != 5 {
		t.Errorf("network-fulfillment read %d times, want 5 (once per scripted step, no more)", n)
	}
}

func TestOrderIDsTimesOutAndSaysWhatItLastSaw(t *testing.T) {
	g := newRig(t, Settings{Interval: 20 * time.Second, Timeout: 5 * time.Minute})
	po := PurchaseOrderNumber("asg-1", 1)
	g.f.assignments["ord-1"] = [][]assignmentJSON{{asg("asg-1", 1, "RELEASED", accepted(1))}}
	g.f.nf[po] = []step{{state: "SUBMITTED"}}
	_, err := g.l.OrderIDs(context.Background(), placed("ord-1"))
	if err == nil {
		t.Fatal("an order that is never acknowledged must time out")
	}
	for _, want := range []string{"ord-1", "5m0s", po, "SUBMITTED", "not acknowledged"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q lacks %q", err, want)
		}
	}
	if waited := g.clock.Now().Sub(g.start); waited < 5*time.Minute || waited > 5*time.Minute+20*time.Second {
		t.Errorf("waited %s, want the 5m bound", waited)
	}
}

func TestOrderIDsNeverWaitsOnARefusalThatCannotChange(t *testing.T) {
	for name, c := range map[string]struct {
		asg  []assignmentJSON
		nf   []step
		want string
	}{
		"the node rejected the release":      {[]assignmentJSON{asg("asg-1", 1, "RELEASED", releaseJSON{ReleaseNo: 1, Outcome: "REJECTED"})}, nil, "outcome REJECTED"},
		"the release expired":                {[]assignmentJSON{asg("asg-1", 1, "RELEASED", releaseJSON{ReleaseNo: 1, Outcome: "EXPIRED"})}, nil, "outcome EXPIRED"},
		"the release was withdrawn":          {[]assignmentJSON{asg("asg-1", 1, "RELEASED", releaseJSON{ReleaseNo: 1, Outcome: "WITHDRAWN"})}, nil, "outcome WITHDRAWN"},
		"the order is unsourceable":          {[]assignmentJSON{asg("asg-1", 1, "UNSOURCEABLE")}, nil, "UNSOURCEABLE"},
		"the assignment was cancelled":       {[]assignmentJSON{asg("asg-1", 1, "CANCELLED")}, nil, "CANCELLED"},
		"network-fulfillment rejected":       {[]assignmentJSON{asg("asg-1", 1, "RELEASED", accepted(1))}, []step{{state: "REJECTED"}}, "rejected the purchase order"},
		"network-fulfillment refuses a read": {[]assignmentJSON{asg("asg-1", 1, "RELEASED", accepted(1))}, []step{{status: 400, slug: "invalid-parameter"}}, "HTTP 400"},
	} {
		t.Run(name, func(t *testing.T) {
			g := newRig(t, Settings{Timeout: time.Hour})
			g.f.assignments["ord-1"] = [][]assignmentJSON{c.asg}
			if c.nf != nil {
				g.f.nf[PurchaseOrderNumber("asg-1", 1)] = c.nf
			}
			_, err := g.l.OrderIDs(context.Background(), placed("ord-1"))
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error %v, want it to contain %q", err, c.want)
			}
			if waited := g.clock.Now().Sub(g.start); waited != 0 {
				t.Errorf("waited %s for something waiting cannot change", waited)
			}
		})
	}
}

// One order can be split in groups, and a group can be released twice: every purchase order has its own
// order-management order, and the caller must see all of them (and a repeat only once).
func TestOrderIDsReturnsEveryOrderOfEveryReleaseSortedAndUnique(t *testing.T) {
	g := newRig(t, Settings{})
	g.f.assignments["ord-1"] = [][]assignmentJSON{{
		asg("asg-2", 2, "RELEASED", accepted(1)),
		asg("asg-1", 1, "RELEASED", accepted(1), accepted(2)),
	}}
	g.f.nf[PurchaseOrderNumber("asg-1", 1)] = []step{{state: "ACKNOWLEDGED", local: "om-b"}}
	g.f.nf[PurchaseOrderNumber("asg-1", 2)] = []step{{state: "ACKNOWLEDGED", local: "om-a"}}
	g.f.nf[PurchaseOrderNumber("asg-2", 1)] = []step{{state: "CONFIRMED", local: "om-b"}}
	ids, err := g.l.OrderIDs(context.Background(), placed("ord-1"))
	if err != nil || strings.Join(ids, ",") != "om-a,om-b" {
		t.Fatalf("ids %v err %v, want [om-a om-b]", ids, err)
	}
}

// ...but it does not answer before the LAST release has an order: a partial answer would hide a duplicate.
func TestOrderIDsWaitsForTheSlowestRelease(t *testing.T) {
	g := newRig(t, Settings{Interval: 10 * time.Second})
	g.f.assignments["ord-1"] = [][]assignmentJSON{{asg("asg-1", 1, "RELEASED", accepted(1), accepted(2))}}
	g.f.nf[PurchaseOrderNumber("asg-1", 1)] = []step{{state: "ACKNOWLEDGED", local: "om-1"}}
	g.f.nf[PurchaseOrderNumber("asg-1", 2)] = []step{{state: "NEW"}, {state: "NEW"}, {state: "ACKNOWLEDGED", local: "om-2"}}
	ids, err := g.l.OrderIDs(context.Background(), placed("ord-1"))
	if err != nil || strings.Join(ids, ",") != "om-1,om-2" {
		t.Fatalf("ids %v err %v", ids, err)
	}
}

func TestOrderIDsOfAnOrderWithoutAnIdIsAnErrorAndReadsNothing(t *testing.T) {
	g := newRig(t, Settings{})
	if _, err := g.l.OrderIDs(context.Background(), retailday.Placed{RunID: "RD1", Expect: retailday.Delivered}); err == nil {
		t.Fatal("want an error")
	}
	if len(g.f.paths) != 0 {
		t.Errorf("requests %v", g.f.paths)
	}
}

func TestOrderIDsEndsWithItsContext(t *testing.T) {
	g := newRig(t, Settings{Timeout: 24 * time.Hour})
	g.f.assignments["ord-1"] = [][]assignmentJSON{{}} // never consumed
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() { _, err := g.l.OrderIDs(ctx, placed("ord-1")); done <- err }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a cancelled wait must be an error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the wait did not end with its context")
	}
}

// ---- ForPurchaseOrders ----

func TestForPurchaseOrdersReadsOnceAndTreatsAMissingOrderAsNone(t *testing.T) {
	g := newRig(t, Settings{})
	g.f.nf["PO-acked"] = []step{{state: "ACKNOWLEDGED", local: "om-1"}}
	g.f.nf["PO-new"] = []step{{state: "NEW"}} // received, not acknowledged: no order exists yet
	// "PO-unknown" was never received (404)
	ids, err := g.l.ForPurchaseOrders(context.Background(), placed("ord-1"), []string{"PO-new", "PO-unknown", "PO-acked"})
	if err != nil || len(ids) != 1 || ids[0] != "om-1" {
		t.Fatalf("ids %v err %v", ids, err)
	}
	if g.clock.Now() != g.start {
		t.Error("a single read must not sleep")
	}
	for _, po := range []string{"PO-acked", "PO-new", "PO-unknown"} {
		if n := g.f.nfRead[po]; n != 1 {
			t.Errorf("%s read %d times, want once", po, n)
		}
	}
}

func TestForPurchaseOrdersReportsADuplicateOnceAndAFailureAsAnError(t *testing.T) {
	g := newRig(t, Settings{})
	g.f.nf["PO-1"] = []step{{state: "ACKNOWLEDGED", local: "om-2"}}
	g.f.nf["PO-2"] = []step{{state: "CONFIRMED", local: "om-1"}}
	g.f.nf["PO-3"] = []step{{state: "ACKNOWLEDGED", local: "om-2"}}
	g.f.nf["PO-4"] = []step{{status: 500, slug: "internal"}}
	ids, err := g.l.ForPurchaseOrders(context.Background(), placed("ord-1"), []string{"PO-1", "PO-2", "PO-3", "PO-4"})
	if strings.Join(ids, ",") != "om-1,om-2" {
		t.Errorf("ids %v: the orders that could be read are returned, sorted, without repeats", ids)
	}
	if err == nil || !strings.Contains(err.Error(), "PO-4") || !strings.Contains(err.Error(), "HTTP 500") {
		t.Errorf("error %v: a server error must name the purchase order and the status", err)
	}
}

// ---- it only reads ----

func TestTheLinkerOnlyEverIssuesGETs(t *testing.T) {
	g := newRig(t, Settings{})
	g.f.assignments["ord-1"] = [][]assignmentJSON{{asg("asg-1", 1, "RELEASED", accepted(1))}}
	g.f.nf[PurchaseOrderNumber("asg-1", 1)] = []step{{state: "ACKNOWLEDGED", local: "om-1"}}
	if _, err := g.l.OrderIDs(context.Background(), placed("ord-1")); err != nil {
		t.Fatal(err)
	}
	if _, err := g.l.ForPurchaseOrders(context.Background(), placed("ord-1"), []string{"PO-x"}); err != nil {
		t.Fatal(err)
	}
	if len(g.f.methods) == 0 {
		t.Fatal("nothing was requested")
	}
	for i, m := range g.f.methods {
		if m != http.MethodGet {
			t.Errorf("request %d: %s %s", i, m, g.f.paths[i])
		}
	}
}

func TestADefaultLinkerWaitsLongEnoughForAWindowedDay(t *testing.T) {
	s := Settings{}.withDefaults()
	if s.Timeout < time.Hour || s.Interval <= 0 || s.Interval > time.Minute || s.PageSize != 200 {
		t.Errorf("defaults %+v: the PO is released up to ~90 minutes before cutoff (ADR 0001 D3), the sweep runs every 15s and outbound-shipping caps limit at 200", s)
	}
	if fmt.Sprintf("%T", s.Clock) != "transport.SystemClock" {
		t.Errorf("default clock %T", s.Clock)
	}
}

// A 404 can mean two things. From the service it says "never received", which is a wait. From the
// gateway it says there is no such route, which is a release not yet deployed: waiting two hours per
// order to learn that would hide it.
func TestAMissingRouteFailsAtOnceInsteadOfPassingForNotReceivedYet(t *testing.T) {
	t.Run("network-fulfillment route not deployed", func(t *testing.T) {
		g := newRig(t, Settings{Timeout: 2 * time.Hour})
		g.f.assignments["ord-1"] = [][]assignmentJSON{{asg("asg-1", 1, "RELEASED", accepted(1))}}
		g.f.nfGateway404 = true
		_, err := g.l.OrderIDs(context.Background(), placed("ord-1"))
		if err == nil || !strings.Contains(err.Error(), "deployed") || !strings.Contains(err.Error(), PurchaseOrderNumber("asg-1", 1)) {
			t.Fatalf("error %v: want the missing route named, with the purchase order", err)
		}
		if waited := g.clock.Now().Sub(g.start); waited != 0 {
			t.Errorf("waited %s for a route that is not there", waited)
		}
	})
	t.Run("order-sourcing route unknown", func(t *testing.T) {
		g := newRig(t, Settings{Timeout: 2 * time.Hour})
		g.f.sourcingStatus = 404
		_, err := g.l.OrderIDs(context.Background(), placed("ord-1"))
		if err == nil || !strings.Contains(err.Error(), "order-sourcing GET /assignments") || !strings.Contains(err.Error(), "404") {
			t.Fatalf("error %v", err)
		}
		if waited := g.clock.Now().Sub(g.start); waited != 0 {
			t.Errorf("waited %s", waited)
		}
	})
	t.Run("a service 404 is still a wait", func(t *testing.T) {
		g := newRig(t, Settings{Interval: 10 * time.Second})
		po := PurchaseOrderNumber("asg-1", 1)
		g.f.assignments["ord-1"] = [][]assignmentJSON{{asg("asg-1", 1, "RELEASED", accepted(1))}}
		g.f.nf[po] = []step{{status: 404, slug: "purchase-order-not-found"}, {state: "ACKNOWLEDGED", local: "om-1"}}
		ids, err := g.l.OrderIDs(context.Background(), placed("ord-1"))
		if err != nil || len(ids) != 1 {
			t.Fatalf("ids %v err %v", ids, err)
		}
	})
	t.Run("a rate limit is a wait, not a refusal", func(t *testing.T) {
		g := newRig(t, Settings{Interval: 10 * time.Second})
		po := PurchaseOrderNumber("asg-1", 1)
		g.f.assignments["ord-1"] = [][]assignmentJSON{{asg("asg-1", 1, "RELEASED", accepted(1))}}
		g.f.nf[po] = []step{{status: 429, slug: "rate-limited"}, {state: "ACKNOWLEDGED", local: "om-1"}}
		if ids, err := g.l.OrderIDs(context.Background(), placed("ord-1")); err != nil || len(ids) != 1 {
			t.Fatalf("ids %v err %v", ids, err)
		}
	})
}
