package inbound

// Contract fakes of the services the restock crew talks to (inbound-receiving, dock-scheduling,
// yard-management, inventory-storage). Each is a small state machine that enforces the rules its published
// contract states, with the real problem slugs, so a test fails when an actor breaks an ordering rule, sends no
// Idempotency-Key, reuses a key after a refusal (the services store a 4xx under its key) or retries what it
// must not. Consumer lag is a per-event REQUEST countdown in front of each event hop, never a sleep.
//
// Everything lives behind ONE lock and ONE manual clock that every request advances by a few milliseconds, so a
// time the service stamps is later than a time the actor already sent.

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/sim"
	"github.com/claudioed/warehouse-e2e/internal/sim/transport"
)

// ---------------------------------------------------------------------
// a tiny contract-fake framework
// ---------------------------------------------------------------------

type reply struct {
	status int
	body   any
	slug   string
}

func ok(v any) reply      { return reply{status: http.StatusOK, body: v} }
func created(v any) reply { return reply{status: http.StatusCreated, body: v} }
func refuse(status int, slug string) reply {
	return reply{status: status, slug: slug}
}

type fctx struct {
	r      *http.Request
	body   []byte
	params map[string]string
}

func (c *fctx) q(name string) string { return c.r.URL.Query().Get(name) }

// obj decodes the JSON body into a map; a body that is not an object is a 400.
func (c *fctx) obj() (map[string]any, *reply) {
	m := map[string]any{}
	if len(c.body) > 0 {
		if err := json.Unmarshal(c.body, &m); err != nil {
			r := refuse(http.StatusBadRequest, "malformed-request")
			return nil, &r
		}
	}
	return m, nil
}

type route struct {
	method   string
	template string
	parts    []string
	fn       func(c *fctx) reply
}

type stored struct {
	fingerprint string
	status      int
	body        []byte
}

type fake struct {
	w      *world
	name   string
	routes []*route
	idem   map[string]stored
	srv    *httptest.Server
}

func newFake(w *world, name string) *fake {
	return &fake{w: w, name: name, idem: map[string]stored{}}
}

func (f *fake) handle(method, template string, fn func(c *fctx) reply) {
	f.routes = append(f.routes, &route{method: method, template: template, parts: strings.Split(strings.Trim(template, "/"), "/"), fn: fn})
}

func (f *fake) match(r *http.Request) (*route, map[string]string) {
	segs := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	for _, rt := range f.routes {
		if rt.method != r.Method || len(rt.parts) != len(segs) {
			continue
		}
		params := map[string]string{}
		match := true
		for i, p := range rt.parts {
			if strings.HasPrefix(p, "{") {
				params[p[1:len(p)-1]] = segs[i]
			} else if p != segs[i] {
				match = false
				break
			}
		}
		if match {
			return rt, params
		}
	}
	return nil, nil
}

func (f *fake) problemBody(status int, slug string) []byte {
	b, _ := json.Marshal(map[string]any{
		"type": "https://errors." + f.name + ".warehouse-systems.dev/" + slug, "title": slug, "status": status,
		"detail": slug, "instance": "/",
	})
	return b
}

func (f *fake) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	f.w.mu.Lock()
	defer f.w.mu.Unlock()
	f.w.clock.Advance(3 * time.Millisecond)
	body, _ := io.ReadAll(r.Body)
	write := func(status int, b []byte, ct string) {
		rw.Header().Set("Content-Type", ct)
		rw.WriteHeader(status)
		_, _ = rw.Write(b)
	}
	rt, params := f.match(r)
	if rt == nil {
		write(http.StatusNotFound, f.problemBody(http.StatusNotFound, "not-found"), "application/problem+json")
		return
	}
	entry := f.name + " " + r.Method + " " + rt.template
	f.w.log = append(f.w.log, entry)
	f.w.sent[entry] = append(f.w.sent[entry], body)
	c := &fctx{r: r, body: body, params: params}
	var rep reply
	if r.Method == http.MethodPost {
		key := r.Header.Get("Idempotency-Key")
		f.w.posts = append(f.w.posts, post{svc: f.name, path: r.URL.Path, key: key})
		fp := r.URL.Path + "|" + string(body)
		switch st, seen := f.idem[key]; {
		case key == "":
			rep = refuse(http.StatusBadRequest, "idempotency-key-required")
		case seen && st.fingerprint != fp:
			rep = refuse(http.StatusConflict, "idempotency-key-reused")
		case seen:
			// A replay answers the stored response, a refusal included: the real services store a 4xx too.
			write(st.status, st.body, "application/json")
			return
		default:
			rep = rt.fn(c)
		}
	} else {
		rep = rt.fn(c)
	}
	var out []byte
	ct := "application/json"
	if rep.slug != "" {
		out, ct = f.problemBody(rep.status, rep.slug), "application/problem+json"
	} else if rep.body != nil {
		out, _ = json.Marshal(rep.body)
	}
	if k := r.Header.Get("Idempotency-Key"); r.Method == http.MethodPost && k != "" && rep.status < 500 && rep.slug != "idempotency-key-reused" {
		f.idem[k] = stored{fingerprint: r.URL.Path + "|" + string(body), status: rep.status, body: out}
	}
	write(rep.status, out, ct)
}

// page serves one keyset page of items; limit is 1..max (the fleet's collections answer 400 above it).
func page(c *fctx, max int, items []any, extra map[string]any) reply {
	limit := 100
	if s := c.q("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 || n > max {
			return refuse(http.StatusBadRequest, "invalid-limit")
		}
		limit = n
	}
	start := 0
	if s := c.q("cursor"); s != "" {
		start, _ = strconv.Atoi(s)
	}
	start = min(start, len(items))
	end := min(start+limit, len(items))
	out := map[string]any{"items": append([]any{}, items[start:end]...)}
	for k, v := range extra {
		out[k] = v
	}
	if end < len(items) {
		out["nextCursor"] = strconv.Itoa(end)
	}
	return ok(out)
}

// lag is a consumer-lag countdown: refuse() is true for the first n asks, then never again.
type lag struct{ n int }

func (l *lag) refuse() bool {
	if l == nil || l.n <= 0 {
		return false
	}
	l.n--
	return true
}

// ---------------------------------------------------------------------
// the world: four fakes behind one lock, wired like the event topics
// ---------------------------------------------------------------------

type post struct{ svc, path, key string }

type world struct {
	t     *testing.T
	mu    sync.Mutex
	clock *transport.ManualClock
	seq   int

	log   []string // "svc METHOD template", in the order served
	posts []post
	sent  map[string][][]byte // "svc METHOD template" -> request bodies, in order

	// Consumer lag knobs: how many requests a consumer refuses before it has applied the event.
	lagASNToDock         int // dock-scheduling learns an ASN (unknown-asn)
	lagDoorToDock        int // dock-scheduling learns a door (unknown-door)
	lagApptToYard        int // yard-management learns an appointment (unknown-appointment)
	lagApptToReceiving   int // inbound-receiving learns an appointment (unknown-appointment)
	lagUnloadReads       int // GET /trailers/{id} reads that still show the unload open after ReceiptClosed
	lagCompleteReads     int // GET /appointments/{id} reads that still show CHECKED_IN after the check-out
	lagStagedSeen        int // tap Seen calls that still miss StockReceived
	toleranceMinutes     int // yard: how early a carrier may arrive and still be sent to the door
	sealPolicyRejects    bool
	appointmentsRequired bool

	recv *fakeReceiving
	dock *fakeDock
	yard *fakeYard
	inv  *fakeInventory
	tap  *fakeTap

	client *sim.Client
}

func (w *world) id(prefix string) string {
	w.seq++
	return fmt.Sprintf("%s-%04d", prefix, w.seq)
}

// served is how many requests of the "svc METHOD template" form the fakes served.
func (w *world) served(entry string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for _, e := range w.log {
		if e == entry {
			n++
		}
	}
	return n
}

// first is the index in the log of the first request of entry, or -1.
func (w *world) first(entry string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	for i, e := range w.log {
		if e == entry {
			return i
		}
	}
	return -1
}

// last is the index in the log of the last request of entry, or -1.
func (w *world) last(entry string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	for i := len(w.log) - 1; i >= 0; i-- {
		if w.log[i] == entry {
			return i
		}
	}
	return -1
}

// with runs f under the world lock: the way a test changes a fake between requests.
func (w *world) with(f func()) {
	w.mu.Lock()
	defer w.mu.Unlock()
	f()
}

// bodies are the request bodies the fakes received for entry, in order.
func (w *world) bodies(entry string) [][]byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([][]byte(nil), w.sent[entry]...)
}

// writes is how many POST/PUT requests the fakes served.
func (w *world) writes() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for _, e := range w.log {
		parts := strings.SplitN(e, " ", 3)
		if parts[1] == http.MethodPost || parts[1] == http.MethodPut {
			n++
		}
	}
	return n
}

func newWorld(t *testing.T) *world {
	t.Helper()
	w := &world{t: t, sent: map[string][][]byte{}, clock: transport.NewManualClock(time.Date(2026, 11, 14, 15, 41, 7, 0, time.UTC)), toleranceMinutes: 30}
	w.recv = newFakeReceiving(w)
	w.dock = newFakeDock(w)
	w.yard = newFakeYard(w)
	w.inv = newFakeInventory(w)
	w.tap = &fakeTap{w: w, byRef: map[string]map[string]int{}}
	base := map[string]string{}
	for svc, f := range map[string]*fake{
		SvcInbound: w.recv.fake, SvcDock: w.dock.fake, SvcYard: w.yard.fake, SvcInventory: w.inv.fake,
	} {
		f.srv = httptest.NewServer(f)
		t.Cleanup(f.srv.Close)
		base[svc] = f.srv.URL
	}
	w.client = sim.NewClient(base, sim.NewCallStats())
	// A private pool: a repeated run churns through many small requests, and the default transport keeps only
	// two idle connections per host, so it would burn through the ephemeral ports.
	tr := &http.Transport{MaxIdleConnsPerHost: 8}
	t.Cleanup(tr.CloseIdleConnections)
	w.client.HTTP = &http.Client{Transport: tr, Timeout: 5 * time.Second}
	t.Cleanup(w.assertEveryPostKeyedOnce)
	return w
}

// assertEveryPostKeyedOnce fails when a POST arrived without an Idempotency-Key or when one key was sent twice
// to the same service (a retry must be a NEW logical request here: the refusals are stored).
func (w *world) assertEveryPostKeyedOnce() {
	w.mu.Lock()
	defer w.mu.Unlock()
	seen := map[string]string{}
	for _, p := range w.posts {
		if p.key == "" {
			w.t.Errorf("POST %s %s carried no Idempotency-Key", p.svc, p.path)
			continue
		}
		k := p.svc + "|" + p.key
		if prev, dup := seen[k]; dup {
			w.t.Errorf("Idempotency-Key %s was sent twice to %s (%s then %s)", p.key, p.svc, prev, p.path)
		}
		seen[k] = p.path
	}
}

const (
	siteCode  = "AUS3"
	inDoorA   = "AUS3-IB-DR-A01-01-01-A"
	inDoorB   = "AUS3-IB-DR-A01-01-02-A"
	outDoor   = "AUS3-OB-DR-A01-01-01-A"
	spotA     = "AUS3-TRL-PRK-A01-01-01-A"
	spotB     = "AUS3-TRL-PRK-A01-01-02-A"
	binA      = "AUS3-POD-BIN-A01-01-01-A"
	binB      = "AUS3-POD-BIN-A01-01-02-A"
	binHaz    = "AUS3-HAZ-BIN-A01-01-01-A"
	binChill  = "AUS3-CHL-BIN-A01-01-01-A"
	skuA      = "SKU-001"
	skuB      = "SKU-002"
	skuC      = "SKU-003"
	skuPrefix = "SKU-00" // SKU-001 is a prefix of SKU-0010: the tap must not confuse them
)

// seedSite registers what the AUS3 building has for an inbound leg: two inbound doors and an outbound one, two yard
// spots, three ambient bins, a hazmat bin and a chilled bin, three known SKUs each stocked with 100 units; and a
// calendar declared with a 5 minute grid.
func (w *world) seedSite() Site {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.recv.doors = []string{inDoorA, inDoorB}
	w.dock.doors = map[string]string{inDoorA: "Inbound", inDoorB: "Both", outDoor: "Outbound"}
	w.yard.doors = map[string]string{inDoorA: "Inbound", inDoorB: "Both", outDoor: "Outbound"}
	w.yard.spots = []string{spotA, spotB}
	w.dock.calendar[siteCode] = &calendar{version: 1, grid: 5, maxWindow: 240, tolerance: 30, noShow: 60}
	for _, b := range []struct {
		id, zone string
		capacity int
	}{{binA, "AMB", 500}, {binB, "AMB", 500}, {binHaz, "HAZ", 500}, {binChill, "CHL", 500}} {
		w.inv.bins[b.id] = &bin{id: b.id, zone: b.zone, capacity: b.capacity}
	}
	for _, sku := range []string{skuA, skuB, skuC} {
		w.recv.skus[sku] = true
		w.inv.stock(sku, binA, 100)
	}
	return Site{Code: siteCode, Doors: []string{inDoorA, inDoorB}, Bins: []string{binA, binB}}
}

func (w *world) crew(site Site) *Crew {
	return &Crew{
		C: w.client, Site: site, RunID: "T1", Carrier: Carrier{Name: "ACME Freight", SCAC: "ACMF"},
		Clock: w.clock, Wait: Waits{Timeout: 30 * time.Second, Interval: 500 * time.Millisecond},
	}
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// ---------------------------------------------------------------------
// the Kafka tap: events by correlation id, as sim.EventTap indexes them
// ---------------------------------------------------------------------

type fakeTap struct {
	w     *world
	refs  []string
	byRef map[string]map[string]int
}

func (t *fakeTap) Track(ref string) {
	t.w.mu.Lock()
	defer t.w.mu.Unlock()
	t.refs = append(t.refs, ref)
}

// publish indexes one message under every tracked ref its payload contains (a substring match, like sim.EventTap).
func (t *fakeTap) publish(typ, payload string) {
	for _, ref := range t.refs {
		if strings.Contains(payload, ref) {
			if t.byRef[ref] == nil {
				t.byRef[ref] = map[string]int{}
			}
			t.byRef[ref][typ]++
		}
	}
}

// Seen counts the messages of the short type that carried ref; while lagStagedSeen asks are left it misses
// StockReceived (the analytics consumer has not produced it yet).
func (t *fakeTap) Seen(ref, typ string) int {
	t.w.mu.Lock()
	defer t.w.mu.Unlock()
	if typ == "StockReceived" && t.w.lagStagedSeen > 0 {
		t.w.lagStagedSeen--
		return 0
	}
	return t.byRef[ref][typ]
}
