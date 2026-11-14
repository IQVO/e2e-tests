package floor

// The contract fakes: ONE httptest server that answers, under /<service>/..., the routes of
// order-management, wes-work-planning, fulfillment-execution and inventory-storage that the floor
// actors use, with the rules the published contracts (apis/openapi.yaml of each repo, read from
// origin/develop) and their ADRs state. The fakes ENFORCE them: a call that breaks a rule is
// answered with the real problem slug, so a test fails when an actor breaks the contract, not only
// when it breaks the fake's author's idea of it.
//
// fake_floor_test.go holds the per-service state machines behind this file's one lock. This file
// holds the shared server: routing under the service prefix, the Idempotency-Key rule (a POST
// without a key is 400, the same key with another request is 409, the same request replays and
// is COUNTED as a reuse, which fails the test: the actors never re-send a key), strict body
// decoding, keyset paging, the call log and the consumer-lag clock.
//
// Consumer lag is a per-event REQUEST countdown, never a sleep: an event another service must
// consume becomes visible once N more requests have reached the server, so a test is the same
// whatever the machine's scheduling does.

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/sim"
)

type reply struct {
	status int
	body   any
}

func ok(v any) reply      { return reply{http.StatusOK, v} }
func created(v any) reply { return reply{http.StatusCreated, v} }
func noContent() reply    { return reply{http.StatusNoContent, nil} }

// problem is an RFC 9457 refusal whose type is the service's slug.
func problem(status int, svc, slug, detail string) reply {
	return reply{status, map[string]any{
		"type": "https://errors." + svc + ".warehouse-systems.dev/" + slug, "title": slug, "status": status, "detail": detail,
	}}
}

type call struct {
	Svc, Method, Route, Path, Key string
	Status                        int
}

type idemEntry struct {
	fingerprint string
	status      int
	body        []byte
}

type event struct {
	at int
	fn func()
}

type world struct {
	t     *testing.T
	mu    sync.Mutex
	clock *manualClock
	srv   *httptest.Server
	mux   *http.ServeMux

	// tick counts the requests the server has seen: the consumer-lag clock.
	tick   int
	events []event
	// pageMax caps every page, whatever limit the caller asked for, so a walk of three items takes
	// several pages and an actor that reads only the first page fails.
	pageMax int

	idem   map[string]idemEntry
	calls  []call
	noKey  int
	reused int

	om  *omFake
	wes *wesFake
	fe  *feFake
	inv *invFake
}

// newWorld starts the fake services.
func newWorld(t *testing.T) *world {
	t.Helper()
	w := &world{t: t, clock: newManualClock(t0), mux: http.NewServeMux(), idem: map[string]idemEntry{}, pageMax: 2}
	w.om = newOMFake(w)
	w.wes = newWESFake(w)
	w.fe = newFEFake(w)
	w.inv = newInvFake(w)
	w.srv = httptest.NewServer(w.mux)
	t.Cleanup(w.srv.Close)
	t.Cleanup(func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		if w.reused > 0 {
			t.Errorf("%d request(s) re-sent an Idempotency-Key: every POST must carry a fresh key (the services store a 4xx under it)", w.reused)
		}
		if w.noKey > 0 {
			t.Errorf("%d POST(s) went out without an Idempotency-Key", w.noKey)
		}
	})
	return w
}

// client is the sim client over the fakes: base map keys are the repo names, as on the gateway.
func (w *world) client() *sim.Client {
	base := map[string]string{}
	for _, s := range []string{SvcOrders, SvcWES, SvcExecution, SvcInventory} {
		base[s] = w.srv.URL + "/" + s
	}
	c := sim.NewClient(base, sim.NewCallStats())
	c.HTTP = &http.Client{Timeout: 15 * time.Second, Transport: &retryDial{http.DefaultTransport}}
	return c
}

// retryDial retries a dial that fails with EADDRNOTAVAIL (a busy dev machine exhausts ephemeral
// ports under -count=N): environmental, not a product bug.
type retryDial struct{ rt http.RoundTripper }

func (r *retryDial) RoundTrip(req *http.Request) (*http.Response, error) {
	var res *http.Response
	var err error
	for i := 0; i < 20; i++ {
		if req.GetBody != nil {
			req.Body, _ = req.GetBody()
		}
		res, err = r.rt.RoundTrip(req)
		if err == nil || !strings.Contains(err.Error(), "can't assign requested address") {
			return res, err
		}
		time.Sleep(50 * time.Millisecond)
	}
	return res, err
}

// later schedules fn to run once n more requests have reached the server (at once when n <= 0).
// Callers hold w.mu.
func (w *world) later(n int, fn func()) {
	if n <= 0 {
		fn()
		return
	}
	w.events = append(w.events, event{at: w.tick + n, fn: fn})
}

// settle runs every event that is due. Callers hold w.mu.
func (w *world) settle() {
	for again := true; again; {
		again = false
		for i, e := range w.events {
			if e.at <= w.tick {
				w.events = append(w.events[:i:i], w.events[i+1:]...)
				e.fn()
				again = true
				break
			}
		}
	}
}

// route registers a handler under /<svc><path>. The handler runs under the world's lock.
func (w *world) route(method, svc, path string, h func(r *http.Request, body []byte) reply) {
	w.mux.HandleFunc(method+" /"+svc+path, func(rw http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.mu.Lock()
		w.tick++
		w.settle()
		rp, _ := w.serve(method, svc, r, body, h)
		// the answer is encoded under the lock: it may point at live state another request is about to change
		rp.body = json.RawMessage(marshal(rp.body))
		w.mu.Unlock()
		writeReply(rw, rp)
	})
}

func (w *world) serve(method, svc string, r *http.Request, body []byte, h func(*http.Request, []byte) reply) (reply, bool) {
	key := r.Header.Get("Idempotency-Key")
	rec := call{Svc: svc, Method: method, Route: r.Pattern, Path: r.URL.RequestURI(), Key: key}
	finish := func(rp reply) (reply, bool) {
		rec.Status = rp.status
		w.calls = append(w.calls, rec)
		return rp, false
	}
	if method != http.MethodPost {
		return finish(h(r, body))
	}
	if key == "" {
		w.noKey++
		return finish(problem(400, svc, "idempotency-key-required", "the Idempotency-Key header is required on a POST"))
	}
	fp := method + " " + r.URL.RequestURI() + " " + string(bytes.TrimSpace(body))
	k := svc + "|" + key
	if e, seen := w.idem[k]; seen {
		w.reused++
		if e.fingerprint != fp {
			return finish(problem(409, svc, "idempotency-key-reused", "the key was used with a different request"))
		}
		rec.Status = e.status
		w.calls = append(w.calls, rec)
		return reply{e.status, json.RawMessage(e.body)}, true
	}
	rp := h(r, body)
	b := marshal(rp.body)
	w.idem[k] = idemEntry{fingerprint: fp, status: rp.status, body: b}
	return finish(rp)
}

func marshal(v any) []byte {
	if v == nil {
		return nil
	}
	if raw, isRaw := v.(json.RawMessage); isRaw {
		return raw
	}
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func writeReply(rw http.ResponseWriter, rp reply) {
	b := marshal(rp.body)
	if rp.status >= 400 {
		rw.Header().Set("Content-Type", "application/problem+json")
	} else if b != nil {
		rw.Header().Set("Content-Type", "application/json")
	}
	rw.WriteHeader(rp.status)
	if b != nil {
		_, _ = rw.Write(b)
	}
}

// decode reads the body strictly: an unknown field is a 400, like the services' decoders.
func decode(svc string, body []byte, v any) (reply, bool) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return problem(400, svc, "malformed-request-body", err.Error()), false
	}
	return reply{}, true
}

// pageOf answers one page of items: limit above 500 is 400 invalid-limit, any query parameter
// other than the declared filters, limit and cursor is 400 unknown-query-parameter.
func (w *world) pageOf(svc string, r *http.Request, filters []string, items []any) reply {
	allowed := map[string]bool{"limit": true, "cursor": true}
	for _, f := range filters {
		allowed[f] = true
	}
	for k := range r.URL.Query() {
		if !allowed[k] {
			return problem(400, svc, "unknown-query-parameter", "undeclared query parameter "+k)
		}
	}
	limit := 100
	if s := r.URL.Query().Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 || n > 500 {
			return problem(400, svc, "invalid-limit", "limit must be 1-500")
		}
		limit = n
	}
	if limit > w.pageMax {
		limit = w.pageMax
	}
	start := 0
	if c := r.URL.Query().Get("cursor"); c != "" {
		n, err := strconv.Atoi(c)
		if err != nil || n < 0 {
			return problem(400, svc, "invalid-cursor", "cursor does not decode")
		}
		start = n
	}
	if start > len(items) {
		start = len(items)
	}
	end := start + limit
	out := map[string]any{}
	if end < len(items) {
		out["nextCursor"] = strconv.Itoa(end)
	} else {
		end = len(items)
	}
	page := items[start:end]
	if page == nil {
		page = []any{}
	}
	out["items"] = page
	return ok(out)
}

// callsTo counts the calls to an exact route (the mux pattern, e.g. "POST /fulfillment-execution/rebin/arrivals").
func (w *world) callsTo(pattern string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for _, c := range w.calls {
		if c.Route == pattern {
			n++
		}
	}
	return n
}

// writes counts the POSTs that reached the server.
func (w *world) writes() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for _, c := range w.calls {
		if c.Method == http.MethodPost {
			n++
		}
	}
	return n
}

// callLog is the ordered "METHOD route" log, for order assertions.
func (w *world) callLog() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]string, len(w.calls))
	for i, c := range w.calls {
		out[i] = c.Route
	}
	return out
}

// firstIndex is the position of the first call to the route in the log, or -1.
func firstIndex(log []string, route string) int {
	for i, r := range log {
		if r == route {
			return i
		}
	}
	return -1
}

// tap is the Kafka event tap's stand-in: Seen counts the PackageManifested events (and nothing
// else) the fulfillment-execution fake published, after their lag. A look at the tap is a request
// like any other, so the lag counts down while an actor waits on it.
type tap struct{ w *world }

func (p tap) Seen(ref, typ string) int {
	p.w.mu.Lock()
	defer p.w.mu.Unlock()
	p.w.tick++
	p.w.settle()
	if typ != TypePackageManifested {
		return 0
	}
	return p.w.fe.manifested[ref]
}

func must(t *testing.T, cond bool, format string, args ...any) {
	t.Helper()
	if !cond {
		t.Fatalf(format, args...)
	}
}

// countStatus counts the calls to an exact route that were answered with the status.
func (w *world) countStatus(pattern string, status int) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for _, c := range w.calls {
		if c.Route == pattern && c.Status == status {
			n++
		}
	}
	return n
}
