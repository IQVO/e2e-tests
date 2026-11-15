package collect

// The contract fakes: ONE httptest server that answers, under /<service>/..., the READ routes
// the collector uses, with the response shapes the published contracts state (apis/openapi.yaml
// of each repo, read from origin/develop on 2026-11-14) and their ADRs. The fakes ENFORCE what
// the contracts enforce: an undeclared query parameter is 400 unknown-query-parameter, a limit
// above the route's cap is 400 invalid-limit, a missing required filter is 400, a page is
// {items, nextCursor} and is kept to two items so every walk crosses a page. A request that is
// not a GET is recorded and answered 405: the tests assert there were none.
//
// fake_*_test.go hold one tiny state machine per service; this file holds the shared server:
// routing under the service prefix, the problem+json writer, the paging, the call log and the
// failure injection (a whole context answering 500, one route answering 500, a connection that
// drops).

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
	"github.com/claudioed/warehouse-e2e/internal/sim"
)

type reply struct {
	status int
	body   any
}

func ok(v any) reply { return reply{http.StatusOK, v} }

// problem is an RFC 9457 refusal whose type is the model's slug.
func problem(status int, slug, detail string) reply {
	return reply{status, map[string]any{
		"type": "https://retailnetwork.example/problems/" + slug, "title": slug, "status": status, "detail": detail,
	}}
}

func notFound(what string) reply { return problem(http.StatusNotFound, "not-found", what) }

type call struct {
	Svc, Method, Path, Query string
	Status                   int
}

// fakePage is how many items the fakes put on one page. Two, so any list of three walks two pages.
const fakePage = 2

type world struct {
	t   *testing.T
	mu  sync.Mutex
	srv *httptest.Server
	mux *http.ServeMux

	calls []call
	// fails: every request to the service answers this status (internal-error problem).
	fails map[string]int
	// routeFails: one route ("customer-ordering GET /orders/{orderId}") answers this status.
	routeFails map[string]int
	// drops: the connection to the service is closed without an answer (a transport error).
	drops map[string]bool
	// links: customer order id -> order-management ids, what the test gives Settings.OrderIDs.
	links map[string][]string

	ordering *orderingFake
	payments *paymentsFake
	checkout *checkoutFake
	sourcing *sourcingFake
	vendor   *vendorFake
	tracking *trackingFake
	oms      *omFake
	wes      *wesFake
	fe       *feFake
	outbound *outboundFake
	inv      *inventoryFake
	bff      *bffFake
}

// newWorld starts the fake services.
func newWorld(t *testing.T) *world {
	t.Helper()
	w := &world{t: t, mux: http.NewServeMux(), fails: map[string]int{}, routeFails: map[string]int{}, drops: map[string]bool{}}
	w.ordering = newOrderingFake(w)
	w.payments = newPaymentsFake(w)
	w.checkout = newCheckoutFake(w)
	w.sourcing = newSourcingFake(w)
	w.vendor = newVendorFake(w)
	w.tracking = newTrackingFake(w)
	w.oms = newOMFake(w)
	w.wes = newWESFake(w)
	w.fe = newFEFake(w)
	w.outbound = newOutboundFake(w)
	w.inv = newInventoryFake(w)
	w.bff = newBFFFake(w)
	w.srv = httptest.NewServer(http.HandlerFunc(w.serve))
	t.Cleanup(w.srv.Close)
	return w
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (s *statusWriter) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func (w *world) serve(rw http.ResponseWriter, r *http.Request) {
	svc := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)[0]
	sw := &statusWriter{ResponseWriter: rw, status: http.StatusOK}
	defer func() {
		w.mu.Lock()
		w.calls = append(w.calls, call{Svc: svc, Method: r.Method, Path: r.URL.Path, Query: r.URL.RawQuery, Status: sw.status})
		w.mu.Unlock()
	}()
	w.mu.Lock()
	drop := w.drops[svc]
	w.mu.Unlock()
	if drop {
		if h, ok := rw.(http.Hijacker); ok {
			conn, _, err := h.Hijack()
			if err == nil {
				sw.status = 0
				_ = conn.Close()
				return
			}
		}
	}
	if r.Method != http.MethodGet {
		write(sw, problem(http.StatusMethodNotAllowed, "method-not-allowed", "the fake serves reads only"))
		return
	}
	w.mux.ServeHTTP(sw, r)
}

func write(rw http.ResponseWriter, rep reply) {
	b, err := json.Marshal(rep.body)
	if err != nil {
		panic(err)
	}
	if rep.status >= 400 {
		rw.Header().Set("Content-Type", "application/problem+json")
	} else {
		rw.Header().Set("Content-Type", "application/json")
	}
	rw.WriteHeader(rep.status)
	_, _ = rw.Write(b)
}

// get registers a GET route of a service. The handler runs under the world's lock and the
// body is encoded inside it (a reply points at live state).
func (w *world) get(svc, pattern string, h func(r *http.Request) reply) {
	route := "GET /" + svc + pattern
	w.mux.HandleFunc(route, func(rw http.ResponseWriter, r *http.Request) {
		w.mu.Lock()
		defer w.mu.Unlock()
		if st := w.fails[svc]; st != 0 {
			write(rw, problem(st, "internal-error", "an unexpected error occurred"))
			return
		}
		if st := w.routeFails[svc+" GET "+pattern]; st != 0 {
			write(rw, problem(st, "internal-error", "an unexpected error occurred"))
			return
		}
		write(rw, h(r))
	})
}

// fail makes every request to the service answer status.
func (w *world) fail(svc string, status int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.fails[svc] = status
}

// failRoute makes one route of the service answer status; pattern as registered ("/orders/{orderId}").
func (w *world) failRoute(svc, pattern string, status int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.routeFails[svc+" GET "+pattern] = status
}

// drop makes the service close the connection without answering.
func (w *world) drop(svc string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.drops[svc] = true
}

// base is the sim.Client base map: one prefix per service, the BFF on its own.
func (w *world) base() map[string]string {
	m := map[string]string{}
	for _, s := range retailday.AllGateway() {
		m[s] = w.srv.URL + "/" + s
	}
	m[retailday.BFF] = w.srv.URL + "/" + retailday.BFF
	return m
}

func (w *world) client() *sim.Client { return sim.NewClient(w.base(), nil) }

// snapshot returns the call log so far.
func (w *world) snapshot() []call {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]call(nil), w.calls...)
}

func (w *world) reset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls = nil
}

// notGets lists every recorded request that was not a GET.
func (w *world) notGets() []call {
	var out []call
	for _, c := range w.snapshot() {
		if c.Method != http.MethodGet {
			out = append(out, c)
		}
	}
	return out
}

// requests counts the recorded GETs of a service whose path starts with prefix (a path is
// "/<svc>/..."; prefix is the part after the service).
func (w *world) requests(svc, prefix string) int {
	n := 0
	for _, c := range w.snapshot() {
		if c.Svc == svc && strings.HasPrefix(c.Path, "/"+svc+prefix) {
			n++
		}
	}
	return n
}

// reads is the distinct "svc path" of every request, sorted: what the collector looked at.
func (w *world) reads() []string {
	seen := map[string]bool{}
	for _, c := range w.snapshot() {
		seen[c.Svc+" "+strings.TrimPrefix(c.Path, "/"+c.Svc)] = true
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// checkQuery refuses a query parameter the route does not declare (400 unknown-query-parameter),
// the way the services do, so the collector cannot send one without a test noticing.
func checkQuery(r *http.Request, allowed ...string) *reply {
	for k := range r.URL.Query() {
		okp := false
		for _, a := range allowed {
			if a == k {
				okp = true
			}
		}
		if !okp {
			p := problem(http.StatusBadRequest, "unknown-query-parameter", "query parameter "+k+" is not declared")
			return &p
		}
	}
	return nil
}

// pageOf serves one page of n items ({items, nextCursor}) the way a keyset-paged collection
// does: limit must be an integer from 1 to maxLimit, the cursor is opaque, a page holds at
// most fakePage items. item renders one element.
func pageOf(r *http.Request, maxLimit, n int, item func(i int) any) reply {
	size := fakePage
	if l := r.URL.Query().Get("limit"); l != "" {
		v, err := strconv.Atoi(l)
		if err != nil || v < 1 || v > maxLimit {
			return problem(http.StatusBadRequest, "invalid-limit", "limit is not an integer from 1 to "+strconv.Itoa(maxLimit))
		}
		size = min(size, v)
	}
	start := 0
	if c := r.URL.Query().Get("cursor"); c != "" {
		v, err := strconv.Atoi(strings.TrimPrefix(c, "cur-"))
		if err != nil || !strings.HasPrefix(c, "cur-") || v < 0 || v > n {
			return problem(http.StatusBadRequest, "invalid-cursor", "the cursor does not decode")
		}
		start = v
	}
	end := min(start+size, n)
	items := make([]any, 0, end-start)
	for i := start; i < end; i++ {
		items = append(items, item(i))
	}
	body := map[string]any{"items": items}
	if end < n {
		body["nextCursor"] = "cur-" + strconv.Itoa(end)
	}
	return ok(body)
}

func q(r *http.Request, k string) string { return r.URL.Query().Get(k) }

var _ = url.QueryEscape
