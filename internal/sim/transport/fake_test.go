package transport

// The contract fakes: ONE httptest server that answers, under /<service>/..., the
// routes of linehaul-transportation, last-mile-delivery, shipping-and-tracking,
// node-topology and customer-ordering that the actors use, with the rules the
// published contracts (apis/openapi.yaml of each repo, read from origin/develop)
// and their ADRs state. The fakes ENFORCE them: a call that breaks a rule is
// answered with the real problem slug, so a test fails when an actor breaks the
// contract, not only when it breaks the fake.
//
// fake_linehaul_test.go, fake_lastmile_test.go and fake_tracking_test.go hold the
// per-service state machines. This file holds the shared server: routing under the
// service prefix, the Idempotency-Key rule (a POST without a key is 400, the same
// key with another request is 409, the same request replays), the problem+json
// writer, strict body decoding and the call log the tests assert on.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/sim"
)

var chicago = func() *time.Location {
	l, err := time.LoadLocation("America/Chicago")
	if err != nil {
		panic(err)
	}
	return l
}()

// requestLatency is the fake time one request takes.
const requestLatency = 5 * time.Millisecond

type reply struct {
	status int
	body   any
}

func ok(v any) reply      { return reply{http.StatusOK, v} }
func created(v any) reply { return reply{http.StatusCreated, v} }

// problem is an RFC 9457 refusal whose type is the model's slug.
func problem(status int, slug, detail string) reply {
	return reply{status, map[string]any{
		"type": "https://retailnetwork.example/problems/" + slug, "title": slug, "status": status, "detail": detail, "instance": "/",
	}}
}

type call struct {
	Svc, Method, Path, Key string
	Status                 int
	Replayed               bool
}

type idemEntry struct {
	fingerprint string
	status      int
	body        []byte
}

type world struct {
	t     *testing.T
	mu    sync.Mutex
	clock *ManualClock
	// skew is how far the services' clocks run ahead of the actors' clock.
	skew time.Duration
	srv  *httptest.Server
	mux  *http.ServeMux

	idem  map[string]idemEntry
	calls []call
	// noKey counts POSTs that arrived without an Idempotency-Key (each was refused 400).
	noKey int

	ordering *orderingFake
	tracking *trackingFake
	linehaul *linehaulFake
	lastmile *lastMileFake
	nodes    map[string]fnode
}

type fnode struct {
	ID       string `json:"nodeId"`
	Type     string `json:"nodeType"`
	Timezone string `json:"timezone"`
	Status   string `json:"status"`
}

// newWorld starts the fake services; the clock starts at t0 (15:45 America/Chicago).
func newWorld(t *testing.T) *world {
	t.Helper()
	w := &world{t: t, clock: NewManualClock(t0), idem: map[string]idemEntry{}, mux: http.NewServeMux(), nodes: map[string]fnode{}}
	w.ordering = newOrderingFake(w)
	w.tracking = newTrackingFake(w)
	w.linehaul = newLinehaulFake(w)
	w.lastmile = newLastMileFake(w)
	w.route("GET", SvcNodes, "/nodes/{id}", func(r *http.Request, _ []byte) reply {
		n, found := w.nodes[r.PathValue("id")]
		if !found {
			return problem(404, "not-found", "unknown node")
		}
		return ok(n)
	})
	w.srv = httptest.NewServer(w.mux)
	t.Cleanup(w.srv.Close)
	return w
}

// now is the SERVICES' clock: the actors' clock plus the skew.
func (w *world) now() time.Time { return w.clock.Now().Add(w.skew).UTC() }

// client returns a sim.Client against the fakes (a fresh Idempotency-Key per POST, as in production).
func (w *world) client() *sim.Client {
	base := map[string]string{}
	for _, s := range []string{SvcLinehaul, SvcLastMile, SvcTracking, SvcNodes, SvcOrdering, SvcVendor} {
		base[s] = w.srv.URL + "/" + s
	}
	c := sim.NewClient(base, sim.NewCallStats())
	// Own transport: connections are closed with the test, and a dial that fails for lack of an
	// ephemeral port (EADDRNOTAVAIL: seen on a busy dev machine running several test binaries) is
	// retried briefly instead of failing a test that has nothing to do with the network.
	tr := &http.Transport{DialContext: retryDial}
	w.t.Cleanup(tr.CloseIdleConnections)
	c.HTTP = &http.Client{Transport: tr, Timeout: c.HTTP.Timeout}
	return c
}

func retryDial(ctx context.Context, network, addr string) (net.Conn, error) {
	var d net.Dialer
	var err error
	for i := 0; i < 20; i++ {
		var conn net.Conn
		if conn, err = d.DialContext(ctx, network, addr); err == nil {
			return conn, nil
		}
		if !errors.Is(err, syscall.EADDRNOTAVAIL) {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return nil, err
}

// route registers one handler: pattern is "<METHOD> /<svc><path>".
func (w *world) route(method, svc, path string, h func(r *http.Request, body []byte) reply) {
	w.mux.HandleFunc(method+" /"+svc+path, func(rw http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.mu.Lock()
		defer w.mu.Unlock()
		// Every request takes a little time, so a service-stamped time is always later than a time
		// the actor sent in an earlier request.
		w.clock.Advance(requestLatency)
		c := call{Svc: svc, Method: r.Method, Path: r.URL.Path[len(svc)+1:], Key: r.Header.Get("Idempotency-Key")}
		if r.URL.RawQuery != "" {
			c.Path += "?" + r.URL.RawQuery
		}
		var rep reply
		var raw []byte
		if r.Method == http.MethodPost {
			if c.Key == "" {
				w.noKey++
				rep = problem(400, "idempotency-key-required", "Idempotency-Key header is required")
			} else {
				fp := r.Method + " " + r.URL.Path + " " + string(body)
				if e, seen := w.idem[svc+"|"+c.Key]; seen {
					if e.fingerprint != fp {
						rep = problem(409, "idempotency-key-reused", "the key was already used with another request")
					} else {
						rw.Header().Set("Idempotency-Replayed", "true")
						rw.Header().Set("Content-Type", "application/json")
						rw.WriteHeader(e.status)
						_, _ = rw.Write(e.body)
						c.Status, c.Replayed = e.status, true
						w.calls = append(w.calls, c)
						return
					}
				}
			}
		}
		if rep.status == 0 {
			rep = h(r, body)
		}
		raw, _ = json.Marshal(rep.body)
		if r.Method == http.MethodPost && c.Key != "" && rep.status >= 200 && rep.status < 300 {
			if _, seen := w.idem[svc+"|"+c.Key]; !seen {
				w.idem[svc+"|"+c.Key] = idemEntry{fingerprint: r.Method + " " + r.URL.Path + " " + string(body), status: rep.status, body: raw}
			}
		}
		ct := "application/json"
		if rep.status >= 400 {
			ct = "application/problem+json"
		}
		rw.Header().Set("Content-Type", ct)
		rw.WriteHeader(rep.status)
		_, _ = rw.Write(raw)
		c.Status = rep.status
		w.calls = append(w.calls, c)
	})
}

// strict decodes a body the way the services do: unknown fields and an empty body are 422.
func strict(body []byte, v any) *reply {
	if len(bytes.TrimSpace(body)) == 0 {
		r := problem(422, "validation-failed", "empty body")
		return &r
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		r := problem(422, "validation-failed", err.Error())
		return &r
	}
	return nil
}

// strictQuery refuses a query parameter the route does not declare (400 unknown-query-parameter).
func strictQuery(r *http.Request, allowed ...string) *reply {
	for k := range r.URL.Query() {
		found := false
		for _, a := range allowed {
			if a == k {
				found = true
			}
		}
		if !found {
			p := problem(400, "unknown-query-parameter", k)
			return &p
		}
	}
	return nil
}

func rfc(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

// callsTo returns the logged calls whose "<METHOD> <path-without-query>" starts with prefix.
func (w *world) callsTo(svc, prefix string) []call {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []call
	for _, c := range w.calls {
		p := c.Path
		if i := strings.IndexByte(p, '?'); i >= 0 {
			p = p[:i]
		}
		if c.Svc == svc && strings.HasPrefix(c.Method+" "+p, prefix) {
			out = append(out, c)
		}
	}
	return out
}

// countCalls counts the logged calls whose "<METHOD> <path-without-query>" is exactly route.
func (w *world) countCalls(svc, route string) int {
	n := 0
	for _, c := range w.callsTo(svc, route) {
		p := c.Path
		if i := strings.IndexByte(p, '?'); i >= 0 {
			p = p[:i]
		}
		if c.Method+" "+p == route {
			n++
		}
	}
	return n
}

// assertEveryPostKeyed fails the test when any POST arrived without a key or when two different
// POSTs shared a key (an actor must mint a fresh key per logical action).
func (w *world) assertEveryPostKeyed() {
	w.t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.noKey != 0 {
		w.t.Errorf("%d POST(s) arrived without an Idempotency-Key", w.noKey)
	}
	seen := map[string]string{}
	for _, c := range w.calls {
		if c.Method != http.MethodPost || c.Replayed {
			continue
		}
		k := c.Svc + "|" + c.Key
		if prev, dup := seen[k]; dup {
			w.t.Errorf("key %q reused by %s and %s %s", c.Key, prev, c.Method, c.Path)
		}
		seen[k] = c.Method + " " + c.Path
	}
}

// sequence returns the "<METHOD> <path>" of the calls of svc whose path (query removed)
// matches one of the templates in order, as a compact list, for order assertions.
func (w *world) sequence(svc string) []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []string
	for _, c := range w.calls {
		if c.Svc != svc || c.Method != http.MethodPost || c.Status < 200 || c.Status >= 300 {
			continue
		}
		out = append(out, c.Method+" "+c.Path)
	}
	return out
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func sprintf(f string, a ...any) string { return fmt.Sprintf(f, a...) }
