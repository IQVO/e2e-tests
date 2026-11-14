// Package shopper is the retail-day SHOPPER actor: one simulated person who buys through the
// storefront BFF over REST only (ADR 0001, phase 3). No database, no injected event, no Go package
// of another repository: it is what a browser does, minus the pixels.
//
// # Where the BFF is
//
// The base URL is configuration, never a constant: STOREFRONT_BASE_URL (DefaultBaseURL when unset).
// The BFF serves its routes under /api/storefront AND, behind a path-stripping proxy, without the
// prefix (its own stripBase), so every edge below reaches the same routes:
//
//   - http://storefront.localhost/api/storefront: the storefront host of the web gateway (nginx,
//     warehouse-infra terraform web-gateway.conf.tftpl: host-based, "/" to the storefront-web SSR
//     server). The SSR server proxies /api/storefront to its BFF_URL (storefront-web server/index.ts
//     and proxy.ts), so the browser and the cookie share one origin. This is what
//     cmd/seed-storefront defaults to. Never aim at a storefront-web started with
//     STOREFRONT_STUB=1: a stub answers there, not the BFF.
//   - http://localhost:8000/api/storefront: Kong (warehouse-infra helm-values/storefront-bff.yaml:
//     gatewayApi path /api/storefront, stripPath: true, so the BFF sees /products and answers).
//   - http://<bff>:8080/api/storefront or http://<bff>:8080: the BFF pod itself.
//
// # State and idempotency
//
// The sf_state cookie carries the shopper, cart and last checkout id. It is state, not
// authentication, and there is no bearer: each Shopper owns a cookie jar (net/http/cookiejar), so
// two Shoppers on one machine never share a cart. The BFF scopes the cookie to Path=/api/storefront;
// the jar here keeps it for any base path (a base without the prefix would otherwise never send it
// back), which is right because one Shopper talks to one base. Every POST and PATCH carries an
// Idempotency-Key: a fresh key per logical action, REUSED when the same request is retried after a
// lost answer (transport error, 502, 503), so a retry replays (Idempotency-Replayed: true) instead
// of acting twice. The PUT routes are idempotent by method and carry none.
//
// # Findings, not panics
//
// A step the BFF rejects returns a *Rejection (status, problem slug, detail) and is also written to
// the shopper's findings; Buy returns them in Outcome.Problems with the timeline of every request.
// Only a setup bug (no base URL, an empty plan, a postal prefix that is not 3 characters) returns
// an error from Buy.
//
// # The shared client
//
// A Shopper is built on a *sim.Client and sends every request through sim.Client.CallWith, so the
// per-route statistics land in the same sim.CallStats as every other actor. The client's HTTP field
// is the only thing that cannot be shared (the cookie jar is per shopper), so New copies the client
// with its own *http.Client, which wraps the parent's transport. sim.Resp carries no response
// headers, so Idempotency-Replayed is read by that wrapper (see replayTap).
package shopper

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/sim"
)

const (
	// Service is the key of the storefront in sim.Client.Base.
	Service = "storefront"
	// EnvBaseURL names the environment variable that configures the BFF base URL.
	EnvBaseURL = "STOREFRONT_BASE_URL"
	// DefaultBaseURL is the storefront host of the local cluster (see the package comment).
	DefaultBaseURL = "http://storefront.localhost/api/storefront"
	// CookieName is the BFF's state cookie.
	CookieName = "sf_state"

	// prefixLen is the length of the postal prefix every destination uses.
	prefixLen = 3

	replayedHeader = "Idempotency-Replayed"
)

// BaseURLFromEnv returns $STOREFRONT_BASE_URL, or DefaultBaseURL when it is unset or blank.
func BaseURLFromEnv() string {
	if v := strings.TrimSpace(os.Getenv(EnvBaseURL)); v != "" {
		return strings.TrimRight(v, "/")
	}
	return DefaultBaseURL
}

// NewClient returns a sim.Client whose only service is the storefront at baseURL; stats may be
// shared with every other actor (nil gets fresh statistics).
func NewClient(baseURL string, stats *sim.CallStats) *sim.Client {
	if stats == nil {
		stats = sim.NewCallStats()
	}
	return sim.NewClient(map[string]string{Service: strings.TrimRight(baseURL, "/")}, stats)
}

// Options tunes a Shopper. The zero value is the production setting.
type Options struct {
	// Email is the shopper's address; empty mints a unique one (sim-<hex>@example.com).
	Email string
	// Log receives one line per step; nil is silent.
	Log sim.LogFunc
	// PollEvery is the pause between two GET /checkout/{id} while a placement is in doubt
	// (default 1 s).
	PollEvery time.Duration
	// PollTimeout bounds the wait for a definitive session (default 45 s). When it ends the
	// shopper records outcome in-doubt and gives up: tok_timeout leaves the session AUTHORIZING
	// with no way out (journey gap G8), and the actor must never loop forever on it.
	PollTimeout time.Duration
	// Retries is how many times a request is repeated, with the SAME Idempotency-Key, after a
	// transport error, 502 or 503 (default 2; negative disables). A 504 is never retried: the BFF
	// answers it when its time budget expired and does not retry it either.
	Retries int
	// RetryDelay is the pause before a retry (default 250 ms).
	RetryDelay time.Duration
	// MaxRequotes is how many times Buy re-quotes, re-selects and re-confirms after the stack
	// asks for it (default 1; negative disables).
	MaxRequotes int
}

func (o Options) withDefaults() Options {
	if o.PollEvery <= 0 {
		o.PollEvery = time.Second
	}
	if o.PollTimeout <= 0 {
		o.PollTimeout = 45 * time.Second
	}
	if o.Retries == 0 {
		o.Retries = 2
	}
	if o.RetryDelay <= 0 {
		o.RetryDelay = 250 * time.Millisecond
	}
	if o.MaxRequotes == 0 {
		o.MaxRequotes = 1
	}
	return o
}

// Step is one request of the timeline.
type Step struct {
	Name     string        `json:"name"`
	At       time.Time     `json:"at"`
	Took     time.Duration `json:"took"`
	Status   int           `json:"status"` // 0 is a transport error
	Replayed bool          `json:"replayed,omitempty"`
	Attempt  int           `json:"attempt,omitempty"` // 0 is the first try; a retry reuses the key
}

// Shopper is one person with one browser: its own cookie jar and the ids it learned. It is not
// safe for concurrent use; run one goroutine per shopper.
type Shopper struct {
	// Email is the address the shopper registers and signs in with.
	Email string
	// ShopperID, CartID and SessionID are the ids the shopper learned from the BFF's answers
	// (the sf_state cookie carries the same three: see State).
	ShopperID string
	CartID    string
	SessionID string
	// AddressID is the saved address a checkout ships to.
	AddressID string

	c    *sim.Client
	tap  *replayTap
	jar  *stateJar
	base *url.URL
	opt  Options

	addrs      []SavedAddress
	findings   []string
	suppressed int // findings beyond maxFindings, counted not kept
	timeline   []Step
}

// New builds a shopper on parent's base URL and statistics, with its own cookie jar. An error is a
// setup bug: no client, or no storefront in its Base.
func New(parent *sim.Client, opt Options) (*Shopper, error) {
	if parent == nil {
		return nil, errors.New("shopper: nil *sim.Client")
	}
	raw := strings.TrimRight(parent.Base[Service], "/")
	if raw == "" {
		return nil, fmt.Errorf("shopper: the client has no %q base URL (set %s)", Service, EnvBaseURL)
	}
	base, err := url.Parse(raw)
	if err != nil || base.Scheme == "" || base.Host == "" {
		return nil, fmt.Errorf("shopper: bad %s %q", EnvBaseURL, raw)
	}
	stats := parent.Stats
	if stats == nil {
		stats = sim.NewCallStats()
	}
	var next http.RoundTripper = http.DefaultTransport
	timeout := 15 * time.Second
	if parent.HTTP != nil {
		if parent.HTTP.Transport != nil {
			next = parent.HTTP.Transport
		}
		if parent.HTTP.Timeout > 0 {
			timeout = parent.HTTP.Timeout
		}
	}
	jar, err := newStateJar()
	if err != nil {
		return nil, err
	}
	tap := &replayTap{next: next}
	opt = opt.withDefaults()
	email := strings.TrimSpace(opt.Email)
	if email == "" {
		email = "sim-" + sim.NewKey()[:12] + "@example.com"
	}
	return &Shopper{
		Email: email,
		c: &sim.Client{
			HTTP:  &http.Client{Transport: tap, Jar: jar, Timeout: timeout},
			Base:  parent.Base,
			Stats: stats,
		},
		tap: tap, jar: jar, base: base, opt: opt,
	}, nil
}

// Timeline returns every request this shopper made, oldest first.
func (s *Shopper) Timeline() []Step { return append([]Step(nil), s.timeline...) }

// Findings returns every rejected request, oldest first (what Outcome.Problems carries for a Buy).
func (s *Shopper) Findings() []string {
	out := append([]string(nil), s.findings...)
	if s.suppressed > 0 {
		out = append(out, fmt.Sprintf("(+%d more findings not listed)", s.suppressed))
	}
	return out
}

// State decodes the sf_state cookie the jar holds: the ids the BFF thinks this browser has.
func (s *Shopper) State() State {
	for _, c := range s.jar.Cookies(s.base) {
		if c.Name == CookieName {
			return decodeState(c.Value)
		}
	}
	return State{}
}

func (s *Shopper) logf(format string, args ...any) {
	if s.opt.Log != nil {
		s.opt.Log("shopper "+s.Email, format, args...)
	}
}

// State is what the sf_state cookie remembers.
type State struct {
	CartID     string
	ShopperID  string
	CheckoutID string
}

func decodeState(raw string) State {
	if u, err := url.QueryUnescape(raw); err == nil {
		raw = u
	}
	v, err := url.ParseQuery(raw)
	if err != nil {
		return State{}
	}
	return State{CartID: v.Get("c"), ShopperID: v.Get("s"), CheckoutID: v.Get("k")}
}

// stateJar is a net/http/cookiejar.Jar that stores every cookie with Path=/. The BFF scopes sf_state
// to /api/storefront, which the jar would not send back to a base URL that has no such prefix (the
// BFF itself, or any path-stripping edge); one Shopper talks to one base, so path scoping adds
// nothing here.
type stateJar struct{ in *cookiejar.Jar }

func newStateJar() (*stateJar, error) {
	in, err := cookiejar.New(nil)
	if err != nil {
		return nil, err
	}
	return &stateJar{in: in}, nil
}

func (j *stateJar) SetCookies(u *url.URL, cookies []*http.Cookie) {
	flat := make([]*http.Cookie, len(cookies))
	for i, c := range cookies {
		cp := *c
		cp.Path = "/"
		flat[i] = &cp
	}
	j.in.SetCookies(u, flat)
}

func (j *stateJar) Cookies(u *url.URL) []*http.Cookie { return j.in.Cookies(u) }

// replayTap wraps the transport to see the Idempotency-Replayed header, which sim.Resp drops.
type replayTap struct {
	next http.RoundTripper
	mu   sync.Mutex
	saw  bool
}

func (t *replayTap) RoundTrip(req *http.Request) (*http.Response, error) {
	res, err := t.next.RoundTrip(req)
	if err == nil && res.Header.Get(replayedHeader) == "true" {
		t.mu.Lock()
		t.saw = true
		t.mu.Unlock()
	}
	return res, err
}

func (t *replayTap) reset() {
	t.mu.Lock()
	t.saw = false
	t.mu.Unlock()
}

func (t *replayTap) replayed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.saw
}

// reply is one answer plus the header sim.Resp does not carry.
type reply struct {
	sim.Resp
	Replayed bool
}

func retryable(status int) bool { return status == 0 || status == 502 || status == 503 }

// call performs one logical step. A POST or PATCH gets one fresh Idempotency-Key which every retry
// of the same request reuses; the other verbs carry none. Every non-2xx answer is a finding, the
// retried ones included: a retry that recovers still shows that the first answer was an error.
func (s *Shopper) call(ctx context.Context, step, method, label, path string, body any) reply {
	// sim.Client keys its statistics by service, method and the path template: no method in route
	route := strings.TrimPrefix(label, method+" ")
	var extra map[string]string
	if method == http.MethodPost || method == http.MethodPatch {
		extra = map[string]string{"Idempotency-Key": sim.NewKey()}
	}
	retries := s.opt.Retries
	if retries < 0 {
		retries = 0
	}
	for attempt := 0; ; attempt++ {
		s.tap.reset()
		start := time.Now()
		r := s.c.CallWith(ctx, Service, method, route, path, body, extra)
		rp := reply{Resp: r, Replayed: s.tap.replayed()}
		s.timeline = append(s.timeline, Step{Name: step, At: start, Took: time.Since(start), Status: r.Status, Replayed: rp.Replayed, Attempt: attempt})
		if !r.OK() {
			s.finding(method+" "+route, r)
		}
		s.logf("%s %s -> %d%s", method, path, r.Status, map[bool]string{true: " (replayed)", false: ""}[rp.Replayed])
		if r.OK() || ctx.Err() != nil || attempt >= retries || !retryable(r.Status) {
			return rp
		}
		if !sleep(ctx, s.opt.RetryDelay) {
			return rp
		}
	}
}

// sleep waits d or until ctx ends; it reports whether the full time elapsed.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
