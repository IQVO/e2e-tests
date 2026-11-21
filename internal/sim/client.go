// Package sim holds the parts of the e2e simulators that do not depend on
// which tier they drive: the black-box REST client, per-route call
// statistics, host-suspension detection and the Kafka event tap.
//
// Both cmd/warehouse-day (the warehouse floor on its own site) and
// cmd/retail-day (shoppers, floor and network on the storefront's site) use
// it, so a rule fixed here (a fresh Idempotency-Key per POST, IPv4-only Kafka
// dialing, the suspension grace window) is fixed for both.
package sim

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/paging"
)

// LogFunc is the simulators' logger: who is speaking, then a format.
type LogFunc func(who, format string, args ...any)

// Client is a black-box REST client over every bounded context's PUBLISHED
// contract. It never touches a database and never imports a service's Go
// packages: exactly what a handheld scanner, a supervisor console or a
// shopper's browser does. Base maps a service name (also the gateway path
// segment: Kong routes /api/<service>/...) to its base URL.
type Client struct {
	HTTP  *http.Client
	Base  map[string]string
	Stats *CallStats
}

// NewClient returns a client with the simulators' 15 s request timeout.
func NewClient(base map[string]string, stats *CallStats) *Client {
	if stats == nil {
		stats = NewCallStats()
	}
	return &Client{HTTP: &http.Client{Timeout: 15 * time.Second}, Base: base, Stats: stats}
}

// GatewayBase maps every service to <gateway>/<service>, the Kong route shape.
func GatewayBase(gateway string, services []string) map[string]string {
	g := trimSlash(gateway)
	base := make(map[string]string, len(services))
	for _, s := range services {
		base[s] = g + "/" + s
	}
	return base
}

func trimSlash(s string) string {
	for len(s) > 0 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}

// Resp is one HTTP answer: status 0 means a transport error and Body then
// holds the error text. Header is the response header (nil on a transport
// error): Idempotency-Replayed, Retry-After, Location.
type Resp struct {
	Status int
	Body   []byte
	Header http.Header
}

// OK reports a 2xx answer.
func (r Resp) OK() bool { return r.Status >= 200 && r.Status < 300 }

// Decode unmarshals the body into v.
func (r Resp) Decode(v any) error {
	if err := json.Unmarshal(r.Body, v); err != nil {
		return fmt.Errorf("decode %d body %q: %w", r.Status, Truncate(string(r.Body), 200), err)
	}
	return nil
}

// Call performs one request. route is the templated route used as the metrics
// key, path the concrete one. The statistics key is "<service> <METHOD> <route>"; a
// route written with its method ("POST /stations/{id}/task-claims") and one written
// without ("/stations/{id}/task-claims") give the same key.
func (c *Client) Call(ctx context.Context, svc, method, route, path string, body any) Resp {
	return c.CallWith(ctx, svc, method, route, path, body, nil)
}

// CallWith is Call with extra request headers (a shopper's session token, an
// explicit Idempotency-Key to model a retry). A header in extra wins over the
// defaults, including the generated Idempotency-Key.
func (c *Client) CallWith(ctx context.Context, svc, method, route, path string, body any, extra map[string]string) Resp {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			panic(err)
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.Base[svc]+path, rd)
	if err != nil {
		panic(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	if method == http.MethodPost || method == http.MethodPatch {
		// ADR-0028: creating POSTs require an Idempotency-Key. A real
		// device mints one per logical action; this client never
		// re-sends on its own, so a fresh key per call is the honest equivalent.
		req.Header.Set("Idempotency-Key", NewKey())
	}
	for k, v := range extra {
		req.Header.Set(k, v)
	}
	key := svc + " " + method + " " + strings.TrimPrefix(route, method+" ")
	start := time.Now()
	res, err := c.HTTP.Do(req)
	if err != nil {
		if ctx.Err() == nil {
			c.Stats.Record(key, 0, time.Since(start), err.Error())
		}
		return Resp{Status: 0, Body: []byte(err.Error())}
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	c.Stats.Record(key, res.StatusCode, time.Since(start), string(b))
	return Resp{Status: res.StatusCode, Body: b, Header: res.Header.Clone()}
}

// NewKey mints a random Idempotency-Key.
func NewKey() string {
	var b [16]byte
	_, _ = crand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Truncate shortens s to n bytes, marking the cut.
func Truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// ListAll reads a paginated collection (GET path) and returns every item,
// following nextCursor until it is absent. route is the templated metrics key,
// as for Call. The returned Resp is the last page's: !OK() means the walk
// failed and the items are what was read so far.
//
// Pages are requested with limit=500 (the guideline's maximum) unless path
// already carries a limit: a provider with a lower cap (outbound-shipping answers
// 400 invalid-parameter above 200) is read with path "/x?limit=200".
func ListAll[T any](ctx context.Context, c *Client, svc, route, path string) ([]T, Resp) {
	var all []T
	cursor := ""
	for pages := 0; pages < 10000; pages++ {
		r := c.Call(ctx, svc, http.MethodGet, route, paging.With(path, cursor), nil)
		if !r.OK() {
			return all, r
		}
		items, next, err := paging.Decode[T](r.Body)
		if err != nil {
			return all, Resp{Status: 0, Body: []byte(err.Error())}
		}
		all = append(all, items...)
		if next == "" || next == cursor {
			return all, r
		}
		cursor = next
	}
	return all, Resp{Status: 0, Body: []byte("paging did not end")}
}
