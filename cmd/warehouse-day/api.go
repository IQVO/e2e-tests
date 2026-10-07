package main

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// Service names double as the gateway path segment (Kong routes
// /api/<service>/...) and as the metrics prefix.
const (
	svcFacility    = "facility-layout"
	svcInventory   = "inventory-storage"
	svcWES         = "wes-work-planning"
	svcFulfillment = "fulfillment-execution"
	svcWorkforce   = "workforce-management"
	svcOrder       = "order-management"
	svcProcessPath = "process-path-management"
	svcLabor       = "labor-performance"
	// svcProductMaster owns SKU classification and the physical profile
	// (product-master ADR 0001/0003); inventory-storage and the readers
	// only keep local copies fed by its events.
	svcProductMaster = "product-master"
)

var allServices = []string{svcFacility, svcProductMaster, svcInventory, svcWES, svcFulfillment, svcWorkforce, svcOrder, svcProcessPath, svcLabor}

// api is a black-box REST client over every bounded context's PUBLISHED
// contract. It never touches a database and never imports a service's Go
// packages — exactly what a handheld scanner, a supervisor console or an
// upstream order channel would do.
type api struct {
	client *http.Client
	base   map[string]string
	stats  *callStats
}

type callStats struct {
	mu       sync.Mutex
	byRoute  map[string]map[int]int
	latency  map[string]time.Duration
	failures []string // transport errors and 5xx, first 50
}

func newCallStats() *callStats {
	return &callStats{byRoute: map[string]map[int]int{}, latency: map[string]time.Duration{}}
}

func (s *callStats) record(route string, status int, d time.Duration, detail string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.byRoute[route] == nil {
		s.byRoute[route] = map[int]int{}
	}
	s.byRoute[route][status]++
	s.latency[route] += d
	if (status == 0 || status >= 500) && len(s.failures) < 50 {
		s.failures = append(s.failures, fmt.Sprintf("%s -> %d %s", route, status, truncate(detail, 300)))
	}
}

// count5xx returns the number of transport errors (status 0) and 5xx
// responses across every route.
func (s *callStats) count5xx() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, codes := range s.byRoute {
		for c, k := range codes {
			if c == 0 || c >= 500 {
				n += k
			}
		}
	}
	return n
}

type routeLine struct {
	Route    string      `json:"route"`
	Calls    int         `json:"calls"`
	Statuses map[int]int `json:"statuses"`
	AvgMs    float64     `json:"avgMs"`
}

func (s *callStats) lines() []routeLine {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]routeLine, 0, len(s.byRoute))
	for r, codes := range s.byRoute {
		n := 0
		cp := map[int]int{}
		for c, k := range codes {
			n += k
			cp[c] = k
		}
		out = append(out, routeLine{Route: r, Calls: n, Statuses: cp, AvgMs: float64(s.latency[r].Milliseconds()) / float64(n)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Route < out[j].Route })
	return out
}

func newAPI(cfg config, stats *callStats) *api {
	base := map[string]string{}
	if cfg.gateway != "" {
		g := strings.TrimRight(cfg.gateway, "/")
		for _, s := range allServices {
			base[s] = g + "/" + s
		}
	} else {
		base[svcFacility] = envOr("FACILITY_BASE_URL", "http://localhost:8081")
		base[svcInventory] = envOr("INVENTORY_BASE_URL", "http://localhost:8082")
		base[svcWES] = envOr("WES_BASE_URL", "http://localhost:8083")
		base[svcFulfillment] = envOr("FULFILLMENT_BASE_URL", "http://localhost:8084")
		base[svcWorkforce] = envOr("WORKFORCE_BASE_URL", "http://localhost:8085")
		base[svcOrder] = envOr("ORDER_BASE_URL", "http://localhost:8086")
		base[svcProcessPath] = envOr("PROCESS_PATH_BASE_URL", "http://localhost:8087")
		base[svcLabor] = envOr("LABOR_BASE_URL", "http://localhost:8088")
		base[svcProductMaster] = envOr("PRODUCT_MASTER_BASE_URL", "http://localhost:8090")
	}
	return &api{
		client: &http.Client{Timeout: 15 * time.Second},
		base:   base,
		stats:  stats,
	}
}

type resp struct {
	status int
	body   []byte
}

func (r resp) ok() bool { return r.status >= 200 && r.status < 300 }

func (r resp) decode(v any) error {
	if err := json.Unmarshal(r.body, v); err != nil {
		return fmt.Errorf("decode %d body %q: %w", r.status, truncate(string(r.body), 200), err)
	}
	return nil
}

// call performs one request. route is the templated route used as the
// metrics key (e.g. "POST /stations/{id}/claim-next"), path the concrete one.
func (a *api) call(ctx context.Context, svc, method, route, path string, body any) resp {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			panic(err)
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, a.base[svc]+path, rd)
	if err != nil {
		panic(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	if method == http.MethodPost {
		// ADR-0028: creating POSTs require an Idempotency-Key. A real
		// device mints one per logical action; this client never
		// re-sends, so a fresh key per call is the honest equivalent.
		req.Header.Set("Idempotency-Key", newKey())
	}
	key := svc + " " + method + " " + route
	start := time.Now()
	res, err := a.client.Do(req)
	if err != nil {
		if ctx.Err() == nil {
			a.stats.record(key, 0, time.Since(start), err.Error())
		}
		return resp{status: 0, body: []byte(err.Error())}
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	a.stats.record(key, res.StatusCode, time.Since(start), string(b))
	return resp{status: res.StatusCode, body: b}
}

func newKey() string {
	var b [16]byte
	_, _ = crand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
