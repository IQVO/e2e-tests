package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
	"github.com/claudioed/warehouse-e2e/internal/sim"
)

// fakeCluster is one httptest server standing for the gateway: /api/<service>/<path>. A route is
// "<METHOD> <service>/<path without query>"; anything unrouted answers a 404 problem document (the
// shape a service gives for something it does not know). Every request is logged.
type fakeCluster struct {
	t      *testing.T
	srv    *httptest.Server
	routes map[string]http.HandlerFunc

	mu  sync.Mutex
	log []string // "<METHOD> <service>/<path>"
}

func newFakeCluster(t *testing.T, routes map[string]http.HandlerFunc) *fakeCluster {
	t.Helper()
	f := &fakeCluster{t: t, routes: routes}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeCluster) serve(w http.ResponseWriter, r *http.Request) {
	key := r.Method + " " + strings.TrimPrefix(r.URL.Path, "/api/")
	f.mu.Lock()
	f.log = append(f.log, key)
	f.mu.Unlock()
	if h, ok := f.routes[key]; ok {
		h(w, r)
		return
	}
	problem(w, http.StatusNotFound, "not-found")
}

// client is a sim.Client whose gateway is the fake, with the storefront keys the day sets.
func (f *fakeCluster) client() *sim.Client {
	base := sim.GatewayBase(f.srv.URL+"/api", retailday.AllGateway())
	base[retailday.BFF] = f.srv.URL + "/api/storefront"
	base["storefront"] = f.srv.URL + "/api/storefront"
	return sim.NewClient(base, nil)
}

func (f *fakeCluster) requests() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.log...)
}

// writes are the requests that are not GETs.
func (f *fakeCluster) writes() []string {
	var out []string
	for _, r := range f.requests() {
		if !strings.HasPrefix(r, "GET ") {
			out = append(out, r)
		}
	}
	return out
}

func problem(w http.ResponseWriter, status int, slug string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"type": "https://errors.example/" + slug, "title": slug, "status": status})
}

func respondJSON(v any) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(v)
	}
}

func page(items ...any) map[string]any {
	if items == nil {
		items = []any{}
	}
	return map[string]any{"items": items}
}
