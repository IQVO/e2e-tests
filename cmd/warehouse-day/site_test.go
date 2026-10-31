package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// committedFixture is the storefront's fulfilment-site fixture, relative to this package.
var committedFixture = filepath.Join("..", "..", "fixtures", "storefront", "fulfilment-site.json")

func liveSite(id string) func() (string, error) {
	return func() (string, error) { return id, nil }
}

func TestStorefrontSiteIDReadsTheCommittedFixture(t *testing.T) {
	got, err := storefrontSiteID(committedFixture)
	if err != nil {
		t.Fatal(err)
	}
	if got != "site-1" {
		t.Fatalf("storefront fulfilment site = %q, want site-1", got)
	}
	if _, err := storefrontSiteID(filepath.Join(t.TempDir(), "absent.json")); err == nil {
		t.Fatal("an absent fixture must be an error, not an empty site")
	}
}

// TestDefaultsRunOnTheSimSite: with nothing set, the CPT schedule goes to the sim's own site,
// never to order-management's DEFAULT_SITE_ID (even when that variable is exported).
func TestDefaultsRunOnTheSimSite(t *testing.T) {
	t.Setenv("DAY_SITE", "")
	t.Setenv("DAY_CPT_SITE", "")
	t.Setenv("DAY_STOREFRONT_SITE_FIXTURE", "")
	t.Setenv("DEFAULT_SITE_ID", "site-1")
	cfg := loadConfig()
	if cfg.siteCode != "SIM1" || cfg.cptSiteID != "SIM1" {
		t.Fatalf("defaults: -site %q -cpt-site %q, want SIM1 for both", cfg.siteCode, cfg.cptSiteID)
	}
	if cfg.allowLiveSite {
		t.Fatal("-allow-live-site must default to false")
	}
	if cfg.storefrontSiteFixture != defaultStorefrontSiteFixture {
		t.Fatalf("fixture default %q, want %q", cfg.storefrontSiteFixture, defaultStorefrontSiteFixture)
	}
	cfg.storefrontSiteFixture = committedFixture
	warnings, err := siteGuard(cfg, func() (string, error) { return storefrontSiteID(cfg.storefrontSiteFixture) })
	if err != nil || len(warnings) != 0 {
		t.Fatalf("the defaults must pass the guard silently against the committed fixture: warnings %v, err %v", warnings, err)
	}
}

func TestSiteGuard(t *testing.T) {
	cfgWith := func(site, cpt string, allow bool) config {
		return config{siteCode: site, cptSiteID: cpt, allowLiveSite: allow, storefrontSiteFixture: defaultStorefrontSiteFixture}
	}
	unreadable := func() (string, error) { return "", errors.New("no such file") }
	cases := []struct {
		name         string
		cfg          config
		live         func() (string, error)
		refuse       bool
		errHas       []string
		wantWarnings int
	}{
		{name: "own site", cfg: cfgWith("SIM1", "SIM1", false), live: liveSite("site-1")},
		{name: "cpt-site is the storefront site", cfg: cfgWith("SIM1", "site-1", false), live: liveSite("site-1"), refuse: true,
			errHas: []string{`-cpt-site "site-1" is the storefront's fulfilment site`, "THROUGHPUT_CONSTRAINED 0", "-allow-live-site"}},
		{name: "storefront site in another case", cfg: cfgWith("SIM1", "SITE-1", false), live: liveSite("site-1"), refuse: true,
			errHas: []string{"-cpt-site"}},
		{name: "order site is the storefront site", cfg: cfgWith("WH1", "SIM1", false), live: liveSite("WH1"), refuse: true,
			errHas: []string{`-site "WH1" is the storefront's fulfilment site`}, wantWarnings: 0},
		{name: "explicit allow-live-site", cfg: cfgWith("SIM1", "site-1", true), live: liveSite("site-1"), wantWarnings: 2},
		{name: "fixture unreadable fails closed", cfg: cfgWith("SIM1", "SIM1", false), live: unreadable, refuse: true,
			errHas: []string{"cannot read the storefront's fulfilment site", "-storefront-site-fixture", "-allow-live-site"}},
		{name: "allow-live-site needs no fixture", cfg: cfgWith("SIM1", "SIM1", true), live: unreadable, wantWarnings: 1},
		{name: "site that order-management rejects", cfg: cfgWith("sim-1", "SIM1", true), live: liveSite("site-1"), refuse: true,
			errHas: []string{"invalid-fulfillment-site-id"}},
		{name: "empty cpt-site", cfg: cfgWith("SIM1", " ", false), live: liveSite("site-1"), refuse: true, errHas: []string{"-cpt-site is empty"}},
		{name: "cpt-site differs from the order site", cfg: cfgWith("SIM1", "SIM2", false), live: liveSite("site-1"), wantWarnings: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			warnings, err := siteGuard(tc.cfg, tc.live)
			if tc.refuse != (err != nil) {
				t.Fatalf("refuse = %v, want %v (err %v)", err != nil, tc.refuse, err)
			}
			for _, s := range tc.errHas {
				if !strings.Contains(err.Error(), s) {
					t.Errorf("error %q does not say %q", err, s)
				}
			}
			if len(warnings) != tc.wantWarnings {
				t.Errorf("warnings %v, want %d", warnings, tc.wantWarnings)
			}
		})
	}
}

// fakeServices records every request the simulator sends through the gateway.
type fakeServices struct {
	mu     sync.Mutex
	method []string
	path   []string
	body   []map[string]any
	reply  func(r *http.Request) (int, string)
}

func (f *fakeServices) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	f.mu.Lock()
	f.method, f.path, f.body = append(f.method, r.Method), append(f.path, r.URL.Path), append(f.body, m)
	f.mu.Unlock()
	status, out := f.reply(r)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, out)
}

func simAgainst(t *testing.T, f *fakeServices) *sim {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return newSim(config{gateway: srv.URL, runID: "T1", siteCode: "SIM1", cptSiteID: "SIM1", wesPickPath: "pick", seed: 1})
}

func TestPlaceOrderCarriesTheSimSiteAsFulfillmentSiteId(t *testing.T) {
	for _, tc := range []struct {
		name        string
		echoedSite  string
		wantFinding bool
	}{
		{"kept by order-management", "SIM1", false},
		{"dropped by order-management", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeServices{reply: func(*http.Request) (int, string) {
				b, _ := json.Marshal(map[string]any{"id": "ORD-1", "status": "Released", "fulfillmentSiteId": tc.echoedSite, "lines": []any{}})
				return http.StatusCreated, string(b)
			}}
			s := simAgainst(t, f)
			s.placeOrder(t.Context(), &orderRec{seq: 1, kind: kindStandard, releaseOnAlloc: true, lines: []orderLinePlan{{sku: "MUG", qty: 2}}})

			if len(f.path) != 1 || f.method[0] != http.MethodPost || f.path[0] != "/order-management/orders" {
				t.Fatalf("requests %v %v, want one POST /order-management/orders", f.method, f.path)
			}
			body := f.body[0]
			if body["fulfillmentSiteId"] != "SIM1" {
				t.Fatalf("POST /orders fulfillmentSiteId = %v, want SIM1 (body %v)", body["fulfillmentSiteId"], body)
			}
			lines, _ := body["lines"].([]any)
			if len(lines) != 1 || lines[0].(map[string]any)["sku"] != "SKU-T1-MUG" {
				t.Fatalf("lines = %v, want the run-scoped MUG line", body["lines"])
			}
			if got := len(s.findings) > 0; got != tc.wantFinding {
				t.Fatalf("findings %v, want a finding = %v", s.findings, tc.wantFinding)
			}
		})
	}
}

func TestPublishCPTScheduleWritesTheSimSite(t *testing.T) {
	f := &fakeServices{reply: func(*http.Request) (int, string) { return http.StatusOK, `{}` }}
	s := simAgainst(t, f)
	s.clock = newSimClock(8 * time.Minute)
	if err := s.publishCPTSchedule(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(f.path) != 1 || f.method[0] != http.MethodPut || f.path[0] != "/process-path-management/sites/SIM1/cpt-schedule" {
		t.Fatalf("requests %v %v, want one PUT /process-path-management/sites/SIM1/cpt-schedule", f.method, f.path)
	}
	body := f.body[0]
	cutoffs, _ := body["cutoffs"].([]any)
	if body["timezone"] != "UTC" || len(cutoffs) != len(truckCutoffs) {
		t.Fatalf("schedule %v, want UTC with %d cutoffs", body, len(truckCutoffs))
	}
	seen := map[any]bool{}
	for _, c := range cutoffs {
		seen[c.(map[string]any)["localTime"]] = true
	}
	if len(seen) != len(truckCutoffs) {
		t.Fatalf("cutoff minutes %v are not distinct", seen)
	}
}
