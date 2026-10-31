package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"
)

// The WMS step: the CPT (critical pull time) schedule of the fulfilment site in
// process-path-management. network-fulfillment (SITE_ID site-1) advertises each listing's quantity
// as min(usable stock, process-path capacity before the site's NEXT cutoff of this schedule), and
// the network declares the same node with the cutoffs of the network data file's node profile. A
// schedule that disagrees (a warehouse-day run from before it moved to its own site SIM1, or one
// with -allow-live-site, leaves compressed UTC truck times behind)
// turns every listing THROUGHPUT_CONSTRAINED 0: nothing is in stock in the storefront.

// SiteFixture is fixtures/storefront/fulfilment-site.json: the CPT schedule the fulfilment site
// must carry, in the shape of process-path-management's DefineCPTScheduleRequest plus the site id.
type SiteFixture struct {
	SiteID   string   `json:"siteId"`
	Timezone string   `json:"timezone"`
	Cutoffs  []Cutoff `json:"cutoffs"`
}

// Cutoff is process-path-management's Cutoff (request and response shape).
type Cutoff struct {
	CptID           string   `json:"cptId"`
	LocalTime       string   `json:"localTime"`
	DaysOfWeek      []string `json:"daysOfWeek"`
	ShipMethod      string   `json:"shipMethod"`
	EligiblePathIDs []string `json:"eligiblePathIds"`
}

// CPTSchedule is process-path-management's GET /sites/{siteId}/cpt-schedule.
type CPTSchedule struct {
	SiteID    string   `json:"siteId,omitempty"`
	Timezone  string   `json:"timezone"`
	Cutoffs   []Cutoff `json:"cutoffs"`
	UpdatedAt string   `json:"updatedAt,omitempty"`
}

// DefineCPTScheduleRequest is the body of PUT /sites/{siteId}/cpt-schedule: a wholesale
// replacement (no partial update, no version guard, no Idempotency-Key in the contract).
type DefineCPTScheduleRequest struct {
	Timezone string   `json:"timezone"`
	Cutoffs  []Cutoff `json:"cutoffs"`
}

// NodeProfile is the slice of the network node's operational profile the CPT schedule must agree
// with (warehouse-infra scripts/data/retail-network-seed.json, fulfillmentNodes[].profile).
type NodeProfile struct {
	VendorNodeCode string
	Timezone       string
	CutoffTimes    []string // "HH:MM", sorted
}

// weekdays is process-path-management's closed daysOfWeek enum, in week order.
var weekdays = []string{"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"}

var localTimeRE = regexp.MustCompile(`^([01][0-9]|2[0-3]):[0-5][0-9]$`)

// LoadSiteFixture reads the fulfilment-site fixture.
func LoadSiteFixture(path string) (*SiteFixture, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("fulfilment-site fixture: %w", err)
	}
	var s SiteFixture
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, fmt.Errorf("fulfilment-site fixture %s: %w", path, err)
	}
	return &s, nil
}

// Validate mirrors process-path-management's 422 rules of DefineCPTScheduleRequest (except the
// Active-path check, which CHECK does against the live service).
func (s *SiteFixture) Validate() []string {
	var problems []string
	add := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }
	if strings.TrimSpace(s.SiteID) == "" {
		add("siteId is empty")
	}
	if _, err := time.LoadLocation(s.Timezone); s.Timezone == "" || s.Timezone == "Local" || err != nil {
		add("timezone %q is not an IANA zone", s.Timezone)
	}
	if len(s.Cutoffs) == 0 {
		add("no cutoffs")
	}
	known := map[string]bool{}
	for _, d := range weekdays {
		known[d] = true
	}
	ids := map[string]bool{}
	for i, c := range s.Cutoffs {
		if c.CptID == "" || ids[c.CptID] {
			add("cutoffs[%d]: cptId %q is empty or duplicate", i, c.CptID)
		}
		ids[c.CptID] = true
		if !localTimeRE.MatchString(c.LocalTime) {
			add("cutoffs[%d] %s: localTime %q is not HH:MM", i, c.CptID, c.LocalTime)
		}
		if len(c.DaysOfWeek) == 0 {
			add("cutoffs[%d] %s: no daysOfWeek", i, c.CptID)
		}
		for _, d := range c.DaysOfWeek {
			if !known[d] {
				add("cutoffs[%d] %s: day %q is not one of %v", i, c.CptID, d, weekdays)
			}
		}
		if strings.TrimSpace(c.ShipMethod) == "" {
			add("cutoffs[%d] %s: shipMethod is empty", i, c.CptID)
		}
		if len(c.EligiblePathIDs) == 0 {
			add("cutoffs[%d] %s: no eligiblePathIds", i, c.CptID)
		}
		for _, p := range c.EligiblePathIDs {
			if strings.TrimSpace(p) == "" {
				add("cutoffs[%d] %s: an empty eligiblePathIds entry", i, c.CptID)
			}
		}
	}
	sort.Strings(problems)
	return problems
}

// LoadNodeProfile reads the operational profile of the fulfillment node siteID from the
// retail-network data file (the node's vendorNodeCode IS network-fulfillment's SITE_ID).
func LoadNodeProfile(path, siteID string) (*NodeProfile, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("retail-network data file: %w", err)
	}
	var doc struct {
		FulfillmentNodes []struct {
			VendorNodeCode string `json:"vendorNodeCode"`
			Profile        *struct {
				Timezone string `json:"timezone"`
				Cutoffs  []struct {
					LocalTime string `json:"localTime"`
				} `json:"cutoffs"`
			} `json:"profile"`
		} `json:"fulfillmentNodes"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("retail-network data file %s: %w", path, err)
	}
	for _, n := range doc.FulfillmentNodes {
		if n.VendorNodeCode != siteID {
			continue
		}
		if n.Profile == nil {
			return nil, fmt.Errorf("retail-network data file %s: fulfillment node %s has no profile", path, siteID)
		}
		np := &NodeProfile{VendorNodeCode: siteID, Timezone: n.Profile.Timezone}
		for _, c := range n.Profile.Cutoffs {
			np.CutoffTimes = append(np.CutoffTimes, c.LocalTime)
		}
		sort.Strings(np.CutoffTimes)
		return np, nil
	}
	return nil, fmt.Errorf("retail-network data file %s declares no fulfillment node with vendorNodeCode %q (is warehouse-infra up to date?)", path, siteID)
}

// cutoffTimes is the sorted set of the fixture's cutoff local times.
func (s *SiteFixture) cutoffTimes() []string {
	out := make([]string, 0, len(s.Cutoffs))
	for _, c := range s.Cutoffs {
		out = append(out, c.LocalTime)
	}
	sort.Strings(out)
	return out
}

// AgreeWith reports where the CPT schedule disagrees with the network's node profile: the
// timezone and the set of cutoff local times must be equal.
func (s *SiteFixture) AgreeWith(n *NodeProfile) []string {
	var problems []string
	if s.Timezone != n.Timezone {
		problems = append(problems, fmt.Sprintf("timezone %q, the network node %s declares %q", s.Timezone, n.VendorNodeCode, n.Timezone))
	}
	if got := s.cutoffTimes(); strings.Join(got, ",") != strings.Join(n.CutoffTimes, ",") {
		problems = append(problems, fmt.Sprintf("cutoffs %v, the network node %s declares %v", got, n.VendorNodeCode, n.CutoffTimes))
	}
	return problems
}

// Request is the PUT body of the fixture's schedule, normalised (see normalCutoffs).
func (s *SiteFixture) Request() DefineCPTScheduleRequest {
	return DefineCPTScheduleRequest{Timezone: s.Timezone, Cutoffs: normalCutoffs(s.Cutoffs)}
}

// normalCutoffs sorts the cutoffs by cptId, their days in week order and their path ids: the
// schedule's meaning does not depend on any of these orders.
func normalCutoffs(cs []Cutoff) []Cutoff {
	rank := map[string]int{}
	for i, d := range weekdays {
		rank[d] = i
	}
	out := make([]Cutoff, 0, len(cs))
	for _, c := range cs {
		n := c
		n.DaysOfWeek = append([]string(nil), c.DaysOfWeek...)
		sort.SliceStable(n.DaysOfWeek, func(i, j int) bool { return rank[n.DaysOfWeek[i]] < rank[n.DaysOfWeek[j]] })
		n.EligiblePathIDs = append([]string(nil), c.EligiblePathIDs...)
		sort.Strings(n.EligiblePathIDs)
		out = append(out, n)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].CptID < out[j].CptID })
	return out
}

// sameSchedule is true when the observed schedule already has the wanted timezone and cutoffs.
func sameSchedule(o *CPTSchedule, want DefineCPTScheduleRequest) bool {
	if o == nil || o.Timezone != want.Timezone {
		return false
	}
	a, b := normalCutoffs(o.Cutoffs), normalCutoffs(want.Cutoffs)
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].CptID != b[i].CptID || a[i].LocalTime != b[i].LocalTime || a[i].ShipMethod != b[i].ShipMethod ||
			strings.Join(a[i].DaysOfWeek, ",") != strings.Join(b[i].DaysOfWeek, ",") ||
			strings.Join(a[i].EligiblePathIDs, ",") != strings.Join(b[i].EligiblePathIDs, ",") {
			return false
		}
	}
	return true
}

// describeSchedule renders a schedule on one line: "America/Chicago [aus-1100 11:00, aus-1700 17:00]".
func describeSchedule(tz string, cs []Cutoff) string {
	var parts []string
	for _, c := range normalCutoffs(cs) {
		parts = append(parts, c.CptID+" "+c.LocalTime)
	}
	return tz + " [" + strings.Join(parts, ", ") + "]"
}

// ppm is process-path-management behind Kong.
func (c *client) ppm() string { return c.base + "/process-path-management" }

// planCPT puts the CPT schedule into the plan: nothing when the observed one already has the
// fixture's timezone and cutoffs.
func planCPT(p *Plan, site *SiteFixture, observed *CPTSchedule) {
	p.CPTSite, p.CPTCurrent = site.SiteID, "none"
	if observed != nil {
		p.CPTCurrent = describeSchedule(observed.Timezone, observed.Cutoffs)
	}
	if want := site.Request(); !sameSchedule(observed, want) {
		p.CPTSchedule = &want
	}
}

// runWMSOnly is the -wms-only run (scripts/10 step 2, before the network tier, whose verify needs
// listings with stock): PLAN, APPLY and VERIFY of the CPT schedule alone. The capability offers
// are a diagnostic: they never fail it.
func runWMSOnly(ctx context.Context, c *client, cfg Config, skus []string, site *SiteFixture, observed *CPTSchedule, res *Result, logf func(string, ...any)) (*Result, error) {
	plan := &Plan{WMSOnly: true}
	planCPT(plan, site, observed)
	res.Plan = plan
	logf("plan:")
	for _, l := range plan.Summary() {
		logf("  %s", l)
	}
	if cfg.DryRun {
		logf("dry run: nothing written")
		return res, nil
	}
	var written time.Time
	if !cfg.VerifyOnly {
		start := time.Now()
		var err error
		if plan.CPTSchedule != nil {
			if err = applyCPT(ctx, c, site.SiteID, *plan.CPTSchedule); err == nil {
				written = time.Now()
				logf("apply: wms: CPT schedule of %s defined: %s", site.SiteID, describeSchedule(plan.CPTSchedule.Timezone, plan.CPTSchedule.Cutoffs))
			}
		}
		res.Writes, res.WritesByKey = c.totalWrites(), c.writes
		logf("apply: %d writes in %s (cpt-schedule %d)", res.Writes, time.Since(start).Round(time.Millisecond), c.writes["cpt-schedule"])
		if err != nil {
			return res, fmt.Errorf("apply: define the CPT schedule of %s: %w (re-run: the PUT is a wholesale replacement)", site.SiteID, err)
		}
	}
	offers := verifyOffers(ctx, c, site.SiteID, skus, written, cfg)
	logf("%s", offers.Line())
	for _, w := range offers.Warnings() {
		logf("  WARNING %s", w)
	}
	logf("verify: PASS (wms: the capability offers are reported, never a failure)")
	return res, nil
}

// readActivePaths lists process-path-management's Active process paths (GET /process-paths
// returns only Active paths by default, as a bare array).
func readActivePaths(ctx context.Context, c *client) (map[string]bool, error) {
	var paths []struct {
		PathID string `json:"pathId"`
		Status string `json:"status"`
	}
	if err := c.get(ctx, c.ppm()+"/process-paths", &paths); err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for _, p := range paths {
		if p.Status == "" || strings.EqualFold(p.Status, "ACTIVE") {
			out[p.PathID] = true
		}
	}
	return out, nil
}

// observeCPT reads the site's current CPT schedule; nil when none is defined (404).
func observeCPT(ctx context.Context, c *client, siteID string) (*CPTSchedule, error) {
	var s CPTSchedule
	switch err := c.get(ctx, c.ppm()+"/sites/"+url.PathEscape(siteID)+"/cpt-schedule", &s); {
	case err == nil:
		return &s, nil
	case isStatus(err, http.StatusNotFound):
		return nil, nil
	default:
		return nil, err
	}
}

// applyCPT defines (wholesale-replaces) the site's CPT schedule.
func applyCPT(ctx context.Context, c *client, siteID string, req DefineCPTScheduleRequest) error {
	return c.put(ctx, "cpt-schedule", "/process-path-management/sites/"+url.PathEscape(siteID)+"/cpt-schedule", req, nil, http.StatusOK)
}

// ---- VERIFY: network-fulfillment's capability offers ----

// CapabilityOffer is network-fulfillment's GET /capability-offers entry.
type CapabilityOffer struct {
	SKU                string    `json:"sku"`
	SiteID             string    `json:"siteId"`
	AdvertisedQuantity int       `json:"advertisedQuantity"`
	Basis              string    `json:"basis"`
	ComputedAt         time.Time `json:"computedAt"`
}

// OfferReport is what VERIFY saw of the storefront SKUs' capability offers at the site.
type OfferReport struct {
	SiteID      string
	SKUs        int
	Advertising int      // basis PHYSICAL or a non-zero advertised quantity
	Constrained []string // THROUGHPUT_CONSTRAINED with 0 advertised
	Missing     []string // no offer for the SKU at the site
	Elapsed     time.Duration
	Written     bool // the CPT schedule was (re)defined in this run
	Err         error
}

// Line renders the report on one line.
func (r *OfferReport) Line() string {
	return fmt.Sprintf("verify: network-fulfillment capability offers at %s: %d/%d storefront SKUs advertise stock (basis PHYSICAL or quantity > 0), %d THROUGHPUT_CONSTRAINED 0, %d without an offer (%s)",
		r.SiteID, r.Advertising, r.SKUs, len(r.Constrained), len(r.Missing), r.Elapsed.Round(100*time.Millisecond))
}

// Warnings explains a report that is not all-advertising. They never fail the run: the capability
// offer is a diagnostic of the purchasable check, which does fail.
func (r *OfferReport) Warnings() []string {
	var out []string
	if r.Err != nil {
		out = append(out, fmt.Sprintf("network-fulfillment capability offers could not be read: %v", r.Err))
	}
	if n := len(r.Constrained); n > 0 {
		when := "the schedule already matched the fixture, so nothing was rewritten"
		if r.Written {
			when = fmt.Sprintf("after the CPT schedule was redefined and %s of recompute (the ACL recomputes every minute)", r.Elapsed.Round(time.Second))
		}
		out = append(out, fmt.Sprintf("%d of %d storefront SKUs stay THROUGHPUT_CONSTRAINED 0 at %s (%s; e.g. %s): network-fulfillment caps the advertised quantity at the eligible paths' capacity before the site's NEXT CPT cutoff. With the schedule correct, the remaining pre-cutoff gap is removed by the next-feasible-cutoff fix in network-fulfillment (separate PR, branch fix/next-feasible-cutoff), not by this seeder; until then those products show out of stock. See docs/storefront-provisioning.md, Troubleshooting",
			n, r.SKUs, r.SiteID, when, strings.Join(firstN(r.Constrained, 3), ", ")))
	}
	if n := len(r.Missing); n > 0 {
		out = append(out, fmt.Sprintf("%d storefront SKUs have no capability offer at %s yet (e.g. %s): the ACL only offers SKUs with WMS stock it has seen", n, r.SiteID, strings.Join(firstN(r.Missing, 3), ", ")))
	}
	return out
}

func firstN(s []string, n int) []string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// verifyOffers reads network-fulfillment's capability offers of the storefront SKUs at the site.
// When the schedule was (re)defined at `written`, it polls within the verify timeout until every
// SKU advertises stock, or until every offer was recomputed at least one recompute interval after
// the write (the new schedule has certainly been applied: waiting longer changes nothing).
// Otherwise one read.
func verifyOffers(ctx context.Context, c *client, siteID string, skus []string, written time.Time, cfg Config) *OfferReport {
	r := &OfferReport{SiteID: siteID, SKUs: len(skus), Written: !written.IsZero()}
	recompute := cfg.OfferRecomputeInterval
	if recompute <= 0 {
		recompute = time.Minute
	}
	interval := cfg.VerifyInterval
	if interval <= 0 {
		interval = time.Second
	}
	start := time.Now()
	for {
		var page struct {
			CapabilityOffers []CapabilityOffer `json:"capabilityOffers"`
		}
		err := c.get(ctx, c.base+"/network-fulfillment/capability-offers", &page)
		r.Err = err
		settled := false
		if err == nil {
			by := map[string]CapabilityOffer{}
			for _, o := range page.CapabilityOffers {
				if o.SiteID == siteID {
					by[o.SKU] = o
				}
			}
			r.Advertising, r.Constrained, r.Missing = 0, nil, nil
			settled = true
			for _, sku := range skus {
				o, ok := by[sku]
				switch {
				case !ok:
					r.Missing = append(r.Missing, sku)
					settled = false
				case o.Basis == "PHYSICAL" || o.AdvertisedQuantity > 0:
					r.Advertising++
				default:
					r.Constrained = append(r.Constrained, sku)
				}
				if ok && r.Written && o.ComputedAt.Before(written.Add(recompute)) {
					settled = false
				}
			}
		}
		r.Elapsed = time.Since(start)
		done := err == nil && r.Advertising == len(skus)
		if done || !r.Written || (err == nil && settled) || r.Elapsed >= cfg.VerifyTimeout || ctx.Err() != nil {
			return r
		}
		select {
		case <-ctx.Done():
			return r
		case <-time.After(interval):
		}
	}
}
