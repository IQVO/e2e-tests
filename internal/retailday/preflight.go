package retailday

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/sim"
)

// Check is one preflight finding. A failed check carries the command or action
// that fixes it, so the operator never has to guess.
type Check struct {
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail"`
	Fix    string `json:"fix,omitempty"`
}

// Config is what the preflight needs to know about the run.
type Config struct {
	// NetworkSite is the id the network knows the fulfilment node by (site-1);
	// BuildingSite the facility-layout site that serves it (AUS3): ADR 0001 D1.
	NetworkSite, BuildingSite string
	VendorID                  string
	// MinProducts is how many purchasable products the BFF must list.
	MinProducts int
	// RequireWindow turns "not inside the release window" into a failure; off,
	// it is reported and the run decides what to do (ADR 0001 D3).
	RequireWindow bool
	// Handling overrides the node's declared handling time (0 = read it from
	// the node); AckBudget is order-sourcing's constant 30 minutes.
	Handling, AckBudget time.Duration
	Now                 func() time.Time
}

func (c Config) withDefaults() Config {
	if c.VendorID == "" {
		c.VendorID = "V-WS"
	}
	if c.AckBudget == 0 {
		c.AckBudget = 30 * time.Minute
	}
	if c.MinProducts == 0 {
		c.MinProducts = 10
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	return c
}

// Preflight reads (never writes) what a run needs and reports every finding.
// It fails closed: an unreadable answer is a failed check, not a skipped one.
func Preflight(ctx context.Context, cl *sim.Client, cfg Config) []Check {
	cfg = cfg.withDefaults()
	var out []Check
	out = append(out, healthChecks(ctx, cl)...)

	out = append(out, networkFulfillmentLive(ctx, cl))
	node, nodeCheck := fulfilmentNode(ctx, cl, cfg)
	out = append(out, nodeCheck)

	netSched, c1 := cptSchedule(ctx, cl, cfg.NetworkSite, "network site")
	bldSched, c2 := cptSchedule(ctx, cl, cfg.BuildingSite, "building site")
	out = append(out, c1, c2, schedulesAgree(cfg, netSched, bldSched), buildingExists(ctx, cl, cfg.BuildingSite))
	out = append(out, windowCheck(cfg, node, bldSched))
	out = append(out, shelfCheck(ctx, cl, cfg))
	return out
}

// Failed counts the checks that did not pass.
func Failed(cs []Check) int {
	n := 0
	for _, c := range cs {
		if !c.OK {
			n++
		}
	}
	return n
}

func healthChecks(ctx context.Context, cl *sim.Client) []Check {
	names := AllGateway()
	res := make([]Check, len(names))
	var wg sync.WaitGroup
	sem := make(chan struct{}, 8)
	for i, n := range names {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			r := cl.Call(ctx, n, http.MethodGet, "GET /healthz", "/healthz", nil)
			c := Check{Name: "health " + n, OK: r.OK(), Detail: fmt.Sprintf("GET /api/%s/healthz -> %d", n, r.Status)}
			if !c.OK {
				c.Detail += " " + sim.Truncate(string(r.Body), 120)
				c.Fix = "kubectl -n warehouse-systems get pods | grep " + n + " ; read its log; a CrashLoopBackOff across many contexts usually means the Kafka broker (kafka-controller-0) is down"
			}
			res[i] = c
		}()
	}
	wg.Wait()
	return res
}

type inboundStatus struct {
	NetworkMode string `json:"networkMode"`
	Failed      int    `json:"failed"`
	Overdue     int    `json:"overdue"`
}

func networkFulfillmentLive(ctx context.Context, cl *sim.Client) Check {
	r := cl.Call(ctx, "network-fulfillment", http.MethodGet, "GET /inbound-status", "/inbound-status", nil)
	c := Check{Name: "network-fulfillment is live"}
	var st inboundStatus
	if !r.OK() || r.Decode(&st) != nil {
		c.Detail = fmt.Sprintf("GET /inbound-status -> %d %s", r.Status, sim.Truncate(string(r.Body), 120))
		c.Fix = "network-fulfillment must answer; see the health check above"
		return c
	}
	c.OK = st.NetworkMode == "live"
	c.Detail = fmt.Sprintf("networkMode=%s failed=%d overdue=%d", st.NetworkMode, st.Failed, st.Overdue)
	if !c.OK {
		c.Fix = "a stub network never sees a purchase order: set networkMode live in warehouse-infra helm-values/network-fulfillment.yaml (merged flip PR #139)"
	}
	return c
}

// FulfilmentNode is the slice of the vendor node the run needs.
type FulfilmentNode struct {
	NodeID         string `json:"nodeId"`
	VendorNodeCode string `json:"vendorNodeCode"`
	Status         string `json:"status"`
	Timezone       string `json:"timezone"`
	HandlingMin    int    `json:"handlingTimeMinutes"`
	ProfileRev     int    `json:"profileRevision"`
}

func fulfilmentNode(ctx context.Context, cl *sim.Client, cfg Config) (*FulfilmentNode, Check) {
	c := Check{Name: "fulfilment node " + cfg.NetworkSite + " is ACTIVE with a profile"}
	path := "/vendor/nodes?vendorId=" + url.QueryEscape(cfg.VendorID)
	nodes, last := sim.ListAll[FulfilmentNode](ctx, cl, "vendor-connectivity", "GET /vendor/nodes", path)
	if !last.OK() {
		c.Detail = fmt.Sprintf("GET %s -> %d %s", path, last.Status, sim.Truncate(string(last.Body), 120))
		c.Fix = "vendor-connectivity must answer and vendor " + cfg.VendorID + " must exist: make seed-network in warehouse-infra"
		return nil, c
	}
	for i := range nodes {
		n := nodes[i]
		if n.VendorNodeCode != cfg.NetworkSite {
			continue
		}
		c.OK = n.Status == "ACTIVE" && n.ProfileRev > 0 && n.HandlingMin > 0
		c.Detail = fmt.Sprintf("node %s status=%s profileRevision=%d handling=%dmin timezone=%s", n.NodeID, n.Status, n.ProfileRev, n.HandlingMin, n.Timezone)
		if !c.OK {
			c.Fix = "make seed-network (warehouse-infra) activates the node and declares its profile"
		}
		return &n, c
	}
	c.Detail = fmt.Sprintf("vendor %s has %d node(s), none with vendorNodeCode %s", cfg.VendorID, len(nodes), cfg.NetworkSite)
	c.Fix = "make seed-network (warehouse-infra)"
	return nil, c
}

// Schedule is process-path-management's GET /sites/{siteId}/cpt-schedule.
type Schedule struct {
	SiteID   string `json:"siteId"`
	Timezone string `json:"timezone"`
	Cutoffs  []struct {
		CptID           string   `json:"cptId"`
		LocalTime       string   `json:"localTime"`
		DaysOfWeek      []string `json:"daysOfWeek"`
		ShipMethod      string   `json:"shipMethod"`
		EligiblePathIDs []string `json:"eligiblePathIds"`
	} `json:"cutoffs"`
}

func cptSchedule(ctx context.Context, cl *sim.Client, site, role string) (*Schedule, Check) {
	c := Check{Name: fmt.Sprintf("CPT schedule of the %s %s", role, site)}
	r := cl.Call(ctx, "process-path-management", http.MethodGet, "GET /sites/{siteId}/cpt-schedule", "/sites/"+url.PathEscape(site)+"/cpt-schedule", nil)
	var s Schedule
	if !r.OK() || r.Decode(&s) != nil || len(s.Cutoffs) == 0 {
		c.Detail = fmt.Sprintf("GET /sites/%s/cpt-schedule -> %d %s", site, r.Status, sim.Truncate(string(r.Body), 120))
		c.Fix = "make provision-storefront defines the schedule (cmd/seed-storefront, WMS step)"
		return nil, c
	}
	c.OK = true
	c.Detail = fmt.Sprintf("%d cutoff(s) in %s", len(s.Cutoffs), s.Timezone)
	return &s, c
}

// signature is the order-insensitive content of a schedule.
func (s *Schedule) signature() string {
	rows := make([]string, 0, len(s.Cutoffs))
	for _, c := range s.Cutoffs {
		d := append([]string(nil), c.DaysOfWeek...)
		p := append([]string(nil), c.EligiblePathIDs...)
		sort.Strings(d)
		sort.Strings(p)
		rows = append(rows, fmt.Sprintf("%s@%s[%s]%s{%s}", c.CptID, c.LocalTime, strings.Join(d, ","), c.ShipMethod, strings.Join(p, ",")))
	}
	sort.Strings(rows)
	return s.Timezone + "|" + strings.Join(rows, ";")
}

func schedulesAgree(cfg Config, net, bld *Schedule) Check {
	c := Check{Name: fmt.Sprintf("CPT schedules of %s and %s agree", cfg.NetworkSite, cfg.BuildingSite)}
	if net == nil || bld == nil {
		c.Detail = "a schedule is missing (see the checks above)"
		c.Fix = "make provision-storefront"
		return c
	}
	c.OK = net.signature() == bld.signature()
	if c.OK {
		c.Detail = "identical cutoffs, days, ship methods, paths and timezone"
		return c
	}
	c.Detail = fmt.Sprintf("%s: %s  /  %s: %s", cfg.NetworkSite, net.signature(), cfg.BuildingSite, bld.signature())
	c.Fix = "order-management promises on the building's schedule and the network on the node's: they must be one (ADR 0001 D1); make provision-storefront restores both"
	return c
}

func buildingExists(ctx context.Context, cl *sim.Client, site string) Check {
	c := Check{Name: "facility-layout knows the building " + site}
	r := cl.Call(ctx, "facility-layout", http.MethodGet, "GET /sites/{siteCode}", "/sites/"+url.PathEscape(site), nil)
	c.OK = r.OK()
	c.Detail = fmt.Sprintf("GET /sites/%s -> %d", site, r.Status)
	if !c.OK {
		c.Fix = "the provisioning repo creates the pilot sites: ~/warehouse-systems/provisioning run_all.sh"
	}
	return c
}

// resolveWindow computes the release window from the building's schedule (its
// latest cutoff and the days it runs) and the node's handling time.
func resolveWindow(cfg Config, node *FulfilmentNode, bld *Schedule) (Window, time.Duration, string, error) {
	if bld == nil {
		return Window{}, 0, "", fmt.Errorf("the building's schedule is unreadable; the window cannot be computed")
	}
	latest, ok := LatestCutoff(cutoffsOf(bld))
	if !ok {
		return Window{}, 0, "", fmt.Errorf("the building's schedule has no cutoff")
	}
	loc, err := time.LoadLocation(bld.Timezone)
	if err != nil {
		return Window{}, 0, "", fmt.Errorf("timezone %q: %w", bld.Timezone, err)
	}
	handling, source := cfg.Handling, "flag"
	if handling == 0 {
		if node == nil || node.HandlingMin == 0 {
			return Window{}, 0, "", fmt.Errorf("the node's handling time is unknown (node check failed) and no override was given")
		}
		handling, source = time.Duration(node.HandlingMin)*time.Minute, "node profile"
	}
	w, err := ReleaseWindow(cfg.Now(), loc, latest.LocalTime, latest.DaysOfWeek, handling, cfg.AckBudget)
	if err != nil {
		return Window{}, 0, "", err
	}
	return w, handling, fmt.Sprintf("pickup %s, handling %s from the %s, ack budget %s", latest.LocalTime, handling, source, cfg.AckBudget), nil
}

// NextWindow reads the node and the building's schedule and returns the release
// window at or after now.
func NextWindow(ctx context.Context, cl *sim.Client, cfg Config) (Window, error) {
	cfg = cfg.withDefaults()
	node, _ := fulfilmentNode(ctx, cl, cfg)
	bld, _ := cptSchedule(ctx, cl, cfg.BuildingSite, "building site")
	w, _, _, err := resolveWindow(cfg, node, bld)
	return w, err
}

func windowCheck(cfg Config, node *FulfilmentNode, bld *Schedule) Check {
	c := Check{Name: "release window of the next pickup"}
	w, _, basis, err := resolveWindow(cfg, node, bld)
	if err != nil {
		c.Detail = err.Error()
		return c
	}
	c.OK = w.InWindow || !cfg.RequireWindow
	c.Detail = fmt.Sprintf("%s (%s)", w, basis)
	if !c.OK {
		c.Fix = "start the run inside the window, or pass -wait-for-window; orders placed earlier are released at the window's start (ADR 0001 D3)"
	}
	return c
}

func cutoffsOf(s *Schedule) []Cutoff {
	out := make([]Cutoff, 0, len(s.Cutoffs))
	for _, c := range s.Cutoffs {
		out = append(out, Cutoff{LocalTime: c.LocalTime, DaysOfWeek: c.DaysOfWeek})
	}
	return out
}

func shelfCheck(ctx context.Context, cl *sim.Client, cfg Config) Check {
	c := Check{Name: fmt.Sprintf("the storefront lists at least %d purchasable products", cfg.MinProducts)}
	r := cl.Call(ctx, BFF, http.MethodGet, "GET /products", "/products?inStock=true", nil)
	var page struct {
		Items []struct {
			Purchasable *bool `json:"purchasable"`
		} `json:"items"`
	}
	if !r.OK() || r.Decode(&page) != nil {
		c.Detail = fmt.Sprintf("GET /products -> %d %s", r.Status, sim.Truncate(string(r.Body), 120))
		c.Fix = "STOREFRONT_BASE_URL must point at the BFF (http://storefront.localhost/api/storefront through k3s-edges.sh); make provision-storefront-verify"
		return c
	}
	n := 0
	for _, it := range page.Items {
		if it.Purchasable == nil || *it.Purchasable {
			n++
		}
	}
	c.OK = n >= cfg.MinProducts
	c.Detail = fmt.Sprintf("%d purchasable in the first page", n)
	if !c.OK {
		c.Fix = "make provision-storefront-verify names what is missing (stock, price, content); restock through inbound-receiving"
	}
	return c
}
