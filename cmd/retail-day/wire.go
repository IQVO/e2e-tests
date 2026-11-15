package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/retailday/collect"
	"github.com/claudioed/warehouse-e2e/internal/retailday/day"
	"github.com/claudioed/warehouse-e2e/internal/retailday/link"
	"github.com/claudioed/warehouse-e2e/internal/sim"
	"github.com/claudioed/warehouse-e2e/internal/sim/floor"
	"github.com/claudioed/warehouse-e2e/internal/sim/inbound"
	"github.com/claudioed/warehouse-e2e/internal/sim/outbound"
	"github.com/claudioed/warehouse-e2e/internal/sim/transport"
)

// wireConfig is everything buildPhases needs to put the real actors together.
type wireConfig struct {
	// Client is the day's one client: every actor, the linker and the collector send their requests
	// through it, so its CallStats see every call.
	Client *sim.Client
	// RunID is short, upper-case and run-unique: it namespaces stations, plates, seals and ASNs.
	RunID string

	// Building is the facility-layout site that serves the storefront (AUS3), NetworkSite the
	// network's id of it (site-1), VendorID the vendor that owns the node.
	Building, NetworkSite, VendorID string

	// FixtureDir holds catalogue.json and fulfilment-site.json; NetworkData is warehouse-infra's
	// scripts/data/retail-network-seed.json (SKU -> catalogue title).
	FixtureDir, NetworkData string

	LinkTimeout time.Duration
	Parallel    int

	// Provision lets the readiness steps WRITE setup data (see building.prepare). Off by default.
	Provision bool
	// Doors and Staging are facility-layout location codes Provision may register when the building
	// lacks them; empty means "use the Active slots the building has". StowBins are the
	// inventory-storage bins the restock may stow into; empty means "discover them, read-only".
	Doors, Staging, StowBins []string

	// Log is the run's progress line (stderr); ActorLog the actors' own chatter (nil unless -v).
	Log      func(format string, args ...any)
	ActorLog sim.LogFunc
}

func (c wireConfig) logf(format string, args ...any) {
	if c.Log != nil {
		c.Log(format, args...)
	}
}

// buildPhases puts the day together. It is a variable so main_test.go can play the day with stub
// phases. The returned cleanup releases what buildPhases started (the host-suspension watch); the
// undoing of what Prepare did is Phases.Cleanup, which the runner always calls.
var buildPhases = newPhases

// dayHTTPClient is the client of the day: a 30 s timeout (the BFF's confirm budget is 15 s, so the
// sim package's 15 s default could cut a slow confirmation off) and a connection pool wide enough for
// the parallel actors, so a run does not open a socket per request and exhaust the host's ephemeral ports.
func dayHTTPClient() *http.Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConns, tr.MaxIdleConnsPerHost, tr.IdleConnTimeout = 128, 64, 90*time.Second
	return &http.Client{Transport: tr, Timeout: 30 * time.Second}
}

func errNotReadied(what string) error {
	return fmt.Errorf("the %s was never readied: the prepare phase did not complete", what)
}

// newPhases reads, writes nothing. Everything it needs from the cluster it needs as a read: the
// SKU dictionary (offer-availability GET /catalog). A fixture or dictionary that does not hold, or a
// cluster that cannot be read, is an error here, before the day has touched anything.
func newPhases(ctx context.Context, cfg wireConfig) (day.Phases, func(), error) {
	if cfg.Client == nil {
		return day.Phases{}, nil, errors.New("wire: no client")
	}
	site, err := loadSiteFixture(filepath.Join(cfg.FixtureDir, "fulfilment-site.json"))
	if err != nil {
		return day.Phases{}, nil, err
	}
	if site.WarehouseSiteID != cfg.Building || (site.SiteID != "" && site.SiteID != cfg.NetworkSite) {
		return day.Phases{}, nil, fmt.Errorf("the fulfilment-site fixture serves %s from %s, the run was given -building %s and -network-site %s", site.SiteID, site.WarehouseSiteID, cfg.Building, cfg.NetworkSite)
	}
	dict, err := loadDictionary(ctx, cfg.Client, filepath.Join(cfg.FixtureDir, "catalogue.json"), cfg.NetworkData)
	if err != nil {
		return day.Phases{}, nil, err
	}
	cfg.logf("dictionary: %d SKU(s) paired with a catalogue item, %d without", len(dict.ItemBySKU), len(dict.Unresolved))
	for _, u := range dict.Unresolved {
		cfg.logf("dictionary: unresolved %s", u)
	}

	prefixes := &orderPrefixes{}
	nodes := &nodeBook{c: cfg.Client, vendorID: cfg.VendorID, networkSite: cfg.NetworkSite, prefixes: prefixes}
	fl := floor.New(cfg.Client, nil, cfg.ActorLog, cfg.RunID, floorSettings(cfg))
	tr := transport.New(cfg.Client, transport.SystemClock{}, cfg.ActorLog, transport.Settings{})
	lk := link.New(cfg.Client, linkSettings(cfg))
	ev := collect.New(cfg.Client, collectSettings(cfg, dict, lk.ForPurchaseOrders))

	watch := &sim.SuspendWatch{Log: cfg.ActorLog}
	wctx, stop := context.WithCancel(ctx)
	go watch.Run(wctx)

	b := &building{
		cfg: cfg, site: site, nodes: nodes, fl: fl,
		provisionOutbound: outbound.Provision,
		readyInbound:      inbound.Ready,
		listStorageBins:   func(ctx context.Context) ([]string, error) { return storageBins(ctx, cfg.Client, cfg.Building) },
	}
	return day.Phases{
		Catalogue: catalogueAdapter{list: newCatalogueLister(cfg.Client), dict: dict.SKUByItem},
		Shopper:   &shopperAdapter{runID: cfg.RunID, newBuyer: newShopperFactory(cfg.Client, cfg.ActorLog), orders: prefixes},
		Linker:    lk,
		Floor:     floorAdapter{p: fl},
		Shipper:   shipperAdapter{get: b.dock},
		Carrier:   carrierAdapter{t: tr, route: nodes.route, opt: transport.Options{}},
		Restocker: restockAdapter{get: b.crew},
		Evidence:  ev,

		Calls:       cfg.Client.Stats,
		Suspensions: watch,

		Prepare: b.prepare,
		Cleanup: b.cleanup,
	}, stop, nil
}

// floorSettings: the floor works for the storefront's building (an order that reaches it without that
// site is a finding: its PackageManifested would be ignored by outbound-shipping, journey gap G2), and
// waits for the order to be released by network-fulfillment's reconcile, a minute or two after the link.
func floorSettings(cfg wireConfig) floor.Settings {
	return floor.Settings{Site: cfg.Building, Lag: floor.Waits{Timeout: 5 * time.Minute, Interval: time.Second}}
}

// linkSettings bounds the wait for ONE purchase order to be raised in order-management.
func linkSettings(cfg wireConfig) link.Settings { return link.Settings{Timeout: cfg.LinkTimeout} }

// collectSettings: the collector reads the building's loads, the vendor's purchase orders and, through
// orderIDs (the linker's single read), the order-management order raised for each purchase order.
func collectSettings(cfg wireConfig, dict dictionary, orderIDs collect.OrderIDs) collect.Settings {
	return collect.Settings{Catalogue: dict.ItemBySKU, BuildingSite: cfg.Building, VendorID: cfg.VendorID, OrderIDs: orderIDs}
}

// stationCrew is what the building needs of *floor.Floor beyond ProcessAll.
type stationCrew interface {
	Provision(ctx context.Context, c floor.Crew) error
	Shutdown(ctx context.Context) error
	Findings() []string
	Stats() floor.Stats
}

// building is the part of the day that needs the building readied before it can be built: the dock
// crew (doors and staging lanes), the restock crew (inbound doors and bins), the floor's stations
// and the transport's node ids. Prepare fills it in; until then its getters return nil.
type building struct {
	cfg   wireConfig
	site  siteFixture
	nodes *nodeBook
	fl    stationCrew

	// the readiness steps; the real ones unless a test swaps them
	provisionOutbound func(ctx context.Context, c *sim.Client, s outbound.Site, o outbound.ProvisionOptions) outbound.Report
	readyInbound      func(ctx context.Context, c *sim.Client, s inbound.Site, o inbound.ReadyOptions) inbound.Readiness
	listStorageBins   func(ctx context.Context) ([]string, error)

	mu       sync.Mutex
	outFloor *outbound.Floor
	inCrew   *inbound.Crew
}

func (b *building) dock() dock {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.outFloor == nil {
		return nil
	}
	return b.outFloor
}

func (b *building) crew() crew {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.inCrew == nil {
		return nil
	}
	return b.inCrew
}

// The assumptions below are chosen, not measured: no actor has ever run on the cluster.
var (
	// carrier is the trucks' carrier (a booking and gate name; nothing registers carriers).
	simCarrier = outbound.Carrier{Name: "Simulation Freight", SCAC: "SIMF"}
	// dockWait bounds the dock and yard waits (consumer lag of PackageManifested, DockAppointmentBooked,
	// TrailerDocked, TrailerDeparted on a cluster with real Kafka consumers).
	dockWait = outbound.Waits{Timeout: 2 * time.Minute, Interval: time.Second}
)

// stowBinCandidates caps how many of the building's Storage slots are looked at as stow bins.
const stowBinCandidates = 24

// crewFor sizes the floor: one picker per order in flight and half as many packers (at least one).
func crewFor(parallel int) floor.Crew {
	p := max(parallel, 1)
	return floor.Crew{Pickers: p, Packers: max(p/2, 1)}
}

// prepare readies the building, read-only unless the run was given -provision, and fails closed:
//
//  1. the transport's node ids are read (vendor-connectivity, node-topology);
//  2. outbound.Provision checks the CPT schedule, the dock doors and staging lanes, the dock calendar
//     and the yard board. Without -provision it is ReadOnly and writes nothing. With it, it may register the
//     facility-layout site, the -doors and -staging slots that are missing and declare a never-declared
//     dock calendar (PUT /sites/{id}/dock-calendar/settings). AllowWriteSchedule is ALWAYS false:
//     the building's CPT schedule belongs to cmd/seed-storefront (make provision-storefront);
//  3. inbound.Ready checks the inbound doors, the calendar, the yard board and the stow bins. It is
//     always read-only (no BinCapacity): bins are stocked by make seed-inventory;
//  4. the floor installs and checks in its stations (POST /stations, /check-ins): the run's own
//     people, undone by cleanup. This happens only when 1-3 passed.
//
// Any check that is not ready is an error naming every one of them.
func (b *building) prepare(ctx context.Context) error {
	cfg := b.cfg
	if err := b.nodes.resolve(ctx); err != nil {
		return err
	}
	cfg.logf("prepare: origin node and delivery stations resolved (read-only)")

	site := outbound.Site{Code: cfg.Building, Doors: cfg.Doors, StagingLocations: cfg.Staging}
	rep := b.provisionOutbound(ctx, cfg.Client, site, outbound.ProvisionOptions{
		Cutoffs:            b.site.Cutoffs,
		AllowWriteSchedule: false,
		ReadOnly:           !cfg.Provision,
		Log:                cfg.ActorLog,
	})
	cfg.logf("prepare: outbound readiness of %s: ready=%v, %d write(s), provision=%v", cfg.Building, rep.Ready(), rep.Writes, cfg.Provision)
	if bad := describeChecks(rep.NotReady()); len(bad) > 0 {
		return fmt.Errorf("the dock is not ready%s: %s", provisionHint(cfg.Provision), strings.Join(bad, "; "))
	}

	bins, discovered := cfg.StowBins, false
	if len(bins) == 0 {
		var err error
		if bins, err = b.listStorageBins(ctx); err != nil {
			return fmt.Errorf("the restock bins: %w", err)
		}
		discovered = true
		if len(bins) == 0 {
			return fmt.Errorf("the restock bins: facility-layout lists no Active Storage slot at %s (name bins with -stow-bins)", cfg.Building)
		}
	}
	rd := b.readyInbound(ctx, cfg.Client, inbound.Site{Code: cfg.Building, Bins: bins}, inbound.ReadyOptions{Log: cfg.ActorLog})
	cfg.logf("prepare: inbound readiness of %s: %d door(s), %d bin(s) with room of %d offered", cfg.Building, len(rd.Doors), len(rd.Bins), len(bins))
	if bad := inboundBlockers(rd, discovered); len(bad) > 0 {
		return fmt.Errorf("the restock crew is not ready: %s", strings.Join(bad, "; "))
	}

	of := &outbound.Floor{
		C: cfg.Client, Site: rep.Apply(site), RunID: cfg.RunID, Carrier: simCarrier,
		Wait: dockWait, Log: cfg.ActorLog, GridMinutes: rep.Calendar.SlotGranularityMinutes,
	}
	ic := &inbound.Crew{
		C: cfg.Client, Site: inbound.Site{Code: cfg.Building}, RunID: cfg.RunID, Carrier: simCarrier,
		Wait: dockWait, Log: cfg.ActorLog,
	}
	rd.Configure(ic)

	if err := b.fl.Provision(ctx, crewFor(cfg.Parallel)); err != nil {
		return fmt.Errorf("the floor's stations: %w", err)
	}
	cfg.logf("prepare: %d picker(s) and %d packer(s) checked in", crewFor(cfg.Parallel).Pickers, crewFor(cfg.Parallel).Packers)

	b.mu.Lock()
	b.outFloor, b.inCrew = of, ic
	b.mu.Unlock()
	return nil
}

func provisionHint(provision bool) string {
	if provision {
		return " even with -provision (it never writes the CPT schedule: make provision-storefront)"
	}
	return " (read-only: -provision may register missing doors, staging lanes and the dock calendar; the CPT schedule is make provision-storefront)"
}

func describeChecks(cs []outbound.Check) []string {
	out := make([]string, len(cs))
	for i, c := range cs {
		out[i] = fmt.Sprintf("%s %s: %s (%s)", c.Area, c.Name, c.Status, c.Detail)
	}
	return out
}

// inboundBlockers lists why the restock crew cannot work. Bins the run DISCOVERED are candidates:
// one that inventory-storage does not know is not a blocker as long as one has room. Bins the user
// named with -stow-bins must all be usable.
func inboundBlockers(rd inbound.Readiness, discoveredBins bool) []string {
	var bad []outbound.Check
	for _, c := range rd.NotReady() {
		if discoveredBins && c.Area == inbound.SvcInventory && strings.HasPrefix(c.Name, "bin ") {
			continue
		}
		bad = append(bad, c)
	}
	out := describeChecks(bad)
	if len(rd.Bins) == 0 {
		out = append(out, "inventory-storage: no stow bin is registered with room (stock is seeded by make seed-inventory; -stow-bins names the bins)")
	}
	return out
}

// storageBins reads the building's Active Storage slots (facility-layout), the candidates for the
// restock's bins, sorted and capped.
func storageBins(ctx context.Context, c *sim.Client, building string) ([]string, error) {
	type slot struct {
		LocationCode string `json:"locationCode"`
		Status       string `json:"status"`
	}
	path := "/sites/" + url.PathEscape(building) + "/locations?role=Storage"
	slots, last := sim.ListAll[slot](ctx, c, "facility-layout", "GET /sites/{code}/locations", path)
	if !last.OK() {
		return nil, fmt.Errorf("facility-layout GET %s: %d %s", path, last.Status, sim.Truncate(string(last.Body), 160))
	}
	var codes []string
	for _, s := range slots {
		if s.Status == "Active" && s.LocationCode != "" {
			codes = append(codes, s.LocationCode)
		}
	}
	sort.Strings(codes)
	if len(codes) > stowBinCandidates {
		codes = codes[:stowBinCandidates]
	}
	return codes, nil
}

// cleanup checks the floor's people out. It runs whether or not prepare completed.
func (b *building) cleanup(ctx context.Context) error {
	err := b.fl.Shutdown(ctx)
	st := b.fl.Stats()
	if st.ForeignReleased+st.ForeignWorked+st.ForeignLeft > 0 {
		b.cfg.logf("floor: worked for others (shared queues): %d work unit(s) released, %d task(s) worked, %d left claimed", st.ForeignReleased, st.ForeignWorked, st.ForeignLeft)
	}
	for _, f := range b.fl.Findings() {
		b.cfg.logf("floor finding: %s", f)
	}
	return err
}
