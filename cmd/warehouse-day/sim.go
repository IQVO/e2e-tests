package main

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------
// catalogue of the simulated building
// ---------------------------------------------------------------------

type skuDef struct {
	key      string
	weightKg float64
	fragile  bool
	stock    int     // opening stock received at 05:30
	demand   float64 // relative popularity in the customer order mix
}

// cartonTareKg is added to every package's expected weight at SLAM.
const cartonTareKg = 0.20

var skuCatalogue = []skuDef{
	{"TSHIRT", 0.25, false, 400, 10},
	{"JEANS", 0.80, false, 300, 7},
	{"MUG", 0.45, true, 220, 5},
	{"BOOK", 0.60, false, 260, 6},
	{"HEADPHONES", 0.35, false, 160, 4},
	{"LAMP", 1.40, true, 90, 2},
	{"SNEAKERS", 1.10, false, 160, 4},
	{"BACKPACK", 0.90, false, 130, 3},
	// Deliberately understocked: orders for it backorder until the
	// 13:00 replenishment arrives and inventory control retries them.
	{"CAMERA", 0.70, false, 1, 0},
	// Never ordered; lives in the overflow bin and is the subject of the
	// deliberate cycle-count shortage.
	{"SAMPLE", 0.10, false, 20, 0},
}

// replenishHour is when the CAMERA replenishment truck arrives (sim time).
const replenishHour = 13

const (
	storageAisles = 4
	storageBays   = 6
	binCapacity   = 2000
)

type orderKind string

const (
	kindStandard     orderKind = "standard"
	kindPartialOK    orderKind = "partial-ok"
	kindHeldRelease  orderKind = "held-then-released"
	kindHeldCancel   orderKind = "held-then-cancelled"
	kindBackorder    orderKind = "backorder"
	kindMispickInjct orderKind = "mispick"
)

type orderLinePlan struct {
	sku      string
	qty      int
	giftWrap bool
}

type orderRec struct {
	seq                          int
	kind                         orderKind
	arriveAt                     time.Time // wall
	simAt                        string
	lines                        []orderLinePlan
	allowPartial, releaseOnAlloc bool

	// filled in as the day goes
	id            string
	intakeStatus  string
	intakeHTTP    int
	promiseDate   string
	placedWall    time.Time
	packageID     string
	packageStatus string
	sortLane      string
	labeledWall   time.Time
	pickedLines   map[int]bool
	cancelled     bool
	retried       bool
	notes         []string
}

func (o *orderRec) fragile() bool {
	for _, l := range o.lines {
		if skuByKey(l.sku).fragile {
			return true
		}
	}
	return false
}

func (o *orderRec) giftWrap() bool {
	for _, l := range o.lines {
		if l.giftWrap {
			return true
		}
	}
	return false
}

// shippable reports whether the order is expected to end the day as a
// package on a truck (or diverted for an injected mispick).
func (o *orderRec) shippable() bool { return o.kind != kindHeldCancel && o.id != "" }

func skuByKey(k string) skuDef {
	for _, s := range skuCatalogue {
		if s.key == k {
			return s
		}
	}
	return skuDef{key: k, weightKg: 0.5}
}

// ---------------------------------------------------------------------
// simulator state
// ---------------------------------------------------------------------

type tote struct {
	orderID string
	lineNo  int
	cpt     string
}

type parcel struct {
	packageID string
	orderID   string
}

type sim struct {
	cfg   config
	api   *api
	stats *callStats
	clock *simClock
	rnd   *rand.Rand
	rndMu sync.Mutex

	mu       sync.Mutex
	orders   []*orderRec
	byID     map[string]*orderRec
	storage  []string // inventory bin ids == facility location codes
	overflow string
	totes    chan tote
	conveyor chan parcel

	associates []*associate
	findings   []string
	// findingsDuringSuspend are findings raised inside a host-suspension
	// window (see suspendWatch); reported, but not counted as failures.
	findingsDuringSuspend []string
	snapshots             []string
	suspend               suspendWatch

	m struct {
		released, releaseRejected               atomic.Int64
		picks, picksNoLocation, foreignTasks    atomic.Int64
		confirmPickConflicts, completeConflicts atomic.Int64
		confirmPickRetries                      atomic.Int64
		rebinScans, packs, slams, labeled       atomic.Int64
		diverted, travelMetres                  atomic.Int64
		breaks, reassignments, cycleCounts      atomic.Int64
		discrepancies, leaseRenewals            atomic.Int64
	}
	intakeDone atomic.Bool
	// slotsImported is how many NEW slots facility-layout registered this
	// morning. The building is mapped once and reused on later days (as in
	// a real FC), so LocationSlotRegistered is only expected on day one.
	slotsImported int
	shiftPlanned  bool
	tap           *eventTap
}

func newSim(cfg config) *sim {
	stats := newCallStats()
	return &sim{
		cfg:      cfg,
		api:      newAPI(cfg, stats),
		stats:    stats,
		rnd:      rand.New(rand.NewSource(cfg.seed)),
		byID:     map[string]*orderRec{},
		totes:    make(chan tote, 1024),
		conveyor: make(chan parcel, 1024),
	}
}

func (s *sim) rf() float64 {
	s.rndMu.Lock()
	defer s.rndMu.Unlock()
	return s.rnd.Float64()
}

func (s *sim) ri(n int) int {
	s.rndMu.Lock()
	defer s.rndMu.Unlock()
	return s.rnd.Intn(n)
}

func (s *sim) sku(key string) string { return fmt.Sprintf("SKU-%s-%s", s.cfg.runID, key) }

func (s *sim) skuKey(sku string) string {
	return strings.TrimPrefix(sku, fmt.Sprintf("SKU-%s-", s.cfg.runID))
}

// suspendGrace covers the real 5-minute claim leases that lapse while the
// host sleeps and only surface on the associates' next calls after wake.
const suspendGrace = 6 * time.Minute

func (s *sim) finding(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	duringSuspend := s.suspend.covers(time.Now(), suspendGrace)
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, f := range append(s.findings, s.findingsDuringSuspend...) {
		if f == msg {
			return
		}
	}
	if duringSuspend {
		s.findingsDuringSuspend = append(s.findingsDuringSuspend, msg)
		logf("FINDING*", "%s (during/just after a host suspension; not counted)", msg)
		return
	}
	s.findings = append(s.findings, msg)
	logf("FINDING", "%s", msg)
}

func (s *sim) order(id string) *orderRec {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.byID[id]
}

func (s *sim) shiftID() string {
	return fmt.Sprintf("%s-DAY-%s", time.Now().Format("20060102"), s.cfg.runID)
}

// ---------------------------------------------------------------------
// 05:30 — opening the building
// ---------------------------------------------------------------------

type setupError struct{ msg string }

func (e setupError) Error() string { return e.msg }

func fail(format string, args ...any) error { return setupError{fmt.Sprintf(format, args...)} }

func (s *sim) healthCheck(ctx context.Context) error {
	var down []string
	for _, svc := range allServices {
		r := s.api.call(ctx, svc, http.MethodGet, "/healthz", "/healthz", nil)
		if r.status != http.StatusOK {
			down = append(down, fmt.Sprintf("%s(%d)", svc, r.status))
		}
	}
	if len(down) > 0 {
		return fail("services not healthy: %s", strings.Join(down, ", "))
	}
	logf("control-tower", "all %d bounded contexts healthy", len(allServices))
	return nil
}

var processPaths = []struct{ id, prefix, capability string }{
	{"PICK", "pick", "pick"},
	{"PACK", "pack", "pack"},
	{"SLAM", "slam", "slam"},
	{"REBIN", "rebin", "rebin"},
}

func (s *sim) ensureProcessPaths(ctx context.Context) error {
	var existing []struct {
		PathID string `json:"pathId"`
		Status string `json:"status"`
	}
	r := s.api.call(ctx, svcProcessPath, http.MethodGet, "/process-paths", "/process-paths", nil)
	if !r.ok() {
		return fail("GET /process-paths -> %d %s", r.status, truncate(string(r.body), 200))
	}
	if err := r.decode(&existing); err != nil {
		return err
	}
	have := map[string]bool{}
	for _, p := range existing {
		if strings.EqualFold(p.Status, "ACTIVE") {
			have[strings.ToUpper(p.PathID)] = true
		}
	}
	for _, p := range processPaths {
		if have[p.id] {
			continue
		}
		r := s.api.call(ctx, svcProcessPath, http.MethodPost, "/process-paths", "/process-paths", map[string]any{
			"pathId": p.id, "matchPrefix": p.prefix, "direct": true,
			"requiredCapabilities": []string{p.capability}, "cycleTimeP95": "2h0m0s",
		})
		if !r.ok() && r.status != http.StatusConflict {
			return fail("define process path %s -> %d %s", p.id, r.status, truncate(string(r.body), 200))
		}
		logf("engineering", "defined process path %s", p.id)
	}
	logf("engineering", "process paths in force: PICK, PACK, SLAM, REBIN")
	return nil
}

// truckCutoffs are the day's carrier pickups, in simulated time.
var truckCutoffs = []struct {
	h, m int
	id   string
}{{12, 0, "sim-1200"}, {16, 0, "sim-1600"}, {20, 0, "sim-2000"}}

// publishCPTSchedule maps the simulated 12:00/16:00/20:00 truck departures
// onto real UTC wall-clock minutes, because order-management promises
// against real time. A missed truck is therefore a REAL missed CPT.
func (s *sim) publishCPTSchedule(ctx context.Context) error {
	seen := map[string]bool{}
	var cutoffs []map[string]any
	for _, t := range truckCutoffs {
		w := s.clock.wallAt(t.h, t.m).UTC().Truncate(time.Minute).Add(time.Minute)
		hhmm := w.Format("15:04")
		for seen[hhmm] {
			w = w.Add(time.Minute)
			hhmm = w.Format("15:04")
		}
		seen[hhmm] = true
		cutoffs = append(cutoffs, map[string]any{
			"cptId": t.id, "localTime": hhmm, "daysOfWeek": []string{"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"},
			"shipMethod": "ground", "eligiblePathIds": []string{"PICK"},
		})
		logf("transport", "truck %02d:%02d (sim) departs at %s UTC (real CPT)", t.h, t.m, hhmm)
	}
	r := s.api.call(ctx, svcProcessPath, http.MethodPut, "/sites/{siteId}/cpt-schedule", "/sites/"+url.PathEscape(s.cfg.cptSiteID)+"/cpt-schedule",
		map[string]any{"timezone": "UTC", "cutoffs": cutoffs})
	if !r.ok() {
		return fail("PUT cpt-schedule for %s -> %d %s", s.cfg.cptSiteID, r.status, truncate(string(r.body), 300))
	}
	return nil
}

func (s *sim) defineLaborStandards(ctx context.Context) error {
	std := map[string]int{
		"PICK": int(s.cfg.pickSeconds + 0.999),
		"PACK": int(s.cfg.packSeconds + 0.999),
		"SLAM": 1,
	}
	for _, tt := range []string{"PICK", "PACK", "SLAM"} {
		r := s.api.call(ctx, svcLabor, http.MethodPost, "/standards", "/standards", map[string]any{
			"taskType": tt, "expectedSeconds": std[tt],
		})
		if !r.ok() {
			return fail("POST /standards %s -> %d %s", tt, r.status, truncate(string(r.body), 200))
		}
	}
	logf("industrial-eng", "engineered labor standards: PICK %ds, PACK %ds, SLAM 1s", std["PICK"], std["PACK"])
	return nil
}

func (s *sim) storageCode(aisle, bay int) string {
	return fmt.Sprintf("%s-STOR-AMB-A%02d-%02d-01-A", s.cfg.siteCode, aisle, bay)
}

func (s *sim) workCentreCode(n int) string {
	return fmt.Sprintf("%s-OPS-WC-W01-%02d-01-A", s.cfg.siteCode, n)
}

func (s *sim) mapBuilding(ctx context.Context, stations int) error {
	for _, lt := range []map[string]any{
		{"name": "SimShelf", "role": "Storage", "defaultCapacity": map[string]any{"maxWeightKg": 250, "maxVolumeM3": 1.2}},
		{"name": "SimWorkStation", "role": "WorkCenter"},
	} {
		r := s.api.call(ctx, svcFacility, http.MethodPost, "/location-types", "/location-types", lt)
		if !r.ok() && r.status != http.StatusConflict {
			return fail("register location type %v -> %d %s", lt["name"], r.status, truncate(string(r.body), 200))
		}
	}
	var rows []map[string]any
	for a := 1; a <= storageAisles; a++ {
		for b := 1; b <= storageBays; b++ {
			rows = append(rows, map[string]any{
				"siteCode": s.cfg.siteCode, "siteName": "Simulated FC", "areaCode": "STOR", "zoneCode": "AMB",
				"temperatureClass": "Ambient", "hazmat": false, "aisleCode": fmt.Sprintf("A%02d", a), "sequenceHint": a,
				"direction": "TwoWay", "bay": fmt.Sprintf("%02d", b), "level": "01", "position": "A", "locationType": "SimShelf",
			})
		}
	}
	for n := 1; n <= stations; n++ {
		rows = append(rows, map[string]any{
			"siteCode": s.cfg.siteCode, "siteName": "Simulated FC", "areaCode": "OPS", "zoneCode": "WC",
			"temperatureClass": "Ambient", "hazmat": false, "aisleCode": "W01", "sequenceHint": 1,
			"direction": "TwoWay", "bay": fmt.Sprintf("%02d", n), "level": "01", "position": "A", "locationType": "SimWorkStation",
			"activities": []string{"Pack"},
		})
	}
	r := s.api.call(ctx, svcFacility, http.MethodPost, "/locations/import", "/locations/import", rows)
	if !r.ok() {
		return fail("POST /locations/import -> %d %s", r.status, truncate(string(r.body), 300))
	}
	var rep struct {
		RowsSubmitted, SlotsImported, RowsRejected int
		Results                                    []struct {
			LocationCode string `json:"locationCode"`
			Succeeded    bool   `json:"succeeded"`
			Error        string `json:"error"`
		} `json:"results"`
	}
	if err := r.decode(&rep); err != nil {
		return err
	}
	reused := 0
	for _, res := range rep.Results {
		if res.Succeeded {
			continue
		}
		if strings.Contains(strings.ToLower(res.Error), "already") || strings.Contains(strings.ToLower(res.Error), "exist") {
			reused++
			continue
		}
		return fail("facility import rejected %s: %s", res.LocationCode, res.Error)
	}
	s.slotsImported = rep.SlotsImported
	logf("facilities", "building map: %d slots imported, %d already on the map (site %s)", rep.SlotsImported, reused, s.cfg.siteCode)

	for a := 1; a <= storageAisles; a++ {
		for b := 1; b <= storageBays; b++ {
			code := s.storageCode(a, b)
			r := s.api.call(ctx, svcInventory, http.MethodPut, "/bins/{binId}", "/bins/"+url.PathEscape(code), map[string]any{"capacity": binCapacity})
			if r.status == http.StatusNotFound || r.status == http.StatusMethodNotAllowed {
				return fail("inventory-storage has no PUT /bins/{binId} (got %d) — deploy inventory-storage with the bin-registration API (claudioed/inventory-storage#112)", r.status)
			}
			if !r.ok() && r.status != http.StatusConflict {
				return fail("PUT /bins/%s -> %d %s", code, r.status, truncate(string(r.body), 200))
			}
			if a == storageAisles && b == storageBays {
				s.overflow = code
			} else {
				s.storage = append(s.storage, code)
			}
		}
	}
	logf("inventory-ctl", "%d pick bins + overflow bin %s registered (capacity %d each)", len(s.storage), s.overflow, binCapacity)
	return nil
}

func (s *sim) receiveAndStow(ctx context.Context, who string, sd skuDef, qty int, bins []string) error {
	sku := s.sku(sd.key)
	r := s.api.call(ctx, svcInventory, http.MethodPost, "/stock/receive", "/stock/receive", map[string]any{"sku": sku, "quantity": qty})
	if !r.ok() {
		return fail("receive %d %s -> %d %s", qty, sku, r.status, truncate(string(r.body), 200))
	}
	left := qty
	start := s.ri(len(bins))
	for i := 0; left > 0 && i < len(bins)*2; i++ {
		chunk := 25 + s.ri(40)
		if chunk > left {
			chunk = left
		}
		bin := bins[(start+i)%len(bins)]
		r := s.api.call(ctx, svcInventory, http.MethodPost, "/stock/stow", "/stock/stow", map[string]any{"sku": sku, "quantity": chunk, "binId": bin})
		switch {
		case r.ok():
			left -= chunk
		case r.status == http.StatusConflict:
			continue // bin full or racing — try the next bin
		default:
			return fail("stow %d %s into %s -> %d %s", chunk, sku, bin, r.status, truncate(string(r.body), 200))
		}
	}
	if left > 0 {
		return fail("%s could not stow %d of %d %s anywhere", who, left, qty, sku)
	}
	return nil
}

// catalogueProducts is the control tower's product-master step: every SKU
// of the day is registered (PUT /products/{sku}) and the fragile ones are
// classified (PUT /products/{sku}/classification) in product-master, the
// owner of classification since product-master ADR 0003 stage C.
// inventory-storage answers its own retired PUT with 410
// classification-moved and learns the classification asynchronously from
// warehouse.product-master.events, like order-management, wes-work-planning
// and fulfillment-execution (local copies). Both PUTs are idempotent, so a
// re-run with the same run id changes nothing.
func (s *sim) catalogueProducts(ctx context.Context) error {
	classified := 0
	for _, sd := range skuCatalogue {
		sku := s.sku(sd.key)
		r := s.api.call(ctx, svcProductMaster, http.MethodPut, "/products/{sku}", "/products/"+url.PathEscape(sku),
			map[string]any{"description": fmt.Sprintf("Simulated %s (%.2f kg)", strings.ToLower(sd.key), sd.weightKg)})
		if !r.ok() {
			return fail("register %s in product-master -> %d %s", sd.key, r.status, truncate(string(r.body), 200))
		}
		if !sd.fragile {
			continue
		}
		r = s.api.call(ctx, svcProductMaster, http.MethodPut, "/products/{sku}/classification", "/products/"+url.PathEscape(sku)+"/classification",
			map[string]any{"handlingTags": []string{"Fragile"}})
		if !r.ok() {
			return fail("classify %s in product-master -> %d %s", sd.key, r.status, truncate(string(r.body), 200))
		}
		classified++
	}
	logf("control-tower", "product-master: %d SKUs registered, %d classified Fragile", len(skuCatalogue), classified)
	return nil
}

func (s *sim) inbound(ctx context.Context) error {
	total := 0
	for _, sd := range skuCatalogue {
		bins := s.storage
		if sd.key == "SAMPLE" {
			bins = []string{s.overflow}
		}
		if err := s.receiveAndStow(ctx, "receiving", sd, sd.stock, bins); err != nil {
			return err
		}
		total += sd.stock
	}
	logf("receiving", "inbound trucks unloaded: %d units of %d SKUs received and stowed (chaotic storage)", total, len(skuCatalogue))
	return nil
}

// planTheDay runs the planner's morning: forecast the charge per truck,
// let workforce-management propose heads, commit the shift plan, and give
// WES its labor plan.
func (s *sim) planTheDay(ctx context.Context) error {
	units := 0
	for _, o := range s.orders {
		for _, l := range o.lines {
			units += l.qty
		}
	}
	var buckets []map[string]any
	for i, t := range truckCutoffs {
		share := []float64{0.35, 0.40, 0.25}[i]
		buckets = append(buckets, map[string]any{
			"cpt":      s.clock.wallAt(t.h, t.m).UTC().Truncate(time.Minute).Add(time.Minute).Format(time.RFC3339),
			"quantity": int(float64(units) * share),
		})
	}
	r := s.api.call(ctx, svcWES, http.MethodPost, "/paths/{pathId}/charge", "/paths/"+s.cfg.wesPickPath+"/charge", map[string]any{"buckets": buckets})
	if !r.ok() {
		return fail("POST /paths/PICK/charge -> %d %s", r.status, truncate(string(r.body), 200))
	}
	pr := s.api.call(ctx, svcWorkforce, http.MethodPost, "/paths/{pathId}/plan/propose", "/paths/PICK/plan/propose",
		map[string]any{"buildingId": s.cfg.buildingID, "charge": float64(units), "plannedRate": 60.0})
	logf("planner", "forecast %d units across %d trucks; workforce proposal -> %d %s", units, len(buckets), pr.status, truncate(strings.TrimSpace(string(pr.body)), 160))

	heads := map[string]int{
		"PICK": s.cfg.pickers + s.cfg.flex, "PACK": s.cfg.packers + s.cfg.flex,
		"SLAM": s.cfg.slammers, "REBIN": s.cfg.rebinners,
	}
	var lines []map[string]any
	for _, p := range []string{"PICK", "PACK", "SLAM", "REBIN"} {
		if heads[p] == 0 {
			continue
		}
		lines = append(lines, map[string]any{
			"pathId": p, "plannedHeads": heads[p], "plannedRate": 60.0,
			"plannedHours": float64(heads[p]) * 8, "installedStations": heads[p],
		})
	}
	r = s.api.call(ctx, svcWorkforce, http.MethodPost, "/shift-plans", "/shift-plans",
		map[string]any{"buildingId": s.cfg.buildingID, "shiftId": s.shiftID(), "lines": lines})
	if !r.ok() {
		// Not fatal for the floor (associates are assigned individually),
		// but the supervisor loses the planned-vs-active staffing view.
		s.finding("workforce-management rejected the shift plan: %d %s", r.status, truncate(string(r.body), 220))
	} else {
		s.shiftPlanned = true
	}
	r = s.api.call(ctx, svcWES, http.MethodPost, "/paths/{pathId}/plan", "/paths/"+s.cfg.wesPickPath+"/plan", map[string]any{
		"plannedHeads": heads["PICK"], "installedStations": heads["PICK"], "rateUnitsPerHour": 60.0, "hours": 8.0,
	})
	if !r.ok() {
		return fail("POST /paths/PICK/plan -> %d %s", r.status, truncate(string(r.body), 200))
	}
	logf("planner", "shift plan %s (committed=%v; PICK %d, PACK %d, SLAM %d, REBIN %d heads)", s.shiftID(), s.shiftPlanned, heads["PICK"], heads["PACK"], heads["SLAM"], heads["REBIN"])
	return nil
}

// ---------------------------------------------------------------------
// the day's customer demand
// ---------------------------------------------------------------------

// hourly arrival weights, 06:00..19:00 (intake closes 19:30).
var demandCurve = []float64{0.3, 0.8, 1.4, 1.8, 1.6, 1.5, 1.9, 1.7, 1.3, 1.1, 1.0, 1.2, 0.9, 0.5}

func (s *sim) planOrders() {
	var weighted []skuDef
	for _, sd := range skuCatalogue {
		if sd.demand > 0 {
			weighted = append(weighted, sd)
		}
	}
	pickSKU := func() skuDef {
		total := 0.0
		for _, sd := range weighted {
			total += sd.demand
		}
		x := s.rf() * total
		for _, sd := range weighted {
			if x -= sd.demand; x <= 0 {
				return sd
			}
		}
		return weighted[0]
	}
	curveTotal := 0.0
	for _, w := range demandCurve {
		curveTotal += w
	}
	simMinute := func() int {
		x := s.rf() * curveTotal
		for h, w := range demandCurve {
			if x -= w; x <= 0 {
				m := h*60 + s.ri(60)
				if m < 15 {
					m = 15 + s.ri(30)
				}
				return m
			}
		}
		return 13*60 + 15
	}

	special := map[int]orderKind{}
	n := s.cfg.orders
	assign := func(k orderKind, count int) {
		for i := 0; i < count; i++ {
			for {
				idx := s.ri(n)
				if _, taken := special[idx]; !taken {
					special[idx] = k
					break
				}
			}
		}
	}
	assign(kindBackorder, 3)
	assign(kindHeldRelease, 2)
	assign(kindHeldCancel, 2)
	assign(kindMispickInjct, max(1, n/40))
	assign(kindPartialOK, n/6)

	for i := 0; i < n; i++ {
		o := &orderRec{seq: i + 1, kind: kindStandard, releaseOnAlloc: true, pickedLines: map[int]bool{}}
		if k, ok := special[i]; ok {
			o.kind = k
		}
		nLines := 1
		if x := s.rf(); x > 0.6 {
			nLines = 2
		}
		if x := s.rf(); x > 0.9 {
			nLines = 3
		}
		used := map[string]bool{}
		for len(o.lines) < nLines {
			sd := pickSKU()
			if used[sd.key] {
				continue
			}
			used[sd.key] = true
			o.lines = append(o.lines, orderLinePlan{sku: sd.key, qty: 1 + s.ri(2), giftWrap: s.rf() < 0.08})
		}
		switch o.kind {
		case kindBackorder:
			o.lines = append(o.lines, orderLinePlan{sku: "CAMERA", qty: 2})
		case kindHeldRelease, kindHeldCancel:
			o.releaseOnAlloc = false
		case kindPartialOK:
			o.allowPartial = true
		}
		m := simMinute()
		if o.kind == kindBackorder {
			// A backorder only exists while CAMERA is out of stock: the
			// 13:00 replenishment would let a later order allocate in full.
			for m >= (replenishHour-dayOpenHour)*60-30 {
				m = simMinute()
			}
		}
		o.arriveAt = s.clock.wallAt(dayOpenHour, 0).Add(s.clock.simDur(time.Duration(m) * time.Minute))
		o.simAt = fmt.Sprintf("%02d:%02d", dayOpenHour+m/60, m%60)
		s.orders = append(s.orders, o)
	}
	sort.Slice(s.orders, func(i, j int) bool { return s.orders[i].arriveAt.Before(s.orders[j].arriveAt) })
}

type omLine struct {
	LineNo        int    `json:"lineNo"`
	SKU           string `json:"sku"`
	Quantity      int    `json:"quantity"`
	PathID        string `json:"pathId"`
	GiftWrap      bool   `json:"giftWrap"`
	Status        string `json:"status"`
	ReservationID string `json:"reservationId"`
}

type omOrder struct {
	ID          string   `json:"id"`
	Status      string   `json:"status"`
	PromiseDate string   `json:"promiseDate"`
	Lines       []omLine `json:"lines"`
}

func (s *sim) getOrder(ctx context.Context, id string) (omOrder, int) {
	var o omOrder
	r := s.api.call(ctx, svcOrder, http.MethodGet, "/orders/{id}", "/orders/"+url.PathEscape(id), nil)
	if r.ok() {
		_ = r.decode(&o)
	}
	return o, r.status
}

func (s *sim) placeOrder(ctx context.Context, o *orderRec) {
	var lines []map[string]any
	for _, l := range o.lines {
		lines = append(lines, map[string]any{"sku": s.sku(l.sku), "quantity": l.qty, "giftWrap": l.giftWrap})
	}
	r := s.api.call(ctx, svcOrder, http.MethodPost, "/orders", "/orders", map[string]any{
		"lines": lines, "allowPartialShipment": o.allowPartial, "releaseOnAllocation": o.releaseOnAlloc,
	})
	o.intakeHTTP = r.status
	o.placedWall = time.Now()
	if r.status != http.StatusCreated {
		s.finding("POST /orders returned %d for a valid order: %s", r.status, truncate(string(r.body), 200))
		return
	}
	var created omOrder
	if err := r.decode(&created); err != nil {
		s.finding("POST /orders body undecodable: %v", err)
		return
	}
	for _, l := range created.Lines {
		if l.PathID != "" && l.PathID != s.cfg.wesPickPath {
			s.finding("order %s line %d landed on path %q, but the floor drives WES pool %q (-wes-pick-path)", created.ID, l.LineNo, l.PathID, s.cfg.wesPickPath)
		}
	}
	s.mu.Lock()
	o.id, o.intakeStatus, o.promiseDate = created.ID, created.Status, created.PromiseDate
	s.byID[o.id] = o
	s.mu.Unlock()

	want := map[orderKind]string{
		kindStandard: "Released", kindPartialOK: "Released", kindMispickInjct: "Released",
		kindHeldRelease: "Allocated", kindHeldCancel: "Allocated", kindBackorder: "Backordered",
	}[o.kind]
	if created.Status != want && !(o.kind == kindBackorder && created.Status == "PartiallyAllocated") {
		o.notes = append(o.notes, fmt.Sprintf("intake status %s, expected %s", created.Status, want))
	}
	logf("customer", "order #%03d %s %-20s %d line(s) -> %s, promise %s", o.seq, short(o.id), o.kind, len(o.lines), created.Status, shortTime(created.PromiseDate))
}

// customerChannel places every planned order at its arrival time.
func (s *sim) customerChannel(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	defer s.intakeDone.Store(true)
	for _, o := range s.orders {
		if !sleepCtx(ctx, time.Until(o.arriveAt)) {
			return
		}
		s.placeOrder(ctx, o)
	}
	logf("customer", "order intake closed: %d orders placed", len(s.orders))
}

// customerService handles held orders: releases half (a fill-or-kill
// customer confirmed) and cancels the other half, ~45 sim minutes later.
func (s *sim) customerService(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	done := map[*orderRec]bool{}
	for ctx.Err() == nil {
		s.mu.Lock()
		var due []*orderRec
		for _, o := range s.orders {
			if done[o] || o.id == "" || (o.kind != kindHeldRelease && o.kind != kindHeldCancel) {
				continue
			}
			if time.Since(o.placedWall) >= s.clock.simDur(45*time.Minute) {
				due = append(due, o)
			}
		}
		s.mu.Unlock()
		for _, o := range due {
			done[o] = true
			if o.kind == kindHeldRelease {
				r := s.api.call(ctx, svcOrder, http.MethodPost, "/orders/{id}/release", "/orders/"+o.id+"/release", nil)
				logf("cust-service", "held order #%03d confirmed by customer -> release %d", o.seq, r.status)
				if !r.ok() {
					s.finding("releasing held order %s -> %d %s", o.id, r.status, truncate(string(r.body), 200))
				}
			} else {
				r := s.api.call(ctx, svcOrder, http.MethodDelete, "/orders/{id}", "/orders/"+o.id, nil)
				logf("cust-service", "held order #%03d cancelled by customer -> %d", o.seq, r.status)
				if r.status != http.StatusNoContent {
					s.finding("cancelling held order %s -> %d %s", o.id, r.status, truncate(string(r.body), 200))
				} else {
					s.mu.Lock()
					o.cancelled = true
					s.mu.Unlock()
				}
			}
		}
		if s.intakeDone.Load() && len(done) >= 4 {
			return
		}
		sleepCtx(ctx, 500*time.Millisecond)
	}
}

func short(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

func shortTime(ts string) string {
	if t, err := time.Parse(time.RFC3339, ts); err == nil {
		return t.UTC().Format("15:04Z")
	}
	if ts == "" {
		return "-"
	}
	return ts
}
