package main

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ---------------------------------------------------------------------
// 05:30 — opening the building
// ---------------------------------------------------------------------

type setupError struct{ msg string }

func (e setupError) Error() string { return e.msg }

func fail(format string, args ...any) error { return setupError{fmt.Sprintf(format, args...)} }

func (s *sim) healthCheck(ctx context.Context) error {
	var down []string
	for _, svc := range allServices {
		r := s.api.Call(ctx, svc, http.MethodGet, "/healthz", "/healthz", nil)
		if r.Status != http.StatusOK {
			down = append(down, fmt.Sprintf("%s(%d)", svc, r.Status))
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

// capabilityOf is the capability a path requires, i.e. the certification an
// associate needs to be assigned to it. Unknown path ids panic: the floor
// plan only staffs the paths this table defines.
func capabilityOf(pathID string) string {
	for _, p := range processPaths {
		if p.id == pathID {
			return p.capability
		}
	}
	panic("warehouse-day: no process path " + pathID)
}

func (s *sim) ensureProcessPaths(ctx context.Context) error {
	type pathRow struct {
		PathID string `json:"pathId"`
		Status string `json:"status"`
	}
	existing, r := listAll[pathRow](ctx, s.api, svcProcessPath, "/process-paths", "/process-paths")
	if !r.OK() {
		return fail("GET /process-paths -> %d %s", r.Status, truncate(string(r.Body), 200))
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
		r := s.api.Call(ctx, svcProcessPath, http.MethodPost, "/process-paths", "/process-paths", map[string]any{
			"pathId": p.id, "matchPrefix": p.prefix, "direct": true,
			"requiredCapabilities": []string{p.capability}, "cycleTimeP95": "2h0m0s",
		})
		if !r.OK() && r.Status != http.StatusConflict {
			return fail("define process path %s -> %d %s", p.id, r.Status, truncate(string(r.Body), 200))
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
// against real time. A missed truck is therefore a REAL missed CPT. The
// schedule is the simulated building's own (-cpt-site, default -site): the
// orders carry that site as fulfillmentSiteId, and siteGuard has already
// refused the storefront's site.
func (s *sim) publishCPTSchedule(ctx context.Context) error {
	cutoffs := s.cptCutoffs()
	for i, t := range truckCutoffs {
		logf("transport", "truck %02d:%02d (sim) departs at %s UTC (real CPT, site %s)", t.h, t.m, cutoffs[i]["localTime"], s.cfg.cptSiteID)
	}
	r := s.api.Call(ctx, svcProcessPath, http.MethodPut, "/sites/{siteId}/cpt-schedule", s.cptSchedulePath(),
		map[string]any{"timezone": "UTC", "cutoffs": cutoffs})
	if !r.OK() {
		return fail("PUT cpt-schedule for %s -> %d %s", s.cfg.cptSiteID, r.Status, truncate(string(r.Body), 300))
	}
	return nil
}

// cptSchedulePath is the process-path-management path of the CPT schedule
// the simulated trucks replace.
func (s *sim) cptSchedulePath() string {
	return "/sites/" + url.PathEscape(s.cfg.cptSiteID) + "/cpt-schedule"
}

// cptCutoffs is the day's truckCutoffs as process-path-management cutoffs
// in UTC, one per distinct wall-clock minute.
func (s *sim) cptCutoffs() []map[string]any {
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
	}
	return cutoffs
}

func (s *sim) defineLaborStandards(ctx context.Context) error {
	std := map[string]int{
		"PICK": int(s.cfg.pickSeconds + 0.999),
		"PACK": int(s.cfg.packSeconds + 0.999),
		"SLAM": 1,
	}
	for _, tt := range []string{"PICK", "PACK", "SLAM"} {
		r := s.api.Call(ctx, svcLabor, http.MethodPost, "/standards", "/standards", map[string]any{
			"taskType": tt, "expectedSeconds": std[tt],
		})
		if !r.OK() {
			return fail("POST /standards %s -> %d %s", tt, r.Status, truncate(string(r.Body), 200))
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
	if err := s.importLocations(ctx, stations); err != nil {
		return err
	}
	return s.registerStorageBins(ctx)
}

// importLocations registers the simulated location types and imports the
// building's storage and work-station rows in one facility-layout import.
func (s *sim) importLocations(ctx context.Context, stations int) error {
	for _, lt := range []map[string]any{
		{"name": "SimShelf", "role": "Storage", "defaultCapacity": map[string]any{"maxWeightKg": 250, "maxVolumeM3": 1.2}},
		{"name": "SimWorkStation", "role": "WorkCenter"},
	} {
		r := s.api.Call(ctx, svcFacility, http.MethodPost, "/location-types", "/location-types", lt)
		if !r.OK() && r.Status != http.StatusConflict {
			return fail("register location type %v -> %d %s", lt["name"], r.Status, truncate(string(r.Body), 200))
		}
	}
	r := s.api.Call(ctx, svcFacility, http.MethodPost, "/location-imports", "/location-imports", s.locationRows(stations))
	if !r.OK() {
		return fail("POST /location-imports -> %d %s", r.Status, truncate(string(r.Body), 300))
	}
	var rep struct {
		RowsSubmitted, SlotsImported, RowsRejected int
		Results                                    []struct {
			LocationCode string `json:"locationCode"`
			Succeeded    bool   `json:"succeeded"`
			Error        string `json:"error"`
		} `json:"results"`
	}
	if err := r.Decode(&rep); err != nil {
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
	return nil
}

// locationRows builds the location-import rows: the storage aisles first, then
// the work stations.
func (s *sim) locationRows(stations int) []map[string]any {
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
	return rows
}

// registerStorageBins PUTs every storage bin into inventory-storage and records
// the pick bins and the overflow bin.
func (s *sim) registerStorageBins(ctx context.Context) error {
	for a := 1; a <= storageAisles; a++ {
		for b := 1; b <= storageBays; b++ {
			code := s.storageCode(a, b)
			r := s.api.Call(ctx, svcInventory, http.MethodPut, "/bins/{binId}", "/bins/"+url.PathEscape(code), map[string]any{"capacity": binCapacity})
			if r.Status == http.StatusNotFound || r.Status == http.StatusMethodNotAllowed {
				return fail("inventory-storage has no PUT /bins/{binId} (got %d) — deploy inventory-storage with the bin-registration API (claudioed/inventory-storage#112)", r.Status)
			}
			if !r.OK() && r.Status != http.StatusConflict {
				return fail("PUT /bins/%s -> %d %s", code, r.Status, truncate(string(r.Body), 200))
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
	r := s.api.Call(ctx, svcInventory, http.MethodPost, "/stock-receipts", "/stock-receipts", map[string]any{"sku": sku, "quantity": qty})
	if !r.OK() {
		return fail("receive %d %s -> %d %s", qty, sku, r.Status, truncate(string(r.Body), 200))
	}
	left := qty
	start := s.ri(len(bins))
	for i := 0; left > 0 && i < len(bins)*2; i++ {
		chunk := 25 + s.ri(40)
		if chunk > left {
			chunk = left
		}
		bin := bins[(start+i)%len(bins)]
		r := s.api.Call(ctx, svcInventory, http.MethodPost, "/stowages", "/stowages", map[string]any{"sku": sku, "quantity": chunk, "binId": bin})
		switch {
		case r.OK():
			left -= chunk
		case r.Status == http.StatusConflict:
			continue // bin full or racing — try the next bin
		default:
			return fail("stow %d %s into %s -> %d %s", chunk, sku, bin, r.Status, truncate(string(r.Body), 200))
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
		r := s.api.Call(ctx, svcProductMaster, http.MethodPut, "/products/{sku}", "/products/"+url.PathEscape(sku),
			map[string]any{"description": fmt.Sprintf("Simulated %s (%.2f kg)", strings.ToLower(sd.key), sd.weightKg)})
		if !r.OK() {
			return fail("register %s in product-master -> %d %s", sd.key, r.Status, truncate(string(r.Body), 200))
		}
		if !sd.fragile {
			continue
		}
		r = s.api.Call(ctx, svcProductMaster, http.MethodPut, "/products/{sku}/classification", "/products/"+url.PathEscape(sku)+"/classification",
			map[string]any{"handlingTags": []string{"Fragile"}})
		if !r.OK() {
			return fail("classify %s in product-master -> %d %s", sd.key, r.Status, truncate(string(r.Body), 200))
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
