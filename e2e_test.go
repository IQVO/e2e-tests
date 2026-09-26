// Package main is the e2e-tests harness: a godog (Cucumber for Go) suite
// that drives all 5 already-running warehouse-systems HTTP services (see
// scripts/03-up-services.sh) purely over their real REST APIs, and asserts
// on cross-service Kafka integration effects by polling read models the
// consumers project. It never imports another repo's Go packages — this
// is black-box, over-the-wire, exactly like a human running curl.
package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cucumber/godog"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// ---------------------------------------------------------------------
// Environment (read from e2e-tests/.env via the shell that invokes this
// binary — scripts/04-run-tests.sh exports every var below before
// `go test` runs).
// ---------------------------------------------------------------------

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

var (
	facilityBaseURL    = envOrDefault("FACILITY_BASE_URL", "http://localhost:8081")
	inventoryBaseURL   = envOrDefault("INVENTORY_BASE_URL", "http://localhost:8082")
	wesBaseURL         = envOrDefault("WES_BASE_URL", "http://localhost:8083")
	fulfillmentBaseURL = envOrDefault("FULFILLMENT_BASE_URL", "http://localhost:8084")
	workforceBaseURL   = envOrDefault("WORKFORCE_BASE_URL", "http://localhost:8085")
	opsAgentBaseURL    = envOrDefault("OPS_AGENT_BASE_URL", "http://localhost:8096")
	orderBaseURL       = envOrDefault("ORDER_BASE_URL", "http://localhost:8086")
	processPathBaseURL = envOrDefault("PROCESS_PATH_BASE_URL", "http://localhost:8087")
	laborBaseURL       = envOrDefault("LABOR_BASE_URL", "http://localhost:8088")
	inventoryDBURL     = envOrDefault("INVENTORY_DB_URL", "postgres://inventory@localhost:5442/inventory?sslmode=disable")
	wesDBURL           = envOrDefault("WES_DB_URL", "postgres://wes@localhost:5443/wes?sslmode=disable")
	fulfillmentDBURL   = envOrDefault("FULFILLMENT_DB_URL", "postgres://fulfillment@localhost:5444/fulfillment_execution?sslmode=disable")
	// This harness's own docker-compose.yml sets each service's Postgres
	// password equal to its own username (facility/facility,
	// inventory/inventory, ...) -- see env.sh's DB_URL comment for why
	// the password is never embedded in the URL itself. Overridable via
	// env for anyone running this harness against different local
	// credentials.
	inventoryDBPassword   = envOrDefault("INVENTORY_DB_PASSWORD", "inventory")
	wesDBPassword         = envOrDefault("WES_DB_PASSWORD", "wes")
	fulfillmentDBPassword = envOrDefault("FULFILLMENT_DB_PASSWORD", "fulfillment")
	eventualWaitTimeout   = 30 * time.Second
	eventualWaitPoll      = 500 * time.Millisecond
)

// dbOpen opens a *sql.DB for dsn with password filled in explicitly via
// pgx's own config (rather than the process-global PGPASSWORD env var),
// since this one test binary talks to three different Postgres databases
// with three different passwords in the same process and PGPASSWORD can
// only ever hold one value at a time.
func dbOpen(dsn, password string) (*sql.DB, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	cfg.Password = password
	return stdlib.OpenDB(*cfg), nil
}

// httpResult captures the last HTTP call this scenario made, so later
// steps ("Then the response status is 201") can assert on it.
type httpResult struct {
	status int
	header http.Header
	body   []byte
}

func (r httpResult) json() map[string]any {
	var m map[string]any
	_ = json.Unmarshal(r.body, &m)
	return m
}

// world is the per-scenario state.
type world struct {
	client *http.Client
	last   httpResult

	claimedTaskID string
	orderID       string

	// capturedPromiseDate holds order-management's promiseDate string
	// (RFC3339, exactly as GET /orders/{id} returns it) captured right
	// after allocation by captureOrderPromiseDate, so a later step
	// (orderPromiseDateEventuallyChanges) can prove the missed-CPT
	// re-promise loop (ADR 0014 §5 / ADR 0018) genuinely moved it.
	capturedPromiseDate string

	// disposableSlot is the run-scoped LocationCode that
	// facility_layout_propagation.feature registers and then
	// decommissions. Resolved lazily from the "<disposable>" token (see
	// resolveSlot) and held for the rest of the scenario, so every step
	// addressing that slot agrees on the same code.
	disposableSlot string

	// runSuffix is the per-process value that the "<run>" token in a
	// feature-file identifier expands to (see rs). Resolved lazily and
	// held for the whole run so every step agrees on it.
	runSuffix string

	// soak carries state across soak_backlog_ramp.feature's own three
	// steps (seed pools -> register stations -> run ramp -> print
	// summary) within a single scenario. Left nil by every other
	// feature's scenarios. See soak_test.go.
	soak *soakState
}

func newWorld() *world {
	return &world{client: &http.Client{Timeout: 10 * time.Second}}
}

// doJSON performs method against url with an optional JSON body and
// records the result on w.last.
func (w *world) doJSON(method, url string, body any) error {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("HTTP %s %s: %w", method, url, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	w.last = httpResult{status: resp.StatusCode, header: resp.Header, body: b}
	return nil
}

// ---------------------------------------------------------------------
// Background
// ---------------------------------------------------------------------

func (w *world) allServicesAreHealthy() error {
	for name, base := range map[string]string{
		"facility-layout":       facilityBaseURL,
		"inventory-storage":     inventoryBaseURL,
		"wes-work-planning":     wesBaseURL,
		"fulfillment-execution": fulfillmentBaseURL,
		"workforce-management":  workforceBaseURL,
		"order-management":      orderBaseURL,
	} {
		if err := w.doJSON(http.MethodGet, base+"/healthz", nil); err != nil {
			return fmt.Errorf("%s not reachable: %w", name, err)
		}
		if w.last.status != http.StatusOK {
			return fmt.Errorf("%s /healthz returned %d, body=%s", name, w.last.status, w.last.body)
		}
	}
	return nil
}

// opsAgentIsHealthy is a separate Background step (rather than folded into
// allServicesAreHealthy) so bootstrap.feature's existing Background —
// unaware of warehouse-ops-agent — keeps working unchanged; only T5's own
// scenario requires the agent to be up.
func (w *world) opsAgentIsHealthy() error {
	if err := w.doJSON(http.MethodGet, opsAgentBaseURL+"/healthz", nil); err != nil {
		return fmt.Errorf("warehouse-ops-agent not reachable: %w", err)
	}
	if w.last.status != http.StatusOK {
		return fmt.Errorf("warehouse-ops-agent /healthz returned %d, body=%s", w.last.status, w.last.body)
	}
	return nil
}

// ---------------------------------------------------------------------
// facility-layout steps
// ---------------------------------------------------------------------

// runScopedSlot returns a LocationCode whose BAY segment is unique to this
// process, e.g. "WH2-STOR-AMB-A01-<bay>-01-A".
//
// This exists for one specific reason: the propagation scenario ends by
// DECOMMISSIONING a slot, and facility-layout treats a decommissioned
// LocationCode as a permanently closed record — re-registering it is
// refused. A fixed code therefore makes the scenario destroy its own
// precondition and pass exactly once per database, which is worse than a
// flaky test: the second run fails with a confusing "was never rejected"
// error that looks like a broken Kafka cache rather than exhausted fixture
// data. A run-scoped code keeps the scenario genuinely re-runnable against
// the same long-lived facility-layout database.
//
// The bay segment is used (not a suffix on the position) because every
// segment must match [A-Z0-9] and bay is the natural numeric one.
func runScopedSlot(zoneID, aisle string) string {
	bay := fmt.Sprintf("%06d", time.Now().UnixNano()%1000000)
	return fmt.Sprintf("%s-%s-%s-01-A", zoneID, aisle, bay)
}

// expectOKOr409 accepts a 2xx OR a 409 duplicate. The facility map is
// reference data whose registration is naturally idempotent-by-intent: "this
// site/zone/aisle/slot exists" is the goal, and a 409 means it already does.
// The strict expectOK2xx variants above stay as they are — scenarios that
// assert on CREATION semantics still need a real 201 — but the setup steps
// of THIS scenario only need the map to be in a known state, and must not
// fail merely because the suite has been run before against the same
// long-lived facility-layout database.
func (w *world) expectOKOr409(err error) error {
	if err != nil {
		return err
	}
	if w.last.status == http.StatusConflict {
		return nil
	}
	if w.last.status < 200 || w.last.status >= 300 {
		return fmt.Errorf("expected 2xx or 409, got %d (body: %s)", w.last.status, w.last.body)
	}
	return nil
}

func (w *world) ensureSite(code, name string) error {
	return w.expectOKOr409(w.doJSON(http.MethodPost, facilityBaseURL+"/sites", map[string]any{
		"siteCode": code, "name": name,
	}))
}

func (w *world) ensureLocationType(name string, weightKg, volumeM3 float64) error {
	return w.expectOKOr409(w.doJSON(http.MethodPost, facilityBaseURL+"/location-types", map[string]any{
		"name": name,
		"defaultCapacity": map[string]any{
			"maxWeightKg": weightKg, "maxVolumeM3": volumeM3,
		},
	}))
}

func (w *world) ensureZone(areaCode, zoneCode, siteCode, temperatureClass string, hazmat bool) error {
	return w.expectOKOr409(w.doJSON(http.MethodPost,
		fmt.Sprintf("%s/sites/%s/zones", facilityBaseURL, siteCode), map[string]any{
			"areaCode": areaCode, "zoneCode": zoneCode,
			"temperatureClass": temperatureClass, "hazmat": hazmat,
		}))
}

func (w *world) ensureAmbientZone(areaCode, zoneCode, siteCode, temperatureClass string) error {
	return w.ensureZone(areaCode, zoneCode, siteCode, temperatureClass, false)
}

func (w *world) ensureHazmatZone(areaCode, zoneCode, siteCode, temperatureClass string) error {
	return w.ensureZone(areaCode, zoneCode, siteCode, temperatureClass, true)
}

func (w *world) ensureAisle(aisleCode, zoneID string, sequenceHint int, direction string) error {
	return w.expectOKOr409(w.doJSON(http.MethodPost,
		fmt.Sprintf("%s/zones/%s/aisles", facilityBaseURL, zoneID), map[string]any{
			"aisleCode": aisleCode, "sequenceHint": sequenceHint, "direction": direction,
		}))
}

// ensureLocationSlot registers a slot, tolerating a duplicate. It also
// tolerates a slot that was DECOMMISSIONED by a previous run of this
// scenario: re-registering the same code is rejected, so the scenario's
// final decommission step would otherwise poison every later run.
func (w *world) ensureLocationSlot(locationCode, locationType string) error {
	return w.expectOKOr409(w.doJSON(http.MethodPost, facilityBaseURL+"/locations", map[string]any{
		"locationCode": w.resolveSlot(locationCode), "locationType": locationType,
	}))
}

// resolveSlot maps the literal token "<disposable>" in a feature file onto a
// run-scoped LocationCode (see runScopedSlot), remembering it for the rest of
// the scenario so later steps address the same slot. Any other value is
// passed through verbatim.
func (w *world) resolveSlot(code string) string {
	if !strings.Contains(code, "<disposable>") {
		return code
	}
	if w.disposableSlot == "" {
		w.disposableSlot = runScopedSlot("WH2-STOR-AMB", "A01")
	}
	return w.disposableSlot
}

func (w *world) decommissionLocationSlot(locationCode string) error {
	return w.expectOK2xx(w.doJSON(http.MethodPost,
		fmt.Sprintf("%s/locations/%s/decommission", facilityBaseURL, w.resolveSlot(locationCode)), nil))
}

// ---------------------------------------------------------------------
// inventory-storage steps
// ---------------------------------------------------------------------

// classifyProduct registers a SKU's ProductClassification. handlingTags is
// a comma-separated list of the closed HandlingTag vocabulary (e.g.
// "Hazmat"); inventory-storage is the source of truth for this master data.
func (w *world) classifyProduct(sku, handlingTags string) error {
	tags := []string{}
	for _, t := range strings.Split(handlingTags, ",") {
		if t = strings.TrimSpace(t); t != "" {
			tags = append(tags, t)
		}
	}
	return w.expectOK2xx(w.doJSON(http.MethodPut,
		fmt.Sprintf("%s/products/%s/classification", inventoryBaseURL, sku),
		map[string]any{"handlingTags": tags}))
}

// stowIsRejected asserts a stow is REFUSED, and specifically by the
// placement-rule check rather than by any other failure: a 409 whose RFC-7807
// "type" names the hazmat-zone rule. Asserting the status alone would also
// pass on an unrelated 409, which would silently hide a broken cache.
func (w *world) stowIsRejected(qty int, sku, binID string) error {
	binID = w.resolveSlot(binID)
	if err := w.doJSON(http.MethodPost, inventoryBaseURL+"/stock/stow", map[string]any{
		"sku": sku, "quantity": qty, "binId": binID,
	}); err != nil {
		return err
	}
	if w.last.status != http.StatusConflict {
		return fmt.Errorf("expected 409 (placement rule violation), got %d (body: %s)",
			w.last.status, w.last.body)
	}
	if !bytes.Contains(w.last.body, []byte("hazmat-zone-required")) {
		return fmt.Errorf("expected a hazmat-zone-required problem, got body: %s", w.last.body)
	}
	return nil
}

// stowEventuallySucceeds retries a stow until it is accepted or the deadline
// passes. The retry is the POINT of the step, not incidental flake-hiding:
// inventory-storage's classification cache is fed asynchronously from
// facility-layout's Kafka topic, so a slot registered moments ago becomes
// stowable only once that event has been consumed. An immediate one-shot
// assertion would be testing the propagation delay, not the behaviour.
func (w *world) stowEventuallySucceeds(qty int, sku, binID string) error {
	binID = w.resolveSlot(binID)
	deadline := time.Now().Add(eventualWaitTimeout)
	var lastErr error
	for time.Now().Before(deadline) {
		lastErr = w.expectOK2xx(w.doJSON(http.MethodPost, inventoryBaseURL+"/stock/stow",
			map[string]any{"sku": sku, "quantity": qty, "binId": binID}))
		if lastErr == nil {
			return nil
		}
		time.Sleep(eventualWaitPoll)
	}
	return fmt.Errorf("stow of %s into %s never succeeded within %s: %w",
		sku, binID, eventualWaitTimeout, lastErr)
}

// stowEventuallyRejected is the inverse: retries until the stow is refused by
// the placement rule, used after registering a hazmat zone (the cache must
// LEARN the zone's attributes) or after a decommission.
func (w *world) stowEventuallyRejected(qty int, sku, binID string) error {
	binID = w.resolveSlot(binID)
	deadline := time.Now().Add(eventualWaitTimeout)
	var lastErr error
	for time.Now().Before(deadline) {
		lastErr = w.stowIsRejected(qty, sku, binID)
		if lastErr == nil {
			return nil
		}
		time.Sleep(eventualWaitPoll)
	}
	return fmt.Errorf("stow of %s into %s was never rejected within %s: %w",
		sku, binID, eventualWaitTimeout, lastErr)
}

// binExists seeds a Bin directly in inventory-storage's own Postgres
// database. There is deliberately no "create bin" HTTP endpoint in this
// service (see its README: "Stow requires a bin to exist first"), so
// every consumer of this service — including this e2e harness — seeds
// bins the same way its own unit/integration tests do: a direct insert.
func (w *world) binExists(binID string, capacity int) error {
	binID = w.rs(binID)
	binID = w.resolveSlot(binID)
	db, err := dbOpen(inventoryDBURL, inventoryDBPassword)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.ExecContext(context.Background(),
		`INSERT INTO bins (id, capacity, occupied) VALUES ($1, $2, 0)
		 ON CONFLICT (id) DO NOTHING`, binID, capacity)
	return err
}

func (w *world) receiveStock(qty int, sku string) error {
	sku = w.rs(sku)
	return w.expectOK2xx(w.doJSON(http.MethodPost, inventoryBaseURL+"/stock/receive", map[string]any{
		"sku": sku, "quantity": qty,
	}))
}

func (w *world) stowStock(qty int, sku, binID string) error {
	sku = w.rs(sku)
	binID = w.rs(binID)
	return w.expectOK2xx(w.doJSON(http.MethodPost, inventoryBaseURL+"/stock/stow", map[string]any{
		"sku": sku, "quantity": qty, "binId": binID,
	}))
}

func (w *world) usableInventoryIs(sku string, want int) error {
	sku = w.rs(sku)
	if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/inventory/%s/usable", inventoryBaseURL, sku), nil); err != nil {
		return err
	}
	if w.last.status != http.StatusOK {
		return fmt.Errorf("GET usable inventory: status %d, body=%s", w.last.status, w.last.body)
	}
	got := w.last.json()
	usable, ok := got["usable"]
	if !ok {
		// field name fallback: some services phrase this "usableQuantity"
		usable = got["usableQuantity"]
	}
	gotF, _ := toFloat(usable)
	if int(gotF) != want {
		return fmt.Errorf("usable inventory for %s = %v, want %d (full body: %s)", sku, usable, want, w.last.body)
	}
	return nil
}

// ---------------------------------------------------------------------
// workforce-management steps
// ---------------------------------------------------------------------

func (w *world) startAssociateShift(associateID, certification string) error {
	associateID = w.rs(associateID)
	return w.expectOK2xx(w.doJSON(http.MethodPost, fmt.Sprintf("%s/associates/%s/start-shift", workforceBaseURL, associateID), map[string]any{
		"certifications": []string{certification},
	}))
}

func (w *world) commitShiftPlan(buildingID, shiftID, pathID string, plannedHeads int, plannedRate, plannedHours float64, installedStations int) error {
	return w.expectOK2xx(w.doJSON(http.MethodPost, workforceBaseURL+"/shift-plans", map[string]any{
		"buildingId": buildingID,
		"shiftId":    shiftID,
		"lines": []map[string]any{
			{
				"pathId": pathID, "plannedHeads": plannedHeads, "plannedRate": plannedRate,
				"plannedHours": plannedHours, "installedStations": installedStations,
			},
		},
	}))
}

// staffingGapIsUnderstaffed asserts GET /paths/{pathId}/staffing-gap
// (workforce-management's own staffing-gap read model, the same one its
// get_staffing_gap MCP tool wraps) reports Understaffed=true with the
// expected planned/active head counts.
func (w *world) staffingGapIsUnderstaffed(pathID, buildingID, shiftID string, plannedHeads, activeHeads int) error {
	url := fmt.Sprintf("%s/paths/%s/staffing-gap?buildingId=%s&shiftId=%s", workforceBaseURL, pathID, buildingID, shiftID)
	if err := w.doJSON(http.MethodGet, url, nil); err != nil {
		return err
	}
	if w.last.status != http.StatusOK {
		return fmt.Errorf("GET staffing-gap: status %d, body=%s", w.last.status, w.last.body)
	}
	got := w.last.json()
	understaffed, _ := got["understaffed"].(bool)
	if !understaffed {
		return fmt.Errorf("staffing gap for %s not flagged understaffed (full body: %s)", pathID, w.last.body)
	}
	gotPlanned, _ := toFloat(got["plannedHeads"])
	gotActive, _ := toFloat(got["activeHeads"])
	if int(gotPlanned) != plannedHeads || int(gotActive) != activeHeads {
		return fmt.Errorf("staffing gap for %s = planned %v/active %v, want planned %d/active %d (full body: %s)",
			pathID, got["plannedHeads"], got["activeHeads"], plannedHeads, activeHeads, w.last.body)
	}
	return nil
}

// ---------------------------------------------------------------------
// wes-work-planning steps
// ---------------------------------------------------------------------

// wesHasWorkPoolFor is unused by name once T5 adds an explicit-WIP-limit
// variant below, but bootstrap.feature still calls it for its own
// default-provisioned (WIP limit 1000) pool.
func (w *world) wesHasWorkPoolFor(pathID string) error {
	// A work pool is auto-provisioned by the first enqueue against a path
	// (see openapi.yaml enqueueWorkUnit description) — nothing to do here
	// beyond documenting scenario intent; kept as its own step for
	// readability.
	return nil
}

// wesHasReleaseFedWorkPoolWithWIPLimit seeds a release-fed pool at a known
// WIP limit AND clears any work units left in it by an earlier run.
//
// The clear is what makes this step's promise true. The scenario's whole
// point is to saturate a pool with a WIP limit of 1 and observe the
// ReassignLabor recommendation, so it needs the pool to start EMPTY. Without
// the delete, a previous run's already-released unit still occupies the only
// WIP slot, and the release under test fails with 409 wip-limit-reached
// before the scenario can make its actual assertion — the pool id cannot be
// run-scoped away here, because "pick-t5-imbalance" is pinned in env.sh's
// OPS_AGENT_PATH_TARGETS and the ops-agent steps later in this same scenario
// query that exact path.
//
// Seeded directly in Postgres like the bins (see binExists):
// wes-work-planning has no "create pool with a specific limit" HTTP
// endpoint, and a pool is otherwise auto-provisioned at its 1000/1000
// default on first enqueue.
func (w *world) wesHasReleaseFedWorkPoolWithWIPLimit(pathID string, wipLimit int) error {
	db, err := dbOpen(wesDBURL, wesDBPassword)
	if err != nil {
		return err
	}
	defer db.Close()
	ctx := context.Background()
	if _, err = db.ExecContext(ctx,
		`INSERT INTO work_pools (path_id, mode, wip_limit, alarm_threshold)
		 VALUES ($1, 'ReleaseFed', $2, 0)
		 ON CONFLICT (path_id) DO UPDATE SET mode = 'ReleaseFed', wip_limit = $2, alarm_threshold = 0`,
		pathID, wipLimit); err != nil {
		return err
	}
	if _, err = db.ExecContext(ctx, `DELETE FROM work_pool_entries WHERE path_id = $1`, pathID); err != nil {
		return fmt.Errorf("clearing stale work pool entries for %s: %w", pathID, err)
	}
	return nil
}

func (w *world) enqueueWorkUnit(workUnitID string, hoursFromNow int, reference, pathID string) error {
	workUnitID = w.rs(workUnitID)
	reference = w.rs(reference)
	cpt := time.Now().UTC().Add(time.Duration(hoursFromNow) * time.Hour).Format(time.RFC3339)
	return w.expectOK2xx(w.doJSON(http.MethodPost, fmt.Sprintf("%s/paths/%s/work-units", wesBaseURL, pathID), map[string]any{
		"workUnitId": workUnitID, "cpt": cpt, "reference": reference,
	}))
}

func (w *world) releaseWorkFor(pathID string) error {
	return w.expectOK2xx(w.doJSON(http.MethodPost, fmt.Sprintf("%s/paths/%s/release", wesBaseURL, pathID), nil))
}

// releaseWorkEventuallyReleasesOrderLine is releaseWorkFor targeted at the
// order line currently held on w, looping (like claimNextTaskForOrder
// does on fulfillment-execution's claim-next) until the release actually
// hands out THIS scenario's own work unit rather than an older leftover
// still pending in the SAME shared "pick" pool (order-management's v1
// lines always default to shared.DefaultPathId — see
// wesEventuallyEnqueuesWorkUnitForOrderLine's doc comment — so this
// scenario's work unit sits in the exact same pool
// order_management_choreographed_release.feature's own scenario enqueues
// into but never releases, per this file's documented shared-queue
// discipline). Releasing an unwanted leftover is harmless — it just
// becomes released early, same as leaving a claimed task's lease to
// expire on its own elsewhere in this file — so this keeps calling
// release, earliest-CPT-first, until it reaches this scenario's own entry.
func (w *world) releaseWorkEventuallyReleasesOrderLine(pathID string, lineNo int) error {
	wantWorkUnitID, err := w.deriveOrderLineWorkUnitID(lineNo)
	if err != nil {
		return err
	}
	return eventually(func() error {
		if err := w.releaseWorkFor(pathID); err != nil {
			return err
		}
		got := w.last.json()
		if got["id"] != wantWorkUnitID {
			return fmt.Errorf("released another scenario's work unit %v, still waiting for %q", got["id"], wantWorkUnitID)
		}
		return nil
	})
}

func (w *world) releasedWorkUnitIs(id string) error {
	id = w.rs(id)
	got := w.last.json()
	if got["id"] != id {
		return fmt.Errorf("released work unit id = %v, want %q (full body: %s)", got["id"], id, w.last.body)
	}
	return nil
}

// wesEventuallyObservesLaborPlan polls GET /paths/{pathId}/labor-plan-view
// until it 200s with the expected plannedHeads, tolerating 404 while the
// Kafka consumer catches up.
func (w *world) wesEventuallyObservesLaborPlan(pathID string, plannedHeads int) error {
	return eventually(func() error {
		if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/paths/%s/labor-plan-view", wesBaseURL, pathID), nil); err != nil {
			return err
		}
		if w.last.status != http.StatusOK {
			return fmt.Errorf("status %d (body=%s)", w.last.status, w.last.body)
		}
		got := w.last.json()
		gotHeads, _ := toFloat(got["plannedHeads"])
		if int(gotHeads) != plannedHeads {
			return fmt.Errorf("plannedHeads = %v, want %d", got["plannedHeads"], plannedHeads)
		}
		return nil
	})
}

// wesEventuallyReportsCompleted polls GET on the labor-plan-view's sibling
// endpoint isn't available for a single work unit's state directly, so
// this re-releases nothing and instead relies on the fact that
// RecordCompletion is idempotently checked via a 409 "already completed"
// on a repeat POST — the cleanest externally-observable proof this
// service's own state (fed by the TaskCompleted Kafka consumer) reflects
// completion, without requiring a new read endpoint.
func (w *world) wesEventuallyReportsCompleted(workUnitID string) error {
	workUnitID = w.rs(workUnitID)
	return eventually(func() error {
		if err := w.doJSON(http.MethodPost, fmt.Sprintf("%s/work-units/%s/complete", wesBaseURL, workUnitID), nil); err != nil {
			return err
		}
		// 409 = already completed (by the Kafka-driven TaskCompleted
		// consumer) -> success for this assertion.
		if w.last.status == http.StatusConflict {
			return nil
		}
		if w.last.status == http.StatusOK {
			// It hadn't been completed by the consumer yet, and our own
			// call just completed it — that's a false pass; the consumer
			// hasn't run. Fail so the poll keeps retrying (until timeout,
			// at which point the consumer truly never delivered).
			return fmt.Errorf("work unit was not yet completed by the Kafka consumer (this call completed it itself)")
		}
		return fmt.Errorf("unexpected status %d (body=%s)", w.last.status, w.last.body)
	})
}

// wesRebalanceRecommendationIs asserts GET /paths/{pathId}/rebalance's
// action field, the same tool wes-work-planning's own get_rebalance_
// recommendation MCP tool wraps (see its internal/adapters/inbound/mcp
// /tools.go) — checked here over plain REST so this step needs no MCP
// client of its own, while still proving the exact signal warehouse-ops-
// agent's outbound MCP client will read moments later in the scenario.
func (w *world) wesRebalanceRecommendationIs(pathID, action string) error {
	if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/paths/%s/rebalance", wesBaseURL, pathID), nil); err != nil {
		return err
	}
	if w.last.status != http.StatusOK {
		return fmt.Errorf("GET rebalance: status %d, body=%s", w.last.status, w.last.body)
	}
	got := w.last.json()
	if got["action"] != action {
		return fmt.Errorf("rebalance action for %s = %v, want %q (full body: %s)", pathID, got["action"], action, w.last.body)
	}
	return nil
}

// ---------------------------------------------------------------------
// fulfillment-execution steps
// ---------------------------------------------------------------------

// registerStation registers a station with the given capabilities.
//
// It deliberately does NOT purge pending tasks. An earlier version did, to
// stop one scenario claiming another's leftover task, but that raced with
// the scenarios themselves: flow_balance_exception releases its work BEFORE
// registering its stations, so the purge deleted the very task the scenario
// was about to wait for. Cross-scenario interference is handled where it
// actually occurs instead — see claimNextTaskForOrder, which keeps claiming
// until it gets the task its own scenario created.
func (w *world) registerStation(stationID, capabilities string) error {
	stationID = w.rs(stationID)
	return w.expectOK2xx(w.doJSON(http.MethodPost, fulfillmentBaseURL+"/stations", map[string]any{
		"stationId": stationID, "capabilities": strings.Split(capabilities, ","),
	}))
}

// checkInStation assigns occupantID to stationID (fulfillment-execution's
// CheckIn invariant: one occupant at a time). This publishes no domain
// event of its own — the occupant is surfaced, best-effort, as
// associateId on a LATER TaskCompleted event for any task this station
// completes while the occupant remains checked in (see ADR-0014). Used
// by labor_performance.feature to give a completed task a real
// associateId for labor-performance's consumer to score.
func (w *world) checkInStation(associateID, stationID string) error {
	associateID = w.rs(associateID)
	stationID = w.rs(stationID)
	return w.expectOK2xx(w.doJSON(http.MethodPost, fmt.Sprintf("%s/stations/%s/check-in", fulfillmentBaseURL, stationID), map[string]any{
		"occupantId": associateID,
	}))
}

// fulfillmentEventuallyCreatesTaskFor waits until fulfillment-execution has
// consumed the WorkReleased event and created the task for THIS order ref.
//
// It queries GET /tasks?orderRef= rather than the PICK queue's aggregate
// depth. Depth was only ever a proxy for "some task arrived", and it stopped
// being a valid one once scenarios began using run-scoped work unit ids:
// several scenarios share the PICK queue, so a non-zero depth can be another
// scenario's leftover task (passing this step while the real one has not
// arrived), and conversely a depth already drained by a preceding scenario's
// claim made this step fail even though the awaited task existed. Both were
// observed -- the failure moved between bootstrap and labor_performance from
// run to run, which is the signature of a shared-queue race rather than a
// fixture problem.
func (w *world) fulfillmentEventuallyCreatesTaskFor(orderRef string) error {
	orderRef = w.rs(orderRef)
	return eventually(func() error {
		if err := w.doJSON(http.MethodGet,
			fmt.Sprintf("%s/tasks?orderRef=%s", fulfillmentBaseURL, orderRef), nil); err != nil {
			return err
		}
		if w.last.status != http.StatusOK {
			return fmt.Errorf("status %d (body=%s)", w.last.status, w.last.body)
		}
		var tasks []map[string]any
		if err := json.Unmarshal(w.last.body, &tasks); err != nil {
			return fmt.Errorf("decode tasks for %s: %w (body=%s)", orderRef, err, w.last.body)
		}
		if len(tasks) == 0 {
			return fmt.Errorf("no task yet for orderRef %q", orderRef)
		}
		return nil
	})
}

// claimNextTask pulls work for the station, skipping past any task that
// belongs to a DIFFERENT scenario.
//
// claim-next is PULL dispatch: the system hands back the earliest-CPT
// pending task the station is capable of, and the caller cannot ask for a
// specific one — fulfillment-execution's OpenAPI is explicit that this is
// intended semantics ("the system — never the caller — selects which task
// to hand it"), so the harness must accommodate it rather than work around
// it in the service.
//
// Several scenarios in this suite share the PICK queue and run in the same
// process against the same broker, so a task created by an earlier scenario
// can still be pending and, being older, is exactly what claim-next returns.
// Simply claiming once made whichever scenario ran second fail on another
// scenario's orderRef. This claims repeatedly, completing nothing and
// leaving each unwanted task leased (its lease expires on its own), until
// the queue yields a task this station's scenario actually created. That
// keeps the assertion strict — the scenario still proves ITS task was
// created and claimable — instead of relaxing it to "some task arrived".
func (w *world) claimNextTask(stationID, taskType string) error {
	stationID = w.rs(stationID)
	return w.expectOK2xx(w.doJSON(http.MethodPost,
		fmt.Sprintf("%s/stations/%s/claim-next", fulfillmentBaseURL, stationID),
		map[string]any{"taskType": taskType}))
}

// claimNextTaskForOrder is claimNextTask targeted at a known orderRef: it
// keeps claiming until it gets that scenario's own task. See claimNextTask
// for why this is necessary.
func (w *world) claimNextTaskForOrder(stationID, taskType, orderRef string) error {
	stationID = w.rs(stationID)
	orderRef = w.rs(orderRef)
	return eventually(func() error {
		if err := w.expectOK2xx(w.doJSON(http.MethodPost,
			fmt.Sprintf("%s/stations/%s/claim-next", fulfillmentBaseURL, stationID),
			map[string]any{"taskType": taskType})); err != nil {
			return err
		}
		if got := w.last.json(); got["orderRef"] != orderRef {
			return fmt.Errorf("claimed another scenario's task %v, still waiting for %q",
				got["orderRef"], orderRef)
		}
		return nil
	})
}

func (w *world) claimedTaskIsForOrder(orderRef string) error {
	orderRef = w.rs(orderRef)
	got := w.last.json()
	if got["orderRef"] != orderRef {
		return fmt.Errorf("claimed task orderRef = %v, want %q (full body: %s)", got["orderRef"], orderRef, w.last.body)
	}
	id, _ := got["id"].(string)
	w.claimedTaskID = id
	return nil
}

func (w *world) completeClaimedTask(stationID string) error {
	stationID = w.rs(stationID)
	if w.claimedTaskID == "" {
		return fmt.Errorf("no task has been claimed yet this scenario")
	}
	return w.expectOK2xx(w.doJSON(http.MethodPost, fmt.Sprintf("%s/tasks/%s/complete", fulfillmentBaseURL, w.claimedTaskID), map[string]any{
		"stationId": stationID,
	}))
}

// claimedTaskLeaseForcedExpired directly back-dates the currently-claimed
// task's lease_expiry column in fulfillment-execution's own Postgres to a
// moment in the past, so its diagnose_stuck_tasks tool (and this scenario's
// stuck-task signal into warehouse-ops-agent) sees it as already expired
// without needing to wait out the real 5-minute DefaultLeaseDuration (see
// internal/application/usecases/claim_next.go). Same direct-seed pattern
// this harness already uses for inventory-storage bins (see binExists) —
// there is no "force-expire one task's lease" HTTP endpoint; only the bulk
// POST /tasks/expire-leases sweep, which also FREES the task (status back
// to Pending) rather than leaving it Claimed-but-expired, which is
// precisely the "stuck" state diagnose_stuck_tasks is meant to catch.
func (w *world) claimedTaskLeaseForcedExpired() error {
	if w.claimedTaskID == "" {
		return fmt.Errorf("no task has been claimed yet this scenario")
	}
	db, err := dbOpen(fulfillmentDBURL, fulfillmentDBPassword)
	if err != nil {
		return err
	}
	defer db.Close()
	past := time.Now().UTC().Add(-1 * time.Hour)
	tag, err := db.ExecContext(context.Background(),
		`UPDATE tasks SET lease_expiry = $1 WHERE id = $2 AND status = 'CLAIMED'`, past, w.claimedTaskID)
	if err != nil {
		return err
	}
	if n, _ := tag.RowsAffected(); n == 0 {
		return fmt.Errorf("no CLAIMED task with id %q found to force-expire", w.claimedTaskID)
	}
	return nil
}

// deriveOrderLineWorkUnitID returns the deterministic work-unit id order-
// management's own frozen "{orderId}-line-{lineNo}" formula produces for
// the order currently held on w.orderID (the SAME derivation
// wesEventuallyEnqueuesWorkUnitForOrderLine already uses). WES's
// EnqueueWorkUnit and fulfillment-execution's own WorkReleased consumer
// (see that repo's internal/adapters/inbound/kafka/consumer.go
// deriveTaskType/orderRef mapping) never rewrite this string — it flows
// through as fulfillment-execution's task OrderRef verbatim — so this one
// helper is enough to know which task belongs to this scenario's order
// line without the feature file ever needing to know order-management's
// real, dynamically-generated order id.
func (w *world) deriveOrderLineWorkUnitID(lineNo int) (string, error) {
	if w.orderID == "" {
		return "", fmt.Errorf("no order has been placed yet this scenario")
	}
	return fmt.Sprintf("%s-line-%d", w.orderID, lineNo), nil
}

// fulfillmentEventuallyCreatesTaskForOrderLine is
// fulfillmentEventuallyCreatesTaskFor targeted at the order currently held
// on w, deriving the deterministic orderRef itself (see
// deriveOrderLineWorkUnitID) rather than requiring the feature file to
// spell out order-management's real order id.
func (w *world) fulfillmentEventuallyCreatesTaskForOrderLine(lineNo int) error {
	orderRef, err := w.deriveOrderLineWorkUnitID(lineNo)
	if err != nil {
		return err
	}
	return w.fulfillmentEventuallyCreatesTaskFor(orderRef)
}

// theTaskForOrderLineHasCPTForcedIntoThePast directly back-dates the
// still-open task's cpt column (NOT lease_expiry) in fulfillment-
// execution's own Postgres, looked up fresh by order_ref rather than by
// w.claimedTaskID — this task never needs to be claimed for this
// scenario's proof, only overdue. Mirrors claimedTaskLeaseForcedExpired's
// exact style/error discipline (same db.Open pattern, same
// fulfillmentDBURL/fulfillmentDBPassword constants, same
// error-on-zero-rows-affected check), applied to a different column so
// fulfillment-execution's Clock-driven sweep (POST /tasks/sweep-cpt-
// misses, ADR-0025) sees the task as missed without waiting out
// LeadTimePolicy's real (many-hour) lead time. "still open" is Pending or
// Claimed (status != 'COMPLETED'), matching FE's own FindOpenPastCPT
// definition exactly (see internal/adapters/outbound/postgres/task_repo.go).
func (w *world) theTaskForOrderLineHasCPTForcedIntoThePast(lineNo int) error {
	orderRef, err := w.deriveOrderLineWorkUnitID(lineNo)
	if err != nil {
		return err
	}
	db, err := dbOpen(fulfillmentDBURL, fulfillmentDBPassword)
	if err != nil {
		return err
	}
	defer db.Close()
	past := time.Now().UTC().Add(-1 * time.Hour)
	tag, err := db.ExecContext(context.Background(),
		`UPDATE tasks SET cpt = $1 WHERE order_ref = $2 AND status != 'COMPLETED'`, past, orderRef)
	if err != nil {
		return err
	}
	if n, _ := tag.RowsAffected(); n == 0 {
		return fmt.Errorf("no open (Pending/Claimed) task with order_ref %q found to back-date", orderRef)
	}
	return nil
}

// triggerFulfillmentCPTMissedSweep calls POST /tasks/sweep-cpt-misses (no
// request body — see PostSweepCPTMisses), fulfillment-execution's
// Clock-driven sweep (ADR-0025) that detects every still-open task whose
// CPT is now in the past and publishes TaskCPTMissed for each via its own
// transactional outbox (the outbox relay runs automatically in-process —
// no separate trigger needed for the Kafka hop itself).
func (w *world) triggerFulfillmentCPTMissedSweep() error {
	return w.expectOK2xx(w.doJSON(http.MethodPost, fulfillmentBaseURL+"/tasks/sweep-cpt-misses", nil))
}

// fulfillmentReportsAtLeastOneCPTMiss asserts the sweep's own response
// ({"reported": N}) counted at least this scenario's task. It is
// deliberately >= 1, not an exact count: "reported" is a shared,
// process-wide counter another concurrent scenario's own overdue task
// could also contribute to on the same sweep pass (this file's own
// "never assert on an aggregate queue depth as a proxy" lesson, applied
// analogously) — the strict per-task proof is the LATER promise-date
// assertion, which can only move because of THIS scenario's own task.
func (w *world) fulfillmentReportsAtLeastOneCPTMiss() error {
	got := w.last.json()
	reported, ok := toFloat(got["reported"])
	if !ok || reported < 1 {
		return fmt.Errorf("sweep reported = %v, want >= 1 (body: %s)", got["reported"], w.last.body)
	}
	return nil
}

// ---------------------------------------------------------------------
// warehouse-ops-agent steps (T5)
// ---------------------------------------------------------------------

// flowBalanceDecision holds the last GET /flow-balance/{pathId} response,
// decoded from w.last.json() into typed fields so later assertion steps
// don't each re-parse the raw map.
type flowBalanceDecision struct {
	RecommendedAction string
	ProposedHeads     int
	Partial           bool
	Evidence          []map[string]any
}

func (w *world) requestFlowBalanceException(pathID, buildingID, shiftID string) error {
	url := fmt.Sprintf("%s/flow-balance/%s?buildingId=%s&shiftId=%s", opsAgentBaseURL, pathID, buildingID, shiftID)
	if err := w.doJSON(http.MethodGet, url, nil); err != nil {
		return err
	}
	if w.last.status != http.StatusOK {
		return fmt.Errorf("GET flow-balance: status %d, body=%s", w.last.status, w.last.body)
	}
	return nil
}

func (w *world) flowBalanceDecodeOrErr() (flowBalanceDecision, error) {
	got := w.last.json()
	if got == nil {
		return flowBalanceDecision{}, fmt.Errorf("no flow-balance response decoded yet this scenario (body: %s)", w.last.body)
	}
	var d flowBalanceDecision
	d.RecommendedAction, _ = got["recommendedAction"].(string)
	if heads, ok := toFloat(got["proposedHeads"]); ok {
		d.ProposedHeads = int(heads)
	}
	d.Partial, _ = got["partial"].(bool)
	if ev, ok := got["evidence"].([]any); ok {
		for _, e := range ev {
			if m, ok := e.(map[string]any); ok {
				d.Evidence = append(d.Evidence, m)
			}
		}
	}
	return d, nil
}

func (w *world) flowBalanceDecisionIsNotPartial() error {
	d, err := w.flowBalanceDecodeOrErr()
	if err != nil {
		return err
	}
	if d.Partial {
		return fmt.Errorf("flow-balance decision is partial (full body: %s)", w.last.body)
	}
	return nil
}

func (w *world) flowBalanceRecommendedActionIs(action string) error {
	d, err := w.flowBalanceDecodeOrErr()
	if err != nil {
		return err
	}
	if d.RecommendedAction != action {
		return fmt.Errorf("flow-balance recommendedAction = %q, want %q (full body: %s)", d.RecommendedAction, action, w.last.body)
	}
	return nil
}

func (w *world) flowBalanceProposedHeadsIs(want int) error {
	d, err := w.flowBalanceDecodeOrErr()
	if err != nil {
		return err
	}
	if d.ProposedHeads != want {
		return fmt.Errorf("flow-balance proposedHeads = %d, want %d (full body: %s)", d.ProposedHeads, want, w.last.body)
	}
	return nil
}

// flowBalanceEvidenceIncludesAllThreeSources asserts the decision's
// evidence trail names all three upstream MCP tool sources this
// correlation depends on (see internal/application/usecases
// /flow_balance_advisory.go's wesSource/wfmSource/feSource constants),
// proving the exception was correlated from the real MCP call chain, not
// synthesized from a subset.
func (w *world) flowBalanceEvidenceIncludesAllThreeSources() error {
	d, err := w.flowBalanceDecodeOrErr()
	if err != nil {
		return err
	}
	want := map[string]bool{
		"wes-work-planning":     false,
		"workforce-management":  false,
		"fulfillment-execution": false,
	}
	for _, e := range d.Evidence {
		source, _ := e["source"].(string)
		for prefix := range want {
			if strings.HasPrefix(source, prefix) {
				want[prefix] = true
			}
		}
	}
	for prefix, found := range want {
		if !found {
			return fmt.Errorf("flow-balance evidence trail missing a %s.* source (full body: %s)", prefix, w.last.body)
		}
	}
	return nil
}

func (w *world) requestDailyBrief() error {
	if err := w.doJSON(http.MethodGet, opsAgentBaseURL+"/daily-brief", nil); err != nil {
		return err
	}
	if w.last.status != http.StatusOK {
		return fmt.Errorf("GET daily-brief: status %d, body=%s", w.last.status, w.last.body)
	}
	return nil
}

// dailyBriefListsOpenExceptionForPath asserts the daily brief's flattened
// openExceptions list (see internal/domain/policy.DailyBrief.OpenExceptions,
// ranked critical-first) contains at least one entry for the given
// siteCode/pathId — proving E3's independent flow-balance-risk correlation
// rule (>= 2 of backlog/staffing/stuck signals; see internal/domain/policy
// /dailybrief.go's deriveExceptions) also caught the same imbalance E1's
// dedicated FlowBalanceAdvisory correlated moments earlier.
func (w *world) dailyBriefListsOpenExceptionForPath(pathID, siteCode string) error {
	got := w.last.json()
	exceptions, _ := got["openExceptions"].([]any)
	for _, raw := range exceptions {
		e, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if e["pathId"] == pathID && e["siteCode"] == siteCode {
			return nil
		}
	}
	return fmt.Errorf("daily brief has no open exception for pathId=%q siteCode=%q (full body: %s)", pathID, siteCode, w.last.body)
}

// ---------------------------------------------------------------------
// order-management steps (choreographed-release redesign)
//
// order-management's only public REST surface in v1: POST /orders,
// GET /orders/{id}, POST /orders/{id}/retry-allocation, DELETE
// /orders/{id}, GET /healthz. There is no /allocate or /release endpoint
// — placing an order that can be immediately, fully allocated triggers
// allocation (synchronous HTTP to inventory-storage, unchanged) AND
// release (publishing OrderAllocated/OrderPartiallyAllocated to Kafka
// topic warehouse.order-management.events) in the SAME POST /orders
// call. wes-work-planning's 4th consumer subscription
// (handleOrderManagementEvent) turns that into a real work unit via its
// existing EnqueueWorkUnit use case, with the deterministic id
// "{order_id}-line-{line_no}".
// ---------------------------------------------------------------------

// placeOrder issues POST /orders for a single-line order against sku with
// the given quantity and allowPartialShipment, recording both the HTTP
// result (for a following "the response status is 201" step) and the
// order id (for later steps that need it, e.g. the deterministic work
// unit id derivation below).
func (w *world) placeOrder(sku string, quantity int, allowPartialShipment bool) error {
	if err := w.expectOK2xx(w.doJSON(http.MethodPost, orderBaseURL+"/orders", map[string]any{
		"lines":                []map[string]any{{"sku": sku, "quantity": quantity}},
		"allowPartialShipment": allowPartialShipment,
	})); err != nil {
		return err
	}
	got := w.last.json()
	id, _ := got["id"].(string)
	if id == "" {
		return fmt.Errorf("POST /orders response has no id (body: %s)", w.last.body)
	}
	w.orderID = id
	return nil
}

// iPlaceAnOrderForUnitsOfSKU is the godog-facing step: "When I place an
// order for N units of SKU "..." allowing ship-complete only in
// order-management" (allowPartialShipment=false — BR3's default).
func (w *world) iPlaceAnOrderForUnitsOfSKU(quantity int, sku string) error {
	sku = w.rs(sku)
	return w.placeOrder(sku, quantity, false)
}

// theOrderIsAllocated asserts GET /orders/{id} reports every line
// Allocated (or further along — Released is also acceptable, since by
// the time this assertion runs the choreographed release may already
// have happened synchronously within the same POST /orders call).
func (w *world) theOrderIsAllocated() error {
	if w.orderID == "" {
		return fmt.Errorf("no order has been placed yet this scenario")
	}
	if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/orders/%s", orderBaseURL, w.orderID), nil); err != nil {
		return err
	}
	if w.last.status != http.StatusOK {
		return fmt.Errorf("GET /orders/%s status = %d (body: %s)", w.orderID, w.last.status, w.last.body)
	}
	got := w.last.json()
	status, _ := got["status"].(string)
	switch status {
	case "Allocated", "PartiallyAllocated", "Released", "PartiallyReleased":
		return nil
	default:
		return fmt.Errorf("order status = %q, want Allocated (or further along) (body: %s)", status, w.last.body)
	}
}

// wesEventuallyEnqueuesWorkUnitForOrderLine polls wes-work-planning's
// backlog-snapshot endpoint (GET /paths/{pathId}/telemetry) for pathID
// until its BacklogDepth is >= 1, proving order-management's Kafka-
// published OrderAllocated/OrderPartiallyAllocated event was consumed by
// wes-work-planning's 4th subscription and turned into a real work unit
// via EnqueueWorkUnit — the deterministic id this proves exists is
// "{order_id}-line-{lineNo}" (see order-management's WorkUnitID helper
// and wes-work-planning's handleOrderManagementEvent, both frozen to
// this exact format). lineNo is almost always 1 for the single-line
// orders this scenario places.
//
// order-management's line PathID always defaults to shared.DefaultPathId
// ("pick") in v1 — see order-management's CLAUDE.md's Ubiquitous
// Language section — so pathID here is expected to be "pick" unless a
// future order-management change threads a real path selection through.
//
// Parameter order (lineNo, pathID) matches the step regex's capture-group
// order, consistent with every other multi-arg step in this file.
func (w *world) wesEventuallyEnqueuesWorkUnitForOrderLine(lineNo int, pathID string) error {
	if w.orderID == "" {
		return fmt.Errorf("no order has been placed yet this scenario")
	}
	wantWorkUnitID := fmt.Sprintf("%s-line-%d", w.orderID, lineNo)
	return eventually(func() error {
		if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/paths/%s/telemetry", wesBaseURL, pathID), nil); err != nil {
			return err
		}
		if w.last.status != http.StatusOK {
			return fmt.Errorf("status %d (body=%s)", w.last.status, w.last.body)
		}
		got := w.last.json()
		depth, _ := toFloat(got["backlogDepth"])
		if depth < 1 {
			return fmt.Errorf("backlogDepth = %v, want >= 1 (work unit %q not yet enqueued; body=%s)", got["backlogDepth"], wantWorkUnitID, w.last.body)
		}
		return nil
	})
}

// currentOrderPromiseDate GETs /orders/{id} for w.orderID and returns its
// promiseDate string exactly as the wire carries it (RFC3339, omitempty —
// see internal/adapters/inbound/http/dto.go's orderResponse and
// server.go's timeFormat/toOrderResponse), erroring loudly if the field is
// missing entirely: an order that was allocated via LeadTimePolicy always
// has a promise date (LeadTimePolicy.PromiseDate never returns ok=false
// for an allocated line), so an absent field here is a real setup bug
// worth failing on, not a silent no-op.
func (w *world) currentOrderPromiseDate() (string, error) {
	if w.orderID == "" {
		return "", fmt.Errorf("no order has been placed yet this scenario")
	}
	if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/orders/%s", orderBaseURL, w.orderID), nil); err != nil {
		return "", err
	}
	if w.last.status != http.StatusOK {
		return "", fmt.Errorf("GET /orders/%s status = %d (body: %s)", w.orderID, w.last.status, w.last.body)
	}
	got := w.last.json()
	promiseDate, _ := got["promiseDate"].(string)
	if promiseDate == "" {
		return "", fmt.Errorf("order %s has no promiseDate (body: %s)", w.orderID, w.last.body)
	}
	return promiseDate, nil
}

// captureOrderPromiseDate records the order's CURRENT promise date on
// w.capturedPromiseDate, so a later step
// (orderPromiseDateEventuallyChanges) can prove a missed-CPT re-promise
// (ADR 0014 §5 / ADR 0018) genuinely moved it. Must be called once, right
// after allocation, before anything downstream (WES/FE/the sweep) has a
// chance to trigger RepromiseOrder.
func (w *world) captureOrderPromiseDate() error {
	d, err := w.currentOrderPromiseDate()
	if err != nil {
		return err
	}
	w.capturedPromiseDate = d
	return nil
}

// orderPromiseDateEventuallyChanges polls GET /orders/{id} until its
// promiseDate differs from w.capturedPromiseDate — the loop's closing
// proof: fulfillment-execution's CPT-missed sweep published
// TaskCPTMissed, order-management's RepromiseConsumer (ADR 0018) consumed
// it, recomputed the promise fresh via the SAME PromisePolicy the order
// was originally promised with (LeadTimePolicy in this harness — see
// promise_repromise_loop.feature's header comment for why that alone is
// enough to discriminate this scenario), found it moved, and persisted +
// published OrderRepromised. Errors loudly (not silently) if the field
// ever goes missing on a later read, matching captureOrderPromiseDate's
// own fail-loud discipline.
func (w *world) orderPromiseDateEventuallyChanges() error {
	if w.capturedPromiseDate == "" {
		return fmt.Errorf("no promise date was captured yet this scenario")
	}
	return eventually(func() error {
		d, err := w.currentOrderPromiseDate()
		if err != nil {
			return err
		}
		if d == w.capturedPromiseDate {
			return fmt.Errorf("promise date unchanged: still %q", d)
		}
		return nil
	})
}

// ---------------------------------------------------------------------
// labor-performance steps
//
// labor-performance is a PURE Kafka consumer: it has no REST endpoint
// any external caller writes a TaskPerformance to. A scorecard can only
// ever be populated by its own consumer of fulfillment-execution's
// TaskCompleted event (warehouse.fulfillment.events, the same shared
// topic wes-work-planning also consumes) actually running against a
// real completed task. defineStandard is its one REST write (an
// operator setting an engineered labor standard), used here purely as
// scenario setup so the consumed task has a standard to be scored
// against.
// ---------------------------------------------------------------------

func (w *world) laborDefinesStandard(expectedSeconds int, taskType string) error {
	return w.expectOK2xx(w.doJSON(http.MethodPost, laborBaseURL+"/standards", map[string]any{
		"taskType": taskType, "expectedSeconds": expectedSeconds,
	}))
}

// laborEventuallyReportsScorecard polls GET /associates/{id}/scorecard
// until it 200s with taskCount >= minTasks, proving fulfillment-
// execution's TaskCompleted event was actually consumed and scored —
// tolerating 404 while the Kafka consumer catches up.
func (w *world) laborEventuallyReportsScorecard(associateID string, minTasks int) error {
	associateID = w.rs(associateID)
	return eventually(func() error {
		if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/associates/%s/scorecard", laborBaseURL, associateID), nil); err != nil {
			return err
		}
		if w.last.status != http.StatusOK {
			return fmt.Errorf("status %d (body=%s)", w.last.status, w.last.body)
		}
		got := w.last.json()
		count, _ := toFloat(got["taskCount"])
		if int(count) < minTasks {
			return fmt.Errorf("taskCount = %v, want >= %d", got["taskCount"], minTasks)
		}
		return nil
	})
}

// ---------------------------------------------------------------------
// process-path-management steps
//
// process-path-management is the fleet's declared process-path
// catalogue SOURCE: every write here (define/revise/deactivate)
// publishes a ProcessPathCreated/Updated/Deactivated event onto Kafka,
// but this scenario exercises the REST lifecycle directly and asserts
// on this service's OWN read model after each write — proving the
// write side and the resulting state transition work against a real,
// independently running process, not just its own unit/integration
// tests. (No sibling context in this harness runs with
// PATH_CATALOGUE_SOURCE=kafka today — see 03-up-services.sh's own
// comment — so a cross-service consumption proof is a separate,
// future scenario once one does.)
// ---------------------------------------------------------------------

// rs ("run scope") expands the literal token "<run>" in an identifier into
// a value unique to this test process, so a scenario creates fresh entities
// on every run instead of colliding with, or accumulating on top of, what
// an earlier run left in these long-lived databases.
//
// Two distinct failure modes made this necessary, both of which read as
// service bugs rather than exhausted fixture data:
//
//   - Hard collisions. process_path_management.feature asserts a STRICT 201
//     on define and ends by DEACTIVATING the path, which
//     process-path-management treats as terminal and refuses to resurrect;
//     the second run got a 409. The strict assertions are the POINT of that
//     scenario, so the identifier moves rather than the assertions
//     weakening.
//
//   - Silent accumulation, which is worse because it produces a wrong
//     NUMBER rather than an error: bootstrap.feature receives 20 units of a
//     fixed SKU and asserts usable inventory is exactly 20, so a second run
//     saw 40. Scoping the SKU keeps the assertion exact instead of
//     relaxing it to ">= 20", which would have stopped testing the thing it
//     exists to test.
//
// Identifiers that name genuinely SHARED reference data are deliberately
// NOT scoped: the facility map (site/zone/aisle/location type) and the
// process-path catalogue ids like "pick-zone-a" are the fleet's real
// configuration, and every scenario should agree on them. Those use the
// idempotent "... exists in ..." steps instead.
func (w *world) rs(id string) string {
	if !strings.Contains(id, "<run>") {
		return id
	}
	if w.runSuffix == "" {
		w.runSuffix = fmt.Sprintf("%d", time.Now().UnixNano()%100000000)
	}
	return strings.ReplaceAll(id, "<run>", w.runSuffix)
}

// resolvePathID is rs with a process-path-shaped default for the bare token.
func (w *world) resolvePathID(pathID string) string {
	if !strings.Contains(pathID, "<run>") {
		return pathID
	}
	return w.rs("E2E-PROCESS-PATH-<run>")
}

func (w *world) definePath(pathID, matchPrefix, capabilities string) error {
	pathID = w.resolvePathID(pathID)
	return w.expectOK2xx(w.doJSON(http.MethodPost, processPathBaseURL+"/process-paths", map[string]any{
		"pathId": pathID, "matchPrefix": matchPrefix, "requiredCapabilities": strings.Split(capabilities, ","),
	}))
}

func (w *world) getPath(pathID string) error {
	pathID = w.resolvePathID(pathID)
	return w.doJSON(http.MethodGet, fmt.Sprintf("%s/process-paths/%s", processPathBaseURL, pathID), nil)
}

func (w *world) pathStatusIs(pathID, want string) error {
	pathID = w.resolvePathID(pathID)
	if err := w.getPath(pathID); err != nil {
		return err
	}
	if w.last.status != http.StatusOK {
		return fmt.Errorf("GET process-path %s: status %d (body=%s)", pathID, w.last.status, w.last.body)
	}
	got := w.last.json()
	if got["status"] != want {
		return fmt.Errorf("process path %s status = %v, want %q (full body: %s)", pathID, got["status"], want, w.last.body)
	}
	return nil
}

func (w *world) pathResponseMatchPrefixIs(want string) error {
	got := w.last.json()
	if got["matchPrefix"] != want {
		return fmt.Errorf("process path matchPrefix = %v, want %q (full body: %s)", got["matchPrefix"], want, w.last.body)
	}
	return nil
}

func (w *world) revisePath(pathID, matchPrefix, capabilities string) error {
	pathID = w.resolvePathID(pathID)
	return w.expectOK2xx(w.doJSON(http.MethodPut, fmt.Sprintf("%s/process-paths/%s", processPathBaseURL, pathID), map[string]any{
		"matchPrefix": matchPrefix, "requiredCapabilities": strings.Split(capabilities, ","),
	}))
}

func (w *world) deactivatePath(pathID string) error {
	pathID = w.resolvePathID(pathID)
	return w.doJSON(http.MethodDelete, fmt.Sprintf("%s/process-paths/%s", processPathBaseURL, pathID), nil)
}

// listPaths lists process paths, passing ?all=true when all is true (the
// audit view including Deactivated paths) or omitting the query
// parameter entirely otherwise (the default Active-only view).
func (w *world) listPaths(all bool) error {
	url := processPathBaseURL + "/process-paths"
	if all {
		url += "?all=true"
	}
	return w.doJSON(http.MethodGet, url, nil)
}

func (w *world) activeListingIncludes(pathID string) error {
	pathID = w.resolvePathID(pathID)
	if err := w.listPaths(false); err != nil {
		return err
	}
	return w.listingContains(pathID, true)
}

func (w *world) activeListingDoesNotInclude(pathID string) error {
	pathID = w.resolvePathID(pathID)
	if err := w.listPaths(false); err != nil {
		return err
	}
	return w.listingContains(pathID, false)
}

func (w *world) fullListingIncludes(pathID string) error {
	pathID = w.resolvePathID(pathID)
	if err := w.listPaths(true); err != nil {
		return err
	}
	return w.listingContains(pathID, true)
}

func (w *world) listingContains(pathID string, want bool) error {
	if w.last.status != http.StatusOK {
		return fmt.Errorf("GET process-paths: status %d (body=%s)", w.last.status, w.last.body)
	}
	var items []map[string]any
	if err := json.Unmarshal(w.last.body, &items); err != nil {
		return fmt.Errorf("decoding process-paths list: %w (body=%s)", err, w.last.body)
	}
	found := false
	for _, item := range items {
		if item["pathId"] == pathID {
			found = true
			break
		}
	}
	if found != want {
		return fmt.Errorf("process-paths listing contains %q = %v, want %v (full body: %s)", pathID, found, want, w.last.body)
	}
	return nil
}

// ---------------------------------------------------------------------
// generic assertion helpers
// ---------------------------------------------------------------------

func (w *world) responseStatusIs(want int) error {
	if w.last.status != want {
		return fmt.Errorf("response status = %d, want %d (body: %s)", w.last.status, want, w.last.body)
	}
	return nil
}

// expectOK2xx is a convenience wrapper: run the doJSON call, propagate a
// transport error, then require a 2xx status.
func (w *world) expectOK2xx(err error) error {
	if err != nil {
		return err
	}
	if w.last.status < 200 || w.last.status >= 300 {
		return fmt.Errorf("expected 2xx, got %d (body: %s)", w.last.status, w.last.body)
	}
	return nil
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	default:
		return 0, false
	}
}

// eventually retries fn until it returns nil or eventualWaitTimeout
// elapses, returning the last error. Used for every assertion that
// depends on asynchronous Kafka consumer delivery.
func eventually(fn func() error) error {
	deadline := time.Now().Add(eventualWaitTimeout)
	var lastErr error
	for {
		if err := fn(); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("condition not met within %s: %w", eventualWaitTimeout, lastErr)
		}
		time.Sleep(eventualWaitPoll)
	}
}

// ---------------------------------------------------------------------
// godog wiring
// ---------------------------------------------------------------------

func InitializeScenario(sc *godog.ScenarioContext) {
	w := newWorld()
	sc.Before(func(ctx context.Context, s *godog.Scenario) (context.Context, error) {
		w = newWorld()
		return ctx, nil
	})

	sc.Step(`^all warehouse-systems services are healthy$`, w.allServicesAreHealthy)
	sc.Step(`^warehouse-ops-agent is healthy$`, w.opsAgentIsHealthy)

	// facility-layout

	// inventory-storage
	sc.Step(`^I decommission location slot "([^"]*)" in facility-layout$`, w.decommissionLocationSlot)

	// Idempotent "ensure" variants, used by facility_layout_propagation.feature
	// so it can be re-run against a long-lived facility-layout database.
	sc.Step(`^site "([^"]*)" named "([^"]*)" exists in facility-layout$`, w.ensureSite)
	sc.Step(`^location type "([^"]*)" with capacity (\d+) kg and ([\d.]+) m3 exists in facility-layout$`, w.ensureLocationType)
	sc.Step(`^zone "([^"]*)"/"([^"]*)" in site "([^"]*)" with temperature class "([^"]*)" exists in facility-layout$`, w.ensureAmbientZone)
	sc.Step(`^hazmat zone "([^"]*)"/"([^"]*)" in site "([^"]*)" with temperature class "([^"]*)" exists in facility-layout$`, w.ensureHazmatZone)
	sc.Step(`^aisle "([^"]*)" in zone "([^"]*)" with sequence hint (\d+) and direction "([^"]*)" exists in facility-layout$`, w.ensureAisle)
	sc.Step(`^location slot "([^"]*)" of type "([^"]*)" exists in facility-layout$`, w.ensureLocationSlot)

	sc.Step(`^a Bin "([^"]*)" with capacity (\d+) exists in inventory-storage$`, w.binExists)
	sc.Step(`^SKU "([^"]*)" is classified with handling tags "([^"]*)" in inventory-storage$`, w.classifyProduct)
	sc.Step(`^stowing (\d+) units of SKU "([^"]*)" into bin "([^"]*)" in inventory-storage is rejected$`, w.stowIsRejected)
	sc.Step(`^stowing (\d+) units of SKU "([^"]*)" into bin "([^"]*)" in inventory-storage eventually succeeds$`, w.stowEventuallySucceeds)
	sc.Step(`^stowing (\d+) units of SKU "([^"]*)" into bin "([^"]*)" in inventory-storage is eventually rejected$`, w.stowEventuallyRejected)
	sc.Step(`^I receive (\d+) units of SKU "([^"]*)" in inventory-storage$`, w.receiveStock)
	sc.Step(`^I stow (\d+) units of SKU "([^"]*)" into bin "([^"]*)" in inventory-storage$`, w.stowStock)
	sc.Step(`^the usable inventory for SKU "([^"]*)" in inventory-storage is (\d+)$`, w.usableInventoryIs)

	// workforce-management
	sc.Step(`^an associate "([^"]*)" starts a shift with certification "([^"]*)" in workforce-management$`, w.startAssociateShift)
	sc.Step(`^workforce-management commits a shift plan for building "([^"]*)" shift "([^"]*)" path "([^"]*)" with (\d+) planned heads, rate (\d+), hours (\d+), installed stations (\d+)$`, w.commitShiftPlan)
	sc.Step(`^the staffing gap for path "([^"]*)" building "([^"]*)" shift "([^"]*)" in workforce-management is understaffed with (\d+) planned heads and (\d+) active heads$`, w.staffingGapIsUnderstaffed)

	// wes-work-planning
	sc.Step(`^wes-work-planning eventually observes a labor plan view for path "([^"]*)" with planned heads (\d+)$`, w.wesEventuallyObservesLaborPlan)
	sc.Step(`^wes-work-planning has a work pool for process path "([^"]*)"$`, w.wesHasWorkPoolFor)
	sc.Step(`^wes-work-planning has a release-fed work pool for process path "([^"]*)" with WIP limit (\d+)$`, w.wesHasReleaseFedWorkPoolWithWIPLimit)
	sc.Step(`^I enqueue work unit "([^"]*)" with cpt in (\d+) hour(?:s)? and reference "([^"]*)" to process path "([^"]*)" in wes-work-planning$`, w.enqueueWorkUnit)
	sc.Step(`^work is released from process path "([^"]*)" in wes-work-planning$`, w.releaseWorkFor)
	sc.Step(`^work is eventually released from process path "([^"]*)" for the order's line (\d+) in wes-work-planning$`, w.releaseWorkEventuallyReleasesOrderLine)
	sc.Step(`^the released work unit is "([^"]*)"$`, w.releasedWorkUnitIs)
	sc.Step(`^wes-work-planning eventually reports work unit "([^"]*)" as completed$`, w.wesEventuallyReportsCompleted)
	sc.Step(`^wes-work-planning's rebalance recommendation for path "([^"]*)" is "([^"]*)"$`, w.wesRebalanceRecommendationIs)

	// fulfillment-execution
	sc.Step(`^a station "([^"]*)" is registered with capabilities "([^"]*)" in fulfillment-execution$`, w.registerStation)
	sc.Step(`^associate "([^"]*)" checks into station "([^"]*)" in fulfillment-execution$`, w.checkInStation)
	sc.Step(`^fulfillment-execution eventually creates a task for order "([^"]*)"$`, w.fulfillmentEventuallyCreatesTaskFor)
	sc.Step(`^station "([^"]*)" claims the next "([^"]*)" task in fulfillment-execution$`, w.claimNextTask)
	sc.Step(`^station "([^"]*)" claims the next "([^"]*)" task for order "([^"]*)" in fulfillment-execution$`, w.claimNextTaskForOrder)
	sc.Step(`^the claimed task is for order "([^"]*)"$`, w.claimedTaskIsForOrder)
	sc.Step(`^station "([^"]*)" completes the claimed task in fulfillment-execution$`, w.completeClaimedTask)
	sc.Step(`^the claimed task's lease is forced to have already expired in fulfillment-execution$`, w.claimedTaskLeaseForcedExpired)
	sc.Step(`^fulfillment-execution eventually creates a task for the order's line (\d+)$`, w.fulfillmentEventuallyCreatesTaskForOrderLine)
	sc.Step(`^the task for the order's line (\d+) has its CPT forced into the past in fulfillment-execution$`, w.theTaskForOrderLineHasCPTForcedIntoThePast)
	sc.Step(`^I trigger fulfillment-execution's CPT-missed sweep$`, w.triggerFulfillmentCPTMissedSweep)
	sc.Step(`^fulfillment-execution reports at least 1 CPT miss$`, w.fulfillmentReportsAtLeastOneCPTMiss)

	// warehouse-ops-agent (T5)
	sc.Step(`^I request the flow-balance exception for path "([^"]*)" building "([^"]*)" shift "([^"]*)" from warehouse-ops-agent$`, w.requestFlowBalanceException)
	sc.Step(`^the flow-balance decision is not partial$`, w.flowBalanceDecisionIsNotPartial)
	sc.Step(`^the flow-balance recommended action is "([^"]*)"$`, w.flowBalanceRecommendedActionIs)
	sc.Step(`^the flow-balance proposed heads is (\d+)$`, w.flowBalanceProposedHeadsIs)
	sc.Step(`^the flow-balance evidence trail includes sources from wes-work-planning, workforce-management, and fulfillment-execution$`, w.flowBalanceEvidenceIncludesAllThreeSources)
	sc.Step(`^I request the daily brief from warehouse-ops-agent$`, w.requestDailyBrief)
	sc.Step(`^the daily brief lists an open exception for path "([^"]*)" in site "([^"]*)"$`, w.dailyBriefListsOpenExceptionForPath)

	// generic
	sc.Step(`^the response status is (\d+)$`, w.responseStatusIs)

	// order-management (choreographed-release redesign)
	sc.Step(`^I place an order for (\d+) units? of SKU "([^"]*)" allowing ship-complete only in order-management$`, w.iPlaceAnOrderForUnitsOfSKU)
	sc.Step(`^the order is allocated in order-management$`, w.theOrderIsAllocated)
	sc.Step(`^wes-work-planning eventually enqueues a work unit for the order's line (\d+) on process path "([^"]*)"$`, w.wesEventuallyEnqueuesWorkUnitForOrderLine)
	sc.Step(`^I capture the order's current promise date in order-management$`, w.captureOrderPromiseDate)
	sc.Step(`^the order's promise date in order-management eventually changes from the captured value$`, w.orderPromiseDateEventuallyChanges)

	// labor-performance
	sc.Step(`^labor-performance defines a standard of (\d+) expected seconds for task type "([^"]*)"$`, w.laborDefinesStandard)
	sc.Step(`^labor-performance eventually reports a scorecard for associate "([^"]*)" with at least (\d+) tasks? scored$`, w.laborEventuallyReportsScorecard)

	// process-path-management
	sc.Step(`^I define process path "([^"]*)" with match prefix "([^"]*)" and required capabilities "([^"]*)" in process-path-management$`, w.definePath)
	sc.Step(`^I get process path "([^"]*)" from process-path-management$`, w.getPath)
	sc.Step(`^process path "([^"]*)" in process-path-management has status "([^"]*)"$`, w.pathStatusIs)
	sc.Step(`^the process path response match prefix is "([^"]*)"$`, w.pathResponseMatchPrefixIs)
	sc.Step(`^I revise process path "([^"]*)" to match prefix "([^"]*)" and required capabilities "([^"]*)" in process-path-management$`, w.revisePath)
	sc.Step(`^I deactivate process path "([^"]*)" in process-path-management$`, w.deactivatePath)
	sc.Step(`^process-path-management's active process path listing includes "([^"]*)"$`, w.activeListingIncludes)
	sc.Step(`^process-path-management's active process path listing does not include "([^"]*)"$`, w.activeListingDoesNotInclude)
	sc.Step(`^process-path-management's full process path listing includes "([^"]*)"$`, w.fullListingIncludes)

	// soak backlog ramp (features/soak_backlog_ramp.feature, @soak —
	// excluded from the default run, see TestMain's Tags option)
	sc.Step(`^wes-work-planning has release-fed work pools for soak process paths "([^"]*)" and "([^"]*)" with the configured WIP limit$`, w.soakSeedWorkPools)
	sc.Step(`^the configured picker and packer stations are registered in fulfillment-execution for soak$`, w.soakRegisterStations)
	sc.Step(`^backlog is ramped into process paths "([^"]*)" and "([^"]*)" for the configured soak duration with pickers and packers continuously processing$`, w.soakRampBacklog)
	sc.Step(`^the soak run summary is printed$`, w.soakSummaryIsPrinted)
}

func TestMain(m *testing.M) {
	// @soak (features/soak_backlog_ramp.feature) is a long, ramping
	// load run — potentially an hour — and must never run as part of an
	// ordinary `go test`/scripts/04-run-tests.sh invocation. Excluded by
	// default; scripts/06-run-soak.sh sets GODOG_TAGS=@soak to run ONLY
	// that scenario. godog's Tags expression: "~@soak" means "not
	// tagged @soak".
	tags := envOrDefault("GODOG_TAGS", "~@soak")

	suite := godog.TestSuite{
		Name:                "e2e",
		ScenarioInitializer: InitializeScenario,
		Options: &godog.Options{
			Format:   "pretty",
			Paths:    []string{"features"},
			Tags:     tags,
			Strict:   true,
			TestingT: nil,
		},
	}
	if suite.Run() != 0 {
		os.Exit(1)
	}
	os.Exit(0)
}
