package main

// Step definitions for features/inter_warehouse_transfer.feature: the
// inter-warehouse transfer saga across real service binaries.
//
// What is REAL here: network-inventory-planning (NIP), warehouse-planning,
// inventory-storage, wes-work-planning and fulfillment-execution are the
// actual binaries; every hop between them travels over real Kafka as
// CloudEvents 1.0, and the floor work (claim/complete) and the destination
// scan/stow are real REST calls.
//
// What is NOT real (and the feature file header says so): see
// injectSiteCapability / injectSiteDemand / siteHoldsStock /
// binRegisteredAtSite. Each one exists because the owning service has no
// producer or write path for that fact today.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	"github.com/cucumber/godog"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

var (
	nipBaseURL               = envOrDefault("NIP_BASE_URL", "http://localhost:8099")
	warehousePlanningBaseURL = envOrDefault("WAREHOUSE_PLANNING_BASE_URL", "http://localhost:8100")
	nipDBURL                 = envOrDefault("NIP_DB_URL", "postgres://nip@localhost:5451/nip?sslmode=disable")
	nipDBPassword            = envOrDefault("NIP_DB_PASSWORD", "nip")
	kafkaBrokers             = envOrDefault("KAFKA_BROKERS", "localhost:9092")
	transferPickPathID       = envOrDefault("NIP_TRANSFER_PICK_PATH_ID", "pick-transfer")
	transferDispatchPathID   = envOrDefault("NIP_TRANSFER_DISPATCH_PATH_ID", "dispatch-transfer")
	// nipTransferReadMode selects how the saga's state is read:
	//   db   (default) SELECT from NIP's own inter_warehouse_transfer table;
	//   rest GET /v1/transfers/{id} (the read endpoint NIP's parallel branch
	//        feature/read-side-and-mcp adds -- switch it on with
	//        NIP_TRANSFER_READ_MODE=rest once that has merged and the e2e
	//        harness builds NIP from develop).
	nipTransferReadMode = envOrDefault("NIP_TRANSFER_READ_MODE", "db")
)

const (
	topicNIPEvents         = "warehouse.network-inventory-planning.events"
	topicInventoryEvents   = "warehouse.inventory.events"
	topicFulfillmentEvents = "warehouse.fulfillment.events"
	topicFacilityEvents    = "warehouse.facility.events"
	topicOrderEvents       = "warehouse.order-management.events"

	// kafkaScanWindow bounds an "eventually this event is on the topic"
	// assertion; kafkaAbsenceWindow bounds a "never appeared" assertion (the
	// scan reaches the end of the log in milliseconds, so this is the idle
	// time spent proving nothing more is arriving).
	kafkaScanWindow    = 45 * time.Second
	kafkaAbsenceWindow = 4 * time.Second
)

// transferState is the per-scenario saga correlation: ids are minted by NIP
// at approval, so every later step reads them from here.
type transferState struct {
	id         string
	lineID     string
	origin     string
	dest       string
	sku        string
	quantity   int
	pickDemand string
	// dispatchDemand is "<transfer id>:dispatch".
	dispatchDemand string
}

func (w *world) xf() (*transferState, error) {
	if w.xfer == nil || w.xfer.id == "" {
		return nil, fmt.Errorf("no transfer has been approved yet in this scenario")
	}
	return w.xfer, nil
}

// ---------------------------------------------------------------------
// Kafka helpers
// ---------------------------------------------------------------------

func kafkaBrokerList() []string { return strings.Split(kafkaBrokers, ",") }

type ceMessage struct {
	Type    string          `json:"type"`
	Subject string          `json:"subject"`
	Data    json.RawMessage `json:"data"`
}

func (m ceMessage) payload() map[string]any {
	var d map[string]any
	_ = json.Unmarshal(m.Data, &d)
	return d
}

// scanTopic reads partition 0 of topic from its first offset (no consumer
// group, so it never perturbs any service's offsets) and returns the first
// CloudEvent for which match is true. It returns (nil, nil) when window
// elapsed without a match; seen counts the messages read.
func scanTopic(topic string, window time.Duration, match func(ceMessage) bool) (found *ceMessage, seen int, err error) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: kafkaBrokerList(), Topic: topic, Partition: 0,
		MinBytes: 1, MaxBytes: 10 << 20, MaxWait: 250 * time.Millisecond,
	})
	defer func() { _ = r.Close() }()
	if err := r.SetOffset(kafka.FirstOffset); err != nil {
		return nil, 0, fmt.Errorf("kafka set offset on %s: %w", topic, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), window)
	defer cancel()
	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.DeadlineExceeded) {
				return nil, seen, nil
			}
			return nil, seen, fmt.Errorf("kafka read %s: %w", topic, err)
		}
		seen++
		var m ceMessage
		if json.Unmarshal(msg.Value, &m) != nil || m.Type == "" {
			continue
		}
		if match(m) {
			return &m, seen, nil
		}
	}
}

// expectKafkaEvent waits (up to kafkaScanWindow) for a CloudEvent of exactly
// this full type whose payload satisfies pred, and returns its payload.
func expectKafkaEvent(topic, fullType string, pred func(map[string]any) bool, desc string) (map[string]any, error) {
	m, seen, err := scanTopic(topic, kafkaScanWindow, func(m ceMessage) bool {
		return m.Type == fullType && pred(m.payload())
	})
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, fmt.Errorf("no %s (%s) on %s within %s (read %d messages)", fullType, desc, topic, kafkaScanWindow, seen)
	}
	return m.payload(), nil
}

// expectNoKafkaEvent proves no CloudEvent of this full type with a matching
// payload exists on the topic.
func expectNoKafkaEvent(topic, fullType string, pred func(map[string]any) bool, desc string) error {
	m, _, err := scanTopic(topic, kafkaAbsenceWindow, func(m ceMessage) bool {
		return m.Type == fullType && pred(m.payload())
	})
	if err != nil {
		return err
	}
	if m != nil {
		return fmt.Errorf("unexpected %s (%s) on %s: %s", fullType, desc, topic, m.Data)
	}
	return nil
}

func intOf(v any) (int, bool) {
	f, ok := toFloat(v)
	return int(f), ok
}

// publishInjectedCloudEvent publishes ONE CloudEvents 1.0 structured-mode
// message built with the official SDK event package (never a hand-rolled
// envelope), with the fleet's mandatory attributes and Kafka header.
func publishInjectedCloudEvent(topic, source, eventType, dataSchema, subject string, at time.Time, data any) error {
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(uuid.NewString())
	e.SetSource(source)
	e.SetType(eventType)
	e.SetSubject(subject)
	e.SetTime(at.UTC())
	e.SetDataSchema(dataSchema)
	if err := e.SetData("application/json", data); err != nil {
		return fmt.Errorf("set CloudEvent data: %w", err)
	}
	if err := e.Validate(); err != nil {
		return fmt.Errorf("invalid CloudEvent: %w", err)
	}
	value, err := json.Marshal(e)
	if err != nil {
		return err
	}
	wr := &kafka.Writer{
		Addr: kafka.TCP(kafkaBrokerList()...), Topic: topic, Balancer: &kafka.Hash{},
		AllowAutoTopicCreation: true, RequiredAcks: kafka.RequireAll,
	}
	defer func() { _ = wr.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	return wr.WriteMessages(ctx, kafka.Message{
		Key: []byte(subject), Value: value,
		Headers: []kafka.Header{{Key: "content-type", Value: []byte("application/cloudevents+json; charset=UTF-8")}},
	})
}

// ---------------------------------------------------------------------
// Background
// ---------------------------------------------------------------------

func (w *world) transferServicesAreHealthy() error {
	for name, base := range map[string]string{
		"network-inventory-planning": nipBaseURL,
		"warehouse-planning":         warehousePlanningBaseURL,
	} {
		if err := w.doJSON(http.MethodGet, base+"/healthz", nil); err != nil {
			return fmt.Errorf("%s not reachable: %w", name, err)
		}
		if w.last.status < 200 || w.last.status >= 300 {
			return fmt.Errorf("%s /healthz returned %d, body=%s", name, w.last.status, w.last.body)
		}
	}
	return nil
}

// ---------------------------------------------------------------------
// NIP read-model inputs
// ---------------------------------------------------------------------

// injectSiteCapability publishes facility-layout's SiteCapabilityChanged.
//
// INJECTED, NOT REAL: facility-layout declares the event (type, payload,
// asyncapi) but nothing in it emits it -- there is no use case or REST route
// that changes a site's transfer capability. Until one exists the only way
// to give NIP the capability fact is to publish the exact CloudEvent here.
// The capability_revision is a millisecond clock so a re-run's fact always
// supersedes the previous run's (last-write-wins on revision).
func (w *world) injectSiteCapability(site string) error {
	site = w.rs(site)
	return publishInjectedCloudEvent(topicFacilityEvents,
		"/warehouse/facility-layout",
		"com.warehouse.wms.facility-layout.site.SiteCapabilityChanged",
		"urn:warehouse:facility-layout:events:SiteCapabilityChanged:v1",
		site, time.Now(), map[string]any{
			"site_code":                    site,
			"transfer_origin_enabled":      true,
			"transfer_destination_enabled": true,
			"capability_revision":          time.Now().UnixMilli(),
		})
}

// injectSiteDemand publishes order-management's SiteSkuDemandChanged.
//
// INJECTED, NOT REAL: order-management projects every order line to ONE
// statically configured site per deployment (DEMAND_PROJECTION_SITE_ID,
// ADR 0035), but a transfer needs demand facts for TWO sites (the origin
// must carry demand facts too, or NIP drops it from the snapshot), and the
// due_at must fall inside the capacity plan's window. The harness therefore
// states the facts directly.
func (w *world) injectSiteDemand(site string, units int, sku string, hours int) error {
	site, sku = w.rs(site), w.rs(sku)
	due := time.Now().UTC().Add(time.Duration(hours) * time.Hour)
	return publishInjectedCloudEvent(topicOrderEvents,
		"/warehouse/order-management",
		"com.warehouse.wes.order-management.siteskudemand.SiteSkuDemandChanged",
		"urn:warehouse:order-management:events:SiteSkuDemandChanged:v1",
		fmt.Sprintf("ORD-E2E-%s-%s", w.rs("<run>"), site), time.Now(), map[string]any{
			"source_order_id":    fmt.Sprintf("ORD-E2E-%s-%s", w.rs("<run>"), site),
			"line_no":            1,
			"site_id":            site,
			"sku":                sku,
			"demanded_units":     units,
			"due_at":             due.Format(time.RFC3339),
			"state":              "ACTIVE",
			"assignment_version": "static-site-v1",
		})
}

// planningPublishesCapacityPlan drives the REAL warehouse-planning: it
// registers a one-step path, registers a LOCATION constraint of ratePerHour
// ORDER/hour for the site over the window, creates a capacity plan scoped
// to the site (site_id is mandatory, ADR 0012) and publishes it, so its
// outbox relay emits CapacityPlanPublished onto Kafka for NIP to ingest.
func (w *world) planningPublishesCapacityPlan(site string, ratePerHour, hours int) error {
	site = w.rs(site)
	start := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Second)
	end := time.Now().UTC().Add(time.Duration(hours) * time.Hour).Truncate(time.Second)
	const pathID = "e2e-transfer-path"

	if err := w.expectOK2xx(w.doJSON(http.MethodPost, warehousePlanningBaseURL+"/process-paths", map[string]any{
		"id": pathID, "name": "E2E inter-warehouse transfer path", "steps": []string{"PICK"},
	})); err != nil {
		return fmt.Errorf("register process path: %w", err)
	}
	if err := w.expectOK2xx(w.doJSON(http.MethodPost, warehousePlanningBaseURL+"/process-capacities", map[string]any{
		"process_type": "PICK", "location": site,
		"window_start": start.Format(time.RFC3339), "window_end": end.Format(time.RFC3339),
		"constraint_type": "LOCATION", "quantity": ratePerHour, "unit": "ORDER", "period_seconds": 3600,
	})); err != nil {
		return fmt.Errorf("register process capacity: %w", err)
	}
	if err := w.expectOK2xx(w.doJSON(http.MethodPost, warehousePlanningBaseURL+"/capacity-plans", map[string]any{
		"warehouse_id": "WH-" + site, "site_id": site, "location": site,
		"window_start": start.Format(time.RFC3339), "window_end": end.Format(time.RFC3339),
		"path_id": pathID, "assigned_demand": 1,
	})); err != nil {
		return fmt.Errorf("create capacity plan: %w", err)
	}
	planID, _ := w.last.json()["id"].(string)
	if planID == "" {
		return fmt.Errorf("capacity plan response has no id: %s", w.last.body)
	}
	if err := w.expectOK2xx(w.doJSON(http.MethodPost, fmt.Sprintf("%s/capacity-plans/%s/publish", warehousePlanningBaseURL, planID), nil)); err != nil {
		return fmt.Errorf("publish capacity plan %s: %w", planID, err)
	}
	return nil
}

// ---------------------------------------------------------------------
// inventory-storage custody
// ---------------------------------------------------------------------

// siteHoldsStock puts qty units of sku into the site's custody.
//
// PARTLY SEEDED, NOT REAL: stock is received and stowed through
// inventory-storage's real REST, but there is NO endpoint that records a
// site on a bin or stock unit (ADR 0030: site custody is a nullable column;
// the only writers are the destination stow and a future backfill). A
// site-less unit is deliberately unallocatable for transfers, so the origin's
// custody is written directly to Postgres, like every other bin seed in
// this harness (see binExists).
func (w *world) siteHoldsStock(site string, qty int, sku, bin string) error {
	site, sku, bin = w.rs(site), w.rs(sku), w.rs(bin)
	if err := w.binRegisteredAtSite(bin, 1000, site); err != nil {
		return err
	}
	if err := w.receiveStock(qty, sku); err != nil {
		return err
	}
	if err := w.stowStock(qty, sku, bin); err != nil {
		return err
	}
	db, err := dbOpen(inventoryDBURL, inventoryDBPassword)
	if err != nil {
		return err
	}
	defer db.Close()
	res, err := db.ExecContext(context.Background(),
		`UPDATE stock_units SET site_id = $1 WHERE sku = $2 AND bin_id = $3`, site, sku, bin)
	if err != nil {
		return fmt.Errorf("record site custody on stock units: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("no stock unit for SKU %s in bin %s to record custody on", sku, bin)
	}
	return nil
}

// binRegisteredAtSite registers a bin whose site custody is site (direct
// Postgres seed: there is no bin-registration endpoint, and stow-to-bin at
// the destination fails closed on a bin with no / another site).
func (w *world) binRegisteredAtSite(bin string, capacity int, site string) error {
	bin, site = w.rs(bin), w.rs(site)
	db, err := dbOpen(inventoryDBURL, inventoryDBPassword)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.ExecContext(context.Background(),
		`INSERT INTO bins (id, capacity, occupied, site_id) VALUES ($1, $2, 0, $3)
		 ON CONFLICT (id) DO UPDATE SET site_id = EXCLUDED.site_id`, bin, capacity, site)
	return err
}

// ---------------------------------------------------------------------
// NIP approval and saga state
// ---------------------------------------------------------------------

func (w *world) approveTransfer(qty int, sku, origin, dest string) error {
	st := &transferState{origin: w.rs(origin), dest: w.rs(dest), sku: w.rs(sku), quantity: qty}
	body := map[string]any{
		"originSiteId": st.origin, "destinationSiteId": st.dest, "sku": st.sku, "quantity": qty,
		"policyVersion": "e2e-policy-v1", "operatorReason": "e2e inter-warehouse transfer",
		"proposalAsOf": time.Now().UTC().Format(time.RFC3339),
	}
	// NIP's read models are fed by Kafka consumers that may still be
	// ingesting the three facts: while they are incomplete the approval is
	// refused (422 facts-incomplete) and nothing is persisted, so retrying is
	// exactly what an operator would do.
	err := eventually(func() error {
		if err := w.doJSON(http.MethodPost, nipBaseURL+"/v1/transfers:approve", body); err != nil {
			return err
		}
		if w.last.status != http.StatusOK {
			return fmt.Errorf("approve answered %d (body: %s)", w.last.status, w.last.body)
		}
		return nil
	})
	if err != nil {
		return err
	}
	got := w.last.json()
	st.id, _ = got["transferId"].(string)
	st.lineID, _ = got["transferLineId"].(string)
	if st.id == "" || st.lineID == "" {
		return fmt.Errorf("approval response lacks transferId/transferLineId: %s", w.last.body)
	}
	if got["state"] != "ALLOCATING" {
		return fmt.Errorf("approved transfer state = %v, want ALLOCATING (body: %s)", got["state"], w.last.body)
	}
	if got["replayed"] != false {
		return fmt.Errorf("approval unexpectedly reported replayed=%v", got["replayed"])
	}
	st.pickDemand = st.id + ":pick"
	st.dispatchDemand = st.id + ":dispatch"
	w.xfer = st
	return nil
}

// transferStateNow reads the saga state: from NIP's read endpoint when
// NIP_TRANSFER_READ_MODE=rest, otherwise from NIP's own table.
func (w *world) transferStateNow(id string) (string, error) {
	if nipTransferReadMode == "rest" {
		if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/v1/transfers/%s", nipBaseURL, id), nil); err != nil {
			return "", err
		}
		if w.last.status != http.StatusOK {
			return "", fmt.Errorf("GET /v1/transfers/%s answered %d (body: %s)", id, w.last.status, w.last.body)
		}
		state, _ := w.last.json()["state"].(string)
		return state, nil
	}
	db, err := dbOpen(nipDBURL, nipDBPassword)
	if err != nil {
		return "", err
	}
	defer db.Close()
	var state string
	err = db.QueryRowContext(context.Background(),
		`SELECT state FROM inter_warehouse_transfer WHERE transfer_id = $1`, id).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("transfer %s not found in network-inventory-planning", id)
	}
	return state, err
}

// nipEventuallyReportsTransferState is the named seam for the saga's state
// assertion: today it reads NIP's own table (the saga has no read endpoint
// yet); with NIP_TRANSFER_READ_MODE=rest the very same step reads
// GET /v1/transfers/{id}.
func (w *world) nipEventuallyReportsTransferState(want string) error {
	st, err := w.xf()
	if err != nil {
		return err
	}
	return eventually(func() error {
		got, err := w.transferStateNow(st.id)
		if err != nil {
			return err
		}
		if got != want {
			return fmt.Errorf("transfer %s state = %q, want %q", st.id, got, want)
		}
		return nil
	})
}

// ---------------------------------------------------------------------
// Kafka assertions on the saga's own messages
// ---------------------------------------------------------------------

func (w *world) inventoryPublishesAllocated(qty int, site string) error {
	st, err := w.xf()
	if err != nil {
		return err
	}
	site = w.rs(site)
	p, err := expectKafkaEvent(topicInventoryEvents,
		"com.warehouse.wms.inventory-storage.reservation.TransferStockAllocated",
		func(p map[string]any) bool { return p["transfer_id"] == st.id }, "transfer "+st.id)
	if err != nil {
		return err
	}
	if q, _ := intOf(p["quantity"]); q != qty || p["origin_site_id"] != site || p["transfer_line_id"] != st.lineID {
		return fmt.Errorf("TransferStockAllocated = %v, want %d units at %s for line %s", p, qty, site, st.lineID)
	}
	return nil
}

func (w *world) inventoryPublishesRejected(reason string) error {
	st, err := w.xf()
	if err != nil {
		return err
	}
	p, err := expectKafkaEvent(topicInventoryEvents,
		"com.warehouse.wms.inventory-storage.reservation.TransferStockAllocationRejected",
		func(p map[string]any) bool { return p["transfer_id"] == st.id }, "transfer "+st.id)
	if err != nil {
		return err
	}
	if p["reason"] != reason {
		return fmt.Errorf("TransferStockAllocationRejected reason = %v, want %s (payload %v)", p["reason"], reason, p)
	}
	return nil
}

func (w *world) nipReleasesDemand(leg string, qty int, pathID, site string) error {
	st, err := w.xf()
	if err != nil {
		return err
	}
	demand, workKind := st.pickDemand, "TRANSFER_PICK"
	if leg == "dispatch" {
		demand, workKind = st.dispatchDemand, "TRANSFER_DISPATCH"
	}
	// the feature states the configured path ids literally; they must agree
	// with the ones 03-up-services.sh gave NIP (both read env.sh).
	wantPath := transferPickPathID
	if leg == "dispatch" {
		wantPath = transferDispatchPathID
	}
	if pathID != wantPath {
		return fmt.Errorf("feature says path %q but the harness configured %q", pathID, wantPath)
	}
	site = w.rs(site)
	p, err := expectKafkaEvent(topicNIPEvents,
		"com.warehouse.wes.network-inventory-planning.workdemand.WorkDemandReleased",
		func(p map[string]any) bool { return p["demand_id"] == demand }, "demand "+demand)
	if err != nil {
		return err
	}
	if q, _ := intOf(p["quantity"]); q != qty || p["work_kind"] != workKind || p["path_id"] != pathID ||
		p["site_id"] != site || p["transfer_ref"] != st.id || p["sku"] != st.sku {
		return fmt.Errorf("WorkDemandReleased = %v, want %s of %d x %s on path %s at %s for %s",
			p, workKind, qty, st.sku, pathID, site, st.id)
	}
	return nil
}

func (w *world) nipReleasesNoDemand() error {
	st, err := w.xf()
	if err != nil {
		return err
	}
	return expectNoKafkaEvent(topicNIPEvents,
		"com.warehouse.wes.network-inventory-planning.workdemand.WorkDemandReleased",
		func(p map[string]any) bool { return p["transfer_ref"] == st.id }, "transfer "+st.id)
}

func (w *world) fulfillmentPublishesFact(fact string, qty int) error {
	st, err := w.xf()
	if err != nil {
		return err
	}
	p, err := expectKafkaEvent(topicFulfillmentEvents,
		"com.warehouse.wes.fulfillment-execution.transfer."+fact,
		func(p map[string]any) bool { return p["transfer_ref"] == st.id }, "transfer "+st.id)
	if err != nil {
		return err
	}
	if q, _ := intOf(p["quantity"]); q != qty {
		return fmt.Errorf("%s quantity = %v, want %d (payload %v)", fact, p["quantity"], qty, p)
	}
	return nil
}

func (w *world) inventoryPublishesDestinationFact(fact string) error {
	st, err := w.xf()
	if err != nil {
		return err
	}
	_, err = expectKafkaEvent(topicInventoryEvents,
		"com.warehouse.wms.inventory-storage.stock."+fact,
		func(p map[string]any) bool { return p["transfer_id"] == st.id }, "transfer "+st.id)
	return err
}

// ---------------------------------------------------------------------
// wes-work-planning / fulfillment-execution legs
// ---------------------------------------------------------------------

func (w *world) legDemand(leg string) (string, string, error) {
	st, err := w.xf()
	if err != nil {
		return "", "", err
	}
	if leg == "dispatch" {
		return st.dispatchDemand, transferDispatchPathID, nil
	}
	return st.pickDemand, transferPickPathID, nil
}

func (w *world) wesEnqueuesLegAsWorkUnit(leg string) error {
	demand, pathID, err := w.legDemand(leg)
	if err != nil {
		return err
	}
	return eventually(func() error {
		if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/work-units?reference=%s", wesBaseURL, demand), nil); err != nil {
			return err
		}
		var units []map[string]any
		if err := json.Unmarshal(w.last.body, &units); err != nil {
			return fmt.Errorf("decode work units for %s: %w (body %s)", demand, err, w.last.body)
		}
		if len(units) != 1 {
			return fmt.Errorf("want exactly 1 work unit for reference %q, got %d", demand, len(units))
		}
		if units[0]["id"] != demand || units[0]["pathId"] != pathID {
			return fmt.Errorf("work unit = %v, want id %q on path %q", units[0], demand, pathID)
		}
		return nil
	})
}

func (w *world) wesHasNoWorkUnitForTransfer() error {
	st, err := w.xf()
	if err != nil {
		return err
	}
	for _, demand := range []string{st.pickDemand, st.dispatchDemand} {
		if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/work-units?reference=%s", wesBaseURL, demand), nil); err != nil {
			return err
		}
		var units []map[string]any
		if err := json.Unmarshal(w.last.body, &units); err != nil {
			return fmt.Errorf("decode work units for %s: %w (body %s)", demand, err, w.last.body)
		}
		if len(units) != 0 {
			return fmt.Errorf("wes-work-planning has work for rejected transfer reference %q: %s", demand, w.last.body)
		}
	}
	return nil
}

func (w *world) fulfillmentHasNoTaskForTransfer() error {
	st, err := w.xf()
	if err != nil {
		return err
	}
	for _, demand := range []string{st.pickDemand, st.dispatchDemand} {
		if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/tasks?orderRef=%s", fulfillmentBaseURL, demand), nil); err != nil {
			return err
		}
		var tasks []map[string]any
		if err := json.Unmarshal(w.last.body, &tasks); err != nil {
			return fmt.Errorf("decode tasks for %s: %w (body %s)", demand, err, w.last.body)
		}
		if len(tasks) != 0 {
			return fmt.Errorf("fulfillment-execution has a task for rejected transfer reference %q: %s", demand, w.last.body)
		}
	}
	return nil
}

// wesReleasesLegWork releases from the leg's path until it hands out THIS
// transfer's work unit: release is earliest-CPT-first on a pool shared by
// every run, so an older leftover is released first (harmless) until ours.
func (w *world) wesReleasesLegWork(leg string) error {
	demand, pathID, err := w.legDemand(leg)
	if err != nil {
		return err
	}
	return eventually(func() error {
		if err := w.releaseWorkFor(pathID); err != nil {
			return err
		}
		if got := w.last.json()["id"]; got != demand {
			return fmt.Errorf("released another run's work unit %v, still waiting for %q", got, demand)
		}
		return nil
	})
}

func (w *world) fulfillmentCreatesLegTask(leg string) error {
	demand, _, err := w.legDemand(leg)
	if err != nil {
		return err
	}
	return w.fulfillmentEventuallyCreatesTaskFor(demand)
}

func (w *world) stationClaimsLegTask(station, taskType, leg string) error {
	demand, _, err := w.legDemand(leg)
	if err != nil {
		return err
	}
	if err := w.claimNextTaskForOrder(station, taskType, demand); err != nil {
		return err
	}
	return w.claimedTaskIsForOrder(demand)
}

// ---------------------------------------------------------------------
// destination receipt (inventory-storage REST, ADR 0033)
// ---------------------------------------------------------------------

func (w *world) destinationScansReceipt(site string, qty int) error {
	st, err := w.xf()
	if err != nil {
		return err
	}
	site = w.rs(site)
	if err := w.expectOK2xx(w.doJSON(http.MethodPost, fmt.Sprintf("%s/transfers/%s/receipt", inventoryBaseURL, st.lineID), map[string]any{
		"transferId": st.id, "destinationSiteId": site, "sku": st.sku, "receivedQuantity": qty,
	})); err != nil {
		return err
	}
	got := w.last.json()
	if got["state"] != "STAGED" {
		return fmt.Errorf("staged receipt state = %v, want STAGED (body: %s)", got["state"], w.last.body)
	}
	return nil
}

func (w *world) destinationStows(qty int, bin string) error {
	st, err := w.xf()
	if err != nil {
		return err
	}
	bin = w.rs(bin)
	if err := w.expectOK2xx(w.doJSON(http.MethodPost, fmt.Sprintf("%s/transfers/%s/stow", inventoryBaseURL, st.lineID), map[string]any{
		"bins": []map[string]any{{"binId": bin, "quantity": qty}},
	})); err != nil {
		return err
	}
	got := w.last.json()
	if q, _ := intOf(got["stowedQuantity"]); got["state"] != "STOWED" || q != qty {
		return fmt.Errorf("stow result = state %v stowedQuantity %v, want STOWED/%d (body: %s)", got["state"], got["stowedQuantity"], qty, w.last.body)
	}
	return nil
}

// destinationScanOfRejectedLineIsQuarantined proves the compensation is
// airtight on the inventory side too: a scan against the REJECTED line is
// quarantined (422) and raises no usable stock.
func (w *world) destinationScanOfRejectedLineIsQuarantined(site string, qty int) error {
	st, err := w.xf()
	if err != nil {
		return err
	}
	site = w.rs(site)
	if err := w.doJSON(http.MethodPost, fmt.Sprintf("%s/transfers/%s/receipt", inventoryBaseURL, st.lineID), map[string]any{
		"transferId": st.id, "destinationSiteId": site, "sku": st.sku, "receivedQuantity": qty,
	}); err != nil {
		return err
	}
	if w.last.status != http.StatusUnprocessableEntity {
		return fmt.Errorf("scan of rejected line answered %d, want 422 quarantine (body: %s)", w.last.status, w.last.body)
	}
	return nil
}

// ---------------------------------------------------------------------
// registration
// ---------------------------------------------------------------------

func registerTransferSteps(sc *godog.ScenarioContext, w *world) {
	// The suite shares ONE world across scenarios (see InitializeScenario),
	// so the saga correlation is reset explicitly before each scenario.
	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		w.xfer = nil
		return ctx, nil
	})

	sc.Step(`^network-inventory-planning and warehouse-planning are healthy$`, w.transferServicesAreHealthy)

	sc.Step(`^the harness injects facility-layout's SiteCapabilityChanged for site "([^"]*)" with transfer origin and destination enabled$`, w.injectSiteCapability)
	sc.Step(`^the harness injects order-management's SiteSkuDemandChanged for site "([^"]*)": (\d+) units of SKU "([^"]*)" due in (\d+) hours?$`, w.injectSiteDemand)
	sc.Step(`^warehouse-planning publishes a capacity plan for site "([^"]*)" of (\d+) orders per hour covering the next (\d+) hours$`, w.planningPublishesCapacityPlan)

	sc.Step(`^site "([^"]*)" holds (\d+) units of SKU "([^"]*)" stowed in bin "([^"]*)" in inventory-storage$`, w.siteHoldsStock)
	sc.Step(`^bin "([^"]*)" with capacity (\d+) is registered at site "([^"]*)" in inventory-storage$`, w.binRegisteredAtSite)

	sc.Step(`^I approve a transfer of (\d+) units of SKU "([^"]*)" from site "([^"]*)" to site "([^"]*)" in network-inventory-planning$`, w.approveTransfer)
	sc.Step(`^network-inventory-planning eventually reports the transfer as ([A-Z_]+)$`, w.nipEventuallyReportsTransferState)

	sc.Step(`^inventory-storage eventually publishes TransferStockAllocated for the transfer: (\d+) units at site "([^"]*)"$`, w.inventoryPublishesAllocated)
	sc.Step(`^inventory-storage eventually publishes TransferStockAllocationRejected for the transfer with reason ([A-Z_]+)$`, w.inventoryPublishesRejected)
	sc.Step(`^network-inventory-planning eventually releases the (pick|dispatch) demand of (\d+) units on path "([^"]*)" at site "([^"]*)"$`, w.nipReleasesDemand)
	sc.Step(`^network-inventory-planning has released no work demand for the transfer$`, w.nipReleasesNoDemand)
	sc.Step(`^fulfillment-execution eventually publishes (TransferPicked|TransferDispatched) for the transfer with quantity (\d+)$`, w.fulfillmentPublishesFact)
	sc.Step(`^inventory-storage eventually publishes (TransferReceiptStaged|TransferStockStowed) for the transfer$`, w.inventoryPublishesDestinationFact)

	sc.Step(`^wes-work-planning eventually enqueues the transfer's (pick|dispatch) demand as a work unit$`, w.wesEnqueuesLegAsWorkUnit)
	sc.Step(`^wes-work-planning has no work unit for the transfer$`, w.wesHasNoWorkUnitForTransfer)
	sc.Step(`^fulfillment-execution has no task for the transfer$`, w.fulfillmentHasNoTaskForTransfer)
	sc.Step(`^wes-work-planning releases the transfer's (pick|dispatch) work unit$`, w.wesReleasesLegWork)
	sc.Step(`^fulfillment-execution eventually creates a task for the transfer's (pick|dispatch) demand$`, w.fulfillmentCreatesLegTask)
	sc.Step(`^station "([^"]*)" claims the next "([A-Z]+)" task for the transfer's (pick|dispatch) demand in fulfillment-execution$`, w.stationClaimsLegTask)

	sc.Step(`^destination site "([^"]*)" scans (\d+) received units of the transfer into inventory-storage$`, w.destinationScansReceipt)
	sc.Step(`^the received units are stowed (\d+) into bin "([^"]*)" in inventory-storage$`, w.destinationStows)
	sc.Step(`^destination site "([^"]*)" scanning (\d+) units against the transfer's rejected line is quarantined by inventory-storage$`, w.destinationScanOfRejectedLineIsQuarantined)
}
