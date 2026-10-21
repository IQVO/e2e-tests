package main

// Step definitions for features/returns_exceptions.feature: the two
// returns/exceptions contexts end to end, against the real binaries.
//
// What is REAL here: returns-processing, exception-management and
// inventory-storage are the actual binaries. Every operator action (declare
// policy, authorize RMA, open receipt, inspect, triage/assign/resolve) is a
// real REST call; DispositionDecided / ReturnLineStaged / ExceptionResolved
// / ExceptionAutoResolved are read from the services' real outbox-relayed
// topics; the staged handover into inventory-storage and the stow that makes
// the units usable go through inventory-storage's real REST and its real
// RETURN_LINE consumer (the same steps inbound_receiving.feature uses).
//
// What is INJECTED (and the feature file header says so, the same idiom
// inter_warehouse_transfer.feature invented): the four upstream facts the
// scenarios publish themselves as exact CloudEvents 1.0 on the real topics —
// order-management's OrderAllocated (the scenario has no inventory to
// allocate against), fulfillment-execution's PackageManifested (no pick/pack
// run happens) and TaskCPTMissed (no CPT sweep is driven), and
// order-management's OrderRepromised (no repromise policy runs). Both
// services' own rules then run for real against their local copies of
// exactly those injected facts.
//
// Why the assertions discriminate (see
// .claude/skills/discriminating-eventual-assertion): the RMA authorization
// keeps answering 404/409 until BOTH local copies have caught up, so the
// eventual 201 proves the consumers ran; occurrence 2 with a SECOND
// CloudEvents id proves the folding key (source type + work unit) did the
// work, not the id dedupe; the auto-resolve rule id is asserted in full on
// both the REST read and the published event, and the second repromise must
// leave exactly ONE ExceptionAutoResolved on the topic.
//
// The suite shares ONE world across scenarios (see InitializeScenario); all
// mutable ids are run-scoped through "<run>", so re-runs against a dirty
// database never collide.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/cucumber/godog"
	"github.com/segmentio/kafka-go"
)

var (
	returnsBaseURL    = envOrDefault("RETURNS_BASE_URL", "http://localhost:8110")
	exceptionsBaseURL = envOrDefault("EXCEPTIONS_BASE_URL", "http://localhost:8111")
)

const (
	topicReturnsEvents    = "warehouse.returns-processing.events"
	topicExceptionsEvents = "warehouse.exception-management.events"

	// returns-processing's published types this feature asserts on
	// (byte-identical to its asyncapi).
	typeDispositionDecided = "com.warehouse.wms.returns-processing.disposition.DispositionDecided"
	typeReturnLineStaged   = "com.warehouse.wms.returns-processing.disposition.ReturnLineStaged"

	// The injected upstream facts' FULL types (byte-identical to their
	// producers' asyncapi; both services dispatch on the full type).
	typeOrderAllocatedFull   = "com.warehouse.wes.order-management.order.OrderAllocated"
	typePackageManifestedFul = "com.warehouse.wes.fulfillment-execution.package.PackageManifested"
	typeTaskCPTMissedFull    = "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed"
	typeOrderRepromisedFull  = "com.warehouse.wes.order-management.order.OrderRepromised"

	// exception-management's published types this feature asserts on.
	typeExceptionResolved     = "com.warehouse.wes.exception-management.exceptioncase.ExceptionResolved"
	typeExceptionAutoResolved = "com.warehouse.wes.exception-management.exceptioncase.ExceptionAutoResolved"

	// returnsSite is the shared (run-invariant) site the return comes back
	// to; sites are reference data, never run-scoped.
	returnsSite = "SITE-E2E-RET"
)

// returnsState carries scenario 1's correlation: the run-scoped order/SKU
// and the ids returns-processing minted (RMA, receipt, disposition).
type returnsState struct {
	orderID       string
	sku           string
	rmaID         string
	receiptID     string
	dispositionID string
	// policyVersion is the version the scenario declared, so the
	// DispositionDecided assertion can pin policy_version to it.
	policyVersion int
	// disposition is the REST InspectionResult's disposition, captured at
	// inspect time: returns-processing exposes no GET of a disposition, so
	// the inspect response (the same transaction that decides it) is the
	// read the Then-step asserts against.
	disposition map[string]any
}

// exceptionsState carries scenarios 2 and 3's correlation: the work unit the
// TaskCPTMissed facts are about and the case id exception-management minted.
type exceptionsState struct {
	workUnit string
	caseID   string
}

func (w *world) rt() (*returnsState, error) {
	if w.ret == nil || w.ret.rmaID == "" {
		return nil, fmt.Errorf("no return has been authorized yet in this scenario")
	}
	return w.ret, nil
}

func (w *world) ex() (*exceptionsState, error) {
	if w.exc == nil || w.exc.workUnit == "" {
		return nil, fmt.Errorf("no exception fact has been published yet in this scenario")
	}
	return w.exc, nil
}

// rtInit lazily creates the scenario-1 state (the Background runs before any
// Given of this feature, so the state starts nil).
func (w *world) rtInit() *returnsState {
	if w.ret == nil {
		w.ret = &returnsState{}
	}
	return w.ret
}

func (w *world) exInit() *exceptionsState {
	if w.exc == nil {
		w.exc = &exceptionsState{}
	}
	return w.exc
}

// jsonList decodes a bare JSON array of objects (GET /exceptions answers an
// array, which w.last.json()'s map cannot hold).
func jsonList(body []byte) ([]map[string]any, error) {
	var list []map[string]any
	if err := json.Unmarshal(body, &list); err != nil {
		return nil, fmt.Errorf("decode JSON array: %w (body: %s)", err, body)
	}
	return list, nil
}

// countKafkaEvents walks the WHOLE topic (the scanTopic throwaway-group
// idiom) and counts CloudEvents of exactly fullType whose payload satisfies
// pred. "Exactly N events" assertions need the count, not the first match.
func countKafkaEvents(topic, fullType string, pred func(map[string]any) bool) (int, error) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: kafkaBrokerList(), Topic: topic,
		GroupID:        scanGroupID(topic),
		StartOffset:    kafka.FirstOffset,
		CommitInterval: time.Second,
		MinBytes:       1, MaxBytes: 10 << 20, MaxWait: 250 * time.Millisecond,
	})
	defer func() { _ = r.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), kafkaScanWindow)
	defer cancel()
	count := 0
	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.DeadlineExceeded) {
				return count, nil
			}
			return count, fmt.Errorf("kafka read %s: %w", topic, err)
		}
		var m ceMessage
		if json.Unmarshal(msg.Value, &m) != nil || m.Type != fullType {
			continue
		}
		if pred(m.payload()) {
			count++
		}
	}
}

// ---------------------------------------------------------------------
// Background
// ---------------------------------------------------------------------

func (w *world) returnsExceptionsServicesAreHealthy() error {
	for name, base := range map[string]string{
		"returns-processing":   returnsBaseURL,
		"exception-management": exceptionsBaseURL,
		"inventory-storage":    inventoryBaseURL,
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
// Scenario 1: returns-processing restocks through inventory-storage
// ---------------------------------------------------------------------

// declareDispositionPolicy PUTs the NEXT policy version (versions are
// append-only and the PUT must be current+1), reading the current version
// first so a re-run against a dirty database declares current+1 again
// instead of failing with concurrent-modification. The rule table is the
// openapi's own example table; the scenario's grade-A line hits the
// productClass "*" default (the SKU is never classified).
func (w *world) declareDispositionPolicy(windowDays, capUnits int) error {
	st := w.rtInit()
	version := 1
	if err := w.doJSON(http.MethodGet, returnsBaseURL+"/disposition-policies/current", nil); err != nil {
		return err
	}
	switch w.last.status {
	case http.StatusOK:
		v, ok := intOf(w.last.json()["version"])
		if !ok {
			return fmt.Errorf("current policy version: %s", w.last.body)
		}
		version = v + 1
	case http.StatusNotFound:
		// no policy declared yet (fresh database): version 1
	default:
		return fmt.Errorf("GET current disposition policy: status %d, body=%s", w.last.status, w.last.body)
	}
	if err := w.expectOK2xx(w.doJSON(http.MethodPut,
		fmt.Sprintf("%s/disposition-policies/%d", returnsBaseURL, version),
		map[string]any{
			"returnWindowDays": windowDays,
			"maxUnitsPerLine":  capUnits,
			"vetoOutcome":      "DISPOSE",
			"rules": []map[string]any{
				{"grade": "A", "productClass": "*", "outcome": "RESTOCK"},
				{"grade": "B", "productClass": "*", "outcome": "RESTOCK"},
				{"grade": "C", "productClass": "*", "outcome": "LIQUIDATE"},
				{"grade": "SCRAP", "productClass": "*", "outcome": "DISPOSE"},
			},
		})); err != nil {
		return err
	}
	if w.last.status != http.StatusCreated {
		return fmt.Errorf("declare disposition policy v%d: status %d, body=%s", version, w.last.status, w.last.body)
	}
	got, _ := intOf(w.last.json()["version"])
	if got != version {
		return fmt.Errorf("declared policy version %d, want %d", got, version)
	}
	st.policyVersion = version
	return nil
}

// injectOrderAllocated publishes order-management's OrderAllocated for a
// one-line order, exactly the pinned v1 payload (lines carry NO quantity).
//
// INJECTED, NOT REAL: the scenario has no inventory to allocate against, so
// order-management could not produce this fact; returns-processing's
// order-lines consumer copies it and its authorize rule then runs for real.
func (w *world) injectOrderAllocated(orderID string, lineNo int, sku string) error {
	st := w.rtInit()
	orderID, sku = w.rs(orderID), w.rs(sku)
	st.orderID, st.sku = orderID, sku
	promise := time.Now().UTC().Add(24 * time.Hour).Format(time.RFC3339)
	return publishInjectedCloudEvent(topicOrderEvents,
		"/warehouse/order-management",
		typeOrderAllocatedFull,
		"urn:warehouse:order-management:events:OrderAllocated:v1",
		orderID, time.Now(), map[string]any{
			"order_id":     orderID,
			"promise_date": promise,
			"lines": []map[string]any{{"line_no": lineNo, "sku": sku,
				"path_id": "pick", "gift_wrap": false, "fulfillment_class": "SAME_SKU_MULTI"}},
		})
}

// injectPackageManifested publishes fulfillment-execution's
// PackageManifested for a work unit `{order_id}-line-{n}`.
//
// INJECTED, NOT REAL: no pick/pack run happens in this scenario, so
// fulfillment-execution never manifests anything; returns-processing's
// manifest consumer reverses order_ref into the (order, line) key and the
// authorize rule demands BOTH copies.
func (w *world) injectPackageManifested(workUnit string) error {
	workUnit = w.rs(workUnit)
	packageID := w.rs("pkg-e2e-ret-<run>")
	return publishInjectedCloudEvent(topicFulfillmentEvents,
		"/warehouse/fulfillment-execution",
		typePackageManifestedFul,
		"urn:warehouse:fulfillment-execution:events:PackageManifested:v1",
		packageID, time.Now(), map[string]any{
			"package_id": packageID,
			"order_ref":  workUnit,
		})
}

// returnEventuallyAuthorized POSTs /returns until it answers 201. Until BOTH
// local copies have caught up the service answers 404 (order line not in the
// allocations copy) or 409 return-not-authorizable (no manifested work
// unit); nothing is persisted on refusal, so retrying is safe and the
// eventual 201 is the proof both consumers really ran.
func (w *world) returnEventuallyAuthorized(units int, orderID string, lineNo int, reason string) error {
	st := w.rtInit()
	orderID = w.rs(orderID)
	return eventually(func() error {
		err := w.doJSON(http.MethodPost, returnsBaseURL+"/returns", map[string]any{
			"orderId":    orderID,
			"lineNo":     lineNo,
			"sku":        st.sku,
			"quantity":   units,
			"siteId":     returnsSite,
			"reasonCode": reason,
		})
		if err != nil {
			return err
		}
		switch w.last.status {
		case http.StatusCreated:
			rmaID, _ := w.last.json()["rmaId"].(string)
			if rmaID == "" || !strings.HasPrefix(rmaID, "rma-") {
				return fmt.Errorf("201 without an rmaId: %s", w.last.body)
			}
			if got := w.last.json()["status"]; got != "AUTHORIZED" {
				return fmt.Errorf("RMA status %v, want AUTHORIZED (body: %s)", got, w.last.body)
			}
			st.rmaID = rmaID
			return nil
		case http.StatusNotFound, http.StatusConflict:
			// the documented transient while the local copies catch up
			// (404 line-not-allocated, 409 return-not-authorizable);
			// a refusal persists nothing, so the retry is safe.
			return fmt.Errorf("not authorizable yet (%d): %s", w.last.status, w.last.body)
		default:
			return fmt.Errorf("POST /returns: unexpected status %d, body=%s", w.last.status, w.last.body)
		}
	})
}

// returnReceiptIsOpened opens the receipt against the authorized RMA with
// one MATCHED line, and remembers the minted receipt id.
func (w *world) returnReceiptIsOpened(site string, units int, sku string) error {
	st, err := w.rt()
	if err != nil {
		return err
	}
	sku = w.rs(sku)
	if err := w.expectOK2xx(w.doJSON(http.MethodPost,
		fmt.Sprintf("%s/returns/%s/receipts", returnsBaseURL, st.rmaID),
		map[string]any{
			"siteId": site,
			"lines":  []map[string]any{{"lineNo": 1, "sku": sku, "quantity": units, "kind": "MATCHED", "rmaId": st.rmaID}},
		})); err != nil {
		return err
	}
	if w.last.status != http.StatusCreated {
		return fmt.Errorf("open return receipt: status %d, body=%s", w.last.status, w.last.body)
	}
	receiptID, _ := w.last.json()["receiptId"].(string)
	if receiptID == "" || !strings.HasPrefix(receiptID, "rcpt-") {
		return fmt.Errorf("201 without a receiptId: %s", w.last.body)
	}
	st.receiptID = receiptID
	return nil
}

// returnLineIsInspected grades the line; the SAME transaction decides the
// disposition (and stages a RESTOCK), so the 200's disposition block is the
// decision of record. returns-processing has no GET of a disposition, so the
// block is captured here for the next step's assertions.
func (w *world) returnLineIsInspected(lineNo int, grade string, units int) error {
	st, err := w.rt()
	if err != nil {
		return err
	}
	if err := w.expectOK2xx(w.doJSON(http.MethodPost,
		fmt.Sprintf("%s/receipts/%s/lines/%d/inspect", returnsBaseURL, st.receiptID, lineNo),
		map[string]any{
			"grade":       grade,
			"quantity":    units,
			"reasonCodes": []string{"E2E_GRADE_" + grade},
		})); err != nil {
		return err
	}
	if w.last.status != http.StatusOK {
		return fmt.Errorf("inspect return line: status %d, body=%s", w.last.status, w.last.body)
	}
	d, _ := w.last.json()["disposition"].(map[string]any)
	if d == nil {
		return fmt.Errorf("inspection result carries no disposition: %s", w.last.body)
	}
	st.disposition = d
	st.dispositionID, _ = d["dispositionId"].(string)
	return nil
}

// dispositionIsRESTOCK asserts the captured decision of record: outcome
// RESTOCK for THIS receipt line, the inspected quantity, the run-scoped SKU,
// and staged=true (ReturnLineStaged was raised in the same transaction as
// the decision).
func (w *world) dispositionIsRESTOCK(lineNo int) error {
	st, err := w.rt()
	if err != nil {
		return err
	}
	d := st.disposition
	if d == nil {
		return fmt.Errorf("no line has been inspected in this scenario")
	}
	units, _ := intOf(d["quantity"])
	for field, want := range map[string]any{
		"outcome":   "RESTOCK",
		"grade":     "A",
		"staged":    true,
		"receiptId": st.receiptID,
		"sku":       st.sku,
		"lineNo":    float64(lineNo),
		"quantity":  float64(units),
	} {
		if got := d[field]; got != want {
			return fmt.Errorf("disposition %s = %v, want %v (disposition: %v)", field, got, want, d)
		}
	}
	if !strings.HasPrefix(st.dispositionID, "disp-") {
		return fmt.Errorf("disposition id %q, want a disp- id", st.dispositionID)
	}
	return nil
}

// returnsPublishedDispositionDecided asserts the exact CloudEvents type and
// payload on warehouse.returns-processing.events next to the REST read: the
// published contract and the decision of record must agree.
func (w *world) returnsPublishedDispositionDecided() error {
	st, err := w.rt()
	if err != nil {
		return err
	}
	p, err := expectKafkaEvent(topicReturnsEvents, typeDispositionDecided,
		func(m map[string]any) bool { return m["receipt_id"] == st.receiptID },
		fmt.Sprintf("receipt %s", st.receiptID))
	if err != nil {
		return err
	}
	for field, want := range map[string]any{
		"outcome":        "RESTOCK",
		"grade":          "A",
		"quantity":       float64(3),
		"sku":            st.sku,
		"disposition_id": st.dispositionID,
		"line_no":        float64(1),
	} {
		if got := p[field]; got != want {
			return fmt.Errorf("DispositionDecided %s = %v, want %v (payload: %v)", field, got, want, p)
		}
	}
	if v, ok := intOf(p["policy_version"]); !ok || v != st.policyVersion {
		return fmt.Errorf("DispositionDecided policy_version = %v, want %d (payload: %v)", p["policy_version"], st.policyVersion, p)
	}
	if _, present := p["supersedes"]; present {
		return fmt.Errorf("DispositionDecided supersedes must be absent on a policy decision (payload: %v)", p)
	}
	if _, present := p["reason"]; present {
		return fmt.Errorf("DispositionDecided reason must be absent on a policy decision (payload: %v)", p)
	}
	return nil
}

// returnsPublishedReturnLineStaged asserts the ONLY downstream effect of a
// RESTOCK: ReturnLineStaged with the pinned payload (condition "Good", the
// RMA, the disposition, the site) that inventory-storage's RETURN_LINE
// consumer books as staged stock.
func (w *world) returnsPublishedReturnLineStaged(site string) error {
	st, err := w.rt()
	if err != nil {
		return err
	}
	p, err := expectKafkaEvent(topicReturnsEvents, typeReturnLineStaged,
		func(m map[string]any) bool { return m["disposition_id"] == st.dispositionID },
		fmt.Sprintf("disposition %s", st.dispositionID))
	if err != nil {
		return err
	}
	for field, want := range map[string]any{
		"receipt_id":     st.receiptID,
		"rma_id":         st.rmaID,
		"disposition_id": st.dispositionID,
		"site_id":        site,
		"condition":      "Good",
		"sku":            st.sku,
		"quantity":       float64(3),
		"line_no":        float64(1),
	} {
		if got := p[field]; got != want {
			return fmt.Errorf("ReturnLineStaged %s = %v, want %v (payload: %v)", field, got, want, p)
		}
	}
	return nil
}

// ---------------------------------------------------------------------
// Scenarios 2 and 3: the exception lifecycle and auto-resolve
// ---------------------------------------------------------------------

// declareExceptionPolicy PUTs the handling policy for TaskCPTMissed.
// Versions are append-only and each PUT is the next version, so a re-run
// declares a fresh version that governs the cases it then opens.
func (w *world) declareExceptionPolicy(severity, queue string, slaMinutes int) error {
	return w.declareExceptionPolicyAutoResolve(severity, queue, slaMinutes, "", 0)
}

// declareExceptionPolicyAutoResolve is declareExceptionPolicy with the
// auto-resolve rule (close on OrderRepromised within N minutes).
func (w *world) declareExceptionPolicyAutoResolve(severity, queue string, slaMinutes int, onType string, withinMinutes int) error {
	body := map[string]any{
		"severity":   severity,
		"queue":      queue,
		"slaMinutes": slaMinutes,
	}
	if onType != "" {
		body["autoResolve"] = map[string]any{"onType": onType, "withinMinutes": withinMinutes}
	}
	if err := w.expectOK2xx(w.doJSON(http.MethodPut,
		fmt.Sprintf("%s/exception-policies/%s", exceptionsBaseURL, typeTaskCPTMissedFull), body)); err != nil {
		return err
	}
	if w.last.status != http.StatusCreated {
		return fmt.Errorf("declare exception policy: status %d, body=%s", w.last.status, w.last.body)
	}
	if got := w.last.json()["sourceType"]; got != typeTaskCPTMissedFull {
		return fmt.Errorf("declared policy sourceType %v, want %s", got, typeTaskCPTMissedFull)
	}
	return nil
}

// injectTaskCPTMissed publishes fulfillment-execution's TaskCPTMissed for a
// work unit. Every call mints a FRESH CloudEvents id (the real sweep re-fires
// with a new id per tick), so the pair in scenario 2 proves the folding key
// (source type + work unit) — not the id dedupe — kept the case one.
//
// INJECTED, NOT REAL: no CPT sweep is driven in this scenario.
func (w *world) injectTaskCPTMissed(workUnit string) error {
	st := w.exInit()
	workUnit = w.rs(workUnit)
	st.workUnit = workUnit
	taskID := w.rs("task-e2e-exc-<run>")
	cpt := time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339)
	return publishInjectedCloudEvent(topicFulfillmentEvents,
		"/warehouse/fulfillment-execution",
		typeTaskCPTMissedFull,
		"urn:warehouse:fulfillment-execution:events:TaskCPTMissed:v1",
		taskID, time.Now(), map[string]any{
			"task_id":   taskID,
			"order_ref": workUnit,
			"task_type": "PICK",
			"cpt":       cpt,
		})
}

// injectOrderRepromised publishes order-management's OrderRepromised for the
// SAME order the work unit reverses into (ORDER-level correlation: the case's
// subject `{order_id}-line-1` vs the fact's `order_id`).
//
// INJECTED, NOT REAL: no repromise policy runs in this scenario.
func (w *world) injectOrderRepromised(orderID string) error {
	orderID = w.rs(orderID)
	return publishInjectedCloudEvent(topicOrderEvents,
		"/warehouse/order-management",
		typeOrderRepromisedFull,
		"urn:warehouse:order-management:events:OrderRepromised:v1",
		orderID, time.Now(), map[string]any{
			"order_id":   orderID,
			"cpt_id_old": "sp1-1200",
			"cpt_id_new": "sp1-1800",
			"reason":     "TaskCPTMissed",
		})
}

// exceptionsShowOneCase polls GET /exceptions (filtered by source type),
// narrows to THIS scenario's work unit client-side (the case's
// dedupKey.sourceSubject is the TaskCPTMissed order_ref), and requires
// exactly ONE case with exactly the expected occurrence count. len > 1 is a
// hard failure, never a transient: a second case for the same work unit is
// the bug the scenario exists to catch.
func (w *world) exceptionsShowOneCase(workUnit string, occurrence int) error {
	st := w.exInit()
	workUnit = w.rs(workUnit)
	st.workUnit = workUnit
	return eventually(func() error {
		if err := w.doJSON(http.MethodGet,
			fmt.Sprintf("%s/exceptions?sourceType=%s", exceptionsBaseURL, typeTaskCPTMissedFull), nil); err != nil {
			return err
		}
		if w.last.status != http.StatusOK {
			return fmt.Errorf("GET /exceptions: status %d, body=%s", w.last.status, w.last.body)
		}
		list, err := jsonList(w.last.body)
		if err != nil {
			return err
		}
		mine := []map[string]any{}
		for _, c := range list {
			key, _ := c["dedupKey"].(map[string]any)
			if key == nil {
				continue
			}
			if key["sourceSubject"] == workUnit {
				mine = append(mine, c)
			}
		}
		if len(mine) > 1 {
			return fmt.Errorf("hard failure: %d cases exist for work unit %s", len(mine), workUnit)
		}
		if len(mine) == 0 {
			return fmt.Errorf("no case for work unit %s yet", workUnit)
		}
		got, _ := intOf(mine[0]["occurrenceCount"])
		if got != occurrence {
			return fmt.Errorf("case occurrenceCount %d, want %d (case: %v)", got, occurrence, mine[0])
		}
		caseID, _ := mine[0]["caseId"].(string)
		if !strings.HasPrefix(caseID, "case-") {
			return fmt.Errorf("case id %q", caseID)
		}
		st.caseID = caseID
		return nil
	})
}

// exceptionCaseAction POSTs one lifecycle command and asserts the 200's
// status echo.
func (w *world) exceptionCaseAction(action string, body map[string]any, wantStatus string) error {
	st, err := w.ex()
	if err != nil {
		return err
	}
	if err := w.expectOK2xx(w.doJSON(http.MethodPost,
		fmt.Sprintf("%s/exceptions/%s/%s", exceptionsBaseURL, st.caseID, action), body)); err != nil {
		return err
	}
	if w.last.status != http.StatusOK {
		return fmt.Errorf("POST /exceptions/%s/%s: status %d, body=%s", st.caseID, action, w.last.status, w.last.body)
	}
	if got := w.last.json()["status"]; got != wantStatus {
		return fmt.Errorf("case status %v after %s, want %s (body: %s)", got, action, wantStatus, w.last.body)
	}
	return nil
}

func (w *world) caseIsTriaged(severity, queue string) error {
	return w.exceptionCaseAction("triage", map[string]any{"severity": severity, "queue": queue}, "TRIAGED")
}

func (w *world) caseIsAssigned(assignee string) error {
	return w.exceptionCaseAction("assign", map[string]any{"assignee": assignee}, "ASSIGNED")
}

func (w *world) caseIsResolved(code, rootCause string) error {
	return w.exceptionCaseAction("resolve", map[string]any{"resolutionCode": code, "rootCause": rootCause}, "RESOLVED")
}

// caseStatusIs reads the case back over REST and asserts its status.
func (w *world) caseStatusIs(want string) error {
	st, err := w.ex()
	if err != nil {
		return err
	}
	if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/exceptions/%s", exceptionsBaseURL, st.caseID), nil); err != nil {
		return err
	}
	if w.last.status != http.StatusOK {
		return fmt.Errorf("GET /exceptions/%s: status %d, body=%s", st.caseID, w.last.status, w.last.body)
	}
	if got := w.last.json()["status"]; got != want {
		return fmt.Errorf("case status %v, want %s (body: %s)", got, want, w.last.body)
	}
	return nil
}

// exceptionsPublishedResolved asserts ExceptionResolved on the real topic
// with the resolution code and root cause the operator gave.
func (w *world) exceptionsPublishedResolved(code, rootCause string) error {
	st, err := w.ex()
	if err != nil {
		return err
	}
	p, err := expectKafkaEvent(topicExceptionsEvents, typeExceptionResolved,
		func(m map[string]any) bool { return m["case_id"] == st.caseID },
		fmt.Sprintf("case %s", st.caseID))
	if err != nil {
		return err
	}
	if got := p["resolution_code"]; got != code {
		return fmt.Errorf("ExceptionResolved resolution_code = %v, want %s (payload: %v)", got, code, p)
	}
	if got := p["root_cause"]; got != rootCause {
		return fmt.Errorf("ExceptionResolved root_cause = %v, want %s (payload: %v)", got, rootCause, p)
	}
	return nil
}

// resolvingAgainIsRefused proves RESOLVED is terminal: the second resolve is
// the documented 409 case-already-resolved problem, not a silent replay.
func (w *world) resolvingAgainIsRefused(status int, slug string) error {
	st, err := w.ex()
	if err != nil {
		return err
	}
	if err := w.doJSON(http.MethodPost,
		fmt.Sprintf("%s/exceptions/%s/resolve", exceptionsBaseURL, st.caseID),
		map[string]any{"resolutionCode": "WORK_COMPLETED", "rootCause": "OPERATOR_UNAVAILABLE"}); err != nil {
		return err
	}
	return w.problemIs(status, slug)
}

// caseEventuallyAutoResolved polls the case until the correlated fact closed
// it: status RESOLVED, the FULL derived rule id, and an AUTO_RESOLVED audit
// entry (so a manual resolve could never satisfy this step).
func (w *world) caseEventuallyAutoResolved(rule string) error {
	st, err := w.ex()
	if err != nil {
		return err
	}
	return eventually(func() error {
		if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/exceptions/%s", exceptionsBaseURL, st.caseID), nil); err != nil {
			return err
		}
		if w.last.status != http.StatusOK {
			return fmt.Errorf("GET /exceptions/%s: status %d, body=%s", st.caseID, w.last.status, w.last.body)
		}
		c := w.last.json()
		if got := c["status"]; got != "RESOLVED" {
			return fmt.Errorf("case status %v, want RESOLVED", got)
		}
		if got := c["autoResolveRuleId"]; got != rule {
			return fmt.Errorf("autoResolveRuleId %v, want %s (body: %s)", got, rule, w.last.body)
		}
		audit, _ := c["auditTrail"].([]any)
		sawAuto := false
		for _, a := range audit {
			e, _ := a.(map[string]any)
			if e["action"] == "AUTO_RESOLVED" {
				sawAuto = true
			}
		}
		if !sawAuto {
			return fmt.Errorf("no AUTO_RESOLVED audit entry (body: %s)", w.last.body)
		}
		return nil
	})
}

// exceptionsPublishedAutoResolved asserts the ExceptionAutoResolved event on
// the real topic with the FULL derived rule id.
func (w *world) exceptionsPublishedAutoResolved() error {
	st, err := w.ex()
	if err != nil {
		return err
	}
	rule := typeTaskCPTMissedFull + "->" + typeOrderRepromisedFull
	p, err := expectKafkaEvent(topicExceptionsEvents, typeExceptionAutoResolved,
		func(m map[string]any) bool { return m["case_id"] == st.caseID },
		fmt.Sprintf("case %s", st.caseID))
	if err != nil {
		return err
	}
	if got := p["rule_id"]; got != rule {
		return fmt.Errorf("ExceptionAutoResolved rule_id = %v, want %s (payload: %v)", got, rule, p)
	}
	return nil
}

// exactlyOneAutoResolvedEvent rescans the WHOLE topic after the second
// repromise (settling first, the same idiom expectNoKafkaEvent uses, so the
// correlation consumer has had time to wrongly fire) and requires exactly
// ONE ExceptionAutoResolved for this case: the second, different-id
// repromise must be a no-op on a resolved case.
func (w *world) exactlyOneAutoResolvedEvent() error {
	st, err := w.ex()
	if err != nil {
		return err
	}
	time.Sleep(kafkaAbsenceWindow)
	count, err := countKafkaEvents(topicExceptionsEvents, typeExceptionAutoResolved,
		func(m map[string]any) bool { return m["case_id"] == st.caseID })
	if err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("case %s has %d ExceptionAutoResolved events, want exactly 1", st.caseID, count)
	}
	return nil
}

// ---------------------------------------------------------------------
// registration
// ---------------------------------------------------------------------

func registerReturnsSteps(sc *godog.ScenarioContext, w *world) {
	// Background
	sc.Step(`^returns-processing, exception-management and inventory-storage are healthy$`, w.returnsExceptionsServicesAreHealthy)

	// Scenario 1: returns-restock
	sc.Step(`^a disposition policy with window (\d+) days? and cap (\d+) units? per line is declared in returns-processing$`, w.declareDispositionPolicy)
	sc.Step(`^order-management's OrderAllocated for order "([^"]*)" line (\d+) of SKU "([^"]*)" is published$`, w.injectOrderAllocated)
	sc.Step(`^fulfillment-execution's PackageManifested for work unit "([^"]*)" is published$`, w.injectPackageManifested)
	sc.Step(`^a return of (\d+) units? of order "([^"]*)" line (\d+) for reason ([A-Z_]+) is eventually authorized in returns-processing$`, w.returnEventuallyAuthorized)
	sc.Step(`^a receipt at site "([^"]*)" with (\d+) units? of SKU "([^"]*)" on the return is opened in returns-processing$`, w.returnReceiptIsOpened)
	sc.Step(`^line (\d+) of the receipt is inspected with grade ([A-Z]+) and (\d+) units? in returns-processing$`, w.returnLineIsInspected)
	sc.Step(`^the disposition of line (\d+) of the receipt is RESTOCK in returns-processing$`, w.dispositionIsRESTOCK)
	sc.Step(`^returns-processing published DispositionDecided for the receipt of the return with outcome RESTOCK and grade A$`, w.returnsPublishedDispositionDecided)
	sc.Step(`^returns-processing published ReturnLineStaged for the receipt of the return at site "([^"]*)" with condition Good$`, w.returnsPublishedReturnLineStaged)
	// (the staged-handover, usable-0, stow and usable-3 steps are the ones
	// inbound_test.go and e2e_test.go already register.)

	// Scenarios 2 and 3: exception lifecycle and auto-resolve
	sc.Step(`^an exception policy with severity ([A-Z]+) queue "([^"]*)" and SLA (\d+) minutes? is declared for TaskCPTMissed in exception-management$`, w.declareExceptionPolicy)
	sc.Step(`^an exception policy with severity ([A-Z]+) queue "([^"]*)" and SLA (\d+) minutes? and auto-resolve on OrderRepromised within (\d+) minutes? is declared for TaskCPTMissed in exception-management$`, w.declareExceptionPolicyAutoResolve)
	sc.Step(`^fulfillment-execution's TaskCPTMissed for work unit "([^"]*)" is published$`, w.injectTaskCPTMissed)
	sc.Step(`^fulfillment-execution's TaskCPTMissed for work unit "([^"]*)" is published again with a different event id$`, w.injectTaskCPTMissed)
	sc.Step(`^exception-management eventually shows exactly one case for work unit "([^"]*)" with occurrence (\d+)$`, w.exceptionsShowOneCase)
	sc.Step(`^the case is triaged to severity ([A-Z]+) and queue "([^"]*)" in exception-management$`, w.caseIsTriaged)
	sc.Step(`^the case is assigned to "([^"]*)" in exception-management$`, w.caseIsAssigned)
	sc.Step(`^the case is resolved with code "([^"]*)" and root cause "([A-Z0-9_]+)" in exception-management$`, w.caseIsResolved)
	sc.Step(`^the case status is (?:still )?"([A-Z_]+)" in exception-management$`, w.caseStatusIs)
	sc.Step(`^exception-management published ExceptionResolved for the case with code "([^"]*)" and root cause "([A-Z0-9_]+)"$`, w.exceptionsPublishedResolved)
	sc.Step(`^resolving the case again is refused with (\d+) "([^"]*)"$`, w.resolvingAgainIsRefused)
	sc.Step(`^order-management's OrderRepromised for order "([^"]*)" is published$`, w.injectOrderRepromised)
	sc.Step(`^order-management's OrderRepromised for order "([^"]*)" is published again with a different event id$`, w.injectOrderRepromised)
	sc.Step(`^the case is eventually AUTO_RESOLVED by rule "([^"]*)" in exception-management$`, w.caseEventuallyAutoResolved)
	sc.Step(`^exception-management published ExceptionAutoResolved for the case with exactly one event under that rule$`, w.exceptionsPublishedAutoResolved)
	sc.Step(`^exception-management published exactly 1 ExceptionAutoResolved event for the case in total$`, w.exactlyOneAutoResolvedEvent)
}
