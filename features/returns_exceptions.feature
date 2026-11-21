Feature: returns-processing restocks through inventory-storage; exception-management closes the loop
  The two returns/exceptions contexts, end to end over their real REST
  surfaces and real Kafka topics:

  1. RETURNS-RESTOCK. A graded-A return line flows returns-processing ->
     inventory-storage exactly the way inbound-receiving's Good lines do:
     the disposition policy's per-grade table decides RESTOCK, the
     ReturnLineStaged event (the ONLY downstream effect of a RESTOCK, its
     ADR 0002) is consumed by inventory-storage's RETURN_LINE consumer
     (its ADR 0044) as staged stock, and a stow through inventory-storage's
     REST makes the units usable. The staged handover is asserted on the
     StockReceived analytics event (staged stock has no REST read) and then
     moved to usable by the stow, mirroring inbound_receiving.feature.
  2. EXCEPTION-LIFECYCLE. TaskCPTMissed re-fires on every CPT sweep tick
     (by design), and exception-management must fold the repeats into ONE
     case (occurrence 2, never a second case), carry it through the
     operator lifecycle (triage, assign, resolve) and refuse a second
     resolution with 409 case-already-resolved (RESOLVED is terminal).
  3. AUTO-RESOLVE. A policy rule (auto-resolve on OrderRepromised within N
     minutes) closes the case the moment order-management re-promises the
     SAME order (ORDER-level correlation: the case's work-unit subject
     ord-...-line-N is reversed to the order id), with the derived
     deterministic rule id
     TaskCPTMissed->OrderRepromised. A second repromise is a no-op.

  Services started by scripts/03-up-services.sh for this feature:
  returns-processing, exception-management and inventory-storage (with its
  RETURN_LINE_CONSUMER_GROUP). No product-master: the SKU is never
  classified, so the disposition policy's per-grade default table applies
  (grade A -> RESTOCK) and inventory-storage never consults a location
  classification for it (fail-open is the documented behaviour for
  unclassified SKUs).

  What is INJECTED rather than produced by a real service (same idiom as
  inter_warehouse_transfer.feature, which invented it): the four upstream
  facts the scenarios publish themselves as exact CloudEvents 1.0 on the
  real topics -- order-management's OrderAllocated (the scenario has no
  inventory to allocate against), fulfillment-execution's
  PackageManifested (no pick/pack run happens) and TaskCPTMissed (no CPT
  sweep is driven), and order-management's OrderRepromised (no repromise
  policy runs). returns-processing's own rule -- a return authorizes only
  against a line that is BOTH allocated AND manifested -- then runs for
  real against its local copies of exactly those injected facts. Every
  mutable id is run-scoped, so repeats never collide with earlier runs.

  What makes each assertion discriminating:
  - The RMA authorization is refused (409 return-not-authorizable) until
    BOTH local copies have caught up, so the eventual 201 proves the two
    consumers really ran, not just that the REST answered.
  - DispositionDecided and ReturnLineStaged are asserted on
    warehouse.returns-processing.events with their exact CloudEvents types
    and pinned snake_case payloads (rma_id, disposition_id, site_id,
    condition "Good"), next to the REST read of the disposition -- the
    published contract and the read model must agree.
  - The staged stock is proved the same way inbound_receiving.feature
    proves its handover: exactly ONE StockReceived on
    warehouse.inventory.analytics for the run-scoped SKU, usable 0 while
    it is only staged, usable 3 after the stow.
  - The TaskCPTMissed pair carries DIFFERENT CloudEvents ids (the sweep
    re-fires with a new id per tick), so occurrence 2 proves the dedup key
    (source type + work unit) did the folding, not the id.
  - The auto-resolve rule id is asserted in full
    (com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed->
    com.warehouse.wes.order-management.order.OrderRepromised) on BOTH the
    REST read and the ExceptionAutoResolved payload, and exactly ONE such
    event exists for the case after a second repromise.

  Not yet deployable to the warehouse-infra kind cluster: neither
  returns-processing nor exception-management is in the cluster's Kong
  routing today (Kong answers 404 on /api/returns-processing and
  /api/exception-management), so these scenarios run against the
  harness-local binaries -- like every other feature here -- and the
  weekly E2E Behaviour workflow job this feature ships with. Nothing here
  needs the deployed cluster.

  Background:
    Given returns-processing, exception-management and inventory-storage are healthy

  @e2e @returns-exceptions
  Scenario: A graded-A return restocks through inventory-storage's staged handover
    Given a Bin "E2E-RET-BIN-<run>" with capacity 100 exists in inventory-storage
    And a disposition policy with window 30 days and cap 5 units per line is declared in returns-processing
    And order-management's OrderAllocated for order "ord-e2e-ret-<run>" line 1 of SKU "SKU-E2E-RET-<run>" is published
    And fulfillment-execution's PackageManifested for work unit "ord-e2e-ret-<run>-line-1" is published
    When a return of 3 units of order "ord-e2e-ret-<run>" line 1 for reason DAMAGED is eventually authorized in returns-processing
    And a receipt at site "SITE-E2E-RET" with 3 units of SKU "SKU-E2E-RET-<run>" on the return is opened in returns-processing
    And line 1 of the receipt is inspected with grade A and 3 units in returns-processing
    Then the disposition of line 1 of the receipt is RESTOCK in returns-processing
    And returns-processing published DispositionDecided for the receipt of the return with outcome RESTOCK and grade A
    And returns-processing published ReturnLineStaged for the receipt of the return at site "SITE-E2E-RET" with condition Good
    And inventory-storage eventually stages 3 units of SKU "SKU-E2E-RET-<run>" in exactly 1 StockReceived event
    And the usable inventory for SKU "SKU-E2E-RET-<run>" in inventory-storage is 0
    When I stow 3 units of SKU "SKU-E2E-RET-<run>" into bin "E2E-RET-BIN-<run>" in inventory-storage
    Then the usable inventory for SKU "SKU-E2E-RET-<run>" in inventory-storage is 3

  @e2e @returns-exceptions
  Scenario: A re-fired TaskCPTMissed stays one case through its operator lifecycle
    Given an exception policy with severity HIGH queue "ps-main" and SLA 30 minutes is declared for TaskCPTMissed in exception-management
    When fulfillment-execution's TaskCPTMissed for work unit "ord-e2e-exc-<run>-line-1" is published
    And fulfillment-execution's TaskCPTMissed for work unit "ord-e2e-exc-<run>-line-1" is published again with a different event id
    Then exception-management eventually shows exactly one case for work unit "ord-e2e-exc-<run>-line-1" with occurrence 2
    When the case is triaged to severity CRITICAL and queue "ps-urgent" in exception-management
    And the case is assigned to "op-7" in exception-management
    And the case is resolved with code "RELOCATED" and root cause "BIN_MISMATCH" in exception-management
    Then the case status is "RESOLVED" in exception-management
    And exception-management published ExceptionResolved for the case with code "RELOCATED" and root cause "BIN_MISMATCH"
    And resolving the case again is refused with 409 "case-already-resolved"

  @e2e @returns-exceptions
  Scenario: A correlated OrderRepromised auto-resolves the TaskCPTMissed case
    Given an exception policy with severity HIGH queue "ps-main" and SLA 30 minutes and auto-resolve on OrderRepromised within 10 minutes is declared for TaskCPTMissed in exception-management
    When fulfillment-execution's TaskCPTMissed for work unit "ord-e2e-auto-<run>-line-1" is published
    Then exception-management eventually shows exactly one case for work unit "ord-e2e-auto-<run>-line-1" with occurrence 1
    When order-management's OrderRepromised for order "ord-e2e-auto-<run>" is published
    Then the case is eventually AUTO_RESOLVED by rule "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed->com.warehouse.wes.order-management.order.OrderRepromised" in exception-management
    And exception-management published ExceptionAutoResolved for the case with exactly one event under that rule
    When order-management's OrderRepromised for order "ord-e2e-auto-<run>" is published again with a different event id
    Then the case status is still "RESOLVED" in exception-management
    And exception-management published exactly 1 ExceptionAutoResolved event for the case in total
