Feature: A missed CPT re-promises the order — the full ADR 0014 feedback loop
  This is the closing proof of the whole "promise derived from fulfillment
  capability" initiative (ADR 0014, all sections). It exercises the full
  round trip across THREE bounded contexts and THREE Kafka topics with zero
  manual intervention:

    1. order-management (OM) places a ship-complete order, synchronously
       allocates against inventory-storage, and publishes OrderAllocated to
       warehouse.order-management.events carrying a PromiseDate (ADR 0014).
    2. wes-work-planning (WES) consumes that event and enqueues a work unit
       with OM's own frozen id format "{orderId}-line-{lineNo}" and CPT =
       the exact PromiseDate from step 1.
    3. WES releases the work -> WorkReleased -> fulfillment-execution (FE)
       consumes it and creates a Task with OrderRef = that same work-unit id
       and CPT = that same timestamp.
    4. FE's Clock-driven sweep (POST /tasks/sweep-cpt-misses, ADR-0025)
       detects the task is still open past its CPT and publishes
       TaskCPTMissed onto warehouse.fulfillment.events via its transactional
       outbox.
    5. OM's RepromiseConsumer (ADR 0018) consumes TaskCPTMissed, parses the
       order_ref back into (OrderId, LineNo), and re-runs the SAME promise
       policy the order was originally promised with. If the promise moved,
       it persists the new promise and publishes OrderRepromised.

  This harness never sets PATH_CATALOGUE_SOURCE=kafka for order-management
  (see scripts/03-up-services.sh), so its PromisePolicy always falls back to
  LeadTimePolicy — and LeadTimePolicy.PromiseDate is literally
  `now.Add(longest)`: a fresh, strictly-later timestamp every time it is
  called purely because wall-clock time has moved on. That is enough, on
  its own, to make RepromiseOrder's fresh recompute genuinely differ from
  the order's original promise — no CPT-schedule/capability infrastructure
  needs to be wired up for this scenario to be a real, deterministic proof.

  Background:
    Given all warehouse-systems services are healthy

  @e2e @order-management @fulfillment-execution
  Scenario: A task's missed CPT triggers fulfillment-execution's sweep and order-management re-promises the order
    # --- inventory-storage: stock the SKU this order will allocate against ---
    Given a Bin "E2E-REPROMISE-BIN-<run>" with capacity 100 exists in inventory-storage
    When I receive 10 units of SKU "SKU-E2E-REPROMISE-<run>" in inventory-storage
    And I stow 10 units of SKU "SKU-E2E-REPROMISE-<run>" into bin "E2E-REPROMISE-BIN-<run>" in inventory-storage
    Then the usable inventory for SKU "SKU-E2E-REPROMISE-<run>" in inventory-storage is 10

    # --- order-management: place a ship-complete order and capture its
    # ORIGINAL promise date before anything downstream can move it ---
    When I place an order for 2 units of SKU "SKU-E2E-REPROMISE-<run>" allowing ship-complete only in order-management
    Then the response status is 201
    And the order is allocated in order-management
    And I capture the order's current promise date in order-management

    # --- wes-work-planning: the Kafka-choreographed release (same proof
    # order_management_choreographed_release.feature already establishes) ---
    Then wes-work-planning eventually enqueues a work unit for the order's line 1 on process path "pick"

    # --- wes-work-planning: release the enqueued work unit so it actually
    # reaches fulfillment-execution (WorkReleased) -- order-management's
    # choreographed-release sibling scenario stops at "enqueued"; this
    # scenario needs the work unit to become a real task, so it goes one
    # step further and releases it. The shared "pick" pool may also hold
    # an older, never-released leftover from that sibling scenario, so
    # this keeps releasing (earliest-CPT-first) until it reaches THIS
    # scenario's own work unit -- same shared-queue discipline this
    # harness's claimNextTaskForOrder already applies on the claim side ---
    When work is eventually released from process path "pick" for the order's line 1 in wes-work-planning

    # --- fulfillment-execution: WorkReleased creates the real task for
    # that exact line, carrying the order's real PromiseDate as its CPT ---
    Then fulfillment-execution eventually creates a task for the order's line 1

    # --- force that task's CPT into the past directly in fulfillment-
    # execution's own Postgres (same direct-seed precedent as
    # claimedTaskLeaseForcedExpired), so the sweep can detect it as missed
    # without waiting out the real (many-hour) LeadTimePolicy lead time ---
    When the task for the order's line 1 has its CPT forced into the past in fulfillment-execution

    # --- trigger the Clock-driven sweep: it publishes TaskCPTMissed via
    # FE's own transactional outbox, which relays it onto Kafka automatically ---
    And I trigger fulfillment-execution's CPT-missed sweep
    Then fulfillment-execution reports at least 1 CPT miss

    # --- order-management: RepromiseConsumer consumes TaskCPTMissed,
    # recomputes the promise fresh via LeadTimePolicy (now has moved on, so
    # now.Add(leadTime) is strictly later than the original promise), and
    # publishes OrderRepromised -- this is the loop closing for real, a
    # missed CPT genuinely and automatically re-promised the customer's
    # order across three repos and three Kafka topics ---
    Then the order's promise date in order-management eventually changes from the captured value
