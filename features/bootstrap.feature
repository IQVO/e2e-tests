Feature: Full warehouse bootstrap across all bounded contexts
  A single work unit flows through every bounded context in
  warehouse-systems, exercising both synchronous HTTP integration
  (inventory-storage -> facility-layout, wes-work-planning ->
  inventory-storage, fulfillment-execution -> inventory-storage) and
  asynchronous Kafka integration (workforce-management ->
  wes-work-planning, wes-work-planning -> fulfillment-execution,
  fulfillment-execution -> wes-work-planning) end-to-end against real,
  independently running service processes and real Postgres/Kafka.

  Background:
    Given all warehouse-systems services are healthy

  @e2e @bootstrap
  Scenario: Bootstrap a warehouse and flow one unit of work end-to-end
    # --- facility-layout: build the physical map -----------------------
    Given site "WH1" named "Fulfilment Centre One" exists in facility-layout
    And location type "PalletRack" with capacity 1200 kg and 2.4 m3 exists in facility-layout
    And zone "STOR"/"AMB" in site "WH1" with temperature class "Ambient" exists in facility-layout
    And aisle "A01" in zone "WH1-STOR-AMB" with sequence hint 1 and direction "TwoWay" exists in facility-layout
    And location slot "WH1-STOR-AMB-A01-01-01-A" of type "PalletRack" exists in facility-layout

    # --- inventory-storage: stock a bin ---------------------------------
    Given a Bin "E2E-BIN-<run>" with capacity 100 exists in inventory-storage
    When I receive 20 units of SKU "SKU-E2E-<run>" in inventory-storage
    And I stow 20 units of SKU "SKU-E2E-<run>" into bin "E2E-BIN-<run>" in inventory-storage
    Then the usable inventory for SKU "SKU-E2E-<run>" in inventory-storage is 20

    # --- fulfillment-execution: register the station BEFORE the shift
    # plan commit below, since workforce-management's CommitShiftPlan now
    # validates planned heads against fulfillment-execution's real,
    # live-registered installed capacity (GET /capacity/{pathId}) rather
    # than trusting the caller's own claim -- and that client uses the
    # PathId's own string form VERBATIM as the capability queried (ADR-
    # 0014/0018), NOT resolved through the process-path catalogue's
    # matchPrefix family rule. So the station must carry "pick-zone-a"
    # as an explicit capability of its own, alongside the "pick" task-
    # type capability claim-next-task itself checks. ---
    Given a station "station-e2e-<run>" is registered with capabilities "pick,pick-zone-a" in fulfillment-execution

    # --- workforce-management: staff the path (publishes ShiftPlanCommitted over Kafka) ---
    Given an associate "assoc-e2e-<run>" starts a shift with certification "pick" in workforce-management
    When workforce-management commits a shift plan for building "wh1" shift "shift-1" path "pick-zone-a" with 1 planned heads, rate 30, hours 8, installed stations 1
    Then wes-work-planning eventually observes a labor plan view for path "pick-zone-a" with planned heads 1

    # --- wes-work-planning: release work (publishes WorkReleased over Kafka) ---
    Given wes-work-planning has a work pool for process path "pick-zone-a"
    When I enqueue work unit "wu-e2e-<run>" with cpt in 1 hour and reference "order-e2e-<run>" to process path "pick-zone-a" in wes-work-planning
    And work is released from process path "pick-zone-a" in wes-work-planning
    Then the released work unit is "wu-e2e-<run>"

    # --- fulfillment-execution: consumes WorkReleased, task claimed and completed ---
    When fulfillment-execution eventually creates a task for order "wu-e2e-<run>"
    And station "station-e2e-<run>" claims the next "PICK" task for order "wu-e2e-<run>" in fulfillment-execution
    Then the claimed task is for order "wu-e2e-<run>"
    When station "station-e2e-<run>" completes the claimed task in fulfillment-execution

    # --- fulfillment-execution publishes TaskCompleted over Kafka; wes-work-planning consumes it ---
    Then wes-work-planning eventually reports work unit "wu-e2e-<run>" as completed
