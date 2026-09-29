Feature: Order-management choreographed release integrates with wes-work-planning
  order-management's choreographed-release redesign proved end-to-end: an
  order that can be fully allocated is allocated (synchronous HTTP to
  inventory-storage, unchanged) and released (Kafka: OrderAllocated ->
  warehouse.order-management.events) in the SAME POST /orders call.
  wes-work-planning's 4th consumer subscription turns that event into a
  real work unit via its existing EnqueueWorkUnit use case. order-
  management's public REST surface in v1 has no /allocate or /release
  endpoint — this scenario is the only proof, across the whole estate,
  that the choreography actually reaches wes-work-planning over a real
  Kafka broker rather than only being exercised inside each repo's own
  unit/integration tests.

  Background:
    Given all warehouse-systems services are healthy

  @e2e @order-management
  Scenario: Placing a fully-allocatable order releases work to wes-work-planning via Kafka
    # --- inventory-storage: stock the SKU this order will allocate against ---
    Given a Bin "E2E-OM-BIN-<run>" with capacity 100 exists in inventory-storage
    When I receive 10 units of SKU "SKU-E2E-OM-<run>" in inventory-storage
    And I stow 10 units of SKU "SKU-E2E-OM-<run>" into bin "E2E-OM-BIN-<run>" in inventory-storage
    Then the usable inventory for SKU "SKU-E2E-OM-<run>" in inventory-storage is 10

    # --- order-management: place a ship-complete order for stock that exists ---
    # No /allocate or /release call follows — order-management's redesign
    # performs both, internally, inside this single POST /orders call.
    When I place an order for 2 units of SKU "SKU-E2E-OM-<run>" allowing ship-complete only in order-management
    Then the response status is 201
    And the order is allocated in order-management

    # --- wes-work-planning: the Kafka-choreographed proof ---
    # order-management publishes OrderAllocated to warehouse.order-management.events;
    # wes-work-planning's handleOrderManagementEvent consumes it and calls
    # EnqueueWorkUnit for line 1 on the default "pick" process path (order-
    # management's v1 lines always default to shared.DefaultPathId).
    Then wes-work-planning eventually enqueues a work unit for the order's line 1 on process path "pick"
