Feature: Inter-warehouse transfer saga across real services
  An operator approves moving stock from one warehouse site to another and
  the saga runs end to end across real, independently running service
  binaries over real Kafka (CloudEvents 1.0):
    network-inventory-planning (NIP) -> inventory-storage (allocate at the
    origin) -> NIP (release the pick demand) -> wes-work-planning ->
    fulfillment-execution (floor picks) -> NIP (release the dispatch demand)
    -> wes-work-planning -> fulfillment-execution (floor dispatches) ->
    inventory-storage (destination scan + stow) -> NIP (RECEIVED).

  WHAT IS REAL: NIP, warehouse-planning, inventory-storage,
  wes-work-planning and fulfillment-execution are the actual binaries; every
  hop between them is a real Kafka message; the floor work and the
  destination receipt are real REST calls; warehouse-planning (the producer
  of CapacityPlanPublished) is a harness service and publishes through its
  own REST + outbox.

  WHAT IS INJECTED OR SEEDED (each one is a step whose wording says so, and
  each exists because the owning service has no producer / write path):
    1. SiteCapabilityChanged  INJECTED. facility-layout declares the event
       but nothing emits it (no use case, no route changes a site's transfer
       capability). The harness publishes the exact CloudEvent.
    2. SiteSkuDemandChanged   INJECTED. order-management projects every
       order line to ONE statically configured site per deployment, but a
       transfer needs demand facts for TWO sites and a due_at inside the
       capacity window, so the harness publishes the exact CloudEvent.
    3. Site custody of stock and bins  SEEDED in inventory-storage's
       Postgres. Receive and stow are real REST, but no endpoint records a
       site on a bin / stock unit (inventory-storage ADR 0030: nullable
       column, written only by the destination stow). Site-less stock is
       deliberately unallocatable for transfers.
    4. The saga's own state is read from NIP's table until NIP ships
       GET /v1/transfers/{id} (parallel branch feature/read-side-and-mcp).
       Set NIP_TRANSFER_READ_MODE=rest and the very same step reads that
       endpoint instead.

  Not covered, and why: a SHORT pick (picked quantity below the allocated
  quantity). fulfillment-execution's completion contract carries no picked
  quantity (POST /tasks/{id}/complete takes only a stationId; the fact's
  quantity is the released quantity), so no real service can produce one
  today. NIP's own short-pick handling (ADR 0005) is proven in its repo.

  Sites and the two bins are fixed reference data; the SKU, the station and
  every id the saga mints are run-scoped, so the suite re-runs against a
  dirty database.

  Background:
    Given all warehouse-systems services are healthy
    And network-inventory-planning and warehouse-planning are healthy

  @e2e @inter-warehouse-transfer
  Scenario: An approved transfer is allocated, picked, dispatched, received and stowed
    # --- NIP's three fail-closed read-model inputs --------------------------
    Given the harness injects facility-layout's SiteCapabilityChanged for site "E2E-ORIGIN" with transfer origin and destination enabled
    And the harness injects facility-layout's SiteCapabilityChanged for site "E2E-DEST" with transfer origin and destination enabled
    And the harness injects order-management's SiteSkuDemandChanged for site "E2E-DEST": 10 units of SKU "SKU-XFER-<run>" due in 2 hours
    And the harness injects order-management's SiteSkuDemandChanged for site "E2E-ORIGIN": 1 units of SKU "SKU-XFER-BASE-<run>" due in 2 hours
    And warehouse-planning publishes a capacity plan for site "E2E-ORIGIN" of 1000 orders per hour covering the next 48 hours
    And warehouse-planning publishes a capacity plan for site "E2E-DEST" of 1000 orders per hour covering the next 48 hours

    # --- physical stock: 20 units in the origin's custody -------------------
    And site "E2E-ORIGIN" holds 20 units of SKU "SKU-XFER-<run>" stowed in bin "E2E-ORIGIN-BIN-<run>" in inventory-storage
    And bin "E2E-DEST-BIN-<run>" with capacity 100 is registered at site "E2E-DEST" in inventory-storage
    And a station "station-xfer-<run>" is registered with capabilities "pick,dispatch" in fulfillment-execution

    # --- approve: NIP -> inventory-storage allocates the origin stock -------
    When I approve a transfer of 5 units of SKU "SKU-XFER-<run>" from site "E2E-ORIGIN" to site "E2E-DEST" in network-inventory-planning
    Then inventory-storage eventually publishes TransferStockAllocated for the transfer: 5 units at site "E2E-ORIGIN"
    And network-inventory-planning eventually reports the transfer as ALLOCATED

    # --- pick leg: NIP releases demand, WES enqueues, floor picks -----------
    And network-inventory-planning eventually releases the pick demand of 5 units on path "pick-transfer" at site "E2E-ORIGIN"
    And wes-work-planning eventually enqueues the transfer's pick demand as a work unit
    When wes-work-planning releases the transfer's pick work unit
    And fulfillment-execution eventually creates a task for the transfer's pick demand
    And station "station-xfer-<run>" claims the next "PICK" task for the transfer's pick demand in fulfillment-execution
    And station "station-xfer-<run>" completes the claimed task in fulfillment-execution
    Then fulfillment-execution eventually publishes TransferPicked for the transfer with quantity 5
    And network-inventory-planning eventually reports the transfer as PICKED

    # --- dispatch leg: NIP releases the dispatch demand from the pick fact --
    And network-inventory-planning eventually releases the dispatch demand of 5 units on path "dispatch-transfer" at site "E2E-ORIGIN"
    And wes-work-planning eventually enqueues the transfer's dispatch demand as a work unit
    When wes-work-planning releases the transfer's dispatch work unit
    And fulfillment-execution eventually creates a task for the transfer's dispatch demand
    And station "station-xfer-<run>" claims the next "DISPATCH" task for the transfer's dispatch demand in fulfillment-execution
    And station "station-xfer-<run>" completes the claimed task in fulfillment-execution
    Then fulfillment-execution eventually publishes TransferDispatched for the transfer with quantity 5
    And network-inventory-planning eventually reports the transfer as IN_TRANSIT

    # --- destination receipt: scan then stow (inventory-storage ADR 0033) --
    When destination site "E2E-DEST" scans 5 received units of the transfer into inventory-storage
    Then inventory-storage eventually publishes TransferReceiptStaged for the transfer
    And network-inventory-planning eventually reports the transfer as ARRIVED
    When the received units are stowed 5 into bin "E2E-DEST-BIN-<run>" in inventory-storage
    Then inventory-storage eventually publishes TransferStockStowed for the transfer
    And network-inventory-planning eventually reports the transfer as RECEIVED

  @e2e @inter-warehouse-transfer
  Scenario: Origin stock that cannot cover the transfer is rejected and compensated, releasing no work
    Given the harness injects facility-layout's SiteCapabilityChanged for site "E2E-ORIGIN" with transfer origin and destination enabled
    And the harness injects facility-layout's SiteCapabilityChanged for site "E2E-DEST" with transfer origin and destination enabled
    And the harness injects order-management's SiteSkuDemandChanged for site "E2E-DEST": 10 units of SKU "SKU-XFER-SHORT-<run>" due in 2 hours
    And the harness injects order-management's SiteSkuDemandChanged for site "E2E-ORIGIN": 1 units of SKU "SKU-XFER-BASE-<run>" due in 2 hours
    And warehouse-planning publishes a capacity plan for site "E2E-ORIGIN" of 1000 orders per hour covering the next 48 hours
    And warehouse-planning publishes a capacity plan for site "E2E-DEST" of 1000 orders per hour covering the next 48 hours
    # the planning facts say the transfer is sensible; the physical stock says
    # otherwise: only 3 of the 5 requested units are in the origin's custody
    And site "E2E-ORIGIN" holds 3 units of SKU "SKU-XFER-SHORT-<run>" stowed in bin "E2E-ORIGIN-BIN-SHORT-<run>" in inventory-storage

    When I approve a transfer of 5 units of SKU "SKU-XFER-SHORT-<run>" from site "E2E-ORIGIN" to site "E2E-DEST" in network-inventory-planning
    Then inventory-storage eventually publishes TransferStockAllocationRejected for the transfer with reason INSUFFICIENT_USABLE
    And network-inventory-planning eventually reports the transfer as UNFULFILLABLE

    # compensation: nothing was released, nothing was worked, nothing can arrive
    And network-inventory-planning has released no work demand for the transfer
    And wes-work-planning has no work unit for the transfer
    And fulfillment-execution has no task for the transfer
    And destination site "E2E-DEST" scanning 5 units against the transfer's rejected line is quarantined by inventory-storage
