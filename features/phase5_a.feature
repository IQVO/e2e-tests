Feature: Phase 5 part A - seller scorecard, labor standards per handling unit, FULL_CASE/CROSS_DOCK paths, pack-aware charge estimate
  Live, black-box, REST-only scenarios through the Kong gateway of the kind cluster, one per
  Phase 5 capability. Every scenario creates its own data (run-scoped ids, valid GS1 check
  digits) so the suite re-runs cleanly; where a context owns shared reference data the limits
  are stated on the scenario.

    P=http://localhost:8000/api
    SELLER_INBOUND_BASE_URL=$P/seller-inbound VENDOR_CONNECTIVITY_BASE_URL=$P/vendor-connectivity \
    NODE_TOPOLOGY_BASE_URL=$P/node-topology OFFER_AVAILABILITY_BASE_URL=$P/offer-availability \
    NODE_PERFORMANCE_BASE_URL=$P/node-performance PRODUCT_MASTER_BASE_URL=$P/product-master \
    LABOR_PERFORMANCE_BASE_URL=$P/labor-performance PROCESS_PATH_BASE_URL=$P/process-path-management \
    WORK_PLANNING_BASE_URL=$P/wes-work-planning GODOG_TAGS=@phase5 \
      go test -count=1 -v -run TestMain -timeout 15m .

  1. SELLER SCORECARD (seller-inbound, GET /sellers/{id}/scorecard). The flow is the seller_inbound
     one (its step definitions are reused): a seller whose shipment closed with one short and one
     missing carton. The documented rules are asserted: receiptAccuracyRate = cartonsReceived /
     cartonsReported (a short carton still counts RECEIVED, only MISSING does not), the tier is the
     worst of three signals, and it is informational. There is no GET /sellers/{id}: "stays ACTIVE"
     is proved by the seller still being able to confirm a new plan (seller-suspended otherwise).

  Background:
    Given seller-inbound, vendor-connectivity, node-topology, offer-availability and node-performance are healthy

  @e2e @phase5 @seller-scorecard
  Scenario: Seller scorecard: a shipment closed with a short and a missing carton scores the seller, informationally
    Given vendor "VEN-SC-<run>" has an ACTIVE INBOUND_CROSS_DOCK node "XD-SC-<run>" accepting cartons up to 20000 g, registered through vendor-connectivity and node-topology
    And a catalogue item with an each GTIN and a CASE level of 12 eaches exists in offer-availability
    And seller "SELLER-SC-<run>" is onboarded in seller-inbound
    And seller-inbound eventually accepts a CASE_PACKED plan of 36 eaches of the item for the node
    And the plan is confirmed
    When a shipment is opened against the plan
    And 3 cartons of 12 eaches are added, the last 1 with a seller-supplied SSCC
    And the shipment is labelled
    And the shipment is shipped
    Then vendor-connectivity eventually polls the shipment for the node as a SHIPPED tree of the 3 SSCCs with 12 eaches each
    When the fleet role is played by the test: it reports carton 1 RECEIVED with 12 eaches, carton 2 RECEIVED with 10 eaches counted and carton 3 MISSING
    Then the receipt transaction eventually succeeds
    And seller-inbound eventually shows the shipment CLOSED_WITH_DISCREPANCY with cartons RECEIVED, RECEIVED and MISSING
    And the 30d scorecard of the seller eventually reports 1 shipment received with 3 cartons reported and 2 received
    And the scorecard receiptAccuracyRate is cartonsReceived over cartonsReported
    And the scorecard has discrepanciesByKind for the short and the missing carton and a complianceTier following the documented rule
    # Informational: even an AT_RISK tier suspends and blocks nothing.
    And the seller can still confirm a new CASE_PACKED plan of 24 eaches for the node

  @e2e @phase5 @seller-scorecard
  Scenario: Seller scorecard: no shipments gives the documented empty shape, an unknown seller is 404, a bad window is 400
    Given seller "SELLER-SC0-<run>" is onboarded in seller-inbound
    Then the 30d scorecard of the seller is the empty shape: zero counts, no rates, no NaN, empty discrepanciesByKind, tier GOOD
    And the 7d scorecard of the seller is the empty shape: zero counts, no rates, no NaN, empty discrepanciesByKind, tier GOOD
    And the scorecard of unknown seller "SELLER-SC-GHOST-<run>" is refused with 404 problem "not-found"
    And the scorecard of the seller with window "1d" is refused with 400 problem "invalid-window"
