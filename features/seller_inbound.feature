Feature: Seller inbound - a seller ships mastercases to a vendor node and the network reconciles the receipt
  The retail-network side of the mastercase flow, across five network bounded
  contexts and two organisations: a SELLER plans and ships SSCC-labelled
  mastercases (seller-inbound), the VENDOR that operates the destination node
  polls them and reports what it counted (vendor-connectivity's Vendor API), and
  the network reconciles the receipt (seller-inbound) and scores the node
  (node-performance). Every context learns about the others over Kafka (node and
  inbound limits from node-topology, pack levels from offer-availability), so the
  scenario creates everything it needs and polls through real behaviour.

  Black-box, REST only, through the Kong gateway of the kind cluster. Self-contained
  and re-runnable: a fresh vendor, node (INBOUND_CROSS_DOCK, so it has the INBOUND
  capability, with inbound limits), catalogue item (valid each GTIN and a CASE level
  of 12 eaches) and seller per run; every SSCC and GTIN is a valid GS1 number that
  is unique per run.

  THE FLEET IS NOT PART OF THIS TEST. In the real flow the fleet's ACL
  (network-fulfillment) polls the Vendor API and submits the receipt after
  inbound-receiving closed it; on this cluster network-fulfillment runs in stub
  mode. So the test plays the fleet's role through the real Vendor API
  (POST /vendor/inbound-receipts); what is proved is the network side, not the fleet.

    P=http://localhost:8000/api
    SELLER_INBOUND_BASE_URL=$P/seller-inbound VENDOR_CONNECTIVITY_BASE_URL=$P/vendor-connectivity \
    NODE_TOPOLOGY_BASE_URL=$P/node-topology OFFER_AVAILABILITY_BASE_URL=$P/offer-availability \
    NODE_PERFORMANCE_BASE_URL=$P/node-performance GODOG_TAGS=@seller-inbound \
      go test -v -run TestMain -timeout 15m .

  (those are also the defaults.)

  - The Vendor API's node-profile request has no `inbound` block, so the node's inbound
    limits are declared on node-topology's PUT /nodes/{id}/operational-profile directly
    (declaredBy VENDOR); the node itself is registered through the Vendor API.
  - No REST exposes InboundDiscrepancyRecorded; the discrepancy is asserted on the
    shipment's CLOSED_WITH_DISCREPANCY state and each carton's receiptStatus.
  - node-performance's documented rule: inboundAccuracyRate = cartons reported RECEIVED
    over all cartons reported. A carton reported RECEIVED with a short count still counts
    as RECEIVED; its shortfall is a discrepancy, not a status.

  Background:
    Given seller-inbound, vendor-connectivity, node-topology, offer-availability and node-performance are healthy

  @e2e @seller-inbound
  Scenario: Network: a seller ships mastercases, the vendor node receives them with discrepancies
    Given vendor "VEN-SI-<run>" has an ACTIVE INBOUND_CROSS_DOCK node "XD-<run>" accepting cartons up to 20000 g, registered through vendor-connectivity and node-topology
    And a catalogue item with an each GTIN and a CASE level of 12 eaches exists in offer-availability
    And seller "SELLER-SI-<run>" is onboarded in seller-inbound
    # seller-inbound's views of the node and the pack levels arrive over Kafka: the plan is accepted once it knows them.
    And seller-inbound eventually accepts a CASE_PACKED plan of 36 eaches of the item for the node
    And the plan is confirmed
    When a shipment is opened against the plan
    # Refusals first, while the plan still has room: a refused carton adds nothing.
    Then a carton of 12 eaches weighing 20001 g is refused with 422 problem "carton-overweight"
    And a carton of 12 eaches with a length of 900 mm is refused with 422 problem "carton-oversize"
    And a carton of 10 eaches weighing 5200 g is refused with 422 problem "case-qty-mismatches-gtin"

    # Carton 1 and 2 get a network-issued SSCC, carton 3 carries the seller's own valid, unique one.
    When 3 cartons of 12 eaches are added, the last 1 with a seller-supplied SSCC
    Then the 3 carton SSCCs are valid 18-digit GS1 numbers and unique
    And a carton reusing the SSCC of carton 3 is refused with problem "sscc-not-unique"
    And shipping the unlabelled shipment is refused with problem "shipment-not-labelled" and the shipment stays DRAFT

    When the shipment is labelled
    Then the label data has 3 labels of 12 eaches, one per carton SSCC, for the node
    When the shipment is shipped

    # The vendor side: only SHIPPED shipments of the vendor's own nodes, as the pallet/carton/item tree.
    Then vendor-connectivity eventually polls the shipment for the node as a SHIPPED tree of the 3 SSCCs with 12 eaches each

    # The fleet's role, played by the test: 1 full carton, 1 short (10 of 12), 1 missing.
    When the fleet role is played by the test: it reports carton 1 RECEIVED with 12 eaches, carton 2 RECEIVED with 10 eaches counted and carton 3 MISSING
    Then the receipt transaction eventually succeeds
    And seller-inbound eventually shows the shipment CLOSED_WITH_DISCREPANCY with cartons RECEIVED, RECEIVED and MISSING
    And the received shipment eventually leaves the vendor poll
    And the vendor reads the shipment by id as CLOSED_WITH_DISCREPANCY
    # node-performance derives the inbound metrics onto an EXISTING scorecard and neither inbound
    # event opens one (ADR 0009), so give the node its first observation: an applied inventory feed.
    When an empty full inventory feed is applied for the node in offer-availability
    # 2 of the 3 cartons were reported RECEIVED.
    Then node-performance eventually exposes for the node an inboundAccuracyRate of 2/3 and an inboundTimelinessP95Minutes of at least 0

  @e2e @seller-inbound
  Scenario: A suspended seller cannot confirm an inbound plan
    Given vendor "VEN-SI2-<run>" has an ACTIVE INBOUND_CROSS_DOCK node "XD2-<run>" accepting cartons up to 20000 g, registered through vendor-connectivity and node-topology
    And a catalogue item with an each GTIN and a CASE level of 12 eaches exists in offer-availability
    And seller "SELLER-SI2-<run>" is onboarded in seller-inbound
    And seller-inbound eventually accepts a CASE_PACKED plan of 24 eaches of the item for the node
    When the seller is suspended
    Then confirming the plan is refused with problem "seller-suspended"
