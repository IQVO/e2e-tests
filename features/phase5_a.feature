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

  # 2. LABOR STANDARDS PER HANDLING UNIT (labor-performance, POST /standards, GET /standards/{taskType}?handlingUnit=).
  #    SHARED STATE: the PICK/PACK/SLAM task types are a closed enum and labor-performance keeps ONE open standard
  #    per (taskType, handlingUnit), so a run necessarily revises the cluster's PICK standards: the scenario
  #    remembers the task-type-only PICK standard and re-declares it at the end (a new revision with the old
  #    seconds). The CASE standard it opens cannot be closed through the API and stays as the open (PICK, CASE)
  #    standard. The EACH and PALLET fallback assertions assume nobody declared (PICK, EACH) or (PICK, PALLET): this scenario never does (and, because its CASE standard stays open, it cannot assert a CASE fallback on a re-run).
  #    No history endpoint exists, so "the same pair revises the prior one" is asserted through what the API
  #    exposes: the second CASE definition is accepted (not a standard-conflict 409), GET resolves to the newer
  #    one, and the task-type-only standard keeps its effectiveFrom (it is never closed by a unit definition).

  @e2e @phase5 @labor-standards
  Scenario: Labor standards per handling unit: the most specific standard wins, a team lift needs CASE, unit-less behaviour is unchanged
    Given labor-performance is healthy
    And the task-type-only PICK standard of labor-performance is remembered
    When a PICK standard of 41 expected seconds with no handling unit is defined
    Then the PICK standard resolved without a handling unit is 41 seconds declared for no handling unit
    And that standard carries no team lift
    # Nothing is declared for EACH: the lookup falls back to the task-type-only standard.
    And the PICK standard resolved for handling unit EACH is 41 seconds declared for no handling unit

    When a PICK standard of 30 expected seconds for handling unit CASE is defined
    Then the PICK standard resolved for handling unit CASE is 30 seconds declared for CASE
    And that standard carries no team lift
    # Defining a CASE standard never closes the task-type-only one.
    And the PICK standard resolved without a handling unit is 41 seconds declared for no handling unit
    And the task-type-only PICK standard kept its effectiveFrom
    And the PICK standard resolved for handling unit PALLET is 41 seconds declared for no handling unit

    When a PICK standard of 32 expected seconds for handling unit CASE with a team lift above 18000 g of 75 seconds is defined
    Then the PICK standard resolved for handling unit CASE is 32 seconds declared for CASE
    And that standard carries a team lift above 18000 g of 75 seconds for 2 operators
    And the CASE standard is not older than the first one
    And the task-type-only PICK standard kept its effectiveFrom
    And the PICK standard resolved without a handling unit is 41 seconds declared for no handling unit

    # Refusals leave everything as it was.
    Then defining a PICK standard of 20 expected seconds with no handling unit and a team lift above 18000 g of 75 seconds is refused with 422 problem "team-lift-requires-case-unit"
    And defining a PICK standard of 20 expected seconds for handling unit EACH and a team lift above 18000 g of 75 seconds is refused with 422 problem "team-lift-requires-case-unit"
    And defining a PICK standard of 20 expected seconds for handling unit BOX is refused with 400 problem "unknown-handling-unit"
    And reading the PICK standard for handling unit BOX is refused with 400 problem "unknown-handling-unit"
    And the PICK standard resolved for handling unit CASE is 32 seconds declared for CASE
    And the PICK standard resolved without a handling unit is 41 seconds declared for no handling unit

    # The unit-less standard still revises exactly as before, and leaves the CASE one alone.
    When a PICK standard of 43 expected seconds with no handling unit is defined
    Then the PICK standard resolved without a handling unit is 43 seconds declared for no handling unit
    And the PICK standard resolved for handling unit CASE is 32 seconds declared for CASE
    And that standard carries a team lift above 18000 g of 75 seconds for 2 operators

  # 3. CASE-FLOW PROCESS PATHS (process-path-management, ADR 0038). The FULL_CASE, CROSS_DOCK and BREAK_CASE paths are
  #    catalogue DATA (catalogue/paths.json in that repo) applied by scripts/seed_catalogue.py through POST /process-paths.
  #    SHARED STATE: path ids are permanent and cluster-wide, so the scenario applies the declared entries exactly as the
  #    seed script does (201 when new, 409 when a previous seeding or run already did) and then asserts what the API
  #    exposes. The three ids stay defined afterwards, which is the intended end state of the catalogue.
  #    NOT EXERCISABLE through the public REST, so not faked: (a) the step lists (a ProcessPath carries capabilities,
  #    intake, eligibility and a destination role, not steps - the put-away / each-handling absence is therefore asserted
  #    only as the declared capabilities, which are the exact lists below); (b) a "select the path for these criteria"
  #    endpoint: none exists (routing is done by the consumers, which replay the events), so selection is asserted as the
  #    documented criteria (eligibility attribute, intake source, capability) matching the listing.
  #    The invalid definitions use a throw-away pathId and are refused before anything is stored.

  @e2e @phase5 @process-paths
  Scenario: Case-flow process paths: FULL_CASE, CROSS_DOCK and BREAK_CASE are declared with their documented definitions and selectable by their criteria
    Given process-path-management is healthy
    When the declared case-flow catalogue entries FULL_CASE, CROSS_DOCK and BREAK_CASE are applied
    Then process path "FULL_CASE" is ACTIVE and direct with matchPrefix "full-case", capabilities "pick", cycleTimeP95 1h30m0s, no destination role, required product attributes "fullCase" and intake "CUSTOMER_ORDER"
    And process path "CROSS_DOCK" is ACTIVE and direct with matchPrefix "cross-dock", capabilities "dispatch", cycleTimeP95 1h0m0s, destination role "Shipping", required product attributes "crossDock" and intake "CUSTOMER_ORDER,STOCK_TRANSFER"
    And process path "BREAK_CASE" is ACTIVE and direct with matchPrefix "break", capabilities "break", cycleTimeP95 2h0m0s, no destination role, required product attributes "" and intake ""
    And applying the declared entry "FULL_CASE" again is refused with 409 (path-already-exists)
    # The order paths are untouched: the new flows sit beside them.
    And the active process path listing has FULL_CASE, CROSS_DOCK and BREAK_CASE and every other path keeps its own definition
    # Selection by the documented criteria.
    And the active paths requiring product attribute "fullCase" are exactly "FULL_CASE"
    And the active paths requiring product attribute "crossDock" are exactly "CROSS_DOCK"
    And the active paths ending at destination role "Shipping" include "CROSS_DOCK"
    And the active paths requiring capability "break" are exactly "BREAK_CASE"
    And the active paths taking intake "STOCK_TRANSFER" include "CROSS_DOCK" and "DISPATCH"

  @e2e @phase5 @process-paths
  Scenario: Case-flow process paths: invalid definitions are refused with their documented problem and store nothing
    Given process-path-management is healthy
    Then defining path "E2E-BAD-<run>" like CROSS_DOCK but with destination role "Dock" is refused with 422 problem "invalid-destination-location-role"
    And defining path "E2E-BAD-<run>" like FULL_CASE but with cycleTimeP95 "0s" is refused with 422 problem "invalid-cycle-time-p95"
    And defining path "E2E-BAD-<run>" like FULL_CASE but with matchPrefix "Full-Case" is refused with 422 problem "match-prefix-not-lowercase"
    And defining path "E2E-BAD-<run>" like FULL_CASE but with intake "NOPE" is refused with 422 problem "invalid-intake"
    And process path "E2E-BAD-<run>" does not exist
