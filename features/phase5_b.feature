Feature: Phase 5 part B - case-multiple transfers and ship-in-own-container
  Two contracts of the mastercase programme that ride on product-master's CASE
  pack level, proven black-box over REST through the Kong gateway of the live
  kind cluster (no Kafka injection, no database access):

  1. network-inventory-planning ADR 0017 (case-multiple transfers). A SKU with an
     ACTIVE CASE level is planned in whole cases: the requested quantity is
     rounded to the NEAREST multiple of the case size (a tie goes UP), never below
     one case, never above what the origin can supply; the demand stays visible as
     caseMultiple, cases, requestedQtyEaches, roundedQtyEaches and a SIGNED
     remainderEaches. A plain SKU is planned exactly as before.
  2. fulfillment-execution ADR 0056 (ship in own container). A pack line that is
     EXACTLY one full active CASE is sealed in the case itself, with the case's
     GTIN, dimensions and weight and containerSource CASE; any other quantity, or a
     SKU without a CASE level, is a 422 problem that seals nothing; sealing
     without the option is unchanged.

  Run it against the live cluster (base URLs are the only configuration):

    P=http://localhost:8000/api
    PRODUCT_MASTER_BASE_URL=$P/product-master FACILITY_BASE_URL=$P/facility-layout \
    NETWORK_INVENTORY_PLANNING_BASE_URL=$P/network-inventory-planning \
    FULFILLMENT_BASE_URL=$P/fulfillment-execution GODOG_TAGS=@phase5 \
      go test -count=1 -v -run TestMain -timeout 15m .

  How each fact is produced (all through the owning service's own REST):
  - The CASE pack level (valid GTIN-14s computed per run, declared dimensions) is
    declared in product-master. It reaches NIP and fulfillment-execution over Kafka
    into their own local pack views; neither view is readable over REST, so the
    "eventually" steps probe the behaviour itself (a proposal that carries the case
    multiple; a refused own-container seal that no longer says no-case-level).
  - Proposals come from POST /v1/transfer-proposals:generate over an explicit
    snapshot (positions, policies, lane): the diagnostic boundary of NIP, which
    still consults NIP's pack view.
  - The PACK task is created with POST /tasks and claimed with claim-next by a
    station that has a private run-scoped capability (so no other task is touched).

  Blocked (tag @blocked): approval (POST /v1/transfers:approve) cannot be driven today. See the
  scenario's own header for the two exact reasons.

  Background:
    Given network-inventory-planning and fulfillment-execution are healthy
    And product-master and facility-layout are healthy

  # ---------------------------------------------------------------------
  # 1. network-inventory-planning: case-multiple proposals (ADR 0017)
  # ---------------------------------------------------------------------

  @e2e @phase5 @network-inventory-planning
  Scenario Outline: A SKU with a CASE of 12 is proposed in whole cases and the demand stays visible
    Given SKU "SKU-P5B-CM-<run>" declares an EACH GTIN and a CASE pack level of 12 eaches with case dimensions 400x300x200 mm and 6500 g in product-master
    And the CASE GTIN of SKU "SKU-P5B-CM-<run>" eventually resolves in product-master as an active CASE of 12 eaches
    And network-inventory-planning eventually plans SKU "SKU-P5B-CM-<run>" in cases of 12 eaches
    When I generate a transfer proposal for SKU "SKU-P5B-CM-<run>" with a destination deficit of <deficit> eaches and an origin supply of <supply> eaches in network-inventory-planning
    Then the proposal moves <cases> cases of 12 eaches: requested <deficit>, rounded <rounded>, remainder <remainder>

    # 25 -> 24 (nearest, +1 left over); 18 -> 24 (tie goes UP, -6: moved beyond the request);
    # 17 -> 12 (nearest is down, +5); 1 -> 12 (at least one case, -11);
    # 100 with an origin supply of 40 -> 36 (capped at floor(40/12) = 3 cases, +64 unserved).
    Examples:
      | deficit | supply | cases | rounded | remainder |
      | 25      | 1000   | 2     | 24      | 1         |
      | 18      | 1000   | 2     | 24      | -6        |
      | 17      | 1000   | 1     | 12      | 5         |
      | 1       | 1000   | 1     | 12      | -11       |
      | 24      | 1000   | 2     | 24      | 0         |
      | 100     | 40     | 3     | 36      | 64        |

  @e2e @phase5 @network-inventory-planning
  Scenario: A plain SKU without a CASE level is not rounded and carries no case fields
    Given SKU "SKU-P5B-PLAIN-<run>" declares only an EACH GTIN in product-master
    # Proves the absence is not just a pack view that has not arrived yet: the CASE SKU
    # of the same product-master is already known to NIP when this step passes.
    And SKU "SKU-P5B-CM2-<run>" declares an EACH GTIN and a CASE pack level of 12 eaches with case dimensions 400x300x200 mm and 6500 g in product-master
    And network-inventory-planning eventually plans SKU "SKU-P5B-CM2-<run>" in cases of 12 eaches
    When I generate a transfer proposal for SKU "SKU-P5B-PLAIN-<run>" with a destination deficit of 25 eaches and an origin supply of 1000 eaches in network-inventory-planning
    Then the proposal moves 25 eaches and carries no case fields

  @e2e @phase5 @network-inventory-planning
  Scenario: An origin that cannot supply one whole case proposes nothing
    Given SKU "SKU-P5B-CM3-<run>" declares an EACH GTIN and a CASE pack level of 12 eaches with case dimensions 400x300x200 mm and 6500 g in product-master
    And network-inventory-planning eventually plans SKU "SKU-P5B-CM3-<run>" in cases of 12 eaches
    When I generate a transfer proposal for SKU "SKU-P5B-CM3-<run>" with a destination deficit of 25 eaches and an origin supply of 11 eaches in network-inventory-planning
    Then no transfer is proposed because the origin cannot supply one whole case

  # BLOCKED, with the exact reasons (checked against the live cluster, 2026-10-24):
  #
  # (a) Demand facts. The snapshot needs ACTIVE site-sku demand at the destination for
  #     the SKU AND a demand fact at the origin. The only producer is order-management,
  #     which projects demand to ONE statically configured site per deployment
  #     (DEMAND_PROJECTION_SITE_ID=AUS3 here): there is no REST route that states a
  #     demand fact for two arbitrary sites. The existing inter_warehouse_transfer
  #     feature injects the CloudEvent into Kafka instead; this suite does not fake a
  #     producing service.
  # (b) Fail-closed staleness is cluster-global. BuildSnapshot refuses the WHOLE
  #     snapshot when ANY capability / demand / capacity fact in the read models is
  #     older than PLANNING_MAX_STALENESS (10m), not just the sites of the transfer.
  #     Live: POST /v1/transfers:approve for ANY sites answers 422 facts-incomplete
  #     "stale site capability for ABE8 (as of 2026-10-19T00:10:49Z, older than 10m0s)"
  #     and so does GET /v1/transfer-simulations (503). facility-layout publishes
  #     SiteCapabilityChanged only on CHANGE (PUT of the current snapshot is a no-op,
  #     ADR 0035), and ~60 sites of the cluster hold a capability fact that can only
  #     be refreshed by toggling it. See the PR description (product finding).
  #
  # The steps below are real (capability via facility-layout PUT, capacity plan via
  # warehouse-planning REST, approve, GET transfer); the demand step is pending. Run
  # with GODOG_TAGS=@blocked once both are resolved.
  @e2e @blocked @phase5-approve @network-inventory-planning
  Scenario Outline: An approved transfer of a CASE SKU is rounded to whole cases and GET shows the demand
    Given SKU "SKU-P5B-AP-<run>" declares an EACH GTIN and a CASE pack level of 12 eaches with case dimensions 400x300x200 mm and 6500 g in product-master
    And network-inventory-planning eventually plans SKU "SKU-P5B-AP-<run>" in cases of 12 eaches
    And order-management has projected site-sku demand for SKU "SKU-P5B-AP-<run>" at destination "P5B-D-<run>" and for another SKU at origin "P5B-O-<run>"
    And facility-layout enables site "P5B-O-<run>" as transfer origin and destination
    And facility-layout enables site "P5B-D-<run>" as transfer origin and destination
    And warehouse-planning publishes a capacity plan for site "P5B-O-<run>" of 1000 orders per hour covering the next 48 hours
    And warehouse-planning publishes a capacity plan for site "P5B-D-<run>" of 1000 orders per hour covering the next 48 hours
    When an operator approves a transfer of <requested> eaches of SKU "SKU-P5B-AP-<run>" from "P5B-O-<run>" to "P5B-D-<run>" in network-inventory-planning
    Then the approval is accepted for <cases> cases of 12 eaches: requested <requested>, rounded <rounded>, remainder <remainder>
    And GET of that transfer in network-inventory-planning shows <cases> cases of 12 eaches: requested <requested>, rounded <rounded>, remainder <remainder>

    Examples:
      | requested | cases | rounded | remainder |
      | 25        | 2     | 24      | 1         |
      | 18        | 2     | 24      | -6        |

  # ---------------------------------------------------------------------
  # 2. fulfillment-execution: ship in own container (ADR 0056)
  # ---------------------------------------------------------------------

  @e2e @phase5 @fulfillment-execution
  Scenario: A pack line of exactly one full case is sealed in its own container with the case's GTIN, dimensions and weight
    Given SKU "SKU-P5B-OC-<run>" declares an EACH GTIN and a CASE pack level of 12 eaches with case dimensions 400x300x200 mm and 6500 g in product-master
    And the CASE GTIN of SKU "SKU-P5B-OC-<run>" eventually resolves in product-master as an active CASE of 12 eaches
    And a PACK station "ST-P5B-OC-<run>" is registered in fulfillment-execution
    And the PACK task of order "ORD-P5B-OC-<run>" is claimed by that station in fulfillment-execution
    And fulfillment-execution eventually knows the CASE level of SKU "SKU-P5B-OC-<run>"
    When the station seals SKU "SKU-P5B-OC-<run>" in fulfillment-execution in its own container with line quantity 12
    Then the seal is accepted with status 201
    And the package is sealed in its own container sourced CASE with the CASE GTIN of SKU "SKU-P5B-OC-<run>", 400x300x200 mm and 6500 g
    And reading that package back from fulfillment-execution shows the same own container of SKU "SKU-P5B-OC-<run>", 400x300x200 mm and 6500 g

  @e2e @phase5 @fulfillment-execution
  Scenario Outline: A pack line that is not exactly one full case is refused and nothing is sealed
    Given SKU "SKU-P5B-RF-<run>" declares an EACH GTIN and a CASE pack level of 12 eaches with case dimensions 400x300x200 mm and 6500 g in product-master
    And a PACK station "ST-P5B-RF-<run>" is registered in fulfillment-execution
    And the PACK task of order "ORD-P5B-RF-<qty>-<run>" is claimed by that station in fulfillment-execution
    And fulfillment-execution eventually knows the CASE level of SKU "SKU-P5B-RF-<run>"
    When the station seals SKU "SKU-P5B-RF-<run>" in fulfillment-execution in its own container with line quantity <qty>
    Then the seal is refused with status 422 and fulfillment-execution problem "ship-in-own-container-quantity-mismatch"
    And nothing was sealed: the station can still seal SKU "SKU-P5B-RF-<run>" as an ordinary carton

    Examples:
      | qty |
      | 11  |
      | 13  |
      | 24  |

  @e2e @phase5 @fulfillment-execution
  Scenario: A SKU without a CASE level cannot ship in its own container
    Given SKU "SKU-P5B-NC-<run>" declares only an EACH GTIN in product-master
    And a PACK station "ST-P5B-NC-<run>" is registered in fulfillment-execution
    And the PACK task of order "ORD-P5B-NC-<run>" is claimed by that station in fulfillment-execution
    When the station seals SKU "SKU-P5B-NC-<run>" in fulfillment-execution in its own container with line quantity 12
    Then the seal is refused with status 422 and fulfillment-execution problem "ship-in-own-container-no-case-level"
    And nothing was sealed: the station can still seal SKU "SKU-P5B-NC-<run>" as an ordinary carton

  @e2e @phase5 @fulfillment-execution
  Scenario: Sealing without the option behaves as before, even for a SKU that has a CASE
    Given SKU "SKU-P5B-OR-<run>" declares an EACH GTIN and a CASE pack level of 12 eaches with case dimensions 400x300x200 mm and 6500 g in product-master
    And a PACK station "ST-P5B-OR-<run>" is registered in fulfillment-execution
    And the PACK task of order "ORD-P5B-OR-<run>" is claimed by that station in fulfillment-execution
    And fulfillment-execution eventually knows the CASE level of SKU "SKU-P5B-OR-<run>"
    When the station seals SKU "SKU-P5B-OR-<run>" in fulfillment-execution as an ordinary carton
    Then the seal is accepted with status 201
    And the package is an ordinary sealed carton without own-container fields

  @e2e @phase5 @fulfillment-execution
  Scenario: shipInOwnContainer without a lineQuantity is a 400 and seals nothing
    Given SKU "SKU-P5B-LQ-<run>" declares an EACH GTIN and a CASE pack level of 12 eaches with case dimensions 400x300x200 mm and 6500 g in product-master
    And a PACK station "ST-P5B-LQ-<run>" is registered in fulfillment-execution
    And the PACK task of order "ORD-P5B-LQ-<run>" is claimed by that station in fulfillment-execution
    When the station seals SKU "SKU-P5B-LQ-<run>" in fulfillment-execution in its own container without a line quantity
    Then the response status is 400
    And nothing was sealed: the station can still seal SKU "SKU-P5B-LQ-<run>" as an ordinary carton
