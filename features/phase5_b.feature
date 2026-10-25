Feature: Phase 5 part B - case-multiple transfers and ship-in-own-container
  Two contracts of the mastercase programme that ride on product-master's CASE
  pack level, proven black-box over REST through the Kong gateway of the live
  kind cluster (no database access; ONE injected fact, named below):

  1. network-inventory-planning ADR 0017 (case-multiple transfers). A SKU with an
     ACTIVE CASE level is planned in whole cases: the requested quantity is
     rounded to the NEAREST multiple of the case size (a tie goes UP), never below
     one case, never above what the origin can supply; the demand stays visible as
     caseMultiple, cases, requestedQtyEaches, roundedQtyEaches and a SIGNED
     remainderEaches. A plain SKU is planned exactly as before. Approval
     (POST /v1/transfers:approve, GET /v1/transfers/{id}) applies the same rule, and
     ADR 0018 (per-site staleness): a stale fact excludes only ITS site, so an
     approval fails closed (422 facts-incomplete) only when the transfer's own origin
     or destination is excluded, never because of another site's stale facts.
  2. fulfillment-execution ADR 0056 (ship in own container). A pack line that is
     EXACTLY one full active CASE is sealed in the case itself, with the case's
     GTIN, dimensions and weight and containerSource CASE; any other quantity, or a
     SKU without a CASE level, is a 422 problem that seals nothing; sealing
     without the option is unchanged.

  Run it against the live cluster (base URLs are the only configuration; the
  approval scenarios also need the kind broker for the injected demand fact and
  warehouse-planning's REST):

    P=http://localhost:8000/api
    PRODUCT_MASTER_BASE_URL=$P/product-master FACILITY_BASE_URL=$P/facility-layout \
    NETWORK_INVENTORY_PLANNING_BASE_URL=$P/network-inventory-planning \
    NIP_BASE_URL=$P/network-inventory-planning WAREHOUSE_PLANNING_BASE_URL=$P/warehouse-planning \
    FULFILLMENT_BASE_URL=$P/fulfillment-execution KAFKA_BROKERS=localhost:9092 \
    GODOG_TAGS=@phase5 go test -count=1 -v -run TestMain -timeout 15m .

  How each fact is produced:
  - The CASE pack level (valid GTIN-14s computed per run, declared dimensions) is
    declared in product-master REST. It reaches NIP and fulfillment-execution over
    Kafka into their own local pack views; neither view is readable over REST, so the
    "eventually" steps probe the behaviour itself (a proposal that carries the case
    multiple; a refused own-container seal that no longer says no-case-level).
  - Proposals come from POST /v1/transfer-proposals:generate over an explicit
    snapshot (positions, policies, lane): the diagnostic boundary of NIP, which
    still consults NIP's pack view.
  - Approvals run on TWO RUN-SCOPED SITES per scenario (P5O<run> origin, P5D<run>
    destination), whose three planning facts are all fresh: capability through
    facility-layout REST (PUT /sites/{code}/transfer-capability), the published
    capacity plan through warehouse-planning REST, and the site-sku demand as the
    CloudEvent order-management would emit, INJECTED into Kafka with the suite's
    existing helper (the inter_warehouse_transfer feature's step): order-management
    projects demand to ONE statically configured site per deployment, so no REST route
    states a demand fact for two arbitrary sites. The origin's availability is not a
    stock level: NIP derives it as the origin's published capacity over the window
    minus its own in-window demand (OriginHeadroom), so a plan of 1 order/hour over a
    12 hour window (the window opens an hour in the past) with 1 unit of origin demand
    leaves exactly 11 eaches.
  - The PACK task is created with POST /tasks and claimed with claim-next by a
    station that has a private run-scoped capability (so no other task is touched).

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

  # Approval over two run-scoped sites with fresh facts. 25 -> 24 (+1) and 18 -> 24 (-6);
  # the transfer quantity is the ROUNDED eaches on the approval AND on GET.
  @e2e @phase5 @network-inventory-planning
  Scenario Outline: An approved transfer of a CASE SKU is rounded to whole cases and GET shows the demand
    Given SKU "SKU-P5B-AP-<run>" declares an EACH GTIN and a CASE pack level of 12 eaches with case dimensions 400x300x200 mm and 6500 g in product-master
    And network-inventory-planning eventually plans SKU "SKU-P5B-AP-<run>" in cases of 12 eaches
    And facility-layout enables site "P5O<run>" as transfer origin and destination
    And facility-layout enables site "P5D<run>" as transfer origin and destination
    And the harness injects order-management's SiteSkuDemandChanged for site "P5D<run>": 100 units of SKU "SKU-P5B-AP-<run>" due in 2 hours
    And the harness injects order-management's SiteSkuDemandChanged for site "P5O<run>": 1 units of SKU "SKU-P5B-AP-OTHER-<run>" due in 2 hours
    And warehouse-planning publishes a capacity plan for site "P5O<run>" of 1000 orders per hour covering the next 48 hours
    And warehouse-planning publishes a capacity plan for site "P5D<run>" of 1000 orders per hour covering the next 48 hours
    When an operator approves a transfer of <requested> eaches of SKU "SKU-P5B-AP-<run>" from "P5O<run>" to "P5D<run>" in network-inventory-planning
    Then the approval is accepted for <cases> cases of 12 eaches: requested <requested>, rounded <rounded>, remainder <remainder>
    And GET of that transfer in network-inventory-planning shows <cases> cases of 12 eaches: requested <requested>, rounded <rounded>, remainder <remainder>
    And replaying that approval with the same Idempotency-Key answers replayed true for the same transfer

    Examples:
      | requested | cases | rounded | remainder |
      | 25        | 2     | 24      | 1         |
      | 18        | 2     | 24      | -6        |

  @e2e @phase5 @network-inventory-planning
  Scenario: An approved transfer of a plain SKU is not rounded and carries no case fields
    # The CASE SKU proves NIP's pack view is live, so the absence below is not a view that
    # has not arrived yet.
    Given SKU "SKU-P5B-APP-<run>" declares only an EACH GTIN in product-master
    And SKU "SKU-P5B-CM4-<run>" declares an EACH GTIN and a CASE pack level of 12 eaches with case dimensions 400x300x200 mm and 6500 g in product-master
    And network-inventory-planning eventually plans SKU "SKU-P5B-CM4-<run>" in cases of 12 eaches
    And facility-layout enables site "P5O<run>" as transfer origin and destination
    And facility-layout enables site "P5D<run>" as transfer origin and destination
    And the harness injects order-management's SiteSkuDemandChanged for site "P5D<run>": 100 units of SKU "SKU-P5B-APP-<run>" due in 2 hours
    And the harness injects order-management's SiteSkuDemandChanged for site "P5O<run>": 1 units of SKU "SKU-P5B-AP-OTHER-<run>" due in 2 hours
    And warehouse-planning publishes a capacity plan for site "P5O<run>" of 1000 orders per hour covering the next 48 hours
    And warehouse-planning publishes a capacity plan for site "P5D<run>" of 1000 orders per hour covering the next 48 hours
    When an operator approves a transfer of 25 eaches of SKU "SKU-P5B-APP-<run>" from "P5O<run>" to "P5D<run>" in network-inventory-planning
    Then the approval is accepted for 25 eaches and carries no case fields
    And GET of that transfer in network-inventory-planning shows 25 eaches and carries no case fields

  # The origin's availability is 12 (1 order/hour over a 12 hour window) minus 1 unit of its own
  # demand = 11 eaches: not one whole case of 12.
  @e2e @phase5 @network-inventory-planning
  Scenario: An approval whose origin can send only 11 eaches of a case of 12 is refused below-one-case
    Given SKU "SKU-P5B-BC-<run>" declares an EACH GTIN and a CASE pack level of 12 eaches with case dimensions 400x300x200 mm and 6500 g in product-master
    And network-inventory-planning eventually plans SKU "SKU-P5B-BC-<run>" in cases of 12 eaches
    And facility-layout enables site "P5O<run>" as transfer origin and destination
    And facility-layout enables site "P5D<run>" as transfer origin and destination
    And the harness injects order-management's SiteSkuDemandChanged for site "P5D<run>": 100 units of SKU "SKU-P5B-BC-<run>" due in 2 hours
    And the harness injects order-management's SiteSkuDemandChanged for site "P5O<run>": 1 units of SKU "SKU-P5B-AP-OTHER-<run>" due in 2 hours
    And warehouse-planning publishes a capacity plan for site "P5O<run>" of 1 orders per hour covering the next 11 hours
    And warehouse-planning publishes a capacity plan for site "P5D<run>" of 1000 orders per hour covering the next 48 hours
    When an operator approves a transfer of 25 eaches of SKU "SKU-P5B-BC-<run>" from "P5O<run>" to "P5D<run>" in network-inventory-planning
    Then the approval is refused with status 422 and problem "below-one-case"

  # ADR 0018 (per-site staleness). A third site holds a demand fact stated 2 hours ago (the
  # CloudEvent time is the fact's watermark; the limit is 10 minutes), so a stale fact of
  # ANOTHER site is in NIP's read models while the fresh pair is approved. Stale facts of other
  # sites in the cluster (AUS3, E2E-ORIGIN, ...) are in the same position. An origin that is
  # the stale site, or a site with no facts at all, is refused naming that site.
  @e2e @phase5 @network-inventory-planning
  Scenario: A stale fact excludes only its own site: the fresh pair is approved and a site without fresh facts is refused by name
    Given SKU "SKU-P5B-ST-<run>" declares an EACH GTIN and a CASE pack level of 12 eaches with case dimensions 400x300x200 mm and 6500 g in product-master
    And network-inventory-planning eventually plans SKU "SKU-P5B-ST-<run>" in cases of 12 eaches
    And facility-layout enables site "P5O<run>" as transfer origin and destination
    And facility-layout enables site "P5D<run>" as transfer origin and destination
    And facility-layout enables site "P5S<run>" as transfer origin and destination
    And the harness injects order-management's SiteSkuDemandChanged for site "P5S<run>": 1 units of SKU "SKU-P5B-AP-OTHER-<run>" due in 2 hours, stated 2 hours ago
    And the harness injects order-management's SiteSkuDemandChanged for site "P5D<run>": 100 units of SKU "SKU-P5B-ST-<run>" due in 2 hours
    And the harness injects order-management's SiteSkuDemandChanged for site "P5O<run>": 1 units of SKU "SKU-P5B-AP-OTHER-<run>" due in 2 hours
    And warehouse-planning publishes a capacity plan for site "P5O<run>" of 1000 orders per hour covering the next 48 hours
    And warehouse-planning publishes a capacity plan for site "P5D<run>" of 1000 orders per hour covering the next 48 hours
    When an operator approves a transfer of 25 eaches of SKU "SKU-P5B-ST-<run>" from "P5O<run>" to "P5D<run>" in network-inventory-planning
    Then the approval is accepted for 2 cases of 12 eaches: requested 25, rounded 24, remainder 1
    When an operator approves a transfer of 25 eaches of SKU "SKU-P5B-ST-<run>" from "P5S<run>" to "P5D<run>" and network-inventory-planning eventually refuses it as facts-incomplete naming site "P5S<run>" as stale
    And an operator approves a transfer of 25 eaches of SKU "SKU-P5B-ST-<run>" from "P5N<run>" to "P5D<run>" and network-inventory-planning eventually refuses it as facts-incomplete naming site "P5N<run>"

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
