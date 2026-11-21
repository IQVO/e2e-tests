Feature: Phase 5 - slotting-optimization plans a whole-case SKU as CASE on a case-pick face and every other SKU as EACH on a forward face
  slotting-optimization ADR 0012 (case-pick faces). With CASE_PICK_ZONE_CODES=CPK a face is a case-pick face when its
  zone's behavioural class (the zoneCode of the facility-layout zone) is CPK. A SKU is planned pickUnit CASE only when
  case faces exist, it has an ACTIVE CASE pack level WITH a size, and the greatest common divisor of its demand line
  units is a whole number of cases; CASE SKUs go ONLY to CPK faces and EACH SKUs only to the other forward faces (FWD).
  An assignment carries pickUnit and, for CASE, casesCapacity: the whole cases the face holds, the tighter of the
  volume and the weight limit. The same appears on GET /forward-slots and on the SlotPlanApproved event (pick_unit and
  cases_capacity on CASE assignments only).

  Live, black-box scenario through the Kong gateway of the kind cluster. It needs the cluster's slotting-optimization
  to run with CASE_PICK_ZONE_CODES=CPK and PACK_LEVEL_MODE=kafka:

    P=http://localhost:8000/api
    PRODUCT_MASTER_BASE_URL=$P/product-master FACILITY_BASE_URL=$P/facility-layout \
    SLOTTING_BASE_URL=$P/slotting-optimization KAFKA_BROKERS=localhost:9092 \
    GODOG_TAGS=@case-pick go test -count=1 -v -run TestMain -timeout 10m .

  How each fact is produced:
  - The layout is created through facility-layout's REST on a RUN-SCOPED site (CP<run>: uppercase letters and digits):
    one zone with zoneCode CPK (the case-pick class) and one with zoneCode FWD (the each-pick class). The two CPK slots
    carry an explicit capacityOverride, chosen so the two limits give DIFFERENT answers on each face: 40 kg / 0.2 m3 is
    8 cases by volume but 6 by weight (weight-bound, 6), 100 kg / 0.1 m3 is 4 cases by volume but 15 by weight
    (volume-bound, 4), for a case of 400x300x200 mm (0.024 m3) and 6500 g. The FWD slots use the location type's
    default (Shelf: 150 kg / 0.4 m3; facility-layout's placement rules allow only CartonFlow, Shelf and ToteWall in a FWD zone). There are exactly as many slots as SKUs per class, so a SKU planned into the wrong zone
    would leave another unassigned.
  - Products are registered in product-master with a declared and measured physical profile; the CASE ones declare an
    EACH and a CASE pack level of 12 with declared dimensions and weight (valid run-unique GTIN-14s, phase5_b steps).
    The mixed SKU's pack level is declared FIRST, so its pack level is in slotting's copy no later than the others'.
    The planner derives the ABC class from the demand.
  - Demand is INJECTED as order-management's SiteSkuDemandChanged (same as features/slotting_optimization.feature: the
    real service projects every line to one static site and a plan counts only lines due in the past).
  - Slotting builds four local copies (layout, product profiles, pack levels, demand) from Kafka and only the demand
    one is readable over REST, so the plan is drafted until it matches (up to 2 minutes), discarding each incomplete
    Draft. A product bug shows as that wait running out with the last plan's mismatch.

  Background:
    Given slotting-optimization, facility-layout and product-master are healthy

  @e2e @phase5 @case-pick
  Scenario: Case-pick faces: whole-case demand is planned as CASE on a CPK face with its casesCapacity, each and mixed demand stay EACH on FWD faces
    # Layout: the case-pick zone (CPK) with a weight-bound and a volume-bound face, and the forward zone (FWD) with two each-pick faces.
    Given site "CP<run>" has slot "CP<run>-PCK-CPK-A01-01-01-A" of location type "Shelf" limited to 40 kg and 0.2 m3 in facility-layout
    And site "CP<run>" has slot "CP<run>-PCK-CPK-A01-01-02-A" of location type "Shelf" limited to 100 kg and 0.1 m3 in facility-layout
    And site "CP<run>" has slot "CP<run>-AMB-FWD-A01-01-01-A" of location type "Shelf" with its default limits in facility-layout
    And site "CP<run>" has slot "CP<run>-AMB-FWD-A01-01-02-A" of location type "Shelf" with its default limits in facility-layout

    # Products: SKU-MIXED has a CASE of 12 but its demand is not whole cases; SKU-EACH has no CASE level at all.
    And SKU "SKU-CPK-MIXED-<run>" is registered and measured for slotting in product-master
    And SKU "SKU-CPK-MIXED-<run>" declares an EACH GTIN and a CASE pack level of 12 eaches with case dimensions 400x300x200 mm and 6500 g in product-master
    And SKU "SKU-CPK-CASE-<run>" is registered and measured for slotting in product-master
    And SKU "SKU-CPK-CASE-<run>" declares an EACH GTIN and a CASE pack level of 12 eaches with case dimensions 400x300x200 mm and 6500 g in product-master
    And SKU "SKU-CPK-CASEB-<run>" is registered and measured for slotting in product-master
    And SKU "SKU-CPK-CASEB-<run>" declares an EACH GTIN and a CASE pack level of 12 eaches with case dimensions 400x300x200 mm and 6500 g in product-master
    And SKU "SKU-CPK-EACH-<run>" is registered and measured for slotting in product-master
    And SKU "SKU-CPK-EACH-<run>" declares only an EACH GTIN in product-master

    # Demand: 24 and 36 eaches have a gcd of 12 (two whole cases and three); 24 alone is two cases; 5 and 7 have a gcd of 1.
    And order-management's demand projection states order lines of 24 and 36 units of SKU "SKU-CPK-CASE-<run>" at site "CP<run>"
    And order-management's demand projection states order lines of 24 units of SKU "SKU-CPK-CASEB-<run>" at site "CP<run>"
    And order-management's demand projection states order lines of 5 and 7 units of SKU "SKU-CPK-MIXED-<run>" at site "CP<run>"
    And order-management's demand projection states order lines of 5 and 7 units of SKU "SKU-CPK-EACH-<run>" at site "CP<run>"
    And slotting-optimization eventually reports 2 picks and 60 units for SKU "SKU-CPK-CASE-<run>" at site "CP<run>"
    And slotting-optimization eventually reports 1 pick and 24 units for SKU "SKU-CPK-CASEB-<run>" at site "CP<run>"
    And slotting-optimization eventually reports 2 picks and 12 units for SKU "SKU-CPK-MIXED-<run>" at site "CP<run>"
    And slotting-optimization eventually reports 2 picks and 12 units for SKU "SKU-CPK-EACH-<run>" at site "CP<run>"

    # The faster CASE SKU gets the lexically first CPK face (weight-bound: 6 cases), the other the volume-bound one (4 cases).
    When slotting-optimization eventually drafts plan "cases" for site "CP<run>" with these pick units:
      | sku                 | slot                        | pickUnit | casesCapacity |
      | SKU-CPK-CASE-<run>  | CP<run>-PCK-CPK-A01-01-01-A | CASE     | 6             |
      | SKU-CPK-CASEB-<run> | CP<run>-PCK-CPK-A01-01-02-A | CASE     | 4             |
      | SKU-CPK-MIXED-<run> | zone FWD                    | EACH     |               |
      | SKU-CPK-EACH-<run>  | zone FWD                    | EACH     |               |
    Then plan "cases" of site "CP<run>" is "Draft" at version 1
    And plan "cases" of site "CP<run>" read back by id carries the same pick units and cases capacities
    And every CASE assignment of plan "cases" of site "CP<run>" holds the whole cases of a 400x300x200 mm, 6500 g case that its slot's volume and weight limits allow
    And plan "cases" of site "CP<run>" has exactly one Assign move per assigned SKU, to the slot it is assigned
    And slotting-optimization has no forward slots for site "CP<run>" yet

    When plan "cases" of site "CP<run>" is approved in slotting-optimization
    Then plan "cases" of site "CP<run>" is "Approved" at version 2
    And slotting-optimization's forward slots of site "CP<run>" come from plan "cases" and carry its pick units and cases capacities
    And slotting-optimization published SlotPlanApproved for plan "cases" of site "CP<run>" with pick_unit and cases_capacity on the CASE assignments only
