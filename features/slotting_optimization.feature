Feature: slotting-optimization proposes forward pick slots by velocity and a human approves them
  slotting-optimization is the WMS-tier planner of forward pick slots (its ADR
  0001/0002). From event-fed local copies (its ADR 0003) of demand
  (order-management's SiteSkuDemandChanged), product master data
  (product-master's classification and physical profile) and layout
  (facility-layout's zones and slots) it computes a SlotPlan under policy
  abc-velocity-v1: rank the SKUs by order-line picks in the lookback window,
  give each placeable SKU a forward slot (zone code FWD, hazmat and temperature
  compatible, one unit fits), best-ranked SKU first onto the best slot (v1: the
  lexically first location code), and let a SKU that still fits its current slot
  keep it. A plan starts as a Draft; only a human approving it makes it the
  site's forward-slot map.

  Services started by scripts/03-up-services.sh for this feature:
  slotting-optimization (LAYOUT_MODE, PRODUCT_MODE and DEMAND_MODE=kafka, each
  with its own per-run consumer group), facility-layout and product-master.
  Planning, the forward-slot map and the SKU velocity are read over REST from
  slotting-optimization itself; SlotPlanApproved is also read from its Kafka
  topic.

  Demand is INJECTED, not produced by a real order-management. slotting-
  optimization learns demand only from SiteSkuDemandChanged. order-management
  projects every order line to ONE statically configured site per deployment
  (DEMAND_PROJECTION_SITE_ID) and stamps the line's PROMISE date (48 h ahead by
  default) as due_at, while a plan counts a line only when due_at falls in
  [now - lookback, now): real orders could neither give each scenario its own
  site nor be visible to a plan for two days. The harness therefore publishes
  the facts order-management would (same topic, type, key and payload, due one
  hour ago), exactly as the inter-warehouse transfer feature does.

  Every scenario owns a run-scoped SITE (SLOTRANK<run>, SLOTSTICK<run>, ...),
  with its own zone, slots, SKUs and demand: no scenario can see another's
  inputs or Approved plan, so every assertion is exact (no "at least", no
  tolerance for leftovers from earlier runs) and nothing is ever cleaned up.

  What makes each assertion discriminating:
  - Ranking: the SKU with 3 picks must get the lexically FIRST slot and the SKU
    with 1 pick the second. Registering the two slots in the opposite order, or
    ranking by SKU name, would flip it. The same table is checked on the plan,
    on GET /forward-slots after approval and on the SlotPlanApproved event.
  - Stickiness: after the first plan is approved, the SLOWER SKU is given five
    more picks, so by velocity alone the two SKUs would swap slots (two
    Relocate moves). The second plan must keep both where they are with no move
    at all; a planner without stickiness fails both checks.
  - Hazmat: the hazmat SKU is the FASTEST one and the ambient forward slot sorts
    BEFORE the hazmat one, so a planner that ignored the hazmat flag would give
    it the ambient slot. A second hazmat SKU finds the only hazmat slot taken and
    must be left unassigned (NoEligibleSlot), not squeezed into the ambient one.
    A guard rejects the plan the moment any hazmat SKU is placed anywhere else,
    even in an intermediate plan.
  - Approval: a second approval of the same plan is refused with the
    plan-not-draft problem (not just any 409), the plan stays at version 2 and
    exactly one SlotPlanApproved is ever published for it.

  A plan reads three local copies and the REST surface only shows the demand
  one (GET /sku-velocity), so the scenarios first wait for the demand to be
  visible, then draft plans until one reflects the layout and product copies too
  (every intermediate Draft is rejected, so none is left behind).

  Background:
    Given slotting-optimization, facility-layout and product-master are healthy

  @e2e @slotting
  Scenario: The faster SKU gets the better forward slot and approving the plan publishes the forward-slot map
    Given site "SLOTRANK<run>" has forward slots "SLOTRANK<run>-AMB-FWD-A01-01-01-A" and "SLOTRANK<run>-AMB-FWD-A01-01-02-A" in facility-layout
    And SKU "SKU-SLOT-HOT-<run>" is registered and measured for slotting in product-master
    And SKU "SKU-SLOT-COLD-<run>" is registered and measured for slotting in product-master
    And order-management's demand projection states 3 order lines of 2 units of SKU "SKU-SLOT-HOT-<run>" at site "SLOTRANK<run>"
    And order-management's demand projection states 1 order line of 1 unit of SKU "SKU-SLOT-COLD-<run>" at site "SLOTRANK<run>"
    And slotting-optimization eventually reports 3 picks and 6 units for SKU "SKU-SLOT-HOT-<run>" at site "SLOTRANK<run>"
    And slotting-optimization eventually reports 1 pick and 1 unit for SKU "SKU-SLOT-COLD-<run>" at site "SLOTRANK<run>"

    When slotting-optimization eventually drafts plan "first" for site "SLOTRANK<run>" that assigns exactly:
      | sku                 | slot                              | unassigned |
      | SKU-SLOT-HOT-<run>  | SLOTRANK<run>-AMB-FWD-A01-01-01-A |            |
      | SKU-SLOT-COLD-<run> | SLOTRANK<run>-AMB-FWD-A01-01-02-A |            |
    Then plan "first" of site "SLOTRANK<run>" is "Draft" at version 1
    And plan "first" of site "SLOTRANK<run>" puts SKU "SKU-SLOT-HOT-<run>" in class "A"
    And plan "first" of site "SLOTRANK<run>" has exactly these moves:
      | sku                 | kind   | from | to                                |
      | SKU-SLOT-HOT-<run>  | Assign |      | SLOTRANK<run>-AMB-FWD-A01-01-01-A |
      | SKU-SLOT-COLD-<run> | Assign |      | SLOTRANK<run>-AMB-FWD-A01-01-02-A |
    # Nothing is the site's forward-slot map until a human approves the Draft.
    And slotting-optimization has no forward slots for site "SLOTRANK<run>" yet

    When plan "first" of site "SLOTRANK<run>" is approved in slotting-optimization
    Then plan "first" of site "SLOTRANK<run>" is "Approved" at version 2
    And slotting-optimization's forward slots of site "SLOTRANK<run>" come from plan "first" and are exactly:
      | sku                 | slot                              |
      | SKU-SLOT-HOT-<run>  | SLOTRANK<run>-AMB-FWD-A01-01-01-A |
      | SKU-SLOT-COLD-<run> | SLOTRANK<run>-AMB-FWD-A01-01-02-A |
    And slotting-optimization published SlotPlanApproved for plan "first" of site "SLOTRANK<run>" with exactly these assignments:
      | sku                 | slot                              |
      | SKU-SLOT-HOT-<run>  | SLOTRANK<run>-AMB-FWD-A01-01-01-A |
      | SKU-SLOT-COLD-<run> | SLOTRANK<run>-AMB-FWD-A01-01-02-A |

  @e2e @slotting
  Scenario: A SKU that still fits keeps its slot even when a faster SKU overtakes it, and the new plan moves nothing
    Given site "SLOTSTICK<run>" has forward slots "SLOTSTICK<run>-AMB-FWD-A01-01-01-A" and "SLOTSTICK<run>-AMB-FWD-A01-01-02-A" in facility-layout
    And SKU "SKU-STICK-HOT-<run>" is registered and measured for slotting in product-master
    And SKU "SKU-STICK-COLD-<run>" is registered and measured for slotting in product-master
    And order-management's demand projection states 3 order lines of 2 units of SKU "SKU-STICK-HOT-<run>" at site "SLOTSTICK<run>"
    And order-management's demand projection states 1 order line of 1 unit of SKU "SKU-STICK-COLD-<run>" at site "SLOTSTICK<run>"
    And slotting-optimization eventually reports 3 picks and 6 units for SKU "SKU-STICK-HOT-<run>" at site "SLOTSTICK<run>"
    And slotting-optimization eventually reports 1 pick and 1 unit for SKU "SKU-STICK-COLD-<run>" at site "SLOTSTICK<run>"
    And slotting-optimization eventually drafts plan "first" for site "SLOTSTICK<run>" that assigns exactly:
      | sku                  | slot                               | unassigned |
      | SKU-STICK-HOT-<run>  | SLOTSTICK<run>-AMB-FWD-A01-01-01-A |            |
      | SKU-STICK-COLD-<run> | SLOTSTICK<run>-AMB-FWD-A01-01-02-A |            |
    And plan "first" of site "SLOTSTICK<run>" is approved in slotting-optimization

    # The slower SKU now outsells the faster one (6 picks against 3): by
    # velocity alone it would take the first slot and push the other out.
    When order-management's demand projection states 5 order lines of 1 unit of SKU "SKU-STICK-COLD-<run>" at site "SLOTSTICK<run>"
    Then slotting-optimization eventually reports 6 picks and 6 units for SKU "SKU-STICK-COLD-<run>" at site "SLOTSTICK<run>"
    And slotting-optimization eventually reports 3 picks and 6 units for SKU "SKU-STICK-HOT-<run>" at site "SLOTSTICK<run>"

    # Every input is now in place (the demand read above is the same ledger the
    # plan reads), so ONE plan is enough: no retry could hide a wrong answer.
    When slotting-optimization drafts plan "second" for site "SLOTSTICK<run>" that assigns exactly:
      | sku                  | slot                               | unassigned |
      | SKU-STICK-HOT-<run>  | SLOTSTICK<run>-AMB-FWD-A01-01-01-A |            |
      | SKU-STICK-COLD-<run> | SLOTSTICK<run>-AMB-FWD-A01-01-02-A |            |
    Then plan "second" of site "SLOTSTICK<run>" has no moves
    And plan "second" of site "SLOTSTICK<run>" is "Draft" at version 1

  @e2e @slotting
  Scenario: A hazmat SKU is never assigned to a non-hazmat slot
    Given site "SLOTHAZ<run>" has forward slot "SLOTHAZ<run>-AMB-FWD-A01-01-01-A" in facility-layout
    And site "SLOTHAZ<run>" has hazmat forward slot "SLOTHAZ<run>-HAZ-FWD-A01-01-01-A" in facility-layout
    And SKU "SKU-HAZ-ONE-<run>" is registered, classified with handling tags "Hazmat" and measured for slotting in product-master
    And SKU "SKU-HAZ-TWO-<run>" is registered, classified with handling tags "Hazmat" and measured for slotting in product-master
    And SKU "SKU-HAZ-PLAIN-<run>" is registered and measured for slotting in product-master
    And slotting-optimization must never place SKU "SKU-HAZ-ONE-<run>" anywhere but in slot "SLOTHAZ<run>-HAZ-FWD-A01-01-01-A" at site "SLOTHAZ<run>"
    And slotting-optimization must never place SKU "SKU-HAZ-TWO-<run>" anywhere but in slot "SLOTHAZ<run>-HAZ-FWD-A01-01-01-A" at site "SLOTHAZ<run>"
    And slotting-optimization must never place SKU "SKU-HAZ-PLAIN-<run>" anywhere but in slot "SLOTHAZ<run>-AMB-FWD-A01-01-01-A" at site "SLOTHAZ<run>"
    # Ranked HAZ-ONE (3 picks), HAZ-TWO (2), PLAIN (1); the ambient slot sorts
    # first, so ignoring the hazmat flag would hand it to HAZ-ONE.
    And order-management's demand projection states 3 order lines of 1 unit of SKU "SKU-HAZ-ONE-<run>" at site "SLOTHAZ<run>"
    And order-management's demand projection states 2 order lines of 1 unit of SKU "SKU-HAZ-TWO-<run>" at site "SLOTHAZ<run>"
    And order-management's demand projection states 1 order line of 1 unit of SKU "SKU-HAZ-PLAIN-<run>" at site "SLOTHAZ<run>"
    And slotting-optimization eventually reports 3 picks and 3 units for SKU "SKU-HAZ-ONE-<run>" at site "SLOTHAZ<run>"
    And slotting-optimization eventually reports 2 picks and 2 units for SKU "SKU-HAZ-TWO-<run>" at site "SLOTHAZ<run>"
    And slotting-optimization eventually reports 1 pick and 1 unit for SKU "SKU-HAZ-PLAIN-<run>" at site "SLOTHAZ<run>"

    When slotting-optimization eventually drafts plan "hazmat" for site "SLOTHAZ<run>" that assigns exactly:
      | sku                 | slot                              | unassigned     |
      | SKU-HAZ-ONE-<run>   | SLOTHAZ<run>-HAZ-FWD-A01-01-01-A  |                |
      | SKU-HAZ-PLAIN-<run> | SLOTHAZ<run>-AMB-FWD-A01-01-01-A  |                |
      | SKU-HAZ-TWO-<run>   |                                   | NoEligibleSlot |
    Then plan "hazmat" of site "SLOTHAZ<run>" is "Draft" at version 1

  @e2e @slotting
  Scenario: Approving a plan that is already approved is refused with 409
    Given site "SLOTAPPR<run>" has forward slot "SLOTAPPR<run>-AMB-FWD-A01-01-01-A" in facility-layout
    And SKU "SKU-APPR-<run>" is registered and measured for slotting in product-master
    And order-management's demand projection states 2 order lines of 1 unit of SKU "SKU-APPR-<run>" at site "SLOTAPPR<run>"
    And slotting-optimization eventually reports 2 picks and 2 units for SKU "SKU-APPR-<run>" at site "SLOTAPPR<run>"
    And slotting-optimization eventually drafts plan "only" for site "SLOTAPPR<run>" that assigns exactly:
      | sku            | slot                              | unassigned |
      | SKU-APPR-<run> | SLOTAPPR<run>-AMB-FWD-A01-01-01-A |            |

    When plan "only" of site "SLOTAPPR<run>" is approved in slotting-optimization
    Then plan "only" of site "SLOTAPPR<run>" is "Approved" at version 2

    Then approving plan "only" of site "SLOTAPPR<run>" in slotting-optimization is refused with 409 "plan-not-draft"
    And plan "only" of site "SLOTAPPR<run>" is "Approved" at version 2
    And slotting-optimization published exactly 1 SlotPlanApproved event for plan "only" of site "SLOTAPPR<run>"
