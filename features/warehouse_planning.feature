Feature: warehouse-planning plans path capacity against assigned demand
  warehouse-planning is the capacity-planning context: it registers the rate
  constraints of each process step (LABOR here), normalizes a process path to
  ORDER per HOUR through a WorkloadProfile, finds the bottleneck step, and
  turns a window's assigned demand into a CapacityPlan (DRAFT, then
  PUBLISHED). It also composes the stations facility-layout tallied with an
  operator-declared per-station throughput at read time (design doc section
  19). These scenarios exercise its REST surface only.

  Deliberate scope: warehouse-planning's MCP server (cmd/mcp) and its Kafka
  consumers/publisher are NOT covered -- this harness has no MCP or Kafka
  step vocabulary, and the service runs here with Kafka disabled (see
  scripts/03-up-services.sh). The facility-layout station tally its Kafka
  consumer would write is seeded directly into its own Postgres instead.

  Locations, process paths and tally zones are run-scoped (E2E<run>...), so
  re-running against a dirty database never collides or accumulates. A
  location code is a single token with no dashes: the service resolves a
  site's zones by the "<location>-" prefix of the zone id.

  Background:
    Given warehouse-planning is healthy

  # Design doc section 43. Pick 4000 UNIT/h -> 1600 ORDER/h, Rebin 2500
  # UNIT/h -> 1000, Pack 1800 PACKAGE/h -> 1800 (units_per_order 2.5,
  # packages_per_order 1). Rebin bottlenecks the path at 1000 ORDER/h; over the
  # 8h window that is 8000 orders against 12000 demand: shortage 4000.
  @e2e @warehouse-planning
  Scenario: A demand above the bottleneck is a shortage, and a plan is published exactly once
    When I register a LABOR constraint of 4000 UNIT per HOUR for PICK at "E2E<run>S43" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" in warehouse-planning
    And I register a LABOR constraint of 2500 UNIT per HOUR for REBIN at "E2E<run>S43" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" in warehouse-planning
    And I register a LABOR constraint of 1800 PACKAGE per HOUR for PACK at "E2E<run>S43" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" in warehouse-planning
    And I register process path "e2e-s43-<run>" named "Pick-Rebin-Pack" with steps PICK, REBIN, PACK in warehouse-planning
    Then the response status is 201
    When I look up the capacity of process path "e2e-s43-<run>" at "E2E<run>S43" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with units_per_order 2.5 and packages_per_order 1 in warehouse-planning
    Then the response status is 200
    And the process path capacity is 1000 ORDER per HOUR bound by REBIN
    And the step breakdown is "PICK:1600:LABOR,REBIN:1000:LABOR,PACK:1800:LABOR"
    And the response has 0 warnings
    When I create a capacity plan for warehouse "WH-E2E" at "E2E<run>S43" on path "e2e-s43-<run>" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with assigned demand 12000, units_per_order 2.5 and packages_per_order 1 in warehouse-planning
    Then the response status is 201
    And the capacity plan is DRAFT with path capacity 1000 ORDER per HOUR, capacity over window 8000, shortage 4000 and bottleneck REBIN
    And the capacity plan is bound by LABOR
    When I publish the capacity plan in warehouse-planning
    Then the response status is 200
    And the capacity plan is PUBLISHED with path capacity 1000 ORDER per HOUR, capacity over window 8000, shortage 4000 and bottleneck REBIN
    When I publish the capacity plan in warehouse-planning
    Then the response is a problem with status 409 and type "capacity-plan-already-published"
    When I get the capacity plan in warehouse-planning
    Then the response status is 200
    And the capacity plan is PUBLISHED with path capacity 1000 ORDER per HOUR, capacity over window 8000, shortage 4000 and bottleneck REBIN

  # Same capacities, demand 6000 <= 8000 orders over the window: no shortage.
  @e2e @warehouse-planning
  Scenario: A demand within the bottleneck's capacity has no shortage
    When I register a LABOR constraint of 4000 UNIT per HOUR for PICK at "E2E<run>WIC" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" in warehouse-planning
    And I register a LABOR constraint of 2500 UNIT per HOUR for REBIN at "E2E<run>WIC" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" in warehouse-planning
    And I register a LABOR constraint of 1800 PACKAGE per HOUR for PACK at "E2E<run>WIC" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" in warehouse-planning
    And I register process path "e2e-wic-<run>" named "Pick-Rebin-Pack" with steps PICK, REBIN, PACK in warehouse-planning
    And I create a capacity plan for warehouse "WH-E2E" at "E2E<run>WIC" on path "e2e-wic-<run>" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with assigned demand 6000, units_per_order 2.5 and packages_per_order 1 in warehouse-planning
    Then the response status is 201
    And the capacity plan is DRAFT with path capacity 1000 ORDER per HOUR, capacity over window 8000, shortage 0 and bottleneck REBIN

  # Requests the service must refuse, as RFC 7807 problem+json.
  @e2e @warehouse-planning
  Scenario: Invalid capacity plan requests are rejected as problem+json
    When I register a LABOR constraint of 4000 UNIT per HOUR for PICK at "E2E<run>VAL" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" in warehouse-planning
    And I register process path "e2e-val-<run>" named "Pick-Only" with steps PICK in warehouse-planning
    When I create a capacity plan for warehouse "WH-E2E" at "E2E<run>VAL" on path "e2e-unknown-<run>" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with assigned demand 100, units_per_order 2.5 and packages_per_order 1 in warehouse-planning
    Then the response is a problem with status 404 and type "process-path-not-found"
    When I create a capacity plan for warehouse "WH-E2E" at "E2E<run>VAL" on path "e2e-val-<run>" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with assigned demand -1, units_per_order 2.5 and packages_per_order 1 in warehouse-planning
    Then the response is a problem with status 422 and type "negative-assigned-demand"
    When I create a capacity plan for warehouse "WH-E2E" at "E2E<run>VAL" on path "e2e-val-<run>" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" without an assigned demand in warehouse-planning
    Then the response is a problem with status 422 and type "missing-assigned-demand"

  # Design doc section 19. facility-layout tallied 10 PACK work-center
  # stations in the site's zone; the operator declares one station does 180
  # PACKAGE/h, so the stations cap PACK at 10 x 180 = 1800 PACKAGE/h even
  # though PACK labor is 2500. Pick 8000 UNIT/h -> 3200 ORDER/h, Rebin 6000
  # UNIT/h -> 2400, PACK 1800 -> 1800: the PACK stations (not labor) bind.
  @e2e @warehouse-planning
  Scenario: The pack stations facility-layout tallied bind the path
    Given warehouse-planning's facility-layout tally holds 10 PACK work-center stations in zone "E2E<run>-OPS-WC"
    When I declare a station standard of 180 PACKAGE per HOUR for PACK at "E2E<run>" in warehouse-planning
    Then the response status is 201
    When I register a LABOR constraint of 2500 PACKAGE per HOUR for PACK at "E2E<run>" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" in warehouse-planning
    And I register a LABOR constraint of 8000 UNIT per HOUR for PICK at "E2E<run>" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" in warehouse-planning
    And I register a LABOR constraint of 6000 UNIT per HOUR for REBIN at "E2E<run>" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" in warehouse-planning
    And I register process path "e2e-stations-<run>" named "Pick-Rebin-Pack" with steps PICK, REBIN, PACK in warehouse-planning
    And I look up the capacity of process path "e2e-stations-<run>" at "E2E<run>" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with units_per_order 2.5 and packages_per_order 1 in warehouse-planning
    Then the response status is 200
    And the process path capacity is 1800 ORDER per HOUR bound by PACK
    And the step breakdown is "PICK:3200:LABOR,REBIN:2400:LABOR,PACK:1800:STATION"
    And the response has 0 warnings
    When I look up the storage capacity of "E2E<run>" in warehouse-planning
    Then the response status is 200
    And the storage capacity lists 10 PACK stations in zone "E2E<run>-OPS-WC"
