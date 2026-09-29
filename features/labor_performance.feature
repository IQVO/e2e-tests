Feature: labor-performance scores a completed task against its engineered standard
  labor-performance is a PURE Kafka consumer of fulfillment-execution's
  TaskCompleted event (warehouse.fulfillment.events, the same shared
  topic wes-work-planning also consumes) -- it has no REST endpoint any
  caller writes to; a TaskPerformance row can only ever be created by
  that consumer actually running against a real completed task. This
  scenario is the only proof, across the whole estate, that the
  consumer really delivers: an associate is checked into a station
  (fulfillment-execution's occupantId, surfaced best-effort on
  TaskCompleted as associateId), completes a task against an active
  engineered standard, and labor-performance eventually reports a
  scorecard reflecting it.

  Background:
    Given all warehouse-systems services are healthy

  @e2e @labor-performance
  Scenario: A completed task is scored against the active standard and reflected on the associate's scorecard
    # --- labor-performance: define the engineered standard this task will be scored against ---
    Given labor-performance defines a standard of 60 expected seconds for task type "PICK"

    # --- fulfillment-execution: an associate checks in, claims, and completes a PICK task ---
    Given a station "station-e2e-labor-<run>" is registered with capabilities "pick" in fulfillment-execution
    And associate "assoc-e2e-labor-<run>" checks into station "station-e2e-labor-<run>" in fulfillment-execution
    And wes-work-planning has a work pool for process path "pick-zone-a"
    And I enqueue work unit "wu-e2e-labor-<run>" with cpt in 1 hour and reference "order-e2e-labor-<run>" to process path "pick-zone-a" in wes-work-planning
    And work is released from process path "pick-zone-a" in wes-work-planning
    When fulfillment-execution eventually creates a task for order "wu-e2e-labor-<run>"
    And station "station-e2e-labor-<run>" claims the next "PICK" task for order "wu-e2e-labor-<run>" in fulfillment-execution
    Then the claimed task is for order "wu-e2e-labor-<run>"
    When station "station-e2e-labor-<run>" completes the claimed task in fulfillment-execution

    # --- labor-performance: the Kafka-consumer proof ---
    # fulfillment-execution publishes TaskCompleted (with associateId
    # "assoc-e2e-labor-<run>", best-effort from the check-in above) to
    # warehouse.fulfillment.events; labor-performance's consumer scores
    # it against the standard defined above and projects a scorecard.
    Then labor-performance eventually reports a scorecard for associate "assoc-e2e-labor-<run>" with at least 1 task scored
