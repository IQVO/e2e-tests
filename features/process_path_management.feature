Feature: process-path-management is the fleet's declared process-path catalogue source
  process-path-management is a Generic Subdomain owning the fleet's
  declared process-path catalogue (canonical identity, matchPrefix,
  requiredCapabilities), replacing the static
  warehouse-infra/config/process-paths/sortable-fc.yaml file the other
  services previously loaded once at boot. This scenario exercises its
  full REST lifecycle directly (define, get, list, revise, deactivate)
  and its Kafka publisher (ProcessPathCreated/Updated/Deactivated onto
  the fleet's shared broker) -- the only proof, across the whole
  estate, that this context's write side and its event contract both
  work against a real, independently running process and a real
  broker, not just its own unit/integration tests.

  Background:
    Given all warehouse-systems services are healthy

  @e2e @process-path-management
  Scenario: Defining, revising, and deactivating a process path is reflected in its own read model
    # --- define: a brand-new path, rejecting a duplicate id ---
    When I define process path "<run>" with match prefix "e2e-pp" and required capabilities "pick" in process-path-management
    Then the response status is 201
    And process path "<run>" in process-path-management has status "ACTIVE"

    # --- get: the just-defined path is retrievable by id ---
    When I get process path "<run>" from process-path-management
    Then the response status is 200
    And the process path response match prefix is "e2e-pp"

    # --- list: the just-defined path appears in the default (active-only) listing ---
    Then process-path-management's active process path listing includes "<run>"

    # --- revise: matchPrefix/requiredCapabilities change, pathId/direct do not ---
    When I revise process path "<run>" to match prefix "e2e-pp-revised" and required capabilities "pick,hazmat" in process-path-management
    Then the response status is 200
    And the process path response match prefix is "e2e-pp-revised"

    # --- deactivate: idempotent, publishes ProcessPathDeactivated once ---
    When I deactivate process path "<run>" in process-path-management
    Then the response status is 204
    And process path "<run>" in process-path-management has status "DEACTIVATED"

    # --- deactivating an already-deactivated path is a no-op success ---
    When I deactivate process path "<run>" in process-path-management
    Then the response status is 204

    # --- the default (active-only) listing no longer includes it, but the audit view (?all=true) does ---
    Then process-path-management's active process path listing does not include "<run>"
    And process-path-management's full process path listing includes "<run>"
