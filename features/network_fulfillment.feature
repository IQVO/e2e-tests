Feature: Network fulfillment receives credential-free stub demand through its real poller
  Network demand is file-seeded before the service starts because its only
  production intake is polling. The setup scenario is deliberately excluded
  from the normal suite and is run once by scripts/03-up-services.sh before
  the poller starts.

  Background:
    Given all warehouse-systems services are healthy

  @setup @network-seed
  Scenario: Seed run-scoped facility and inventory prerequisites before polling begins
    Given site "WH1" named "Fulfilment Centre One" exists in facility-layout
    And a Bin "E2E-NET-BIN-<run>" with capacity 100 exists in inventory-storage
    When I receive 10 units of SKU "NET-SKU-<run>" in inventory-storage
    And I stow 10 units of SKU "NET-SKU-<run>" into bin "E2E-NET-BIN-<run>" in inventory-storage
    Then the usable inventory for SKU "NET-SKU-<run>" in inventory-storage is 10
