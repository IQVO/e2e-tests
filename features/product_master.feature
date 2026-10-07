Feature: product-master owns classification and the physical profile; inventory-storage keeps a copy
  product-master is the WMS-tier owner of SKU master data (its ADR 0001):
  the handling classification moved there from inventory-storage (its ADR
  0003, inventory-storage ADR 0034), and it adds the declared-versus-measured
  physical profile (its ADR 0002). Writes are idempotent PUTs; every accepted
  change leaves through the outbox as a CloudEvent on
  warehouse.product-master.events, which inventory-storage, order-management,
  wes-work-planning and fulfillment-execution consume into local copies.

  What makes each assertion discriminating:
  - inventory-storage only learns "Hazmat" through the Kafka hop: its own
    write endpoint is retired, so nothing else can put the tag there.
  - The retired inventory-storage PUT is sent with a DIFFERENT tag set
    ("Fragile"); the 410 must name classification-moved AND the local copy
    must still say "Hazmat", so a PUT that silently wrote would fail.
  - The physical profile is read twice: declared only (effective source
    declared, no discrepancy), then after a measurement whose weight differs
    by more than 10 % (effective source measured, discrepancy true). The
    flip proves the measurement, not a default, produced the answer.

  The SKU is run-scoped: a measurement older than the stored one is a 409,
  so a fixed SKU would fail on the second run against a dirty database.

  Background:
    Given all warehouse-systems services are healthy

  @e2e @product-master
  Scenario: Classify and dimension a SKU in product-master; inventory-storage follows and refuses writes
    Given SKU "SKU-PM-<run>" is registered in product-master
    When SKU "SKU-PM-<run>" is classified with handling tags "Hazmat" in product-master
    Then inventory-storage eventually knows SKU "SKU-PM-<run>" as "Hazmat"

    When SKU "SKU-PM-<run>" is declared in product-master as 200x120x80 mm and 1500 g
    Then product-master's physical profile of SKU "SKU-PM-<run>" has effective source "declared" and discrepancy false

    When SKU "SKU-PM-<run>" is measured in product-master as 205x121x82 mm and 1720 g
    Then product-master's physical profile of SKU "SKU-PM-<run>" has effective source "measured" and discrepancy true
    And product-master's effective dimensions of SKU "SKU-PM-<run>" are 205x121x82 mm and 1720 g

    Then classifying SKU "SKU-PM-<run>" with handling tags "Fragile" in inventory-storage is refused with 410 "classification-moved"
    And inventory-storage still knows SKU "SKU-PM-<run>" as "Hazmat"
