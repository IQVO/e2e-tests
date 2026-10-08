Feature: inbound-receiving takes goods from ASN to staged stock
  inbound-receiving is the WMS-tier inbound dock workflow (its ADR 0001/0002):
  a supplier's advance ship notice (ASN), a carrier's booked dock appointment
  and the counted receipt, with the discrepancies found when the receipt
  closes. It hands every Good receipt line to inventory-storage as a
  ReceiptLineReceived event (inbound-receiving ADR 0003, inventory-storage ADR
  0037); no REST or MCP call goes in either direction, and stow stays an RF
  action in inventory-storage.

  Services started by scripts/03-up-services.sh for this feature:
  product-master (SKU existence; inbound-receiving runs PRODUCT_MODE=kafka, so
  an ASN line for a SKU it has not heard of is refused with 422 unknown-sku),
  inbound-receiving and inventory-storage (INBOUND_RECEIPT_CONSUMER_GROUP set).
  Dock doors stay in DOCK_DOOR_MODE=permissive: a door code is accepted
  without a facility-layout dock slot, so no scenario depends on facility
  data. Nothing is injected; every fact comes from a real service.

  What makes each assertion discriminating:
  - The ASN expects 50 units and the dock receives 40, so the Short
    discrepancy carries all three numbers (expected 50, received 40, damaged
    0); the same table is checked on the REST read AND on the ReceiptClosed
    event, so the published contract and the read model agree.
  - inventory-storage has no read of STAGED stock (POST /stock/receive is a
    bare acknowledgement and usable only counts stowed units). The handover is
    therefore proved by the StockReceived it raises on its analytics topic:
    the units, and the NUMBER of events for the SKU, must be exact. Usable
    inventory is read as 0 only AFTER that event, when the consumer has
    provably run, and moves to the received quantity only after the stow.
  - The Damaged line is received BEFORE the Good one on the same ASN (same
    Kafka key, so same partition order): once the Good line's StockReceived is
    seen, a Damaged line that had been booked would already be there as a
    second event, and the "exactly 1 event totalling 6 units" check fails.
  - The overlapping booking is refused with the door-window-overlap problem,
    not just any 409, while a window that starts exactly when the first one
    ends (the window is half-open) is accepted.

  Every mutable id is run-scoped. The world is shared by all scenarios of a
  process, so each scenario also owns its own ASN and SKU tokens.

  Background:
    Given inbound-receiving, product-master and inventory-storage are healthy

  @e2e @inbound-receiving
  Scenario: A short delivery closes with the exact Short discrepancy
    Given SKU "SKU-INB-SHORT-<run>" is registered in product-master
    And ASN "ASN-INB-SHORT-<run>" from supplier "ACME" expecting 50 units of SKU "SKU-INB-SHORT-<run>" is eventually registered in inbound-receiving
    And a receipt is opened for ASN "ASN-INB-SHORT-<run>" in inbound-receiving
    When 40 Good units of line 1 are received on the receipt of ASN "ASN-INB-SHORT-<run>" in inbound-receiving
    And the receipt of ASN "ASN-INB-SHORT-<run>" is closed in inbound-receiving
    Then the receipt of ASN "ASN-INB-SHORT-<run>" in inbound-receiving is Closed with exactly these discrepancies:
      | line | sku                 | kind  | expected | received | damaged |
      | 1    | SKU-INB-SHORT-<run> | Short | 50       | 40       | 0       |
    And inbound-receiving published ReceiptClosed for ASN "ASN-INB-SHORT-<run>" with exactly these discrepancies:
      | line | sku                 | kind  | expected | received | damaged |
      | 1    | SKU-INB-SHORT-<run> | Short | 50       | 40       | 0       |
    And inbound-receiving's ASN "ASN-INB-SHORT-<run>" is "Closed"

  @e2e @inbound-receiving
  Scenario: Good units reach inventory-storage as staged stock and become usable only after the stow
    Given SKU "SKU-INB-STOW-<run>" is registered in product-master
    And a Bin "E2E-INB-BIN-<run>" with capacity 100 exists in inventory-storage
    And ASN "ASN-INB-STOW-<run>" from supplier "ACME" expecting 40 units of SKU "SKU-INB-STOW-<run>" is eventually registered in inbound-receiving
    And a receipt is opened for ASN "ASN-INB-STOW-<run>" in inbound-receiving
    When 40 Good units of line 1 are received on the receipt of ASN "ASN-INB-STOW-<run>" in inbound-receiving
    Then inventory-storage eventually stages 40 units of SKU "SKU-INB-STOW-<run>" in exactly 1 StockReceived event
    And the usable inventory for SKU "SKU-INB-STOW-<run>" in inventory-storage is 0

    When I stow 40 units of SKU "SKU-INB-STOW-<run>" into bin "E2E-INB-BIN-<run>" in inventory-storage
    Then the usable inventory for SKU "SKU-INB-STOW-<run>" in inventory-storage is 40

  @e2e @inbound-receiving
  Scenario: A dock appointment guards its door, checks in and completes when the receipt closes
    Given SKU "SKU-INB-APPT-<run>" is registered in product-master
    And ASN "ASN-INB-APPT-<run>" from supplier "ACME" expecting 5 units of SKU "SKU-INB-APPT-<run>" is eventually registered in inbound-receiving
    And ASN "ASN-INB-APPT2-<run>" from supplier "ACME" expecting 5 units of SKU "SKU-INB-APPT-<run>" is eventually registered in inbound-receiving

    When a dock appointment at door "E2E-INB-DOOR-<run>" for carrier "ACME Freight" and ASN "ASN-INB-APPT-<run>" is booked to start in 10 minutes for 60 minutes in inbound-receiving
    Then the appointment of ASN "ASN-INB-APPT-<run>" in inbound-receiving is "Booked"
    And booking a dock appointment at door "E2E-INB-DOOR-<run>" for ASN "ASN-INB-APPT2-<run>" to start in 40 minutes for 60 minutes in inbound-receiving is refused with 409 "door-window-overlap"

    # The window is half-open: the next one may start exactly when this ends.
    When a dock appointment at door "E2E-INB-DOOR-<run>" for carrier "ACME Freight" and ASN "ASN-INB-APPT2-<run>" is booked to start in 70 minutes for 30 minutes in inbound-receiving
    And the appointment of ASN "ASN-INB-APPT2-<run>" in inbound-receiving is cancelled
    Then the appointment of ASN "ASN-INB-APPT2-<run>" in inbound-receiving is "Cancelled"

    When the carrier of ASN "ASN-INB-APPT-<run>"'s appointment checks in at inbound-receiving
    Then the appointment of ASN "ASN-INB-APPT-<run>" in inbound-receiving is "CheckedIn"

    When a receipt is opened for ASN "ASN-INB-APPT-<run>" from its appointment in inbound-receiving
    And 5 Good units of line 1 are received on the receipt of ASN "ASN-INB-APPT-<run>" in inbound-receiving
    And the receipt of ASN "ASN-INB-APPT-<run>" is closed in inbound-receiving
    Then the appointment of ASN "ASN-INB-APPT-<run>" in inbound-receiving is "Completed"
    And inbound-receiving published DockAppointmentCompleted for the appointment of ASN "ASN-INB-APPT-<run>"

  @e2e @inbound-receiving
  Scenario: A Damaged line is recorded on the receipt and is not booked as staged stock
    Given SKU "SKU-INB-DMG-<run>" is registered in product-master
    And ASN "ASN-INB-DMG-<run>" from supplier "ACME" expecting 10 units of SKU "SKU-INB-DMG-<run>" is eventually registered in inbound-receiving
    And a receipt is opened for ASN "ASN-INB-DMG-<run>" in inbound-receiving

    # Damaged first, Good second: see the header for why that order matters.
    When 4 Damaged units of line 1 are received on the receipt of ASN "ASN-INB-DMG-<run>" in inbound-receiving
    And 6 Good units of line 1 are received on the receipt of ASN "ASN-INB-DMG-<run>" in inbound-receiving
    Then the receipt of ASN "ASN-INB-DMG-<run>" in inbound-receiving records 6 good and 4 damaged units on line 1
    And inventory-storage eventually stages 6 units of SKU "SKU-INB-DMG-<run>" in exactly 1 StockReceived event

    When the receipt of ASN "ASN-INB-DMG-<run>" is closed in inbound-receiving
    Then the receipt of ASN "ASN-INB-DMG-<run>" in inbound-receiving is Closed with exactly these discrepancies:
      | line | sku               | kind    | expected | received | damaged |
      | 1    | SKU-INB-DMG-<run> | Damaged | 10       | 10       | 4       |
