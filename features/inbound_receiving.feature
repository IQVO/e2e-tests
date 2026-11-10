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
  facility-layout, dock-scheduling and yard-management (the appointment
  scenario), inbound-receiving and inventory-storage
  (INBOUND_RECEIPT_CONSUMER_GROUP set).
  Dock appointments are NOT inbound-receiving's to write any more (its ADR
  0019: POST /appointments and /check-ins answer 410 appointment-moved).
  dock-scheduling owns the booking and yard-management the gate check-in; the
  appointment scenario drives those owners and reads inbound-receiving's LOCAL
  COPY of the appointment, which fills over Kafka (eventually consistent, so
  every read of the copy is a poll). The dock door is real: a Dock slot
  declared in facility-layout, which all three services learn over Kafka.
  Nothing is injected; every fact comes from a real service.

  What makes each assertion discriminating:
  - The ASN expects 50 units and the dock receives 40, so the Short
    discrepancy carries all three numbers (expected 50, received 40, damaged
    0); the same table is checked on the REST read AND on the ReceiptClosed
    event, so the published contract and the read model agree.
  - inventory-storage has no read of STAGED stock (POST /stock-receipts is a
    bare acknowledgement and usable only counts stowed units). The handover is
    therefore proved by the StockReceived it raises on its analytics topic:
    the units, and the NUMBER of events for the SKU, must be exact. Usable
    inventory is read as 0 only AFTER that event, when the consumer has
    provably run, and moves to the received quantity only after the stow.
  - The Damaged line is received BEFORE the Good one on the same ASN (same
    Kafka key, so same partition order): once the Good line's StockReceived is
    seen, a Damaged line that had been booked would already be there as a
    second event, and the "exactly 1 event totalling 6 units" check fails.
  - The overlapping booking is refused by dock-scheduling with the
    door-double-booked problem, not just any 409, while a window that starts
    exactly when the first one ends (the window is half-open) is accepted.
  - The gate check-in is yard-management's: dock-scheduling and inbound-
    receiving's copy both reach CHECKED_IN / CheckedIn only through it, and the
    completion comes back through inbound-receiving's DockAppointmentCompleted.

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
  Scenario: A dock appointment booked in dock-scheduling is checked in at the yard gate and completes when the receipt closes
    Given dock-scheduling, yard-management and facility-layout are healthy
    And an Inbound dock door "INBD<run>-DOCK-DR-A01-01-01-A" exists in facility-layout
    And the dock calendar of site "INBD<run>" is declared in dock-scheduling with 5 minute slots and 60 minutes of arrival tolerance
    And SKU "SKU-INB-APPT-<run>" is registered in product-master
    And ASN "ASN-INB-APPT-<run>" from supplier "ACME" expecting 5 units of SKU "SKU-INB-APPT-<run>" is eventually registered in inbound-receiving
    And ASN "ASN-INB-APPT2-<run>" from supplier "ACME" expecting 5 units of SKU "SKU-INB-APPT-<run>" is eventually registered in inbound-receiving

    When a dock appointment at door "INBD<run>-DOCK-DR-A01-01-01-A" for carrier "ACME Freight" and ASN "ASN-INB-APPT-<run>" is booked in dock-scheduling to start in 0 minutes for 60 minutes
    Then dock-scheduling eventually shows the appointment of ASN "ASN-INB-APPT-<run>" as BOOKED
    And inbound-receiving's copy of the appointment of ASN "ASN-INB-APPT-<run>" is eventually "Booked"
    And booking a dock appointment at door "INBD<run>-DOCK-DR-A01-01-01-A" for ASN "ASN-INB-APPT2-<run>" in dock-scheduling to start in 30 minutes for 60 minutes is refused with 409 "door-double-booked"

    # The window is half-open: the next one may start exactly when this ends.
    When a dock appointment at door "INBD<run>-DOCK-DR-A01-01-01-A" for carrier "ACME Freight" and ASN "ASN-INB-APPT2-<run>" is booked in dock-scheduling to start in 60 minutes for 30 minutes
    And the appointment of ASN "ASN-INB-APPT2-<run>" is cancelled in dock-scheduling
    Then dock-scheduling eventually shows the appointment of ASN "ASN-INB-APPT2-<run>" as CANCELLED
    And inbound-receiving's copy of the appointment of ASN "ASN-INB-APPT2-<run>" is eventually "Cancelled"

    When the carrier of ASN "ASN-INB-APPT-<run>" checks in at the gate of yard-management
    Then dock-scheduling eventually shows the appointment of ASN "ASN-INB-APPT-<run>" as CHECKED_IN
    And inbound-receiving's copy of the appointment of ASN "ASN-INB-APPT-<run>" is eventually "CheckedIn"

    When a receipt is opened for ASN "ASN-INB-APPT-<run>" from its appointment in inbound-receiving
    And 5 Good units of line 1 are received on the receipt of ASN "ASN-INB-APPT-<run>" in inbound-receiving
    And the receipt of ASN "ASN-INB-APPT-<run>" is closed in inbound-receiving
    Then inbound-receiving's copy of the appointment of ASN "ASN-INB-APPT-<run>" is eventually "Completed"
    And inbound-receiving published DockAppointmentCompleted for the appointment of ASN "ASN-INB-APPT-<run>"
    And dock-scheduling eventually shows the appointment of ASN "ASN-INB-APPT-<run>" as COMPLETED

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
