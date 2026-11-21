Feature: Inbound journey - the whole dock chain, with real services only
  A carrier books a dock door, arrives at the gate, docks, is unloaded against
  the ASN, the goods are stowed and the dock is released. Five owners take part
  and none of them is simulated:

    facility-layout   declares the Dock slot (the door) every other service learns
    dock-scheduling   books the appointment (inbound-receiving ADR 0019)
    yard-management   records the gate check-in, the dock move and the door work
    inbound-receiving keeps a LOCAL COPY of the appointment, opens the receipt on
                      it, closes it and raises the dock-to-stock fact (ADR 0021)
    inventory-storage seals the cartons into containers and stows them (RF stow)

  Everything between the owners travels over Kafka, so each cross-service
  assertion is a poll (the copy of the appointment, the CHECKED_IN and COMPLETED
  state in dock-scheduling, the auto-completed unload in yard-management); every
  fact that is read from a topic is read-only (the harness never writes to Kafka).

  Services started by scripts/03-up-services.sh for this feature: facility-layout,
  product-master, dock-scheduling, yard-management, inbound-receiving and
  inventory-storage. The receiving-performance report of inbound-receiving lives
  on its reports binary behind a projector; neither is in the native harness, so
  the dock-to-stock fact is proved on the event ReceiptStowCompleted itself.

  What makes each assertion discriminating:
  - The booking declares a seal; the gate check-in carries the same seal and the
    trailer reports it, and no SealExceptionRaised exists for the appointment.
  - The visit goes straight to the appointment door: the move is a CLAIMED
    VISITING_DRIVER TO_DOOR move to exactly that door, not a parking spot.
  - VisitCheckedIn carries the same arrival instant as the visit REST answer, and
    that instant is the arrived_at of ReceiptStowCompleted.
  - The appointment is completed by the door release, not by the receipt: after the
    close dock-scheduling still shows CHECKED_IN, and only the check-out (the
    trailer leaving the door) makes it COMPLETED. inbound-receiving's copy holds only
    Booked and Cancelled (ADR 0019), so it reads Booked throughout.
  - The unload is released by the receipt, not by the clerk: while door work is
    IN_PROGRESS the visit cannot check out (409 trailer-not-released); once the
    receipt is Closed yard-management completes the door work (EMPTY) on its own
    and the same check-out succeeds.
  - ReceiptStowCompleted is exact: stowed_at equals the LATEST ContainerStowed
    time of the three cartons, dock_to_stock_seconds equals stowed_at minus the
    gate arrival in whole seconds, carton_count is 3, and it is raised once.
  - Negatives: an ASN cannot sit on two active appointments (409), an inbound
    booking on an outbound-only door is refused (422), an OUTBOUND appointment is
    never copied into inbound-receiving, a cancelled appointment cannot open a
    receipt (409), and a reschedule to another door moves the door of the next
    receipt (inbound-receiving ADR 0020) and frees the old door's window.

  Every mutable id is run-scoped: the site, the doors, the SKU, the ASNs, the GTINs
  and SSCCs, the seal, the tractor and the trailer.

  Background:
    Given inbound-receiving, product-master and inventory-storage are healthy
    And dock-scheduling, yard-management and facility-layout are healthy

  @e2e @inbound-journey
  Scenario: A booked carrier is gated, docked, received, stowed and completed across the dock chain
    Given an Inbound dock door "INBJ<run>-DOCK-DR-A01-01-01-A" exists in facility-layout
    And the dock calendar of site "INBJ<run>" is declared in dock-scheduling with 5 minute slots and 60 minutes of arrival tolerance
    And SKU "SKU-JRN-<run>" declares an EACH GTIN and a CASE pack level of 12 eaches in product-master
    And the CASE GTIN of SKU "SKU-JRN-<run>" eventually resolves in product-master as an active CASE of 12 eaches
    And facility-layout has a seeded case bin and a seeded loose bin at site "INBJ<run>"
    And ASN "ASN-JRN-<run>" announcing 3 homogeneous cartons of 12 eaches of SKU "SKU-JRN-<run>" with valid SSCCs is registered in inbound-receiving

    # 1. dock-scheduling books; inbound-receiving's copy fills.
    When a dock appointment at door "INBJ<run>-DOCK-DR-A01-01-01-A" for carrier "ACME Freight" and ASN "ASN-JRN-<run>" is booked in dock-scheduling to start in 0 minutes for 60 minutes
    Then inbound-receiving's copy of the appointment of ASN "ASN-JRN-<run>" eventually mirrors dock-scheduling's door, window, ASN and version
    And inbound-receiving's copy of the appointment of ASN "ASN-JRN-<run>" is eventually "Booked"
    And booking a dock appointment at door "INBJ<run>-DOCK-DR-A01-01-01-A" for ASN "ASN-JRN-<run>" in dock-scheduling to start in 120 minutes for 30 minutes is refused with 409 "asn-already-scheduled"

    # 2. The carrier arrives: yard-management's gate check-in, its seal check and its dock move.
    When the carrier of ASN "ASN-JRN-<run>" checks in at the gate of yard-management
    Then yard-management published VisitCheckedIn for the appointment of ASN "ASN-JRN-<run>" with the arrival of the visit
    And yard-management raised no seal exception for the appointment of ASN "ASN-JRN-<run>"
    And dock-scheduling eventually shows the appointment of ASN "ASN-JRN-<run>" as CHECKED_IN
    # The copy holds only Booked and Cancelled (ADR 0019): arrival and completion are not copied.
    And inbound-receiving's copy of the appointment of ASN "ASN-JRN-<run>" is still "Booked"
    When the gate clerk docks the trailer of ASN "ASN-JRN-<run>" at its appointment door in yard-management
    And door work starts on the trailer of ASN "ASN-JRN-<run>" in yard-management

    # 3. inbound-receiving opens the receipt on the appointment (taking its door) and counts the cartons.
    And a receipt is opened for ASN "ASN-JRN-<run>" from its appointment in inbound-receiving
    And every carton SSCC of ASN "ASN-JRN-<run>" is scanned on its receipt in inbound-receiving and each scan is accepted
    Then yard-management refuses to check out the visit of ASN "ASN-JRN-<run>" while its trailer is still being unloaded

    # 4. The close releases the unload; the appointment stays CHECKED_IN until the door is released.
    When the receipt of ASN "ASN-JRN-<run>" is closed in inbound-receiving
    Then the receipt of ASN "ASN-JRN-<run>" in inbound-receiving is Closed with no discrepancy, 36 eaches booked and every carton Accepted
    And yard-management eventually completes the unload of the trailer of ASN "ASN-JRN-<run>" after the receipt closed
    And dock-scheduling still shows the appointment of ASN "ASN-JRN-<run>" as CHECKED_IN
    And inventory-storage eventually holds 3 SEALED containers of 12 eaches each for the cartons of ASN "ASN-JRN-<run>" and SKU "SKU-JRN-<run>"

    # 5. The RF stow closes the dock-to-stock measure (inbound-receiving ADR 0021).
    When I stow every container of ASN "ASN-JRN-<run>" into the case bin in inventory-storage
    Then inbound-receiving published ReceiptStowCompleted for ASN "ASN-JRN-<run>" with the gate arrival, the last of its 3 stows and the exact dock-to-stock seconds
    And the usable inventory of SKU "SKU-JRN-<run>" in inventory-storage is 36 eaches, 0 loose and 3 reservable cases

    # 6. The released trailer leaves: the door is free again and dock-scheduling completes the appointment.
    When the visit of ASN "ASN-JRN-<run>" checks out in yard-management and its door is released
    Then dock-scheduling eventually shows the appointment of ASN "ASN-JRN-<run>" as COMPLETED

  @e2e @inbound-journey
  Scenario: Only a live inbound appointment reaches a receipt, and a reschedule moves the door of the next one
    Given an Inbound dock door "INBR<run>-DOCK-DR-A01-01-01-A" exists in facility-layout
    And an Inbound dock door "INBR<run>-DOCK-DR-A01-01-01-B" exists in facility-layout
    And an Outbound dock door "INBR<run>-DOCK-DR-A01-01-01-C" exists in facility-layout
    And the dock calendar of site "INBR<run>" is declared in dock-scheduling with 5 minute slots and 60 minutes of arrival tolerance
    And SKU "SKU-JRN2-<run>" is registered in product-master
    And ASN "ASN-JRN-CNCL-<run>" from supplier "ACME" expecting 5 units of SKU "SKU-JRN2-<run>" is eventually registered in inbound-receiving
    And ASN "ASN-JRN-MOVE-<run>" from supplier "ACME" expecting 5 units of SKU "SKU-JRN2-<run>" is eventually registered in inbound-receiving
    And ASN "ASN-JRN-OUT-<run>" from supplier "ACME" expecting 5 units of SKU "SKU-JRN2-<run>" is eventually registered in inbound-receiving
    And ASN "ASN-JRN-FLOW-<run>" from supplier "ACME" expecting 5 units of SKU "SKU-JRN2-<run>" is eventually registered in inbound-receiving

    # An inbound booking needs an inbound-capable door.
    Then booking a dock appointment at door "INBR<run>-DOCK-DR-A01-01-01-C" for ASN "ASN-JRN-FLOW-<run>" in dock-scheduling to start in 0 minutes for 30 minutes is refused with 422 "door-flow-mismatch"

    # An OUTBOUND appointment is booked in dock-scheduling and is never copied.
    When an outbound dock appointment for shipment "SHP-JRN-<run>" at door "INBR<run>-DOCK-DR-A01-01-01-C" for carrier "ACME Freight" is booked in dock-scheduling to start in 0 minutes for 30 minutes
    And a dock appointment at door "INBR<run>-DOCK-DR-A01-01-01-A" for carrier "ACME Freight" and ASN "ASN-JRN-CNCL-<run>" is booked in dock-scheduling to start in 0 minutes for 30 minutes
    And a dock appointment at door "INBR<run>-DOCK-DR-A01-01-01-A" for carrier "ACME Freight" and ASN "ASN-JRN-MOVE-<run>" is booked in dock-scheduling to start in 60 minutes for 30 minutes
    Then inbound-receiving's copy of the appointment of ASN "ASN-JRN-CNCL-<run>" eventually mirrors dock-scheduling's door, window, ASN and version
    And inbound-receiving never holds a copy of the outbound appointment for shipment "SHP-JRN-<run>"
    And opening the receipt of ASN "ASN-JRN-OUT-<run>" in inbound-receiving from the appointment of "SHP-JRN-<run>" is refused with 422 "unknown-appointment"

    # A cancelled appointment cannot open a receipt.
    When the appointment of ASN "ASN-JRN-CNCL-<run>" is cancelled in dock-scheduling
    Then inbound-receiving's copy of the appointment of ASN "ASN-JRN-CNCL-<run>" is eventually "Cancelled"
    And opening the receipt of ASN "ASN-JRN-CNCL-<run>" in inbound-receiving from the appointment of "ASN-JRN-CNCL-<run>" is refused with 409 "appointment-cancelled"

    # A reschedule to another door: the copy follows (ADR 0020), the old window is free again, the receipt takes the new door.
    When the appointment of ASN "ASN-JRN-MOVE-<run>" is rescheduled in dock-scheduling to door "INBR<run>-DOCK-DR-A01-01-01-B" starting in 60 minutes for 30 minutes
    Then inbound-receiving's copy of the appointment of ASN "ASN-JRN-MOVE-<run>" eventually mirrors dock-scheduling's door, window, ASN and version
    When a dock appointment at door "INBR<run>-DOCK-DR-A01-01-01-A" for carrier "ACME Freight" and ASN "ASN-JRN-FLOW-<run>" is booked in dock-scheduling to start in 60 minutes for 30 minutes
    And a receipt is opened for ASN "ASN-JRN-MOVE-<run>" from its appointment in inbound-receiving
