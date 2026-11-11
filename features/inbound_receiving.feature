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
  - The gate check-in is yard-management's and reaches dock-scheduling as
    CHECKED_IN; inbound-receiving's copy holds only Booked and Cancelled (ADR
    0019), so it is read as Booked throughout. The receipt's close releases
    the unload in yard-management; only the door release (the check-out)
    completes the appointment in dock-scheduling.
  - The label scenarios (inbound-receiving ADR 0023 + 0024) drive the SAME
    raw ]C1 scan surface for every positive and negative: a rule-violating
    label answers 200 accepted=false with ONE finding whose closed reason
    code (conflicting_ais for AI 01+02 on otherwise-valid GTINs,
    bad_check_digit for a flipped mod-10 digit on an otherwise-valid SSCC),
    supplier_ref, CBV disposition and absent w13_code are each asserted on
    the REST answer AND on the CartonDiscrepancyRaised event, and the two
    refused scans must leave the line at exactly 0 booked units before a
    good SSCC scan of the same receipt books the carton.
  - The destination rule is proved both ways: carton 1's label names a
    DIFFERENT valid GLN (0614141000029, correct check digit, so only the
    destination differs) and is refused with the GLN and the SSCC on the
    finding; carton 2's label names the harness site GLN
    (RECEIVING_SITE_GLNS in scripts/services.sh) and is accepted, booking
    exactly its own 6 eaches (a double booking by the refused scan would read
    12), with sscc_source supplier and the label's ship_to_gln kept on the
    REST carton and on ReceiptCartonReceived.
  - The earliest best-before reads pick dates that disagree with every wrong
    implementation: 270401/270430 give the earlier-of-two case, then 270300
    (AI 15 day 00, "day not specified") is the pallet's earliest ONLY when
    compared as its month's LAST day (2027-03-31) — a raw-string comparison,
    a skip-the-00-date comparison or a day-0 parse would answer 2027-04-01 or
    a non-date instead. No AI 17 is scanned, so the asserted value can only
    come from the best-before rule.
  - The sscc-assignment scenario receives a FLAT ASN (no carton manifest, so
    an unlabelled case raises no UnlabelledCarton) and asserts the allocated
    SSCC structurally: 18 digits, extension 0 + the configured prefix
    0614141, a re-computed mod-10 check digit, ssccSource receiver on the
    answer and sscc_source receiver on ReceiptCartonReceived; the replay
    under the SAME Idempotency-Key must return the SAME SSCC with
    Idempotency-Replayed true (a second serial would differ and fail).
  - The advice scenario closes a two-line receipt whose line 1 carries 4
    Damaged units and line 2 is clean, and asserts the ReceivingAdviceIssued
    payload exactly: despatch_advice_ref = the ASN number, supplier_ref,
    within_sla under the 24h default, accepted/rejected/held per line, the
    W13 code 01 ONLY on the damaged line's Damaged finding (the clean line
    has NO finding), and the message ORDER ReceiptClosed-then-advice on the
    topic. While the receipt is still Open, no advice for its ASN may exist
    on the topic at all.

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
    # The copy holds only Booked and Cancelled (ADR 0019): the arrival stays in yard-management and dock-scheduling.
    And inbound-receiving's copy of the appointment of ASN "ASN-INB-APPT-<run>" is still "Booked"
    When the gate clerk docks the trailer of ASN "ASN-INB-APPT-<run>" at its appointment door in yard-management

    When a receipt is opened for ASN "ASN-INB-APPT-<run>" from its appointment in inbound-receiving
    And 5 Good units of line 1 are received on the receipt of ASN "ASN-INB-APPT-<run>" in inbound-receiving
    And the receipt of ASN "ASN-INB-APPT-<run>" is closed in inbound-receiving
    # The close releases the unload (yard-management's AutoCompleteUnload); the door release completes the appointment.
    Then yard-management eventually completes the unload of the trailer of ASN "ASN-INB-APPT-<run>" after the receipt closed
    And dock-scheduling still shows the appointment of ASN "ASN-INB-APPT-<run>" as CHECKED_IN
    When the visit of ASN "ASN-INB-APPT-<run>" checks out in yard-management and its door is released
    Then dock-scheduling eventually shows the appointment of ASN "ASN-INB-APPT-<run>" as COMPLETED

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

  # The five GS1-label / advice scenario groups below prove inbound-receiving
  # ADR 0024 (label validation at the scan, the AI 410 destination rule, the
  # earliest best-before of a mixed pallet, the receiver-assigned SSCC) and
  # ADR 0023 (the ReceivingAdviceIssued at close) end to end through the same
  # raw ]C1 scan surface a scanner uses. The harness starts the service with
  # RECEIVING_SITE_GLNS=0614141000012 and RECEIVER_GS1_COMPANY_PREFIX=0614141
  # (scripts/services.sh); see the header for what makes each one
  # discriminating.

  @e2e @inbound-receiving
  Scenario: A rule-violating GS1-128 label is refused, recorded as LabelNonCompliant, and does not poison the receipt
    Given SKU "SKU-INB-LBL-<run>" declares an EACH GTIN and a CASE pack level of 6 eaches in product-master
    And the CASE GTIN of SKU "SKU-INB-LBL-<run>" eventually resolves in product-master as an active CASE of 6 eaches
    And ASN "ASN-INB-LBL-<run>" announcing 2 homogeneous cartons of 6 eaches of SKU "SKU-INB-LBL-<run>" with valid SSCCs is registered in inbound-receiving
    And a receipt is opened for ASN "ASN-INB-LBL-<run>" in inbound-receiving

    # AI 01 and AI 02 never appear on one label: both GTINs are valid, so the
    # only possible reason is conflicting_ais.
    When a ]C1 label naming both AI 01 and AI 02 of SKU "SKU-INB-LBL-<run>" is scanned on the receipt of ASN "ASN-INB-LBL-<run>" in inbound-receiving
    Then that scan is refused as LabelNonCompliant with reason "conflicting_ais" and supplier "ACME", nothing booked
    And inbound-receiving published CartonDiscrepancyRaised kind LabelNonCompliant with reason "conflicting_ais" and supplier "ACME" for the receipt of ASN "ASN-INB-LBL-<run>"

    # The SSCC of carton 1 with its check digit flipped: every other digit is
    # valid, so the only possible reason is bad_check_digit.
    When a ]C1 label carrying an SSCC with a wrong check digit is scanned on the receipt of ASN "ASN-INB-LBL-<run>" in inbound-receiving
    Then that scan is refused as LabelNonCompliant with reason "bad_check_digit" and supplier "ACME", nothing booked
    And inbound-receiving published CartonDiscrepancyRaised kind LabelNonCompliant with reason "bad_check_digit" and supplier "ACME" for the receipt of ASN "ASN-INB-LBL-<run>"
    And the receipt of ASN "ASN-INB-LBL-<run>" in inbound-receiving keeps exactly the 2 LabelNonCompliant findings and no booked unit

    # The receipt is not poisoned: the same surface accepts carton 1's own
    # label afterwards and books exactly its 6 eaches.
    When carton 1 of ASN "ASN-INB-LBL-<run>" is scanned on its receipt with a valid ]C1 SSCC label in inbound-receiving
    Then the receipt of ASN "ASN-INB-LBL-<run>" in inbound-receiving records 6 good and 0 damaged units on line 1

  @e2e @inbound-receiving
  Scenario: A label whose AI 410 names another site is refused as WrongDestination; one naming this site is accepted
    Given SKU "SKU-INB-DST-<run>" declares an EACH GTIN and a CASE pack level of 6 eaches in product-master
    And the CASE GTIN of SKU "SKU-INB-DST-<run>" eventually resolves in product-master as an active CASE of 6 eaches
    And ASN "ASN-INB-DST-<run>" announcing 2 homogeneous cartons of 6 eaches of SKU "SKU-INB-DST-<run>" with valid SSCCs is registered in inbound-receiving
    And a receipt is opened for ASN "ASN-INB-DST-<run>" in inbound-receiving

    # 0614141000029 is a VALID GLN that is simply not this site: the refusal
    # can only be the destination rule, never a malformed label.
    When carton 1 of ASN "ASN-INB-DST-<run>" is scanned with a ]C1 label whose AI 410 names GLN "0614141000029" on its receipt in inbound-receiving
    Then that scan is refused as WrongDestination for GLN "0614141000029" and the SSCC of carton 1 of ASN "ASN-INB-DST-<run>", nothing booked
    And inbound-receiving published CartonDiscrepancyRaised kind WrongDestination for GLN "0614141000029" and the SSCC of carton 1 of ASN "ASN-INB-DST-<run>" with supplier "ACME"

    # The positive side of the same rule: the harness site GLN
    # (RECEIVING_SITE_GLNS, scripts/services.sh) is accepted, books exactly
    # its own carton, and the label's data is kept, not just tolerated.
    When carton 2 of ASN "ASN-INB-DST-<run>" is scanned with a ]C1 label whose AI 410 names the receiving site GLN on its receipt in inbound-receiving
    Then that scan is accepted, books 6 eaches on line 1 and the carton shows ssccSource "supplier" and shipToGln "0614141000012"
    And inbound-receiving published ReceiptCartonReceived for carton 2 of ASN "ASN-INB-DST-<run>" with sscc_source "supplier" and ship_to_gln "0614141000012"

  @e2e @inbound-receiving
  Scenario: The earliest best-before of a pallet is the earliest AI 15 of its accepted cartons, day 00 as the month's last day
    Given SKU "SKU-INB-BBE-<run>" declares an EACH GTIN and a CASE pack level of 6 eaches in product-master
    And the CASE GTIN of SKU "SKU-INB-BBE-<run>" eventually resolves in product-master as an active CASE of 6 eaches
    And ASN "ASN-INB-BBE-<run>" announcing 3 homogeneous cartons of 6 eaches of SKU "SKU-INB-BBE-<run>" on one pallet with valid SSCCs is registered in inbound-receiving
    And a receipt is opened for ASN "ASN-INB-BBE-<run>" in inbound-receiving

    # Two cartons, differing dates: the read exposes the EARLIER (2027-04-01).
    When carton 1 of ASN "ASN-INB-BBE-<run>" is scanned with a ]C1 label carrying AI 15 "270401" on its receipt in inbound-receiving
    And carton 2 of ASN "ASN-INB-BBE-<run>" is scanned with a ]C1 label carrying AI 15 "270430" on its receipt in inbound-receiving
    Then the pallet of ASN "ASN-INB-BBE-<run>" in inbound-receiving carries 2 accepted cartons with earliest best-before "2027-04-01" and no expiry
    And line 1 of the receipt of ASN "ASN-INB-BBE-<run>" in inbound-receiving has earliest best-before "2027-04-01"

    # Day 00 is "day not specified", compared as the LAST day of its month:
    # 270300 counts as 2027-03-31 and becomes the earliest ONLY under that
    # rule (a raw-string or skip-the-00 comparison would keep 2027-04-01).
    When carton 3 of ASN "ASN-INB-BBE-<run>" is scanned with a ]C1 label carrying AI 15 "270300" on its receipt in inbound-receiving
    Then the pallet of ASN "ASN-INB-BBE-<run>" in inbound-receiving carries 3 accepted cartons with earliest best-before "2027-03-31" and no expiry
    And line 1 of the receipt of ASN "ASN-INB-BBE-<run>" in inbound-receiving has earliest best-before "2027-03-31"

  @e2e @inbound-receiving
  Scenario: A case that arrived with no label is received under a receiver-assigned SSCC, once per Idempotency-Key
    Given SKU "SKU-INB-ASG-<run>" declares an EACH GTIN and a CASE pack level of 10 eaches in product-master
    And the CASE GTIN of SKU "SKU-INB-ASG-<run>" eventually resolves in product-master as an active CASE of 10 eaches
    # A FLAT ASN: no carton manifest, so nobody promised a label and the
    # assignment raises no UnlabelledCarton finding.
    And ASN "ASN-INB-ASG-<run>" from supplier "ACME" expecting 20 units of SKU "SKU-INB-ASG-<run>" is eventually registered in inbound-receiving
    And a receipt is opened for ASN "ASN-INB-ASG-<run>" in inbound-receiving

    When I assign an SSCC to one unlabelled case of SKU "SKU-INB-ASG-<run>" of 10 eaches on the receipt of ASN "ASN-INB-ASG-<run>" in inbound-receiving
    And inbound-receiving published ReceiptCartonReceived for the assigned SSCC of ASN "ASN-INB-ASG-<run>" with sscc_source "receiver"

    # The same logical request under the same key: the stored 201 answer with
    # the SAME SSCC and the Idempotency-Replayed header (a second serial from
    # the sequence would differ and fail the step's own assertions).
    When the same sscc-assignment is replayed with the same Idempotency-Key for ASN "ASN-INB-ASG-<run>"

  @e2e @inbound-receiving
  Scenario: Closing a receipt issues the receiving advice right after ReceiptClosed, coded and within the SLA
    Given SKU "SKU-INB-ADV1-<run>" is registered in product-master
    And SKU "SKU-INB-ADV2-<run>" is registered in product-master
    And ASN "ASN-INB-ADV-<run>" from supplier "ACME" with 10 units of SKU "SKU-INB-ADV1-<run>" on line 1 and 6 units of SKU "SKU-INB-ADV2-<run>" on line 2 is eventually registered in inbound-receiving
    And a receipt is opened for ASN "ASN-INB-ADV-<run>" in inbound-receiving

    # Line 1 gets 6 Good and 4 Damaged (expected 10, received 10: the only
    # line discrepancy is the Damaged one); line 2 is clean.
    When 6 Good units of line 1 are received on the receipt of ASN "ASN-INB-ADV-<run>" in inbound-receiving
    And 4 Damaged units of line 1 are received on the receipt of ASN "ASN-INB-ADV-<run>" in inbound-receiving
    And 6 Good units of line 2 are received on the receipt of ASN "ASN-INB-ADV-<run>" in inbound-receiving
    Then no ReceivingAdviceIssued for ASN "ASN-INB-ADV-<run>" is on warehouse.inbound-receiving.events while its receipt is open

    When the receipt of ASN "ASN-INB-ADV-<run>" is closed in inbound-receiving
    Then inbound-receiving published ReceivingAdviceIssued for ASN "ASN-INB-ADV-<run>" from supplier "ACME" right after ReceiptClosed with:
      | line | sku                | expected | received | accepted | rejected | held | w13 |
      | 1    | SKU-INB-ADV1-<run> | 10       | 10       | 6        | 4        | 0   | 01  |
      | 2    | SKU-INB-ADV2-<run> | 6        | 6        | 6        | 0        | 0   | -   |
