Feature: Mastercase - sealed SSCC cartons keep their eaches from receipt to stock
  A supplier ships SEALED cases (cartons with an SSCC), each a CASE pack of 12
  eaches. The ledger unit stays the EACH everywhere (mastercase contract G1):
  product-master declares the pack levels (EACH and CASE GTINs), inbound-receiving
  accepts the whole carton manifest on one SSCC scan, inventory-storage keeps each
  carton as a SEALED container (LPN = SSCC) and can break one open into loose
  eaches without gaining or losing a single unit. A scan that cannot be accepted
  (a repeated SSCC) is not an error response: it books nothing and raises a
  carton discrepancy that exception-management turns into a case.

  Black-box, REST only, through the Kong gateway of the kind cluster. Run it
  against the live cluster (base URLs are the only configuration):

    P=http://localhost:8000/api
    PRODUCT_MASTER_BASE_URL=$P/product-master INBOUND_BASE_URL=$P/inbound-receiving \
    INVENTORY_BASE_URL=$P/inventory-storage FACILITY_BASE_URL=$P/facility-layout \
    EXCEPTIONS_BASE_URL=$P/exception-management GODOG_TAGS=@mastercase \
      go test -v -run TestMain -timeout 10m .

  Every GTIN-14 and SSCC-18 is a valid GS1 number computed per run (mod-10 check
  digit) and unique per run, so the suite re-runs cleanly against a dirty cluster.

  - Pack levels reach inbound-receiving and inventory-storage over Kafka (their
    own local PackagingView, no REST on a hot path). The REST-visible gate is
    GET /gtins/{gtin} on product-master, then a short settle before the first
    scan; an unpropagated pack would show up as UnknownGtin / CaseQtyMismatch on
    that scan and fail the "no discrepancy" assertion, never be hidden.
  - @blocked scenarios are excluded from the default run (TestMain's default tags
    are "~@soak && ~@blocked"); run them with GODOG_TAGS=@blocked or @mastercase.
    The reason is stated on each one.

  Background:
    Given inbound-receiving, product-master and inventory-storage are healthy
    And exception-management is healthy

  @e2e @mastercase @inbound-receiving
  Scenario: Mastercase receipt: 3 SSCC cartons are received as 3 SEALED containers of 12 eaches
    Given SKU "SKU-MC-RCV-<run>" declares an EACH GTIN and a CASE pack level of 12 eaches in product-master
    And the CASE GTIN of SKU "SKU-MC-RCV-<run>" eventually resolves in product-master as an active CASE of 12 eaches
    And ASN "ASN-MC-RCV-<run>" announcing 3 homogeneous cartons of 12 eaches of SKU "SKU-MC-RCV-<run>" with valid SSCCs is registered in inbound-receiving
    And a receipt is opened for ASN "ASN-MC-RCV-<run>" in inbound-receiving
    When every carton SSCC of ASN "ASN-MC-RCV-<run>" is scanned on its receipt in inbound-receiving and each scan is accepted
    And the receipt of ASN "ASN-MC-RCV-<run>" is closed in inbound-receiving
    Then the receipt of ASN "ASN-MC-RCV-<run>" in inbound-receiving is Closed with no discrepancy, 36 eaches booked and every carton Accepted
    And inventory-storage eventually holds 3 SEALED containers of 12 eaches each for the cartons of ASN "ASN-MC-RCV-<run>" and SKU "SKU-MC-RCV-<run>"
    # Received but not yet stowed: staged stock is not usable.
    And the usable inventory of SKU "SKU-MC-RCV-<run>" in inventory-storage is 0 eaches, 0 loose and 0 reservable cases

  # Needs facility-layout's bin handling unit (ADR 0040, PUT /locations/{code}/handling-unit)
  # and inventory-storage's LocationSlotHandlingUnitDeclared consumer (ADR 0047): the
  # seeded-bin step declares CASE / EACH on the two bins it picks, so no manual setup.
  @e2e @mastercase @inbound-receiving @inventory-storage
  Scenario: Mastercase: 3 SSCC cartons = 36 eaches, conserved from receipt to pick
    Given SKU "SKU-MC-ALL-<run>" declares an EACH GTIN and a CASE pack level of 12 eaches in product-master
    And the CASE GTIN of SKU "SKU-MC-ALL-<run>" eventually resolves in product-master as an active CASE of 12 eaches
    And facility-layout has a seeded case bin and a seeded loose bin at site "ABE8"
    And ASN "ASN-MC-ALL-<run>" announcing 3 homogeneous cartons of 12 eaches of SKU "SKU-MC-ALL-<run>" with valid SSCCs is registered in inbound-receiving
    And a receipt is opened for ASN "ASN-MC-ALL-<run>" in inbound-receiving
    When every carton SSCC of ASN "ASN-MC-ALL-<run>" is scanned on its receipt in inbound-receiving and each scan is accepted
    And the receipt of ASN "ASN-MC-ALL-<run>" is closed in inbound-receiving
    Then the receipt of ASN "ASN-MC-ALL-<run>" in inbound-receiving is Closed with no discrepancy, 36 eaches booked and every carton Accepted
    And inventory-storage eventually holds 3 SEALED containers of 12 eaches each for the cartons of ASN "ASN-MC-ALL-<run>" and SKU "SKU-MC-ALL-<run>"

    # Stow the 3 sealed cases: 36 eaches on hand as 3 reservable cases, none loose.
    When I stow every container of ASN "ASN-MC-ALL-<run>" into the case bin in inventory-storage
    Then the usable inventory of SKU "SKU-MC-ALL-<run>" in inventory-storage is 36 eaches, 0 loose and 3 reservable cases

    # Break ONE case: the container empties, its 12 eaches become loose, nothing is lost.
    When I break container 1 of ASN "ASN-MC-ALL-<run>" into the loose bin in inventory-storage
    Then container 1 of ASN "ASN-MC-ALL-<run>" is EMPTY holding 12 eaches
    And container 2 of ASN "ASN-MC-ALL-<run>" is SEALED holding 12 eaches
    And container 3 of ASN "ASN-MC-ALL-<run>" is SEALED holding 12 eaches
    And the usable inventory of SKU "SKU-MC-ALL-<run>" in inventory-storage is 36 eaches, 12 loose and 2 reservable cases

    # Reserving eaches draws only on loose stock; on hand (usable + reserved) stays 36.
    When I reserve 5 eaches of SKU "SKU-MC-ALL-<run>" for demand "MC-DEMAND-<run>" in inventory-storage
    Then the usable inventory of SKU "SKU-MC-ALL-<run>" in inventory-storage is 31 eaches, 7 loose and 2 reservable cases
    And on hand for SKU "SKU-MC-ALL-<run>" with demand "MC-DEMAND-<run>" reserved is still exactly 36 eaches
    And container 2 of ASN "ASN-MC-ALL-<run>" is SEALED holding 12 eaches

    # TODO(mastercase fulfillment leg, not implemented, NOT faked): scanning
    # eaches (POST /tasks/{id}/scans with an each GTIN) and cases (case GTIN)
    # against a fulfillment-execution PICK task for this SKU. A PICK task is only
    # produced by order-management -> wes-work-planning -> fulfillment-execution
    # from a placed, allocated order; the existing steps that do that
    # (order_management_choreographed_release.feature) allocate through the
    # fleet's order flow, not against this scenario's SKU and reservation, and
    # the case-pick path needs the stowed containers above (blocked). Add the
    # scan-confirm scenario once a PICK task can be produced for a freshly
    # stowed SKU through those steps.

  @e2e @mastercase @inbound-receiving @exception-management
  Scenario: Scanning the same SSCC twice raises DuplicateSscc and exception-management opens a problem-solve case
    Given SKU "SKU-MC-DUP-<run>" declares an EACH GTIN and a CASE pack level of 12 eaches in product-master
    And the CASE GTIN of SKU "SKU-MC-DUP-<run>" eventually resolves in product-master as an active CASE of 12 eaches
    And ASN "ASN-MC-DUP-<run>" announcing 1 homogeneous cartons of 12 eaches of SKU "SKU-MC-DUP-<run>" with valid SSCCs is registered in inbound-receiving
    And a receipt is opened for ASN "ASN-MC-DUP-<run>" in inbound-receiving
    And every carton SSCC of ASN "ASN-MC-DUP-<run>" is scanned on its receipt in inbound-receiving and each scan is accepted
    When the SSCC of carton 1 of ASN "ASN-MC-DUP-<run>" is scanned again on its receipt in inbound-receiving
    Then that scan is refused with a single DuplicateSscc finding for carton 1 of ASN "ASN-MC-DUP-<run>"
    And the receipt of ASN "ASN-MC-DUP-<run>" in inbound-receiving still books exactly 12 good eaches
    And exception-management eventually has an OPEN case in queue "problem-solve" of kind DuplicateSscc keyed to the receipt and SSCC of carton 1 of ASN "ASN-MC-DUP-<run>"
