Feature: The yard journey crosses facility-layout, inbound-receiving, dock-scheduling and yard-management
  A trailer's visit touches four bounded contexts that never call one another:
  facility-layout owns the door and the spots, inbound-receiving the ASN and the
  receipt, dock-scheduling the door's calendar (appointments) and yard-management
  everything physically on site. They agree ONLY through Kafka. A yard topic
  mismatch once went unnoticed because every consumer's own tests used fixtures
  that the producer never emitted; this feature is the one place the contracts
  between the four are exercised for real, process to process, nothing injected.

  Services started by scripts/03-up-services.sh for this feature: facility-layout,
  product-master (inbound-receiving refuses an ASN line for a SKU it has not been
  told about), inbound-receiving, dock-scheduling and yard-management with its
  separate outbox relay (the API does not relay; without the relay nothing
  reaches Kafka), plus outbound-shipping for scenario 3.

  Injected facts (scenario 3 only, published to the real topics as exact
  CloudEvents, the idiom of inter_warehouse_transfer.feature and
  returns_exceptions.feature): fulfillment-execution's PackageManifested (no
  pick/pack runs here) and process-path-management's CPTScheduleChanged. The
  latter is injected, not produced by process-path-management's real
  PUT /sites/{siteId}/cpt-schedule, because outbound-shipping dead-letters the
  real event (it requires a `version` the producer's contract never carries): see
  the step's comment and the PR that added this feature.

  The contracts under test, and what makes each assertion discriminating:
  - facility-layout -> yard-management and dock-scheduling. Door and spots are
    registered in facility-layout only. The yard board lists exactly them, and
    dock-scheduling books on the door: a booking on a door it had not learned is
    refused 422 unknown-door.
  - inbound-receiving -> dock-scheduling. The ASN is registered in
    inbound-receiving only; booking a covering appointment is refused 422
    unknown-asn until ASNRegistered arrives.
  - dock-scheduling -> yard-management and inbound-receiving. The yard admits the
    carrier against an appointment it knows only from DockAppointmentBooked (a
    wrong or unknown id is a 422 at the gate), and inbound-receiving opens the
    receipt against its copy of the same appointment: the receipt carries the
    facility door of the booking.
  - yard-management -> dock-scheduling. The appointment must provably START
    BOOKED, then turns CHECKED_IN (VisitCheckedIn) and COMPLETED when the trailer
    leaves the door (TrailerUndocked): neither can happen by any call dock-
    scheduling's REST API offers. A turn-away that names no appointment cancels
    none; a turn-away that names one cancels exactly it (TURNED_AWAY).
  - inbound-receiving -> yard-management. The door work is NEVER completed by a
    REST call: it is DONE and the trailer EMPTY only because ReceiptClosed
    reached the yard (AutoCompleteUnload), and only after the unload was
    IN_PROGRESS.
  - yard-management -> outbound-shipping (scenario 3). A load is SHIP_CONFIRMED
    only when the trailer physically leaves: the load is asserted CLOSED (and its
    package LOADED) after door work is done and the seal is recorded, i.e. with
    everything but the departure in place, and turns SHIP_CONFIRMED only after the
    check-out. The ShipmentConfirmed on outbound-shipping's integration topic must
    name the trailer the yard minted, the seal recorded on it and the order.
    Upstream, outbound-shipping takes the door of its load from dock-scheduling's
    OUTBOUND appointment (same cutoff id) and starts loading on yard-management's
    TrailerDocked: there is no REST route for either.
  - yard-management's INTEGRATION topic. The feature reads
    warehouse.yard-management.events from the first offset on a fresh consumer
    group and requires VisitCheckedIn, TrailerDocked, DoorWorkCompleted and
    TrailerDeparted for THIS run's trailer (ids the yard minted a moment ago, so
    no event of an earlier run can satisfy it). DoorWorkCompleted and
    TrailerDeparted once reached only the analytics topic: dock-scheduling and
    outbound-shipping consume the integration topic, so this is the guard. An
    analytics-only type (DoorWorkStarted) asserted the same way fails.

  Every site is run-scoped (<run>): the yard board and the yard policy are per
  site, so a shared site would carry the trailers and the policy of an earlier
  run (the sanctioned exception to "do not scope shared reference data"; see
  slotting_optimization.feature). Appointments are booked on the 5-minute grid a
  few minutes from now with a 30-minute arrival tolerance, so a carrier arriving
  right away is "on time" and goes straight to the door. Nothing is cleaned up.

  Background:
    Given facility-layout, product-master, inbound-receiving, dock-scheduling and yard-management are healthy

  @e2e @yard-journey
  Scenario: An inbound visit with an appointment goes from the gate to the door and out, and every context agrees
    # --- facility-layout is the only place the layout is registered ---
    Given site "YJA<run>" with one Inbound dock door, parking bays "01,02" and holding bay "09" is registered in facility-layout
    And yard-management eventually knows the layout of site "YJA<run>" from facility-layout
    And dock-scheduling's calendar of site "YJA<run>" allows arrival 30 minutes early on a 5-minute slot grid

    # --- inbound-receiving is the only place the ASN is registered ---
    And SKU "SKU-YJ-<run>" is registered in product-master
    And ASN "ASN-YJ-A-<run>" from supplier "ACME" expecting 5 units of SKU "SKU-YJ-<run>" is eventually registered in inbound-receiving

    # --- the appointment lives in dock-scheduling and starts BOOKED ---
    When an INBOUND appointment "APPT-YJ-A-<run>" on the door of site "YJA<run>" for ASN "ASN-YJ-A-<run>" with expected seal "SL-YJ-A-<run>" is eventually booked in dock-scheduling to start in 2 minutes for 60 minutes
    Then dock-scheduling's appointment "APPT-YJ-A-<run>" is "BOOKED"

    # --- the gate: the yard knows the appointment only from the event ---
    When the carrier checks in at the gate of site "YJA<run>" with plate "PLATE-YJ-A-<run>" for purpose LIVE_UNLOAD and trailer "TRL-YJ-A-<run>" (DRY_VAN, LOADED) on appointment "APPT-YJ-A-<run>" sealed "SL-YJ-A-<run>"
    Then the gate admits the visit with a TO_DOOR move of trailer "TRL-YJ-A-<run>" by the VISITING_DRIVER to the door of site "YJA<run>"
    And dock-scheduling's appointment "APPT-YJ-A-<run>" is eventually "CHECKED_IN"

    When the move of trailer "TRL-YJ-A-<run>" is completed in yard-management
    Then trailer "TRL-YJ-A-<run>" is DOCKED at the door of site "YJA<run>" with door work WAITING and load state LOADED
    And the yard board of site "YJA<run>" shows trailer "TRL-YJ-A-<run>" at the door

    # --- the dock works the trailer; the receipt is opened on the appointment ---
    When the dock operator starts door work on trailer "TRL-YJ-A-<run>" in yard-management
    And inbound-receiving opens a receipt for ASN "ASN-YJ-A-<run>" on appointment "APPT-YJ-A-<run>" at the booked door
    And 5 Good units of line 1 are received on the receipt of ASN "ASN-YJ-A-<run>" in inbound-receiving
    And the receipt of ASN "ASN-YJ-A-<run>" is closed in inbound-receiving

    # --- no door-work completion call anywhere: only ReceiptClosed can have done it ---
    Then trailer "TRL-YJ-A-<run>" eventually has door work DONE and load state EMPTY

    # --- the visit leaves; dock-scheduling learns the door was released ---
    When the visit of plate "PLATE-YJ-A-<run>" checks out taking trailer "TRL-YJ-A-<run>" in yard-management
    Then trailer "TRL-YJ-A-<run>" is DEPARTED
    And dock-scheduling's appointment "APPT-YJ-A-<run>" is eventually "COMPLETED"
    And the yard board of site "YJA<run>" is empty

    # --- the regression guard: these types are on the INTEGRATION topic ---
    And warehouse.yard-management.events, read from the first offset on a fresh consumer group, carries VisitCheckedIn of visit "PLATE-YJ-A-<run>"
    And warehouse.yard-management.events, read from the first offset on a fresh consumer group, carries TrailerDocked, DoorWorkCompleted and TrailerDeparted of trailer "TRL-YJ-A-<run>"

  @e2e @yard-journey
  Scenario: A walk-in is held, an appointment-only site turns it away, and a seal mismatch is rejected
    Given site "YJB<run>" with one Inbound dock door, parking bays "01,02" and holding bay "09" is registered in facility-layout
    And yard-management eventually knows the layout of site "YJB<run>" from facility-layout
    And dock-scheduling's calendar of site "YJB<run>" allows arrival 30 minutes early on a 5-minute slot grid
    And SKU "SKU-YJ-<run>" is registered in product-master
    And ASN "ASN-YJ-B1-<run>" from supplier "ACME" expecting 5 units of SKU "SKU-YJ-<run>" is eventually registered in inbound-receiving
    And ASN "ASN-YJ-B2-<run>" from supplier "ACME" expecting 5 units of SKU "SKU-YJ-<run>" is eventually registered in inbound-receiving
    # Two bookings on the one door: the second window starts when the first ends.
    And an INBOUND appointment "APPT-YJ-B1-<run>" on the door of site "YJB<run>" for ASN "ASN-YJ-B1-<run>" with expected seal "SL-YJ-B1-<run>" is eventually booked in dock-scheduling to start in 2 minutes for 30 minutes
    And an INBOUND appointment "APPT-YJ-B2-<run>" on the door of site "YJB<run>" for ASN "ASN-YJ-B2-<run>" with expected seal "SL-YJ-B2-<run>" is eventually booked in dock-scheduling to start in 32 minutes for 30 minutes

    # --- a walk-in goes to the policy's holding spot (bay 09, NOT the first free spot, bay 01) ---
    Given the yard policy of site "YJB<run>" does not require an appointment, flags a seal mismatch and holds walk-ins at bay "09"
    When the carrier checks in at the gate of site "YJB<run>" with plate "PLATE-YJ-W1-<run>" for purpose DROP and trailer "TRL-YJ-W1-<run>" (DRY_VAN, EMPTY)
    Then the gate admits the visit with a TO_HOLDING move of trailer "TRL-YJ-W1-<run>" by the VISITING_DRIVER to the holding spot of site "YJB<run>"
    When the move of trailer "TRL-YJ-W1-<run>" is completed in yard-management
    Then trailer "TRL-YJ-W1-<run>" is PARKED at the holding spot of site "YJB<run>"

    # --- an appointment-only site refuses the walk-in and persists nothing ---
    Given the yard policy of site "YJB<run>" requires an appointment, flags a seal mismatch and holds walk-ins at bay "09"
    When the carrier checks in at the gate of site "YJB<run>" with plate "PLATE-YJ-W2-<run>" for purpose DROP and trailer "TRL-YJ-W2-<run>" (DRY_VAN, EMPTY)
    Then the gate refuses the check-in with 409 "appointment-required"
    And the gate refused check-in of trailer "TRL-YJ-W2-<run>" at site "YJB<run>" left no trailer behind
    And yard-management holds 1 trailer at site "YJB<run>"
    When the gate records the turn-away of plate "PLATE-YJ-W2-<run>" at site "YJB<run>" because APPOINTMENT_REQUIRED

    # --- a seal mismatch under REJECT is rejected in the same call, naming the appointment ---
    Given the yard policy of site "YJB<run>" does not require an appointment, rejects a seal mismatch and holds walk-ins at bay "09"
    When the carrier checks in at the gate of site "YJB<run>" with plate "PLATE-YJ-S1-<run>" for purpose LIVE_UNLOAD and trailer "TRL-YJ-S1-<run>" (DRY_VAN, LOADED) on appointment "APPT-YJ-B2-<run>" sealed "SL-WRONG-<run>"
    Then the check-in of plate "PLATE-YJ-S1-<run>" is rejected as SEAL_MISMATCH with nothing admitted
    And yard-management holds 1 trailer at site "YJB<run>"
    And warehouse.yard-management.events, read from the first offset on a fresh consumer group, carries SealExceptionRaised and VisitRejected of visit "PLATE-YJ-S1-<run>"
    And warehouse.yard-management.events, read from the first offset on a fresh consumer group, carries VisitRejected of visit "PLATE-YJ-W2-<run>"

    # --- dock-scheduling cancels exactly the appointment a turn-away names. The two
    # VisitRejected events are published in order on one partition (cmd/ensure-topics
    # creates every topic with one), so once the named appointment is CANCELLED the
    # walk-in's turn-away (published first, naming none) has been consumed too: the
    # unnamed appointment B1 is still BOOKED, which is what "cancels nothing"
    # means. ---
    Then dock-scheduling's appointment "APPT-YJ-B2-<run>" is eventually CANCELLED as TURNED_AWAY
    And dock-scheduling's appointment "APPT-YJ-B1-<run>" is "BOOKED"

  @e2e @yard-journey
  Scenario: Ship-confirm follows the physical departure of the trailer from the door
    Given outbound-shipping is healthy
    And site "YJC<run>" with one Outbound dock door, parking bays "01,02" and holding bay "09" and a staging lane is registered in facility-layout
    And yard-management eventually knows the layout of site "YJC<run>" from facility-layout
    And dock-scheduling's calendar of site "YJC<run>" allows arrival 30 minutes early on a 5-minute slot grid

    # --- upstream facts: the cutoff, then a manifested package of one order ---
    And the CPT schedule of site "YJC<run>" with cutoff "CPT-YJ-<run>" at 16:00 UTC is published to warehouse.process-path-management.events
    And PackageManifested of package "PKG-YJ-<run>" for order "ORD-YJ-<run>" at site "YJC<run>" is published until outbound-shipping has planned a load holding it
    Then outbound-shipping's load of site "YJC<run>" is "PLANNED" and package "PKG-YJ-<run>" is "EXPECTED"

    # --- the OUTBOUND appointment on the door gives the load its door ---
    When an OUTBOUND appointment "APPT-YJ-C-<run>" on the door of site "YJC<run>" for cutoff "CPT-YJ-<run>" is eventually booked in dock-scheduling to start in 2 minutes for 60 minutes
    Then outbound-shipping eventually takes the door of appointment "APPT-YJ-C-<run>" for the load of site "YJC<run>"
    And dock-scheduling's appointment "APPT-YJ-C-<run>" is "BOOKED"
    When package "PKG-YJ-<run>" stages at the staging lane of site "YJC<run>" in outbound-shipping

    # --- the trailer comes to the door; TrailerDocked starts the loading ---
    When the carrier checks in at the gate of site "YJC<run>" with plate "PLATE-YJ-C-<run>" for purpose LIVE_LOAD and trailer "TRL-YJ-C-<run>" (DRY_VAN, EMPTY) on appointment "APPT-YJ-C-<run>"
    Then the gate admits the visit with a TO_DOOR move of trailer "TRL-YJ-C-<run>" by the VISITING_DRIVER to the door of site "YJC<run>"
    When the move of trailer "TRL-YJ-C-<run>" is completed in yard-management
    Then outbound-shipping eventually has the load of site "YJC<run>" LOADING on trailer "TRL-YJ-C-<run>"

    # --- load, close, finish the door work and seal: everything but the departure ---
    When package "PKG-YJ-<run>" loads through the door of site "YJC<run>" in outbound-shipping
    And the load of site "YJC<run>" is closed in outbound-shipping
    And the dock operator starts door work on trailer "TRL-YJ-C-<run>" in yard-management
    And the dock operator completes door work on trailer "TRL-YJ-C-<run>" as LOADED in yard-management
    And the dock operator records an INTACT seal "SEAL-YJ-C-<run>" on trailer "TRL-YJ-C-<run>" in yard-management
    Then outbound-shipping's load of site "YJC<run>" is "CLOSED" and package "PKG-YJ-<run>" is "LOADED"

    # --- only the physical departure confirms the shipment ---
    When the visit of plate "PLATE-YJ-C-<run>" checks out taking trailer "TRL-YJ-C-<run>" in yard-management
    Then outbound-shipping eventually has the load of site "YJC<run>" SHIP_CONFIRMED on trailer "TRL-YJ-C-<run>" with package "PKG-YJ-<run>" SHIPPED
    And outbound-shipping published ShipmentConfirmed for the load of site "YJC<run>" on trailer "TRL-YJ-C-<run>" sealed "SEAL-YJ-C-<run>" for order "ORD-YJ-<run>"
    And dock-scheduling's appointment "APPT-YJ-C-<run>" is eventually "COMPLETED"
    And warehouse.yard-management.events, read from the first offset on a fresh consumer group, carries TrailerDocked, DoorWorkCompleted and TrailerDeparted of trailer "TRL-YJ-C-<run>"
