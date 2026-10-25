Feature: facility-layout's warehouse map propagates to inventory-storage over Kafka
  facility-layout is the Open Host Service owning the physical warehouse
  map, and its domain events are its Published Language. inventory-storage
  is a Conformist downstream: instead of calling facility-layout over HTTP
  on every stow, it maintains a LOCAL, in-memory read model of location
  classifications fed by warehouse.facility.events (inventory-storage
  ADR-0013).

  This scenario is the only proof across the estate that the two contexts
  actually agree over the wire: that a zone's behavioural attributes
  (hazmat, temperature class) reach inventory-storage's placement rules
  through Kafka alone, that a slot registered AFTER inventory-storage
  started is picked up live without a restart, and that decommissioning a
  slot propagates too. Unit and integration tests in either repo cannot
  cover this — one owns the publisher, the other owns the consumer, and
  neither runs the other.

  The stow assertions are the real assertions. Reading a log line saying
  "cache ready" would prove only that a consumer started; refusing a hazmat
  SKU from a non-hazmat zone proves the zone's ATTRIBUTES actually crossed
  the boundary and reached a domain invariant.

  Background:
    Given all warehouse-systems services are healthy

  @e2e @facility-propagation
  Scenario: Layout changes reach inventory-storage's placement rules through Kafka alone
    # --- facility-layout: two zones that differ ONLY in their hazmat flag.
    # Two zones are deliberate: a single zone cannot distinguish "the cache
    # learned this zone's real attributes" from "the cache answers the same
    # way for everything". ---
    # The site and zones are run-scoped (<run>): inventory-storage's
    # propagation consumer is EVENT-SOURCED (its cache is built solely from
    # warehouse.facility.events history), and the shared broker's topics
    # were recreated empty during the 2026-10-08 kind consolidation while
    # facility-layout's Postgres kept the old WH2 rows. A fixed site id
    # means ensureZone gets 409 and never publishes ZoneRegistered, so
    # inventory-storage can never learn the zones. A run-scoped site emits
    # fresh events every run — these fixtures are the documented exception
    # to "do NOT scope shared reference data". ---
    Given site "WH2<run>" named "Propagation Test FC" exists in facility-layout
    And location type "PropRack" with capacity 1000 kg and 2.0 m3 exists in facility-layout
    And zone "STOR"/"AMB" in site "WH2<run>" with temperature class "Ambient" exists in facility-layout
    And hazmat zone "STOR"/"HAZ" in site "WH2<run>" with temperature class "Ambient" exists in facility-layout
    And aisle "A01" in zone "WH2<run>-STOR-AMB" with sequence hint 1 and direction "TwoWay" exists in facility-layout
    And aisle "A01" in zone "WH2<run>-STOR-HAZ" with sequence hint 2 and direction "TwoWay" exists in facility-layout

    # --- these slots are registered AFTER inventory-storage booted, so they
    # can only be known via a live-consumed event, never via a boot-time read ---
    And location slot "<disposable>" of type "PropRack" exists in facility-layout
    And location slot "WH2<run>-STOR-HAZ-A01-01-01-A" of type "PropRack" exists in facility-layout

    # --- inventory-storage: a hazmat SKU and matching bins. The SKU is
    # classified in product-master (the owner since product-master ADR 0003
    # stage C; inventory-storage's own PUT answers 410). inventory-storage
    # learns it asynchronously from warehouse.product-master.events. There
    # is no REST read of that local copy anymore (K21 stage E), so no
    # barrier step here: discrimination is preserved by the AMBIENT-zone
    # stow refusal asserted below -- if inventory-storage had NOT learned
    # the tags, fail-open placement would let that stow succeed and the
    # refusal assertion would fail.
    Given a Bin "<disposable>" with capacity 100 exists in inventory-storage
    And a Bin "WH2<run>-STOR-HAZ-A01-01-01-A" with capacity 100 exists in inventory-storage
    And SKU "SKU-PROP-HAZ" is registered and classified with handling tags "Hazmat" in product-master
    When I receive 10 units of SKU "SKU-PROP-HAZ" in inventory-storage

    # --- the hazmat zone's attributes crossed the boundary: a hazmat SKU is
    # ACCEPTED into the hazmat-rated zone, once the event has been consumed ---
    Then stowing 1 units of SKU "SKU-PROP-HAZ" into bin "WH2<run>-STOR-HAZ-A01-01-01-A" in inventory-storage eventually succeeds

    # --- and the ambient zone's DIFFERING attributes crossed too: the same
    # SKU is REFUSED there by the placement rule, not by a generic error ---
    And stowing 1 units of SKU "SKU-PROP-HAZ" into bin "<disposable>" in inventory-storage is eventually rejected

    # --- decommissioning propagates as well. The slot leaves the cache, so
    # it becomes unknown and the placement check fails OPEN (Known=false) --
    # deliberately the same answer the retired HTTP client gave on a 404. A
    # previously-REJECTED stow now succeeds, which is the discriminating
    # outcome: a stow that already succeeded would prove nothing here. ---
    When I decommission location slot "<disposable>" in facility-layout
    Then stowing 1 units of SKU "SKU-PROP-HAZ" into bin "<disposable>" in inventory-storage eventually succeeds
