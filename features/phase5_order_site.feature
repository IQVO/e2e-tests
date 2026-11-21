Feature: An order's fulfillment site decides where its demand is projected
  order-management ADR 0044: an order may carry an OPTIONAL fulfillmentSiteId,
  assigned by the caller and never inferred. The SiteSkuDemandChanged event is
  projected to that site (assignment_version order-site-v1); an order that names
  none goes to the one static site (static-site-v1) exactly as before. This is
  what lets a transfer between two sites find demand at both ends.

  Background:
    Given all warehouse-systems services are healthy

  @e2e @phase5 @order-site
  Scenario: Demand follows the site the caller names, and falls back to the static site
    # Demand is emitted only for lines that ALLOCATE (order-management ADR 0035,
    # "lines that never allocated emit nothing"), so the SKU needs usable stock.
    Given inventory-storage holds 10 loose units of SKU "SKU-E2E-OS-<run>" in a seeded forward bin at site "ABE8"

    # --- an order naming a site: demand goes THERE ---
    When I place an order for 2 units of SKU "SKU-E2E-OS-<run>" to be fulfilled from site "OS<run>" in order-management
    Then the response status is 201
    And the order in order-management reports the fulfillment site "OS<run>"
    And order-management projects 2 demanded units of SKU "SKU-E2E-OS-<run>" to site "OS<run>" with assignment version "order-site-v1"

    # --- an order naming none: the static site, byte-for-byte as before ---
    When I place an order for 1 unit of SKU "SKU-E2E-OS-<run>" naming no site in order-management
    Then the response status is 201
    And the order in order-management reports no fulfillment site
    And order-management projects 1 demanded units of SKU "SKU-E2E-OS-<run>" to a site other than "OS<run>" with assignment version "static-site-v1"

    # --- a malformed site is refused with the documented problem ---
    When I place an order for 1 unit of SKU "SKU-E2E-OS-<run>" naming the malformed site "wh-1" in order-management
    Then the response status is 400
    And the response is a problem of type "invalid-fulfillment-site-id"
