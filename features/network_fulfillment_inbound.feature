Feature: network-fulfillment ingests external network demand and asks order-management for feasibility
  network-fulfillment (ADR 0001) has no HTTP intake — inbound demand
  arrives ONLY by polling a network gateway (here, the stub gateway seeded
  from fixtures/network-fulfillment/seed-demand.json, since this harness
  never runs a real credentialed network). This proves the whole inbound
  leg end-to-end over real HTTP/Postgres across THREE bounded contexts:

    1. network-fulfillment's poller (POLL_INTERVAL=5s) pulls the seeded
       demand and runs ReceiveNetworkDemand.
    2. The Anti-Corruption Layer translates each line's network product id
       to a SKU via PRODUCT_TRANSLATION_FILE. A product with no mapping
       rejects the WHOLE order without ever calling order-management —
       ReceiveUntranslatable builds a lineless order recording the refusal.
    3. A translatable order is raised as a HELD order in order-management
       (POST /orders, releaseOnAllocation=false) and its feasibility is
       asked via requiredShipBy. This harness runs order-management with
       PATH_CATALOGUE_SOURCE unset (see scripts/03-up-services.sh), so
       PromisePolicy.FeasibleBy has no CPT schedule/capability wired and —
       by design, with NO lead-time fallback (ADR 0001 §7 / order-
       management's own FeasibleBy doc comment: "a guess dressed as a
       commitment is how a vendor loses its account") — always answers
       infeasible. network-fulfillment must then cancel the held order and
       reject the network demand, never acknowledge a date it cannot
       actually keep.

  Both outcomes are REJECTED, but for different, independently observable
  reasons: the untranslatable order never carries a SKU or reaches order-
  management at all, while the translatable-but-infeasible one carries the
  real translated line.

  Background:
    Given all warehouse-systems services are healthy
    And network-fulfillment is healthy

  @e2e @network-fulfillment
  Scenario: An unmapped network product id is rejected without ever reaching order-management
    Then network-fulfillment eventually reports network order "po-e2e-netful-untranslatable" as REJECTED
    And that network order has no lines

  @e2e @network-fulfillment
  Scenario: A translatable order is raised in order-management and rejected when no promise is feasible
    Then network-fulfillment eventually reports network order "po-e2e-netful-known" as REJECTED
    And that network order has a line with network product id "ASIN-E2E-NETFUL-1" and SKU "SKU-E2E-NETFUL"
