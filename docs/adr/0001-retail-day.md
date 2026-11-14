# ADR 0001: the retail day, one run across the warehouse, the network and the storefront

Status: Proposed (accepted when this PR merges). Date: 2026-11-14.

Companion: [retail-day-journey.md](../retail-day-journey.md) (the chain, the role -> endpoint tables, the gaps).
Basis: code read from `origin/develop`; nothing was run live (the cluster was down). UNVERIFIED items are named.

## Context

Today three things exist and never meet: `cmd/warehouse-day` (a warehouse floor on its own site SIM1, orders posted
straight to order-management, stops at SLAM), a Playwright purchase against the storefront (stops at "Order placed"),
and network scenarios that play the fleet's role by hand. No run follows a shopper's order to the doorstep.

Research for this ADR found that the chain is broken in two places before any simulator can be written, and that the
network's clock makes a run slower than assumed.

1. **No site reaches outbound-shipping.** The storefront's building is `site-1` for the network (node `vendorNodeCode`,
   the CPT schedule in process-path-management, network-fulfillment's `SITE_ID`). order-management, facility-layout and
   outbound-shipping accept only `^[A-Z0-9]+$`, so network-fulfillment drops the site and storefront orders enter
   order-management, WES and the floor site-less. Separately, fulfillment-execution drops the site it receives on
   `WorkReleased` for order work and publishes `PackageManifested` with neither `site_id` nor (in practice) `cpt`;
   outbound-shipping ignores such a manifest. `yard_journey.feature` passes only because it injects the manifest.
2. **The floor has no site.** WES pools are keyed by path id, a claim has no site predicate, inventory-storage reserves
   from every bin of the SKU. A second "simulated site" floor shares the PICK pool and the stock with the storefront.
3. **A PO is not there when the order is.** order-sourcing releases a group to the node at `max(now, pickup cutoff -
   handling - 30 min)` and ranks pickups by cost, preferring the latest pickup that reaches the same wave. For site-1
   (cutoffs 11:00 and 17:00 America/Chicago, handling 60 min assumed) an order placed at 09:00 or 12:00 CT is released at
   15:30 CT; one placed between 15:30 and 16:00 CT is released at once. Neither the lead nor the handling is configurable
   by policy.

## Decisions

### D1. The storefront's building is AUS3; network-fulfillment maps `site-1` to it

AUS3 is a facility-layout site of the provisioning catalogue (archetype `SSD-FC`, Austin, TX), already the site order-
management projects demand to, already stocked, and its archetype has outbound dock (`OB`), outbound staging (`STG`) and
SLAM (`SLM`) zones. The network node keeps its code `site-1`. network-fulfillment gets a config map
(`NETWORK_SITE_MAP`, `site-1=AUS3`) applied where it decides `fulfillmentSiteId`, so held orders carry `AUS3`.

Rejected: renaming the node to an uppercase code. network-fulfillment adopts a node by `vendorNodeCode`, so a rename
means a new node, 86 new listing ids, a regenerated product dictionary, a new feed and lane, and an orphaned old node
that order-sourcing can still pick. The map is about 100 lines in one repo and touches no network data.

Consequences: process-path-management must hold a CPT schedule for AUS3 equal to site-1's (`aus-1100`, `aus-1700`,
America/Chicago); `cmd/seed-storefront` writes both from one fixture so they cannot drift. One PUT per site feeds
order-management, network-fulfillment and outbound-shipping. The fixture declares both ids; `warehouse-day`'s site guard
refuses both.

### D2. fulfillment-execution carries the order's site to `PackageManifested`

Only fulfillment-execution changes (order-management, WES and outbound-shipping already carry or read the site): a
`tasks.order_site_id`, filled from `WorkReleased.site_id` for non-transfer work; the PACK task created at rebin takes the
site of the order's PICK tasks; `PackageManifested` publishes `site_id`, and its `cpt` falls back to the PACK task's CPT
when there is no SLAM task (nothing creates one for customer orders). An addendum to fulfillment-execution ADR 0061 and
its AsyncAPI follow. The injected manifest in `yard_journey.feature` stays, because that scenario's point is the yard
contract.

Open: that order-management's promise instant equals a cutoff `local_time` of the site's schedule. The code says yes when
the promise basis is the site's capability (it is built from the schedule), and no on the lead-time fallback, which
happens when order-management has no schedule for the order's site. D1 therefore requires the AUS3 schedule to exist.

### D3. The run is windowed, in real time, on the storefront's own node

No second node, no compressed clock, no schedule rewrite on the storefront's site. network-fulfillment serves one site
and a profile revision on site-1 would break the CPT agreement the storefront depends on. The consequence is accepted:

- the shopper phase starts so that its orders are released inside the window, normally 15:30-16:00 America/Chicago
  (`retail-day -preflight` prints the minutes to it; `-wait-for-window` sleeps until it; `-start-now` runs anyway and
  the PO wait then lasts until the computed release time, with a bounded timeout);
- after release the chain is minutes: network poll 1 min, acknowledge, reconcile, floor, outbound;
- the transport legs are compressed by actors, not by clocks: linehaul has no time guard (a load takes the next scheduled
  pickup departure and is dispatched at once), last-mile accepts a wave for today with a cutoff minutes ahead.
  shipping-and-tracking applies scans in time order, so the actors keep scan times monotonic.

The run uses STANDARD and TWO_DAY tiers. NEXT_DAY exists only before 11:00 CT and its pickup choice is UNVERIFIED; it
gets its own morning window later. `make warehouse-day` stays as the compressed-clock warehouse regression on SIM1.

### D4. The day's shelf is a deterministic subset of the 81 storefront SKUs, restocked by an inbound leg

A run cannot create its own catalogue: the listing -> SKU dictionary is GitOps. The shelf is `N` SKUs chosen by a seed
from the purchasable set. The preflight refuses to start when any shelf SKU's usable stock is below the day's demand;
a restock leg (ASN -> dock appointment -> gate -> receipt -> stow through inbound-receiving and inventory-storage) ends
the day at or above the stock it started with.

### D5. Only published REST; a missing route is added where it belongs

No database, no injected event, no direct Kafka write (the tap only reads). A step with no route becomes a feature PR
in the owning repo. Known: none for the roles in the journey page, which all have routes; the gaps are data and defects
(G1-G12 there).

### D6. The verdict is per order and per estate

Per order: payment authorised once and captured once for the order total (capture follows `FulfillmentShipped`); one PO;
one order-management order; one work unit per line; PICK tasks done and one PACK; one package, one label, one
shipment confirmation; one tracking journey ending DELIVERED inside the promised window; the ids agree across
contexts. Estate: stock consumed equals units shipped; listing quantity and `purchasable` followed; no dead letter
carrying the run id; no 5xx outside a recorded suspension. Failure scenarios (decline, cancel before the PO is
accepted, out of area, sold out, infeasible deadline, network-fulfillment restarted mid-flow = exactly one order per PO)
are godog `@cluster` features. `tok_timeout` is NOT asserted green: today it wedges the checkout session (journey G8), so
the scenario records that behaviour as a finding until checkout and customer-payments change.

### D7. Cluster only, k3s only

The retail-day runs on the k3s cluster through Kong; the local compose harness keeps the warehouse tier. Nothing is
deployed from here: images come from released tags (standing rule).

## Consequences

- Work in other repos: fulfillment-execution (D2), network-fulfillment (D1 map), warehouse-infra (values, AUS3 schedule
  through the seeder), e2e-tests (everything else).
- Until D1 and D2 are released and deployed, a real SLAM cannot ship through outbound-shipping and the full run cannot
  pass; the actor packages are built and unit-tested against contract fakes of the published specs meanwhile.
- A run is a scheduled event, not a button: roughly one per day, about 45-90 minutes from the window.

## Not decided here

Whether the pickup choice, handling time or the 30-minute release budget should become policy so a simulation can shorten
them; a dedicated simulation node (needs multi-site network-fulfillment); the return leg (customer-returns is not
released or deployed).
