# Retail day: the journey one simulated order takes

Evidence: read from `origin/develop` of every repo named here on 2026-11-14, by reading code and specs. Nothing was
run against the cluster (it was down), so every behaviour below is "the code says", not "observed". Items marked
UNVERIFIED could not be settled from the code. This page is the input of the retail-day simulator
(`cmd/retail-day`); it is not a service contract, the owning repos' `apis/` are.

The simulator drives published REST only: no database, no injected events. Where a step has no REST route it is
listed under [Gaps](#gaps) with the repo that owns the fix.

## 1. The chain

```text
shopper -> storefront-bff -> shopping-cart / checkout / customer-payments(+provider) / customer-ordering
        -> CustomerOrderPlaced -> order-sourcing (assignment per group, release timer)
        -> FulfillmentReleasedToNode -> vendor-connectivity (PO ISSUED, V-WS)
        -> network-fulfillment polls the PO (1 min), translates listing -> SKU, raises a HELD order in order-management,
           acknowledges the PO, reconciles, releases the order
        -> OrderAllocated -> wes-work-planning unit `<order>-line-<n>` (pool PICK) -> work release
        -> fulfillment-execution tasks: pick -> rebin -> pack -> SLAM -> PackageManifested
        -> outbound-shipping: stage -> load -> close -> yard departure -> ShipmentConfirmed
        -> network-fulfillment: labels (shipping-and-tracking) + vendor shipment confirmation
        -> VendorShipmentConfirmed -> FulfillmentShipped -> payment capture, order SHIPPED
        -> linehaul load (by tracking number) -> LoadDeparted / LoadArrived
        -> last-mile wave induction -> route -> out for delivery -> DeliveryAttempted(DELIVERED)
        -> PackageDelivered -> customer-ordering order DELIVERED
```

## 2. The shopper (storefront-bff, base `/api/storefront`)

Every POST needs an `Idempotency-Key`; the `sf_state` cookie carries shopperId, cartId and checkoutId and is not
authentication. Budgets: 2 s query, 5 s command, 15 s confirm. The BFF may call only product-discovery,
delivery-promise, pricing, shopping-cart, checkout and customer-ordering.

| Step | BFF route | Downstream |
|---|---|---|
| register / sign in | `POST /shoppers`, `POST /sessions` | customer-ordering `POST /shoppers` |
| saved address | `POST /me/addresses` | customer-ordering `POST /shoppers/{id}/addresses` -> `addressId` |
| browse | `GET /products`, `GET /products/{id}` | product-discovery |
| delivery date | `POST /products/{id}/delivery-date-quotes {postalPrefix}` | delivery-promise `POST /delivery-quotes`, pricing charges |
| cart | `POST /carts`, `POST /carts/{id}/lines`, `PATCH .../lines/{item}`, `PUT /carts/{id}/destination` | shopping-cart |
| delivery options | `POST /carts/{id}/delivery-quotes`, `PUT /carts/{id}/delivery-selection {groupNo, offerId}` | shopping-cart -> delivery-promise |
| checkout | `POST /checkout {cartId, shipToAddressId}` | checkout `POST /checkout-sessions` (cart, delivery-promise, pricing) |
| pay | `POST /checkout/{id}/confirmations {paymentMethodToken}` | checkout -> customer-payments -> provider; then customer-ordering `POST /checkout` |
| read | `GET /orders/{id}`, `GET /orders` | customer-ordering order + tracking |
| cancel | `POST /orders/{id}/cancellations {reason}` | customer-ordering (409 `cancel-after-lock`) |

Destination is a 3-character postal prefix; only `786` and `787` are served. NEXT_DAY exists only before 11:00
America/Chicago; SAME_DAY is not offered. Confirm answers 200 `COMPLETED | DECLINED | REQUOTE_REQUIRED | FAILED`, or
202 `AUTHORIZING | PLACING` (poll `GET /checkout/{id}`).

## 3. Money

- Authorised at confirm (`pay-<12hex>`, provider hold `hold_<12hex>`); `tok_approve` gives a 7-day hold, `tok_decline`
  a provider 402 -> session DECLINED (re-confirm with another token while the quote is valid), `tok_timeout` a provider
  504 after up to 2 s -> customer-payments 503 -> session stays AUTHORIZING.
- Bound to the order by `CheckoutCompleted` (`retailnetwork.checkout.events`), which also closes the cart.
- Captured per group on order-sourcing's `FulfillmentShipped` (carries tracking numbers).
- Voided on amount mismatch, requote, rejected placement and the 22 h give-up; released on `FulfillmentCancelled` /
  `FulfillmentUnsourceable`. Refunded only on `PackageLost` / `PackageReturned` or a manual `POST /refunds` (not in
  the BFF).
- Cancel before the PO is accepted releases the hold; after that, 409 and no money moves.

## 4. Placed order to held order in the warehouse

1. customer-ordering `POST /checkout` (key = sessionId, so `orderId` = UUIDv5(sessionId)) makes and confirms a promise
   per group and publishes `CustomerOrderPlaced` on `retailnetwork.customer-ordering.events`.
2. order-sourcing creates one assignment per group (`asg-<uuid>`), picks a node that can ship every line, and
   publishes `FulfillmentReleasedToNode` at `releaseAt` = pickup cutoff - handling - 30 min (SAME_DAY at once), fired
   by a sweep. See [Timing](#timing).
3. vendor-connectivity turns it into a PO for VENDOR nodes: `PO-` + sha256(assignmentId:releaseNo)[:16], lines
   `{lineNo, listingId, quantity}`, `ackBy` = min(issuedAt + ack window, requiredShipBy - handling); state ISSUED at
   `GET /vendor/purchase-orders`. Which node: nothing chooses `site-1`; the seed creates ONE vendor node (vendor V-WS,
   vendorNodeCode `site-1`) with every listing, so it is the only candidate.
4. network-fulfillment (live, SITE_ID `site-1`) polls every minute, keeps ISSUED POs of mapped nodes, stores
   `NR-<uuid>`, translates `listingId` -> SKU through the helm-values `productTranslation` dictionary (a miss rejects the
   whole PO `UNKNOWN_LISTING`), then:
   - `POST /orders` on order-management, key `netful-raise-<networkRef>`, `releaseOnAllocation:false`,
     `allowPartialShipment:false`, `requiredShipBy`; no promise date means infeasible -> `CANNOT_MEET_SHIP_BY` and the hold
     is cancelled;
   - `POST /vendor/acknowledgements` (ACCEPT, every line echoed) -> 202; ReconcileSubmittedOrders checks the
     transaction, and only on SUCCESS calls `POST /orders/{id}/releases` (key `netful-release-<id>`);
   - one order-management order per PO is held by the unique PO row, the stable key and the stored hold.
5. order-management publishes `OrderAllocated` (lines `{line_no, sku, path_id, gift_wrap}`, promise date, promise cpt id).
   wes-work-planning creates a unit `<orderId>-line-<n>` in pool `PICK`.

## 5. The warehouse floor and outbound

| Role | Calls |
|---|---|
| release operator | wes `POST /paths/PICK/work-releases` (no in-fleet scheduler calls it; the simulator does) |
| picker | fulfillment-execution `POST /stations`, `POST /stations/{id}/check-ins`, `POST /stations/{id}/task-claims {taskType}`; inventory-storage `POST /reservations/{id}/pick-confirmations`; `POST /tasks/{id}/completions` |
| rebin | `POST /rebin/arrivals` (creates the PACK task) |
| packer | `POST /tasks/{id}/package-seals` |
| SLAM | `POST /packages/{id}/slam-runs` -> `PackageManifested {package_id, order_ref, source_order_id, cpt, weight_g}` |
| dock planner | dock-scheduling `POST /appointments {direction OUTBOUND, doorCode, carrier, windowStart/End, cptId}` |
| gate / yard clerk | yard-management `POST /visits {purpose LIVE_LOAD, appointmentId, trailers}`, `POST /moves/claims`, `POST /moves/{id}/completions` (docking raises `TrailerDocked`), `POST /trailers/{id}/door-work-starts`, `.../door-work-completions {loadState LOADED}`, `.../seals`, `POST /visits/{id}/check-outs` |
| stager | outbound-shipping `POST /packages/{id}/stagings {staging_location}` |
| loader | `POST /packages/{id}/loadings {load_id, door_code}` |
| load supervisor | `POST /loads/{id}/closures` (needs state LOADING, nothing outstanding) |

`ShipmentConfirmed` is raised by outbound-shipping when `TrailerDeparted` arrives for a CLOSED load with a seal; a
departure while still LOADING raises `PackageMissedLoad` instead. Payload: `load_id`, `site_id`, `cpt_id`, `carrier`,
`trailer_id`, `seal_number`, `departed_at`, `orders[{order_id}]`. network-fulfillment finds the network order by
`orders[].order_id` (order-management's id), requests labels (shipping-and-tracking `POST /labels` through vendor-connectivity
`/vendor/shipping-labels`), then `POST /vendor/shipment-confirmations` (tracking numbers must equal the labelled
ones). order-management also consumes it and moves the order to Shipped.

outbound-shipping prerequisites: a `CPTScheduleChanged` for the manifest's site (`{site_id, timezone, cutoffs[{cpt_id,
local_time, ship_method, days_of_week}]}`; `version` optional, defaults to 1, so the real
`PUT /sites/{id}/cpt-schedule` is accepted), the manifest's `cpt` equal to a cutoff `local_time` on a listed day in the
site's timezone, facility-layout zones and Staging slots for the same site, and an OUTBOUND dock appointment with
`cpt_id` (gives the load its door) plus a yard `TrailerDocked` at that door (starts LOADING).

## 6. Network transport and delivery

| Role | Calls |
|---|---|
| linehaul dispatcher | linehaul-transportation `POST /loads {departureId}`, `POST /loads/{id}/packages {trackingNumber}`, `.../closures`, `.../dispatches`, `.../receipts` |
| station associate | last-mile-delivery `POST /waves`, `POST /waves/{id}/induction-closures`, `POST /waves/{id}/dispatches`, `POST /partners {stationNodeId, vanCapacity}`, `POST /routes {waveId, dspId, stops[]}` |
| delivery driver | `POST /routes/{id}/starts`, `POST /routes/{id}/attempts {trackingNumber, outcome DELIVERED\|FAILED\|COLLECTED, at}` |
| carrier | shipping-and-tracking `POST /journeys/{tn}/scans`, `POST /carrier-events` (only for CARRIER_HANDOFF areas; 786/787 run the network's own route) |

- Linehaul has no time guard on close, dispatch or receive: a load can be opened, packed, closed and dispatched before
  the scheduled departure. The only link from the warehouse side is the tracking number (the load id of
  outbound-shipping and the linehaul load id are unrelated). `LoadArrived` at a DELIVERY_STATION runs the last-mile
  induction: the earliest OPEN wave whose cutoff is after the arrival time.
- `POST /waves` validates only `inductCutoffAt < windowStart < windowEnd`, a valid service date, the stop capacity and a
  known DELIVERY_STATION: a wave for today with a cutoff minutes ahead is accepted.
- A route needs the wave INDUCTION_CLOSED, an active partner at the same station and stops within van capacity; a start
  needs the wave DISPATCHED and emits `PackageOutForDelivery`; a DELIVERED attempt reaches shipping-and-tracking
  (`PackageDelivered`) and customer-ordering (DELIVERED once every announced package of every non-cancelled group is).
- shipping-and-tracking applies scans in time order and stores an older scan without applying it: scan times must be
  monotonic in a compressed timeline.

Order status: PLACED -> IN_FULFILLMENT (PO accepted) -> PARTIALLY_SHIPPED / SHIPPED -> PARTIALLY_DELIVERED /
DELIVERED; CANCELLED, LOST, RETURNED on the matching events.

## 7. What the floor is not

The warehouse side has no site scoping: WES pools are keyed by path id only, a fulfillment-execution claim takes the
earliest-CPT pending task of the requested type with no site predicate, workforce shifts are optional and FE never
calls them, and inventory-storage reserves from every bin of the SKU. A station registered for the simulator's own
site can therefore claim storefront PICK tasks, and the simulator and the storefront share the PICK pool and the stock.

## Gaps

Kind: P = product gap (owning repo changes), D = data/provisioning prerequisite, H = harness work, F = finding about
existing behaviour to report, not to work around.

| # | Kind | Gap | Owner |
|---|---|---|---|
| G1 | P, blocking | `PackageManifested` carries no `site_id` and, in practice, no `cpt` (the cpt comes from a SLAM task nothing creates), so outbound-shipping ignores a real SLAM: no load, no staging, no `ShipmentConfirmed`. fulfillment-execution drops the `site_id` it receives on `WorkReleased` for order work. The yard journey feature injects the manifest. | fulfillment-execution (ADR 0001 D2) |
| G2 | P/design, blocking | The storefront's site is `site-1` (network node, CPT schedule, `SITE_ID`) but order-management, facility-layout and outbound-shipping accept only `^[A-Z0-9]+$`. network-fulfillment drops the invalid value, so storefront orders reach order-management, WES and the floor without a site, and `OrderAllocated` has no `site_id`. Decided: the building is AUS3, mapped from `site-1` in network-fulfillment (ADR 0001 D1). | network-fulfillment, warehouse-infra, e2e-tests |
| G3 | H | Nothing in the fleet calls WES work release; the floor actor must. | e2e-tests |
| G4 | D | Outbound needs a dock appointment per CPT, yard visit and seal, facility-layout Staging slots and dock calendar settings for the site; they only come from REST on dock-scheduling, yard-management and facility-layout. | e2e-tests (provision step) |
| G5 | H | Nothing feeds a tracking number into a linehaul load; the linehaul actor reads the label and pushes it. | e2e-tests |
| G6 | D/F | Pickup departures for site-1 are generated only from its declared cutoffs (11:00, 17:00 America/Chicago); a lane leaving a vendor FC yields nothing from `POST /lane-schedules`. A run uses the next scheduled pickup departure and dispatches early (no time guard). A profile revision would give a near-term cutoff but cancels unbooked pickups and breaks the CPT agreement with the storefront. | linehaul-transportation |
| G7 | H/D | Waves, partners, routes, starts and attempts are all manual; the station and its service areas (786/787 -> DS-SIM-1) come from the network seed; induction needs `LoadArrived` before the wave cutoff. | e2e-tests |
| G8 | F | `tok_timeout` leaves the checkout session in AUTHORIZING: it cannot be re-confirmed or expired, and the sweep re-calls with the same key and token. No way out found. | checkout, customer-payments |
| G9 | F | `REQUOTE_REQUIRED` has no BFF route to accept the new quote (shopping-cart has `delivery-requote-acceptances`); the shopper must re-quote and re-select. | storefront-bff |
| G10 | D | The listing -> SKU dictionary is GitOps (helm values) and listing ids are network-generated, so a reseed makes every PO `UNKNOWN_LISTING`. A run cannot create its own catalogue. | warehouse-infra |
| G11 | F | `GET /orders/{id}` on the BFF shows checkout totals only when the cookie's checkoutId placed the order. | storefront-bff |
| G12 | F | Cancel is possible only until the PO is accepted. | customer-ordering |

## Timing

A PO does not exist when the order is placed. order-sourcing searches pickups with cutoff >= now + handling, pairs each
with the earliest wave it feeds, ranks by declared cost and then by legacy cost (10 per whole hour from pickup departure
to wave window start, which prefers the LATEST pickup reaching the same wave), and releases at
`max(now, cutoff - handling - 30 min)`. The 30 minutes is a constant, not policy; the sweep runs every 15 s.

For site-1 (pickups 11:00 and 17:00 America/Chicago arriving 120 min later; waves induct 05:00, window 09:00-21:00 on
the same service date; handling 60 min assumed, the live value is UNVERIFIED because network-fulfillment derives and
declares it):

| Order placed (CT) | Pickup chosen | Released |
|---|---|---|
| 09:00 | 17:00 (cost 160 beats the 11:00 pickup's 220) | 15:30, 6 h 30 later |
| 12:00 | 17:00 | 15:30, 3 h 30 later |
| 15:30-16:00 | 17:00 | at once |
| after 16:00 | next day's 17:00 | next day 15:30 |

A run therefore starts in the 15:30-16:00 CT window, or waits for 15:30. See ADR 0001 D3 for what that implies and what
is deliberately not done to shorten it.

## Running it: `cmd/retail-day`

`make retail-day-preflight` is read-only (cluster gate, then the REST preflight). `make retail-day` plays the day:
the preflight, the release-window gate, then `internal/retailday/day` over the real actors, and a report on stdout
(progress goes to stderr). It WRITES to the cluster: shoppers register, buy and cancel, the floor, the dock, the
network and the restock leg act. Nothing here has been run against the cluster.

| Phase | Real actor behind the interface | Reads its ids from |
|---|---|---|
| catalogue | `shopper.LoadCatalogue` (BFF `GET /products`) | SKU dictionary: `fixtures/storefront/catalogue.json` SKUs, warehouse-infra `retail-network-seed.json` (`-network-data`) SKU -> title, offer-availability `GET /catalog` title -> catalogItemId (as `cmd/seed-storefront`) |
| shoppers | one `shopper.Shopper` per buyer, e-mail `retailday-<run>-b<NN>@example.com` | the plan |
| link | `link.Linker.OrderIDs` (waits up to `-link-timeout`) | order-sourcing, network-fulfillment |
| floor | `floor.Floor.ProcessAll` | order-management ids of the link |
| ship | `outbound.Floor.ShipAll`, one call per load | doors and staging lanes found by the readiness step |
| deliver | `transport.Transport.DeliverOrder` | origin: vendor-connectivity `GET /vendor/nodes` (node with `vendorNodeCode` site-1, ACTIVE); station: node-topology `GET /service-areas/{prefix}` of the order's prefix |
| restock | `inbound.Crew.Restock` | inbound doors (inbound-receiving), stow bins (`-stow-bins`, else the building's Storage slots that are registered bins with room) |
| evidence | composite: `collect.Collector` (with the linker's `ForPurchaseOrders`) answers Order and Shelf over published REST; `dlq.Counter` answers DeadLetters over Kafka, read only | the broker at `-brokers` (below) |

Flags: `-seed`, `-shoppers` (>= 5), `-shelf`, `-parallel`, `-json`, `-allow-unverified`, `-brokers`, `-run-id`, `-fixtures`,
`-network-data`, `-stow-bins`, `-v`; `-start-now` (play outside the release window: the PO wait lasts until the computed
release time, so `-link-timeout`, default 2h, must be longer than that; without it, or `-wait-for-window`, a run outside
the window is refused); `-provision` (below).

Exit codes: 0 the verdict is PASSED with every planned order accounted for; 1 the preflight failed, a phase could not be
built or the verdict is FAILED; 2 usage, the cluster gate or the release window refusing the run.

**Dead letters are counted from Kafka, read only** (`internal/retailday/dlq`; no REST route lists them). A dead letter is the
original message (key, value and headers unchanged) republished to `<source-topic>.dlq` with `x-dlq-*` headers
(warehouse-infra `docs/dlq-runbook.md`; the services' `deadletter.go`). `Prepare` takes, before anything is read or written,
a snapshot of the end offset of every partition of every topic whose name ends in `.dlq` (found by the suffix: a new context
adds one, and a `.dlq` topic exists only after its first poison message). After the day the audit reads each partition from
the snapshot to its end now and counts the messages whose value, a header value or key contains the run id. A run with none
is `0, known`; a dead letter of the run fails the day, `-allow-unverified` or not. Anything that cannot be read (broker
unreachable, a topic unreadable or gone, messages deleted before they were counted, no snapshot) makes the count UNKNOWN,
never 0: the day then fails on the NOT VERIFIED line unless `-allow-unverified` is given, which lists what the evidence
could not read and excuses no failed rule.

**`-brokers` (env `KAFKA_BROKERS`, default `localhost:9092`).** One host:port; `""` runs without counting. The default is the
address `cmd/warehouse-day` and `env.sh` use and the one `kubectl port-forward svc/kafka 9092:9092` binds, **not an address the
k3s cluster answers on by itself**: `k3s-edges.sh` forwards no Kafka and the broker has no external listener
(warehouse-infra ADR 0011). So start `kubectl --context k3s-multipass -n warehouse-systems port-forward --address 127.0.0.1
svc/kafka 9092:9092` and leave it open. The broker advertises its in-cluster DNS name, so the counter dials the one address
it was given for every connection, whatever the metadata names (one broker only). The preflight checks the broker (a
throwaway snapshot, reads only) after the REST checks pass, so an unreachable broker stops the run before the release window
is waited for; `Prepare` takes the real snapshot and fails closed with the same message.

**`-provision` (default off).** The readiness steps (`outbound.Provision`, `inbound.Ready`) are read-only and fail closed:
anything not ready is an error naming the check. With `-provision` the dock readiness may write what the building lacks, each
through the owning route: register the facility-layout site, the `-doors` and `-staging` slots, and declare a never-declared
dock calendar (`PUT /sites/{id}/dock-calendar/settings`). It never writes the CPT schedule (`make provision-storefront` owns
it) and never registers bins (`make seed-inventory`). The floor's own stations (`POST /stations`, check-ins) are the run's
people, installed only after the readiness passed and checked out at the end, with or without `-provision`.

## What is still UNVERIFIED

- Whether any storefront order has ever shipped end to end. No e2e feature mentions linehaul, last-mile,
  shipping-and-tracking or customer-ordering; the outbound-shipping ADRs call the live one-order proof pending.
- Whether order-management's `promise_cpt_id` for a storefront order equals a cutoff `local_time` of the site's
  CPT schedule (outbound-shipping needs that match).
- order-management's idempotency-key retention (the "one order per PO" guarantee leans on it).
- Live versions: which images the pods run against these contracts.
