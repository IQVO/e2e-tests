# warehouse-e2e (`e2e-tests`)

Black-box, over-the-wire end-to-end / system test harness for the
[warehouse-systems](https://github.com/claudioed) estate: builds and runs
every bounded context's real HTTP service (and MCP server) as independent
OS processes against real Postgres + Kafka, then drives them purely over
their published REST APIs with a [godog](https://github.com/cucumber/godog)
(Cucumber for Go) suite. It never imports another repo's Go packages —
exactly like a human running curl against a live deployment.

This repo is a study-project companion to the bounded-context repos
(`facility-layout`, `product-master`, `inventory-storage`,
`wes-work-planning`, `fulfillment-execution`, `workforce-management`,
`order-management`, `process-path-management`, `labor-performance`,
`network-fulfillment`, `network-inventory-planning`, `warehouse-planning`,
`inbound-receiving`, `slotting-optimization`)
plus the read-side decision-support agent `warehouse-ops-agent`, all siblings
under the same `warehouse-systems/` workspace root — see `env.sh`'s
`REPOS_ROOT` for the layout this harness assumes (it is overridable:
`REPOS_ROOT=/path/to/repos`, so a git worktree of this repo can build the
siblings).

## Disclaimer

This project is a domain-driven design study. It is not associated with,
endorsed by or affiliated with any company, and it uses no company's code,
data, logos or trademarks. All names, figures and examples are illustrative
or synthetic.

## Services the harness runs

Ports and DSNs live in `env.sh` only; this table mirrors it.

| Service | Binary (`cmd/`) | HTTP | Postgres | Classification role |
|---|---|---|---|---|
| process-path-management | `pathmgmt` | 8087 | 5447 | — |
| facility-layout | `facility` | 8081 | 5441 | — |
| **product-master** | `api` | 8090 | 5450 | **owner**: `PUT /products/{sku}` + `PUT /products/{sku}/classification`, physical profile; publishes `warehouse.product-master.events` |
| inventory-storage | `inventory` | 8082 | 5442 | local copy (`PRODUCT_MASTER_CONSUMER_GROUP`); its classification `PUT` answers `410 classification-moved`; books inbound-receiving's Good receipt lines as staged stock (`INBOUND_RECEIPT_CONSUMER_GROUP`) |
| wes-work-planning | `wes` | 8083 | 5443 | local copy (`PRODUCT_CLASSIFICATION_MODE=kafka`) |
| fulfillment-execution | `execution` | 8084 | 5444 | local copy (`PRODUCT_CLASSIFICATION_MODE=kafka`) |
| labor-performance | `labor` | 8088 | 5448 | — |
| workforce-management | `workforce` | 8085 | 5445 | — |
| order-management | `order` | 8086 | 5446 | local copy (`PRODUCT_CLASSIFICATION_MODE=kafka`) |
| network-fulfillment | `netfulfil` | 8089 | 5449 | — |
| network-inventory-planning | `network-inventory-planning` | 8099 | 5451 | — |
| warehouse-planning | `api` | 8100 | 5452 | — |
| **inbound-receiving** | `api` | 8108 | 5453 | ASN / dock appointment / receipt (`/asns`, `/appointments`, `/receipts`); known SKUs from product-master's `ProductRegistered` (`PRODUCT_MODE=kafka`), dock doors permissive; publishes `warehouse.inbound-receiving.events` |
| **slotting-optimization** | `api` | 8109 | 5454 | forward-slot planner (`/slot-plans`, `/forward-slots`, `/sku-velocity`); three local copies, all `kafka` mode: product-master's classification and physical profile (`PRODUCT_MODE`), facility-layout's zones and slots (`LAYOUT_MODE`), order-management's `SiteSkuDemandChanged` (`DEMAND_MODE`, injected by the scenarios); publishes `warehouse.slotting-optimization.events` |
| warehouse-ops-agent | `agent` | 8096 | — | — |

Every local copy uses a per-run consumer group derived from
`E2E_CONSUMER_GROUP_SUFFIX` (never the cluster's group): that includes
inbound-receiving's product copy (`PRODUCT_CONSUMER_GROUP`),
inventory-storage's consumer of inbound-receiving
(`INBOUND_RECEIPT_CONSUMER_GROUP`, which does not exist when unset) and
slotting-optimization's three copies (`DEMAND_CONSUMER_GROUP`,
`PRODUCT_CONSUMER_GROUP`, `LAYOUT_CONSUMER_GROUP`, required in `kafka` mode).
`PRODUCT_CLASSIFICATION_MODE=http` no longer exists: the three readers
refuse to boot with it.

## What's covered

14 feature files (the inter-warehouse transfer feature has two scenarios,
the inbound-receiving and slotting-optimization ones four each,
returns/exceptions three):

- **`features/slotting_optimization.feature`** (`@slotting`) — the forward-slot
  planner, against slotting-optimization, facility-layout and product-master
  only. Each scenario plans its own run-scoped site (zone, slots, SKUs and
  demand), so every assertion is exact: (1) with SKUs registered, classified
  and measured in product-master and demand of 3 lines for one SKU and 1 for
  another, the generated Draft gives the faster SKU the lexically first
  forward slot (`abc-velocity-v1`), approval makes `GET /forward-slots` and the
  `SlotPlanApproved` event carry exactly that map; (2) **stickiness** — once
  the slower SKU outsells the faster one, a new plan still keeps both SKUs
  where they are and has no move at all (a planner without stickiness would
  Relocate both); (3) a hazmat SKU, the fastest one, is placed only in the
  hazmat forward slot, a second hazmat SKU is left unassigned
  (`NoEligibleSlot`) rather than put in the ambient slot that sorts first;
  (4) approving an already approved plan answers `409 plan-not-draft`, leaves
  it at version 2 and publishes exactly one `SlotPlanApproved`. **Demand is
  injected**, not placed through order-management: that service projects every
  line to one static site per deployment and stamps the promise date (48 h
  ahead) as `due_at`, while a plan counts only lines due in
  `[now - lookback, now)`; the scenarios publish the same
  `SiteSkuDemandChanged` it would, as the transfer feature does.

- **`features/inbound_receiving.feature`** (`@inbound-receiving`) — the inbound
  dock workflow and its handover, against inbound-receiving, product-master
  and inventory-storage only: (1) an ASN for a product-master-registered SKU
  is received short (expects 50, 40 Good) and closes with the exact `Short`
  discrepancy, asserted on the REST read and on the `ReceiptClosed` event;
  (2) the **handover**: Good units reach inventory-storage as staged stock
  (exactly one `StockReceived` for the SKU on its analytics topic, because
  staged stock has no REST read), usable stays 0 until `POST /stock/stow`
  and is then the received quantity; (3) a dock appointment refuses an
  overlapping booking on the same door with `409 door-window-overlap`
  (while a window starting exactly when the first ends is accepted), checks
  in, and completes when the receipt opened from it closes; (4) a `Damaged`
  line is recorded on the receipt and is NOT booked as staged stock. Door
  codes are accepted without a facility-layout dock slot
  (`DOCK_DOOR_MODE` permissive); SKU existence is enforced
  (`PRODUCT_MODE=kafka`), so ASN registration waits for `ProductRegistered`.

- **`features/inter_warehouse_transfer.feature`** — the whole
  inter-warehouse transfer saga across real service binaries and real Kafka:
  network-inventory-planning approves a transfer, inventory-storage
  allocates the origin stock, NIP releases the pick demand
  (`WorkDemandReleased`), wes-work-planning enqueues and releases it,
  fulfillment-execution's floor picks (`TransferPicked`), NIP releases the
  dispatch demand, the floor dispatches (`TransferDispatched`), the
  destination scans and stows through inventory-storage's REST
  (`TransferReceiptStaged` / `TransferStockStowed`) and NIP reaches
  `RECEIVED`. A second scenario proves the compensation: origin stock that
  cannot cover the transfer → `TransferStockAllocationRejected` → `UNFULFILLABLE`,
  no work released. The feature header lists exactly what is injected or
  seeded rather than produced by a real service (`SiteCapabilityChanged`,
  `SiteSkuDemandChanged`, site custody of stock/bins) and why. It needs
  warehouse-planning (the real producer of `CapacityPlanPublished`) and
  network-inventory-planning, both started by `scripts/03-up-services.sh`.
  A short-pick scenario is not possible today: fulfillment-execution's
  completion carries no picked quantity.
- **`features/bootstrap.feature`** — a single work unit flowing through
  every bounded context: facility-layout's physical map, inventory-storage
  stock, workforce-management's committed shift plan (Kafka →
  wes-work-planning's labor-plan-view), wes-work-planning's release (Kafka →
  fulfillment-execution's task creation), and fulfillment-execution's task
  completion (Kafka → wes-work-planning's completion read model).
- **`features/product_master.feature`** (`@product-master`) — product-master
  owns classification and the physical profile: a SKU is registered and
  classified `Hazmat` there, inventory-storage eventually knows it as
  `Hazmat` through `warehouse.product-master.events` alone, declared then
  measured dimensions flip the profile to `effectiveSource=measured` with
  the discrepancy flag set, and inventory-storage's retired
  `PUT /products/{sku}/classification` answers `410 classification-moved`
  without writing anything.
- **`features/facility_layout_propagation.feature`** — proves
  facility-layout's warehouse-map events (zone hazmat/temperature
  attributes, newly-registered and decommissioned slots) reach
  inventory-storage's local placement-rules read model purely over Kafka,
  picked up live without a restart — the only proof these two contexts
  actually agree over the wire. Its hazmat SKU is classified in
  product-master and the scenario waits until inventory-storage's copy
  knows it before the stow assertions (the real proof) run.
- **`features/flow_balance_exception.feature`** (T5) — proves
  `warehouse-ops-agent`, the agentic read-side decision-support layer, end
  -to-end against the real MCP servers of all five contexts: three
  independent signals (a saturated wes-work-planning pool, a
  workforce-management staffing gap, a stuck fulfillment-execution task) are
  seeded for the same process path, and the agent must correlate them into
  the expected `FlowBalanceException` (E1) — a ranked recommendation with a
  full evidence trail — and surface the same exception in its daily brief
  (E3).
- **`features/labor_performance.feature`** — proves labor-performance's
  PURE Kafka consumption of fulfillment-execution's `TaskCompleted` event
  (no REST endpoint any caller writes to): an associate checked into a
  station completes a task against an active engineered standard, and
  labor-performance eventually reports a scorecard reflecting it.
- **`features/order_management_choreographed_release.feature`** — proves
  order-management's choreographed-release redesign end-to-end: placing an
  order (`POST /orders`) with `allowPartialShipment=false` and lines that
  can be immediately allocated triggers, in the SAME call, synchronous
  allocation against inventory-storage (unchanged HTTP) followed by
  publishing `OrderAllocated`/`OrderPartiallyAllocated` to Kafka topic
  `warehouse.order-management.events` — which wes-work-planning's 4th
  consumer subscription (`handleOrderManagementEvent`) picks up and turns
  into a real work unit via its existing `EnqueueWorkUnit` use case, using
  the deterministic id `{order_id}-line-{line_no}`. This is order-
  management's only public REST surface in v1 (`POST /orders`,
  `GET /orders/{id}`, `POST /orders/{id}/retry-allocation`,
  `DELETE /orders/{id}`, `GET /healthz`) — there is no `/allocate` or
  `/release` endpoint anymore; release happens implicitly, choreographed
  over Kafka.
- **`features/process_path_management.feature`** — exercises
  process-path-management's full REST lifecycle (define, get, list,
  revise, deactivate) directly plus its Kafka publisher
  (`ProcessPathCreated`/`Updated`/`Deactivated`) — the only proof this
  context's write side and event contract both work against a real,
  independently running process and a real broker.
- **`features/promise_repromise_loop.feature`** — the closing proof of
  the "promise derived from fulfillment capability" initiative (ADR 0014,
  all sections): a missed CPT flows through three bounded contexts and
  three Kafka topics — order-management's original promise, fulfillment-
  execution's CPT-miss sweep publishing `TaskCPTMissed`, and
  order-management's `RepromiseConsumer` (ADR 0018) re-running its promise
  policy and publishing `OrderRepromised` — with zero manual intervention.
- **`features/returns_exceptions.feature`** (`@returns-exceptions`) — the
  two returns/exceptions contexts end to end against returns-processing,
  exception-management and inventory-storage: (1) a graded-A return line
  restocks — the injected `OrderAllocated`/`PackageManifested` facts make the
  line authorizable, the RMA authorization keeps refusing (404/409) until
  BOTH local copies catch up, the grade-A inspection decides RESTOCK and
  publishes `DispositionDecided` + `ReturnLineStaged`, which
  inventory-storage's RETURN_LINE consumer books as staged stock (exactly one
  `StockReceived`, usable 0) until a stow makes the units usable; (2) a
  re-fired `TaskCPTMissed` (two different CloudEvents ids) folds into ONE
  case at occurrence 2 and walks the operator lifecycle
  (triage → assign → resolve, `ExceptionResolved` on the topic, second
  resolve 409); (3) a correlated `OrderRepromised` (same order, injected)
  auto-resolves the case with the full derived rule id
  `...TaskCPTMissed->...OrderRepromised`, and a second repromise leaves
  exactly one `ExceptionAutoResolved` on the topic.
- **`features/soak_backlog_ramp.feature`** (`@soak`, excluded from the
  default run) — ramps injected backlog into both the PICK and PACK
  process paths over a sustained, configurable duration while a pool of
  picker/packer stations continuously claims and completes the resulting
  tasks, proving the estate stays up and keeps processing under sustained
  load rather than a single deterministic unit of work; see "Sustained
  backlog-ramp soak" below for details.

## Running locally

Prerequisites: Docker (for Postgres + the shared Kafka broker), Go
(matching `go.mod`), and every sibling repo in the table above
(including `product-master`) checked out alongside this one under the same
parent directory (`scripts/01-build.sh` stops with a clone hint when one is
missing).

```bash
cd e2e-tests
bash scripts/02-up-infra.sh      # Postgres (14 instances, this repo) + shared Kafka; pre-creates every topic (cmd/ensure-topics) so a fresh broker cannot lose the first publish or leave a consumer group unassigned
bash scripts/01-build.sh         # builds all 22 binaries (14 HTTP incl. product-master, inbound-receiving, slotting-optimization, network-inventory-planning, warehouse-planning + 7 MCP + ops-agent)
bash scripts/03-up-services.sh   # starts them as background processes (product-master, inbound-receiving and slotting-optimization before inventory-storage; NIP last)
bash scripts/04-run-tests.sh     # runs the default godog suite (excludes @soak)
GODOG_TAGS=@product-master bash scripts/04-run-tests.sh  # just the product-master scenario
GODOG_TAGS=@inbound-receiving bash scripts/04-run-tests.sh  # just the inbound-receiving scenarios
GODOG_TAGS=@slotting bash scripts/04-run-tests.sh  # just the slotting-optimization scenarios
GODOG_TAGS=@returns-exceptions bash scripts/04-run-tests.sh  # just the returns/exceptions scenarios
bash scripts/06-run-soak.sh      # OPTIONAL: the long-running @soak backlog-ramp run (see below)
bash scripts/05-down-services.sh # stops only what this harness started
```

`env.sh` is the single source of truth for every port, DB URL, and MCP
endpoint `scripts/*.sh` uses — edit ports there only. MCP is unauthenticated.

## Configuration

The harness binaries are driven by flags and Makefile variables; only two
environment variables are read directly by `cmd/`:

| Env var | Default | Meaning |
|---|---|---|
| `KAFKA_BROKERS` | `localhost:9092` | Broker list `cmd/ensure-topics` creates the contract topics on (comma-separated; the first entry is used) |
| `STOREFRONT_NETWORK_DATA` | path relative to the repo root | `cmd/seed-storefront`: warehouse-infra retail-network data file (SKU -> catalogue title); the CHECK step fails before any write when a storefront SKU is missing from it |

Sibling service base URLs are passed inline by `scripts/lib.sh` and the
Makefile (the `make run` block above); no `cmd/` binary reads them from the
environment.

## Sustained backlog-ramp soak (`features/soak_backlog_ramp.feature`, `@soak`)

`scripts/06-run-soak.sh` runs a dedicated, long-running scenario — by
default one hour — that ramps injected backlog into BOTH the `pick-soak`
and `pack-soak` process paths while a configurable pool of picker and
packer stations continuously claims and completes the resulting
fulfillment-execution tasks. It proves the estate stays up and keeps
processing under sustained, ramping load — unlike every other scenario
here, which proves a single deterministic unit of work flows correctly.

- **Excluded by default.** `e2e_test.go`'s `TestMain` filters scenarios
  with `GODOG_TAGS` (default `~@soak`, i.e. "everything except `@soak`"),
  so `scripts/04-run-tests.sh` and a plain `go test ./...` never run it.
  Only `scripts/06-run-soak.sh` sets `GODOG_TAGS=@soak` to run it in
  isolation.
- **Observational, not assertive.** Once picker/packer throughput can't
  keep up with the ramp, `wes-work-planning`'s `/release` legitimately
  returns `409` (WIP limit reached / pool empty) and
  `fulfillment-execution`'s `/claim-next` legitimately returns `409` (no
  claimable task) — both are counted in the printed summary, not treated
  as failures. The scenario only fails on a hard connectivity/setup
  problem (it never managed to enqueue a single work unit for the whole
  run).
- **Tunable via env vars** (all optional — see `soak_test.go` for exact
  defaults): `SOAK_DURATION` (default `1h`), `SOAK_RAMP_START_INTERVAL`
  (default `2s`), `SOAK_RAMP_END_INTERVAL` (default `200ms`),
  `SOAK_WIP_LIMIT` (default `20`, both paths), `SOAK_PICKERS` (default
  `3`), `SOAK_PACKERS` (default `2`).
- A quick smoke run before committing to the full hour:
  `SOAK_DURATION=1m SOAK_RAMP_START_INTERVAL=1s SOAK_RAMP_END_INTERVAL=200ms bash scripts/06-run-soak.sh`.

## Simulated operating day (`cmd/warehouse-day`)

A deep, black-box simulation of one warehouse working day, compressed into
minutes of wall time (default 8m for 06:00-22:00). Every action is a call to
a bounded context's **published REST contract** — no DB seeding, no internal
endpoints — and every integration topic is tapped to audit the async side.

```sh
make up                                     # infra + build + services
make warehouse-day                          # default day: 150 orders, 13 associates
DAY_DURATION=90s DAY_ORDERS=20 make warehouse-day   # quick smoke
```

The day, hour by hour:

| Sim time | Persona | APIs driven |
|---|---|---|
| 05:30 | control tower | facility-layout `POST /location-types` + `POST /locations/import` (site/zone/aisle/slot hierarchy); inventory-storage `PUT /bins/{binId}`; product-master `PUT /products/{sku}` (every SKU) + `PUT /products/{sku}/classification` (fragile SKUs; inventory-storage and the readers learn it from `warehouse.product-master.events`); process-path-management `POST /process-paths` + `PUT /sites/{siteId}/cpt-schedule`; labor-performance `POST /standards` |
| 05:30 | receiving | inventory-storage `POST /stock/receive` + `POST /stock/stow` into registered bins (the ASN-driven path, inbound-receiving `POST /asns` → `/receipts` → `/receipts/{id}/lines` → `/close`, whose Good lines inventory-storage books as staged stock, is covered by `@inbound-receiving` and is not part of this simulated day) |
| 06:00 | supervisor | workforce-management start-shift, `POST /shift-plans`, assignments; WES charge/plan; fulfillment station registration + check-in |
| 06:00-20:00 | customers | order-management `POST /orders` on a daily demand curve: standard, partial-ok, held-then-released/cancelled, backorder (understocked SKU), injected mispick |
| all day | pickers / flex | `claim-next PICK`, `GET /work-units/{id}`, reservation + bin lookup, travel via facility-layout `/distance`, `confirm-pick`, complete, `POST /rebin/arrivals` |
| all day | packers / SLAM | `claim-next PACK`, `seal-package`, `POST /packages/{id}/slam` (weight mismatch injected), `GET /packages?orderRef=` -> LABELED / DIVERTED |
| hourly | supervisor | WES telemetry + rebalance, queue depths, staffing gaps, flex reassignment, breaks (lunch 11:30-12:30) |
| 10:00, 13:00 | inventory control | cycle counts (one deliberate shortage), replenishment + `retry-allocation` for backorders |
| 22:00+ | auditor | per-order trace across order-management, WES, inventory, fulfillment, packages, labor scorecards and Kafka; inventory conservation; DLQ count |

The run prints an end-of-day report (hourly snapshots, per-associate
throughput, events per topic, per-order problems, PASS/FAIL checks) and writes
`run/warehouse-day-<RUN_ID>.json`. Exit code is non-zero unless the warehouse
worked end to end.

Pin `E2E_CONSUMER_GROUP_SUFFIX` (e.g. `warehouse-day`) before `make up` when
running against a long-lived shared broker, so services resume their
committed offsets across restarts instead of replaying the topic history.

## Seller inbound, cross-org (`@seller-inbound`, live cluster)

`features/seller_inbound.feature` + `seller_inbound_test.go`: a seller plans and ships
SSCC-labelled mastercases to a vendor node, the vendor polls them and reports a short and a missing
carton, the seller sees `CLOSED_WITH_DISCREPANCY` and node-performance exposes the inbound metrics.
It runs against the **kind cluster through Kong** (not the local harness) and is self-contained: a
fresh vendor, an `INBOUND_CROSS_DOCK` node with inbound limits, a catalogue item (each GTIN + CASE of
12) and a seller per run, every GTIN/SSCC valid and unique. The fleet is not part of it: its ACL
(network-fulfillment) runs in stub mode there, so the test plays the fleet's role through the Vendor API
(`POST /vendor/inbound-receipts`).

```sh
P=http://localhost:8000/api
SELLER_INBOUND_BASE_URL=$P/seller-inbound VENDOR_CONNECTIVITY_BASE_URL=$P/vendor-connectivity \
NODE_TOPOLOGY_BASE_URL=$P/node-topology OFFER_AVAILABILITY_BASE_URL=$P/offer-availability \
NODE_PERFORMANCE_BASE_URL=$P/node-performance GODOG_TAGS=@seller-inbound \
  go test -v -run TestMain -timeout 15m .
```

| Env var | Default | Context |
|---|---|---|
| `SELLER_INBOUND_BASE_URL` | `http://localhost:8000/api/seller-inbound` | sellers, plans, shipments, cartons, labels |
| `VENDOR_CONNECTIVITY_BASE_URL` | `http://localhost:8000/api/vendor-connectivity` | vendors, nodes, inbound poll and receipts |
| `NODE_TOPOLOGY_BASE_URL` | `http://localhost:8000/api/node-topology` | node profile with `inbound` limits, activation |
| `OFFER_AVAILABILITY_BASE_URL` | `http://localhost:8000/api/offer-availability` | catalogue item, pack levels, inventory feed |
| `NODE_PERFORMANCE_BASE_URL` | `http://localhost:8000/api/node-performance` | `GET /scorecards/{nodeId}` |

Tags: `@e2e @seller-inbound`. Every cross-context hop is Kafka, so the steps poll (up to 90 s) through real
behaviour (plan accepted, first carton accepted, poll returns the shipment); only the "not yet learned"
refusals are awaited, any other refusal fails at once. Requires the Kafka topic
`retailnetwork.seller-inbound.events` to exist before vendor-connectivity starts (see the PR notes).

## Phase 5 part A (`@phase5`, live cluster)

`features/phase5_a.feature` + `phase5_a_test.go`: six small black-box scenarios over the Phase 5 capabilities,
through Kong. Each creates its own run-scoped data; the shared-state limits are stated in the feature file.

| Tag | Context | Proves |
|---|---|---|
| `@seller-scorecard` | seller-inbound | `GET /sellers/{id}/scorecard`: a shipment closed with a short and a missing carton gives `receiptAccuracyRate` = RECEIVED / reported, `discrepanciesByKind`, the documented `complianceTier`, and the seller still plans/confirms (informational); empty shape without NaN, 404, `invalid-window` 400. Reuses the `seller_inbound` steps for the shipment flow |
| `@labor-standards` | labor-performance | `POST /standards` per handling unit: most-specific resolution with fallback, team lift only on CASE (`team-lift-requires-case-unit`), `unknown-handling-unit`, unit-less standard untouched. Restores the task-type-only PICK standard afterwards |
| `@process-paths` | process-path-management | FULL_CASE / CROSS_DOCK / BREAK_CASE declared as in `catalogue/paths.json` (applied like `seed_catalogue.py`: 201 or 409), their definitions, selection by the declared criteria, invalid definitions refused |
| `@pack-aware-estimate` | wes-work-planning + product-master | `POST /paths/{pathId}/charge` with `sku` + `pickUnit`: 36 eaches of a 12-per-case SKU are 3 handles with the case cube/weight; EACH is 36 handles; a plain bucket is unchanged |

Not exercisable through the public REST (not faked): process-path *step lists* (a ProcessPath has no steps) and a
"select the path for these criteria" endpoint (none exists; selection is asserted on the listing).

```sh
P=http://localhost:8000/api
SELLER_INBOUND_BASE_URL=$P/seller-inbound VENDOR_CONNECTIVITY_BASE_URL=$P/vendor-connectivity \
NODE_TOPOLOGY_BASE_URL=$P/node-topology OFFER_AVAILABILITY_BASE_URL=$P/offer-availability \
NODE_PERFORMANCE_BASE_URL=$P/node-performance PRODUCT_MASTER_BASE_URL=$P/product-master \
LABOR_PERFORMANCE_BASE_URL=$P/labor-performance PROCESS_PATH_BASE_URL=$P/process-path-management \
WORK_PLANNING_BASE_URL=$P/wes-work-planning GODOG_TAGS=@phase5 \

## Phase 5 part B: case-multiple transfers and ship-in-own-container (`@phase5`, live cluster)

`features/phase5_b.feature` + `phase5_b_test.go`, black-box REST through Kong, no database touched
(one injected Kafka fact in the approval scenarios, below). Valid GTIN-14s are computed per run (prefix `9502`), every id is run-scoped.

- **network-inventory-planning ADR 0017**: with an ACTIVE CASE of 12 in product-master, a
  proposal (`POST /v1/transfer-proposals:generate`) is rounded to the nearest whole case, a tie
  up, at least one case, capped at what the origin supplies; `caseMultiple`, `cases`,
  `requestedQtyEaches`, `roundedQtyEaches` and the signed `remainderEaches` are asserted exactly
  (25 -> 24 +1, 18 -> 24 -6, 17 -> 12 +5, 1 -> 12 -11, 24 -> 24 0, supply 40 -> 36); a plain SKU
  has no case fields; an origin below one case proposes nothing.
- **fulfillment-execution ADR 0056**: a PACK task (`POST /tasks`, claimed by a station with a
  private run-scoped capability) is sealed with `shipInOwnContainer` + `lineQuantity`:
  exactly 12 -> 201 with `containerSource: CASE`, the case GTIN and its declared
  dimensions/weight (also on `GET /packages/{id}`); 11, 13, 24 -> 422
  `ship-in-own-container-quantity-mismatch`; SKU without CASE -> 422
  `ship-in-own-container-no-case-level`; refusals seal nothing; no option = ordinary carton;
  option without `lineQuantity` = 400.
- **Approval, live (`POST /v1/transfers:approve` + `GET /v1/transfers/{id}`)**: two RUN-SCOPED sites per
  scenario (`P5O<run>` origin, `P5D<run>` destination; facility-layout site codes are uppercase
  letters and digits) with FRESH facts: transfer capability through facility-layout REST, the capacity
  plan through warehouse-planning REST (set `WAREHOUSE_PLANNING_BASE_URL`), the CASE of 12 through
  product-master REST. The ONE injected fact is `SiteSkuDemandChanged` (order-management projects demand
  to a single configured site, so no REST route states it for two arbitrary sites): the existing
  `inter_warehouse_transfer` step publishes the CloudEvent to `warehouse.order-management.events` on the
  kind broker (`KAFKA_BROKERS`, default `localhost:9092`). Asserted: 25 -> 24 (2 cases, +1) and 18 -> 24
  (-6) on the approval AND on GET, the transfer quantity is the rounded eaches, a replay with the same
  `Idempotency-Key` answers `replayed: true` for the same transfer, a plain SKU approves with no case
  fields, an origin that can send 11 eaches (availability is NIP's `OriginHeadroom` = published capacity
  over the window minus the origin's own in-window demand: 1 order/hour over a 12 h window, 1 unit of
  origin demand) is refused 422 `below-one-case`. **ADR 0018 (per-site staleness)**: a third site holds a
  demand fact stated 2 hours ago (the CloudEvent time is the watermark, limit 10 min) while the fresh pair
  is approved; an origin that is that stale site, or a site with no facts, is refused 422
  `facts-incomplete` naming it. After approval the real saga continues (inventory-storage does not know
  the run-scoped sites, so the transfer ends `UNFULFILLABLE`); the assertions are on the case fields only.

```sh
P=http://localhost:8000/api
PRODUCT_MASTER_BASE_URL=$P/product-master FACILITY_BASE_URL=$P/facility-layout \
NETWORK_INVENTORY_PLANNING_BASE_URL=$P/network-inventory-planning \
NIP_BASE_URL=$P/network-inventory-planning WAREHOUSE_PLANNING_BASE_URL=$P/warehouse-planning \
FULFILLMENT_BASE_URL=$P/fulfillment-execution KAFKA_BROKERS=localhost:9092 GODOG_TAGS=@phase5 \
  go test -count=1 -v -run TestMain -timeout 15m .
```

| Env var | Default | Context |
|---|---|---|
| `LABOR_PERFORMANCE_BASE_URL` | `http://localhost:8000/api/labor-performance` | `/standards` (the older `LABOR_BASE_URL` serves the local-harness scenarios) |
| `PROCESS_PATH_BASE_URL` | `http://localhost:8000/api/process-path-management` here (the harness default is `:8087`) | `/process-paths` |
| `WORK_PLANNING_BASE_URL` | `http://localhost:8000/api/wes-work-planning` | `POST /paths/{pathId}/charge` |
| `PRODUCT_MASTER_BASE_URL` | the existing variable | pack levels with dims |
| `NETWORK_INVENTORY_PLANNING_BASE_URL` | `NIP_BASE_URL` | NIP approval, GET transfer, proposals (the approval steps also use `NIP_BASE_URL` and `WAREHOUSE_PLANNING_BASE_URL`) |
| `KAFKA_BROKERS` | `localhost:9092` | the kind broker the injected `SiteSkuDemandChanged` is published to |

Plus the seller-inbound variables above. Tags: `@e2e @phase5` plus the one in the table.

| `NETWORK_INVENTORY_PLANNING_BASE_URL` | `NIP_BASE_URL` (`http://localhost:8099`) | proposals, approval, transfers |
| `FULFILLMENT_BASE_URL` | `http://localhost:8084` | tasks, stations, seal-package, packages |
| `PRODUCT_MASTER_BASE_URL`, `FACILITY_BASE_URL` | as elsewhere | pack levels; site transfer capability |

Both pack levels reach NIP and fulfillment-execution over Kafka into local views that have no REST
read, so the "eventually" steps probe the behaviour (a proposal carrying `caseMultiple`; a refused
own-container seal that no longer answers `no-case-level`).

## Phase 5: case-pick faces and the CASE pick unit (`@phase5 @case-pick`, live cluster)

`features/phase5_case_pick.feature` + `phase5_case_pick_test.go`: one black-box scenario for slotting-optimization
ADR 0012, REST through Kong plus the injected demand and the `SlotPlanApproved` read, on a RUN-SCOPED site
(`CP<run>`). The cluster's slotting-optimization must run with `CASE_PICK_ZONE_CODES=CPK` and `PACK_LEVEL_MODE=kafka`
(it does in the kind cluster); facility-layout has no "behavioural class" field apart from the zone code, so the
case-pick zone is a zone registered with `zoneCode: CPK` and the each-pick zone one with `zoneCode: FWD`; a slot's
volume/weight limits are `capacityOverride` on `POST /locations`. facility-layout's placement rules allow only
CartonFlow, Shelf and ToteWall in a FWD zone, so every slot is a `Shelf`.

- Layout: two CPK faces (40 kg / 0.2 m3 = 8 cases by volume, 6 by weight; 100 kg / 0.1 m3 = 4 by volume, 15 by
  weight) and two FWD faces. A case is 400x300x200 mm (0.024 m3), 6500 g, 12 eaches.
- SKUs: `CASE` (demand lines 24 + 36, gcd 12) and `CASEB` (one line of 24) with a CASE level, `MIXED` (CASE level, lines
  5 + 7, gcd 1) and `EACH` (no CASE level, lines 5 + 7).
- Asserted on the generated Draft (POST and GET by id): `CASE` and `CASEB` are `pickUnit: CASE` on the CPK faces with
  `casesCapacity` 6 and 4 (also recomputed as floor(min(volume, weight)) from the declared limits), `MIXED` and `EACH`
  are EACH (absent or `EACH`, no `casesCapacity`) on FWD faces, nothing unassigned, one Assign move per SKU; after
  approval `GET /forward-slots?siteId=` and the `SlotPlanApproved` event (`pick_unit`/`cases_capacity` on the CASE
  assignments only) carry the same.
- Slotting builds four local copies from Kafka (layout, product profiles, pack levels, demand) and only the demand one
  is readable over REST, so the plan is drafted until it matches (up to 2 minutes), rejecting each incomplete Draft.

```sh
P=http://localhost:8000/api
PRODUCT_MASTER_BASE_URL=$P/product-master FACILITY_BASE_URL=$P/facility-layout \
SLOTTING_BASE_URL=$P/slotting-optimization KAFKA_BROKERS=localhost:9092 GODOG_TAGS=@case-pick \
  go test -count=1 -v -run TestMain -timeout 10m .
```

No new environment variable: `SLOTTING_BASE_URL`, `FACILITY_BASE_URL`, `PRODUCT_MASTER_BASE_URL` and `KAFKA_BROKERS` are the
existing ones (`SLOTTING_BASE_URL` defaults to `http://localhost:8109`, so through Kong it must be set as above).

## Real product catalogue (`cmd/seed-products`)

Registers **at least 10,000 real products** in product-master through its
published REST contract, **with their fulfilment data** (real unit dimensions and
weight, handling tags, temperature class, DOT hazard class), then reads the whole
catalogue back with `GET /products` and fails on any missing or different product.

```sh
make seed-products                                                    # local harness (:8090)
PRODUCT_MASTER_BASE_URL=http://localhost:8000/api/product-master \
  bash scripts/08-seed-products.sh                                    # kind cluster via Kong
SEED_LIMIT=2000 SEED_WORKERS=16 make seed-products                    # tunables
```

- **Data (two fixtures, see `fixtures/products/README.md`):**
  `openfoodfacts-products.json` - ~2,370 grocery products from
  [Open Food Facts](https://world.openfoodfacts.org) (ODbL 1.0), SKU = GS1 GTIN,
  and `fulfilment-products.json` - ~11,300 products with **real unit dimensions
  and weight** from the Berkeley ABO listings (CC BY 4.0) and the Wayfair WANDS
  catalogue (MIT), harvested by `scripts/harvest-fulfilment-products.py`.
- **API calls:** `PUT /products/{sku}` for every product,
  `PUT /products/{sku}/dimensions/declared` where the source has dimensions and
  weight, and `PUT /products/{sku}/classification` where a handling tag applies
  (Hazmat + DOT class, Fragile, TemperatureSensitive + Frozen/Chilled, Oversized,
  HighValue). The tags are *derived* from the category and name by explicit rules
  and every product records its evidence; dimensions and weight are the source's
  own, converted to mm and g, and implausible ones are dropped, not repaired.
- **Idempotent:** every write is an idempotent PUT, so a re-run against a dirty
  database reports the products as already present and changes nothing (no
  version bump, no event). The GTINs are shared reference data and are not
  run-scoped.
- **Offline:** the run never touches the internet. Refresh the Open Food Facts
  fixture with `go run ./cmd/seed-products -fetch` (slow on purpose: about 10
  searches a minute, 503 when busy; paced and retried) and the fulfilment fixture
  with `python3 scripts/harvest-fulfilment-products.py` (downloads ~180 MB once,
  cached under `~/.cache/warehouse-e2e`; deterministic).
- **Tests:** `go test ./cmd/seed-products/...` pushes the whole committed
  catalogue (twice, to prove idempotence) through the real client against a
  contract fake of product-master and verifies the read-back;
  `python3 scripts/test_harvest_fulfilment_products.py` covers the derivation rules.

## Inventory (`cmd/seed-inventory`)

Stocks inventory-storage with **at least 100,000 units** of the products that are *registered*
in product-master, through the services' published REST APIs only, then reads every bin back.

```sh
bash scripts/09-seed-inventory.sh --dry-run                         # check + plan, write nothing
PRODUCT_MASTER_BASE_URL=http://localhost:8000/api/product-master \
FACILITY_LAYOUT_BASE_URL=http://localhost:8000/api/facility-layout \
INVENTORY_STORAGE_BASE_URL=http://localhost:8000/api/inventory-storage \
  bash scripts/09-seed-inventory.sh                                 # kind cluster via Kong
```

- **Check:** lists every registered product (`GET /products`: dimensions, handling tags,
  temperature class, DOT class) and every Active storage slot of the pilot sites in
  facility-layout (zone temperature and hazmat rating, real volume and weight limits). A registered
  product with no dimensions that is not a GTIN grocery is reported and skipped.
- **Plan** (deterministic, `plan.go`): sortable FCs stock small and medium items, non-sortable and XL
  FCs the oversized, fresh-grocery and micro-FCs the groceries and the whole cold chain (chilled and
  frozen zones). Hazmat goes only to hazmat-rated zones, one SKU per bin (same-bin DOT segregation,
  inventory-storage ADR 0010). Quantities are case-pack scale by unit volume; bins are filled to 80 %
  of their volume and weight limits, chaotic storage (no fixed product-to-bin mapping); a bin's
  capacity in units is the placed units divided by the fill factor. Groceries without dimensions use an
  assumed 1.5 L / 0.6 kg unit for the fill arithmetic only; nothing is written back to product-master.
- **Stock:** `PUT /bins/{binId}` then `POST /stock/stow` (item scan + location scan). It does not call
  `/stock/receive`: stow does not consume staged receipts, so receiving as well would only inflate the
  staged quantity.
- **Idempotent:** a bin already holding its planned units is skipped, `PUT /bins` converges, and every
  stow carries a deterministic `Idempotency-Key` (bin, SKU, quantity), so a retry or an interrupted run
  does not stow twice.
- **Tests:** `go test ./cmd/seed-inventory/...` runs the planner rules and the whole stocking path
  against a contract fake of inventory-storage that enforces its invariants (capacity, hazmat zone,
  temperature class, DOT segregation, idempotency keys).

## Storefront (`make provision-storefront`)

Provisions everything a shopper needs to buy a product in the storefront
(<http://storefront.localhost>) and see its delivery promise, on the kind cluster through Kong, and
verifies it; a second run writes nothing. **The register of what is provisioned, by whom, why, and
how to rerun or rebuild: [docs/storefront-provisioning.md](docs/storefront-provisioning.md).**

```sh
make provision-storefront-dry-run   # preflight + network plan + dictionary check + storefront plan, writes nothing
make provision-storefront           # scripts/10-provision-storefront.sh: provisions, then verifies
make provision-storefront-verify    # read-only verification
```

- **Order:** preflight (Kong, the sibling `../warehouse-infra` checkout, one read per layer, WMS
  usable stock of the 81 storefront SKUs; the WMS seeders are NOT re-run) -> warehouse-infra's
  `seed-retail-network.py` (network tier; waves are dated, so it runs every time) -> its
  `--check-translation` of network-fulfillment's product dictionary -> `go run ./cmd/seed-storefront`.
- **`cmd/seed-storefront`:** CHECK (fixture vs real data, SKU -> network title -> catalogue item,
  fails before any write when one is missing) -> PLAN (pure diff, printed per kind) -> APPLY (only
  the diffs: tax table, delivery charges, categories, prices, content create/revise/publish; every
  POST under a deterministic `Idempotency-Key`) -> VERIFY (all products purchasable, a delivery quote
  for prefix 787 through the BFF, a search hit per department). Content is derived from real data
  only (title, real size and weight, handling tags, department); department, subcategory and price
  are curated in `fixtures/storefront/catalogue.json`.
- **Tests:** `go test ./cmd/seed-storefront/...` (also `make seed-tests`) seeds the whole fixture
  twice against a contract fake of offer-availability, pricing, product-content, product-discovery
  and the BFF that enforces their invariants (second run: zero writes), plus an interrupted run, a
  lost response, a missing catalogue title, WITHDRAWN/RETIRED handling and the content rules.

## Service lifecycle notes

- `scripts/lib.sh`'s `start_service_in`/`start_service` launch each binary
  in the background and record its PID to `run/pids/<name>.pid`;
  `stop_service` (`scripts/05-down-services.sh`) kills strictly by that
  PID file — never a blind `pkill` — so re-running the harness never
  touches an unrelated process on your machine.
- Known pitfall (fixed): backgrounding a `cd ... && ...` list forces an
  extra subshell fork, and on some bash builds `$!` taken right after
  resolves to the *caller's* PID rather than the child's, so the recorded
  PID could be wrong. `start_service_in` now backgrounds a single `( cd
  ... && exec ... )` subshell with no nested `&&` list, so `$!` is always
  the actual binary's PID.
- `fulfillment-execution`'s `main()` hardcodes a relative `migrations`
  path, so it (and its MCP server) are the only two services launched via
  `start_service_in` with their CWD set to that repo's root; every other
  service reads `MIGRATIONS_PATH` from the environment and runs fine from
  this harness's own CWD.
- EVERY service self-migrates on startup via `postgres.RunMigrations` /
  `postgres.Migrate` inside its own `main()`, so no separate migrate step
  is needed for any of them. `wes-work-planning` used to be the sole
  exception and needed a `02b-migrate-wes.sh` workaround; it now migrates
  itself like the rest (wes-work-planning PR #52), and that script has
  been removed.
- `KAFKA_BROKERS` (default `localhost:9092`, the warehouse-infra kind
  cluster's broker) and `REPOS_ROOT` are overridable. Run the transfer
  feature against a throwaway broker, not the shared cluster: the saga
  publishes AND consumes commands (`TransferAllocationRequested`) on shared
  topics, so a live in-cluster inventory-storage could answer the same
  command with a different reservation. E.g.
  `docker run -d -p 29092:29092 -e KAFKA_NODE_ID=1 ... apache/kafka:3.8.0`
  (the CI job's single-node KRaft config with the listener on `:29092`) and
  `export KAFKA_BROKERS=localhost:29092` before the `scripts/*.sh`. Create
  the topics up front (CI does) so no first publish races topic creation.
- `fixtures/process-paths/sortable-fc.yaml` is this harness's own copy of
  warehouse-infra's process-path catalogue plus a `DISPATCH` family
  (`dispatch-*`), which the transfer saga's dispatch leg needs
  (fulfillment-execution ADR-0036) and warehouse-infra's frozen file lacks.
  NIP's pick leg is `pick-transfer` (inside the existing `PICK` family) and
  its dispatch leg `dispatch-transfer`.
- `PLANNING_MAX_STALENESS` for NIP is `720h` here (`NIP_PLANNING_MAX_STALENESS`):
  NIP refuses to plan from ANY stale fact in its read models, and this
  harness's Postgres volumes persist across runs.
- `NIP_TRANSFER_READ_MODE=rest` switches the transfer feature's state
  assertion from NIP's own table (default, `db`) to `GET /v1/transfers/{id}`
  once that endpoint is on NIP's develop.

## Tests

- `make seed-tests` — `go test ./cmd/seed-products/... ./cmd/seed-inventory/... ./cmd/seed-storefront/...`:
  the seeders against contract fakes of their siblings that enforce the real
  invariants (capacity, hazmat, DOT segregation, idempotency keys; a second
  seed run writes nothing, an interrupted run and a lost response are covered).
- `make harness-test` — unit tests for the agent harness hooks
  (`scripts/harness/test_hook.py`); `make test-compile` compiles the suite.
- `make scripts-sanity` — bash syntax check plus invariants over every
  `scripts/*.sh`.
- `make soak` — the sustained backlog-ramp soak (see above).

In `.github/workflows/ci.yml` these run as the `build-and-vet`,
`seed-products-tests`, `scripts-sanity` and `guide-lint` jobs; the godog
suite itself is not a CI job (see CI below).

## CI

`.github/workflows/ci.yml` runs `gofmt`, `go build`/`go vet`, a shell
syntax check on every `scripts/*.sh`, and `docker compose config`
validation. The full godog suite is NOT run in CI: it is a genuinely
multi-repo black-box harness (it builds and runs binaries from fourteen
sibling bounded-context repos, product-master, inbound-receiving and
slotting-optimization included, plus
`warehouse-ops-agent`, none of which are checked out in
a single-repo GitHub Actions run) — it is run and verified locally as part
of every change that touches it, the same pattern `e2s-tests`' equivalent
harness follows.

`.github/workflows/e2e-behaviour.yml` (weekly + manual dispatch) is the
exception, with five jobs. The first checks out the 7 core repos
(facility-layout, product-master, inventory-storage, wes-work-planning,
fulfillment-execution, workforce-management, order-management), starts them
against a throwaway Kafka and per-service Postgres, and runs
`@bootstrap,@product-master`. The second (`@inter-warehouse-transfer`) adds
network-inventory-planning and warehouse-planning (9 services in all) and
pre-creates the topics they exchange. The third (`@inbound-receiving`) needs
only product-master, inbound-receiving and inventory-storage (with its
`INBOUND_RECEIPT_CONSUMER_GROUP`) and four pre-created topics. The fourth
(`@slotting`) needs only facility-layout, product-master and
slotting-optimization (all three consumers in `kafka` mode) and four
pre-created topics. The fifth (`@returns-exceptions`) needs only
returns-processing, exception-management and inventory-storage (with its
`RETURN_LINE_CONSUMER_GROUP`); the feature injects the upstream facts
itself, so no order-management, fulfillment-execution or product-master
checkout is needed.

### Order fulfillment site (`@order-site`)

`features/phase5_order_site.feature` proves order-management ADR 0044 through the real
`POST /orders` and the real `warehouse.order-management.events` topic: an order that names a
`fulfillmentSiteId` projects its `SiteSkuDemandChanged` to THAT site (`order-site-v1`), an order
that names none goes to the static site (`static-site-v1`), a malformed site is refused with
`400 invalid-fulfillment-site-id`, and `GET /orders/{id}` returns the site.

- Demand is emitted only for lines that **allocate** (order-management ADR 0035), so the scenario
  first puts 10 loose units into a seeded forward bin (`ABE8`, the same helper as `@mastercase`).
- No new environment variables: it uses the existing `ORDER_BASE_URL`, `INVENTORY_BASE_URL`,
  `FACILITY_BASE_URL` and `KAFKA_BROKERS`.
- If every plain order answers `422 line-ineligible-for-resolved-path`, order-management's path
  cache (rebuilt from the process-path-management events topic on every start) has lost a path
  definition, usually because the topic's retention expired it. Revising the path (PUT
  `/process-paths/{id}` with a real change, then back) republishes it.

### Running the whole suite against the kind cluster

Every base URL has a per-service default (`localhost:81xx`) that only exists when each service runs
standalone. Against the kind cluster set them all to the Kong routes, including the ones that are
easy to forget: `RETURNS_BASE_URL=$P/returns-processing` (the returns scenarios otherwise fail on a
refused connection to `localhost:8110`) and `INBOUND_BASE_URL`, `EXCEPTIONS_BASE_URL`,
`SLOTTING_BASE_URL`, `NIP_BASE_URL`. `INVENTORY_DB_URL` and the other `*_DB_URL` defaults point at
standalone containers, not at the cluster's databases; scenarios must reach the service through its
REST API wherever it offers one (bins are registered with `PUT /bins/{binId}`), because a direct SQL
write lands in a database the running service never reads.

## Documentation

The harness scenarios are documented in warehouse-docs (fleet-operations
pages, the e2e section) and in the feature files themselves
(<https://iqvo.github.io/warehouse-docs/>).

## Architecture

A Go/godog BDD harness over the fleet's real deployments: `cmd/warehouse-day`
simulates an operating day, `cmd/seed-products`/`cmd/seed-inventory` seed real
catalogues, `cmd/ensure-topics` provisions Kafka topics; features run against
docker compose locally or the kind cluster live.

## Local development / quality gate

`make check-fast` (fmt + vet + guide-lint), `make check` adds the
compose-config and scripts sanity gates; CI jobs: `build-and-vet`,
`seed-products-tests`, `scripts-sanity`, `guide-lint`.

## License

MIT — see [LICENSE](LICENSE).
