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
`network-fulfillment`, `network-inventory-planning`, `warehouse-planning`)
plus the read-side decision-support agent `warehouse-ops-agent`, all siblings
under the same `warehouse-systems/` workspace root — see `env.sh`'s
`REPOS_ROOT` for the layout this harness assumes (it is overridable:
`REPOS_ROOT=/path/to/repos`, so a git worktree of this repo can build the
siblings).

## Services the harness runs

Ports and DSNs live in `env.sh` only; this table mirrors it.

| Service | Binary (`cmd/`) | HTTP | Postgres | Classification role |
|---|---|---|---|---|
| process-path-management | `pathmgmt` | 8087 | 5447 | — |
| facility-layout | `facility` | 8081 | 5441 | — |
| **product-master** | `api` | 8090 | 5450 | **owner**: `PUT /products/{sku}` + `PUT /products/{sku}/classification`, physical profile; publishes `warehouse.product-master.events` |
| inventory-storage | `inventory` | 8082 | 5442 | local copy (`PRODUCT_MASTER_CONSUMER_GROUP`); its classification `PUT` answers `410 classification-moved` |
| wes-work-planning | `wes` | 8083 | 5443 | local copy (`PRODUCT_CLASSIFICATION_MODE=kafka`) |
| fulfillment-execution | `execution` | 8084 | 5444 | local copy (`PRODUCT_CLASSIFICATION_MODE=kafka`) |
| labor-performance | `labor` | 8088 | 5448 | — |
| workforce-management | `workforce` | 8085 | 5445 | — |
| order-management | `order` | 8086 | 5446 | local copy (`PRODUCT_CLASSIFICATION_MODE=kafka`) |
| network-fulfillment | `netfulfil` | 8089 | 5449 | — |
| network-inventory-planning | `network-inventory-planning` | 8099 | 5451 | — |
| warehouse-planning | `api` | 8100 | 5452 | — |
| warehouse-ops-agent | `agent` | 8096 | — | — |

Every local copy uses a per-run consumer group derived from
`E2E_CONSUMER_GROUP_SUFFIX` (never the cluster's group).
`PRODUCT_CLASSIFICATION_MODE=http` no longer exists: the three readers
refuse to boot with it.

## What's covered

11 feature files (the inter-warehouse transfer feature has two scenarios):

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
bash scripts/02-up-infra.sh      # Postgres (12 instances, this repo) + shared Kafka; pre-creates every topic (cmd/ensure-topics) so a fresh broker cannot lose the first publish or leave a consumer group unassigned
bash scripts/01-build.sh         # builds all 20 binaries (12 HTTP incl. product-master, network-inventory-planning, warehouse-planning + 7 MCP + ops-agent)
bash scripts/03-up-services.sh   # starts them as background processes (product-master before inventory-storage; NIP last)
bash scripts/04-run-tests.sh     # runs the default godog suite (excludes @soak)
GODOG_TAGS=@product-master bash scripts/04-run-tests.sh  # just the product-master scenario
bash scripts/06-run-soak.sh      # OPTIONAL: the long-running @soak backlog-ramp run (see below)
bash scripts/05-down-services.sh # stops only what this harness started
```

`env.sh` is the single source of truth for every port, DB URL, and MCP
bearer key `scripts/*.sh` uses — edit ports there only.

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
| 05:30 | receiving | inventory-storage `POST /stock/receive` + `POST /stock/stow` into registered bins |
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

## CI

`.github/workflows/ci.yml` runs `gofmt`, `go build`/`go vet`, a shell
syntax check on every `scripts/*.sh`, and `docker compose config`
validation. The full godog suite is NOT run in CI: it is a genuinely
multi-repo black-box harness (it builds and runs binaries from six
sibling repos plus `warehouse-ops-agent`, none of which are checked out in
a single-repo GitHub Actions run) — it is run and verified locally as part
of every change that touches it, the same pattern `e2s-tests`' equivalent
harness follows.

`.github/workflows/e2e-behaviour.yml` (weekly + manual dispatch) is the
exception, with two jobs. The first checks out the 7 core repos
(facility-layout, product-master, inventory-storage, wes-work-planning,
fulfillment-execution, workforce-management, order-management), starts them
against a throwaway Kafka and per-service Postgres, and runs
`@bootstrap,@product-master`. The second (`@inter-warehouse-transfer`) adds
network-inventory-planning and warehouse-planning (9 services in all) and
pre-creates the topics they exchange.
