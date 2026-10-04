---
paths:
  - "features/**"
  - "e2e_test.go"
  - "soak_test.go"
  - "cmd/warehouse-day/**"
---

# Scenario and step rules (the suite is idempotent by design)

Verified: three consecutive full runs, no reset between them. Keep it that way. Step-by-step how-to: skill `adding-e2e-scenario-or-service`; assertion design: skill `discriminating-eventual-assertion`.

- **Run-scope mutable entities** (bins, SKUs, stations, associates, work units, order refs, process paths) with `<run>` in the feature file. `world.rs()` in `e2e_test.go` expands it per process. Every step taking such an id MUST call `w.rs(id)`; otherwise the literal token reaches the service (404/409 quoting `%3Crun%3E` or `<run>`).
- **Do not scope shared reference data** (sites, zones, aisles, location types, catalogue ids like `pick-zone-a`): use the idempotent `... exists in facility-layout` steps. `pick-t5-imbalance` cannot be scoped; `env.sh` pins it in `OPS_AGENT_PATH_TARGETS`.
- **Accumulation is silent**: a fixed SKU receiving 20 units per run reads 40 on run 2. Scope the id; never relax an assertion to `>=`.
- **No aggregate queue depth as a proxy** for "my task arrived" (scenarios share the PICK queue). Use `GET /tasks?orderRef=`.
- **`claim-next` is pull dispatch** (earliest CPT wins, caller cannot choose): use `claimNextTaskForOrder`, which re-claims until it gets its own task. Same for release: `releaseWorkEventuallyReleasesOrderLine`.
- **No table purges in setup steps.** Purging pending tasks in `registerStation` deleted the task `flow_balance_exception.feature` was about to await.
- **Eventual assertions** use `eventually(...)` (30s / 500ms). A bounded wait that expires reads like a service bug; first suspect run-scoping, a shared queue or a consumer group (see `.claude/rules/harness-infra-and-ci.md`).
- **Creating POSTs need an `Idempotency-Key` header** (new fleet contract, 2026-10-03). Fresh unique key per logical action; reuse only to retry the identical request; same key with a different body is 422, a missing key is 400. `cmd/warehouse-day/api.go` already sets one per POST; `doJSON`/`soakPostJSON` get it in the separate PR #28, so do not patch them from an unrelated branch. Put the header in the shared helper, never per step.
- `world.last` is not goroutine-safe: concurrent code (soak) must use its own request helper (`soakPostJSON`).
- Forcing a condition no endpoint offers: direct-seed the owning service's Postgres via `dbOpen`, mirroring `claimedTaskLeaseForcedExpired`, then trigger the consumer through its real REST endpoint.
