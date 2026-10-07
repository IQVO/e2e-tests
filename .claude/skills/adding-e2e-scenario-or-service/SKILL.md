---
name: adding-e2e-scenario-or-service
description: How to add an idempotent godog scenario or step to the e2e suite (run-scoped ids, shared reference data, pull-dispatch claim helpers, Idempotency-Key on creating POSTs), and how to register a new bounded-context service in the harness (env.sh, docker-compose, scripts 01-05). Use when editing features/*.feature, e2e_test.go or soak_test.go, or when a new service must be built/started by the harness.
---

# Adding a scenario, a step, or a service

The suite must re-run cleanly against a DIRTY database (no reset between runs). Every rule below exists to keep that true.

## 1. Add a scenario (`features/<name>.feature`)

1. Copy the shape of `features/promise_repromise_loop.feature` (one `Background:` calling `all warehouse-systems services are healthy`, one scenario tagged `@e2e @<topic>`). The tag line is what `GODOG_TAGS` selects on; check for collisions first: `grep -n '^  @' features/*.feature`.
2. **Run-scope every MUTABLE entity** with the `<run>` token: bins, SKUs, stations, associates, work units, order refs, process paths (`E2E-BIN-<run>`, `SKU-E2E-<run>`, `station-e2e-<run>` as in `features/bootstrap.feature`).
3. **Never scope shared reference data**: sites, zones, aisles, location types, catalogue path ids such as `pick-zone-a`. Use the idempotent `... exists in facility-layout` steps (`ensureSite`, `ensureAisle`, ... in `e2e_test.go`). `pick-t5-imbalance` cannot be scoped: `env.sh` pins it in `OPS_AGENT_PATH_TARGETS`.
4. Keep assertions EXACT. If a number is wrong on run 2 (a fixed SKU reads 40 instead of 20) the id was not scoped; fix the id, never relax to `>=`.
5. Never assert an aggregate queue depth as a proxy for "my task arrived" (scenarios share the PICK queue); query `GET /tasks?orderRef=` instead.
6. `claim-next` is PULL dispatch (earliest CPT wins, caller cannot pick). You WILL claim an older scenario's leftover. Use the `claimNextTaskForOrder` step (it re-claims inside `eventually` until it gets its own task); the same idea for release is `releaseWorkEventuallyReleasesOrderLine`.
7. Do NOT purge tables in a setup step to "clean" interference. A purge inside `registerStation` deleted the task `flow_balance_exception.feature` was about to await (it releases work BEFORE registering stations).

## 2. Add a step (`e2e_test.go`)

- Write a `func (w *world) yourStep(...) error` and register it in `InitializeScenario` with `sc.Step(...)`. A step that is not registered makes godog `Strict: true` fail the whole run.
- Any parameter that is an id from a feature file MUST go through `w.rs(id)` first. If you forget, the literal `<run>` (or `%3Crun%3E`) reaches the service and you get a 404/409 quoting it.
- Use `w.doJSON` plus `w.expectOK2xx`; wrap anything that depends on Kafka delivery in `eventually(func() error {...})` (30s timeout, 500ms poll; constants `eventualWaitTimeout`/`eventualWaitPoll`).
- Direct-DB seeding (to force a condition no endpoint offers, e.g. back-dating a CPT or lease) goes through `dbOpen(dsn, password)`, one DSN+password pair per service, copying `theTaskForOrderLineHasCPTForcedIntoThePast`. Prefer a real REST trigger for the consumer afterwards (`POST /tasks/sweep-cpt-misses`) over inventing a backdoor endpoint.
- `world.last` is not goroutine-safe. Concurrent code (see `soak_test.go`, `soakPostJSON`) must not use `doJSON`.

## 3. Creating POSTs need an `Idempotency-Key` header (rule, 2026-10-03)

Fleet services now reject a creating `POST` (resource-creating routes such as place order, receive stock, enqueue work unit) with 400 when the `Idempotency-Key` header is missing. Same key + same body replays the cached response; same key + a DIFFERENT body is 422.

- Send a fresh unique key per logical action, never a constant, never reused across scenarios or `<run>`s. Reuse a key only when deliberately retrying the identical request.
- `cmd/warehouse-day/api.go` already does this (a new key per POST). `w.doJSON` and `soakPostJSON` get the same treatment in a separate PR (#28): do not change them from an unrelated branch, and when you write a new helper that issues POSTs, set the header in the helper, not per step.
- Symptom of a missing key: `expected 2xx, got 400` on a step that worked before the provider service upgraded.

## 4. Register a new service in the harness

Work from the previous service's commit as a template (`git log -S network -- env.sh`). Touch, in order:

1. `env.sh`: `<NAME>_REPO` (`${REPOS_ROOT}/<repo>`), `<NAME>_HTTP_PORT` (next free after 8090, and clear of the 8091-8098 MCP and 81xx reports ports), `<NAME>_BASE_URL`, `<NAME>_DB_URL` (next free after 5450; DSN carries NO password; user/db from that service's own compose file). If it consumes Kafka, add `<NAME>_CONSUMER_GROUP="<svc>-${E2E_CONSUMER_GROUP_SUFFIX}"` next to `WES_CONSUMER_GROUP`. Never a fixed group: it would join the live cluster's group and starve the local process of partitions (symptom: `condition not met within 30s`; confirm with `kafka-consumer-groups.sh --describe`).
2. `docker-compose.yml`: a `postgres-<name>` block (copy `postgres-network`), its port and volume.
3. `scripts/01-build.sh`: `build_one <name> "${<NAME>_REPO}" <cmd-pkg>`. Look at the target's `cmd/` first: the dir is not always the repo name (`pathmgmt`, `netfulfil`). Update the binary-count log lines.
4. `scripts/03-up-services.sh`: a `start_service <name> ...` block plus `wait_for_http "${<NAME>_BASE_URL}/healthz"`, placed in dependency order; update the header list and counts. Read `cmd/<svc>/main.go` for the env vars it REALLY reads (a pure consumer has no `EVENT_PUBLISHER`); do not paste the previous block blindly. Declarative test data goes in `fixtures/<name>/` wired by ABSOLUTE path through an `env.sh` var, and confirm it loaded by grepping the startup log (a wrong path silently seeds nothing).
5. `scripts/05-down-services.sh`: add the name to the `stop_service` loop. Update `scripts/02-up-infra.sh` and `README.md` counts (they drift; grep for the old number).
6. `e2e_test.go`: base-URL var via `envOrDefault`, a health check in `allServicesAreHealthy` only if every scenario needs it (otherwise a separate Background step, like `opsAgentIsHealthy`).
7. CI: `.github/workflows/e2e-behaviour.yml` builds and starts only the 7 core services (incl. product-master) for `@bootstrap,@product-master`. Add a service there only if a CI-run scenario needs it (checkout step, Postgres in `compose up`, `build_one`, start block, topics).

Verify by running the real build+up+suite from the real checkout (`~/warehouse-systems/e2e-tests`), not a worktree: see the `running-e2e-suite-local-vs-ci` skill. Finish with `make check-fast`.
