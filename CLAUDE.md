# Project: e2e-tests (warehouse-e2e)

Black-box, over-the-wire end-to-end / system test harness for the
`warehouse-systems` fleet: builds and runs every bounded context's real
HTTP service (and MCP server) as independent OS processes against real
Postgres + the shared Kafka broker, then drives them purely over their
published REST APIs with a godog (Cucumber for Go) suite. It never
imports another repo's Go packages — exactly like a human running curl
against a live deployment.

> **Study project.** Personal DDD/hexagonal-architecture learning
> exercise. Not a production system; no uptime or support guarantee.

## Layout and assumptions

```
features/*.feature      Gherkin scenarios (bootstrap, flow_balance_exception,
                         order_management_choreographed_release, @soak)
e2e_test.go              godog TestMain / step wiring
soak_test.go              @soak scenario driver
env.sh                    ALL config: repo paths, ports, per-run Kafka
                           consumer group suffixes, seed data
scripts/01-build.sh       builds every sibling repo's binaries
scripts/02-up-infra.sh    Postgres (this repo) + shared Kafka broker
scripts/03-up-services.sh starts every service + MCP server + ops-agent
scripts/04-run-tests.sh   THE way to run the suite (see below — never a
                           bare `go test`)
scripts/05-down-services.sh
scripts/06-run-soak.sh
```

`env.sh`'s `REPOS_ROOT` assumes every sibling bounded-context repo is
checked out alongside this one under the same parent directory
(`~/warehouse-systems/<repo>`, same layout `git worktree` mirrors under
`.worktrees/`). Nothing here auto-discovers repos elsewhere.

## Non-negotiables (read before touching a scenario or step definition)

1. **Always run via `scripts/04-run-tests.sh`, never a bare `go test`.**
   The script exports every base URL and DB DSN `env.sh` defines; without
   them the DB-seeding steps fail with `failed SASL auth`, not a clearer
   error. `TestMain` reads scenario tags from the `GODOG_TAGS` env var —
   the `-godog.tags` CLI flag is NOT wired — so `GODOG_TAGS=@x bash
   scripts/04-run-tests.sh` is the way to run one scenario.

2. **The suite is idempotent by design — keep it that way.** It re-runs
   cleanly against a DIRTY database (verified: three consecutive full runs,
   6 scenarios / 102 steps, no reset between them). When adding a scenario:
   - Give every run-scope MUTABLE entity (bins, SKUs, stations, associates,
     work units, order refs) the `<run>` token in the feature file —
     `world.rs()` expands it per process. A new step definition taking such
     an id must call `w.rs(id)` or the token reaches the service literally
     (symptom: a 404/409 quoting a literal `%3Crun%3E` or `<run>`).
   - Do NOT scope shared reference data (sites, zones, aisles, location
     types, catalogue path ids like `pick-zone-a`) — every scenario should
     agree on the fleet's real configuration. Use the idempotent `"...
     exists in facility-layout"` steps for those.
     `pick-t5-imbalance` especially cannot be scoped — `env.sh` pins it in
     `OPS_AGENT_PATH_TARGETS`.
   - Watch for silent ACCUMULATION, not just collisions: a fixed SKU
     receiving 20 units per run reads "40" on the second run if unscoped.
     Scope the id; never relax an assertion to `>=` to paper over this.
   - Never assert on an aggregate queue depth as a proxy for "my task
     arrived" — several scenarios share the PICK queue. Use
     `GET /tasks?orderRef=` instead.
   - `claim-next` is PULL dispatch by design (earliest-CPT wins; the
     caller cannot request a specific task) — a scenario WILL claim an
     older scenario's leftover task. Use the `claimNextTaskForOrder`
     helper, which re-claims until it gets its own.
   - Don't "fix" cross-scenario interference by purging tables in a setup
     step. Purging pending tasks inside `registerStation` looked correct
     and broke `flow_balance_exception`, which releases work BEFORE
     registering its stations — the purge deleted the very task it was
     about to await.

3. **Never share a Kafka consumer group with the live cluster.** This
   fleet's Kafka is ONE broker platform-wide; running this harness's local
   `bin/wes`/`bin/execution`/`bin/labor` processes against the same broker
   the in-cluster Deployments use makes them join the EXACT SAME consumer
   group unless the group id is per-run-unique. `env.sh` derives
   `E2E_CONSUMER_GROUP_SUFFIX="e2e-$$-$(date +%s)"` and passes a unique
   `<SERVICE>_CONSUMER_GROUP` to every Kafka-consuming service this harness
   starts — if adding a new Kafka-consuming service to this harness, wire
   its own unique-suffixed group the same way, never a fixed string. Symptom
   of getting this wrong: `condition not met within 30s` on a projection
   that never arrives (reads like a service bug, isn't one) — confirm the
   real cause with `kubectl ... exec kafka-controller-0 -c kafka --
   kafka-consumer-groups.sh --describe --group <group>`; if the
   CONSUMER-ID column names an in-cluster pod, the local process is being
   starved of the partition.

## Key commands

```bash
make check                       # build-and-vet + scripts-sanity (mirrors ci.yml)
bash scripts/01-build.sh         # build every sibling repo's binaries
bash scripts/02-up-infra.sh      # Postgres + shared Kafka
bash scripts/03-up-services.sh   # start every service + MCP server + ops-agent
bash scripts/04-run-tests.sh     # run the godog suite — THE way to run tests
GODOG_TAGS=@soak bash scripts/06-run-soak.sh   # soak/backlog-ramp scenario
bash scripts/05-down-services.sh
```

CI (`.github/workflows/ci.yml`) currently runs `go vet` + `go test -c`
(compile-check) + shell syntax only — it does NOT stand up the fleet and
run the suite against a live cluster (that requires 8+ sibling repos
checked out and built, which a GitHub-hosted runner cannot cheaply do
today). This is a KNOWN gap: the suite's real value is only realised when
a human or agent remembers to run it locally against the kind cluster.
See the harness-coverage-expansion plan
(`.hermes/plans/2026-09-13_234500-harness-coverage-expansion.md`) Phase 5
Task 5.2 for the staged testcontainers-based CI plan to close this.
