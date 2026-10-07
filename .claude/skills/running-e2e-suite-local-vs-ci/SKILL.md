---
name: running-e2e-suite-local-vs-ci
description: How to run the godog e2e suite locally (build, infra, services, tags, soak, warehouse-day) and how CI differs (ci.yml compile checks vs the weekly bootstrap-only e2e-behaviour.yml). Use when asked to run, verify, debug or reproduce a failing e2e scenario, or to change what CI runs.
---

# Running the suite: local vs CI

## Local (the only way to run the full suite)

Always through the scripts, never a bare `go test` (it lacks the base URLs and DB DSNs; DB steps fail with `failed SASL auth`).

```bash
bash scripts/02-up-infra.sh    # 9 Postgres via docker-compose.yml; REQUIRES the shared Kafka on localhost:9092
bash scripts/01-build.sh       # binaries into bin/ from the sibling repos
bash scripts/03-up-services.sh # starts every service, MCP server and ops-agent
bash scripts/04-run-tests.sh   # THE suite (excludes @soak by default)
bash scripts/05-down-services.sh
```

`make up|run|soak|warehouse-day|down` wrap the same scripts.

- One scenario: `GODOG_TAGS=@network-fulfillment bash scripts/04-run-tests.sh`. The `-godog.tags` flag is NOT wired; `TestMain` reads only `GODOG_TAGS` (default `~@soak`). Combine tags with `&&`. List real tags: `grep -n '^  @' features/*.feature`.
- Kafka is NOT started by this repo: it is the warehouse-infra kind cluster's broker. `scripts/02-up-infra.sh` dies with a pointer if `localhost:9092` is down.
- Run from the REAL checkout `~/warehouse-systems/e2e-tests`. `env.sh` sets `REPOS_ROOT` to the parent of the directory it lives in, so from a `git worktree` (`.worktrees/e2e-tests-*`) every sibling-repo path is wrong and the build fails with "no such file or directory". If your real checkout is dirty, `git stash push -u` first, copy your changed files in, run, revert, `git stash pop`.
- Never reuse a consumer group with the live cluster: `env.sh` derives `E2E_CONSUMER_GROUP_SUFFIX` per run. If a projection "never arrives" (`condition not met within 30s`), check `kafka-consumer-groups.sh --describe --group <group>`: an in-cluster pod in CONSUMER-ID means the local process is starved.
- Stale binaries cause schema skew: rebuild with `scripts/01-build.sh` after any sibling repo change you want tested.
- Idempotency check for a new scenario: run the full suite 2-3 times back to back with NO reset; all runs must pass with the same exact assertions.
- Soak: `SOAK_DURATION=2m bash scripts/06-run-soak.sh` (observational; 409s on release/claim are counted, not failed; stop a run once a real bug is understood). Warehouse-day simulator: `DAY_DURATION=90s DAY_ORDERS=20 bash scripts/07-run-warehouse-day.sh`. Neither is in any default run.
- Verifying someone else's "I ran it live" claim: replay it yourself (isolate with the new scenario's tag, then the full suite), and confirm a claimed "pre-existing failure" by re-running the same tag on the unchanged base commit.

## CI

- `.github/workflows/ci.yml` (every PR): `guide-lint` (blocking), gofmt, `go vet`, `go test -c` compile check, shell syntax. It never starts services. Local mirror: `make check-fast` (fmt-check + vet, the agent Stop-hook gate) and `make check` (adds `go test -c`, `bash -n` of scripts, `docker compose config`).
- `.github/workflows/e2e-behaviour.yml` (weekly Mon 07:00 UTC + manual): checks out 7 sibling repos (incl. product-master) plus a sparse `warehouse-infra` (process-path catalogue), runs a KRaft Kafka service container, pre-creates every topic and waits for partition leaders (auto-create races on a fresh broker), starts 7 Postgres, builds and starts ONLY the 7 core services (readers in `PRODUCT_CLASSIFICATION_MODE=kafka`), then runs `GODOG_TAGS="@bootstrap,@product-master"`. It is a deliberate two-scenario slice, not the full suite. On a scheduled failure `scripts/harness/red_issue.py` opens/refreshes a `harness:red` issue, and closes it on recovery.
- To widen CI coverage add one feature at a time: its tag in `GODOG_TAGS`, then whatever services/Postgres/topics that scenario needs (mirror the existing steps; the process-path, labor, network and ops-agent services are not built there). Read the failing job's logs, not just the red X: dump of service logs runs on failure.
- CI uses the sibling repos' `develop`; an unmerged sibling PR is invisible to it.
