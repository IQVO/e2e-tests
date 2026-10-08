---
paths:
  - "scripts/*.sh"
  - "env.sh"
  - "docker-compose.yml"
  - "Makefile"
  - ".github/workflows/**"
  - "fixtures/**"
---

# Harness scripts, env and CI rules

Playbooks: skills `adding-e2e-scenario-or-service` (registering a service) and `running-e2e-suite-local-vs-ci`.

- **All config lives in `env.sh`** (repo paths, ports, DSNs, consumer groups, fixture paths). Ports and DSNs are edited only there. DSNs carry no password; `dbOpen` in `e2e_test.go` supplies it per service.
- **Never share a Kafka consumer group with the live cluster.** The fleet has ONE broker; a local `wes`/`execution`/`labor` with the cluster's group id joins the SAME group and starves. `env.sh` derives `E2E_CONSUMER_GROUP_SUFFIX` per run and each Kafka-consuming service gets `<SERVICE>_CONSUMER_GROUP` from it. New consumers get their own suffixed group, never a fixed string. Symptom: `condition not met within 30s` on a projection that never arrives. Diagnose: `kubectl ... exec kafka-controller-0 -c kafka -- kafka-consumer-groups.sh --describe --group <group>`; an in-cluster pod in CONSUMER-ID confirms it.
- **This repo does not start Kafka**; `scripts/02-up-infra.sh` expects the warehouse-infra kind cluster's broker on `localhost:9092`.
- **Run from `~/warehouse-systems/e2e-tests`, not a worktree**: `REPOS_ROOT` is the parent of `env.sh`'s directory.
- **A service's real env vars come from its `cmd/<svc>/main.go`** (a pure consumer has no `EVENT_PUBLISHER`); `cmd/` dir names differ from repo names. Fixture files are wired by absolute path and verified in the startup log.
- Keep the counts in comments/log lines of `scripts/01-build.sh`, `scripts/02-up-infra.sh`, `scripts/03-up-services.sh` and `README.md` in sync when you add a service (they drift).
- `scripts/04-run-tests.sh` is the only supported runner; `GODOG_TAGS` selects scenarios (default `~@soak`, so soak never runs in a default or CI run).
- **CI**: `.github/workflows/ci.yml` only compile-checks (and `guide-lint` is blocking). `.github/workflows/e2e-behaviour.yml` has three jobs: `@bootstrap,@product-master` against 7 services (incl. product-master), `@inter-warehouse-transfer` against 9 (adds network-inventory-planning and warehouse-planning), and `@inbound-receiving` against 3 (product-master, inbound-receiving, inventory-storage with `INBOUND_RECEIPT_CONSUMER_GROUP`); weekly, and it opens a `harness:red` issue on scheduled failure via `scripts/harness/red_issue.py`. Widen it one feature at a time. Do not edit managed files under `scripts/harness/`.
- **Product classification** is owned by product-master (`PUT /products/{sku}` then `PUT /products/{sku}/classification`); inventory-storage's PUT is `410 classification-moved`. Readers run `PRODUCT_CLASSIFICATION_MODE=kafka` with a per-run `PRODUCT_CLASSIFICATION_CONSUMER_GROUP`; inventory-storage gets a per-run `PRODUCT_MASTER_CONSUMER_GROUP`. `http` mode fails at boot.
- `make check-fast` is the agent gate; `make check` adds `go test -c`, script syntax and `docker compose config`.
