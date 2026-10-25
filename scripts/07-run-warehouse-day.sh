#!/usr/bin/env bash
# e2e-tests/scripts/07-run-warehouse-day.sh
#
# Runs the warehouse-day simulator (cmd/warehouse-day) against the
# already-running services (scripts/03-up-services.sh): one compressed
# 06:00-22:00 operating day driven ONLY through every bounded context's
# published REST contract, audited against the Kafka integration topics.
#
# Personas: control tower (facility layout, bins, catalogue, CPT schedule,
# labor standards), receiving/stow, customers (orders on a daily curve,
# partial/held/backorder/cancel mixes), customer service, supervisor
# (shift plan, assignments, breaks, staffing/rebalance checks), pickers,
# rebin, packers, SLAM operators (weight mismatches injected), inventory
# control (cycle counts, retry-allocation). At end of day an auditor traces
# every order across all contexts and prints a KPI report plus a JSON
# report in run/; the process exits non-zero if the warehouse did not work.
#
# Not part of any default run (like 06-run-soak.sh).
#
# Tunables (env or flags; see cmd/warehouse-day/main.go):
#   DAY_DURATION   wall length of the simulated day   (default 8m)
#   DRAIN_TIMEOUT  overtime allowed after 22:00       (default 3m)
#   DAY_ORDERS     customer orders                    (default 150)
#   DAY_PICKERS / DAY_PACKERS / DAY_SLAM / DAY_REBIN / DAY_FLEX
#   DAY_SEED       deterministic run (default: time based)
#
# Quick smoke: DAY_DURATION=90s DAY_ORDERS=20 bash scripts/07-run-warehouse-day.sh
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib.sh"

cd "${WORKSPACE_ROOT}"
mkdir -p "${BIN_DIR}" run
log "building cmd/warehouse-day"
CGO_ENABLED=0 go build -o "${BIN_DIR}/warehouse-day" ./cmd/warehouse-day
ok "warehouse-day -> ${BIN_DIR}/warehouse-day"

set -a
# shellcheck disable=SC1091
source "${WORKSPACE_ROOT}/env.sh"
set +a

exec "${BIN_DIR}/warehouse-day" "$@"
