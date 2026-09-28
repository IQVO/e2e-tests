#!/usr/bin/env bash
# e2e-tests/scripts/01-build.sh
#
# Builds each bounded context's HTTP service binary (cmd/<svc>, the same
# entrypoint each Dockerfile builds — NOT cmd/mcp) straight out of its own
# repo, into e2e-tests/bin/. Native binaries, not containers: this is a
# local full-warehouse-bootstrap harness, not a Kubernetes deploy — that
# job already belongs to warehouse-infra/terraform.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib.sh"

log "building 9 service binaries into ${BIN_DIR}"

build_one() {
  local name="$1" repo="$2" cmd_pkg="$3"
  log "go build ${name} (${repo}/cmd/${cmd_pkg})"
  ( cd "${repo}" && CGO_ENABLED=0 go build -o "${BIN_DIR}/${name}" "./cmd/${cmd_pkg}" )
  ok "${name} -> ${BIN_DIR}/${name}"
}

build_one facility     "${FACILITY_REPO}"     facility
build_one inventory    "${INVENTORY_REPO}"    inventory
build_one wes          "${WES_REPO}"          wes
build_one execution    "${FULFILLMENT_REPO}"  execution
build_one workforce    "${WORKFORCE_REPO}"    workforce
# order-management (6th bounded context): cmd/order is its HTTP binary
# package, same cmd/<name> convention as the other five services above.
build_one order        "${ORDER_REPO}"        order
# process-path-management (8th bounded context): cmd/pathmgmt is its HTTP
# binary package.
build_one process-path "${PROCESS_PATH_REPO}" pathmgmt
# labor-performance (7th bounded context): cmd/labor is its HTTP binary
# package. It also has cmd/labor-projector and cmd/labor-reports (the
# analytics data product) which this harness does not build/run --
# out of scope, no consumer of them exists in this harness's own scenarios.
build_one labor         "${LABOR_REPO}"         labor
# network-fulfillment (9th bounded context): cmd/netfulfil is its HTTP
# binary package (poller + read-only REST, ADR 0001). No cmd/mcp exists
# yet in this repo (see its own AGENTS.md "CURRENT STATE"), so there is
# no *-mcp binary to build for it below, unlike the other 6 contexts.
build_one network       "${NETWORK_REPO}"       netfulfil

log "building 7 MCP server binaries into ${BIN_DIR} (cmd/mcp — the agentic see-layer)"
build_one facility-mcp     "${FACILITY_REPO}"     mcp
build_one inventory-mcp    "${INVENTORY_REPO}"    mcp
build_one wes-mcp          "${WES_REPO}"          mcp
build_one execution-mcp    "${FULFILLMENT_REPO}"  mcp
build_one workforce-mcp    "${WORKFORCE_REPO}"    mcp
build_one labor-mcp        "${LABOR_REPO}"        mcp
build_one order-mcp        "${ORDER_REPO}"        mcp

log "building warehouse-ops-agent (cmd/agent — the agentic analyze/act layer, T5)"
build_one ops-agent "${OPS_AGENT_REPO}" agent

log "all 17 binaries built"
ls -la "${BIN_DIR}"
