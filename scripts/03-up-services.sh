#!/usr/bin/env bash
# e2e-tests/scripts/03-up-services.sh
#
# Starts all 10 bounded-context HTTP services as background processes
# against Postgres + Kafka, in dependency order:
#   1. process-path-management — no deps (Generic Subdomain owning the
#                           fleet's declared process-path catalogue;
#                           publishes ProcessPathCreated/Updated/
#                           Deactivated to Kafka, which the other five
#                           services below optionally consume when they
#                           set PATH_CATALOGUE_SOURCE=kafka -- see
#                           warehouse-infra's deploy_process_path_kafka_source
#                           Terraform variable for the equivalent live-
#                           cluster toggle)
#   2. facility-layout   — no deps (Open Host Service for the warehouse map).
#                           Runs with EVENT_PUBLISHER=kafka so its Published
#                           Language actually reaches
#                           warehouse.facility.events — without that the
#                           topic is never created and inventory-storage's
#                           cache below has nothing to replay.
#   3. inventory-storage — maintains a LOCAL CACHE of facility-layout's
#                           location classifications, fed by that topic
#                           (LOCATION_LOOKUP_MODE=kafka, inventory-storage
#                           ADR-0013), instead of calling facility-layout
#                           over HTTP on every stow. FACILITY_LAYOUT_BASE_URL
#                           is still exported so a local run can be flipped
#                           back to LOCATION_LOOKUP_MODE=http (the rollback)
#                           by changing one word.
#   4. wes-work-planning — calls inventory-storage over HTTP for product
#                           classification (PRODUCT_CLASSIFICATION_MODE=http),
#                           consumes workforce/inventory/fulfillment/order-management Kafka topics
#   5. fulfillment-execution — consumes WorkReleased from wes-work-planning's
#                           Kafka topic, calls inventory-storage over HTTP for
#                           DOT hazard segregation, publishes TaskCompleted
#   6. labor-performance  — consumes fulfillment-execution's TaskCompleted
#                           (unconditional, no toggle) to compute
#                           engineered-labor-standards performance scoring;
#                           no HTTP calls to/from any other context.
#   7. workforce-management — publishes ShiftPlanCommitted to Kafka, which
#                           wes-work-planning's labor-plan-view projects
#   8. order-management  — calls inventory-storage over HTTP (synchronous
#                           allocation), then publishes OrderAllocated /
#                           OrderPartiallyAllocated to Kafka, which
#                           wes-work-planning's 4th consumer subscription
#                           turns into a work unit via EnqueueWorkUnit —
#                           the choreographed-release path this repo's new
#                           order_management_choreographed_release.feature
#                           proves end-to-end.
#   9. network-fulfillment — the anti-corruption layer to an external
#                           retail network (ADR 0001). Calls
#                           order-management over HTTP (POST /orders with
#                           releaseOnAllocation=false, a HELD order) to ask
#                           feasibility, then POST /orders/{id}/release on
#                           acceptance. No Kafka publisher of its own in
#                           this harness (NETWORK_MODE=stub, no
#                           EVENT_PUBLISHER); its inbound demand is a
#                           poller reading a seeded stub file, never HTTP.
#  10. warehouse-planning  — capacity planning (what a process path can do
#                           over a window vs. the demand assigned to it).
#                           No HTTP dependency on any other context. Runs
#                           with NO KAFKA_BROKERS and EVENT_PUBLISHER=log:
#                           its labor/storage Kafka consumers are disabled
#                           and its outbox relay only logs, so it never
#                           joins the live cluster's consumer groups. Its
#                           scenarios drive its REST surface and seed its
#                           own facility-layout tally table directly.
#
# All seven publisher-capable services run with EVENT_PUBLISHER=kafka
# against the shared broker (labor-performance is the exception -- it has
# no EVENT_PUBLISHER flag at all, being a pure consumer, though it still
# needs KAFKA_BROKERS to build its consumer group) so the cross-context
# event flow (WorkReleased -> Task, TaskCompleted -> WorkUnit completion /
# labor-performance scoring, ShiftPlanCommitted -> labor-plan-view,
# OrderAllocated/OrderPartiallyAllocated -> WorkUnit) is exercised for
# real, not just each service in isolation. process-path-management's own
# catalogue events are NOT consumed by any of the six below in this harness
# today (each still
# defaults to PATH_CATALOGUE_SOURCE=file, matching the live cluster's own
# default-off toggle) -- process-path-management is started here so its
# REST API and Kafka publisher are available to exercise directly, and so
# opting a service into PATH_CATALOGUE_SOURCE=kafka locally is a one-line
# addition to that service's start_service call below, not a new harness
# feature.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib.sh"

log "starting process-path-management on ${PROCESS_PATH_BASE_URL}"
start_service process-path "${BIN_DIR}/process-path" \
  HTTP_ADDR=":${PROCESS_PATH_HTTP_PORT}" \
  DATABASE_URL="${PROCESS_PATH_DB_URL}" \
  PGPASSWORD="process_path" \
  MIGRATIONS_PATH="${PROCESS_PATH_REPO}/migrations" \
  EVENT_PUBLISHER=kafka \
  KAFKA_BROKERS="${KAFKA_BROKERS}" \
  LOG_LEVEL=info
wait_for_http "${PROCESS_PATH_BASE_URL}/healthz"

log "starting facility-layout on ${FACILITY_BASE_URL}"
start_service facility "${BIN_DIR}/facility" \
  HTTP_ADDR=":${FACILITY_HTTP_PORT}" \
  DATABASE_URL="${FACILITY_DB_URL}" \
  PGPASSWORD="facility" \
  MIGRATIONS_PATH="${FACILITY_REPO}/migrations" \
  EVENT_PUBLISHER=kafka \
  KAFKA_BROKERS="${KAFKA_BROKERS}" \
  LOG_LEVEL=info
wait_for_http "${FACILITY_BASE_URL}/healthz"

log "starting inventory-storage on ${INVENTORY_BASE_URL}"
start_service inventory "${BIN_DIR}/inventory" \
  HTTP_ADDR=":${INVENTORY_HTTP_PORT}" \
  DATABASE_URL="${INVENTORY_DB_URL}" \
  PGPASSWORD="inventory" \
  MIGRATIONS_PATH="${INVENTORY_REPO}/migrations" \
  EVENT_PUBLISHER=kafka \
  KAFKA_BROKERS="${KAFKA_BROKERS}" \
  LOCATION_LOOKUP_MODE=kafka \
  FACILITY_LAYOUT_BASE_URL="${FACILITY_BASE_URL}" \
  LOG_LEVEL=info
wait_for_http "${INVENTORY_BASE_URL}/healthz"

log "starting wes-work-planning on ${WES_BASE_URL}"
start_service wes "${BIN_DIR}/wes" \
  HTTP_ADDR=":${WES_HTTP_PORT}" \
  DATABASE_URL="${WES_DB_URL}" \
  PGPASSWORD="wes" \
  MIGRATIONS_PATH="${WES_REPO}/migrations" \
  EVENT_PUBLISHER=kafka \
  KAFKA_BROKERS="${KAFKA_BROKERS}" \
  KAFKA_CONSUMER_GROUP="${WES_CONSUMER_GROUP}" \
  PRODUCT_CLASSIFICATION_MODE=http \
  INVENTORY_STORAGE_BASE_URL="${INVENTORY_BASE_URL}" \
  PATH_CATALOGUE_FILE="${PATH_CATALOGUE_FILE}" \
  LOG_LEVEL=info
wait_for_http "${WES_BASE_URL}/healthz"

log "starting fulfillment-execution on ${FULFILLMENT_BASE_URL}"
# fulfillment-execution's main() hardcodes postgres.Migrate(databaseURL,
# "migrations") — a relative path, unlike the other services which read
# MIGRATIONS_PATH from the environment — so it must be launched with its
# CWD set to the repo root for that relative path to resolve.
start_service_in execution "${FULFILLMENT_REPO}" "${BIN_DIR}/execution" \
  HTTP_ADDR=":${FULFILLMENT_HTTP_PORT}" \
  DATABASE_URL="${FULFILLMENT_DB_URL}" \
  PGPASSWORD="fulfillment" \
  EVENT_PUBLISHER=kafka \
  KAFKA_BROKERS="${KAFKA_BROKERS}" \
  WORK_RELEASED_CONSUMER_GROUP="${FULFILLMENT_CONSUMER_GROUP}" \
  PRODUCT_CLASSIFICATION_MODE=http \
  INVENTORY_STORAGE_BASE_URL="${INVENTORY_BASE_URL}" \
  PATH_CATALOGUE_FILE="${PATH_CATALOGUE_FILE}" \
  LOG_LEVEL=info
wait_for_http "${FULFILLMENT_BASE_URL}/healthz"

log "starting labor-performance on ${LABOR_BASE_URL}"
# labor-performance (7th bounded context): consumes fulfillment-execution's
# TaskCompleted event (unconditional -- no PATH_CATALOGUE-style toggle;
# see env.sh's own comment) to compute engineered-labor-standards
# performance scoring. Started right after fulfillment-execution so a
# real TaskCompleted has already been published by the time any scenario
# exercises it.
start_service labor "${BIN_DIR}/labor" \
  HTTP_ADDR=":${LABOR_HTTP_PORT}" \
  DATABASE_URL="${LABOR_DB_URL}" \
  PGPASSWORD="labor" \
  MIGRATIONS_PATH="${LABOR_REPO}/migrations" \
  KAFKA_BROKERS="${KAFKA_BROKERS}" \
  KAFKA_CONSUMER_GROUP="${LABOR_CONSUMER_GROUP}" \
  LOG_LEVEL=info
wait_for_http "${LABOR_BASE_URL}/healthz"

log "starting workforce-management on ${WORKFORCE_BASE_URL}"
start_service workforce "${BIN_DIR}/workforce" \
  HTTP_ADDR=":${WORKFORCE_HTTP_PORT}" \
  DATABASE_URL="${WORKFORCE_DB_URL}" \
  PGPASSWORD="workforce" \
  MIGRATIONS_PATH="${WORKFORCE_REPO}/migrations" \
  EVENT_PUBLISHER=kafka \
  KAFKA_BROKERS="${KAFKA_BROKERS}" \
  PATH_CATALOGUE_FILE="${PATH_CATALOGUE_FILE}" \
  INSTALLED_CAPACITY_MODE=http \
  FULFILLMENT_EXECUTION_BASE_URL="${FULFILLMENT_BASE_URL}" \
  LOG_LEVEL=info
wait_for_http "${WORKFORCE_BASE_URL}/healthz"

log "starting order-management on ${ORDER_BASE_URL}"
# order-management (6th bounded context): the choreographed-release
# redesign. It calls inventory-storage over HTTP (synchronous allocation,
# unchanged) and — instead of also calling wes-work-planning's HTTP API —
# publishes OrderAllocated/OrderPartiallyAllocated to Kafka topic
# warehouse.order-management.events, which wes-work-planning's 4th
# consumer subscription (already started above) picks up and turns into a
# work unit via its existing EnqueueWorkUnit use case. No
# WES_WORK_PLANNING_BASE_URL/WES_WORK_PLANNING_MODE is set here: this
# service's HTTP release path (POST /paths/{pathId}/work-units) is no
# longer part of its choreographed flow at all.
start_service order "${BIN_DIR}/order" \
  HTTP_ADDR=":${ORDER_HTTP_PORT}" \
  DATABASE_URL="${ORDER_DB_URL}" \
  PGPASSWORD="order" \
  MIGRATIONS_PATH="${ORDER_REPO}/migrations" \
  EVENT_PUBLISHER=kafka \
  KAFKA_BROKERS="${KAFKA_BROKERS}" \
  INVENTORY_STORAGE_MODE=http \
  INVENTORY_STORAGE_BASE_URL="${INVENTORY_BASE_URL}" \
  LOG_LEVEL=info
wait_for_http "${ORDER_BASE_URL}/healthz"

log "starting network-fulfillment on ${NETWORK_BASE_URL}"
# network-fulfillment (9th bounded context): the anti-corruption layer to
# the external retail network (ADR 0001). Started LAST because its only
# outbound dependency is order-management (ORDER_MANAGEMENT_URL, HTTP,
# POST /orders with releaseOnAllocation=false -- the same held-order
# pattern ADR 0020 documents), which must already be up. NETWORK_MODE is
# left at its default (stub) deliberately -- see that package's own doc
# comment: the kind cluster and this harness must never need real network
# credentials. Inbound demand arrives ONLY via NETWORK_SEED_FILE (there is
# no HTTP intake endpoint, ADR 0001 §5) and PRODUCT_TRANSLATION_FILE is
# the ACL dictionary without which every seeded order rejects as
# untranslatable -- both fixtures live in this repo's own
# fixtures/network-fulfillment/, not network-fulfillment's.
start_service network "${BIN_DIR}/network" \
  PORT="${NETWORK_HTTP_PORT}" \
  DATABASE_URL="${NETWORK_DB_URL}" \
  PGPASSWORD="network" \
  MIGRATIONS_PATH="${NETWORK_REPO}/migrations" \
  ORDER_MANAGEMENT_URL="${ORDER_BASE_URL}" \
  PRODUCT_TRANSLATION_FILE="${NETWORK_PRODUCT_TRANSLATION_FILE}" \
  NETWORK_SEED_FILE="${NETWORK_SEED_FILE}" \
  POLL_INTERVAL="5s" \
  SWEEP_INTERVAL="30s" \
  LOG_LEVEL=info
wait_for_http "${NETWORK_BASE_URL}/healthz"

log "starting warehouse-planning on ${PLANNING_BASE_URL}"
# warehouse-planning (10th bounded context): cmd/api reads these env vars
# (see its main.go). KAFKA_BROKERS is blanked EXPLICITLY, not just left
# out: with it set, startKafkaConsumers would start the labor-capacity and
# storage-capacity consumers under the service's DEFAULT group ids
# ("warehouse-planning-labor-capacity"/"-storage-capacity"), i.e. the very
# groups the live in-cluster pod holds, and the local process would either
# starve or steal its partitions. Blank = ingestion disabled (logged, not
# fatal). EVENT_PUBLISHER=log (its default) keeps the outbox relay off
# Kafka too; CapacityPlan events are only logged. No
# *_CONSUMER_GROUP is needed while Kafka is off; if a scenario ever turns
# it on, add LABOR_CAPACITY_CONSUMER_GROUP/STORAGE_CAPACITY_CONSUMER_GROUP
# derived from E2E_CONSUMER_GROUP_SUFFIX in env.sh first.
# The migrations live under internal/adapters/outbound/postgres/migrations
# (its default is relative to the repo root, hence the absolute path).
start_service planning "${BIN_DIR}/planning" \
  HTTP_ADDR=":${PLANNING_HTTP_PORT}" \
  DATABASE_URL="${PLANNING_DB_URL}" \
  PGPASSWORD="planning" \
  MIGRATIONS_PATH="${PLANNING_REPO}/internal/adapters/outbound/postgres/migrations" \
  EVENT_PUBLISHER=log \
  KAFKA_BROKERS="" \
  LOG_LEVEL=info
wait_for_http "${PLANNING_BASE_URL}/healthz"

log "all 10 services up and healthy"
printf '  %-24s %s\n' process-path-management "${PROCESS_PATH_BASE_URL}"
printf '  %-24s %s\n' facility-layout        "${FACILITY_BASE_URL}"
printf '  %-24s %s\n' inventory-storage      "${INVENTORY_BASE_URL}"
printf '  %-24s %s\n' wes-work-planning      "${WES_BASE_URL}"
printf '  %-24s %s\n' fulfillment-execution  "${FULFILLMENT_BASE_URL}"
printf '  %-24s %s\n' labor-performance      "${LABOR_BASE_URL}"
printf '  %-24s %s\n' workforce-management   "${WORKFORCE_BASE_URL}"
printf '  %-24s %s\n' order-management       "${ORDER_BASE_URL}"
printf '  %-24s %s\n' network-fulfillment    "${NETWORK_BASE_URL}"
printf '  %-24s %s\n' warehouse-planning     "${PLANNING_BASE_URL}"

# --- MCP servers (cmd/mcp), one per context, pointed at the SAME
# Postgres each HTTP service above just started against — so a fact an
# HTTP call writes (e.g. a shift plan, a work pool) is immediately
# visible to warehouse-ops-agent's MCP-tool reads. Every one of these
# only needs its own MCP_READ_KEY: none of the 5 contexts' T1 outbound
# clients call a write tool. -----------------------------------------

log "starting facility-layout MCP server on :${FACILITY_MCP_PORT}"
start_service facility-mcp "${BIN_DIR}/facility-mcp" \
  MCP_ADDR=":${FACILITY_MCP_PORT}" \
  DATABASE_URL="${FACILITY_DB_URL}" \
  PGPASSWORD="facility" \
  MIGRATIONS_PATH="${FACILITY_REPO}/migrations" \
  MCP_READ_KEY="${FACILITY_MCP_READ_KEY}" \
  LOG_LEVEL=info
wait_for_tcp localhost "${FACILITY_MCP_PORT}"

log "starting inventory-storage MCP server on :${INVENTORY_MCP_PORT}"
start_service inventory-mcp "${BIN_DIR}/inventory-mcp" \
  MCP_ADDR=":${INVENTORY_MCP_PORT}" \
  DATABASE_URL="${INVENTORY_DB_URL}" \
  PGPASSWORD="inventory" \
  MIGRATIONS_PATH="${INVENTORY_REPO}/migrations" \
  MCP_READ_KEY="${INVENTORY_MCP_READ_KEY}" \
  LOG_LEVEL=info
wait_for_tcp localhost "${INVENTORY_MCP_PORT}"

log "starting wes-work-planning MCP server on :${WES_MCP_PORT}"
start_service wes-mcp "${BIN_DIR}/wes-mcp" \
  MCP_ADDR=":${WES_MCP_PORT}" \
  DATABASE_URL="${WES_DB_URL}" \
  PGPASSWORD="wes" \
  MCP_READ_KEY="${WES_MCP_READ_KEY}" \
  LOG_LEVEL=info
wait_for_tcp localhost "${WES_MCP_PORT}"

log "starting fulfillment-execution MCP server on :${FULFILLMENT_MCP_PORT}"
# Same relative-migrations-path quirk as cmd/execution (see
# 03-up-services.sh above): must launch with CWD set to the repo root.
start_service_in execution-mcp "${FULFILLMENT_REPO}" "${BIN_DIR}/execution-mcp" \
  MCP_ADDR=":${FULFILLMENT_MCP_PORT}" \
  DATABASE_URL="${FULFILLMENT_DB_URL}" \
  PGPASSWORD="fulfillment" \
  MCP_READ_KEY="${FULFILLMENT_MCP_READ_KEY}" \
  LOG_LEVEL=info
wait_for_tcp localhost "${FULFILLMENT_MCP_PORT}"

log "starting workforce-management MCP server on :${WORKFORCE_MCP_PORT}"
start_service workforce-mcp "${BIN_DIR}/workforce-mcp" \
  MCP_ADDR=":${WORKFORCE_MCP_PORT}" \
  DATABASE_URL="${WORKFORCE_DB_URL}" \
  PGPASSWORD="workforce" \
  MIGRATIONS_PATH="${WORKFORCE_REPO}/migrations" \
  MCP_READ_KEY="${WORKFORCE_MCP_READ_KEY}" \
  LOG_LEVEL=info
wait_for_tcp localhost "${WORKFORCE_MCP_PORT}"

# labor-performance's MCP server is started here too, for harness parity
# with the other 5 -- but it is deliberately NOT wired into
# warehouse-ops-agent's config below, unlike them. No T5 use case
# (console_reports, dailybrief, flow_balance_advisory, order_lifecycle,
# stranded_reservation) consumes labor-performance's scorecard/coaching-
# flag data today; adding a 6th mcpclient with nothing calling it would
# violate the MCP governance charter's own "tools map to a real decision"
# rule. Starting it here still lets a scenario or a manual client exercise
# it directly over the wire.
log "starting labor-performance MCP server on :${LABOR_MCP_PORT}"
start_service labor-mcp "${BIN_DIR}/labor-mcp" \
  MCP_ADDR=":${LABOR_MCP_PORT}" \
  DATABASE_URL="${LABOR_DB_URL}" \
  PGPASSWORD="labor" \
  MIGRATIONS_PATH="${LABOR_REPO}/migrations" \
  MCP_READ_KEY="${LABOR_MCP_READ_KEY}" \
  LOG_LEVEL=info
wait_for_tcp localhost "${LABOR_MCP_PORT}"

# order-management's MCP server, same deliberate-non-wiring rationale as
# labor-mcp above: no existing T5 use case reaches order-management via
# MCP rather than its existing REST client, so no mcpclient/ops-agent
# wiring is added here either.
log "starting order-management MCP server on :${ORDER_MCP_PORT}"
start_service order-mcp "${BIN_DIR}/order-mcp" \
  MCP_ADDR=":${ORDER_MCP_PORT}" \
  DATABASE_URL="${ORDER_DB_URL}" \
  PGPASSWORD="order" \
  MIGRATIONS_PATH="${ORDER_REPO}/migrations" \
  MCP_READ_KEY="${ORDER_MCP_READ_KEY}" \
  LOG_LEVEL=info
wait_for_tcp localhost "${ORDER_MCP_PORT}"

log "all 7 MCP servers up"

# --- warehouse-ops-agent (T5): the agentic analyze/act layer, wired to
# the 5 MCP servers just started above as its only upstream dependency. --

log "starting warehouse-ops-agent on ${OPS_AGENT_BASE_URL}"
start_service ops-agent "${BIN_DIR}/ops-agent" \
  AGENT_ADDR=":${OPS_AGENT_HTTP_PORT}" \
  WES_WORK_PLANNING_MCP_ENDPOINT="${WES_MCP_URL}" \
  WES_WORK_PLANNING_MCP_READ_KEY="${WES_MCP_READ_KEY}" \
  FULFILLMENT_EXECUTION_MCP_ENDPOINT="${FULFILLMENT_MCP_URL}" \
  FULFILLMENT_EXECUTION_MCP_READ_KEY="${FULFILLMENT_MCP_READ_KEY}" \
  INVENTORY_STORAGE_MCP_ENDPOINT="${INVENTORY_MCP_URL}" \
  INVENTORY_STORAGE_MCP_READ_KEY="${INVENTORY_MCP_READ_KEY}" \
  WORKFORCE_MANAGEMENT_MCP_ENDPOINT="${WORKFORCE_MCP_URL}" \
  WORKFORCE_MANAGEMENT_MCP_READ_KEY="${WORKFORCE_MCP_READ_KEY}" \
  FACILITY_LAYOUT_MCP_ENDPOINT="${FACILITY_MCP_URL}" \
  FACILITY_LAYOUT_MCP_READ_KEY="${FACILITY_MCP_READ_KEY}" \
  MCP_READ_KEY="${OPS_AGENT_MCP_READ_KEY}" \
  DAILY_BRIEF_PATH_TARGETS="${OPS_AGENT_PATH_TARGETS}" \
  LOG_LEVEL=info
wait_for_http "${OPS_AGENT_BASE_URL}/healthz"

log "warehouse-ops-agent up"
printf '  %-24s %s\n' warehouse-ops-agent "${OPS_AGENT_BASE_URL}"
