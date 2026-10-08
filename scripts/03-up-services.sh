#!/usr/bin/env bash
# e2e-tests/scripts/03-up-services.sh
#
# Starts all 13 bounded-context HTTP services as background processes
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
#   3. product-master    — no deps. Owner of SKU classification and the
#                           physical profile (its ADR 0001/0002/0003).
#                           EVENT_PUBLISHER=kafka so ProductClassified
#                           reaches warehouse.product-master.events, the ONLY
#                           way the four readers below learn a
#                           classification. Its legacy importer
#                           (LEGACY_IMPORT_CONSUMER_GROUP) is NOT started:
#                           it is a one-off migration aid, and on the shared
#                           broker it would replay the cluster's whole
#                           inventory history into this local database.
#   3b. inbound-receiving — the WMS-tier inbound dock workflow (ASN, dock
#                           appointment, receipt; its ADR 0001/0002).
#                           EVENT_PUBLISHER=kafka so ReceiptLineReceived
#                           reaches warehouse.inbound-receiving.events, the
#                           ONLY way inventory-storage (below, its ADR 0037)
#                           learns of a received Good line. It keeps a local
#                           copy of product-master's ProductRegistered
#                           (PRODUCT_MODE=kafka + a per-run
#                           PRODUCT_CONSUMER_GROUP), so it is started right
#                           after product-master. Its dock-door copy stays
#                           permissive (DOCK_DOOR_MODE unset): no scenario
#                           here needs a facility-layout dock slot.
#   4. inventory-storage — maintains a LOCAL CACHE of facility-layout's
#                           location classifications, fed by that topic
#                           (LOCATION_LOOKUP_MODE=kafka, inventory-storage
#                           ADR-0013), instead of calling facility-layout
#                           over HTTP on every stow. FACILITY_LAYOUT_BASE_URL
#                           is still exported so a local run can be flipped
#                           back to LOCATION_LOOKUP_MODE=http (the rollback)
#                           by changing one word. Its product classifications
#                           are a local copy of product-master's events
#                           (PRODUCT_MASTER_CONSUMER_GROUP, ADR 0034); its
#                           own classification PUT answers 410. It also
#                           consumes inbound-receiving's ReceiptLineReceived
#                           (INBOUND_RECEIPT_CONSUMER_GROUP, ADR 0037): Good
#                           lines become staged stock, Damaged ones do not.
#   5. wes-work-planning — reads a local copy of product-master's
#                           classifications (PRODUCT_CLASSIFICATION_MODE=kafka
#                           + PRODUCT_CLASSIFICATION_CONSUMER_GROUP; "http"
#                           fails at boot), consumes
#                           workforce/inventory/fulfillment/order-management Kafka topics
#   6. fulfillment-execution — consumes WorkReleased from wes-work-planning's
#                           Kafka topic, reads the same kind of local
#                           classification copy for DOT hazard segregation,
#                           publishes TaskCompleted
#   7. labor-performance  — consumes fulfillment-execution's TaskCompleted
#                           (unconditional, no toggle) to compute
#                           engineered-labor-standards performance scoring;
#                           no HTTP calls to/from any other context.
#   8. workforce-management — publishes ShiftPlanCommitted to Kafka, which
#                           wes-work-planning's labor-plan-view projects
#   9. order-management  — calls inventory-storage over HTTP (synchronous
#                           allocation), reads a local classification copy
#                           (PRODUCT_CLASSIFICATION_MODE=kafka), then
#                           publishes OrderAllocated /
#                           OrderPartiallyAllocated to Kafka, which
#                           wes-work-planning's 4th consumer subscription
#                           turns into a work unit via EnqueueWorkUnit —
#                           the choreographed-release path this repo's new
#                           order_management_choreographed_release.feature
#                           proves end-to-end.
#  10. network-fulfillment — the anti-corruption layer to an external
#                           retail network (ADR 0001). Calls
#                           order-management over HTTP (POST /orders with
#                           releaseOnAllocation=false, a HELD order) to ask
#                           feasibility, then POST /orders/{id}/release on
#                           acceptance. No Kafka publisher of its own in
#                           this harness (NETWORK_MODE=stub, no
#                           EVENT_PUBLISHER); its inbound demand is a
#                           poller reading a seeded stub file, never HTTP.
#  10. warehouse-planning  — producer of CapacityPlanPublished (its own REST
#                           API creates + publishes the plans; the outbox
#                           relay publishes to Kafka). No deps on the
#                           services above beyond Kafka.
#  11. network-inventory-planning — plans/drives the inter-warehouse
#                           transfer saga. Kafka + own Postgres only; it
#                           consumes SiteCapabilityChanged,
#                           SiteSkuDemandChanged, CapacityPlanPublished
#                           (read models) and inventory-storage /
#                           fulfillment-execution replies+facts, and
#                           publishes TransferAllocationRequested /
#                           WorkDemandReleased. inventory-storage (3) now
#                           also runs its transfer allocation consumer.
#
# All eight publisher-capable services run with EVENT_PUBLISHER=kafka
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

log "starting product-master on ${PRODUCT_MASTER_BASE_URL}"
# product-master: cmd/api, migrations embedded in the binary (no
# MIGRATIONS_PATH). SHUTDOWN_DRAIN_DELAY=0 so stop_service's bounded wait
# is not spent idling on the default 5s readiness drain.
start_service product-master "${BIN_DIR}/product-master" \
  HTTP_ADDR=":${PRODUCT_MASTER_HTTP_PORT}" \
  DATABASE_URL="${PRODUCT_MASTER_DB_URL}" \
  PGPASSWORD="product_master" \
  EVENT_PUBLISHER=kafka \
  KAFKA_BROKERS="${KAFKA_BROKERS}" \
  SHUTDOWN_DRAIN_DELAY=0 \
  LOG_LEVEL=info
wait_for_http "${PRODUCT_MASTER_BASE_URL}/healthz"

log "starting inbound-receiving on ${INBOUND_BASE_URL}"
# inbound-receiving: cmd/api, migrations embedded (no MIGRATIONS_PATH).
# PRODUCT_MODE=kafka makes it refuse an ASN line whose SKU product-master has
# not announced yet (422 unknown-sku), fed by a per-run consumer group.
# OUTBOX_RELAY_INTERVAL keeps the handover fast. SHUTDOWN_DRAIN_DELAY=0 for the
# same reason as product-master above.
start_service inbound-receiving "${BIN_DIR}/inbound-receiving" \
  HTTP_ADDR=":${INBOUND_HTTP_PORT}" \
  DATABASE_URL="${INBOUND_DB_URL}" \
  PGPASSWORD="inbound_receiving" \
  EVENT_PUBLISHER=kafka \
  KAFKA_BROKERS="${KAFKA_BROKERS}" \
  PRODUCT_MODE=kafka \
  PRODUCT_CONSUMER_GROUP="${INBOUND_PRODUCT_CONSUMER_GROUP}" \
  OUTBOX_RELAY_INTERVAL="500ms" \
  SHUTDOWN_DRAIN_DELAY=0 \
  LOG_LEVEL=info
wait_for_http "${INBOUND_BASE_URL}/healthz"

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
  PRODUCT_MASTER_CONSUMER_GROUP="${INVENTORY_PRODUCT_MASTER_CONSUMER_GROUP}" \
  INBOUND_RECEIPT_CONSUMER_GROUP="${INVENTORY_INBOUND_RECEIPT_CONSUMER_GROUP}" \
  TRANSFER_ALLOCATION_CONSUMER_MODE=kafka \
  TRANSFER_ALLOCATION_CONSUMER_GROUP="${INVENTORY_TRANSFER_CONSUMER_GROUP}" \
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
  PRODUCT_CLASSIFICATION_MODE=kafka \
  PRODUCT_CLASSIFICATION_CONSUMER_GROUP="${WES_CLASSIFICATION_CONSUMER_GROUP}" \
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
  PRODUCT_CLASSIFICATION_MODE=kafka \
  PRODUCT_CLASSIFICATION_CONSUMER_GROUP="${FULFILLMENT_CLASSIFICATION_CONSUMER_GROUP}" \
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
  PRODUCT_CLASSIFICATION_MODE=kafka \
  PRODUCT_CLASSIFICATION_CONSUMER_GROUP="${ORDER_CLASSIFICATION_CONSUMER_GROUP}" \
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

log "starting warehouse-planning on ${WAREHOUSE_PLANNING_BASE_URL}"
# warehouse-planning: the real producer of CapacityPlanPublished (one of
# the three fail-closed read-model inputs network-inventory-planning needs).
# Its plans are created through its own REST API by the inter-warehouse
# transfer scenario. EVENT_PUBLISHER=kafka makes its outbox relay publish to
# warehouse.warehouse-planning.events. Its two inbound consumers (labor
# capacity, facility storage tally) have LITERAL default consumer groups, so
# both are overridden with the run-scoped suffix; its order-demand consumer
# stays off (no DEMAND_CONSUMER_GROUP). Plans here are created with an
# explicit assigned_demand, which never reads that consumer's read model.
start_service planning "${BIN_DIR}/planning" \
  HTTP_ADDR=":${WAREHOUSE_PLANNING_HTTP_PORT}" \
  DATABASE_URL="${WAREHOUSE_PLANNING_DB_URL}" \
  PGPASSWORD="planning" \
  MIGRATIONS_PATH="${WAREHOUSE_PLANNING_REPO}/internal/adapters/outbound/postgres/migrations" \
  EVENT_PUBLISHER=kafka \
  KAFKA_BROKERS="${KAFKA_BROKERS}" \
  LABOR_CAPACITY_CONSUMER_GROUP="${WAREHOUSE_PLANNING_LABOR_CONSUMER_GROUP}" \
  STORAGE_CAPACITY_CONSUMER_GROUP="${WAREHOUSE_PLANNING_STORAGE_CONSUMER_GROUP}" \
  OUTBOX_RELAY_INTERVAL="500ms" \
  LOG_LEVEL=info
wait_for_http "${WAREHOUSE_PLANNING_BASE_URL}/healthz"

log "starting network-inventory-planning on ${NIP_BASE_URL}"
# network-inventory-planning (NIP): started after every context it
# exchanges events with. No HTTP calls out; Kafka + its own Postgres. Each
# consumer group env var is its own on-switch (no defaults, so a local
# process can never join the live cluster's group). OUTBOX_RELAY_ENABLED
# turns on the relay that drains approvals / work demands onto
# warehouse.network-inventory-planning.events; without it POST
# /v1/transfers:approve would persist but never emit. TRANSFER_PICK_PATH_ID
# has no default (unset => approve answers 503 config-incomplete).
start_service nip "${BIN_DIR}/nip" \
  HTTP_ADDR=":${NIP_HTTP_PORT}" \
  DATABASE_URL="${NIP_DB_URL}" \
  PGPASSWORD="nip" \
  MIGRATIONS_PATH="${NIP_REPO}/internal/adapters/outbound/postgres/migrations" \
  KAFKA_BROKERS="${KAFKA_BROKERS}" \
  SITE_CAPABILITY_CONSUMER_GROUP="${NIP_CAPABILITY_CONSUMER_GROUP}" \
  SITE_SKU_DEMAND_CONSUMER_GROUP="${NIP_DEMAND_CONSUMER_GROUP}" \
  CAPACITY_PLAN_CONSUMER_GROUP="${NIP_CAPACITY_PLAN_CONSUMER_GROUP}" \
  TRANSFER_REPLY_CONSUMER_GROUP="${NIP_TRANSFER_REPLY_CONSUMER_GROUP}" \
  TRANSFER_FACT_CONSUMER_GROUP="${NIP_TRANSFER_FACT_CONSUMER_GROUP}" \
  OUTBOX_RELAY_ENABLED=true \
  TRANSFER_PICK_PATH_ID="${NIP_TRANSFER_PICK_PATH_ID}" \
  TRANSFER_DISPATCH_PATH_ID="${NIP_TRANSFER_DISPATCH_PATH_ID}" \
  PLANNING_MAX_STALENESS="${NIP_PLANNING_MAX_STALENESS}" \
  LOG_LEVEL=info
wait_for_http "${NIP_BASE_URL}/healthz"

log "all 13 services up and healthy"
printf '  %-24s %s\n' process-path-management "${PROCESS_PATH_BASE_URL}"
printf '  %-24s %s\n' facility-layout        "${FACILITY_BASE_URL}"
printf '  %-24s %s\n' product-master         "${PRODUCT_MASTER_BASE_URL}"
printf '  %-24s %s\n' inbound-receiving      "${INBOUND_BASE_URL}"
printf '  %-24s %s\n' inventory-storage      "${INVENTORY_BASE_URL}"
printf '  %-24s %s\n' wes-work-planning      "${WES_BASE_URL}"
printf '  %-24s %s\n' fulfillment-execution  "${FULFILLMENT_BASE_URL}"
printf '  %-24s %s\n' labor-performance      "${LABOR_BASE_URL}"
printf '  %-24s %s\n' workforce-management   "${WORKFORCE_BASE_URL}"
printf '  %-24s %s\n' order-management       "${ORDER_BASE_URL}"
printf '  %-24s %s\n' network-fulfillment    "${NETWORK_BASE_URL}"
printf '  %-24s %s\n' warehouse-planning     "${WAREHOUSE_PLANNING_BASE_URL}"
printf '  %-24s %s\n' network-inventory-planning "${NIP_BASE_URL}"

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
# PRODUCT_CLASSIFICATION_MODE=kafka here makes the MCP server READ the
# product_classification_copy table cmd/wes maintains (it never consumes
# itself, so it needs no consumer group); "http" would fail its boot.
start_service wes-mcp "${BIN_DIR}/wes-mcp" \
  MCP_ADDR=":${WES_MCP_PORT}" \
  DATABASE_URL="${WES_DB_URL}" \
  PGPASSWORD="wes" \
  PRODUCT_CLASSIFICATION_MODE=kafka \
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
  PATH_CATALOGUE_FILE="${PATH_CATALOGUE_FILE}" \
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
