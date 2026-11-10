#!/usr/bin/env bash
# e2e-tests/scripts/services.sh -- one up_<service> function per bounded-context
# HTTP service: the SINGLE definition of the environment each binary is started
# with. Sourced (after lib.sh) by scripts/03-up-services.sh for the whole
# estate and by every job of .github/workflows/e2e-behaviour.yml for the subset
# that job checks out. Not runnable on its own.
#
# Why one definition: the workflow used to carry its own copy of every
# service's env per job. Each service has since moved to the real-wiring
# standard (DATABASE_URL, KAFKA_BROKERS and every *_CONSUMER_GROUP mandatory,
# no permissive mode) and keeps gaining consumers, so every new mandatory
# variable had to be added in N+1 places and the copies drifted: all five jobs
# of the 2026-11-09 run died at boot on a group env var one copy lacked. A
# service that gains a mandatory variable is now fixed here, once.
#
# Every function logs, starts the binary under scripts/lib.sh's start_service
# (log in ${LOG_DIR}/<name>.log, pid in ${RUN_DIR}/pids) and waits for its
# /healthz. Required env of each service: its cmd/*/config.go on develop.
# shellcheck shell=bash

[[ -n "${BIN_DIR:-}" ]] || { echo "services.sh: source scripts/lib.sh first" >&2; return 1; }
SERVICES_SH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

up_process_path() {
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
  bash "${SERVICES_SH_DIR}/seed-process-paths.sh"
}

up_facility() {
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
}

# product-master: cmd/api, migrations embedded in the binary (no
# MIGRATIONS_PATH). SHUTDOWN_DRAIN_DELAY=0 so stop_service's bounded wait is
# not spent idling on the default 5s readiness drain. Its legacy importer
# (LEGACY_IMPORT_CONSUMER_GROUP) is NOT started: it is a one-off migration
# aid, and on a shared broker it would replay the cluster's whole inventory
# history into this local database.
up_product_master() {
  log "starting product-master on ${PRODUCT_MASTER_BASE_URL}"
  start_service product-master "${BIN_DIR}/product-master" \
    HTTP_ADDR=":${PRODUCT_MASTER_HTTP_PORT}" \
    DATABASE_URL="${PRODUCT_MASTER_DB_URL}" \
    PGPASSWORD="product_master" \
    EVENT_PUBLISHER=kafka \
    KAFKA_BROKERS="${KAFKA_BROKERS}" \
    SHUTDOWN_DRAIN_DELAY=0 \
    LOG_LEVEL=info
  wait_for_http "${PRODUCT_MASTER_BASE_URL}/healthz"
}

# inbound-receiving: cmd/api, migrations embedded. Six mandatory consumer
# groups (product, dock door, packaging, dock appointment, visit arrival,
# carton stow); PRODUCT_MODE and DOCK_DOOR_MODE may only be kafka or unset (a
# stale "DOCK_DOOR_MODE=permissive" is a boot error). Its dock-door copy is fed
# by facility-layout's dock slots, its appointment copy by dock-scheduling and
# its arrival copy by yard-management, so a scenario that opens a receipt
# against an appointment needs those three running.
up_inbound_receiving() {
  log "starting inbound-receiving on ${INBOUND_BASE_URL}"
  start_service inbound-receiving "${BIN_DIR}/inbound-receiving" \
    HTTP_ADDR=":${INBOUND_HTTP_PORT}" \
    DATABASE_URL="${INBOUND_DB_URL}" \
    PGPASSWORD="inbound_receiving" \
    EVENT_PUBLISHER=kafka \
    KAFKA_BROKERS="${KAFKA_BROKERS}" \
    PRODUCT_MODE=kafka \
    PRODUCT_CONSUMER_GROUP="${INBOUND_PRODUCT_CONSUMER_GROUP}" \
    DOCK_DOOR_CONSUMER_GROUP="${INBOUND_DOCK_DOOR_CONSUMER_GROUP}" \
    PACKAGING_CONSUMER_GROUP="${INBOUND_PACKAGING_CONSUMER_GROUP}" \
    DOCK_APPOINTMENT_CONSUMER_GROUP="${INBOUND_DOCK_APPOINTMENT_CONSUMER_GROUP}" \
    VISIT_ARRIVAL_CONSUMER_GROUP="${INBOUND_VISIT_ARRIVAL_CONSUMER_GROUP}" \
    CARTON_STOW_CONSUMER_GROUP="${INBOUND_CARTON_STOW_CONSUMER_GROUP}" \
    OUTBOX_RELAY_INTERVAL="500ms" \
    SHUTDOWN_DRAIN_DELAY=0 \
    LOG_LEVEL=info
  wait_for_http "${INBOUND_BASE_URL}/healthz"
}

# slotting-optimization: cmd/api, migrations embedded. Five always-on local-copy
# consumers (demand, product, layout, relocation, pack level), each with its
# own per-run group. FORWARD_ZONE_CODES stays at its default (FWD).
up_slotting() {
  log "starting slotting-optimization on ${SLOTTING_BASE_URL}"
  start_service slotting-optimization "${BIN_DIR}/slotting-optimization" \
    HTTP_ADDR=":${SLOTTING_HTTP_PORT}" \
    DATABASE_URL="${SLOTTING_DB_URL}" \
    PGPASSWORD="slotting" \
    EVENT_PUBLISHER=kafka \
    KAFKA_BROKERS="${KAFKA_BROKERS}" \
    DEMAND_MODE=kafka \
    DEMAND_CONSUMER_GROUP="${SLOTTING_DEMAND_CONSUMER_GROUP}" \
    PRODUCT_MODE=kafka \
    PRODUCT_CONSUMER_GROUP="${SLOTTING_PRODUCT_CONSUMER_GROUP}" \
    LAYOUT_MODE=kafka \
    LAYOUT_CONSUMER_GROUP="${SLOTTING_LAYOUT_CONSUMER_GROUP}" \
    RELOCATION_MODE=kafka \
    RELOCATION_CONSUMER_GROUP="${SLOTTING_RELOCATION_CONSUMER_GROUP}" \
    PACK_LEVEL_MODE=kafka \
    PACK_LEVEL_CONSUMER_GROUP="${SLOTTING_PACK_LEVEL_CONSUMER_GROUP}" \
    OUTBOX_RELAY_INTERVAL="500ms" \
    SHUTDOWN_DRAIN_DELAY=0 \
    LOG_LEVEL=info
  wait_for_http "${SLOTTING_BASE_URL}/healthz"
}

# returns-processing: cmd/api, migrations embedded. Three mandatory
# local-copy consumer groups. Start it BEFORE inventory-storage so
# warehouse.returns-processing.events exists when its RETURN_LINE consumer
# subscribes.
up_returns() {
  log "starting returns-processing on ${RETURNS_BASE_URL}"
  start_service returns "${BIN_DIR}/returns" \
    HTTP_ADDR=":${RETURNS_HTTP_PORT}" \
    DATABASE_URL="${RETURNS_DB_URL}" \
    PGPASSWORD="returns_processing" \
    KAFKA_BROKERS="${KAFKA_BROKERS}" \
    PRODUCT_CLASSIFICATION_CONSUMER_GROUP="${RETURNS_PRODUCT_CLASSIFICATION_CONSUMER_GROUP}" \
    ORDER_LINES_CONSUMER_GROUP="${RETURNS_ORDER_LINES_CONSUMER_GROUP}" \
    MANIFEST_CONSUMER_GROUP="${RETURNS_MANIFEST_CONSUMER_GROUP}" \
    OUTBOX_RELAY_INTERVAL="500ms" \
    SHUTDOWN_DRAIN_DELAY=0 \
    LOG_LEVEL=info
  wait_for_http "${RETURNS_BASE_URL}/healthz"
}

# inventory-storage: DATABASE_URL, KAFKA_BROKERS, LOCATION_LOOKUP_MODE
# (kafka|http; permissive and unset were removed) and FIVE mandatory
# consumer groups (transfer allocation, product master, inbound receipt,
# return line, task completed). FACILITY_LAYOUT_BASE_URL is only read in
# http mode; it is exported so one word flips the rollback.
up_inventory() {
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
    RETURN_LINE_CONSUMER_GROUP="${INVENTORY_RETURN_LINE_CONSUMER_GROUP}" \
    TASK_COMPLETED_CONSUMER_GROUP="${INVENTORY_TASK_COMPLETED_CONSUMER_GROUP}" \
    TRANSFER_ALLOCATION_CONSUMER_MODE=kafka \
    TRANSFER_ALLOCATION_CONSUMER_GROUP="${INVENTORY_TRANSFER_CONSUMER_GROUP}" \
    LOG_LEVEL=info
  wait_for_http "${INVENTORY_BASE_URL}/healthz"
}

# wes-work-planning: process-path catalogue from Kafka (process-path-management
# must be up and seeded), product classification and pack-view copies of
# product-master, pick-face replenishment consumer, and the integration-event
# consumer group.
up_wes() {
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
    PACK_VIEW_CONSUMER_GROUP="${WES_PACK_VIEW_CONSUMER_GROUP}" \
    REPLENISHMENT_CONSUMER_GROUP="${WES_REPLENISHMENT_CONSUMER_GROUP}" \
    PATH_CATALOGUE_SOURCE=kafka \
    LOG_LEVEL=info
  wait_for_http "${WES_BASE_URL}/healthz"
}

# fulfillment-execution's main() hardcodes postgres.Migrate(databaseURL,
# "migrations") -- a relative path, unlike the other services which read
# MIGRATIONS_PATH from the environment -- so it must be launched with its CWD
# set to the repo root for that relative path to resolve. Its pack-view group
# must differ from its classification group (both read
# warehouse.product-master.events); INVENTORY_STORAGE_BASE_URL is mandatory
# (a BREAK_CASE task completes through inventory-storage).
up_execution() {
  log "starting fulfillment-execution on ${FULFILLMENT_BASE_URL}"
  start_service_in execution "${FULFILLMENT_REPO}" "${BIN_DIR}/execution" \
    HTTP_ADDR=":${FULFILLMENT_HTTP_PORT}" \
    DATABASE_URL="${FULFILLMENT_DB_URL}" \
    PGPASSWORD="fulfillment" \
    EVENT_PUBLISHER=kafka \
    KAFKA_BROKERS="${KAFKA_BROKERS}" \
    WORK_RELEASED_CONSUMER_GROUP="${FULFILLMENT_CONSUMER_GROUP}" \
    PRODUCT_CLASSIFICATION_MODE=kafka \
    PRODUCT_CLASSIFICATION_CONSUMER_GROUP="${FULFILLMENT_CLASSIFICATION_CONSUMER_GROUP}" \
    PACK_VIEW_CONSUMER_GROUP="${FULFILLMENT_PACK_VIEW_CONSUMER_GROUP}" \
    TASK_INTERLEAVING_CONSUMER_GROUP="${FULFILLMENT_TASK_INTERLEAVING_CONSUMER_GROUP}" \
    INVENTORY_STORAGE_BASE_URL="${INVENTORY_BASE_URL}" \
    PATH_CATALOGUE_SOURCE=kafka \
    LOG_LEVEL=info
  wait_for_http "${FULFILLMENT_BASE_URL}/healthz"
}

# labor-performance: a pure Kafka consumer of fulfillment-execution's
# TaskCompleted (no EVENT_PUBLISHER flag at all).
up_labor() {
  log "starting labor-performance on ${LABOR_BASE_URL}"
  start_service labor "${BIN_DIR}/labor" \
    HTTP_ADDR=":${LABOR_HTTP_PORT}" \
    DATABASE_URL="${LABOR_DB_URL}" \
    PGPASSWORD="labor" \
    MIGRATIONS_PATH="${LABOR_REPO}/migrations" \
    KAFKA_BROKERS="${KAFKA_BROKERS}" \
    KAFKA_CONSUMER_GROUP="${LABOR_CONSUMER_GROUP}" \
    LOG_LEVEL=info
  wait_for_http "${LABOR_BASE_URL}/healthz"
}

# workforce-management: PATH_CATALOGUE_SOURCE=file is REJECTED at boot (the
# catalogue is replayed from process-path-management's topic, so that service
# must be up and seeded before it commits a shift plan).
up_workforce() {
  log "starting workforce-management on ${WORKFORCE_BASE_URL}"
  start_service workforce "${BIN_DIR}/workforce" \
    HTTP_ADDR=":${WORKFORCE_HTTP_PORT}" \
    DATABASE_URL="${WORKFORCE_DB_URL}" \
    PGPASSWORD="workforce" \
    MIGRATIONS_PATH="${WORKFORCE_REPO}/migrations" \
    EVENT_PUBLISHER=kafka \
    KAFKA_BROKERS="${KAFKA_BROKERS}" \
    PATH_CATALOGUE_SOURCE=kafka \
    INSTALLED_CAPACITY_MODE=http \
    FULFILLMENT_EXECUTION_BASE_URL="${FULFILLMENT_BASE_URL}" \
    LOG_LEVEL=info
  wait_for_http "${WORKFORCE_BASE_URL}/healthz"
}

# order-management: calls inventory-storage over HTTP (synchronous
# allocation), reads a local classification copy and publishes
# OrderAllocated / OrderPartiallyAllocated, which wes-work-planning turns into
# a work unit. No WES_WORK_PLANNING_BASE_URL: the HTTP release path is gone.
up_order() {
  log "starting order-management on ${ORDER_BASE_URL}"
  start_service order "${BIN_DIR}/order" \
    HTTP_ADDR=":${ORDER_HTTP_PORT}" \
    DATABASE_URL="${ORDER_DB_URL}" \
    PGPASSWORD="order" \
    MIGRATIONS_PATH="${ORDER_REPO}/migrations" \
    EVENT_PUBLISHER=kafka \
    KAFKA_BROKERS="${KAFKA_BROKERS}" \
    PATH_CATALOGUE_SOURCE=kafka \
    INVENTORY_STORAGE_MODE=http \
    INVENTORY_STORAGE_BASE_URL="${INVENTORY_BASE_URL}" \
    PRODUCT_CLASSIFICATION_MODE=kafka \
    PRODUCT_CLASSIFICATION_CONSUMER_GROUP="${ORDER_CLASSIFICATION_CONSUMER_GROUP}" \
    LOG_LEVEL=info
  wait_for_http "${ORDER_BASE_URL}/healthz"
}

# network-fulfillment: the anti-corruption layer to the external retail
# network. NETWORK_MODE stays at its default (stub): the harness must never
# need real network credentials. Inbound demand arrives ONLY via
# NETWORK_SEED_FILE; PRODUCT_TRANSLATION_FILE is the ACL dictionary.
up_network() {
  log "starting network-fulfillment on ${NETWORK_BASE_URL}"
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
}

# warehouse-planning: producer of CapacityPlanPublished. Every consumer group
# is mandatory (labor capacity, storage capacity, order demand, dock
# appointment, observed capacity) plus the one DEMAND_SITE_ID the consumed
# orders and task completions are attributed to.
up_planning() {
  log "starting warehouse-planning on ${WAREHOUSE_PLANNING_BASE_URL}"
  start_service planning "${BIN_DIR}/planning" \
    HTTP_ADDR=":${WAREHOUSE_PLANNING_HTTP_PORT}" \
    DATABASE_URL="${WAREHOUSE_PLANNING_DB_URL}" \
    PGPASSWORD="planning" \
    MIGRATIONS_PATH="${WAREHOUSE_PLANNING_REPO}/internal/adapters/outbound/postgres/migrations" \
    EVENT_PUBLISHER=kafka \
    KAFKA_BROKERS="${KAFKA_BROKERS}" \
    LABOR_CAPACITY_CONSUMER_GROUP="${WAREHOUSE_PLANNING_LABOR_CONSUMER_GROUP}" \
    STORAGE_CAPACITY_CONSUMER_GROUP="${WAREHOUSE_PLANNING_STORAGE_CONSUMER_GROUP}" \
    DEMAND_CONSUMER_GROUP="${WAREHOUSE_PLANNING_DEMAND_CONSUMER_GROUP}" \
    DOCK_APPOINTMENT_CONSUMER_GROUP="${WAREHOUSE_PLANNING_DOCK_APPOINTMENT_CONSUMER_GROUP}" \
    OBSERVED_CAPACITY_CONSUMER_GROUP="${WAREHOUSE_PLANNING_OBSERVED_CAPACITY_CONSUMER_GROUP}" \
    DEMAND_SITE_ID="${WAREHOUSE_PLANNING_DEMAND_SITE_ID}" \
    OUTBOX_RELAY_INTERVAL="500ms" \
    LOG_LEVEL=info
  wait_for_http "${WAREHOUSE_PLANNING_BASE_URL}/healthz"
}

# network-inventory-planning: Kafka + its own Postgres, no HTTP out. Each
# consumer group env var is its own on-switch (no defaults) and the pack-level
# group is mandatory once KAFKA_BROKERS is set. OUTBOX_RELAY_ENABLED turns on
# the relay that drains approvals / work demands. TRANSFER_PICK_PATH_ID has no
# default (unset => approve answers 503 config-incomplete).
up_nip() {
  log "starting network-inventory-planning on ${NIP_BASE_URL}"
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
    PACK_LEVEL_CONSUMER_GROUP="${NIP_PACK_LEVEL_CONSUMER_GROUP}" \
    OUTBOX_RELAY_ENABLED=true \
    TRANSFER_PICK_PATH_ID="${NIP_TRANSFER_PICK_PATH_ID}" \
    TRANSFER_DISPATCH_PATH_ID="${NIP_TRANSFER_DISPATCH_PATH_ID}" \
    PLANNING_MAX_STALENESS="${NIP_PLANNING_MAX_STALENESS}" \
    LOG_LEVEL=info
  wait_for_http "${NIP_BASE_URL}/healthz"
}

# exception-management: cmd/api, migrations embedded. TEN mandatory fact
# consumer groups (fulfillment, work planning, network fulfillment, order
# correlation, inventory, returns, carton discrepancy, yard management, dock
# scheduling, outbound shipping), all per-run. SLA_SWEEP_INTERVAL stays at its
# 1m default (escalation is not under test). Started LAST: it watches facts
# from nearly every other context.
up_exceptions() {
  log "starting exception-management on ${EXCEPTIONS_BASE_URL}"
  start_service exceptions "${BIN_DIR}/exceptions" \
    HTTP_ADDR=":${EXCEPTIONS_HTTP_PORT}" \
    DATABASE_URL="${EXCEPTIONS_DB_URL}" \
    PGPASSWORD="exception_management" \
    KAFKA_BROKERS="${KAFKA_BROKERS}" \
    FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP="${EXCEPTIONS_FULFILLMENT_CONSUMER_GROUP}" \
    WORK_PLANNING_EXCEPTIONS_CONSUMER_GROUP="${EXCEPTIONS_WORK_PLANNING_CONSUMER_GROUP}" \
    NETWORK_FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP="${EXCEPTIONS_NETWORK_FULFILLMENT_CONSUMER_GROUP}" \
    ORDER_CORRELATION_CONSUMER_GROUP="${EXCEPTIONS_ORDER_CORRELATION_CONSUMER_GROUP}" \
    INVENTORY_EXCEPTIONS_CONSUMER_GROUP="${EXCEPTIONS_INVENTORY_CONSUMER_GROUP}" \
    RETURNS_EXCEPTIONS_CONSUMER_GROUP="${EXCEPTIONS_RETURNS_CONSUMER_GROUP}" \
    CARTON_DISCREPANCY_CONSUMER_GROUP="${EXCEPTIONS_CARTON_DISCREPANCY_CONSUMER_GROUP}" \
    YARD_MANAGEMENT_EXCEPTIONS_CONSUMER_GROUP="${EXCEPTIONS_YARD_MANAGEMENT_CONSUMER_GROUP}" \
    DOCK_SCHEDULING_EXCEPTIONS_CONSUMER_GROUP="${EXCEPTIONS_DOCK_SCHEDULING_CONSUMER_GROUP}" \
    OUTBOUND_SHIPPING_EXCEPTIONS_CONSUMER_GROUP="${EXCEPTIONS_OUTBOUND_SHIPPING_CONSUMER_GROUP}" \
    OUTBOX_RELAY_INTERVAL="500ms" \
    SHUTDOWN_DRAIN_DELAY=0 \
    LOG_LEVEL=info
  wait_for_http "${EXCEPTIONS_BASE_URL}/healthz"
}

# dock-scheduling: cmd/api, migrations embedded. It OWNS the appointment
# writes since inbound-receiving ADR 0019 (book, reschedule, cancel). FOUR
# mandatory consumer groups (yard-management facts, facility-layout dock slots
# and operating hours, inbound-receiving's ASN, process-path-management's
# cut-off times). The transactional-outbox relay runs in this process (its
# cmd/relay is the same loop as a stand-alone binary), so there is no second
# worker to start. DOCK_LEGACY_IMPORT_CONSUMER_GROUP stays unset: that is the
# strangler stage A importer and would replay a shared broker's history.
up_dock_scheduling() {
  log "starting dock-scheduling on ${DOCK_SCHEDULING_BASE_URL}"
  start_service dock-scheduling "${BIN_DIR}/dock-scheduling" \
    HTTP_ADDR=":${DOCK_SCHEDULING_HTTP_PORT}" \
    DATABASE_URL="${DOCK_SCHEDULING_DB_URL}" \
    PGPASSWORD="dock_scheduling" \
    KAFKA_BROKERS="${KAFKA_BROKERS}" \
    YARD_CONSUMER_GROUP="${DOCK_YARD_CONSUMER_GROUP}" \
    FACILITY_CONSUMER_GROUP="${DOCK_FACILITY_CONSUMER_GROUP}" \
    ASN_CONSUMER_GROUP="${DOCK_ASN_CONSUMER_GROUP}" \
    CPT_CONSUMER_GROUP="${DOCK_CPT_CONSUMER_GROUP}" \
    OUTBOX_RELAY_INTERVAL="500ms" \
    SHUTDOWN_DRAIN_DELAY=0 \
    LOG_LEVEL=info
  wait_for_http "${DOCK_SCHEDULING_BASE_URL}/healthz"
}

# yard-management: cmd/api, migrations embedded. THREE mandatory consumer
# groups (dock-scheduling's appointments, facility-layout, inbound-receiving's
# ReceiptClosed, which drives the auto-complete of the unload). Unlike
# dock-scheduling its outbox relay is a SEPARATE binary (up_yard_management_relay,
# its own Deployment in the chart): without it VisitCheckedIn and every other
# yard fact stays in the outbox.
up_yard_management() {
  log "starting yard-management on ${YARD_BASE_URL}"
  start_service yard-management "${BIN_DIR}/yard-management" \
    HTTP_ADDR=":${YARD_HTTP_PORT}" \
    DATABASE_URL="${YARD_DB_URL}" \
    PGPASSWORD="yard_management" \
    KAFKA_BROKERS="${KAFKA_BROKERS}" \
    DOCK_SCHEDULING_CONSUMER_GROUP="${YARD_DOCK_CONSUMER_GROUP}" \
    FACILITY_CONSUMER_GROUP="${YARD_FACILITY_CONSUMER_GROUP}" \
    INBOUND_RECEIVING_CONSUMER_GROUP="${YARD_INBOUND_CONSUMER_GROUP}" \
    SHUTDOWN_DRAIN_DELAY=0 \
    LOG_LEVEL=info
  wait_for_http "${YARD_BASE_URL}/healthz"
}

# yard-management's outbox relay worker. It serves /healthz on RELAY_HEALTH_ADDR
# (default :8081, which is facility-layout's port here, hence its own).
# Started AFTER the API: the API owns the (embedded) migrations of the outbox
# table the relay drains.
up_yard_management_relay() {
  log "starting yard-management outbox relay on ${YARD_RELAY_BASE_URL}"
  start_service yard-relay "${BIN_DIR}/yard-relay" \
    DATABASE_URL="${YARD_DB_URL}" \
    PGPASSWORD="yard_management" \
    KAFKA_BROKERS="${KAFKA_BROKERS}" \
    OUTBOX_POLL_INTERVAL="500ms" \
    RELAY_HEALTH_ADDR=":${YARD_RELAY_HEALTH_PORT}" \
    LOG_LEVEL=info
  wait_for_http "${YARD_RELAY_BASE_URL}/healthz"
}

# outbound-shipping: cmd/api, migrations embedded. SIX mandatory consumer groups
# (fulfillment-execution, dock-scheduling, yard-management, CPT schedules,
# facility-layout, analytics), all per-run. The outbox relay runs in this
# process (it publishes warehouse.outbound-shipping.events and .analytics, both
# pre-created by cmd/ensure-topics). The listen address variable is ADDR, not
# HTTP_ADDR.
up_outbound_shipping() {
  log "starting outbound-shipping on ${OUTBOUND_SHIPPING_BASE_URL}"
  start_service outbound-shipping "${BIN_DIR}/outbound-shipping" \
    ADDR=":${OUTBOUND_SHIPPING_HTTP_PORT}" \
    DATABASE_URL="${OUTBOUND_SHIPPING_DB_URL}" \
    PGPASSWORD="outbound_shipping" \
    KAFKA_BROKERS="${KAFKA_BROKERS}" \
    FULFILLMENT_CONSUMER_GROUP="${OUTBOUND_FULFILLMENT_CONSUMER_GROUP}" \
    DOCK_CONSUMER_GROUP="${OUTBOUND_DOCK_CONSUMER_GROUP}" \
    YARD_CONSUMER_GROUP="${OUTBOUND_YARD_CONSUMER_GROUP}" \
    CPT_CONSUMER_GROUP="${OUTBOUND_CPT_CONSUMER_GROUP}" \
    LAYOUT_CONSUMER_GROUP="${OUTBOUND_LAYOUT_CONSUMER_GROUP}" \
    ANALYTICS_CONSUMER_GROUP="${OUTBOUND_ANALYTICS_CONSUMER_GROUP}" \
    OUTBOX_INTERVAL_MS=250 \
    LOG_LEVEL=info
  wait_for_http "${OUTBOUND_SHIPPING_BASE_URL}/healthz"
}
