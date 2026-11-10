#!/usr/bin/env bash
# e2e-tests/scripts/03-up-services.sh
#
# Starts all 18 bounded-context HTTP services (+ yard-management's relay worker) as background processes
# against Postgres + Kafka, in dependency order:
#   1. process-path-management — no deps (Generic Subdomain owning the
#                           fleet's declared process-path catalogue;
#                           publishes ProcessPathCreated/Updated/
#                           Deactivated to Kafka, which the other five
#                           publish catalogue changes to Kafka, consumed
#                           live by the four catalogue clients below)
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
#   3a. dock-scheduling   — dock appointment scheduling; OWNS the appointment
#                           writes since inbound-receiving ADR 0019. Four
#                           mandatory consumer groups; its outbox relay is
#                           in-process. Started before inbound-receiving,
#                           whose appointment copy it feeds.
#   3a. yard-management   — gate check-in, dock the trailer, unload, release.
#                           Three mandatory consumer groups; its outbox relay
#                           is a SEPARATE worker, started right after the API.
#   3b. inbound-receiving — the WMS-tier inbound dock workflow (ASN, dock
#                           appointment, receipt; its ADR 0001/0002).
#                           EVENT_PUBLISHER=kafka so ReceiptLineReceived
#                           reaches warehouse.inbound-receiving.events, the
#                           ONLY way inventory-storage (below, its ADR 0037)
#                           learns of a received Good line. It keeps a local
#                           copy of product-master's ProductRegistered
#                           (PRODUCT_MODE=kafka + a per-run
#                           PRODUCT_CONSUMER_GROUP), so it is started right
#                           after product-master. Six mandatory consumer
#                           groups (services.sh); its dock-door copy is fed
#                           by facility-layout's dock slots, so a receipt
#                           needs a declared door.
#   3c. slotting-optimization — the WMS-tier planner of forward pick slots
#                           (SlotPlan, its ADR 0001/0002). Three event-fed
#                           local copies (ADR 0003), each a kafka-mode
#                           consumer with its own per-run group:
#                           LAYOUT_MODE (facility-layout zones and slots),
#                           PRODUCT_MODE (product-master classification and
#                           physical profile) and DEMAND_MODE
#                           (order-management's SiteSkuDemandChanged -- the
#                           scenarios publish those facts themselves, see
#                           slotting_optimization.feature). No DEMAND_SITE_ID:
#                           it would drop demand of any other site, and every
#                           scenario plans its own run-scoped site. Started
#                           after facility-layout and product-master so the
#                           topics it reads exist.
#   3d. returns-processing — the WMS-tier returns context (RMA
#                           authorization against an allocated AND manifested
#                           order line, receipt, inspection, disposition;
#                           its ADR 0001-0003). Three local-copy consumers
#                           (product classification, order lines, package
#                           manifests), each with its own per-run group;
#                           publishes warehouse.returns-processing.events,
#                           whose ReturnLineStaged inventory-storage (below)
#                           books as staged stock (inventory-storage ADR
#                           0044). Started BEFORE inventory-storage so the
#                           topic exists when its RETURN_LINE consumer
#                           subscribes.
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
#                           Its ReturnLineStaged consumer of returns-
#                           processing (RETURN_LINE_CONSUMER_GROUP, ADR 0044)
#                           books RESTOCK lines the same way, and its
#                           TaskCompleted consumer (TASK_COMPLETED_
#                           CONSUMER_GROUP, ADR 0035) confirms picks; both
#                           groups are mandatory env on the service.
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
#                           feasibility, then POST /orders/{id}/releases on
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
#  12. exception-management — the WES-tier exception context (its ADR
#                           0001/0002): one case per consumed exception
#                           fact, the operator lifecycle (triage, assign,
#                           resolve) and declarative auto-resolve
#                           (ExceptionPolicyDeclared rules). Six fact
#                           consumers (fulfillment, work-planning, network-
#                           fulfillment, order correlation, inventory,
#                           returns), each with its own per-run group; it
#                           calls no sibling over REST. Started LAST: it
#                           watches facts from nearly every other context,
#                           and every topic it reads exists by then.
#
# All eight publisher-capable services run with EVENT_PUBLISHER=kafka
# against the shared broker (labor-performance is the exception -- it has
# no EVENT_PUBLISHER flag at all, being a pure consumer, though it still
# needs KAFKA_BROKERS to build its consumer group) so the cross-context
# event flow (WorkReleased -> Task, TaskCompleted -> WorkUnit completion /
# labor-performance scoring, ShiftPlanCommitted -> labor-plan-view,
# OrderAllocated/OrderPartiallyAllocated -> WorkUnit) is exercised for
# process-path-management catalogue events are seeded before consumers start,
# replayed by four Kafka-fed clients, and then applied live without restarts.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib.sh"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/services.sh"

# The environment of every service lives in services.sh (one up_<service>
# function each, shared with the CI workflow); this script only fixes the
# dependency order described in the header above.
up_process_path
up_facility
up_product_master
up_dock_scheduling
up_yard_management
up_yard_management_relay
up_inbound_receiving
up_slotting
up_returns
up_inventory
up_wes
up_execution
up_labor
up_workforce
up_order
up_network
up_planning
up_nip
up_exceptions

log "all 18 services (+ the yard relay worker) up and healthy"
printf '  %-24s %s\n' process-path-management "${PROCESS_PATH_BASE_URL}"
printf '  %-24s %s\n' facility-layout        "${FACILITY_BASE_URL}"
printf '  %-24s %s\n' product-master         "${PRODUCT_MASTER_BASE_URL}"
printf '  %-24s %s\n' dock-scheduling        "${DOCK_SCHEDULING_BASE_URL}"
printf '  %-24s %s\n' yard-management        "${YARD_MANAGEMENT_BASE_URL}"
printf '  %-24s %s\n' yard-management-relay  "${YARD_MANAGEMENT_RELAY_HEALTH_URL}"
printf '  %-24s %s\n' inbound-receiving      "${INBOUND_BASE_URL}"
printf '  %-24s %s\n' slotting-optimization   "${SLOTTING_BASE_URL}"
printf '  %-24s %s\n' returns-processing     "${RETURNS_BASE_URL}"
printf '  %-24s %s\n' inventory-storage      "${INVENTORY_BASE_URL}"
printf '  %-24s %s\n' wes-work-planning      "${WES_BASE_URL}"
printf '  %-24s %s\n' fulfillment-execution  "${FULFILLMENT_BASE_URL}"
printf '  %-24s %s\n' labor-performance      "${LABOR_BASE_URL}"
printf '  %-24s %s\n' workforce-management   "${WORKFORCE_BASE_URL}"
printf '  %-24s %s\n' order-management       "${ORDER_BASE_URL}"
printf '  %-24s %s\n' network-fulfillment    "${NETWORK_BASE_URL}"
printf '  %-24s %s\n' warehouse-planning     "${WAREHOUSE_PLANNING_BASE_URL}"
printf '  %-24s %s\n' network-inventory-planning "${NIP_BASE_URL}"
printf '  %-24s %s\n' exception-management   "${EXCEPTIONS_BASE_URL}"

# --- MCP servers (cmd/mcp), one per context, pointed at the SAME
# Postgres each HTTP service above just started against — so a fact an
# HTTP call writes (e.g. a shift plan, a work pool) is immediately
# visible to warehouse-ops-agent's MCP-tool reads. Every one of these
# serves unauthenticated Streamable HTTP. -----------------------------

log "starting facility-layout MCP server on :${FACILITY_MCP_PORT}"
start_service facility-mcp "${BIN_DIR}/facility-mcp" \
  MCP_ADDR=":${FACILITY_MCP_PORT}" \
  DATABASE_URL="${FACILITY_DB_URL}" \
  PGPASSWORD="facility" \
  MIGRATIONS_PATH="${FACILITY_REPO}/migrations" \
  LOG_LEVEL=info
wait_for_tcp localhost "${FACILITY_MCP_PORT}"

log "starting inventory-storage MCP server on :${INVENTORY_MCP_PORT}"
start_service inventory-mcp "${BIN_DIR}/inventory-mcp" \
  MCP_ADDR=":${INVENTORY_MCP_PORT}" \
  DATABASE_URL="${INVENTORY_DB_URL}" \
  PGPASSWORD="inventory" \
  MIGRATIONS_PATH="${INVENTORY_REPO}/migrations" \
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
  LOG_LEVEL=info
wait_for_tcp localhost "${WES_MCP_PORT}"

log "starting fulfillment-execution MCP server on :${FULFILLMENT_MCP_PORT}"
# Same relative-migrations-path quirk as cmd/execution (see
# 03-up-services.sh above): must launch with CWD set to the repo root.
start_service_in execution-mcp "${FULFILLMENT_REPO}" "${BIN_DIR}/execution-mcp" \
  MCP_ADDR=":${FULFILLMENT_MCP_PORT}" \
  DATABASE_URL="${FULFILLMENT_DB_URL}" \
  PGPASSWORD="fulfillment" \
  LOG_LEVEL=info
wait_for_tcp localhost "${FULFILLMENT_MCP_PORT}"

log "starting workforce-management MCP server on :${WORKFORCE_MCP_PORT}"
start_service workforce-mcp "${BIN_DIR}/workforce-mcp" \
  MCP_ADDR=":${WORKFORCE_MCP_PORT}" \
  DATABASE_URL="${WORKFORCE_DB_URL}" \
  PGPASSWORD="workforce" \
  MIGRATIONS_PATH="${WORKFORCE_REPO}/migrations" \
  PATH_CATALOGUE_FILE="${PATH_CATALOGUE_FILE}" \
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
  LOG_LEVEL=info
wait_for_tcp localhost "${ORDER_MCP_PORT}"

log "all 7 MCP servers up"

# --- warehouse-ops-agent (T5): the agentic analyze/act layer, wired to
# the 5 MCP servers just started above as its only upstream dependency. --

log "starting warehouse-ops-agent on ${OPS_AGENT_BASE_URL}"
start_service ops-agent "${BIN_DIR}/ops-agent" \
  AGENT_ADDR=":${OPS_AGENT_HTTP_PORT}" \
  WES_WORK_PLANNING_MCP_ENDPOINT="${WES_MCP_URL}" \
  FULFILLMENT_EXECUTION_MCP_ENDPOINT="${FULFILLMENT_MCP_URL}" \
  INVENTORY_STORAGE_MCP_ENDPOINT="${INVENTORY_MCP_URL}" \
  WORKFORCE_MANAGEMENT_MCP_ENDPOINT="${WORKFORCE_MCP_URL}" \
  FACILITY_LAYOUT_MCP_ENDPOINT="${FACILITY_MCP_URL}" \
  DAILY_BRIEF_PATH_TARGETS="${OPS_AGENT_PATH_TARGETS}" \
  LOG_LEVEL=info
wait_for_http "${OPS_AGENT_BASE_URL}/healthz"

log "warehouse-ops-agent up"
printf '  %-24s %s\n' warehouse-ops-agent "${OPS_AGENT_BASE_URL}"
