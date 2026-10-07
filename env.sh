####################################################################
# e2e-tests/.env  (sourced by every scripts/*.sh — edit ports here only)
####################################################################

# ---- repo layout -------------------------------------------------
WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# REPOS_ROOT is the directory holding every sibling repo checkout. It
# defaults to the parent of this checkout, but is overridable so a git
# worktree of e2e-tests (whose parent is NOT the repos root) can still point
# at the sibling checkouts: REPOS_ROOT=/path/to/repos bash scripts/01-build.sh
REPOS_ROOT="${REPOS_ROOT:-$(cd "${WORKSPACE_ROOT}/.." && pwd)}"

FACILITY_REPO="${REPOS_ROOT}/facility-layout"
INVENTORY_REPO="${REPOS_ROOT}/inventory-storage"
WES_REPO="${REPOS_ROOT}/wes-work-planning"
FULFILLMENT_REPO="${REPOS_ROOT}/fulfillment-execution"
WORKFORCE_REPO="${REPOS_ROOT}/workforce-management"
OPS_AGENT_REPO="${REPOS_ROOT}/warehouse-ops-agent"
ORDER_REPO="${REPOS_ROOT}/order-management"

# The fleet's declared process-path catalogue file (a Published Language,
# owned by warehouse-infra -- see that file's own header comment), read
# at boot by fulfillment-execution/wes-work-planning/workforce-management
# whenever they run with PATH_CATALOGUE_SOURCE=file (this harness's own
# default -- see 03-up-services.sh's header comment). Genuinely required:
# each of those three services' main() treats a missing/malformed
# catalogue as a boot-time fatal error, by design (never falls back to
# an empty catalogue).
#
# This harness points at ITS OWN copy (fixtures/process-paths/sortable-fc.yaml):
# warehouse-infra's four entries plus the dispatch family the inter-warehouse
# transfer saga needs (fulfillment-execution ADR-0036 requires a
# `dispatch-*` family in the catalogue before TRANSFER_DISPATCH work can be
# released; warehouse-infra's frozen file has none). Override
# PATH_CATALOGUE_FILE to run against the infra file verbatim (the transfer
# scenario then fails at the dispatch leg, by design).
PATH_CATALOGUE_FILE="${PATH_CATALOGUE_FILE:-${WORKSPACE_ROOT}/fixtures/process-paths/sortable-fc.yaml}"
# process-path-management (8th bounded context — Generic Subdomain owning
# the fleet's declared process-path catalogue, replacing the static
# config/process-paths/*.yaml file the other five services used to boot
# from — see warehouse-infra's ADR for the Kafka-propagation redesign).
PROCESS_PATH_REPO="${REPOS_ROOT}/process-path-management"
# labor-performance (7th bounded context — engineered labor standards /
# performance scoring, a pure Kafka consumer of fulfillment-execution's
# TaskCompleted event; no HTTP dependency on any other context, so it
# starts after fulfillment-execution purely so its consumer has a real
# topic to subscribe to from the start, not because of a synchronous
# call).
LABOR_REPO="${REPOS_ROOT}/labor-performance"
# network-fulfillment (9th bounded context — Supporting Subdomain / ACL to
# the external retail network, ADR 0001). Started LAST: its own gateway is
# ModeStub by default (no credentials ever needed), and its ONLY outbound
# HTTP dependency is order-management, which must already be up. It
# receives inbound demand entirely by polling a seeded stub file (never
# by an HTTP intake -- there is none, ADR 0001 §5), so this harness feeds
# it via NETWORK_SEED_FILE/PRODUCT_TRANSLATION_FILE fixtures in
# fixtures/network-fulfillment/ rather than an HTTP step.
NETWORK_REPO="${REPOS_ROOT}/network-fulfillment"
# product-master (10th bounded context -- the WMS-tier owner of SKU master
# data: handling classification and the declared/measured physical profile,
# its ADR 0001/0002). Classification moved here from inventory-storage (its
# ADR 0003, inventory-storage ADR 0034): inventory-storage's PUT answers
# 410 classification-moved and every reader (inventory-storage,
# order-management, wes-work-planning, fulfillment-execution) keeps a local
# copy fed by warehouse.product-master.events. Started right after
# facility-layout and BEFORE inventory-storage, so its topic exists by the
# time the consumers subscribe.
PRODUCT_MASTER_REPO="${REPOS_ROOT}/product-master"
# network-inventory-planning (NIP): plans and drives the inter-warehouse
# transfer saga (approve -> allocate at origin -> pick -> dispatch ->
# destination receipt). Started AFTER every service it exchanges events
# with; it has no outbound HTTP at all (Kafka + its own Postgres only).
NIP_REPO="${REPOS_ROOT}/network-inventory-planning"
# warehouse-planning: the producer of CapacityPlanPublished, one of the
# three fail-closed read-model inputs NIP needs before it will simulate or
# approve anything (the other two are facility-layout's SiteCapabilityChanged
# and order-management's SiteSkuDemandChanged -- see
# features/inter_warehouse_transfer.feature's header for which of the three
# this harness can produce from a real service and which it must inject).
WAREHOUSE_PLANNING_REPO="${REPOS_ROOT}/warehouse-planning"

BIN_DIR="${WORKSPACE_ROOT}/bin"
LOG_DIR="${WORKSPACE_ROOT}/logs"
RUN_DIR="${WORKSPACE_ROOT}/run"

# ---- HTTP ports (offset from each repo's own default :8080 so all
#      five can run side by side on one machine) -------------------
FACILITY_HTTP_PORT=8081
INVENTORY_HTTP_PORT=8082
WES_HTTP_PORT=8083
FULFILLMENT_HTTP_PORT=8084
WORKFORCE_HTTP_PORT=8085
# order-management (6th bounded context, choreographed-release redesign —
# see CLAUDE.md's "Cross-service integration" section) — next free slot
# after workforce-management's :8085.
ORDER_HTTP_PORT=8086
# process-path-management (8th bounded context) — next free slot after
# order-management's :8086.
PROCESS_PATH_HTTP_PORT=8087
# labor-performance (7th bounded context) — next free slot after
# process-path-management's :8087.
LABOR_HTTP_PORT=8088
# network-fulfillment (9th bounded context) — next free slot after
# labor-performance's :8088.
NETWORK_HTTP_PORT=8089
# product-master (10th bounded context) — next free slot after
# network-fulfillment's :8089 (8091+ are the MCP ports below).
PRODUCT_MASTER_HTTP_PORT=8090
# network-inventory-planning / warehouse-planning -- 8099 and 8100 are the
# next free slots after the 8091-8098 MCP/agent range and before the
# 8101-8107 analytics reports range below.
NIP_HTTP_PORT=8099
WAREHOUSE_PLANNING_HTTP_PORT=8100

FACILITY_BASE_URL="http://localhost:${FACILITY_HTTP_PORT}"
INVENTORY_BASE_URL="http://localhost:${INVENTORY_HTTP_PORT}"
WES_BASE_URL="http://localhost:${WES_HTTP_PORT}"
FULFILLMENT_BASE_URL="http://localhost:${FULFILLMENT_HTTP_PORT}"
WORKFORCE_BASE_URL="http://localhost:${WORKFORCE_HTTP_PORT}"
ORDER_BASE_URL="http://localhost:${ORDER_HTTP_PORT}"
PROCESS_PATH_BASE_URL="http://localhost:${PROCESS_PATH_HTTP_PORT}"
LABOR_BASE_URL="http://localhost:${LABOR_HTTP_PORT}"
NETWORK_BASE_URL="http://localhost:${NETWORK_HTTP_PORT}"
PRODUCT_MASTER_BASE_URL="http://localhost:${PRODUCT_MASTER_HTTP_PORT}"
NIP_BASE_URL="http://localhost:${NIP_HTTP_PORT}"
WAREHOUSE_PLANNING_BASE_URL="http://localhost:${WAREHOUSE_PLANNING_HTTP_PORT}"

# ---- MCP ports (each context's Streamable-HTTP MCP server, cmd/mcp,
#      alongside its HTTP service above) ----------------------------
FACILITY_MCP_PORT=8091
INVENTORY_MCP_PORT=8092
WES_MCP_PORT=8093
FULFILLMENT_MCP_PORT=8094
WORKFORCE_MCP_PORT=8095
# labor-performance (7th bounded context) -- placed after
# OPS_AGENT_HTTP_PORT (8096) below rather than immediately following
# WORKFORCE_MCP_PORT, since 8096 is already claimed by ops-agent's own
# HTTP port. Registered here for harness parity with the other 5
# contexts' MCP servers; NOT yet wired into warehouse-ops-agent's config
# (no T5 use case consumes it today -- see 03-up-services.sh's own note
# on the labor-mcp block for the full rationale).
LABOR_MCP_PORT=8097
# order-management (6th bounded context) -- next free slot after
# LABOR_MCP_PORT.
ORDER_MCP_PORT=8098

FACILITY_MCP_URL="http://localhost:${FACILITY_MCP_PORT}/mcp"
INVENTORY_MCP_URL="http://localhost:${INVENTORY_MCP_PORT}/mcp"
WES_MCP_URL="http://localhost:${WES_MCP_PORT}/mcp"
FULFILLMENT_MCP_URL="http://localhost:${FULFILLMENT_MCP_PORT}/mcp"
WORKFORCE_MCP_URL="http://localhost:${WORKFORCE_MCP_PORT}/mcp"
LABOR_MCP_URL="http://localhost:${LABOR_MCP_PORT}/mcp"
ORDER_MCP_URL="http://localhost:${ORDER_MCP_PORT}/mcp"

# Fixed test-only bearer read keys, one per context's own MCP server —
# same static-bearer-key scheme every context uses in prod (ADR-0008),
# just a throwaway value for this local harness.
FACILITY_MCP_READ_KEY="e2e-facility-mcp-read-key"
INVENTORY_MCP_READ_KEY="e2e-inventory-mcp-read-key"
WES_MCP_READ_KEY="e2e-wes-mcp-read-key"
FULFILLMENT_MCP_READ_KEY="e2e-fulfillment-mcp-read-key"
WORKFORCE_MCP_READ_KEY="e2e-workforce-mcp-read-key"
LABOR_MCP_READ_KEY="e2e-labor-mcp-read-key"
ORDER_MCP_READ_KEY="e2e-order-mcp-read-key"

# ---- warehouse-ops-agent (the agentic decision-support layer, T5) --
OPS_AGENT_HTTP_PORT=8096
OPS_AGENT_BASE_URL="http://localhost:${OPS_AGENT_HTTP_PORT}"
OPS_AGENT_MCP_READ_KEY="e2e-ops-agent-mcp-read-key"

# ---- Analytics *-reports ports (each context's separate read-only reports
#      binary -- cmd/<svc>-reports, backed by its own analytical Postgres,
#      NOT the OLTP HTTP ports above) -- feeds warehouse-ops-agent's
#      console-bff WMS/WES dashboard fan-out (GET /console/reports/wms and
#      /wes; see internal/config/config.go's *ReportsRESTURL fields).
#
#      New port assignment, not an existing convention: every *-reports
#      binary defaults to the SAME HTTP_ADDR=":8092" today (which also
#      collides with INVENTORY_MCP_PORT above), so running more than one
#      locally already needed a per-service override before this harness
#      ever cared about reports ports. This 8101-8107 range mirrors the
#      8081-8086 OLTP ordering above, shifted by +20, clear of the existing
#      8081-8096 OLTP/MCP/agent range this file already occupies.
FACILITY_REPORTS_HTTP_PORT=8101
INVENTORY_REPORTS_HTTP_PORT=8102
WES_REPORTS_HTTP_PORT=8103
FULFILLMENT_REPORTS_HTTP_PORT=8104
WORKFORCE_REPORTS_HTTP_PORT=8105
ORDER_REPORTS_HTTP_PORT=8106
# labor-performance is not (yet) one of this harness's orchestrated OLTP
# services (no LABOR_REPO/LABOR_HTTP_PORT above) -- its reports port is
# still assigned here, in sequence, purely so
# LABOR_PERFORMANCE_REPORTS_REST_URL lines up with warehouse-ops-agent's
# own env var naming if/when this harness starts that service too.
LABOR_REPORTS_HTTP_PORT=8107

FACILITY_REPORTS_BASE_URL="http://localhost:${FACILITY_REPORTS_HTTP_PORT}"
INVENTORY_REPORTS_BASE_URL="http://localhost:${INVENTORY_REPORTS_HTTP_PORT}"
WES_REPORTS_BASE_URL="http://localhost:${WES_REPORTS_HTTP_PORT}"
FULFILLMENT_REPORTS_BASE_URL="http://localhost:${FULFILLMENT_REPORTS_HTTP_PORT}"
WORKFORCE_REPORTS_BASE_URL="http://localhost:${WORKFORCE_REPORTS_HTTP_PORT}"
ORDER_REPORTS_BASE_URL="http://localhost:${ORDER_REPORTS_HTTP_PORT}"
LABOR_REPORTS_BASE_URL="http://localhost:${LABOR_REPORTS_HTTP_PORT}"

# Maps 1:1 onto warehouse-ops-agent's own env var names, so a local run of
# the agent against this harness's services can source this file directly
# rather than re-deriving the mapping by hand.
FACILITY_LAYOUT_REPORTS_REST_URL="${FACILITY_REPORTS_BASE_URL}"
INVENTORY_STORAGE_REPORTS_REST_URL="${INVENTORY_REPORTS_BASE_URL}"
WES_WORK_PLANNING_REPORTS_REST_URL="${WES_REPORTS_BASE_URL}"
FULFILLMENT_EXECUTION_REPORTS_REST_URL="${FULFILLMENT_REPORTS_BASE_URL}"
WORKFORCE_MANAGEMENT_REPORTS_REST_URL="${WORKFORCE_REPORTS_BASE_URL}"
ORDER_MANAGEMENT_REPORTS_REST_URL="${ORDER_REPORTS_BASE_URL}"
LABOR_PERFORMANCE_REPORTS_REST_URL="${LABOR_REPORTS_BASE_URL}"

# DAILY_BRIEF_PATH_TARGETS override: points the E3 daily-brief synthesis at
# a dedicated T5 process path ("pick-t5-imbalance", building "wh1", shift
# "shift-t5") instead of ops-agent's built-in default ("pick-zone-a",
# "wh1"/"shift-1" — the same identifiers the bootstrap.feature scenario
# already seeds). Using a disjoint path keeps the T5 exception-decisioning
# scenario deterministic and independent of bootstrap.feature's execution
# order/state, per the T5 card's "deterministic seeding" guardrail.
OPS_AGENT_PATH_TARGETS='[{"siteCode":"WH1","pathId":"pick-t5-imbalance","processPath":"PICK","buildingId":"wh1","shiftId":"shift-t5"}]'

# ---- Postgres (docker-compose.yml in this directory) --------------
FACILITY_DB_URL="postgres://facility@localhost:5441/facility?sslmode=disable"
INVENTORY_DB_URL="postgres://inventory@localhost:5442/inventory?sslmode=disable"
WES_DB_URL="postgres://wes@localhost:5443/wes?sslmode=disable"
FULFILLMENT_DB_URL="postgres://fulfillment@localhost:5444/fulfillment_execution?sslmode=disable"
WORKFORCE_DB_URL="postgres://workforce@localhost:5445/workforce?sslmode=disable"
# order-management's own docker-compose.yml defaults to host port 5434 —
# this harness's own e2e-specific offset continues past workforce's :5445
# (avoiding both the 5441-5445 range already in use here AND order-
# management's own :5434 default, per this file's own port-offset
# convention documented in docker-compose.yml's header comment).
ORDER_DB_URL="postgres://order@localhost:5446/order?sslmode=disable"
# process-path-management (8th bounded context) — next free slot after
# order-management's :5446.
PROCESS_PATH_DB_URL="postgres://process_path@localhost:5447/process_path?sslmode=disable"
# labor-performance (7th bounded context) — next free slot after
# process-path-management's :5447.
LABOR_DB_URL="postgres://labor@localhost:5448/labor?sslmode=disable"
# network-fulfillment (9th bounded context) — next free slot after
# labor-performance's :5448.
NETWORK_DB_URL="postgres://network@localhost:5449/network?sslmode=disable"
# product-master (10th bounded context) — next free slot after
# network-fulfillment's :5449. product-master ships no docker-compose of its
# own; user/db "product_master" follows the fleet's <context> naming.
PRODUCT_MASTER_DB_URL="postgres://product_master@localhost:5450/product_master?sslmode=disable"
# network-inventory-planning -- next free slot after network-fulfillment's
# :5449. Own user/db name "nip" (it ships no docker-compose.yml of its own).
NIP_DB_URL="postgres://nip@localhost:5451/nip?sslmode=disable"
# warehouse-planning -- next free slot after NIP's :5451.
WAREHOUSE_PLANNING_DB_URL="postgres://planning@localhost:5452/planning?sslmode=disable"

# ---- Kafka: single broker platform-wide, owned by the warehouse-infra
#      kind cluster and exposed to the host at localhost:9092 via a
#      Bitnami externalAccess NodePort (warehouse-infra PR #6). This
#      harness does not start its own broker -- see scripts/02-up-infra.sh.
#      KAFKA_BROKERS is overridable so a run can target a throwaway broker
#      instead (CI's `services: kafka` container is on :9092 too; a
#      developer who must not touch the shared cluster topics -- the
#      transfer saga publishes and CONSUMES commands on shared topics --
#      starts e.g. an apache/kafka container on :29092 and exports
#      KAFKA_BROKERS=localhost:29092).
KAFKA_BROKERS="${KAFKA_BROKERS:-localhost:9092}"

# ---- Kafka consumer-group isolation --------------------------------
# Consumer-group offsets are SHARED infrastructure state, not per-process
# state. Because the broker above is the same one the warehouse-infra kind
# cluster runs, a harness binary using its service's default group id joins
# the SAME group as that service's live in-cluster Deployment. With one
# partition per topic, Kafka's rebalance protocol awards the partition to
# exactly ONE member -- and the live pod usually wins, leaving the harness
# process healthy but consuming nothing. Scenarios then fail as "condition
# not met within 30s" on a projection that never arrives, which reads like a
# service bug and is not one.
#
# Confirmed concretely: kafka-consumer-groups.sh --describe showed the
# in-cluster wes/fulfillment/labor pods holding every partition of the groups
# this harness needs.
#
# A per-run suffix gives each harness process its own group, so it replays
# the topics itself instead of competing for them. This does NOT change how
# the services behave in the cluster, where sharing one group per service is
# exactly right.
# Overridable: the warehouse-day simulator pins a STABLE suffix so the
# services resume their committed offsets across restarts (as in
# production) instead of replaying the shared broker's whole history on
# every boot. Unset, every run still gets an isolated, unique group.
E2E_CONSUMER_GROUP_SUFFIX="${E2E_CONSUMER_GROUP_SUFFIX:-e2e-$$-$(date +%s)}"
WES_CONSUMER_GROUP="wes-work-planning-${E2E_CONSUMER_GROUP_SUFFIX}"
FULFILLMENT_CONSUMER_GROUP="fulfillment-execution-${E2E_CONSUMER_GROUP_SUFFIX}"
LABOR_CONSUMER_GROUP="labor-performance-${E2E_CONSUMER_GROUP_SUFFIX}"
# product-master's ProductClassified fan-out (product-master ADR 0003 stages
# C and D): one per-run group per local copy, so a harness process never
# joins the live cluster's group for warehouse.product-master.events.
# inventory-storage reads PRODUCT_MASTER_CONSUMER_GROUP; the three readers
# read PRODUCT_CLASSIFICATION_CONSUMER_GROUP (with
# PRODUCT_CLASSIFICATION_MODE=kafka -- "http" is rejected at boot).
INVENTORY_PRODUCT_MASTER_CONSUMER_GROUP="inventory-storage-product-master-${E2E_CONSUMER_GROUP_SUFFIX}"
WES_CLASSIFICATION_CONSUMER_GROUP="wes-work-planning-product-classification-${E2E_CONSUMER_GROUP_SUFFIX}"
FULFILLMENT_CLASSIFICATION_CONSUMER_GROUP="fulfillment-execution-product-classification-${E2E_CONSUMER_GROUP_SUFFIX}"
ORDER_CLASSIFICATION_CONSUMER_GROUP="order-management-product-classification-${E2E_CONSUMER_GROUP_SUFFIX}"

# inventory-storage's transfer allocation command consumer (ADR-0030; dark
# unless TRANSFER_ALLOCATION_CONSUMER_MODE=kafka, set in 03-up-services.sh)
# and network-inventory-planning's five consumers (each group env var is its
# own on-switch; none has a default).
INVENTORY_TRANSFER_CONSUMER_GROUP="inventory-storage-transfer-${E2E_CONSUMER_GROUP_SUFFIX}"
NIP_CAPABILITY_CONSUMER_GROUP="nip-site-capability-${E2E_CONSUMER_GROUP_SUFFIX}"
NIP_DEMAND_CONSUMER_GROUP="nip-site-sku-demand-${E2E_CONSUMER_GROUP_SUFFIX}"
NIP_CAPACITY_PLAN_CONSUMER_GROUP="nip-capacity-plan-${E2E_CONSUMER_GROUP_SUFFIX}"
NIP_TRANSFER_REPLY_CONSUMER_GROUP="nip-transfer-reply-${E2E_CONSUMER_GROUP_SUFFIX}"
NIP_TRANSFER_FACT_CONSUMER_GROUP="nip-transfer-fact-${E2E_CONSUMER_GROUP_SUFFIX}"
# warehouse-planning's two inbound consumers DO have literal defaults, which
# would join the live cluster's groups -- always override.
WAREHOUSE_PLANNING_LABOR_CONSUMER_GROUP="warehouse-planning-labor-${E2E_CONSUMER_GROUP_SUFFIX}"
WAREHOUSE_PLANNING_STORAGE_CONSUMER_GROUP="warehouse-planning-storage-${E2E_CONSUMER_GROUP_SUFFIX}"

# ---- inter-warehouse transfer saga configuration --------------------
# The pick leg's path is `pick-transfer` (inside the existing PICK family,
# matchPrefix `pick`); the dispatch leg's is `dispatch-transfer` (the
# DISPATCH family this harness's catalogue fixture adds). NIP has no
# default for the pick path: unset, POST /v1/transfers:approve answers 503.
NIP_TRANSFER_PICK_PATH_ID="pick-transfer"
NIP_TRANSFER_DISPATCH_PATH_ID="dispatch-transfer"
# NIP refuses to plan from a single stale fact, anywhere in its read models
# (ADR 0002), and this harness's Postgres volumes persist across runs, so a
# row published by an earlier run would otherwise poison the next. A wide
# budget keeps the suite re-runnable; production uses the 10m default.
NIP_PLANNING_MAX_STALENESS="${NIP_PLANNING_MAX_STALENESS:-720h}"

# ---- misc -----------------------------------------------------------
HEALTH_TIMEOUT_SECS=60

# ---- network-fulfillment fixtures ------------------------------------
# The ACL dictionary (network product id -> SKU) and the stub demand this
# harness feeds network-fulfillment's poller. Both live in THIS repo
# (fixtures/network-fulfillment/), not in network-fulfillment's own repo:
# they are e2e-test data this harness owns, the same way
# PATH_CATALOGUE_FILE above is warehouse-infra's fixture, not a sibling
# service's. NETWORK_SEED_FILE's networkRef/site/SKU values are picked to
# match the fixed SKUs this file's own PRODUCT_TRANSLATION_FILE maps, and
# the network-fulfillment.feature scenario receives/stows those same SKUs
# in inventory-storage before asserting on the resulting NetworkOrder.
NETWORK_PRODUCT_TRANSLATION_FILE="${WORKSPACE_ROOT}/fixtures/network-fulfillment/product-translation.json"
NETWORK_SEED_FILE="${WORKSPACE_ROOT}/fixtures/network-fulfillment/seed-demand.json"
