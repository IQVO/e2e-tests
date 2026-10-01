####################################################################
# e2e-tests/.env  (sourced by every scripts/*.sh — edit ports here only)
####################################################################

# ---- repo layout -------------------------------------------------
WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPOS_ROOT="$(cd "${WORKSPACE_ROOT}/.." && pwd)"

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
PATH_CATALOGUE_FILE="${REPOS_ROOT}/warehouse-infra/config/process-paths/sortable-fc.yaml"
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

FACILITY_BASE_URL="http://localhost:${FACILITY_HTTP_PORT}"
INVENTORY_BASE_URL="http://localhost:${INVENTORY_HTTP_PORT}"
WES_BASE_URL="http://localhost:${WES_HTTP_PORT}"
FULFILLMENT_BASE_URL="http://localhost:${FULFILLMENT_HTTP_PORT}"
WORKFORCE_BASE_URL="http://localhost:${WORKFORCE_HTTP_PORT}"
ORDER_BASE_URL="http://localhost:${ORDER_HTTP_PORT}"
PROCESS_PATH_BASE_URL="http://localhost:${PROCESS_PATH_HTTP_PORT}"
LABOR_BASE_URL="http://localhost:${LABOR_HTTP_PORT}"
NETWORK_BASE_URL="http://localhost:${NETWORK_HTTP_PORT}"

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

# ---- Kafka: single broker platform-wide, owned by the warehouse-infra
#      kind cluster and exposed to the host at localhost:9092 via a
#      Bitnami externalAccess NodePort (warehouse-infra PR #6). This
#      harness does not start its own broker -- see scripts/02-up-infra.sh.
KAFKA_BROKERS="localhost:9092"

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
