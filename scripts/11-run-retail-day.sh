#!/usr/bin/env bash
# e2e-tests/scripts/11-run-retail-day.sh
#
# The retail day: shoppers, the network and the warehouse floor in one run, on the k3s cluster
# through Kong (docs/adr/0001-retail-day.md, docs/retail-day-journey.md).
#
#   bash scripts/11-run-retail-day.sh --preflight   # make retail-day-preflight: gate + read-only checks, writes nothing
#   bash scripts/11-run-retail-day.sh [flags]       # make retail-day [RETAIL_DAY_ARGS="..."]: plays the day
#
# The day WRITES to the cluster: shoppers register, buy and cancel on the storefront, the floor picks and
# packs, the dock ships, the network delivers, the restock leg stows stock. Its readiness steps write
# nothing unless -provision is given (go run ./cmd/retail-day -h). Run it in the release window (the
# preflight prints the minutes to it), or pass -wait-for-window, or -start-now (see ADR 0001 D3).
# Exit: 0 the day passed; 1 the gate or the preflight failed or the verdict is FAILED; 2 usage, or the
# cluster gate / the release window refuses the run. The dead letters of a run are counted from the Kafka
# *.dlq topics (read only; cmd/retail-day -brokers, KAFKA_BROKERS, default localhost:9092). k3s-edges.sh does
# NOT forward Kafka, so keep `kubectl --context k3s-multipass -n warehouse-systems port-forward --address
# 127.0.0.1 svc/kafka 9092:9092` open while this runs; without it the preflight fails and says so. With
# -brokers "" the count is unknown and the day passes only with -allow-unverified, which lists it as NOT
# VERIFIED and excuses nothing else.
#
# 1. CLUSTER GATE (kubectl): the context is k3s-multipass (kind is retired), both nodes Ready, every
#    non-Job pod of warehouse-systems Running and Ready, the container restart counters do not move over
#    RETAIL_DAY_SETTLE seconds (a rollout storm is not over), every ArgoCD application Synced and Healthy.
#    A CrashLoopBackOff across many contexts almost always means kafka-controller-0: look there first.
# 2. REST PREFLIGHT: go run ./cmd/retail-day (health of every context through Kong, network-fulfillment
#    live, the node ACTIVE, both CPT schedules equal, the building known, the release window, a shelf).
#
# Env: KONG_URL (http://localhost:8000), STOREFRONT_BASE_URL (http://storefront.localhost/api/storefront),
#   RETAIL_DAY_SETTLE (30), RETAIL_DAY_SKIP_ARGO=1 (skip the ArgoCD check), RETAIL_DAY_SKIP_CLUSTER_GATE=1
#   (REST preflight only: use when kubectl is not available), WAREHOUSE_INFRA_DIR (${REPOS_ROOT}/warehouse-infra:
#   its scripts/data/retail-network-seed.json maps the storefront's SKUs to catalogue titles). Other arguments
#   go to cmd/retail-day.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib.sh"

NS="${RETAIL_DAY_NAMESPACE:-warehouse-systems}"
SETTLE="${RETAIL_DAY_SETTLE:-30}"
KONG="${KONG_URL:-http://localhost:8000}"

# gate_die: the cluster gate refusing the run is exit 2 (a usage-class refusal, not a failed day).
gate_die() { fail "$*"; exit 2; }

# pod_state prints "<total container restarts> <names of pods that are not Running+Ready>" as JSON.
# Job-owned pods (the CronJob sweeps) are history, not capacity: they are ignored.
pod_state() {
  kubectl get pods -n "${NS}" -o json --request-timeout=30s | python3 -c '
import json, sys
d = json.load(sys.stdin)
restarts, bad = 0, []
for p in d["items"]:
    if any(o.get("kind") == "Job" for o in p["metadata"].get("ownerReferences", [])):
        continue
    cs = p["status"].get("containerStatuses", [])
    restarts += sum(c.get("restartCount", 0) for c in cs)
    ready = p["status"].get("phase") == "Running" and cs and all(c.get("ready") for c in cs)
    if not ready:
        bad.append(p["metadata"]["name"])
print(json.dumps({"restarts": restarts, "bad": sorted(bad)}))
'
}

cluster_gate() {
  command -v kubectl >/dev/null || gate_die "kubectl not found (RETAIL_DAY_SKIP_CLUSTER_GATE=1 skips this gate)"
  local ctx
  ctx="$(kubectl config current-context)"
  [[ "${ctx}" == "k3s-multipass" ]] || gate_die "kube context is '${ctx}': the retail day runs on k3s-multipass (kind is retired)"
  ok "context ${ctx}"

  local not_ready
  not_ready="$(kubectl get nodes --no-headers --request-timeout=15s | awk '$2 != "Ready" {print $1 "=" $2}')" \
    || gate_die "the API did not answer: do not touch the machine, wait for it"
  [[ -z "${not_ready}" ]] || gate_die "nodes not Ready: ${not_ready}"
  ok "both nodes Ready"

  local first summary restarts_a restarts_b
  first="$(pod_state)"
  summary="$(python3 -c '
import json, sys
bad = json.loads(sys.argv[1])["bad"]
if bad:
    print("%d pods not Running+Ready, first: %s%s" % (len(bad), " ".join(bad[:6]), " ..." if len(bad) > 6 else ""))
' "${first}")"
  if [[ -n "${summary}" ]]; then
    fail "${summary}"
    gate_die "fix the cluster first; if many contexts crash-loop look at: kubectl -n ${NS} get pod kafka-controller-0"
  fi
  ok "every non-Job pod is Running and Ready"

  restarts_a="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["restarts"])' "${first}")"
  log "settling ${SETTLE}s to see whether restart counters move"
  sleep "${SETTLE}"
  restarts_b="$(pod_state | python3 -c 'import json,sys; print(json.load(sys.stdin)["restarts"])')"
  [[ "${restarts_a}" == "${restarts_b}" ]] || gate_die "restart counters moved (${restarts_a} -> ${restarts_b}): a rollout or a crash loop is still going"
  ok "restart counters steady at ${restarts_a}"

  if [[ "${RETAIL_DAY_SKIP_ARGO:-}" == "1" ]]; then
    log "ArgoCD check skipped (RETAIL_DAY_SKIP_ARGO=1)"
    return 0
  fi
  local unhealthy
  unhealthy="$(kubectl get applications -n argocd --no-headers --request-timeout=20s -o custom-columns=N:.metadata.name,S:.status.sync.status,H:.status.health.status \
    | awk '$2 != "Synced" || $3 != "Healthy" {print $1 "(" $2 "/" $3 ")"}')" || gate_die "could not read the ArgoCD applications"
  [[ -z "${unhealthy}" ]] || gate_die "ArgoCD applications not Synced+Healthy: ${unhealthy//$'\n'/ }"
  ok "every ArgoCD application Synced and Healthy"
}

MODE="run"
ARGS=()
for arg in "$@"; do
  case "${arg}" in
    --preflight|-preflight) MODE="preflight"; ARGS+=(-preflight) ;;
    *) ARGS+=("${arg}") ;;
  esac
done

if [[ "${RETAIL_DAY_SKIP_CLUSTER_GATE:-}" == "1" ]]; then
  log "cluster gate skipped (RETAIL_DAY_SKIP_CLUSTER_GATE=1)"
else
  log "cluster gate"
  cluster_gate
fi

INFRA="${WAREHOUSE_INFRA_DIR:-${REPOS_ROOT}/warehouse-infra}"
DAY_ARGS=()
if [[ "${MODE}" == "run" ]]; then
  DAY_ARGS+=(-fixtures "${WORKSPACE_ROOT}/fixtures/storefront" -network-data "${INFRA}/scripts/data/retail-network-seed.json")
  log "the retail day through ${KONG}/api (it writes to the cluster; -preflight does not)"
else
  log "REST preflight through ${KONG}/api"
fi
cd "${SCRIPT_DIR}/.."
exec go run ./cmd/retail-day -gateway "${KONG}/api" "${DAY_ARGS[@]+"${DAY_ARGS[@]}"}" "${ARGS[@]+"${ARGS[@]}"}"
