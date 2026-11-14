#!/usr/bin/env bash
# e2e-tests/scripts/11-run-retail-day.sh
#
# The retail day: shoppers, the network and the warehouse floor in one run, on the k3s cluster
# through Kong (docs/adr/0001-retail-day.md, docs/retail-day-journey.md).
#
#   bash scripts/11-run-retail-day.sh --preflight   # make retail-day-preflight: gate + read-only checks, writes nothing
#   bash scripts/11-run-retail-day.sh               # make retail-day: refuses to run until the day is built
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
#   (REST preflight only: use when kubectl is not available). Other arguments go to cmd/retail-day.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib.sh"

NS="${RETAIL_DAY_NAMESPACE:-warehouse-systems}"
SETTLE="${RETAIL_DAY_SETTLE:-30}"
KONG="${KONG_URL:-http://localhost:8000}"

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
  command -v kubectl >/dev/null || die "kubectl not found (RETAIL_DAY_SKIP_CLUSTER_GATE=1 skips this gate)"
  local ctx
  ctx="$(kubectl config current-context)"
  [[ "${ctx}" == "k3s-multipass" ]] || die "kube context is '${ctx}': the retail day runs on k3s-multipass (kind is retired)"
  ok "context ${ctx}"

  local not_ready
  not_ready="$(kubectl get nodes --no-headers --request-timeout=15s | awk '$2 != "Ready" {print $1 "=" $2}')" \
    || die "the API did not answer: do not touch the machine, wait for it"
  [[ -z "${not_ready}" ]] || die "nodes not Ready: ${not_ready}"
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
    die "fix the cluster first; if many contexts crash-loop look at: kubectl -n ${NS} get pod kafka-controller-0"
  fi
  ok "every non-Job pod is Running and Ready"

  restarts_a="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["restarts"])' "${first}")"
  log "settling ${SETTLE}s to see whether restart counters move"
  sleep "${SETTLE}"
  restarts_b="$(pod_state | python3 -c 'import json,sys; print(json.load(sys.stdin)["restarts"])')"
  [[ "${restarts_a}" == "${restarts_b}" ]] || die "restart counters moved (${restarts_a} -> ${restarts_b}): a rollout or a crash loop is still going"
  ok "restart counters steady at ${restarts_a}"

  if [[ "${RETAIL_DAY_SKIP_ARGO:-}" == "1" ]]; then
    log "ArgoCD check skipped (RETAIL_DAY_SKIP_ARGO=1)"
    return 0
  fi
  local unhealthy
  unhealthy="$(kubectl get applications -n argocd --no-headers --request-timeout=20s -o custom-columns=N:.metadata.name,S:.status.sync.status,H:.status.health.status \
    | awk '$2 != "Synced" || $3 != "Healthy" {print $1 "(" $2 "/" $3 ")"}')" || die "could not read the ArgoCD applications"
  [[ -z "${unhealthy}" ]] || die "ArgoCD applications not Synced+Healthy: ${unhealthy//$'\n'/ }"
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

log "REST preflight through ${KONG}/api"
cd "${SCRIPT_DIR}/.."
exec go run ./cmd/retail-day -gateway "${KONG}/api" "${ARGS[@]+"${ARGS[@]}"}"
