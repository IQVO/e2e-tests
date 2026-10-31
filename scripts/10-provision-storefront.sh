#!/usr/bin/env bash
# e2e-tests/scripts/10-provision-storefront.sh
#
# Provisions EVERYTHING a shopper needs to buy a product in the storefront
# (http://storefront.localhost) and see its delivery promise, on the kind cluster, through the
# services' published REST APIs (Kong). Repeatable: a second run against an unchanged cluster
# writes nothing (the network seed only extends the dated wave horizon).
# The register of what is provisioned, by whom and why: docs/storefront-provisioning.md.
#
#   1. PREFLIGHT  Kong answers; the sibling warehouse-infra checkout exists; one real read per
#                 layer (offer-availability, pricing, product-content, product-discovery, the BFF);
#                 WMS usable stock of every fixture SKU (inventory-storage). WMS prerequisites
#                 (sites, seed-products, seed-inventory) are NOT re-run: heavy, and stocking is
#                 one-way. A missing one fails here with the commands to run.
#   2. WMS        go run ./cmd/seed-storefront -wms-only: site-1's CPT schedule in
#                 process-path-management (fixtures/storefront/fulfilment-site.json), PUT only when
#                 it differs (a warehouse-day run with -allow-live-site would overwrite it; by
#                 default it runs on its own site SIM1). FIRST, because network-fulfillment
#                 caps every listing's stock at the capacity before the next cutoff: the network
#                 verify (listings with stock) and the storefront both depend on it.
#   3. NETWORK    python3 ../warehouse-infra/scripts/seed-retail-network.py: vendor, region,
#                 station, service areas, node + profile, lane, waves (DATED: every run extends
#                 the 14-day horizon), catalogue items, listings, inventory feed; then verifies.
#   4. DICTIONARY seed-retail-network.py --check-translation helm-values/network-fulfillment.yaml:
#                 a stale product dictionary means orders never reach the warehouse.
#   5. STOREFRONT go run ./cmd/seed-storefront: the CPT schedule again (equal by now: skipped),
#                 then tax table, delivery charges, categories, prices, product content; then
#                 VERIFY (capability offers, purchasable, delivery quote, search).
#   6. SUMMARY    the storefront URL and the deliverable postal codes.
#
#   bash scripts/10-provision-storefront.sh                 # make provision-storefront
#   bash scripts/10-provision-storefront.sh --dry-run       # make provision-storefront-dry-run (writes nothing)
#   bash scripts/10-provision-storefront.sh --verify-only   # make provision-storefront-verify  (writes nothing)
#
# Env: WAREHOUSE_INFRA_DIR (default <repos root>/warehouse-infra), KONG_URL (default
#   http://localhost:8000), STOREFRONT_URL (default http://storefront.localhost),
#   STOREFRONT_VERIFY_TIMEOUT (default 3m), STOREFRONT_SKIP_NETWORK=1 (skip steps 3-4: only when
#   you just ran them). Other arguments are passed to cmd/seed-storefront (step 5).
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib.sh"

MODE="provision"
SEED_ARGS=()
NET_ARGS=()
WMS_ARGS=()
for arg in "$@"; do
  case "${arg}" in
    --dry-run|-dry-run)
      MODE="dry-run"; SEED_ARGS+=(-dry-run); NET_ARGS+=(--dry-run); WMS_ARGS+=(-dry-run) ;;
    --verify-only|-verify-only)
      MODE="verify-only"; SEED_ARGS+=(-verify-only); NET_ARGS+=(--verify); WMS_ARGS+=(-verify-only) ;;
    *)
      SEED_ARGS+=("${arg}") ;;
  esac
done

KONG="${KONG_URL:-http://localhost:8000}"
API="${KONG}/api"
SF="${STOREFRONT_URL:-http://storefront.localhost}"
BFF="${SF}/api/storefront"
INFRA="${WAREHOUSE_INFRA_DIR:-${REPOS_ROOT}/warehouse-infra}"
NETWORK_DATA="${INFRA}/scripts/data/retail-network-seed.json"
NF_VALUES="${INFRA}/helm-values/network-fulfillment.yaml"
FIXTURE="${WORKSPACE_ROOT}/fixtures/storefront/catalogue.json"
# python.org builds ship without CA certificates; Homebrew's python3 has them
PY="python3"
[[ -x /opt/homebrew/bin/python3 ]] && PY="/opt/homebrew/bin/python3"

# http_status URL -> prints the HTTP status ("000" when nothing answers)
http_status() {
  curl -s -o /dev/null -m 10 -w '%{http_code}' "$1" || true
}

# expect_read LABEL URL STATUS... -> dies unless URL answers one of the statuses
expect_read() {
  local label="$1" url="$2"
  shift 2
  local got
  got="$(http_status "${url}")"
  local s
  for s in "$@"; do
    if [[ "${got}" == "${s}" ]]; then
      ok "${label}: GET ${url} -> ${got}"
      return 0
    fi
  done
  die "${label}: GET ${url} -> ${got} (want $*). Is the kind cluster up and the context deployed? (warehouse-infra: scripts/up.sh)"
}

log "storefront provisioning (${MODE})"

# ---- 1. PREFLIGHT -----------------------------------------------------------------------------
log "1/6 preflight"
[[ "$(http_status "${API}/offer-availability/catalog?limit=1")" != "000" ]] \
  || die "Kong does not answer at ${KONG} -- start the kind cluster (warehouse-infra: scripts/up.sh)"
[[ -d "${INFRA}" ]] \
  || die "warehouse-infra is not checked out at ${INFRA} -- clone it next to e2e-tests (git clone git@github.com:IQVO/warehouse-infra.git \"${INFRA}\") or set WAREHOUSE_INFRA_DIR"
[[ -f "${INFRA}/scripts/seed-retail-network.py" && -f "${NETWORK_DATA}" && -f "${NF_VALUES}" ]] \
  || die "${INFRA} has no scripts/seed-retail-network.py, scripts/data/retail-network-seed.json or helm-values/network-fulfillment.yaml -- update it (git -C \"${INFRA}\" pull)"
ok "warehouse-infra at ${INFRA} ($(git -C "${INFRA}" rev-parse --abbrev-ref HEAD 2>/dev/null || echo '?') $(git -C "${INFRA}" rev-parse --short HEAD 2>/dev/null || echo '?'))"
expect_read "offer-availability" "${API}/offer-availability/catalog?limit=1" 200
expect_read "pricing" "${API}/pricing/tax-tables/current" 200 404
expect_read "product-content" "${API}/product-content/categories" 200
expect_read "product-discovery" "${API}/product-discovery/products?limit=1" 200
expect_read "storefront BFF" "${BFF}/products?limit=1" 200
expect_read "inventory-storage" "${API}/inventory-storage/inventory/preflight-unknown-sku/usable" 200 404
expect_read "process-path-management" "${API}/process-path-management/process-paths" 200

# WMS usable stock of every fixture SKU: the network's inventory feed (and so "in stock" in the
# storefront) comes from it through network-fulfillment's ACL.
skus="$("${PY}" -c 'import json,sys; print("\n".join(p["sku"] for p in json.load(open(sys.argv[1]))["products"]))' "${FIXTURE}")"
total=0
empty=()
while IFS= read -r sku; do
  [[ -n "${sku}" ]] || continue
  total=$((total + 1))
  body="$(curl -s -m 10 "${API}/inventory-storage/inventory/${sku}/usable" || true)"
  usable="$(printf '%s' "${body}" | "${PY}" -c 'import json,sys
try: print(int(json.load(sys.stdin).get("usable", 0)))
except Exception: print(0)')"
  if [[ "${usable}" -le 0 ]]; then
    empty+=("${sku}")
  fi
done <<EOF
${skus}
EOF
if [[ "${#empty[@]}" -gt 0 ]]; then
  fail "${#empty[@]} of ${total} storefront SKUs have NO usable stock in the WMS: ${empty[*]}"
  fail "they cannot be in stock (nor purchasable) in the storefront. Stock is one-way, so this script does not re-run it:"
  fail "  1. facility layouts of the pilot sites:  ~/warehouse-systems/provisioning (see its README)"
  fail "  2. products:  PRODUCT_MASTER_BASE_URL=${API}/product-master bash scripts/08-seed-products.sh"
  fail "  3. stock:     PRODUCT_MASTER_BASE_URL=${API}/product-master FACILITY_LAYOUT_BASE_URL=${API}/facility-layout \\"
  fail "                INVENTORY_STORAGE_BASE_URL=${API}/inventory-storage bash scripts/09-seed-inventory.sh"
  fail "  (a SKU emptied by repeated test purchases needs restocking: see docs/storefront-provisioning.md)"
  die "preflight: WMS stock missing"
fi
ok "WMS: all ${total} storefront SKUs have usable stock"

# ---- 2. WMS -----------------------------------------------------------------------------------
# site-1's CPT schedule before anything that reads stock through network-fulfillment's ACL
log "2/6 WMS: site-1's CPT schedule in process-path-management (go run ./cmd/seed-storefront -wms-only ${WMS_ARGS[*]+"${WMS_ARGS[*]}"})"
( cd "${WORKSPACE_ROOT}" && go run ./cmd/seed-storefront -wms-only \
    -base-url "${API}" \
    -network-data "${NETWORK_DATA}" \
    -site "${WORKSPACE_ROOT}/fixtures/storefront/fulfilment-site.json" \
    -verify-timeout "${STOREFRONT_VERIFY_TIMEOUT:-3m}" \
    ${WMS_ARGS[@]+"${WMS_ARGS[@]}"} ) \
  || die "the WMS step failed (see above; docs/storefront-provisioning.md, WMS side of the promise)"

# ---- 3. NETWORK -------------------------------------------------------------------------------
if [[ "${STOREFRONT_SKIP_NETWORK:-0}" == "1" ]]; then
  log "3/6 network tier: SKIPPED (STOREFRONT_SKIP_NETWORK=1)"
  log "4/6 product dictionary: SKIPPED (STOREFRONT_SKIP_NETWORK=1)"
else
  log "3/6 network tier: ${PY} scripts/seed-retail-network.py ${NET_ARGS[*]+"${NET_ARGS[*]}"} (vendor, topology, waves, catalogue, listings, stock feed)"
  net_log="${LOG_DIR}/provision-storefront-network.log"
  net_rc=0
  ( cd "${INFRA}" && RETAIL_NETWORK_BASE_URL="${API}" "${PY}" scripts/seed-retail-network.py ${NET_ARGS[@]+"${NET_ARGS[@]}"} ) >"${net_log}" 2>&1 || net_rc=$?
  # everything but the unchanged entities (the full output stays in the log)
  grep -v '^  exists ' "${net_log}" | grep -v '^$' >&2 || true
  ok "$(grep -c '^  exists ' "${net_log}" || true) network entities already in place (full output: ${net_log})"
  [[ "${net_rc}" -eq 0 ]] \
    || die "the retail-network seed failed (exit ${net_rc}; see above and warehouse-infra scripts/README-retail-network-seed.md)"
  ok "network tier OK (${MODE})"

  # ---- 4. DICTIONARY ----------------------------------------------------------------------------
  log "4/6 network-fulfillment product dictionary (helm-values/network-fulfillment.yaml) vs the network"
  tr_log="${LOG_DIR}/provision-storefront-check-translation.log"
  if ! ( cd "${INFRA}" && RETAIL_NETWORK_BASE_URL="${API}" "${PY}" scripts/seed-retail-network.py --check-translation helm-values/network-fulfillment.yaml ) >"${tr_log}" 2>&1; then
    cat "${tr_log}" >&2
    fail "the product dictionary of network-fulfillment is STALE: purchase-order lines fail translation and orders never reach the warehouse."
    fail "fix (warehouse-infra): make seed-network-translation > block.yaml; replace the productTranslation block of"
    fail "  helm-values/network-fulfillment.yaml with it; make seed-network-translation-check; open the values PR into"
    fail "  develop; once merged ArgoCD rolls network-fulfillment. Then re-run this script."
    die "stale product dictionary"
  fi
  ok "$(tail -n 1 "${tr_log}") (full output: ${tr_log})"
fi

# ---- 5. STOREFRONT ----------------------------------------------------------------------------
log "5/6 storefront tier (CPT schedule re-checked): go run ./cmd/seed-storefront ${SEED_ARGS[*]+"${SEED_ARGS[*]}"}"
cd "${WORKSPACE_ROOT}"
go run ./cmd/seed-storefront \
  -base-url "${API}" \
  -storefront-url "${BFF}" \
  -network-data "${NETWORK_DATA}" \
  -site "${WORKSPACE_ROOT}/fixtures/storefront/fulfilment-site.json" \
  -verify-timeout "${STOREFRONT_VERIFY_TIMEOUT:-3m}" \
  ${SEED_ARGS[@]+"${SEED_ARGS[@]}"}

# ---- 6. SUMMARY -------------------------------------------------------------------------------
prefixes="$("${PY}" -c 'import json,sys; print(" ".join(p + "xx" for p in json.load(open(sys.argv[1]))["deliverablePostalPrefixes"]))' "${FIXTURE}")"
log "6/6 summary (${MODE})"
ok "storefront:              ${SF}"
ok "deliverable postal codes: ${prefixes} (e.g. 78701 Austin, TX); any other ZIP answers DESTINATION_NOT_SERVED"
ok "payment test tokens:     tok_approve, tok_decline, tok_timeout (the shopper registers, adds an address and pays in the UI)"
ok "register:                docs/storefront-provisioning.md"
