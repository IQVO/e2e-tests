#!/usr/bin/env bash
# e2e-tests/scripts/09-seed-inventory.sh
#
# Stocks the pilot sites' bins in inventory-storage with the products REGISTERED in
# product-master (>= 100,000 units), through the services' published REST APIs only:
#
#   CHECK  product-master  GET /products (dimensions, handling tags, temperature class, DOT class)
#          facility-layout GET /sites, /sites/{c}/zones, /sites/{c}/locations?role=Storage
#   PLAN   deterministic assortment by site role, size class and handling class; bins filled to
#          80 % of their real volume and weight limits; hazmat in hazmat-rated zones one SKU per
#          bin; cold chain in chilled / frozen zones. Fails below SEED_MIN_UNITS.
#   STOCK  inventory-storage PUT /bins/{binId} + POST /stock/stow, then reads every bin back
#          (GET /bins/{binId}) and a sample of SKUs (GET /inventory/{sku}/usable).
#
# Idempotent: a bin already holding its planned units is skipped, PUT /bins converges, and every
# stow carries a deterministic Idempotency-Key. Run it AFTER 08-seed-products.sh and after the
# facility layouts exist (provisioning/). Not part of any default run.
#
#   bash scripts/09-seed-inventory.sh --dry-run          # check + plan, write nothing
#   PRODUCT_MASTER_BASE_URL=http://localhost:8000/api/product-master \
#   FACILITY_LAYOUT_BASE_URL=http://localhost:8000/api/facility-layout \
#   INVENTORY_STORAGE_BASE_URL=http://localhost:8000/api/inventory-storage \
#     bash scripts/09-seed-inventory.sh                  # kind cluster via Kong
#
# Tunables (env): SEED_SITES (default: ten pilot sites, see cmd/seed-inventory/main.go),
#   SEED_MIN_UNITS (100000), SEED_WORKERS (6). Extra arguments are passed to the binary.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# env.sh assigns the base URLs unconditionally (local harness ports): capture the caller's first.
CALLER_PM="${PRODUCT_MASTER_BASE_URL:-}"
CALLER_FL="${FACILITY_LAYOUT_BASE_URL:-}"
CALLER_INV="${INVENTORY_STORAGE_BASE_URL:-}"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib.sh"
PM_URL="${CALLER_PM:-${PRODUCT_MASTER_BASE_URL}}"
FL_URL="${CALLER_FL:-${FACILITY_BASE_URL}}"
INV_URL="${CALLER_INV:-${INVENTORY_BASE_URL}}"

cd "${WORKSPACE_ROOT}"
log "building cmd/seed-inventory"
CGO_ENABLED=0 go build -o "${BIN_DIR}/seed-inventory" ./cmd/seed-inventory
ok "seed-inventory -> ${BIN_DIR}/seed-inventory"

args=(
  -product-master-url "${PM_URL}"
  -facility-url "${FL_URL}"
  -inventory-url "${INV_URL}"
  -min-units "${SEED_MIN_UNITS:-100000}"
  -workers "${SEED_WORKERS:-6}"
)
[[ -n "${SEED_SITES:-}" ]] && args+=(-sites "${SEED_SITES}")

log "stocking inventory-storage at ${INV_URL}"
"${BIN_DIR}/seed-inventory" "${args[@]}" "$@"
ok "seed-inventory finished (a --dry-run writes nothing)"
