#!/usr/bin/env bash
# e2e-tests/scripts/08-seed-products.sh
#
# Registers a catalogue of REAL products (>= 2,000) in product-master through
# its published REST API (PUT /products/{sku}, PUT /products/{sku}/classification),
# then reads every one back with GET /products and fails on any gap.
#
# Products: fixtures/products/openfoodfacts-products.json, harvested from
# Open Food Facts (ODbL 1.0). Each SKU is the product's GS1 GTIN (check digit
# verified), each description its real brand + name + net quantity. Frozen /
# chilled items are classified TemperatureSensitive from their own category
# tags; everything else is left unclassified (no invented Hazmat/Fragile, no
# invented dimensions: the source has none).
#
# Idempotent: every write is an idempotent PUT, so re-running against a dirty
# database reports "already present" and changes nothing. GTINs are shared
# reference data, not run-scoped.
#
# Not part of any default run (like 06/07). Needs product-master up
# (scripts/03-up-services.sh), or point it at the kind cluster through Kong:
#
#   bash scripts/08-seed-products.sh
#   PRODUCT_MASTER_BASE_URL=http://localhost:8000/api/product-master bash scripts/08-seed-products.sh
#
# Tunables (env, passed as flags; see cmd/seed-products/main.go):
#   SEED_WORKERS   concurrent workers            (default 8)
#   SEED_MIN       fail below this many products (default 2000)
#   SEED_LIMIT     register only the first N     (default 0 = all)
#
# Refresh the fixture from the internet (slow: Open Food Facts allows ~10
# searches/min):  go run ./cmd/seed-products -fetch
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# env.sh assigns PRODUCT_MASTER_BASE_URL unconditionally, so capture the
# caller's value BEFORE sourcing it; otherwise the documented Kong override
# would be silently replaced by the local harness URL.
CALLER_PRODUCT_MASTER_BASE_URL="${PRODUCT_MASTER_BASE_URL:-}"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib.sh"
PRODUCT_MASTER_BASE_URL="${CALLER_PRODUCT_MASTER_BASE_URL:-${PRODUCT_MASTER_BASE_URL}}"

cd "${WORKSPACE_ROOT}"
log "building cmd/seed-products"
CGO_ENABLED=0 go build -o "${BIN_DIR}/seed-products" ./cmd/seed-products
ok "seed-products -> ${BIN_DIR}/seed-products"

log "seeding product-master at ${PRODUCT_MASTER_BASE_URL}"
"${BIN_DIR}/seed-products" \
  -fixture "${WORKSPACE_ROOT}/fixtures/products/openfoodfacts-products.json" \
  -base-url "${PRODUCT_MASTER_BASE_URL}" \
  -workers "${SEED_WORKERS:-8}" \
  -min "${SEED_MIN:-2000}" \
  -limit "${SEED_LIMIT:-0}"
ok "product catalogue seeded and verified"
