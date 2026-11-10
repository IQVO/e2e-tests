#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib.sh"

# Every POST carries a fresh Idempotency-Key (fleet REST guideline 2.2); the PUT is idempotent by method.
seed_path() {
  local id="$1" prefix="$2" capability="$3" cycle="$4" payload status
  payload="$(printf '{"pathId":"%s","matchPrefix":"%s","requiredCapabilities":["%s"],"cycleTimeP95":"%s"}' "$id" "$prefix" "$capability" "$cycle")"
  status="$(curl -sS -o /dev/null -w '%{http_code}' -H 'Content-Type: application/json' -H "Idempotency-Key: $(uuidgen)" -d "$payload" "${PROCESS_PATH_BASE_URL}/process-paths")"
  case "$status" in
    201) ;;
    409)
      payload="$(printf '{"matchPrefix":"%s","requiredCapabilities":["%s"],"cycleTimeP95":"%s"}' "$prefix" "$capability" "$cycle")"
      curl -fsS -H 'Content-Type: application/json' -X PUT -d "$payload" "${PROCESS_PATH_BASE_URL}/process-paths/${id}" >/dev/null
      ;;
    *) die "seeding process path ${id} returned HTTP ${status}" ;;
  esac
}

seed_path PICK pick pick 90s
seed_path PACK pack pack 60s
seed_path REBIN rebin rebin 45s
seed_path SLAM slam slam 30s
# The inter-warehouse transfer saga's dispatch leg (a DISPATCH task type whose
# `dispatch-*` family must be declared before such work can be released,
# fulfillment-execution ADR-0036); same entry as fixtures/process-paths/
# sortable-fc.yaml. Every catalogue client reads the catalogue from Kafka now,
# so the family has to exist in process-path-management itself.
seed_path DISPATCH dispatch dispatch 120s
ok "process-path catalogue seeded through REST"
