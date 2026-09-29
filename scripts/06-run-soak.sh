#!/usr/bin/env bash
# e2e-tests/scripts/06-run-soak.sh
#
# Runs ONLY the @soak scenario (features/soak_backlog_ramp.feature) against
# the already-running services (scripts/03-up-services.sh) — a sustained,
# ramping backlog-injection load run across BOTH the PICK and PACK process
# paths, with a configurable pool of picker/packer stations continuously
# claiming and completing the resulting tasks.
#
# Unlike scripts/04-run-tests.sh, this is NOT part of any default run: it
# is excluded there via TestMain's default GODOG_TAGS="~@soak", and this
# script is the only place GODOG_TAGS=@soak is set, deliberately isolating
# the two so a plain `go test ./...` (or CI, or 04-run-tests.sh) can never
# accidentally kick off an hour-long run.
#
# Tunables (all optional, see soak_test.go's envDurationOrDefault/
# envIntOrDefault for the exact defaults each falls back to):
#   SOAK_DURATION              total ramp duration          (default 1h)
#   SOAK_RAMP_START_INTERVAL   injector interval at t=0      (default 2s)
#   SOAK_RAMP_END_INTERVAL     injector interval at t=duration (default 200ms)
#   SOAK_WIP_LIMIT             release-fed WIP limit, both paths (default 20)
#   SOAK_PICKERS               number of picker stations      (default 3)
#   SOAK_PACKERS               number of packer stations      (default 2)
#
# Example: a quick 2-minute smoke run of the soak scenario itself, before
# committing to the full default 1-hour run:
#   SOAK_DURATION=2m SOAK_RAMP_START_INTERVAL=1s SOAK_RAMP_END_INTERVAL=200ms \
#     bash scripts/06-run-soak.sh
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib.sh"

# soak_timeout_secs converts a Go duration string (e.g. "1h", "90m", "2m")
# to whole seconds plus a fixed 5-minute safety margin for `go test
# -timeout`, so the test framework's own timeout never fires before the
# ramp itself has had a chance to finish and print its summary. Must be
# defined before use below (bash has no forward declarations).
soak_timeout_secs() {
  local dur="$1"
  python3 - "${dur}" <<'PYEOF'
import re, sys
d = sys.argv[1]
units = {"h": 3600, "m": 60, "s": 1}
total = 0
for value, unit in re.findall(r"(\d+(?:\.\d+)?)([hms])", d):
    total += float(value) * units[unit]
print(int(total) + 300)
PYEOF
}

export FACILITY_BASE_URL INVENTORY_BASE_URL WES_BASE_URL FULFILLMENT_BASE_URL WORKFORCE_BASE_URL ORDER_BASE_URL
export INVENTORY_DB_URL WES_DB_URL FULFILLMENT_DB_URL

: "${SOAK_DURATION:=1h}"
export SOAK_DURATION

export GODOG_TAGS="@soak"

log "running @soak scenario (features/soak_backlog_ramp.feature) for SOAK_DURATION=${SOAK_DURATION}"
log "(this call will block for roughly SOAK_DURATION — that is expected)"
cd "${WORKSPACE_ROOT}"
go test -v -run TestMain -timeout "$(soak_timeout_secs "${SOAK_DURATION}")s" ./...
