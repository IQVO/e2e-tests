#!/usr/bin/env bash
# e2e-tests/scripts/04-run-tests.sh
#
# Runs the godog suite (e2e_test.go) against the already-running services
# (scripts/03-up-services.sh). Exports the same base URLs/DB URL the
# scripts use so the Go test binary and the shell agree on where
# everything lives.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib.sh"

export FACILITY_BASE_URL INVENTORY_BASE_URL WES_BASE_URL FULFILLMENT_BASE_URL WORKFORCE_BASE_URL OPS_AGENT_BASE_URL ORDER_BASE_URL INVENTORY_DB_URL WES_DB_URL FULFILLMENT_DB_URL ORDER_DB_URL

# The *_DB_URL values above deliberately carry NO password in the URL
# itself (see env.sh's own comment) -- e2e_test.go's dbOpen helper fills
# in each DSN's password explicitly via pgx.ParseConfig +
# stdlib.RegisterConnConfig rather than relying on the process-global
# PGPASSWORD env var, since this one test binary talks to THREE different
# databases with three different passwords in the same process.

log "running e2e suite (godog) against the live services"
cd "${WORKSPACE_ROOT}"
go test -v -run TestMain -timeout 5m ./...
