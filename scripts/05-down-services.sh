#!/usr/bin/env bash
# e2e-tests/scripts/05-down-services.sh
#
# Stops only the processes started by this harness (the 13 HTTP services,
# their 7 MCP servers, and warehouse-ops-agent), via lib.sh's PID-file
# based stop_service — never a blind pkill.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib.sh"

for service in ops-agent workforce-mcp execution-mcp wes-mcp inventory-mcp facility-mcp labor-mcp order-mcp nip planning network order workforce labor execution wes inventory inbound-receiving product-master facility process-path; do
  stop_service "${service}"
done
ok "e2e service processes stopped"
