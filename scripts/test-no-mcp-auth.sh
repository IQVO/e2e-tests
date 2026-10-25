#!/usr/bin/env bash
# Guard the unauthenticated MCP launch contract. Runtime SDK probes cover all eight.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
if rg -n 'MCP_(READ|WRITE|READ_WRITE)_KEY|static.bearer|bearer key' env.sh scripts/03-up-services.sh README.md CLAUDE.md AGENTS.md; then
  echo 'obsolete MCP authentication wiring remains' >&2
  exit 1
fi
