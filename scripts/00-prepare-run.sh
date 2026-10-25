#!/usr/bin/env bash
# Prepare once per fleet lifetime. --new requires every recorded process stopped.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../env.sh"
mkdir -p "${RUN_DIR}/pids"
if [[ "${1:-}" == "--new" ]]; then
  for pidfile in "${RUN_DIR}"/pids/*.pid; do
    [[ -f "${pidfile}" ]] || continue
    if kill -0 "$(cat "${pidfile}")" 2>/dev/null; then
      echo "cannot prepare a new run while a service is running: ${pidfile}" >&2
      exit 1
    fi
  done
fi
python3 - "${RUN_DIR}" "${1:-}" <<'PY'
import pathlib, re, sys, uuid
run, mode = pathlib.Path(sys.argv[1]), sys.argv[2]
identity = run / 'e2e-run-id'
if mode == '--new' or not identity.exists():
    value = uuid.uuid4().hex.upper()
    tmp = identity.with_suffix('.tmp')
    tmp.write_text(value + '\n')
    tmp.replace(identity)
value = identity.read_text().strip()
if not re.fullmatch(r'[A-Za-z0-9]+', value):
    raise SystemExit('invalid persisted E2E_RUN_ID')
print(value)
PY
