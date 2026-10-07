#!/usr/bin/env bash
# e2e-tests/scripts/lib.sh — sourced by every other script. Not runnable
# on its own.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/../env.sh"

mkdir -p "${BIN_DIR}" "${LOG_DIR}" "${RUN_DIR}/pids"

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*" >&2; }
ok()   { printf '\033[1;32m  OK\033[0m   %s\n' "$*" >&2; }
fail() { printf '\033[1;31m  FAIL\033[0m %s\n' "$*" >&2; }
die()  { fail "$*"; exit 1; }

# require_repo name path -- dies with an actionable message when a sibling
# repo the harness builds from is not checked out next to this one (the
# newest context, product-master, is the usual one missing on an older
# workspace).
require_repo() {
  local name="$1" path="$2"
  [[ -d "${path}" ]] || die "${name} is not checked out at ${path} -- clone it next to e2e-tests (git clone git@github.com:IQVO/${name}.git \"${path}\")"
}

# wait_for_http URL [timeout_secs]
# Polls URL until it returns any 2xx, or dies after timeout.
wait_for_http() {
  local url="$1" timeout="${2:-${HEALTH_TIMEOUT_SECS}}" waited=0
  while true; do
    if curl -fsS --max-time 2 -o /dev/null "${url}"; then
      ok "${url} is up"
      return 0
    fi
    sleep 1
    waited=$((waited + 1))
    if [[ "${waited}" -ge "${timeout}" ]]; then
      die "timed out after ${timeout}s waiting for ${url}"
    fi
  done
}

# wait_for_tcp host port [timeout_secs]
wait_for_tcp() {
  local host="$1" port="$2" timeout="${3:-${HEALTH_TIMEOUT_SECS}}" waited=0
  while true; do
    if (exec 3<>"/dev/tcp/${host}/${port}") 2>/dev/null; then
      exec 3<&- 3>&- || true
      ok "${host}:${port} is accepting connections"
      return 0
    fi
    sleep 1
    waited=$((waited + 1))
    if [[ "${waited}" -ge "${timeout}" ]]; then
      die "timed out after ${timeout}s waiting for ${host}:${port}"
    fi
  done
}

# start_service_in name workdir binary_path env_kv... -- launches a
# background process with its CWD set to workdir (needed by any service
# whose main() hardcodes a relative "migrations" path instead of reading
# MIGRATIONS_PATH from the environment — fulfillment-execution does this),
# redirecting output to logs/<name>.log and remembering its PID in
# run/pids/<name>.pid.
start_service_in() {
  local name="$1" workdir="$2" bin="$3"
  shift 3
  local pidfile="${RUN_DIR}/pids/${name}.pid"
  local logfile="${LOG_DIR}/${name}.log"

  if [[ -f "${pidfile}" ]] && kill -0 "$(cat "${pidfile}")" 2>/dev/null; then
    log "${name} already running (pid $(cat "${pidfile}")), skipping"
    return 0
  fi

  log "starting ${name} (${bin}, cwd=${workdir})"
  # NOTE on the pidfile-capture quirk: the original form of this function
  # wrapped the backgrounded command in `(cd workdir && env ... bin & echo
  # $! >pidfile)` — backgrounding a `&&`-list forces bash to fork an
  # *extra* subshell to host that list, and on macOS's bundled bash 3.2
  # (no job-control/monitor mode in a non-interactive script) `$!` taken
  # right after was observed to resolve to the CALLING script's own PID,
  # not the forked subshell's — so the pidfile recorded the wrong PID and
  # stop_service's `kill "${pid}"` either hit an unrelated process or
  # silently no-opped (kill -0 on the caller's own live PID always
  # succeeds, masking the bug). Verified via a standalone repro before
  # this fix: the recorded PID matched the top-level script, not the
  # child. Fixed by making `cd` happen INSIDE the single backgrounded
  # subshell (no nested `&&` list) and `exec`-ing the target binary so
  # the subshell's process image is replaced in place — one fork, one
  # PID, captured immediately via $! with nothing else run in between.
  # shellcheck disable=SC2068
  ( cd "${workdir}" && exec env "$@" "${bin}" >"${logfile}" 2>&1 ) &
  local pid=$!
  echo "${pid}" >"${pidfile}"
  sleep 0.3
  if ! kill -0 "${pid}" 2>/dev/null; then
    fail "${name} exited immediately — see ${logfile}"
    tail -n 40 "${logfile}" >&2 || true
    exit 1
  fi
}

# start_service name binary_path env_kv... -- same as start_service_in but
# runs with the harness's own CWD (fine for services that read
# MIGRATIONS_PATH from the environment instead of hardcoding it).
start_service() {
  local name="$1" bin="$2"
  shift 2
  start_service_in "${name}" "${WORKSPACE_ROOT}" "${bin}" "$@"
}

stop_service() {
  local name="$1"
  local pidfile="${RUN_DIR}/pids/${name}.pid"
  if [[ -f "${pidfile}" ]]; then
    local pid
    pid="$(cat "${pidfile}")"
    if kill -0 "${pid}" 2>/dev/null; then
      log "stopping ${name} (pid ${pid})"
      kill "${pid}" 2>/dev/null || true
      for _ in $(seq 1 20); do
        kill -0 "${pid}" 2>/dev/null || break
        sleep 0.2
      done
      kill -9 "${pid}" 2>/dev/null || true
    fi
    rm -f "${pidfile}"
  fi
}
