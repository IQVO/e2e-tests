#!/usr/bin/env bash
# e2e-tests/scripts/02-up-infra.sh
#
# Verifies the shared Kafka broker is reachable and brings up this
# harness's ten dedicated Postgres instances (docker-compose.yml in this
# directory), then waits for all of them to report healthy.
#
# Kafka itself is NOT started by this script (see below) -- it is owned by
# the warehouse-infra kind cluster and exposed to the host at
# localhost:9092 via a Bitnami externalAccess NodePort (warehouse-infra
# PR #6). This fleet runs exactly ONE Kafka broker platform-wide; the
# docker-compose-based broker this script used to start
# (docker-compose.kafka.yml, container "warehouse-kafka") was retired
# when that consolidation landed, so KAFKA_COMPOSE_FILE/
# KAFKA_CONTAINER_NAME in env.sh are dead weight kept only so a future
# cleanup PR can remove them from every script at once rather than
# leaving a partial migration.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib.sh"

command -v docker >/dev/null 2>&1 || die "docker is not on PATH"
docker info >/dev/null 2>&1 || die "the Docker daemon is not reachable — start Docker Desktop first"

if nc -z localhost 9092 2>/dev/null; then
  ok "shared Kafka broker reachable at localhost:9092 (warehouse-infra kind cluster)"
else
  die "Kafka is not reachable at localhost:9092. This harness no longer starts its own broker -- bring up the warehouse-infra kind cluster first (cd ../warehouse-infra && scripts/up.sh), which exposes Kafka to the host via externalAccess."
fi
wait_for_tcp localhost 9092 30

log "starting per-service Postgres instances (docker compose, project warehouse-e2e)"
docker compose -f "${WORKSPACE_ROOT}/docker-compose.yml" up -d

log "waiting for all 10 Postgres instances to report healthy"
deadline=$((SECONDS + 60))
for svc in postgres-facility postgres-inventory postgres-wes postgres-fulfillment postgres-workforce postgres-order postgres-process-path postgres-labor postgres-network postgres-planning; do
  while true; do
    status="$(docker inspect -f '{{.State.Health.Status}}' "e2e-${svc}" 2>/dev/null || echo starting)"
    if [[ "${status}" == "healthy" ]]; then
      ok "${svc} healthy"
      break
    fi
    if [[ "${SECONDS}" -ge "${deadline}" ]]; then
      die "${svc} did not become healthy in time (status=${status})"
    fi
    sleep 1
  done
done

log "infra up: Kafka on :9092 (warehouse-infra kind cluster), Postgres on :5441-:5450"
