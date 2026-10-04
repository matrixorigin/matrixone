#!/usr/bin/env bash
# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.
#
# End-to-end harness for the Kafka external table (ENGINE = KAFKA, issue
# #27518). It stands up a single-node KRaft Kafka via docker compose on a
# pre-picked free host port (Kafka clients bootstrap through the ADVERTISED
# listener, so the port must be known before the container starts), boots a
# fresh mo-service on a random frontend port, then runs the Go driver
# (test/kafkaexttab/kafka_e2e_local.go), which seeds the topic itself and
# exercises the exactly-once read semantics over a MySQL DSN. Everything is
# torn down on exit. Mirrors optools/esql_ci.bash.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE="${1:-e2e-local}"
REPORT_DIR="${MO_KAFKA_REPORT_DIR:-${ROOT_DIR}/test/kafkaexttab/reports/ci_$(date -u +%Y%m%dT%H%M%SZ)}"
TMP_DIR=""
MO_PID=""

COMPOSE_FILE="$ROOT_DIR/etc/launch-kafka-local/compose.yaml"

log() { printf '[kafka-ci] %s\n' "$*"; }
die() { printf '[kafka-ci] ERROR: %s\n' "$*" >&2; exit 1; }
require() { command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"; }

compose() {
  KAFKA_PORT="${KAFKA_PORT:-0}" \
    docker compose -p "${COMPOSE_PROJECT_NAME:-mo-kafka-unused}" -f "$COMPOSE_FILE" "$@"
}

collect() {
  [[ -n "$TMP_DIR" ]] || return
  mkdir -p "$REPORT_DIR"
  if [[ -f "$TMP_DIR/mo-service.log" ]]; then
    cp "$TMP_DIR/mo-service.log" "$REPORT_DIR/mo-service.log"
  fi
  if command -v docker >/dev/null 2>&1; then
    compose logs --no-color >"$REPORT_DIR/kafka.log" 2>&1 || true
  fi
}

cleanup() {
  local status=$?
  set +e
  if [[ -n "$MO_PID" ]] && kill -0 "$MO_PID" >/dev/null 2>&1; then
    kill "$MO_PID" >/dev/null 2>&1 || true
    for _ in {1..40}; do
      kill -0 "$MO_PID" >/dev/null 2>&1 || break
      sleep 0.25
    done
    if kill -0 "$MO_PID" >/dev/null 2>&1; then
      kill -KILL "$MO_PID" >/dev/null 2>&1 || true
    fi
    wait "$MO_PID" >/dev/null 2>&1 || true
  fi
  collect
  if [[ -n "$TMP_DIR" ]]; then
    compose down --volumes --remove-orphans >/dev/null 2>&1 || true
    if [[ -d "$TMP_DIR" && "$(basename "$TMP_DIR")" == mo-kafka-e2e.* ]]; then
      rm -rf -- "$TMP_DIR"
    else
      log "refusing to remove unexpected temporary path: $TMP_DIR"
    fi
  fi
  return "$status"
}

pick_free_port() {
  python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

wait_kafka() {
  # the driver retries topic creation itself; this only bounds container boot
  local deadline=$((SECONDS + 180))
  until compose ps kafka 2>/dev/null | grep -q healthy; do
    (( SECONDS < deadline )) || die "Kafka did not become healthy"
    sleep 2
  done
}

wait_mo_port() {
  local deadline=$((SECONDS + 120)) port=""
  while (( SECONDS < deadline )); do
    if [[ -n "$MO_PID" ]] && ! kill -0 "$MO_PID" >/dev/null 2>&1; then
      die "MatrixOne exited before publishing its frontend listener"
    fi
    port="$(sed -nE 's/.*Server Listening on : [^ ]*:([0-9]+).*/\1/p' "$TMP_DIR/mo-service.log" | tail -1)"
    if [[ "$port" =~ ^[1-9][0-9]*$ ]]; then
      printf '%s\n' "$port"
      return
    fi
    sleep 0.25
  done
  die "MatrixOne did not publish its frontend listener"
}

generate_mo_config() {
  local source_dir="$ROOT_DIR/etc/launch"
  local generated_dir="$TMP_DIR/mo-config"
  mkdir -p "$generated_dir"
  for name in log tn; do
    sed -e "s#\./mo-data#$TMP_DIR/mo-data#g" -e "s#\"mo-data/#\"$TMP_DIR/mo-data/#g" \
      "$source_dir/$name.toml" >"$generated_dir/$name.toml"
  done
  sed -e "s#\./mo-data#$TMP_DIR/mo-data#g" -e "s#\"mo-data/#\"$TMP_DIR/mo-data/#g" \
      "$source_dir/cn.toml" | awk -v port="$MO_PORT" '
        /^\[cn\.frontend\.iceberg\]$/ && !inserted {
          print "[cn.frontend]"
          print "port = " port
          print ""
          inserted = 1
        }
        { print }
        END { if (!inserted) exit 42 }
      ' >"$generated_dir/cn.toml"
  sed -e "s#\./etc/launch/log.toml#$generated_dir/log.toml#" \
      -e "s#\./etc/launch/tn.toml#$generated_dir/tn.toml#" \
      -e "s#\./etc/launch/cn.toml#$generated_dir/cn.toml#" \
      "$source_dir/launch.toml" >"$generated_dir/launch.toml"
}

run_e2e() {
  require docker; require go; require python3
  mkdir -p "$REPORT_DIR"
  TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/mo-kafka-e2e.XXXXXX")"
  trap cleanup EXIT
  export COMPOSE_PROJECT_NAME="mo-kafka-$(basename "$TMP_DIR" | tr '[:upper:].' '[:lower:]-')"
  export KAFKA_PORT="$(pick_free_port)"
  export MO_PORT="0"

  (cd "$ROOT_DIR" && make build)
  generate_mo_config

  compose up -d
  wait_kafka
  log "Kafka is healthy on 127.0.0.1:$KAFKA_PORT"

  if [[ "$(uname -s)" == Darwin ]]; then
    export DYLD_LIBRARY_PATH="$ROOT_DIR/cgo:$ROOT_DIR/thirdparties/install/lib${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
  else
    export LD_LIBRARY_PATH="$ROOT_DIR/cgo:$ROOT_DIR/thirdparties/install/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
  fi
  "$ROOT_DIR/mo-service" -launch "$TMP_DIR/mo-config/launch.toml" >"$TMP_DIR/mo-service.log" 2>&1 &
  MO_PID=$!
  MO_PORT="$(wait_mo_port)"
  export MO_PORT

  (cd "$ROOT_DIR" && go run ./test/kafkaexttab/kafka_e2e_local.go \
    --dsn "root:111@tcp(127.0.0.1:$MO_PORT)/?timeout=5s&readTimeout=60s&writeTimeout=60s" \
    --bootstrap "127.0.0.1:$KAFKA_PORT" \
    --report-dir "$REPORT_DIR")
}

run_unit() {
  (cd "$ROOT_DIR" && .agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=10m \
    -run 'Kafka' ./pkg/sql/colexec/external/... ./pkg/sql/kafka/...)
}

case "$PROFILE" in
  unit) run_unit ;;
  e2e-local) run_e2e ;;
  nightly)
    run_unit
    run_e2e
    ;;
  *) die "unknown profile: $PROFILE" ;;
esac
