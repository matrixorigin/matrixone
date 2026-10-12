#!/usr/bin/env bash
# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE="${1:-e2e-local}"
COMPOSE_BASE="$ROOT_DIR/etc/launch-kafka-local/compose.yaml"
COMPOSE_CONNECT="$ROOT_DIR/etc/launch-kafka-local/compose.connect.yaml"
REPORT_DIR="${MO_KAFKA_CONNECT_REPORT_DIR:-$ROOT_DIR/test/kafkaconnect/reports/ci_$(date -u +%Y%m%dT%H%M%SZ)}"
TMP_DIR=""
MO_PID=""
PORT_LEASE_PID=""
PORT_BLOCK_BASE=""
MO_PORT=""
STATUS_PORT=""
KAFKA_PORT=""
CONNECT_PORT=""
COMPOSE_PROJECT_NAME=""
CURRENT_STAGE="preflight"
SOURCE_SHA=""

readonly PORT_BLOCK_WIDTH=80

source "$ROOT_DIR/optools/mo_e2e_common.bash"

log() { printf '[kafka-connect-ci] %s\n' "$*"; }
die() { printf '[kafka-connect-ci] ERROR: %s\n' "$*" >&2; exit 1; }
require() { command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"; }

e2e_tmp_root() {
  if [[ "$(uname -s)" == Darwin ]]; then
    printf '%s\n' /private/tmp
  else
    printf '%s\n' /tmp
  fi
}

compose() {
  KAFKA_PORT="$KAFKA_PORT" CONNECT_PORT="$CONNECT_PORT" \
    docker compose -p "$COMPOSE_PROJECT_NAME" -f "$COMPOSE_BASE" -f "$COMPOSE_CONNECT" "$@"
}

acquire_port_lease() {
  local status=0
  local lease_dir="${MO_MONGODB_PORT_LEASE_DIR:-/tmp/mo-mongodb-port-leases-$(id -u)}"
  mo_port_block_acquire_lease "$1" "$lease_dir" "${TMPDIR:-/tmp}" || status=$?
  if (( status == 2 )); then
    die "could not create or secure Kafka Connect E2E port-block lease"
  fi
  return "$status"
}

allocate_ports() {
  local requested="${MO_KAFKA_CONNECT_LOG_PORT_BASE:-}" candidate
  if [[ -n "$requested" ]]; then
    [[ "$requested" =~ ^[0-9]+$ ]] || die "MO_KAFKA_CONNECT_LOG_PORT_BASE must be an integer"
    (( requested % PORT_BLOCK_WIDTH == 0 )) || die "MO_KAFKA_CONNECT_LOG_PORT_BASE must align to an ${PORT_BLOCK_WIDTH}-port block"
    (( requested >= 36000 && requested + PORT_BLOCK_WIDTH <= 65536 )) || die "MO_KAFKA_CONNECT_LOG_PORT_BASE is outside the supported range"
    PORT_BLOCK_BASE="$requested"
    acquire_port_lease "$PORT_BLOCK_BASE" || \
      die "MO_KAFKA_CONNECT_LOG_PORT_BASE is already leased by another local E2E run"
    mo_port_block_is_available "$PORT_BLOCK_BASE" "$PORT_BLOCK_WIDTH" || {
      mo_port_block_release_lease
      die "MO_KAFKA_CONNECT_LOG_PORT_BASE is not available on this host"
    }
  else
    while IFS= read -r candidate; do
      PORT_BLOCK_BASE="$candidate"
      acquire_port_lease "$PORT_BLOCK_BASE" || continue
      if mo_port_block_is_available "$PORT_BLOCK_BASE" "$PORT_BLOCK_WIDTH"; then
        break
      fi
      mo_port_block_release_lease
      PORT_BLOCK_BASE=""
    done < <(mo_port_block_candidates "$PORT_BLOCK_WIDTH")
    [[ -n "$PORT_BLOCK_BASE" ]] || die "could not find an available 80-port range for the Kafka Connect E2E cluster"
  fi

  MO_PORT=$((PORT_BLOCK_BASE + 72))
  STATUS_PORT=$((PORT_BLOCK_BASE + 73))
  KAFKA_PORT=$((PORT_BLOCK_BASE + 74))
  CONNECT_PORT=$((PORT_BLOCK_BASE + 75))
}

collect_artifacts() {
  [[ -n "$TMP_DIR" ]] || return 0
  mkdir -p "$REPORT_DIR" || return 1
  if [[ -f "$TMP_DIR/mo-service.log" ]]; then
    cp "$TMP_DIR/mo-service.log" "$REPORT_DIR/mo-service.log" || return 1
  fi
  compose logs --no-color >"$TMP_DIR/connect-compose.raw.log" 2>&1 || true
  python3 - "$TMP_DIR/connect-compose.raw.log" "$REPORT_DIR/connect-compose.log" <<'PY'
import re
import sys

source, destination = sys.argv[1:]
patterns = [
    re.compile(r"(?i)(connection\.password\s*[=:]\s*)(\"[^\"]*\"|[^,\s}]+)"),
    re.compile(r"(?i)(password\s*[=:]\s*)(\"[^\"]*\"|[^,\s}]+)"),
]
with open(source, encoding="utf-8", errors="replace") as input_file, open(destination, "w", encoding="utf-8") as output_file:
    for line in input_file:
        for pattern in patterns:
            line = pattern.sub(r"\1<redacted>", line)
        output_file.write(line)
PY
  compose ps --all --format json >"$REPORT_DIR/connect-compose-ps.json" 2>&1 || true
}

cleanup() {
  local status=$?
  set +e
  mo_e2e_stop_service "$MO_PID"
  MO_PID=""
  collect_artifacts
  if [[ -n "$TMP_DIR" ]]; then
    compose down --volumes --remove-orphans >/dev/null 2>&1 || true
    if [[ -d "$TMP_DIR" && "$(basename "$TMP_DIR")" == mo-kafka-connect-e2e.* ]]; then
      rm -rf -- "$TMP_DIR"
    else
      log "refusing to remove unexpected temporary path: $TMP_DIR"
    fi
  fi
  mo_port_block_release_lease
  if (( status != 0 )) && [[ -n "$REPORT_DIR" && ! -f "$REPORT_DIR/report.json" ]]; then
    mkdir -p "$REPORT_DIR"
    python3 - "$REPORT_DIR/setup-failure.json" "$CURRENT_STAGE" "$SOURCE_SHA" "$status" <<'PY'
import json
import sys

path, stage, sha, status = sys.argv[1:]
with open(path, "w") as report:
    json.dump({"status": "failed", "stage": stage, "source_sha": sha, "exit_status": int(status)}, report, indent=2)
    report.write("\n")
PY
  fi
  return "$status"
}

run_e2e() {
  require docker
  require go
  require python3
  require make
  require shasum
  mkdir -p "$REPORT_DIR"
  HARNESS_SHA="$(git -C "$ROOT_DIR" rev-parse HEAD)"
  SOURCE_SHA="$HARNESS_SHA"
  local tmp_root
  tmp_root="$(e2e_tmp_root)"
  TMP_DIR="$(mktemp -d "${tmp_root%/}/mo-kafka-connect-e2e.XXXXXX")"
  trap cleanup EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM
  COMPOSE_PROJECT_NAME="mo-kc-$(basename "$TMP_DIR" | tr '[:upper:].' '[:lower:]-')"

  CURRENT_STAGE="allocate_ports"
  allocate_ports
  export MO_PORT KAFKA_PORT CONNECT_PORT
  log "owned port block $PORT_BLOCK_BASE-$((PORT_BLOCK_BASE + PORT_BLOCK_WIDTH - 1)); MO=$MO_PORT Kafka=$KAFKA_PORT Connect=$CONNECT_PORT"

  CURRENT_STAGE="build_matrixone"
  local service_bin="$TMP_DIR/mo-service"
  (cd "$ROOT_DIR" && make BIN_NAME="$service_bin" build-with-prebuilt-native)
  mo_e2e_generate_launch_config "$ROOT_DIR" "$TMP_DIR" "$PORT_BLOCK_BASE" "$MO_PORT" "$STATUS_PORT"

  if [[ "$(uname -s)" == Darwin ]]; then
    export DYLD_LIBRARY_PATH="$ROOT_DIR/cgo:$ROOT_DIR/thirdparties/install/lib${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
  else
    export LD_LIBRARY_PATH="$ROOT_DIR/cgo:$ROOT_DIR/thirdparties/install/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
  fi
  CURRENT_STAGE="start_matrixone"
  "$service_bin" -launch "$TMP_DIR/mo-config/launch.toml" >"$TMP_DIR/mo-service.log" 2>&1 &
  MO_PID=$!
  mo_e2e_wait_frontend_port "$MO_PID" "$TMP_DIR/mo-service.log" "$MO_PORT" >/dev/null || die "MatrixOne did not bind the owned frontend port"

  CURRENT_STAGE="validate_compose"
  compose config -q
  CURRENT_STAGE="start_kafka_connect"
  compose up -d
  CURRENT_STAGE="run_recovery_driver"
  local binary_sha
  binary_sha="$(shasum -a 256 "$service_bin" | awk '{print $1}')"
  (cd "$ROOT_DIR" && go run ./test/kafkaconnect/kafka_connect_e2e_local.go \
    --dsn "root:111@tcp(127.0.0.1:$MO_PORT)/?timeout=5s&readTimeout=30s&writeTimeout=30s" \
    --bootstrap "127.0.0.1:$KAFKA_PORT" \
    --connect-url "http://127.0.0.1:$CONNECT_PORT" \
    --compose-base "$COMPOSE_BASE" --compose-connect "$COMPOSE_CONNECT" \
    --compose-project "$COMPOSE_PROJECT_NAME" --connector-name "mo-issue-29848-recovery" \
    --source-sha "$SOURCE_SHA" --harness-sha "$HARNESS_SHA" \
    --mo-binary-sha "$binary_sha" --report-dir "$REPORT_DIR")
}

case "$PROFILE" in
  e2e-local) run_e2e ;;
  *) die "unknown profile: $PROFILE" ;;
esac
