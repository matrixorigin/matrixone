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
CLEANUP_ERRORS=()

readonly PORT_BLOCK_WIDTH=80

source "$ROOT_DIR/optools/mo_e2e_common.bash"

log() { printf '[kafka-connect-ci] %s\n' "$*"; }
die() { printf '[kafka-connect-ci] ERROR: %s\n' "$*" >&2; exit 1; }
require() { command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"; }
record_cleanup_error() {
  CLEANUP_ERRORS+=("$1")
  log "cleanup failed: $1"
}

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
      if ! acquire_port_lease "$PORT_BLOCK_BASE"; then
        PORT_BLOCK_BASE=""
        continue
      fi
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
  compose logs --no-color >"$TMP_DIR/connect-compose.raw.log" 2>&1 || return 1
  python3 - "$TMP_DIR/connect-compose.raw.log" "$REPORT_DIR/connect-compose.log" <<'PY' || return 1
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
  compose ps --all --format json >"$REPORT_DIR/connect-compose-ps.json" 2>&1 || return 1
}

cleanup() {
  local status=$?
  local service_pid="$MO_PID"
  set +e
  set +u
  if ! mo_e2e_stop_service "$service_pid"; then
    record_cleanup_error "could not stop the owned MatrixOne process"
  fi
  if [[ -n "$service_pid" ]] && kill -0 "$service_pid" >/dev/null 2>&1; then
    record_cleanup_error "owned MatrixOne process remains after shutdown"
  fi
  MO_PID=""
  if ! collect_artifacts; then
    record_cleanup_error "could not collect run artifacts"
  fi
  if [[ -n "$TMP_DIR" ]]; then
    if ! compose down --volumes --remove-orphans >/dev/null 2>&1; then
      record_cleanup_error "docker compose down failed for owned project $COMPOSE_PROJECT_NAME"
    fi
    if [[ -d "$TMP_DIR" && "$(basename "$TMP_DIR")" == mo-kafka-connect-e2e.* ]]; then
      if ! rm -rf -- "$TMP_DIR"; then
        record_cleanup_error "could not remove owned temporary directory $TMP_DIR"
      fi
    else
      record_cleanup_error "refusing to remove unexpected temporary path: $TMP_DIR"
    fi
  fi
  mo_port_block_release_lease
  if ((${#CLEANUP_ERRORS[@]} > 0)); then
    (( status != 0 )) || status=1
    if [[ -n "$REPORT_DIR" && -f "$REPORT_DIR/report.json" ]]; then
      python3 - "$REPORT_DIR/report.json" "${CLEANUP_ERRORS[@]}" <<'PY'
import json
import sys

path, *cleanup_errors = sys.argv[1:]
with open(path, encoding="utf-8") as report_file:
    report = json.load(report_file)
report["cleanup_status"] = "failed"
report["cleanup_errors"] = cleanup_errors
report["status"] = "failed"
message = "cleanup failed: " + "; ".join(cleanup_errors)
report["error"] = "; ".join(value for value in (report.get("error"), message) if value)
with open(path, "w", encoding="utf-8") as report_file:
    json.dump(report, report_file, indent=2)
    report_file.write("\n")
PY
    fi
  fi
  if (( status != 0 )) && [[ -n "$REPORT_DIR" && ! -f "$REPORT_DIR/report.json" ]]; then
    mkdir -p "$REPORT_DIR"
    python3 - "$REPORT_DIR/setup-failure.json" "$CURRENT_STAGE" "$SOURCE_SHA" "$status" "${CLEANUP_ERRORS[@]}" <<'PY'
import json
import sys

path, stage, sha, status, *cleanup_errors = sys.argv[1:]
with open(path, "w") as report:
    json.dump({"status": "failed", "stage": stage, "source_sha": sha, "exit_status": int(status), "cleanup_errors": cleanup_errors}, report, indent=2)
    report.write("\n")
PY
  fi
  trap - EXIT
  exit "$status"
}

run_e2e() {
  mkdir -p "$REPORT_DIR"
  rm -f -- "$REPORT_DIR/report.json" "$REPORT_DIR/setup-failure.json"
  trap cleanup EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM

  require docker
  require go
  require python3
  require make
  require shasum
  HARNESS_SHA="$(git -C "$ROOT_DIR" rev-parse HEAD)"
  SOURCE_SHA="$HARNESS_SHA"
  local tmp_root
  tmp_root="$(e2e_tmp_root)"
  TMP_DIR="$(mktemp -d "${tmp_root%/}/mo-kafka-connect-e2e.XXXXXX")"
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
