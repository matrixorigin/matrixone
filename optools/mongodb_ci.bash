#!/usr/bin/env bash
# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE="${1:-e2e-local}"
REPORT_DIR="${MO_MONGODB_REPORT_DIR:-${ROOT_DIR}/test/mongodb/reports/ci_$(date -u +%Y%m%dT%H%M%SZ)}"
TMP_DIR=""
MO_PID=""

readonly PORT_BLOCK_WIDTH=80
# AddressManager permits its base port plus 20 fallback slots when an address
# is claimed concurrently, so reserve all 21 possible listener ports.
readonly ADDRESS_MANAGER_PORT_COUNT=21
readonly TN_PORT_OFFSET=24
readonly CN_PORT_OFFSET=48
readonly FRONTEND_PORT_OFFSET=72
readonly STATUS_PORT_OFFSET=73

log() { printf '[mongodb-ci] %s\n' "$*"; }
die() { printf '[mongodb-ci] ERROR: %s\n' "$*" >&2; exit 1; }
require() { command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"; }

sanitize() {
  local source="$1" target="$2"
  if [[ ! -f "$source" ]]; then printf 'not captured\n' >"$target"; return; fi
  sed -E \
    -e 's#mongodb(\+srv)?://[^[:space:]]+#mongodb://<redacted>#g' \
    -e 's#(password|credential|token|secret)[=:][^ ,}\"]+#\1=<redacted>#Ig' \
    -e 's#MO_MONGODB_E2E_CREDENTIAL[^[:space:]]*#MO_MONGODB_E2E_CREDENTIAL=<redacted>#g' \
    "$source" >"$target"
}

collect() {
  [[ -n "$TMP_DIR" ]] || return
  mkdir -p "$REPORT_DIR"
  sanitize "$TMP_DIR/mo-service.log" "$REPORT_DIR/mo-service.log"
  if command -v docker >/dev/null 2>&1; then
    MONGODB_PORT="${MONGODB_PORT:-27017}" MONGODB_ROOT_USER="${MONGODB_ROOT_USER:-x}" \
      MONGODB_ROOT_PASSWORD="${MONGODB_ROOT_PASSWORD:-x}" MONGODB_KEYFILE="${MONGODB_KEYFILE:-/dev/null}" \
      docker compose -p "${COMPOSE_PROJECT_NAME:-mo-mongodb-unused}" -f "$ROOT_DIR/etc/launch-mongodb-local/compose.yaml" logs --no-color \
      >"$TMP_DIR/mongodb.log" 2>&1 || true
    sanitize "$TMP_DIR/mongodb.log" "$REPORT_DIR/mongodb.log"
    docker inspect "${COMPOSE_PROJECT_NAME:-mo-mongodb-unused}-mongo-1" >"$TMP_DIR/docker-inspect.json" 2>&1 || true
    sanitize "$TMP_DIR/docker-inspect.json" "$REPORT_DIR/docker-inspect.json"
  fi
}

cleanup() {
  local status=$?
  # Cleanup must continue even when artifact collection itself fails. Preserve
  # the original test status and make every teardown action best-effort.
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
    MONGODB_PORT="${MONGODB_PORT:-27017}" MONGODB_ROOT_USER="${MONGODB_ROOT_USER:-x}" \
      MONGODB_ROOT_PASSWORD="${MONGODB_ROOT_PASSWORD:-x}" MONGODB_KEYFILE="${MONGODB_KEYFILE:-/dev/null}" \
      docker compose -p "${COMPOSE_PROJECT_NAME:-mo-mongodb-unused}" -f "$ROOT_DIR/etc/launch-mongodb-local/compose.yaml" down --volumes --remove-orphans >/dev/null 2>&1 || true
    # TMP_DIR is created by the exact mktemp template below. Refuse a broad
    # deletion if that invariant is ever changed or corrupted.
    if [[ -d "$TMP_DIR" && "$(basename "$TMP_DIR")" == mo-mongodb-e2e.* ]]; then
      rm -rf -- "$TMP_DIR"
    else
      log "refusing to remove unexpected temporary path: $TMP_DIR"
    fi
  fi
  return "$status"
}

wait_mongo() {
  local deadline=$((SECONDS + 120))
  until docker compose -p "$COMPOSE_PROJECT_NAME" -f "$ROOT_DIR/etc/launch-mongodb-local/compose.yaml" exec -T mongo \
      mongosh --quiet -u "$MONGODB_ROOT_USER" -p "$MONGODB_ROOT_PASSWORD" --authenticationDatabase admin \
      --eval 'quit(db.adminCommand({ping:1}).ok ? 0 : 2)' >/dev/null 2>&1; do
    (( SECONDS < deadline )) || die "MongoDB did not become healthy"
    sleep 1
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

wait_primary() {
  local deadline=$((SECONDS + 120))
  until docker compose -p "$COMPOSE_PROJECT_NAME" -f "$ROOT_DIR/etc/launch-mongodb-local/compose.yaml" exec -T mongo \
      mongosh --quiet -u "$MONGODB_ROOT_USER" -p "$MONGODB_ROOT_PASSWORD" --authenticationDatabase admin \
      --eval 'quit(db.hello().isWritablePrimary ? 0 : 2)' >/dev/null 2>&1; do
    (( SECONDS < deadline )) || die "ReplicaSet did not elect a writable primary"
    sleep 1
  done
}

find_free_port_block() {
  python3 - <<'PY'
import random
import socket

# A CN/TN address manager can claim its base port plus 20 fallback ports. Keep every
# MatrixOne listener in one verified block so this disposable E2E deployment
# cannot collide with itself or a local developer cluster.
width = 80
candidates = list(range(36000, 57000 - width, width))
random.shuffle(candidates)
for base in candidates:
    sockets = []
    try:
        for port in range(base, base + width):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(("127.0.0.1", port))
            sockets.append(sock)
    except OSError:
        pass
    else:
        print(base)
        raise SystemExit(0)
    finally:
        for sock in sockets:
            sock.close()
raise SystemExit("could not find a free 80-port range for the MongoDB E2E cluster")
PY
}

is_tcp_port() {
  local port="$1"
  [[ "$port" =~ ^[1-9][0-9]{0,4}$ ]] && (( 10#$port <= 65535 ))
}

validate_tcp_port() {
  local name="$1" port="$2"
  is_tcp_port "$port" || die "$name must be between 1 and 65535"
}

validate_port_block_base() {
  local name="$1" port="$2"
  validate_tcp_port "$name" "$port"
  (( 10#$port + PORT_BLOCK_WIDTH - 1 <= 65535 )) || \
    die "$name must leave room for the ${PORT_BLOCK_WIDTH}-port block"
}

assert_disjoint_port_ranges() {
  local left_name="$1" left_start="$2" left_end="$3"
  local right_name="$4" right_start="$5" right_end="$6"
  if (( left_start <= right_end && right_start <= left_end )); then
    die "$left_name overlaps $right_name (${left_start}-${left_end} vs ${right_start}-${right_end})"
  fi
}

validate_port_allocation() {
  validate_tcp_port "MO_MONGODB_FRONTEND_PORT" "$MO_PORT"
  validate_tcp_port "MO_MONGODB_STATUS_PORT" "$STATUS_PORT"
  validate_port_block_base "MO_MONGODB_LOG_PORT_BASE" "$LOG_PORT_BASE"

  assert_disjoint_port_ranges "LogService" "$LOG_RAFT_PORT" "$LOG_GOSSIP_PORT" \
    "TN" "$TN_PORT_BASE" "$((TN_PORT_BASE + ADDRESS_MANAGER_PORT_COUNT - 1))"
  assert_disjoint_port_ranges "LogService" "$LOG_RAFT_PORT" "$LOG_GOSSIP_PORT" \
    "CN" "$CN_PORT_BASE" "$((CN_PORT_BASE + ADDRESS_MANAGER_PORT_COUNT - 1))"
  assert_disjoint_port_ranges "LogService" "$LOG_RAFT_PORT" "$LOG_GOSSIP_PORT" \
    "frontend" "$MO_PORT" "$MO_PORT"
  assert_disjoint_port_ranges "LogService" "$LOG_RAFT_PORT" "$LOG_GOSSIP_PORT" \
    "status" "$STATUS_PORT" "$STATUS_PORT"
  assert_disjoint_port_ranges "TN" "$TN_PORT_BASE" "$((TN_PORT_BASE + ADDRESS_MANAGER_PORT_COUNT - 1))" \
    "CN" "$CN_PORT_BASE" "$((CN_PORT_BASE + ADDRESS_MANAGER_PORT_COUNT - 1))"
  assert_disjoint_port_ranges "TN" "$TN_PORT_BASE" "$((TN_PORT_BASE + ADDRESS_MANAGER_PORT_COUNT - 1))" \
    "frontend" "$MO_PORT" "$MO_PORT"
  assert_disjoint_port_ranges "TN" "$TN_PORT_BASE" "$((TN_PORT_BASE + ADDRESS_MANAGER_PORT_COUNT - 1))" \
    "status" "$STATUS_PORT" "$STATUS_PORT"
  assert_disjoint_port_ranges "CN" "$CN_PORT_BASE" "$((CN_PORT_BASE + ADDRESS_MANAGER_PORT_COUNT - 1))" \
    "frontend" "$MO_PORT" "$MO_PORT"
  assert_disjoint_port_ranges "CN" "$CN_PORT_BASE" "$((CN_PORT_BASE + ADDRESS_MANAGER_PORT_COUNT - 1))" \
    "status" "$STATUS_PORT" "$STATUS_PORT"
  assert_disjoint_port_ranges "frontend" "$MO_PORT" "$MO_PORT" \
    "status" "$STATUS_PORT" "$STATUS_PORT"
}

allocate_mo_ports() {
  LOG_PORT_BASE="${MO_MONGODB_LOG_PORT_BASE:-$(find_free_port_block)}"
  validate_port_block_base "MO_MONGODB_LOG_PORT_BASE" "$LOG_PORT_BASE"

  LOG_RAFT_PORT="$LOG_PORT_BASE"
  LOG_SERVICE_PORT="$((LOG_PORT_BASE + 1))"
  LOG_GOSSIP_PORT="$((LOG_PORT_BASE + 2))"
  TN_PORT_BASE="$((LOG_PORT_BASE + TN_PORT_OFFSET))"
  CN_PORT_BASE="$((LOG_PORT_BASE + CN_PORT_OFFSET))"
  MO_PORT="${MO_MONGODB_FRONTEND_PORT:-$((LOG_PORT_BASE + FRONTEND_PORT_OFFSET))}"
  STATUS_PORT="${MO_MONGODB_STATUS_PORT:-$((LOG_PORT_BASE + STATUS_PORT_OFFSET))}"

  validate_port_allocation
}

print_port_plan() {
  printf 'LOG_PORT_BASE=%s\n' "$LOG_PORT_BASE"
  printf 'LOG_RAFT_PORT=%s\n' "$LOG_RAFT_PORT"
  printf 'LOG_SERVICE_PORT=%s\n' "$LOG_SERVICE_PORT"
  printf 'LOG_GOSSIP_PORT=%s\n' "$LOG_GOSSIP_PORT"
  printf 'TN_PORT_BASE=%s\n' "$TN_PORT_BASE"
  printf 'CN_PORT_BASE=%s\n' "$CN_PORT_BASE"
  printf 'MO_PORT=%s\n' "$MO_PORT"
  printf 'STATUS_PORT=%s\n' "$STATUS_PORT"
}

generate_mo_config() {
  local source_dir="$ROOT_DIR/etc/launch"
  local generated_dir="$TMP_DIR/mo-config"
  mkdir -p "$generated_dir"
  for name in log tn; do
    sed -e "s#\./mo-data#$TMP_DIR/mo-data#g" -e "s#\"mo-data/#\"$TMP_DIR/mo-data/#g" \
      -e "s#^status-port = [0-9][0-9]*#status-port = $STATUS_PORT#" \
      -e "s#^port-base = [0-9][0-9]*#port-base = $TN_PORT_BASE#" \
      "$source_dir/$name.toml" >"$generated_dir/$name.toml"
  done
  sed -e "s#\./mo-data#$TMP_DIR/mo-data#g" -e "s#\"mo-data/#\"$TMP_DIR/mo-data/#g" \
      -e "s#^status-port = [0-9][0-9]*#status-port = $STATUS_PORT#" \
      -e "s#^port-base = [0-9][0-9]*#port-base = $CN_PORT_BASE#" \
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
  {
    printf '\n[logservice]\n'
    printf 'raft-port = %s\n' "$LOG_RAFT_PORT"
    printf 'logservice-port = %s\n' "$LOG_SERVICE_PORT"
    printf 'gossip-port = %s\n' "$LOG_GOSSIP_PORT"
    printf 'gossip-seed-addresses = ["127.0.0.1:%s"]\n' "$LOG_GOSSIP_PORT"
  } >>"$generated_dir/log.toml"
  {
    printf '\n[hakeeper-client]\n'
    printf 'service-addresses = ["127.0.0.1:%s"]\n' "$LOG_SERVICE_PORT"
  } >>"$generated_dir/log.toml"
  {
    printf '\n[hakeeper-client]\n'
    printf 'service-addresses = ["127.0.0.1:%s"]\n' "$LOG_SERVICE_PORT"
  } >>"$generated_dir/tn.toml"
  {
    printf '\n[hakeeper-client]\n'
    printf 'service-addresses = ["127.0.0.1:%s"]\n' "$LOG_SERVICE_PORT"
  } >>"$generated_dir/cn.toml"
  {
    printf '\n[cn.frontend.mongodb]\n'
    printf 'enable = true\nallow-loopback = true\n'
    printf 'connect-timeout = "10s"\nserver-selection-timeout = "10s"\nsocket-timeout = "30s"\n'
    printf 'batch-rows = 2\nmax-batch-bytes = 1048576\nmax-value-bytes = 524288\nmax-source-concurrency = 2\n'
  } >>"$generated_dir/cn.toml"
  sed -e "s#\./etc/launch/log.toml#$generated_dir/log.toml#" \
      -e "s#\./etc/launch/tn.toml#$generated_dir/tn.toml#" \
      -e "s#\./etc/launch/cn.toml#$generated_dir/cn.toml#" \
      "$source_dir/launch.toml" >"$generated_dir/launch.toml"
}

run_e2e() {
  require docker; require go; require python3; require openssl
  mkdir -p "$REPORT_DIR"
  TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/mo-mongodb-e2e.XXXXXX")"
  trap cleanup EXIT
  export COMPOSE_PROJECT_NAME="mo-mongodb-$(basename "$TMP_DIR" | tr '[:upper:].' '[:lower:]-')"
  # Docker owns its published listener. MatrixOne treats a frontend port of 0
  # as its default (6001), so use one verified reservation for this disposable
  # cluster instead of separately selecting ports that can overlap each other.
  allocate_mo_ports
  export MONGODB_PORT="" MO_PORT
  export MONGODB_ROOT_USER="root_$(openssl rand -hex 6)"
  export MONGODB_ROOT_PASSWORD="$(openssl rand -hex 24)"
  export MONGODB_READER_PASSWORD="$(openssl rand -hex 24)"
  export MONGODB_READER_NEXT_PASSWORD="$(openssl rand -hex 24)"
  export MONGODB_KEYFILE="$TMP_DIR/mongodb-keyfile"
  openssl rand -base64 756 >"$MONGODB_KEYFILE"
  chmod 600 "$MONGODB_KEYFILE"

  (cd "$ROOT_DIR" && make build)
  generate_mo_config
  docker compose -p "$COMPOSE_PROJECT_NAME" -f "$ROOT_DIR/etc/launch-mongodb-local/compose.yaml" up -d
  MONGODB_PORT="$(docker compose -p "$COMPOSE_PROJECT_NAME" -f "$ROOT_DIR/etc/launch-mongodb-local/compose.yaml" port mongo 27017 | sed -nE 's/.*:([0-9]+)$/\1/p' | tail -1)"
  [[ "$MONGODB_PORT" =~ ^[1-9][0-9]*$ ]] || die "Docker did not publish the MongoDB listener"
  export MONGODB_PORT
  wait_mongo
  docker compose -p "$COMPOSE_PROJECT_NAME" -f "$ROOT_DIR/etc/launch-mongodb-local/compose.yaml" exec -T mongo \
    mongosh --quiet -u "$MONGODB_ROOT_USER" -p "$MONGODB_ROOT_PASSWORD" --authenticationDatabase admin \
    --eval 'try { rs.status() } catch (_) { rs.initiate({_id:"rs0",members:[{_id:0,host:"mongo:27017"}]}) }' >/dev/null
  wait_primary
  docker compose -p "$COMPOSE_PROJECT_NAME" -f "$ROOT_DIR/etc/launch-mongodb-local/compose.yaml" exec -T \
    -e MONGODB_READER_PASSWORD="$MONGODB_READER_PASSWORD" \
    -e MONGODB_READER_NEXT_PASSWORD="$MONGODB_READER_NEXT_PASSWORD" mongo \
    mongosh --quiet -u "$MONGODB_ROOT_USER" -p "$MONGODB_ROOT_PASSWORD" --authenticationDatabase admin \
    <"$ROOT_DIR/etc/launch-mongodb-local/init_and_seed.js" >/dev/null
  export MO_MONGODB_E2E_CREDENTIAL="{\"Username\":\"mo_reader\",\"Password\":\"$MONGODB_READER_PASSWORD\"}"
	export MO_MONGODB_E2E_CREDENTIAL_NEXT="{\"Username\":\"mo_reader_next\",\"Password\":\"$MONGODB_READER_NEXT_PASSWORD\"}"
	if [[ "$(uname -s)" == Darwin ]]; then
		export DYLD_LIBRARY_PATH="$ROOT_DIR/cgo:$ROOT_DIR/thirdparties/install/lib${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
	else
		export LD_LIBRARY_PATH="$ROOT_DIR/cgo:$ROOT_DIR/thirdparties/install/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
	fi
  "$ROOT_DIR/mo-service" -launch "$TMP_DIR/mo-config/launch.toml" >"$TMP_DIR/mo-service.log" 2>&1 &
  MO_PID=$!
  PUBLISHED_MO_PORT="$(wait_mo_port)"
  [[ "$PUBLISHED_MO_PORT" == "$MO_PORT" ]] || die "MatrixOne published frontend port $PUBLISHED_MO_PORT, expected $MO_PORT"
  export MO_PORT
  (cd "$ROOT_DIR" && go run ./test/mongodb/mongodb_e2e_local.go \
    --dsn "root:111@tcp(127.0.0.1:$MO_PORT)/?timeout=5s&readTimeout=30s&writeTimeout=30s" \
    --mongo-host "127.0.0.1:$MONGODB_PORT" --report-dir "$REPORT_DIR")
}

run_unit() {
	(cd "$ROOT_DIR" && make build >/dev/null)
	(cd "$ROOT_DIR" && python3 -m unittest discover -s test/mongodb -p 'test_*.py')
	(cd "$ROOT_DIR" && .agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=10m \
		./pkg/sql/mongodb ./pkg/sql/colexec/mongoscan ./pkg/sql/colexec/aggexec ./pkg/sql/colexec/timewin \
		./pkg/sql/parsers/dialect/mysql ./pkg/sql/plan ./pkg/sql/compile ./pkg/frontend)
}

case "$PROFILE" in
  unit) run_unit ;;
  e2e-local) run_e2e ;;
  port-plan)
    require python3
    allocate_mo_ports
    print_port_plan
    ;;
  nightly)
	run_unit
	run_e2e
	;;
  *) die "unknown profile: $PROFILE" ;;
esac
