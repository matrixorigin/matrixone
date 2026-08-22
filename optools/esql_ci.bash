#!/usr/bin/env bash
# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.
#
# End-to-end harness for the esql_tvf table function. It stands up a single-node
# Elasticsearch via docker compose on a random published port, seeds a small
# deterministic index, boots a fresh mo-service on a random frontend port, then
# runs the Go driver (test/esqltvf/esql_e2e_local.go) which issues esql_tvf
# queries against the seeded index over a MySQL DSN. Everything is torn down on
# exit. Mirrors optools/mongodb_ci.bash.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE="${1:-e2e-local}"
REPORT_DIR="${MO_ESQL_REPORT_DIR:-${ROOT_DIR}/test/esqltvf/reports/ci_$(date -u +%Y%m%dT%H%M%SZ)}"
TMP_DIR=""
MO_PID=""

COMPOSE_FILE="$ROOT_DIR/etc/launch-esql-local/compose.yaml"

log() { printf '[esql-ci] %s\n' "$*"; }
die() { printf '[esql-ci] ERROR: %s\n' "$*" >&2; exit 1; }
require() { command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"; }

compose() {
  ES_PASSWORD="${ES_PASSWORD:-x}" \
    docker compose -p "${COMPOSE_PROJECT_NAME:-mo-esql-unused}" -f "$COMPOSE_FILE" "$@"
}

sanitize() {
  local source="$1" target="$2"
  if [[ ! -f "$source" ]]; then printf 'not captured\n' >"$target"; return; fi
  sed -E \
    -e 's#(ELASTIC_PASSWORD|password|apikey|api_key|credential|token|secret)([=:"]+)[^ ,}"]+#\1\2<redacted>#Ig' \
    -e "s#${ES_PASSWORD:-__no_such_password__}#<redacted>#g" \
    "$source" >"$target"
}

collect() {
  [[ -n "$TMP_DIR" ]] || return
  mkdir -p "$REPORT_DIR"
  sanitize "$TMP_DIR/mo-service.log" "$REPORT_DIR/mo-service.log"
  if command -v docker >/dev/null 2>&1; then
    compose logs --no-color >"$TMP_DIR/elasticsearch.log" 2>&1 || true
    sanitize "$TMP_DIR/elasticsearch.log" "$REPORT_DIR/elasticsearch.log"
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
    compose down --volumes --remove-orphans >/dev/null 2>&1 || true
    # TMP_DIR is created by the exact mktemp template below. Refuse a broad
    # deletion if that invariant is ever changed or corrupted.
    if [[ -d "$TMP_DIR" && "$(basename "$TMP_DIR")" == mo-esql-e2e.* ]]; then
      rm -rf -- "$TMP_DIR"
    else
      log "refusing to remove unexpected temporary path: $TMP_DIR"
    fi
  fi
  return "$status"
}

wait_es() {
  local deadline=$((SECONDS + 180))
  until curl -s -u "elastic:$ES_PASSWORD" "http://127.0.0.1:$ES_PORT/_cluster/health" 2>/dev/null \
      | grep -qE '"status":"(green|yellow)"'; do
    (( SECONDS < deadline )) || die "Elasticsearch did not become healthy"
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

seed() {
  # Deterministic employees index. One document has a null salary so the driver
  # can assert null -> SQL NULL. Salaries are chosen so exactly two rows exceed
  # 100000 (Bob 150000, Dave 120000).
  curl -s -u "elastic:$ES_PASSWORD" -X PUT "http://127.0.0.1:$ES_PORT/employees" \
    -H 'Content-Type: application/json' -d '{
      "mappings": {"properties": {
        "name": {"type": "keyword"},
        "dept": {"type": "keyword"},
        "salary": {"type": "long"},
        "active": {"type": "boolean"}
      }}
    }' >/dev/null || die "create index failed"

  curl -s -u "elastic:$ES_PASSWORD" -X POST "http://127.0.0.1:$ES_PORT/employees/_bulk" \
    -H 'Content-Type: application/x-ndjson' --data-binary $'{"index":{"_id":"1"}}\n{"name":"Alice","dept":"eng","salary":90000,"active":true}\n{"index":{"_id":"2"}}\n{"name":"Bob","dept":"eng","salary":150000,"active":true}\n{"index":{"_id":"3"}}\n{"name":"Carol","dept":"sales","salary":80000,"active":false}\n{"index":{"_id":"4"}}\n{"name":"Dave","dept":"sales","salary":120000,"active":true}\n{"index":{"_id":"5"}}\n{"name":"Eve","dept":"ops","salary":null,"active":false}\n' \
    >/dev/null || die "bulk seed failed"

  curl -s -u "elastic:$ES_PASSWORD" -X POST "http://127.0.0.1:$ES_PORT/employees/_refresh" >/dev/null || die "refresh failed"
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
  require docker; require go; require curl; require openssl
  mkdir -p "$REPORT_DIR"
  TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/mo-esql-e2e.XXXXXX")"
  trap cleanup EXIT
  export COMPOSE_PROJECT_NAME="mo-esql-$(basename "$TMP_DIR" | tr '[:upper:].' '[:lower:]-')"
  export ES_PASSWORD="$(openssl rand -hex 24)"
  export MO_PORT="0"

  (cd "$ROOT_DIR" && make build)
  generate_mo_config

  compose up -d
  ES_PORT="$(compose port elasticsearch 9200 | sed -nE 's/.*:([0-9]+)$/\1/p' | tail -1)"
  [[ "$ES_PORT" =~ ^[1-9][0-9]*$ ]] || die "Docker did not publish the Elasticsearch listener"
  export ES_PORT
  wait_es
  seed

  if [[ "$(uname -s)" == Darwin ]]; then
    export DYLD_LIBRARY_PATH="$ROOT_DIR/cgo:$ROOT_DIR/thirdparties/install/lib${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
  else
    export LD_LIBRARY_PATH="$ROOT_DIR/cgo:$ROOT_DIR/thirdparties/install/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
  fi
  "$ROOT_DIR/mo-service" -launch "$TMP_DIR/mo-config/launch.toml" >"$TMP_DIR/mo-service.log" 2>&1 &
  MO_PID=$!
  MO_PORT="$(wait_mo_port)"
  export MO_PORT

  (cd "$ROOT_DIR" && go run ./test/esqltvf/esql_e2e_local.go \
    --dsn "root:111@tcp(127.0.0.1:$MO_PORT)/?timeout=5s&readTimeout=30s&writeTimeout=30s" \
    --es-endpoint "http://127.0.0.1:$ES_PORT" \
    --es-user "elastic" --es-password "$ES_PASSWORD" \
    --report-dir "$REPORT_DIR")
}

run_unit() {
  (cd "$ROOT_DIR" && .agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=10m \
    ./pkg/sql/foreigntvf/...)
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
