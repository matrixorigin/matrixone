#!/usr/bin/env bash
# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.
#
# Runs the real Connector/J pool-borrow regression against a fresh local
# MatrixOne. The Java fixture and server are mandatory prerequisites.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR=""
MO_PID=""

die() { printf '[connectorj-pool-reset] ERROR: %s\n' "$*" >&2; exit 1; }

cleanup() {
  local status=$?
  set +e
  if [[ -n "$MO_PID" ]] && kill -0 "$MO_PID" >/dev/null 2>&1; then
    kill "$MO_PID" >/dev/null 2>&1 || true
    wait "$MO_PID" >/dev/null 2>&1 || true
  fi
  if [[ -n "$TMP_DIR" && -d "$TMP_DIR" && "$(basename "$TMP_DIR")" == mo-connectorj-pool-reset.* ]]; then
    rm -rf -- "$TMP_DIR"
  fi
  return "$status"
}

wait_mo_port() {
  local deadline=$((SECONDS + 180)) port=""
  while (( SECONDS < deadline )); do
    if [[ -n "$MO_PID" ]] && ! kill -0 "$MO_PID" >/dev/null 2>&1; then
      tail -200 "$TMP_DIR/mo-service.log" >&2 || true
      die "MatrixOne exited before publishing its frontend listener"
    fi
    port="$(sed -nE 's/.*Server Listening on : [^ ]*:([0-9]+).*/\1/p' "$TMP_DIR/mo-service.log" | tail -1)"
    if [[ "$port" =~ ^[1-9][0-9]*$ ]]; then
      printf '%s\n' "$port"
      return
    fi
    sleep 0.25
  done
  tail -200 "$TMP_DIR/mo-service.log" >&2 || true
  die "MatrixOne did not publish its frontend listener"
}

generate_mo_config() {
  local source_dir="$ROOT_DIR/etc/launch"
  local config_dir="$TMP_DIR/mo-config"
  mkdir -p "$config_dir"
  for name in log tn; do
    sed -e "s#\./mo-data#$TMP_DIR/mo-data#g" -e "s#\"mo-data/#\"$TMP_DIR/mo-data/#g" \
      "$source_dir/$name.toml" >"$config_dir/$name.toml"
  done
  sed -e "s#\./mo-data#$TMP_DIR/mo-data#g" -e "s#\"mo-data/#\"$TMP_DIR/mo-data/#g" \
    "$source_dir/cn.toml" | awk '
      /^\[cn\.frontend\.iceberg\]$/ && !inserted {
        print "[cn.frontend]"
        print "port = 0"
        print ""
        inserted = 1
      }
      { print }
      END { if (!inserted) exit 42 }
    ' >"$config_dir/cn.toml"
  sed -e "s#\./etc/launch/log.toml#$config_dir/log.toml#" \
      -e "s#\./etc/launch/tn.toml#$config_dir/tn.toml#" \
      -e "s#\./etc/launch/cn.toml#$config_dir/cn.toml#" \
      "$source_dir/launch.toml" >"$config_dir/launch.toml"
}

build_mo_service() {
  # CI compiles the native runtime from this checkout. Local validation may
  # reuse the verified read-only CGo test facilities, but still compiles this
  # checkout's Go server into the per-run directory.
  if [[ "${MO_CONNECTORJ_USE_PREBUILT_NATIVE:-0}" != "1" ]]; then
    (cd "$ROOT_DIR" && make cgo)
  fi

  local native_include="$ROOT_DIR/thirdparties/install/include"
  local native_lib="$ROOT_DIR/thirdparties/install/lib"
  [[ -d "$native_include" && -d "$native_lib" ]] || die "native CGo prerequisites are unavailable"

  local extldflags="-L$ROOT_DIR/cgo -lmo -L$native_lib -Wl,-rpath,$ROOT_DIR/cgo -Wl,-rpath,$native_lib"
  if [[ "$(uname -s)" != Darwin ]]; then
    extldflags="$extldflags -fopenmp"
  fi
  (cd "$ROOT_DIR" && CGO_CFLAGS="-I$ROOT_DIR/cgo -I$native_include" \
    go build -mod=readonly -ldflags="-extldflags '$extldflags'" -o "$TMP_DIR/mo-service" ./cmd/mo-service)
}

command -v go >/dev/null 2>&1 || die "go is required"
command -v java >/dev/null 2>&1 || die "java is required"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/mo-connectorj-pool-reset.XXXXXX")"
trap cleanup EXIT INT HUP TERM

(cd "$ROOT_DIR" && make jstfu)
build_mo_service
generate_mo_config
if [[ "$(uname -s)" == Darwin ]]; then
  export DYLD_LIBRARY_PATH="$ROOT_DIR/cgo:$ROOT_DIR/thirdparties/install/lib${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
else
  export LD_LIBRARY_PATH="$ROOT_DIR/cgo:$ROOT_DIR/thirdparties/install/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
fi
"$TMP_DIR/mo-service" -launch "$TMP_DIR/mo-config/launch.toml" >"$TMP_DIR/mo-service.log" 2>&1 &
MO_PID=$!
MO_PORT="$(wait_mo_port)"

(cd "$ROOT_DIR" && MO_DATASTREAM_E2E_DSN="dump:111@tcp(127.0.0.1:${MO_PORT})/" \
  MO_CONNECTORJ_POOL_RESET_REQUIRED=1 \
  go test -mod=readonly -v -count=1 -timeout=10m -run '^TestConnectorJConnectionPoolReset$' ./test/datastream)
