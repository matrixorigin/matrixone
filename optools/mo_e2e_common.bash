#!/usr/bin/env bash
# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

# Shared host-wide port-block ownership for local integration runners.
# Callers own PORT_LEASE_PID and must keep it alive until their services stop.

mo_port_block_candidates() {
  local width="${1:-80}"
  python3 - "$width" <<'PY'
import random
import sys

width = int(sys.argv[1])
candidates = list(range(36000, 57000 - width, width))
random.shuffle(candidates)
for base in candidates:
    print(base)
PY
}

mo_port_block_is_available() {
  local base="$1" width="${2:-80}"
  python3 - "$base" "$width" <<'PY'
import socket
import sys

base = int(sys.argv[1])
width = int(sys.argv[2])
sockets = []
try:
    for port in range(base, base + width):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("127.0.0.1", port))
        sockets.append(sock)
except OSError:
    raise SystemExit(1)
finally:
    for sock in sockets:
        sock.close()
PY
}

mo_port_block_acquire_lease() {
  local base="$1" lease_dir="$2" tmp_root="${3:-${TMPDIR:-/tmp}}"
  local status_file status=""
  mkdir -p "$lease_dir" || return 2
  chmod 700 "$lease_dir" || return 2
  local lease_file="$lease_dir/$base.lock"
  status_file="$(mktemp "${tmp_root%/}/mo-port-block-lease.XXXXXX")" || return 2

  python3 - "$lease_file" "$status_file" "$$" <<'PY' &
import fcntl
import os
import signal
import sys
import time

lease_file, status_file, parent_pid = sys.argv[1:]
parent_pid = int(parent_pid)
with open(lease_file, "a+") as lease:
    try:
        fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        with open(status_file, "w") as status:
            status.write("busy\n")
        raise SystemExit(1)
    with open(status_file, "w") as status:
        status.write("locked\n")

    def release(_signal, _frame):
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, release)
    signal.signal(signal.SIGINT, release)
    while os.getppid() == parent_pid:
        time.sleep(0.05)
PY
  PORT_LEASE_PID=$!

  for _ in {1..100}; do
    if [[ -s "$status_file" ]]; then
      status="$(cat "$status_file")"
      break
    fi
    if ! kill -0 "$PORT_LEASE_PID" >/dev/null 2>&1; then
      break
    fi
    sleep 0.05
  done
  rm -f -- "$status_file"

  if [[ "$status" == "locked" ]]; then
    return 0
  fi
  wait "$PORT_LEASE_PID" >/dev/null 2>&1 || true
  PORT_LEASE_PID=""
  return 1
}

mo_port_block_release_lease() {
  if [[ -n "${PORT_LEASE_PID:-}" ]]; then
    kill "$PORT_LEASE_PID" >/dev/null 2>&1 || true
    wait "$PORT_LEASE_PID" >/dev/null 2>&1 || true
  fi
  PORT_LEASE_PID=""
}

mo_e2e_generate_launch_config() {
  local root_dir="$1" tmp_dir="$2" log_port_base="$3" frontend_port="$4" status_port="$5"
  local source_dir="$root_dir/etc/launch" generated_dir="$tmp_dir/mo-config"
  local log_service_port=$((log_port_base + 1)) log_gossip_port=$((log_port_base + 2))
  local tn_port_base=$((log_port_base + 24)) cn_port_base=$((log_port_base + 48))
  mkdir -p "$generated_dir"
  local name
  for name in log tn; do
    sed -e "s#\./mo-data#$tmp_dir/mo-data#g" -e "s#\"mo-data/#\"$tmp_dir/mo-data/#g" \
      -e "s#^status-port = [0-9][0-9]*#status-port = $status_port#" \
      -e "s#^port-base = [0-9][0-9]*#port-base = $tn_port_base#" \
      "$source_dir/$name.toml" >"$generated_dir/$name.toml"
  done
  sed -e "s#\./mo-data#$tmp_dir/mo-data#g" -e "s#\"mo-data/#\"$tmp_dir/mo-data/#g" \
    -e "s#^status-port = [0-9][0-9]*#status-port = $status_port#" \
    -e "s#^port-base = [0-9][0-9]*#port-base = $cn_port_base#" \
    "$source_dir/cn.toml" | awk -v port="$frontend_port" -v socket="$tmp_dir/mysql.sock" '
      /^\[cn\.frontend\.iceberg\]$/ && !inserted {
        print "[cn.frontend]"
        print "port = " port
        print "unix-socket = \"" socket "\""
        print ""
        inserted = 1
      }
      { print }
      END { if (!inserted) exit 42 }
    ' >"$generated_dir/cn.toml"
  {
    printf '\n[logservice]\n'
    printf 'raft-port = %s\n' "$log_port_base"
    printf 'logservice-port = %s\n' "$log_service_port"
    printf 'gossip-port = %s\n' "$log_gossip_port"
    printf 'gossip-seed-addresses = ["127.0.0.1:%s"]\n' "$log_gossip_port"
  } >>"$generated_dir/log.toml"
  for name in log tn cn; do
    {
      printf '\n[hakeeper-client]\n'
      printf 'service-addresses = ["127.0.0.1:%s"]\n' "$log_service_port"
    } >>"$generated_dir/$name.toml"
  done
  sed -e "s#\./etc/launch/log.toml#$generated_dir/log.toml#" \
    -e "s#\./etc/launch/tn.toml#$generated_dir/tn.toml#" \
    -e "s#\./etc/launch/cn.toml#$generated_dir/cn.toml#" \
    "$source_dir/launch.toml" >"$generated_dir/launch.toml"
}

mo_e2e_wait_frontend_port() {
  local pid="$1" log_file="$2" expected_port="${3:-}" deadline=$((SECONDS + 120)) port=""
  while (( SECONDS < deadline )); do
    if [[ -n "$pid" ]] && ! kill -0 "$pid" >/dev/null 2>&1; then
      printf 'MatrixOne exited before publishing its frontend listener\n' >&2
      return 1
    fi
    port="$(sed -nE 's/.*Server Listening on : [^ ]*:([0-9]+).*/\1/p' "$log_file" | tail -1)"
    if [[ "$port" =~ ^[1-9][0-9]*$ ]]; then
      if [[ -n "$expected_port" && "$port" != "$expected_port" ]]; then
        printf 'MatrixOne published frontend port %s, expected %s\n' "$port" "$expected_port" >&2
        return 1
      fi
      printf '%s\n' "$port"
      return 0
    fi
    sleep 0.25
  done
  printf 'MatrixOne did not publish its frontend listener before timeout\n' >&2
  return 1
}

mo_e2e_stop_service() {
  local pid="$1"
  [[ -n "$pid" ]] || return 0
  if kill -0 "$pid" >/dev/null 2>&1; then
    kill "$pid" >/dev/null 2>&1 || true
    for _ in {1..40}; do
      kill -0 "$pid" >/dev/null 2>&1 || break
      sleep 0.25
    done
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill -KILL "$pid" >/dev/null 2>&1 || true
    fi
    wait "$pid" >/dev/null 2>&1 || true
  fi
}
