#!/usr/bin/env bash
# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

# Shared helpers for MongoDB and Iceberg connector E2E jobs. This file is
# sourced by their profile scripts; it deliberately does not change shell
# options or execute a command on its own.

connector_ci_enabled() {
  [[ -n "${MO_CONNECTOR_CI_IMAGE:-}" ]]
}

connector_ci_verify_image() {
  local image="${MO_CONNECTOR_CI_IMAGE:-}"
  local expected="${MO_CONNECTOR_CI_REVISION:-}"
  [[ -n "$image" ]] || die "MO_CONNECTOR_CI_IMAGE is required for container mode"
  [[ -n "$expected" ]] || die "MO_CONNECTOR_CI_REVISION is required for container mode"

  local label embedded
  label="$(docker image inspect --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}' "$image")" || \
    die "connector CI image is unavailable: $image"
  embedded="$(docker run --rm --entrypoint /bin/sh "$image" -c \
    'test -x /connector-bin/mongodb-e2e && test -x /connector-bin/iceberg-e2e && cat /connector-source-revision')" || \
    die "connector CI image smoke check failed: $image"
  if [[ "$label" != "$expected" || "$embedded" != "$expected" ]]; then
    die "connector CI image revision mismatch: expected $expected, label=$label, embedded=$embedded"
  fi
}

connector_ci_wait_for_mo() {
  local container="$1"
  local expected_port="${2:-6001}"
  local deadline=$((SECONDS + 120))
  while (( SECONDS < deadline )); do
    if [[ "$(docker inspect --format '{{.State.Running}}' "$container" 2>/dev/null || true)" != "true" ]]; then
      docker logs --tail 2000 "$container" >&2 || true
      die "MatrixOne container exited before it became ready: $container"
    fi
    if docker logs --tail 2000 "$container" 2>&1 | grep -Eq "Server Listening on : .*:${expected_port}([^0-9]|$)"; then
      return
    fi
    sleep 1
  done
  docker logs --tail 2000 "$container" >&2 || true
  die "MatrixOne container did not publish port $expected_port: $container"
}
