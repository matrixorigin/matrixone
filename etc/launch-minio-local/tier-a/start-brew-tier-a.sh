#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
DATA_DIR="${MO_ICEBERG_TIER_A_DATA_DIR:-${ROOT_DIR}/mo-data}"
MINIO_DATA_DIR="${DATA_DIR}/minio-data"
LOG_DIR="${DATA_DIR}/logs"
PID_DIR="${DATA_DIR}/pids"

MINIO_API="${MO_ICEBERG_S3_ENDPOINT:-http://127.0.0.1:9000}"
MINIO_CONSOLE_ADDR="${MO_ICEBERG_MINIO_CONSOLE_ADDR:-:9001}"
MINIO_ACCESS_KEY="${MO_ICEBERG_S3_AK:-minio}"
MINIO_SECRET_KEY="${MO_ICEBERG_S3_SK:-minio123}"
NESSIE_URI="${MO_ICEBERG_CATALOG_URI:-http://127.0.0.1:19120/iceberg}"
NESSIE_HTTP_PORT="${MO_ICEBERG_NESSIE_HTTP_PORT:-19120}"
NESSIE_MANAGEMENT_PORT="${MO_ICEBERG_NESSIE_MANAGEMENT_PORT:-19121}"
WAREHOUSE="${MO_ICEBERG_WAREHOUSE:-s3://mo-iceberg/warehouse}"
WAREHOUSE_NAME="${MO_ICEBERG_NESSIE_WAREHOUSE_NAME:-warehouse}"
S3_REGION="${MO_ICEBERG_S3_REGION:-us-east-1}"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "required command not found: $1" >&2
    exit 1
  fi
}

wait_url() {
  local url="$1"
  local name="$2"
  for _ in $(seq 1 60); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return
    fi
    sleep 1
  done
  echo "timed out waiting for ${name}: ${url}" >&2
  exit 1
}

start_background() {
  local name="$1"
  local pid_file="${PID_DIR}/${name}.pid"
  shift
  if [[ -f "$pid_file" ]] && kill -0 "$(cat "$pid_file")" >/dev/null 2>&1; then
    echo "${name} already running with pid $(cat "$pid_file")"
    return
  fi
  nohup "$@" >"${LOG_DIR}/${name}.log" 2>&1 &
  echo "$!" >"$pid_file"
  echo "started ${name} with pid $(cat "$pid_file")"
}

main() {
  require_cmd minio
  require_cmd mc
  require_cmd nessie
  require_cmd curl

  mkdir -p "$MINIO_DATA_DIR" "$LOG_DIR" "$PID_DIR"

  start_background minio env \
    MINIO_ROOT_USER="$MINIO_ACCESS_KEY" \
    MINIO_ROOT_PASSWORD="$MINIO_SECRET_KEY" \
    minio server --address :9000 --console-address "$MINIO_CONSOLE_ADDR" "$MINIO_DATA_DIR"

  wait_url "${MINIO_API%/}/minio/health/live" "MinIO"

  mc alias set local "$MINIO_API" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" >/dev/null
  mc mb --ignore-existing local/mo-iceberg >/dev/null
  mc mb --ignore-existing local/mo-test >/dev/null
  mc anonymous set public local/mo-iceberg >/dev/null
  mc anonymous set public local/mo-test >/dev/null

  start_background nessie env \
    QUARKUS_HTTP_HOST=127.0.0.1 \
    QUARKUS_HTTP_PORT="$NESSIE_HTTP_PORT" \
    QUARKUS_MANAGEMENT_PORT="$NESSIE_MANAGEMENT_PORT" \
    NESSIE_CATALOG_DEFAULT_WAREHOUSE="$WAREHOUSE_NAME" \
    NESSIE_CATALOG_WAREHOUSES_WAREHOUSE_LOCATION="$WAREHOUSE" \
    NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_ENDPOINT="$MINIO_API" \
    NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_EXTERNAL_ENDPOINT="$MINIO_API" \
    NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_REGION="$S3_REGION" \
    NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_AUTH_TYPE=STATIC \
    NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_ACCESS_KEY=urn:nessie-secret:quarkus:s3default \
    NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_PATH_STYLE_ACCESS=true \
    S3DEFAULT_NAME="$MINIO_ACCESS_KEY" \
    S3DEFAULT_SECRET="$MINIO_SECRET_KEY" \
    AWS_ACCESS_KEY_ID="$MINIO_ACCESS_KEY" \
    AWS_SECRET_ACCESS_KEY="$MINIO_SECRET_KEY" \
    AWS_REGION="$S3_REGION" \
    nessie

  wait_url "${NESSIE_URI%/}/v1/config" "Nessie Iceberg REST catalog"

  cat <<EOF
Iceberg Tier A brew services are ready.
  MinIO:  ${MINIO_API}
  Nessie: ${NESSIE_URI}
  Warehouse: ${WAREHOUSE}

Run:
  MO_ICEBERG_IT=1 \\
  MO_ICEBERG_CATALOG_URI=${NESSIE_URI} \\
  MO_ICEBERG_S3_ENDPOINT=${MINIO_API} \\
  go test ./pkg/sql/iceberg -run TestIcebergTierALocalNessieMinIOPreflight -count=1
EOF
}

main "$@"
