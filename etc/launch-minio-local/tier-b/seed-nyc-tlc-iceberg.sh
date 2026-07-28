#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
SPARK_SQL_BIN="${SPARK_SQL_BIN:-spark-sql}"
CATALOG="${MO_ICEBERG_SPARK_CATALOG:-tiera}"
MO_DB="${MO_ICEBERG_TIER_B_MO_DB:-iceberg_tier_b}"
MO_CATALOG="${MO_ICEBERG_TIER_B_MO_CATALOG:-tlc}"
CATALOG_URI="${MO_ICEBERG_CATALOG_URI:-http://127.0.0.1:19120/iceberg}"
WAREHOUSE="${MO_ICEBERG_WAREHOUSE:-s3://mo-iceberg/warehouse}"
S3_ENDPOINT="${MO_ICEBERG_S3_ENDPOINT:-http://127.0.0.1:9000}"
S3_REGION="${MO_ICEBERG_S3_REGION:-us-east-1}"
S3_AK="${MO_ICEBERG_S3_AK:-minio}"
S3_SK="${MO_ICEBERG_S3_SK:-minio123}"
ICEBERG_VERSION="${MO_ICEBERG_SPARK_ICEBERG_VERSION:-1.6.1}"
HADOOP_AWS_VERSION="${MO_ICEBERG_SPARK_HADOOP_AWS_VERSION:-3.3.4}"
AWS_SDK_VERSION="${MO_ICEBERG_SPARK_AWS_SDK_VERSION:-2.29.52}"
DATASET_URL="${MO_ICEBERG_NYC_TLC_URL:-https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet}"
CACHE_DIR="${MO_ICEBERG_NYC_TLC_CACHE_DIR:-${ROOT_DIR}/etc/launch-minio-local/mo-data/public-source/nyc_tlc}"
LOCAL_PARQUET="${MO_ICEBERG_NYC_TLC_PARQUET:-${CACHE_DIR}/yellow_tripdata_2024-01.parquet}"
START_TS="${MO_ICEBERG_NYC_TLC_START_TS:-2024-01-01 00:00:00}"
END_TS="${MO_ICEBERG_NYC_TLC_END_TS:-2024-02-01 00:00:00}"
ROW_LIMIT="${MO_ICEBERG_NYC_TLC_ROW_LIMIT:-200000}"
SEED_SQL="${MO_ICEBERG_TIER_B_SEED_SQL:-${SCRIPT_DIR}/seed_nyc_tlc.spark.sql}"
MO_SETUP_SQL="${MO_ICEBERG_TIER_B_MO_SETUP_SQL:-${SCRIPT_DIR}/mo_setup_nyc_tlc.sql}"
OUT_ENV="${MO_ICEBERG_TIER_B_ENV:-${SCRIPT_DIR}/tier_b_nyc_tlc.generated.env}"
SPARK_WAREHOUSE_DIR="${MO_ICEBERG_SPARK_WAREHOUSE_DIR:-${ROOT_DIR}/etc/launch-minio-local/mo-data/spark-warehouse}"

if [[ -z "${JAVA_HOME:-}" && "$(uname -s)" == "Darwin" && "$(uname -m)" == "arm64" ]]; then
  homebrew_openjdk21="/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home"
  if [[ -d "${homebrew_openjdk21}" ]]; then
    export JAVA_HOME="${homebrew_openjdk21}"
  fi
fi

spark_packages=(
  "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:${ICEBERG_VERSION}"
  "org.apache.iceberg:iceberg-aws-bundle:${ICEBERG_VERSION}"
  "org.apache.hadoop:hadoop-aws:${HADOOP_AWS_VERSION}"
  "software.amazon.awssdk:bundle:${AWS_SDK_VERSION}"
  "software.amazon.awssdk:url-connection-client:${AWS_SDK_VERSION}"
)
SPARK_PACKAGES="${MO_ICEBERG_SPARK_PACKAGES:-$(IFS=,; echo "${spark_packages[*]}")}"

spark_args=(
  --master "local[4]"
  --packages "${SPARK_PACKAGES}"
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
  --conf "spark.sql.catalogImplementation=in-memory"
  --conf "spark.sql.warehouse.dir=${SPARK_WAREHOUSE_DIR}"
  --conf "spark.default.parallelism=8"
  --conf "spark.sql.shuffle.partitions=8"
  --conf "spark.sql.adaptive.enabled=false"
  --conf "spark.sql.iceberg.vectorization.enabled=false"
  --conf "spark.sql.parquet.enableVectorizedReader=false"
  --conf "spark.sql.catalog.${CATALOG}=org.apache.iceberg.spark.SparkCatalog"
  --conf "spark.sql.catalog.${CATALOG}.type=rest"
  --conf "spark.sql.catalog.${CATALOG}.uri=${CATALOG_URI}"
  --conf "spark.sql.catalog.${CATALOG}.warehouse=${WAREHOUSE}"
  --conf "spark.sql.catalog.${CATALOG}.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
  --conf "spark.sql.catalog.${CATALOG}.s3.endpoint=${S3_ENDPOINT}"
  --conf "spark.sql.catalog.${CATALOG}.s3.path-style-access=true"
  --conf "spark.sql.catalog.${CATALOG}.s3.access-key-id=${S3_AK}"
  --conf "spark.sql.catalog.${CATALOG}.s3.secret-access-key=${S3_SK}"
  --conf "spark.sql.catalog.${CATALOG}.client.region=${S3_REGION}"
  --conf "spark.hadoop.fs.s3a.endpoint=${S3_ENDPOINT}"
  --conf "spark.hadoop.fs.s3a.access.key=${S3_AK}"
  --conf "spark.hadoop.fs.s3a.secret.key=${S3_SK}"
  --conf "spark.hadoop.fs.s3a.path.style.access=true"
  --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false"
)

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "required command not found: $1" >&2
    exit 1
  fi
}

shell_quote() {
  printf "'%s'" "$(printf "%s" "$1" | sed "s/'/'\\\\''/g")"
}

limit_clause() {
  if [[ "$ROW_LIMIT" == "0" || "$ROW_LIMIT" == "" ]]; then
    printf ''
    return
  fi
  case "$ROW_LIMIT" in
    ''|*[!0-9]*)
      echo "MO_ICEBERG_NYC_TLC_ROW_LIMIT must be a non-negative integer: ${ROW_LIMIT}" >&2
      exit 1
      ;;
  esac
  printf 'LIMIT %s' "$ROW_LIMIT"
}

render_template_to_file() {
  local template="$1"
  local out="$2"
  TPL_CATALOG="$CATALOG" \
  TPL_MO_DB="$MO_DB" \
  TPL_MO_CATALOG="$MO_CATALOG" \
  TPL_CATALOG_URI="$CATALOG_URI" \
  TPL_WAREHOUSE="$WAREHOUSE" \
  TPL_SOURCE_PARQUET="$LOCAL_PARQUET" \
  TPL_START_TS="$START_TS" \
  TPL_END_TS="$END_TS" \
  TPL_LIMIT_CLAUSE="$(limit_clause)" \
    python3 - "$template" "$out" <<'PY'
import os
import pathlib
import sys

template = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8")
for key, value in os.environ.items():
    if key.startswith("TPL_"):
        template = template.replace(f"__{key[4:]}__", value)
pathlib.Path(sys.argv[2]).write_text(template, encoding="utf-8")
PY
}

run_spark_sql_file() {
  local rendered
  rendered="$(mktemp)"
  render_template_to_file "$1" "$rendered"
  "${SPARK_SQL_BIN}" "${spark_args[@]}" -f "$rendered"
  rm -f "$rendered"
}

run_sql_template() {
  local template="$1"
  local sql="$2"
  local quoted rendered
  quoted="$(shell_quote "$sql")"
  rendered="${template//\{sql\}/$quoted}"
  eval "$rendered"
}

run_mo_setup_if_requested() {
  if [[ -z "${MO_ICEBERG_MO_SQL_CMD:-}" ]]; then
    echo "MO_ICEBERG_MO_SQL_CMD is not set; skipping MatrixOne mapping setup."
    echo "To map the table later, render and execute ${MO_SETUP_SQL} with the same environment."
    return
  fi
  local rendered sql
  rendered="$(mktemp)"
  render_template_to_file "$MO_SETUP_SQL" "$rendered"
  sql="$(cat "$rendered")"
  rm -f "$rendered"
  run_sql_template "$MO_ICEBERG_MO_SQL_CMD" "$sql"
}

emit_env() {
  local key="$1"
  local value="$2"
  printf "export %s=%s\n" "$key" "$(shell_quote "$value")" >>"$OUT_ENV"
}

spark_sql_command_template() {
  local parts=()
  if [[ -n "${JAVA_HOME:-}" ]]; then
    parts+=("JAVA_HOME=$(shell_quote "$JAVA_HOME")")
  fi
  if [[ -n "${PYTHONPATH:-}" ]]; then
    parts+=("PYTHONPATH=$(shell_quote "$PYTHONPATH")")
  fi
  if [[ -n "${SPARK_HOME:-}" ]]; then
    parts+=("SPARK_HOME=$(shell_quote "$SPARK_HOME")")
  fi
  if [[ -n "${SPARK_LOCAL_IP:-}" ]]; then
    parts+=("SPARK_LOCAL_IP=$(shell_quote "$SPARK_LOCAL_IP")")
  fi
  if [[ -n "${SPARK_LOCAL_HOSTNAME:-}" ]]; then
    parts+=("SPARK_LOCAL_HOSTNAME=$(shell_quote "$SPARK_LOCAL_HOSTNAME")")
  fi
  parts+=("$(shell_quote "$SPARK_SQL_BIN")")
  for arg in "${spark_args[@]}"; do
    parts+=("$(shell_quote "$arg")")
  done
  parts+=("-e")
  parts+=("{sql}")
  printf "%s" "${parts[*]}"
}

download_dataset() {
  mkdir -p "$CACHE_DIR"
  if [[ -s "$LOCAL_PARQUET" ]]; then
    echo "Using cached NYC TLC file: ${LOCAL_PARQUET}"
    return
  fi
  echo "Downloading NYC TLC file:"
  echo "  ${DATASET_URL}"
  echo "  -> ${LOCAL_PARQUET}"
  curl -fL --retry 3 --retry-delay 3 -o "$LOCAL_PARQUET" "$DATASET_URL"
}

require_cmd "$SPARK_SQL_BIN"
require_cmd curl
require_cmd python3

download_dataset

echo "Seeding NYC TLC Iceberg table into ${CATALOG}.public_nyc_tlc.yellow_tripdata..."
echo "  source: ${LOCAL_PARQUET}"
echo "  row limit: ${ROW_LIMIT} (set MO_ICEBERG_NYC_TLC_ROW_LIMIT=0 for the full month)"
run_spark_sql_file "$SEED_SQL"

mo_table="${MO_DB}.yellow_tripdata"
official_table="${CATALOG}.public_nyc_tlc.yellow_tripdata"
date_filter="tpep_pickup_datetime >= timestamp '${START_TS}' and tpep_pickup_datetime < timestamp '${END_TS}'"
projection_sql="select vendor_id, pu_location_id, do_location_id, passenger_count, cast(total_amount as decimal(12,2)) as total_amount from ${mo_table} where passenger_count between 1 and 4 order by tpep_pickup_datetime, vendor_id, pu_location_id limit 50"
official_projection_sql="select vendor_id, pu_location_id, do_location_id, passenger_count, cast(total_amount as decimal(12,2)) as total_amount from ${official_table} where passenger_count between 1 and 4 order by tpep_pickup_datetime, vendor_id, pu_location_id limit 50"
agg_sql="select pu_location_id, count(*) as trip_count, cast(sum(total_amount) as decimal(18,2)) as total_sum from ${mo_table} where passenger_count >= 1 group by pu_location_id order by trip_count desc, pu_location_id limit 20"
official_agg_sql="select pu_location_id, count(*) as trip_count, cast(sum(total_amount) as decimal(18,2)) as total_sum from ${official_table} where passenger_count >= 1 group by pu_location_id order by trip_count desc, pu_location_id limit 20"
time_range_sql="select count(*) as trip_count, cast(sum(trip_distance) as decimal(18,2)) as distance_sum, cast(sum(total_amount) as decimal(18,2)) as total_sum from ${mo_table} where tpep_pickup_datetime >= timestamp '2024-01-15 00:00:00' and tpep_pickup_datetime < timestamp '2024-01-16 00:00:00'"
official_time_range_sql="select count(*) as trip_count, cast(sum(trip_distance) as decimal(18,2)) as distance_sum, cast(sum(total_amount) as decimal(18,2)) as total_sum from ${official_table} where tpep_pickup_datetime >= timestamp '2024-01-15 00:00:00' and tpep_pickup_datetime < timestamp '2024-01-16 00:00:00'"

rm -f "$OUT_ENV"
emit_env MO_ICEBERG_PUBLIC_DATASET 1
emit_env MO_ICEBERG_ALLOW_PLAIN_HTTP 1
emit_env MO_ICEBERG_PUBLIC_DATASET_NAME "nyc_tlc_yellow_tripdata_2024_01"
emit_env MO_ICEBERG_PUBLIC_DATASET_SOURCE_URL "$DATASET_URL"
emit_env MO_ICEBERG_PUBLIC_DATASET_LOCAL_PARQUET "$LOCAL_PARQUET"
emit_env MO_ICEBERG_PUBLIC_DATASET_CATALOG_URI "$CATALOG_URI"
emit_env MO_ICEBERG_PUBLIC_DATASET_WAREHOUSE "$WAREHOUSE"
emit_env MO_ICEBERG_PUBLIC_DATASET_SCENARIOS "${ROOT_DIR}/test/iceberg/tier_b_public_dataset_scenarios.example.json"
emit_env MO_ICEBERG_PUBLIC_DATASET_OFFICIAL_SQL_CMD "$(spark_sql_command_template)"
if [[ -n "${MO_ICEBERG_MO_SQL_CMD:-}" ]]; then
  emit_env MO_ICEBERG_PUBLIC_DATASET_MO_SQL_CMD "${MO_ICEBERG_MO_QUERY_SQL_CMD:-$MO_ICEBERG_MO_SQL_CMD}"
fi
emit_env MO_ICEBERG_PUBLIC_DATASET_NAMESPACE "public_nyc_tlc"
emit_env MO_ICEBERG_PUBLIC_DATASET_TABLE "yellow_tripdata"
emit_env MO_ICEBERG_PUBLIC_DATASET_MO_TABLE "$mo_table"
emit_env MO_ICEBERG_PUBLIC_DATASET_OFFICIAL_TABLE "$official_table"
emit_env MO_ICEBERG_PUBLIC_DATASET_FILTER "$date_filter"
emit_env MO_ICEBERG_PUBLIC_DATASET_MO_PROJECTION_SQL "$projection_sql"
emit_env MO_ICEBERG_PUBLIC_DATASET_OFFICIAL_PROJECTION_SQL "$official_projection_sql"
emit_env MO_ICEBERG_PUBLIC_DATASET_MO_AGG_SQL "$agg_sql"
emit_env MO_ICEBERG_PUBLIC_DATASET_OFFICIAL_AGG_SQL "$official_agg_sql"
emit_env MO_ICEBERG_PUBLIC_DATASET_MO_TIME_RANGE_SQL "$time_range_sql"
emit_env MO_ICEBERG_PUBLIC_DATASET_OFFICIAL_TIME_RANGE_SQL "$official_time_range_sql"
emit_env MO_ICEBERG_NYC_TLC_ROW_LIMIT "$ROW_LIMIT"
emit_env MO_ICEBERG_NYC_TLC_PERF_Q1_MO_SQL "select count(*) from ${mo_table}"
emit_env MO_ICEBERG_NYC_TLC_PERF_Q2_MO_SQL "$time_range_sql"
emit_env MO_ICEBERG_NYC_TLC_PERF_Q3_MO_SQL "$agg_sql"
emit_env MO_ICEBERG_NYC_TLC_PERF_Q4_MO_SQL "select payment_type, count(*) as c, cast(avg(total_amount) as decimal(18,2)) as avg_total from ${mo_table} group by payment_type order by payment_type"

run_mo_setup_if_requested

echo "NYC TLC Tier B seed complete."
echo "Environment file: ${OUT_ENV}"
echo "Load it with:"
echo "  set -a && source ${OUT_ENV} && set +a"
echo "Then run:"
echo "  MO_ICEBERG_CI_PROFILE=tier-b make test-iceberg-nightly"
