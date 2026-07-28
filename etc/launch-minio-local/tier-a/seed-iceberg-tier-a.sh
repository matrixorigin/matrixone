#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPARK_SQL_BIN="${SPARK_SQL_BIN:-spark-sql}"
CATALOG="${MO_ICEBERG_SPARK_CATALOG:-tiera}"
TRINO_CATALOG="${MO_ICEBERG_TRINO_CATALOG:-iceberg}"
MO_DB="${MO_ICEBERG_TIER_A_MO_DB:-iceberg_tier_a}"
MO_CATALOG="${MO_ICEBERG_TIER_A_MO_CATALOG:-tiera}"
CATALOG_URI="${MO_ICEBERG_CATALOG_URI:-http://127.0.0.1:19120/iceberg}"
WAREHOUSE="${MO_ICEBERG_WAREHOUSE:-s3://mo-iceberg/warehouse}"
S3_ENDPOINT="${MO_ICEBERG_S3_ENDPOINT:-http://127.0.0.1:9000}"
S3_REGION="${MO_ICEBERG_S3_REGION:-us-east-1}"
S3_AK="${MO_ICEBERG_S3_AK:-minio}"
S3_SK="${MO_ICEBERG_S3_SK:-minio123}"
ICEBERG_VERSION="${MO_ICEBERG_SPARK_ICEBERG_VERSION:-1.6.1}"
HADOOP_AWS_VERSION="${MO_ICEBERG_SPARK_HADOOP_AWS_VERSION:-3.3.4}"
AWS_SDK_VERSION="${MO_ICEBERG_SPARK_AWS_SDK_VERSION:-2.29.52}"
SEED_SQL="${MO_ICEBERG_TIER_A_SEED_SQL:-${SCRIPT_DIR}/seed.spark.sql}"
MO_SETUP_SQL="${MO_ICEBERG_TIER_A_MO_SETUP_SQL:-${SCRIPT_DIR}/mo_setup.sql}"
OUT_ENV="${MO_ICEBERG_TIER_A_ENV:-${SCRIPT_DIR}/tier_a.generated.env}"

spark_packages=(
  "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:${ICEBERG_VERSION}"
  "org.apache.iceberg:iceberg-aws-bundle:${ICEBERG_VERSION}"
  "org.apache.hadoop:hadoop-aws:${HADOOP_AWS_VERSION}"
  "software.amazon.awssdk:bundle:${AWS_SDK_VERSION}"
  "software.amazon.awssdk:url-connection-client:${AWS_SDK_VERSION}"
)
SPARK_PACKAGES="${MO_ICEBERG_SPARK_PACKAGES:-$(IFS=,; echo "${spark_packages[*]}")}"

spark_args=(
  --packages "${SPARK_PACKAGES}"
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
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

render_template() {
  sed \
    -e "s#__CATALOG__#${CATALOG}#g" \
    -e "s#__MO_DB__#${MO_DB}#g" \
    -e "s#__MO_CATALOG__#${MO_CATALOG}#g" \
    -e "s#__CATALOG_URI__#${CATALOG_URI}#g" \
    -e "s#__WAREHOUSE__#${WAREHOUSE}#g" \
    "$1"
}

render_seed_template() {
  if [[ "${MO_ICEBERG_SEED_REFS:-0}" == "1" ]]; then
    render_template "$1"
    return
  fi
  render_template "$1" | sed \
    -e "/CREATE BRANCH tier_a_branch/d" \
    -e "/CREATE TAG tier_a_tag/d"
}

run_spark_sql_file() {
  local rendered
  rendered="$(mktemp)"
  trap 'rm -f "$rendered"' RETURN
  if [[ "$1" == "$SEED_SQL" ]]; then
    render_seed_template "$1" >"$rendered"
  else
    render_template "$1" >"$rendered"
  fi
  "${SPARK_SQL_BIN}" "${spark_args[@]}" -f "$rendered"
}

run_spark_query() {
  "${SPARK_SQL_BIN}" "${spark_args[@]}" -e "$1"
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
		echo "Run the rendered setup manually if needed:"
		echo "  render_template ${MO_SETUP_SQL}"
		return
	fi
	local sql
	sql="$(render_template "$MO_SETUP_SQL")"
	run_sql_template "$MO_ICEBERG_MO_SQL_CMD" "$sql"
}

last_numeric_row() {
  awk '/^-?[0-9]+$/ { value=$1 } END { if (value != "") print value }'
}

first_timestamp_row() {
  awk '/^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]/ { value=$1 " " $2 } END { sub(/\.[0-9]+$/, "", value); if (value != "") print value }'
}

emit_env() {
  local key="$1"
  local value="$2"
  printf "export %s=%s\n" "$key" "$(shell_quote "$value")" >>"$OUT_ENV"
}

spark_sql_command_template() {
  local parts=()
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

snapshot_id_for() {
  local table="$1"
  local order="${2:-desc}"
  run_spark_query "select snapshot_id from ${table}.snapshots order by committed_at ${order} limit 1" | last_numeric_row
}

committed_at_for_snapshot() {
  local table="$1"
  local snapshot="$2"
  run_spark_query "select committed_at from ${table}.snapshots where snapshot_id = ${snapshot}" | first_timestamp_row
}

timestamp_as_of_for_snapshot() {
  local table="$1"
  local snapshot="$2"
  run_spark_query "select date_format(committed_at + interval 1 seconds, 'yyyy-MM-dd HH:mm:ss') from ${table}.snapshots where snapshot_id = ${snapshot}" | first_timestamp_row
}

timestamp_as_of_for_snapshot_in_riyadh() {
  local table="$1"
  local snapshot="$2"
  run_spark_query "select date_format(from_utc_timestamp(to_utc_timestamp(committed_at + interval 1 seconds, current_timezone()), 'Asia/Riyadh'), 'yyyy-MM-dd HH:mm:ss') from ${table}.snapshots where snapshot_id = ${snapshot}" | first_timestamp_row
}

require_cmd "$SPARK_SQL_BIN"

echo "Seeding Iceberg Tier A tables into ${CATALOG} (${CATALOG_URI}, ${WAREHOUSE})..."
run_spark_sql_file "$SEED_SQL"

orders_table="${CATALOG}.tpch_sf01.orders"
first_orders_snapshot="$(snapshot_id_for "$orders_table" asc)"
current_orders_snapshot="$(snapshot_id_for "$orders_table" desc)"
first_orders_ts="$(committed_at_for_snapshot "$orders_table" "$first_orders_snapshot")"
first_orders_spark_as_of_ts="$(timestamp_as_of_for_snapshot "$orders_table" "$first_orders_snapshot")"
first_orders_mo_as_of_ts="$(timestamp_as_of_for_snapshot_in_riyadh "$orders_table" "$first_orders_snapshot")"

if [[ -z "$first_orders_snapshot" || -z "$current_orders_snapshot" || -z "$first_orders_ts" || -z "$first_orders_spark_as_of_ts" || -z "$first_orders_mo_as_of_ts" ]]; then
  echo "failed to resolve orders snapshot metadata" >&2
  exit 1
fi

rm -f "$OUT_ENV"
emit_env MO_ICEBERG_IT 1
emit_env MO_ICEBERG_ALLOW_PLAIN_HTTP 1
emit_env MO_ICEBERG_CATALOG_URI "$CATALOG_URI"
emit_env MO_ICEBERG_S3_ENDPOINT "$S3_ENDPOINT"
emit_env MO_ICEBERG_SPARK_SQL_CMD "$(spark_sql_command_template)"
if [[ -n "${MO_ICEBERG_MO_SQL_CMD:-}" ]]; then
  emit_env MO_ICEBERG_MO_SQL_CMD "$MO_ICEBERG_MO_SQL_CMD"
fi
emit_env MO_ICEBERG_TIER_A_MO_DB "$MO_DB"
emit_env MO_ICEBERG_TIER_A_MO_CATALOG "$MO_CATALOG"
emit_env MO_ICEBERG_TIER_A_SPARK_CATALOG "$CATALOG"
emit_env MO_ICEBERG_TIER_A_MO_ORDERS "${MO_DB}.tpch_sf01_orders"
emit_env MO_ICEBERG_TIER_A_MO_ORDERS_IMPORT_NATIVE "${MO_DB}.tpch_sf01_orders_imported_native"
emit_env MO_ICEBERG_TIER_A_SPARK_ORDERS "${CATALOG}.tpch_sf01.orders"
emit_env MO_ICEBERG_TIER_A_SPARK_TAXI "${CATALOG}.nyc_taxi.trips_small"
emit_env MO_ICEBERG_TIER_A_SPARK_USERS "${CATALOG}.evolution.users"
emit_env MO_ICEBERG_TIER_A_SPARK_DELETE_ORDERS "${CATALOG}.delete_files.orders_mor"
emit_env MO_ICEBERG_TIER_A_TRINO_DELETE_ORDERS "${TRINO_CATALOG}.delete_files.orders_mor"
emit_env MO_ICEBERG_TIER_A_SPARK_DML_ACCOUNTS "${CATALOG}.dml.accounts"
emit_env MO_ICEBERG_TIER_A_SPARK_DML_ACCOUNTS_BY_REGION "${CATALOG}.dml.accounts_by_region"
emit_env MO_ICEBERG_TIER_A_SPARK_REF_ORDERS "${CATALOG}.refs.orders_branch"
emit_env MO_ICEBERG_TIER_A_MO_DML_ACCOUNTS "${MO_DB}.dml_accounts"
emit_env MO_ICEBERG_TIER_A_MO_DML_ACCOUNTS_UPDATES "${MO_DB}.dml_accounts_updates"
emit_env MO_ICEBERG_TIER_A_TRINO_DML_ACCOUNTS "${TRINO_CATALOG}.dml.accounts"
emit_env MO_ICEBERG_TIER_A_MO_WRITER_GOLD_KPI "${MO_DB}.writer_gold_kpi"
emit_env MO_ICEBERG_TIER_A_SPARK_WRITER_GOLD_KPI "${CATALOG}.writer.gold_kpi"
emit_env MO_ICEBERG_TIER_A_TRINO_WRITER_GOLD_KPI "${TRINO_CATALOG}.writer.gold_kpi"
emit_env MO_ICEBERG_TIER_A_MO_DML_ACCOUNTS_BY_REGION "${MO_DB}.dml_accounts_by_region"
emit_env MO_ICEBERG_TIER_A_MO_DML_ACCOUNTS_BY_REGION_STAGE "${MO_DB}.dml_accounts_by_region_stage"
emit_env MO_ICEBERG_TIER_A_TRINO_DML_ACCOUNTS_BY_REGION "${TRINO_CATALOG}.dml.accounts_by_region"
emit_env MO_ICEBERG_TIER_A_MO_MAINTENANCE_ORDERS "${MO_DB}.maintenance_orders_small"
emit_env MO_ICEBERG_TIER_A_SPARK_MAINTENANCE_ORDERS "${CATALOG}.maintenance.orders_small"
emit_env MO_ICEBERG_TIER_A_TRINO_MAINTENANCE_ORDERS "${TRINO_CATALOG}.maintenance.orders_small"
emit_env MO_ICEBERG_TIER_A_MAINTENANCE_TARGET "${MO_CATALOG}.maintenance.orders_small"
emit_env MO_ICEBERG_TIER_A_READ_PROJECTION_MO_SQL "select order_id, cust_id, cast(total_price as decimal(12,2)) from ${MO_DB}.tpch_sf01_orders where bucket in (1,2) order by order_id"
emit_env MO_ICEBERG_TIER_A_READ_PROJECTION_SPARK_SQL "select order_id, cust_id, cast(total_price as decimal(12,2)) from ${CATALOG}.tpch_sf01.orders where bucket in (1,2) order by order_id"
emit_env MO_ICEBERG_TIER_A_PARTITION_PRUNING_MO_SQL "select bucket, count(*) as c, sum(cast(order_id as bigint)) as id_sum from ${MO_DB}.tpch_sf01_orders where bucket = 2 group by bucket order by bucket"
emit_env MO_ICEBERG_TIER_A_PARTITION_PRUNING_SPARK_SQL "select bucket, count(*) as c, sum(cast(order_id as bigint)) as id_sum from ${CATALOG}.tpch_sf01.orders where bucket = 2 group by bucket order by bucket"
emit_env MO_ICEBERG_TIER_A_TIME_TRAVEL_SNAPSHOT_MO_SQL "select count(*) from ${MO_DB}.tpch_sf01_orders for iceberg snapshot ${first_orders_snapshot}"
emit_env MO_ICEBERG_TIER_A_TIME_TRAVEL_SNAPSHOT_SPARK_SQL "select count(*) from ${CATALOG}.tpch_sf01.orders version as of ${first_orders_snapshot}"
emit_env MO_ICEBERG_TIER_A_TIME_TRAVEL_TIMESTAMP_MO_ACTION_SQL "set time_zone = '+03:00'"
emit_env MO_ICEBERG_TIER_A_TIME_TRAVEL_TIMESTAMP_MO_SQL "select count(*) from ${MO_DB}.tpch_sf01_orders for iceberg timestamp as of timestamp '${first_orders_mo_as_of_ts}'"
emit_env MO_ICEBERG_TIER_A_TIME_TRAVEL_TIMESTAMP_SPARK_SQL "select count(*) from ${CATALOG}.tpch_sf01.orders timestamp as of '${first_orders_spark_as_of_ts}'"
emit_env MO_ICEBERG_TIER_A_SCHEMA_EVOLUTION_MO_SQL "select id, full_name, region, age, cast(score as decimal(10,2)), cast(credit as decimal(12,2)) from ${MO_DB}.evolution_users order by id"
emit_env MO_ICEBERG_TIER_A_SCHEMA_EVOLUTION_SPARK_SQL "select id, full_name, region, age, cast(score as decimal(10,2)), cast(credit as decimal(12,2)) from ${CATALOG}.evolution.users order by id"
emit_env MO_ICEBERG_TIER_A_DELETE_APPLY_MO_SQL "select order_id, hidden_key, bucket, amount from ${MO_DB}.delete_files_orders_mor order by order_id"
emit_env MO_ICEBERG_TIER_A_DELETE_APPLY_SPARK_SQL "select order_id, hidden_key, bucket, amount from ${CATALOG}.delete_files.orders_mor order by order_id"
emit_env MO_ICEBERG_TIER_A_APPEND_ONLY_DELETE_FAIL_MO_SQL "select count(*) from ${MO_DB}.delete_files_orders_append_only"
emit_env MO_ICEBERG_TIER_A_APPEND_MO_ACTION_SQL "insert into ${MO_DB}.writer_gold_kpi values (cast(9001 as bigint), 'ksa', cast('2026-06-29' as date), cast(900 as bigint), 'mo_append'), (cast(9002 as bigint), 'uae', cast('2026-06-29' as date), cast(901 as bigint), 'mo_append')"
emit_env MO_ICEBERG_TIER_A_APPEND_MO_SQL "select source, region, count(*) as c, sum(cast(kpi_value as bigint)) as value_sum from ${MO_DB}.writer_gold_kpi where source in ('spark_base', 'mo_append') group by source, region order by source, region"
emit_env MO_ICEBERG_TIER_A_APPEND_SPARK_SQL "select source, region, count(*) as c, sum(cast(kpi_value as bigint)) as value_sum from ${CATALOG}.writer.gold_kpi where source in ('spark_base', 'mo_append') group by source, region order by source, region"
emit_env MO_ICEBERG_TIER_A_IMPORT_NATIVE_DROP_MO_SQL "drop table if exists ${MO_DB}.tpch_sf01_orders_imported_native"
emit_env MO_ICEBERG_TIER_A_IMPORT_NATIVE_CREATE_MO_SQL "create table ${MO_DB}.tpch_sf01_orders_imported_native as select order_id, cust_id, order_status, order_date, total_price, bucket from ${MO_DB}.tpch_sf01_orders for iceberg snapshot ${first_orders_snapshot}"
emit_env MO_ICEBERG_TIER_A_IMPORT_NATIVE_MO_SQL "select count(*) as c, sum(cast(order_id as bigint)) as order_sum, sum(cast(cust_id as bigint)) as cust_sum, cast(sum(cast(total_price as decimal(12,2))) as decimal(12,2)) as total_sum, min(order_date), max(order_date) from ${MO_DB}.tpch_sf01_orders_imported_native"
emit_env MO_ICEBERG_TIER_A_IMPORT_NATIVE_SPARK_SQL "select count(*) as c, sum(cast(order_id as bigint)) as order_sum, sum(cast(cust_id as bigint)) as cust_sum, cast(sum(cast(total_price as decimal(12,2))) as decimal(12,2)) as total_sum, min(order_date), max(order_date) from ${CATALOG}.tpch_sf01.orders version as of ${first_orders_snapshot}"
emit_env MO_ICEBERG_TIER_A_MAINTENANCE_REWRITE_DATA_MO_SQL "call iceberg_rewrite_data_files('${MO_CATALOG}.maintenance.orders_small', 'ref=main,target_file_size=1048576')"
emit_env MO_ICEBERG_TIER_A_MAINTENANCE_REWRITE_MANIFESTS_MO_SQL "call iceberg_rewrite_manifests('${MO_CATALOG}.maintenance.orders_small', 'ref=main')"
emit_env MO_ICEBERG_TIER_A_MAINTENANCE_EXPIRE_MO_SQL "call iceberg_expire_snapshots('${MO_CATALOG}.maintenance.orders_small', 'older_than=9999-12-31 00:00:00,retain_last=1')"
emit_env MO_ICEBERG_TIER_A_MAINTENANCE_MO_SQL "select bucket, count(*) as c, sum(cast(order_id as bigint)) as order_sum, sum(cast(amount as bigint)) as amount_sum from ${MO_DB}.maintenance_orders_small group by bucket order by bucket"
emit_env MO_ICEBERG_TIER_A_MAINTENANCE_SPARK_SQL "select bucket, count(*) as c, sum(cast(order_id as bigint)) as order_sum, sum(cast(amount as bigint)) as amount_sum from ${CATALOG}.maintenance.orders_small group by bucket order by bucket"
emit_env MO_ICEBERG_TIER_A_MAINTENANCE_TRINO_SQL "select bucket, count(*) as c, sum(cast(order_id as bigint)) as order_sum, sum(cast(amount as bigint)) as amount_sum from ${TRINO_CATALOG}.maintenance.orders_small group by bucket order by bucket"

run_mo_setup_if_requested

echo "Tier A seed complete."
echo "Environment file: ${OUT_ENV}"
echo "Load it with:"
echo "  source ${OUT_ENV}"
