#!/usr/bin/env bash

set -euo pipefail

HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-6001}"
FAULT_PORTS=(${FAULT_PORTS:-6001 16001 16002})
MO_USER="${MO_USER:-root}"
MO_PASSWORD="${MO_PASSWORD:-111}"
MYSQL_BIN="${MYSQL_BIN:-mysql}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-120}"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-2}"
CASE="${CASE:-all-cpu}"

FP_SEND_ERROR="fj/iscp/index/send/error"
FP_SEND_BLOCK="fj/iscp/index/send/block"
FP_EXEC_ERROR="fj/iscp/index/exec/error"
FP_WATERMARK_ERROR="fj/iscp/index/watermark/error"
FP_CLOSE_BLOCK="fj/iscp/index/close/block"
FP_HNSW_UPDATE_ERROR="fj/iscp/index/hnsw/update/error"
FP_HNSW_SAVE_ERROR="fj/iscp/index/hnsw/save/error"
FP_CUVS_APPEND_ERROR="fj/iscp/index/cuvs/append/error"
FP_CUVS_SAVE_ERROR="fj/iscp/index/cuvs/save/error"

if ! command -v "${MYSQL_BIN}" >/dev/null 2>&1; then
  echo "mysql client not found: ${MYSQL_BIN}" >&2
  exit 1
fi

mysql_cmd() {
  MYSQL_PWD="${MO_PASSWORD}" "${MYSQL_BIN}" \
    -h "${HOST}" -P "${PORT}" -u "${MO_USER}" \
    --batch --raw --skip-column-names --connect-timeout=5 "$@"
}

mysql_cmd_port() {
  local port="$1"
  shift
  MYSQL_PWD="${MO_PASSWORD}" "${MYSQL_BIN}" \
    -h "${HOST}" -P "${port}" -u "${MO_USER}" \
    --batch --raw --skip-column-names --connect-timeout=5 "$@"
}

sql() {
  mysql_cmd -e "$1"
}

sql_on_port() {
  local port="$1"
  local statement="$2"
  mysql_cmd_port "${port}" -e "${statement}"
}

log() {
  printf '[%s] %s\n' "$(date '+%H:%M:%S')" "$*"
}

enable_faults() {
  local port
  for port in "${FAULT_PORTS[@]}"; do
    sql_on_port "${port}" "select enable_fault_injection();" >/dev/null
  done
}

add_fault() {
  local name="$1"
  local freq="$2"
  local action="${3:-echo}"
  local iarg="${4:-0}"
  local sarg="${5:-}"
  local port
  for port in "${FAULT_PORTS[@]}"; do
    sql_on_port "${port}" "select add_fault_point('${name}', '${freq}', '${action}', ${iarg}, '${sarg}');" >/dev/null
  done
}

remove_fault() {
  local name="$1"
  local port
  for port in "${FAULT_PORTS[@]}"; do
    sql_on_port "${port}" "select remove_fault_point('${name}');" >/dev/null 2>&1 || true
  done
}

clear_faults() {
  for fp in \
    "${FP_SEND_ERROR}" "${FP_SEND_BLOCK}" "${FP_EXEC_ERROR}" \
    "${FP_WATERMARK_ERROR}" "${FP_CLOSE_BLOCK}" \
    "${FP_HNSW_UPDATE_ERROR}" "${FP_HNSW_SAVE_ERROR}" \
    "${FP_CUVS_APPEND_ERROR}" "${FP_CUVS_SAVE_ERROR}"; do
    remove_fault "${fp}"
  done
}

drop_db() {
  local db="$1"
  sql "drop database if exists ${db};"
}

setup_db() {
  local db="$1"
  drop_db "${db}"
  sql "create database ${db};"
}

job_error_query() {
  local index_name="$1"
  cat <<SQL
select json_unquote(json_extract(job_status, '$.ErrorMsg'))
from mo_catalog.mo_iscp_log
where job_name in ('${index_name}', 'index_${index_name}')
  and json_unquote(json_extract(job_status, '$.ErrorMsg')) like '%injected ISCP index%'
order by create_at desc
limit 1;
SQL
}

job_status_query() {
  local index_name="$1"
  cat <<SQL
select job_state, json_unquote(json_extract(job_status, '$.ErrorMsg')), job_status
from mo_catalog.mo_iscp_log
where job_name in ('${index_name}', 'index_${index_name}')
order by create_at desc
limit 3;
SQL
}

wait_job_registered() {
  local case_name="$1"
  local index_name="$2"
  local deadline=$((SECONDS + TIMEOUT_SECONDS))
  local out=""

  while (( SECONDS < deadline )); do
    out="$(sql "select count(*) from mo_catalog.mo_iscp_log where job_name in ('${index_name}', 'index_${index_name}');" | tr -d '\r' || true)"
    if [[ "${out}" != "" && "${out}" != "0" ]]; then
      log "READY ${case_name}: ISCP job registered"
      sleep 3
      return 0
    fi
    sleep "${POLL_INTERVAL_SECONDS}"
  done

  log "FAIL ${case_name}: timed out waiting for ISCP job registration"
  return 1
}

expect_iscp_error() {
  local case_name="$1"
  local index_name="$2"
  local deadline=$((SECONDS + TIMEOUT_SECONDS))
  local out=""

  while (( SECONDS < deadline )); do
    out="$(sql "$(job_error_query "${index_name}")" | tr -d '\r' || true)"
    if [[ "${out}" == *"injected ISCP index"* ]]; then
      log "PASS ${case_name}: ${out}"
      return 0
    fi
    sleep "${POLL_INTERVAL_SECONDS}"
  done

  log "FAIL ${case_name}: timed out waiting for ISCP injected error"
  sql "$(job_status_query "${index_name}")" || true
  return 1
}

assert_alive() {
  local case_name="$1"
  local out
  out="$(sql "select 1;" | tr -d '\r')"
  if [[ "${out}" != "1" ]]; then
    log "FAIL ${case_name}: cluster did not answer select 1"
    return 1
  fi
  log "PASS ${case_name}: cluster answered after injected slow path"
}

create_ivf_table() {
  local db="$1"
  setup_db "${db}"
  sql "use ${db};
      create table t(a bigint primary key, b vecf32(3), c int);
      insert into t values
        (1, '[1,2,3]', 1),
        (2, '[2,3,4]', 2),
        (3, '[3,4,5]', 3);"
}

create_empty_ivf_table() {
  local db="$1"
  setup_db "${db}"
  sql "use ${db}; create table t(a bigint primary key, b vecf32(3), c int);"
}

create_fulltext_table() {
  local db="$1"
  setup_db "${db}"
  sql "use ${db};
      set ft_relevancy_algorithm='TF-IDF';
      create table t(a bigint primary key, body varchar, title text);"
}

create_hnsw_table() {
  local db="$1"
  setup_db "${db}"
  sql "use ${db};
      set global experimental_hnsw_index = 1;
      set experimental_hnsw_index = 1;
      create table t(a bigint primary key, b vecf32(3), c int);"
}

run_ivf_snapshot_exec_first() {
  local db="iscp_fp_ivf_snapshot_exec_first"
  local idx="idx_ivf_snap_first"
  create_ivf_table "${db}"
  add_fault "${FP_EXEC_ERROR}" "1:1:1:" "echo" 0 ""
  sql "use ${db}; create index ${idx} using ivfflat on t(b) lists=2 op_type 'vector_l2_ops' async;"
  expect_iscp_error "ivf-snapshot-exec-first" "${idx}"
}

run_ivf_snapshot_exec_second() {
  local db="iscp_fp_ivf_snapshot_exec_second"
  local idx="idx_ivf_snap_second"
  create_empty_ivf_table "${db}"
  sql "use ${db};
      insert into t
      select result, concat('[', result % 10, ',', (result + 1) % 10, ',', (result + 2) % 10, ']'), result
      from generate_series(1, 8300) g;"
  add_fault "${FP_EXEC_ERROR}" "2:2:1:" "echo" 0 ""
  sql "use ${db}; create index ${idx} using ivfflat on t(b) lists=2 op_type 'vector_l2_ops' async;"
  expect_iscp_error "ivf-snapshot-exec-second" "${idx}"
}

run_ivf_tail_exec_first() {
  local db="iscp_fp_ivf_tail_exec_first"
  local idx="idx_ivf_tail_first"
  create_empty_ivf_table "${db}"
  sql "use ${db}; create index ${idx} using ivfflat on t(b) lists=2 op_type 'vector_l2_ops' async;"
  wait_job_registered "ivf-tail-exec-first" "${idx}"
  add_fault "${FP_EXEC_ERROR}" "1:1:1:" "echo" 0 ""
  sql "use ${db}; insert into t values (10, '[1,1,1]', 10), (11, '[2,2,2]', 11);"
  expect_iscp_error "ivf-tail-exec-first" "${idx}"
}

run_ivf_tail_exec_second() {
  local db="iscp_fp_ivf_tail_exec_second"
  local idx="idx_ivf_tail_second"
  create_empty_ivf_table "${db}"
  sql "use ${db}; create index ${idx} using ivfflat on t(b) lists=2 op_type 'vector_l2_ops' async;"
  wait_job_registered "ivf-tail-exec-second" "${idx}"
  add_fault "${FP_EXEC_ERROR}" "2:2:1:" "echo" 0 ""
  sql "use ${db};
      insert into t
      select result, concat('[', result % 10, ',', (result + 1) % 10, ',', (result + 2) % 10, ']'), result
      from generate_series(1, 8300) g;"
  expect_iscp_error "ivf-tail-exec-second" "${idx}"
}

run_fulltext_snapshot_exec_first() {
  local db="iscp_fp_ft_snapshot_exec_first"
  local idx="idx_ft_snap_first"
  create_fulltext_table "${db}"
  sql "use ${db}; insert into t values (1, 'red blue', 'first'), (2, 'yellow car', 'second');"
  add_fault "${FP_EXEC_ERROR}" "1:1:1:" "echo" 0 ""
  sql "use ${db}; create fulltext index ${idx} on t(body, title) async;"
  expect_iscp_error "fulltext-snapshot-exec-first" "${idx}"
}

run_fulltext_tail_exec_first() {
  local db="iscp_fp_ft_tail_exec_first"
  local idx="idx_ft_tail_first"
  create_fulltext_table "${db}"
  sql "use ${db}; create fulltext index ${idx} on t(body, title) async;"
  wait_job_registered "fulltext-tail-exec-first" "${idx}"
  add_fault "${FP_EXEC_ERROR}" "1:1:1:" "echo" 0 ""
  sql "use ${db};
      insert into t values (1, 'red blue', 'first'), (2, 'yellow car', 'second');
      delete from t where a = 1;"
  expect_iscp_error "fulltext-tail-exec-first" "${idx}"
}

run_send_error() {
  local db="iscp_fp_send_error"
  local idx="idx_send_error"
  create_ivf_table "${db}"
  add_fault "${FP_SEND_ERROR}" "1:1:1:" "echo" 0 ""
  sql "use ${db}; create index ${idx} using ivfflat on t(b) lists=2 op_type 'vector_l2_ops' async;"
  expect_iscp_error "send-error" "${idx}"
}

run_send_block() {
  local db="iscp_fp_send_block"
  local idx="idx_send_block"
  create_ivf_table "${db}"
  add_fault "${FP_SEND_BLOCK}" "1:1:1:" "sleep" 2 ""
  sql "use ${db}; create index ${idx} using ivfflat on t(b) lists=2 op_type 'vector_l2_ops' async;"
  sleep 4
  assert_alive "send-block"
}

run_watermark_error() {
  local db="iscp_fp_watermark_error"
  local idx="idx_watermark"
  create_ivf_table "${db}"
  sql "use ${db}; create index ${idx} using ivfflat on t(b) lists=2 op_type 'vector_l2_ops' async;"
  wait_job_registered "watermark-error" "${idx}"
  sleep 8
  add_fault "${FP_WATERMARK_ERROR}" "1:1:1:" "echo" 0 ""
  sql "use ${db}; insert into t values (30, '[3,3,3]', 30);"
  expect_iscp_error "watermark-error" "${idx}"
}

run_close_block() {
  local db="iscp_fp_close_block"
  local idx="idx_close_block"
  create_ivf_table "${db}"
  add_fault "${FP_CLOSE_BLOCK}" "1:1:1:" "sleep" 2 ""
  sql "use ${db}; create index ${idx} using ivfflat on t(b) lists=2 op_type 'vector_l2_ops' async;"
  sleep 4
  assert_alive "close-block"
}

run_hnsw_update_error() {
  local db="iscp_fp_hnsw_update_error"
  local idx="idx_hnsw_update"
  create_hnsw_table "${db}"
  sql "use ${db}; create index ${idx} using hnsw on t(b) op_type 'vector_l2_ops' M 16 EF_CONSTRUCTION 64 EF_SEARCH 64 async;"
  wait_job_registered "hnsw-update-error" "${idx}"
  add_fault "${FP_HNSW_UPDATE_ERROR}" "1:1:1:" "echo" 0 ""
  sql "use ${db}; insert into t values (1, '[1,2,3]', 1);"
  expect_iscp_error "hnsw-update-error" "${idx}"
}

run_hnsw_save_error() {
  local db="iscp_fp_hnsw_save_error"
  local idx="idx_hnsw_save"
  create_hnsw_table "${db}"
  sql "use ${db}; insert into t values (1, '[1,2,3]', 1), (2, '[2,3,4]', 2);"
  add_fault "${FP_HNSW_SAVE_ERROR}" "1:1:1:" "echo" 0 ""
  sql "use ${db}; create index ${idx} using hnsw on t(b) op_type 'vector_l2_ops' M 16 EF_CONSTRUCTION 64 EF_SEARCH 64 async;"
  expect_iscp_error "hnsw-save-error" "${idx}"
}

run_cuvs_append_error() {
  local db="iscp_fp_cuvs_append_error"
  local idx="idx_cuvs_append"
  setup_db "${db}"
  sql "use ${db};
      set experimental_cagra_index = 1;
      create table t(a bigint primary key, b vecf32(3), c int);
      create index ${idx} using cagra on t(b) op_type 'vector_l2_ops' lists=2 async;"
  wait_job_registered "cuvs-append-error" "${idx}"
  add_fault "${FP_CUVS_APPEND_ERROR}" "1:1:1:" "echo" 0 ""
  sql "use ${db}; insert into t values (1, '[1,2,3]', 1);"
  expect_iscp_error "cuvs-append-error" "${idx}"
}

run_cuvs_save_error() {
  local db="iscp_fp_cuvs_save_error"
  local idx="idx_cuvs_save"
  setup_db "${db}"
  sql "use ${db};
      set experimental_cagra_index = 1;
      create table t(a bigint primary key, b vecf32(3), c int);
      insert into t values (1, '[1,2,3]', 1), (2, '[2,3,4]', 2);"
  add_fault "${FP_CUVS_SAVE_ERROR}" "1:1:1:" "echo" 0 ""
  sql "use ${db}; create index ${idx} using cagra on t(b) op_type 'vector_l2_ops' lists=2 async;"
  expect_iscp_error "cuvs-save-error" "${idx}"
}

run_one() {
  local case_name="$1"
  clear_faults
  log "START ${case_name}"
  case "${case_name}" in
    ivf-snapshot-exec-first) run_ivf_snapshot_exec_first ;;
    ivf-snapshot-exec-second) run_ivf_snapshot_exec_second ;;
    ivf-tail-exec-first) run_ivf_tail_exec_first ;;
    ivf-tail-exec-second) run_ivf_tail_exec_second ;;
    fulltext-snapshot-exec-first) run_fulltext_snapshot_exec_first ;;
    fulltext-tail-exec-first) run_fulltext_tail_exec_first ;;
    send-error) run_send_error ;;
    send-block) run_send_block ;;
    watermark-error) run_watermark_error ;;
    close-block) run_close_block ;;
    hnsw-update-error) run_hnsw_update_error ;;
    hnsw-save-error) run_hnsw_save_error ;;
    cuvs-append-error) run_cuvs_append_error ;;
    cuvs-save-error) run_cuvs_save_error ;;
    *) echo "unknown case: ${case_name}" >&2; return 2 ;;
  esac
  clear_faults
}

run_case_set() {
  local set_name="$1"
  case "${set_name}" in
    all-cpu)
      for c in \
        ivf-snapshot-exec-first \
        ivf-snapshot-exec-second \
        ivf-tail-exec-first \
        ivf-tail-exec-second \
        fulltext-snapshot-exec-first \
        fulltext-tail-exec-first \
        send-block \
        close-block \
        hnsw-update-error; do
        run_one "${c}"
      done
      ;;
    all-cpu-extra)
      for c in send-error watermark-error hnsw-save-error; do
        run_one "${c}"
      done
      ;;
    gpu)
      for c in cuvs-append-error cuvs-save-error; do
        run_one "${c}"
      done
      ;;
    all)
      run_case_set all-cpu
      run_case_set gpu
      ;;
    *)
      run_one "${set_name}"
      ;;
  esac
}

enable_faults
trap clear_faults EXIT
run_case_set "${CASE}"
log "DONE ${CASE}"
