#!/usr/bin/env bash
set -euo pipefail

HOST="127.0.0.1"
PORT="6001"
USER="dump"
PASS="111"

print_step() {
  local title="$1"
  local detail="${2:-}"
  echo
  echo "Step: ${title}"
  if [[ -n "$detail" ]]; then
    echo "SQL/operation:"
    printf '%s\n' "$detail" | sed 's/^/  /'
  fi
  echo
}

usage() {
  cat <<'USAGE'
Usage: ./branch_self_diff.sh [options] (-export_dir_path DIR -tbl db.tbl | -apply_file FILE -apply_to_tbl db.tbl)
Defaults: -h 127.0.0.1 -P 6001 -u dump -p 111
Options:
  -h HOST              Override host
  -P PORT              Override port
  -u USER              Override user
  -p PASS              Override password
  -export_dir_path DIR Diff mode output directory (required for diff mode)
  -tbl db.tbl          Source table for diff mode, format db.tbl
  -apply_file FILE     Apply mode input file (.csv or .sql)
  -apply_to_tbl db.tbl Target table for apply mode, format db.tbl
  -help                Show this message

Rules:
  -export_dir_path and -apply_file are mutually exclusive

Examples:
  ./branch_self_diff.sh -export_dir_path '/tmp/output' -tbl test.t0
  ./branch_self_diff.sh -apply_file '/tmp/output/diff_example.csv' -apply_to_tbl test.t1

USAGE
}

err() {
  echo "Error: $*" >&2
  exit 1
}

escape_sql_literal() {
  printf "%s" "$1" | sed "s/'/''/g"
}

validate_db_tbl() {
  local value="$1"
  [[ "$value" =~ ^[A-Za-z0-9_]+\.[A-Za-z0-9_]+$ ]] || err "Table must be in format db.tbl: $value"
}

mysql_exec() {
  local sql="$1"
  MYSQL_PWD="$PASS" mysql -h "$HOST" -P "$PORT" -u "$USER" -N -e "$sql"
}

mysql_exec_db() {
  local db="$1"
  local sql="$2"
  MYSQL_PWD="$PASS" mysql -h "$HOST" -P "$PORT" -u "$USER" -D "$db" -N -e "$sql"
}

run_self_diff() {
  local export_dir_path="$1"
  local src_tbl="$2"

  validate_db_tbl "$src_tbl"
  [[ -d "$export_dir_path" ]] || mkdir -p "$export_dir_path"
  [[ -d "$export_dir_path" ]] || err "export_dir_path must be an existing directory"

  local db="${src_tbl%%.*}"
  local tbl="${src_tbl#*.}"

  local sp_last
  local step0_sql="select sname as sp_last from mo_catalog.mo_snapshots where database_name = '${db}' and table_name = '${tbl}' order by ts desc limit 1;"
  print_step "step0 get last snapshot" "$step0_sql"
  sp_last=$(mysql_exec "select sname as sp_last from mo_catalog.mo_snapshots where database_name = '${db}' and table_name = '${tbl}' order by ts desc limit 1;")
  sp_last=${sp_last//$'\n'/}

  local sp_newest="sp_$(date +"%Y%m%d%H%M%S%3N")"
  print_step "step1 create newest snapshot" "create snapshot ${sp_newest} for table ${db} ${tbl};"
  mysql_exec "create snapshot ${sp_newest} for table ${db} ${tbl};"

  local tbl_copy="${tbl}_copy"
  local tbl_empty="${tbl}_empty"

  cleanup_db="$db"
  cleanup_tbl_copy="$tbl_copy"
  cleanup_tbl_empty="$tbl_empty"
  cleanup_sp_last="$sp_last"

  cleanup_msgs=()
  cleanup_sqls=()
  cleanup_msgs+=("cleanup drop copy table")
  cleanup_sqls+=("drop table if exists ${cleanup_db}.${cleanup_tbl_copy};")
  cleanup_msgs+=("cleanup drop empty table")
  cleanup_sqls+=("drop table if exists ${cleanup_db}.${cleanup_tbl_empty};")
  if [[ -n "${cleanup_sp_last:-}" ]]; then
    cleanup_msgs+=("cleanup drop last snapshot")
    cleanup_sqls+=("drop snapshot if exists ${cleanup_sp_last};")
  fi

  cleanup() {
    set +e
    set +u
    for i in "${!cleanup_msgs[@]}"; do
      print_step "${cleanup_msgs[$i]}" "${cleanup_sqls[$i]}"
      mysql_exec "${cleanup_sqls[$i]}" >/dev/null 2>&1
    done
    set -euo pipefail
  }
  trap cleanup EXIT

  print_step "step2 drop old copy table" "drop table if exists ${db}.${tbl_copy};"
  mysql_exec "drop table if exists ${db}.${tbl_copy};"
  print_step "step3 clone newest snapshot" "create table ${db}.${tbl_copy} clone ${db}.${tbl}{snapshot = \"${sp_newest}\"};"
  mysql_exec "create table ${db}.${tbl_copy} clone ${db}.${tbl}{snapshot = \"${sp_newest}\"};"

  if [[ -n "$sp_last" ]]; then
    local diff_sql="data branch diff ${db}.${tbl_copy} against ${db}.${tbl}{snapshot=\"${sp_last}\"} output file '${export_dir_path}';"
    print_step "step4 diff copy against last snapshot" "$diff_sql"
    mysql_exec "$diff_sql"
  else
    print_step "step4 create empty table" "create table ${db}.${tbl_empty} like ${db}.${tbl};"
    mysql_exec "drop table if exists ${db}.${tbl_empty};"
    mysql_exec "create table ${db}.${tbl_empty} like ${db}.${tbl};"
    local diff_sql="data branch diff ${db}.${tbl_copy} against ${db}.${tbl_empty} output file '${export_dir_path}';"
    print_step "step5 diff copy against empty table" "$diff_sql"
    mysql_exec "$diff_sql"
  fi

  trap - EXIT
  cleanup
}

run_apply() {
  local apply_file="$1"
  local apply_to_tbl="$2"

  [[ -f "$apply_file" ]] || err "apply_file not found: $apply_file"
  validate_db_tbl "$apply_to_tbl"

  local apply_db="${apply_to_tbl%%.*}"
  local apply_tbl="${apply_to_tbl#*.}"

  case "$apply_file" in
    *.csv|*.CSV)
      local escaped_file
      escaped_file=$(escape_sql_literal "$apply_file")
      local sql
      sql="load data infile '${escaped_file}' into table ${apply_tbl} FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\\\\' LINES TERMINATED BY '\\n' parallel 'true';"
      print_step "apply csv via load data" "$sql"
      mysql_exec_db "$apply_db" "$sql"
      ;;
    *.sql|*.SQL)
      local tmp_sql
      tmp_sql=$(mktemp /tmp/data_branch_apply_XXXX.sql)
      sed -E \
        -e "s/(delete[[:space:]]+from[[:space:]]+)[A-Za-z0-9_]+\.[A-Za-z0-9_]+/\\1${apply_to_tbl}/Ig" \
        -e "s/(replace[[:space:]]+into[[:space:]]+)[A-Za-z0-9_]+\.[A-Za-z0-9_]+/\\1${apply_to_tbl}/Ig" \
        "$apply_file" > "$tmp_sql"
      local detail=$'rewrite delete/replace targets to '"${apply_to_tbl}"$'\noriginal file: '"${apply_file}"
      print_step "apply sql replace targets" "$detail"
      MYSQL_PWD="$PASS" mysql -h "$HOST" -P "$PORT" -u "$USER" -D "$apply_db" < "$tmp_sql"
      rm -f "$tmp_sql"
      ;;
    *)
      err "apply_file must be a .csv or .sql file"
      ;;
  esac
}

main() {
  local export_dir_path=""
  local src_tbl=""
  local apply_file=""
  local apply_to_tbl=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      -h) HOST="$2"; shift 2;;
      -P) PORT="$2"; shift 2;;
      -u) USER="$2"; shift 2;;
      -p) PASS="$2"; shift 2;;
      -export_dir_path) export_dir_path="$2"; shift 2;;
      -tbl) src_tbl="$2"; shift 2;;
      -apply_file) apply_file="$2"; shift 2;;
      -apply_to_tbl) apply_to_tbl="$2"; shift 2;;
      -help|--help) usage; exit 0;;
      *) err "Unknown argument: $1";;
    esac
  done

  if [[ -n "$export_dir_path" && -n "$apply_file" ]]; then
    err "-export_dir_path and -apply_file cannot be used together"
  fi

  if [[ -n "$export_dir_path" ]]; then
    [[ -n "$src_tbl" ]] || err "-tbl is required when using -export_dir_path"
    run_self_diff "$export_dir_path" "$src_tbl"
  elif [[ -n "$apply_file" ]]; then
    [[ -n "$apply_to_tbl" ]] || err "-apply_to_tbl is required when using -apply_file"
    run_apply "$apply_file" "$apply_to_tbl"
  else
    usage
    exit 1
  fi
}

main "$@"
