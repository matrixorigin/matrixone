#!/usr/bin/env bash
# Controlled issue #25782 experiment. Missing per-operator evidence is always
# INCONCLUSIVE; table row totals are never used as spill evidence.
set -euo pipefail

readonly HARNESS_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
# shellcheck disable=SC1091
source "${HARNESS_DIR}/lib.sh"

KEEP=0
RUNTIME="${MO_25782_RUNTIME:-}"
PROXY_HOST="${MO_25782_PROXY_HOST:-127.0.0.1}"
PROXY_PORT="${MO_25782_PROXY_PORT:-${PROXY_PORT:-6001}}"
MYSQL_BIN="${MO_25782_MYSQL_BIN:-mysql}"
DB_NAME="${MO_25782_DB_NAME:-issue25782}"
QUERY_TIMEOUT="${MO_25782_QUERY_TIMEOUT:-120}"
WATCHDOG_INTERVAL="${MO_25782_WATCHDOG_INTERVAL:-1}"
RUN_ID="$(date +%Y%m%dT%H%M%S)-$$"

usage() {
    local rc="${1:-2}"
    cat >&2 <<'EOF'
usage: run.sh --runtime ABSOLUTE_RUNTIME_DIR [--keep]

The runtime must have been prepared and started by prepare.sh/start.sh.  The
default EXIT trap invokes stop.sh; --keep preserves the runtime for review.
Proxy is fixed to 127.0.0.1:6001 unless MO_25782_ALLOW_PROXY_PORT_OVERRIDE=1.
EOF
    exit "$rc"
}

while (( $# > 0 )); do
    case "$1" in
        --runtime)
            (( $# >= 2 )) || usage
            RUNTIME="$2"
            shift 2
            ;;
        --keep)
            KEEP=1
            shift
            ;;
        -h|--help)
            usage 0
            ;;
        *)
            usage
            ;;
    esac
done

[[ -n "$RUNTIME" ]] || usage
[[ "$PROXY_HOST" == "127.0.0.1" || "$PROXY_HOST" == "localhost" ]] || die "Proxy must be local"
if [[ "$PROXY_PORT" != 6001 && "${MO_25782_ALLOW_PROXY_PORT_OVERRIDE:-0}" != 1 ]]; then
    die "Proxy port must be 6001 (set MO_25782_ALLOW_PROXY_PORT_OVERRIDE=1 only for an approved lab override)"
fi
[[ "$QUERY_TIMEOUT" =~ ^[1-9][0-9]*$ ]] || die "MO_25782_QUERY_TIMEOUT must be a positive integer"

cleanup() {
    local rc=$? stop_rc=0
    if (( KEEP == 0 )) && [[ -x "${HARNESS_DIR}/stop.sh" ]] && [[ -n "${RUNTIME}" ]]; then
        "${HARNESS_DIR}/stop.sh" --runtime "${RUNTIME}" >/dev/null 2>&1 || stop_rc=$?
        if declare -F validate_stopped_telemetry >/dev/null 2>&1 && [[ -n "${RESULT_DIR:-}" ]]; then
            if ! validate_stopped_telemetry "$stop_rc"; then
                printf 'classification=INCONCLUSIVE\nreason=post_stop_telemetry_invalid\n' >"${RESULT_DIR}/classification.overall"
                printf 'final_telemetry=invalid\nstop_rc=%s\n' "$stop_rc" >"${RESULT_DIR}/manifest.final_telemetry"
                rc=1
            fi
        elif (( stop_rc != 0 )); then
            rc=1
        fi
    fi
    exit "$rc"
}
trap cleanup EXIT INT TERM

load_runtime "$RUNTIME"
readonly RESULT_DIR="${MO_25782_RUNTIME}/results/${RUN_ID}"
readonly ALLOWED_CNS="${CN1_SERVICE_ADDR:-127.0.0.1:18100},${CN2_SERVICE_ADDR:-127.0.0.1:18200}"
mkdir -p -- "${RESULT_DIR}/plans" "${RESULT_DIR}/execution" "${RESULT_DIR}/evidence" "${RESULT_DIR}/monitor"
chmod 0700 -- "${RESULT_DIR}" "${RESULT_DIR}/plans" "${RESULT_DIR}/execution" "${RESULT_DIR}/evidence" "${RESULT_DIR}/monitor"

readonly EXPECTED_COMMIT="${MO_25782_EXPECTED_COMMIT:-$(git -C "${REPO_ROOT}" rev-parse HEAD)}"
readonly ACCEPTANCE_MODE="${MO_25782_MODE:-fixed}"
case "${ACCEPTANCE_MODE}" in
    fixed|historical) ;;
    *) die "MO_25782_MODE must be fixed or historical" ;;
esac
readonly SOURCE_ROOT="${REPO_ROOT}"

validate_stopped_telemetry() {
    local stop_rc="$1" name active peak max swap pre_oom pre_oom_kill base_oom base_oom_kill base_swap value ok=1
    (( stop_rc == 0 )) || ok=0
    for name in log tn cn1 cn2 proxy; do
        unset state FINAL_ACTIVE_STATE CGROUP_MEMORY_PEAK_PRESTOP CGROUP_MEMORY_MAX_PRESTOP \
            CGROUP_SWAP_CURRENT_PRESTOP CGROUP_OOM_PRESTOP CGROUP_OOM_KILL_PRESTOP \
            CGROUP_OOM_BASE CGROUP_OOM_KILL_BASE CGROUP_SWAP_CURRENT_BASE || true
        if ! manifest_load "$name" 2>/dev/null; then ok=0; continue; fi
        active="${FINAL_ACTIVE_STATE:-}"; peak="${CGROUP_MEMORY_PEAK_PRESTOP:-}"; max="${CGROUP_MEMORY_MAX_PRESTOP:-}"
        swap="${CGROUP_SWAP_CURRENT_PRESTOP:-}"; pre_oom="${CGROUP_OOM_PRESTOP:-}"; pre_oom_kill="${CGROUP_OOM_KILL_PRESTOP:-}"
        base_oom="${CGROUP_OOM_BASE:-}"; base_oom_kill="${CGROUP_OOM_KILL_BASE:-}"; base_swap="${CGROUP_SWAP_CURRENT_BASE:-}"
        [[ "${state:-}" == stopped && ( "$active" == inactive || "$active" == failed ) ]] || ok=0
        for value in "$peak" "$max" "$swap" "$pre_oom" "$pre_oom_kill" "$base_oom" "$base_oom_kill" "$base_swap"; do
            [[ "$value" =~ ^[0-9]+$ ]] || ok=0
        done
        if [[ "$pre_oom" =~ ^[0-9]+$ && "$base_oom" =~ ^[0-9]+$ ]] && (( pre_oom != base_oom )); then ok=0; fi
        if [[ "$pre_oom_kill" =~ ^[0-9]+$ && "$base_oom_kill" =~ ^[0-9]+$ ]] && (( pre_oom_kill != base_oom_kill )); then ok=0; fi
        if [[ "$swap" =~ ^[0-9]+$ && "$base_swap" =~ ^[0-9]+$ ]] && (( swap > base_swap )); then ok=0; fi
        if [[ "$name" == cn1 || "$name" == cn2 ]] && [[ "$peak" =~ ^[0-9]+$ && "$max" =~ ^[1-9][0-9]*$ ]] &&
            awk -v p="$peak" -v m="$max" 'BEGIN {exit !((p/m)>=0.90)}'; then ok=0; fi
    done
    if (( ok )); then
        printf 'final_telemetry=valid\nstop_rc=0\n' >"${RESULT_DIR}/manifest.final_telemetry"
        return 0
    fi
    return 1
}

verify_source_gate() {
    local commit threshold_src spill_src ctor_src name manifest_hash manifest_commit running_hash
    local binary_hash="" binary_commit=""
    commit="$(git -C "$SOURCE_ROOT" rev-parse HEAD)"
    [[ "$commit" == "$EXPECTED_COMMIT"* ]] || die "locked source mismatch: expected ${EXPECTED_COMMIT}, got ${commit}"
    threshold_src="${SOURCE_ROOT}/pkg/sql/colexec/spill_threshold.go"
    spill_src="${SOURCE_ROOT}/pkg/sql/colexec/hashbuild/spill.go"
    ctor_src="${SOURCE_ROOT}/pkg/sql/compile/operator.go"
    [[ -f "$threshold_src" && -f "$spill_src" && -f "$ctor_src" ]] || die "source gate files are missing"
    grep -Eq 'threshold[[:space:]]*<=[[:space:]]*100000' "$threshold_src" || die "ShouldSpill row threshold gate missing"
    grep -Eq 'rowCount[[:space:]]*>=[[:space:]]*threshold' "$threshold_src" || die "ShouldSpill does not use rowCount for small threshold"
    grep -Eq 'if[[:space:]]+![[:space:]]*hashBuild\.IsShuffle' "$spill_src" || die "shouldSpillBatches !IsShuffle exclusion missing"
    grep -Eq 'ret\.IsShuffle[[:space:]]*=[[:space:]]*false' "$ctor_src" || die "broadcast HashBuild constructor setting missing"
    grep -Eq 'ret\.IsShuffle[[:space:]]*=[[:space:]]*true' "$ctor_src" || die "shuffle HashBuild constructor setting missing"
    grep -Eq 'ret\.SpillThreshold[[:space:]]*=[[:space:]]*node\.SpillMem' "$ctor_src" || die "shuffle SpillThreshold constructor setting missing"
    for name in log tn cn1 cn2 proxy; do
        unset state MainPID PROC_STARTTIME EXE CMDLINE BINARY_SHA256 BINARY_COMMIT || true
        manifest_load "$name" || die "${name}: running manifest missing"
        [[ "${state:-}" == running ]] || die "${name}: service is not in running generation"
        manifest_hash="${BINARY_SHA256:-}"; manifest_commit="${BINARY_COMMIT:-}"
        [[ "$manifest_hash" =~ ^[0-9a-f]{64}$ ]] || die "${name}: binary hash missing from manifest"
        [[ "$manifest_commit" == "$EXPECTED_COMMIT"* ]] || die "${name}: binary commit does not match locked source"
        proc_identity_matches "${MainPID:-}" "${PROC_STARTTIME:-}" "${EXE:-}" "${CMDLINE:-}" ||
            die "${name}: running process identity no longer matches manifest"
        running_hash="$(sha256sum -- "/proc/${MainPID}/exe" | awk '{print $1}')"
        [[ "$running_hash" == "$manifest_hash" ]] || die "${name}: running binary hash differs from manifest"
        if [[ -z "$binary_hash" ]]; then
            binary_hash="$manifest_hash"; binary_commit="$manifest_commit"
        else
            [[ "$manifest_hash" == "$binary_hash" && "$manifest_commit" == "$binary_commit" ]] ||
                die "${name}: service binary differs from the other harness services"
        fi
    done
    {
        printf 'source_commit=%s\nexpected_commit_prefix=%s\n' "$commit" "$EXPECTED_COMMIT"
        printf 'binary_commit=%s\nbinary_sha256=%s\n' "$binary_commit" "$binary_hash"
        printf 'threshold_file=%s\nspill_file=%s\nconstructor_file=%s\n' "$threshold_src" "$spill_src" "$ctor_src"
    } >"${RESULT_DIR}/manifest.source"
}

source_gate
verify_source_gate
if declare -F preflight_manifest_growth_gate >/dev/null 2>&1; then
    preflight_manifest_growth_gate >"${RESULT_DIR}/manifest.growth"
else
    preflight_growth_gate "${MO_25782_CGROUP_PATH:-}" >"${RESULT_DIR}/manifest.growth"
fi

{
    printf 'run_id=%s\nproxy_host=%s\nproxy_port=%s\ndatabase=%s\nquery_timeout_s=%s\n' \
        "$RUN_ID" "$PROXY_HOST" "$PROXY_PORT" "$DB_NAME" "$QUERY_TIMEOUT"
    printf 'source_commit=%s\n' "$(git -C "$SOURCE_ROOT" rev-parse HEAD)"
    printf 'runtime=%s\n' "$MO_25782_RUNTIME"
    printf 'acceptance_mode=%s\n' "$ACCEPTANCE_MODE"
    printf 'join_spill_mem=1000\nphysical_scale_build=132096\nphysical_scale_probe=132096\n'
    printf 'allowed_cns=%s\nwatchdog_interval_s=%s\n' "$ALLOWED_CNS" "$WATCHDOG_INTERVAL"
} >"${RESULT_DIR}/manifest.config"

cat "${HARNESS_DIR}"/sql/*.sql >"${RESULT_DIR}/sql.full.sql"

proxy_mysql_file() {
    local sql_file="$1" output="$2" with_headers="${3:-0}" route_label="${4:-}"
    local mysql_user="${MO_25782_MYSQL_USER:-root}"
    [[ -z "$route_label" ]] || mysql_user="${mysql_user}?issue25782_route=${route_label}"
    local -a cmd=("$MYSQL_BIN" --protocol=tcp -h "$PROXY_HOST" -P "$PROXY_PORT"
        -u "$mysql_user" --connect-timeout="$QUERY_TIMEOUT"
        --batch --raw --skip-ssl)
    [[ "$with_headers" == 1 ]] || cmd+=(--skip-column-names)
    command -v "$MYSQL_BIN" >/dev/null 2>&1 || die "mysql client not found: $MYSQL_BIN"
    if [[ -n "${MO_25782_MYSQL_PASSWORD:-111}" ]]; then
        MYSQL_PWD="${MO_25782_MYSQL_PASSWORD:-111}" timeout "${QUERY_TIMEOUT}s" "${cmd[@]}" <"$sql_file" >"$output" 2>&1
    else
        timeout "${QUERY_TIMEOUT}s" "${cmd[@]}" <"$sql_file" >"$output" 2>&1
    fi
}

cn_mysql_file() {
    local port="$1" sql_file="$2" output="$3"
    local -a cmd=("$MYSQL_BIN" --protocol=tcp -h 127.0.0.1 -P "$port"
        -u "${MO_25782_MYSQL_USER:-root}" --connect-timeout="$QUERY_TIMEOUT"
        --batch --raw --skip-ssl)
    command -v "$MYSQL_BIN" >/dev/null 2>&1 || die "mysql client not found: $MYSQL_BIN"
    if [[ -n "${MO_25782_MYSQL_PASSWORD:-111}" ]]; then
        MYSQL_PWD="${MO_25782_MYSQL_PASSWORD:-111}" timeout "${QUERY_TIMEOUT}s" "${cmd[@]}" <"$sql_file" >"$output" 2>&1
    else
        timeout "${QUERY_TIMEOUT}s" "${cmd[@]}" <"$sql_file" >"$output" 2>&1
    fi
}

sync_plan_stats() {
    local phase="$1" file="$2" port
    for port in 16001 16002; do
        log_msg "stats_sync phase=${phase} cn_port=${port}"
        cn_mysql_file "$port" "$file" "${RESULT_DIR}/execution/${phase}_sync_cn${port}.out" ||
            die "${phase}: failed to synchronize planner statistics on CN port ${port}"
    done
}

configure_route_labels() {
    local output="${RESULT_DIR}/execution/route_labels.out" backends="" try
    mysql_exec 127.0.0.1 16001 \
        "SELECT mo_ctl('cn','label','${CN1_UUID}:issue25782_route:cn1'); SELECT mo_ctl('cn','label','${CN2_UUID}:issue25782_route:cn2');" \
        >"$output" 2>&1 || die "failed to assign private CN route labels"
    for ((try=0; try<30; try++)); do
        backends="$(mysql_exec 127.0.0.1 16001 'SHOW BACKEND SERVERS;' 2>&1 || true)"
        printf 'attempt=%s\n%s\n' "$try" "$backends" >"${RESULT_DIR}/execution/route_labels.backends"
        if awk -v id="$CN1_UUID" '$0 ~ id && $0 ~ /issue25782_route/ && $0 ~ /cn1/ {found=1} END {exit !found}' <<<"$backends" &&
           awk -v id="$CN2_UUID" '$0 ~ id && $0 ~ /issue25782_route/ && $0 ~ /cn2/ {found=1} END {exit !found}' <<<"$backends"; then
            sleep 4
            printf 'route_labels=ready\n' >"${RESULT_DIR}/manifest.route_labels"
            return 0
        fi
        sleep 1
    done
    die "private CN route labels did not become visible"
}

run_sql() {
    local name="$1" file="$2" output with_headers=0
    output="${RESULT_DIR}/execution/${name}.out"
    [[ "$name" == *_plan ]] && with_headers=1
    log_msg "sql_start phase=${name} file=${file}"
    set +e
    proxy_mysql_file "$file" "$output" "$with_headers"
    local rc=$?
    set -e
    printf 'phase=%s\nquery_rc=%s\noutput=%s\n' "$name" "$rc" "$output" >"${RESULT_DIR}/manifest.${name}"
    log_msg "sql_end phase=${name} rc=${rc}"
    return "$rc"
}

plan_has_hash_join() { grep -Eiq 'hash[[:space:]_-]*join|join[[:space:]]+cond' "$1"; }
plan_has_hash_build() { grep -Eiq 'hash[[:space:]_-]*build' "$1"; }
plan_join_has_shuffle() { grep -Eiq 'join[[:space:]]+cond.*shuffle[[:space:]:]|hash[[:space:]_-]*join.*shuffle[[:space:]:]' "$1"; }
plan_is_multicn() { grep -Fq 'AP QUERY PLAN ON MULTICN' "$1"; }
plan_has_one_left_join() {
    [[ "$(grep -Eic '^[[:space:]]*Join Type:[[:space:]]+' "$1")" == 1 ]] &&
        grep -Eiq '^[[:space:]]*Join Type:[[:space:]]+LEFT([[:space:]]|$)' "$1"
}

machine_plan_gate() {
    local mode="$1" file="$2"
    plan_has_hash_join "$file" || die "${mode} EXPLAIN gate: hash join not found"
    plan_has_hash_build "$file" || die "${mode} PHYPLAN gate: hash build not found"
    plan_is_multicn "$file" || die "${mode} EXPLAIN gate: MULTICN plan not found"
    plan_has_one_left_join "$file" || die "${mode} EXPLAIN gate: expected exactly one LEFT join"
    ! grep -Fq '[ForceOneCN]' "$file" || die "${mode} EXPLAIN gate: ForceOneCN found"
    if [[ "$mode" == broadcast ]]; then
        ! plan_join_has_shuffle "$file" || die "${mode} HashJoin plan unexpectedly contains shuffle; refusing execution"
    else
        plan_join_has_shuffle "$file" || die "${mode} positive-control HashJoin plan has no shuffle; refusing execution"
    fi
    cp -- "$file" "${RESULT_DIR}/plans/${mode}.explain.out"
    printf 'plan_gate=%s\n' "$mode" >>"${RESULT_DIR}/manifest.${mode}_plan"
}

collect_evidence() {
    local phase="$1" query_rc="$2" shuffle=false
    local src snapshot dst provenance marker tmp snapshot_sha evidence_sha extracted_at
    src="${RESULT_DIR}/execution/${phase}.out"
    snapshot="${RESULT_DIR}/evidence/source.${phase}.out"
    dst="${RESULT_DIR}/evidence/operators.${phase}.tsv"
    provenance="${RESULT_DIR}/evidence/operators.${phase}.provenance.tsv"
    marker="${RESULT_DIR}/evidence/operators.${phase}.complete"
    rm -f -- "$snapshot" "$dst" "$provenance" "$marker"
    [[ -s "$src" && -x "${HARNESS_DIR}/extract-phyplan.sh" ]] || return 1
    case "$src$snapshot$dst$MO_25782_RUNTIME" in *$'\t'*|*$'\n'*) return 1 ;; esac

    tmp="$(mktemp "${snapshot}.tmp.XXXXXX")" || return 1
    if ! cp -- "$src" "$tmp"; then rm -f -- "$tmp"; return 1; fi
    chmod 0400 -- "$tmp"
    mv -f -- "$tmp" "$snapshot"
    [[ "$phase" == shuffle ]] && shuffle=true
    if ! "${HARNESS_DIR}/extract-phyplan.sh" "$snapshot" "$shuffle" "$dst"; then
        rm -f -- "$dst" "$provenance" "$marker"
        return 1
    fi
    snapshot_sha="$(sha256sum -- "$snapshot" | awk '{print $1}')"
    evidence_sha="$(sha256sum -- "$dst" | awk '{print $1}')"
    extracted_at="$(date -Is)"
    tmp="$(mktemp "${provenance}.tmp.XXXXXX")" || { rm -f -- "$dst"; return 1; }
    {
        printf 'schema_version\trun_id\truntime\tphase\tsnapshot_path\tsnapshot_sha256\tevidence_path\tevidence_sha256\tquery_rc\textracted_at\n'
        printf '1\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "$RUN_ID" "$MO_25782_RUNTIME" "$phase" "$(realpath -e -- "$snapshot")" "$snapshot_sha" \
            "$(realpath -e -- "$dst")" "$evidence_sha" "$query_rc" "$extracted_at"
    } >"$tmp"
    chmod 0400 -- "$tmp"
    mv -f -- "$tmp" "$provenance"
    tmp="$(mktemp "${marker}.tmp.XXXXXX")" || { rm -f -- "$dst" "$provenance"; return 1; }
    printf 'complete\n' >"$tmp"
    chmod 0400 -- "$tmp"
    mv -f -- "$tmp" "$marker"
}

sample_safety() {
    local phase="$1" query_pid="$2" out flags coverage mono wall memavail psi name danger=0
    local unit pid cg invocation exec_start nrestarts proc_start exe cmdline
    local actual_pid actual_cg actual_invocation actual_exec_start actual_nrestarts
    local current peak max swap_current swap_max oom oom_kill
    local base_swap base_oom base_oom_kill value
    out="${RESULT_DIR}/monitor/${phase}.tsv"
    flags="${RESULT_DIR}/monitor/${phase}.flags"
    coverage="${RESULT_DIR}/monitor/${phase}.coverage.tsv"
    mono="$(awk '{printf "%.0f", $1 * 1000000000; exit}' /proc/uptime 2>/dev/null || true)"
    wall="$(date -Is)"
    memavail="$(awk '/^MemAvailable:/ {printf "%.0f", $2 * 1024; exit}' /proc/meminfo 2>/dev/null || true)"
    psi="$(awk '/^some/ {for (i=1;i<=NF;i++) if ($i ~ /^avg10=/) {sub("avg10=", "", $i); print $i; exit}}' /proc/pressure/memory 2>/dev/null || true)"
    [[ "$mono" =~ ^[0-9]+$ && "$memavail" =~ ^[0-9]+$ && "$psi" =~ ^[0-9]+([.][0-9]+)?$ ]] || danger=1

    for name in log tn cn1 cn2 proxy; do
        unset UNIT MainPID CGROUP INVOCATION_ID EXEC_MAIN_START_MONOTONIC NRESTARTS_BASE \
            PROC_STARTTIME EXE CMDLINE CGROUP_SWAP_CURRENT_BASE CGROUP_OOM_BASE CGROUP_OOM_KILL_BASE || true
        if ! manifest_load "$name" 2>/dev/null; then
            danger=1
            printf '%s\t%s\t%s\t%s\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t%s\t%s\n' \
                "$mono" "$wall" "$phase" "$name" "$memavail" "$psi" >>"$out"
            continue
        fi
        unit="${UNIT:-}"; pid="${MainPID:-}"; cg="${CGROUP:-}"; invocation="${INVOCATION_ID:-}"
        exec_start="${EXEC_MAIN_START_MONOTONIC:-}"; nrestarts="${NRESTARTS_BASE:-}"
        proc_start="${PROC_STARTTIME:-}"; exe="${EXE:-}"; cmdline="${CMDLINE:-}"
        base_swap="${CGROUP_SWAP_CURRENT_BASE:-}"; base_oom="${CGROUP_OOM_BASE:-}"; base_oom_kill="${CGROUP_OOM_KILL_BASE:-}"
        actual_pid="$(systemctl --user show "$unit" -p MainPID --value 2>/dev/null || true)"
        actual_cg="$(systemctl --user show "$unit" -p ControlGroup --value 2>/dev/null || true)"
        actual_invocation="$(systemctl --user show "$unit" -p InvocationID --value 2>/dev/null || true)"
        actual_exec_start="$(systemctl --user show "$unit" -p ExecMainStartTimestampMonotonic --value 2>/dev/null || true)"
        actual_nrestarts="$(systemctl --user show "$unit" -p NRestarts --value 2>/dev/null || true)"
        current="$(cat "/sys/fs/cgroup${cg}/memory.current" 2>/dev/null || true)"
        peak="$(cat "/sys/fs/cgroup${cg}/memory.peak" 2>/dev/null || true)"
        max="$(cat "/sys/fs/cgroup${cg}/memory.max" 2>/dev/null || true)"
        swap_current="$(cat "/sys/fs/cgroup${cg}/memory.swap.current" 2>/dev/null || true)"
        swap_max="$(cat "/sys/fs/cgroup${cg}/memory.swap.max" 2>/dev/null || true)"
        oom="$(awk '$1 == "oom" {print $2}' "/sys/fs/cgroup${cg}/memory.events" 2>/dev/null || true)"
        oom_kill="$(awk '$1 == "oom_kill" {print $2}' "/sys/fs/cgroup${cg}/memory.events" 2>/dev/null || true)"
        if [[ "$actual_pid" != "$pid" || "$actual_cg" != "$cg" || "$actual_invocation" != "$invocation" ||
              "$actual_exec_start" != "$exec_start" || "$actual_nrestarts" != "$nrestarts" ]] ||
            ! proc_identity_matches "$pid" "$proc_start" "$exe" "$cmdline"; then
            danger=1
        fi
        for value in "$current" "$peak" "$max" "$swap_current" "$swap_max" "$oom" "$oom_kill" "$base_swap" "$base_oom" "$base_oom_kill"; do
            [[ "$value" =~ ^[0-9]+$ ]] || danger=1
        done
        if [[ "$current" =~ ^[0-9]+$ && "$peak" =~ ^[0-9]+$ && "$max" =~ ^[1-9][0-9]*$ ]] &&
            [[ "$name" == cn1 || "$name" == cn2 ]] &&
            awk -v c="$current" -v p="$peak" -v m="$max" 'BEGIN {exit !((c/m)>=0.90 || (p/m)>=0.90)}'; then
            danger=1
        fi
        if [[ "$swap_current" =~ ^[0-9]+$ && "$base_swap" =~ ^[0-9]+$ ]] && (( swap_current > base_swap )); then danger=1; fi
        if [[ "$oom" =~ ^[0-9]+$ && "$base_oom" =~ ^[0-9]+$ ]] && (( oom > base_oom )); then danger=1; fi
        if [[ "$oom_kill" =~ ^[0-9]+$ && "$base_oom_kill" =~ ^[0-9]+$ ]] && (( oom_kill > base_oom_kill )); then danger=1; fi
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "$mono" "$wall" "$phase" "$name" "$pid" "$invocation" "$cg" "$nrestarts" \
            "$current" "$peak" "$max" "$swap_current" "$oom" "$oom_kill" "$query_pid" "$memavail" "$psi" >>"$out"
    done
    [[ "$memavail" =~ ^[0-9]+$ ]] && (( memavail >= ${MO_25782_MIN_MEM_AVAILABLE_BYTES:-4294967296} )) || danger=1
    if [[ "$psi" =~ ^[0-9]+([.][0-9]+)?$ ]] &&
        awk -v p="$psi" -v m="${MO_25782_MAX_PSI_AVG10:-50}" 'BEGIN {exit !(p >= m)}'; then danger=1; fi
    printf '%s\t%s\n' "$mono" "$wall" >>"$coverage"
    if (( danger )); then
        printf 'watchdog_failure=1\nsafety_cancel=1\n' >>"$flags"
        return 1
    fi
    return 0
}

monitor_query() {
    local phase="$1" sql_file="$2" route_label="${3:-}" output
    output="${RESULT_DIR}/execution/${phase}.out"
    local query_pid=0 rc danger=0 start now elapsed
    : >"${RESULT_DIR}/monitor/${phase}.flags"
    printf 'mono_ns\twall_time\tphase\tunit\tmain_pid\tinvocation_id\tcgroup\tnrestarts\tmemory_current\tmemory_peak\tmemory_max\tswap_current\toom\toom_kill\tquery_pid\tmem_available\tpsi_some_avg10\n' >"${RESULT_DIR}/monitor/${phase}.tsv"
    printf 'mono_ns\twall_time\n' >"${RESULT_DIR}/monitor/${phase}.coverage.tsv"
    if ! sample_safety "$phase" 0; then
        printf 'preflight_watchdog_failure=1\nquery_rc=125\n' >>"${RESULT_DIR}/monitor/${phase}.flags"
        return 125
    fi
    set +e
    proxy_mysql_file "$sql_file" "$output" 0 "$route_label" &
    query_pid=$!
    set -e
    start="$(date +%s)"
    start_monitor "$phase" "$query_pid" "${RESULT_DIR}/monitor" || true
    while kill -0 "$query_pid" 2>/dev/null; do
        if ! sample_safety "$phase" "$query_pid"; then
            danger=1
            printf 'cancelled=1\nsafety_cancel=1\n' >>"${RESULT_DIR}/monitor/${phase}.flags"
            cancel_query "$query_pid" || true
            kill -TERM "$query_pid" 2>/dev/null || true
            break
        fi
        now="$(date +%s)"; elapsed=$((now - start))
        if (( elapsed >= QUERY_TIMEOUT )); then
            danger=1
            printf 'timeout=1\n' >>"${RESULT_DIR}/monitor/${phase}.flags"
            cancel_query "$query_pid" || true
            kill -TERM "$query_pid" 2>/dev/null || true
            break
        fi
        sleep "$WATCHDOG_INTERVAL"
    done
    set +e
    wait "$query_pid"
    rc=$?
    set -e
    stop_monitor "$phase" "${RESULT_DIR}/monitor" || true
    if ! sample_safety "$phase" "$query_pid"; then danger=1; fi
    if ! awk -F '\t' 'NR == 1 {next} {n++; if (prev && ($1-prev) > 3000000000) bad=1; prev=$1} END {exit !(n >= 2 && !bad)}' \
        "${RESULT_DIR}/monitor/${phase}.coverage.tsv"; then
        danger=1
        printf 'coverage_gap=1\n' >>"${RESULT_DIR}/monitor/${phase}.flags"
    fi
    if (( danger )); then
        printf 'cancelled=1\n' >>"${RESULT_DIR}/monitor/${phase}.flags"
    fi
    printf 'query_rc=%s\n' "$rc" >>"${RESULT_DIR}/monitor/${phase}.flags"
    return "$rc"
}

validate_query_result() {
    local phase="$1" file values threshold count
    file="${RESULT_DIR}/execution/${phase}.out"
    values="$(awk '/^[0-9]+$/ {print; n++; if (n == 2) exit}' "$file" 2>/dev/null || true)"
    threshold="$(sed -n '1p' <<<"$values")"
    count="$(sed -n '2p' <<<"$values")"
    printf 'phase=%s\nobserved_join_spill_mem=%s\nobserved_count=%s\nexpected_count=132096\n' \
        "$phase" "${threshold:-missing}" "${count:-missing}" >"${RESULT_DIR}/manifest.${phase}_result"
    if [[ "$threshold" != 1000 || "$count" != 132096 ]]; then
        printf 'result_mismatch=1\n' >>"${RESULT_DIR}/monitor/${phase}.flags"
        return 1
    fi
}

execute_phase() {
    local phase="$1" sql_file="$2" attempt tag route_label rc combined_rc=0
    : >"${RESULT_DIR}/execution/${phase}.out"
    : >"${RESULT_DIR}/monitor/${phase}.flags"
    : >"${RESULT_DIR}/manifest.${phase}_result"
    for attempt in 1 2; do
        tag="${phase}.attempt${attempt}"
        route_label="cn${attempt}"
        set +e
        monitor_query "$tag" "$sql_file" "$route_label"
        rc=$?
        set -e
        validate_query_result "$tag" || true
        printf '=== MO_25782_ATTEMPT %s ===\n' "$attempt" >>"${RESULT_DIR}/execution/${phase}.out"
        cat "${RESULT_DIR}/execution/${tag}.out" >>"${RESULT_DIR}/execution/${phase}.out"
        cat "${RESULT_DIR}/monitor/${tag}.flags" >>"${RESULT_DIR}/monitor/${phase}.flags"
        printf 'attempt=%s\nroute_label=%s\nquery_rc=%s\n' "$attempt" "$route_label" "$rc" >>"${RESULT_DIR}/manifest.${phase}_result"
        (( rc == 0 )) || combined_rc="$rc"
    done
    return "$combined_rc"
}

classify_phase() {
    local phase="$1" query_rc="$2"
    "${HARNESS_DIR}/classifier.sh" "$phase" \
        "${RESULT_DIR}/evidence/operators.${phase}.tsv" \
        "${RESULT_DIR}/monitor/${phase}.flags" "$query_rc" "$ALLOWED_CNS" \
        "${RESULT_DIR}/evidence/operators.${phase}.provenance.tsv" \
        "${RESULT_DIR}/evidence/operators.${phase}.complete" "$RUN_ID" "$MO_25782_RUNTIME" \
        "${RESULT_DIR}/evidence/source.${phase}.out"
}

# Keep the target and positive-control executions adjacent to their own stats
# patch; a later patch must never alter the target query's runtime semantics.
configure_route_labels
if ! run_sql setup "${HARNESS_DIR}/sql/01_setup.sql"; then
    printf 'classification=INCONCLUSIVE\nreason=setup_process_exit\n' >"${RESULT_DIR}/classification.overall"
    exit 1
fi
sync_plan_stats broadcast "${HARNESS_DIR}/sql/02_broadcast_plan.sql"
if ! run_sql broadcast_plan "${HARNESS_DIR}/sql/02_broadcast_plan.sql"; then
    printf 'classification=INCONCLUSIVE\nreason=plan_process_exit\n' >"${RESULT_DIR}/classification.overall"
    exit 1
fi
if ! machine_plan_gate broadcast "${RESULT_DIR}/execution/broadcast_plan.out"; then
    printf 'classification=INCONCLUSIVE\nreason=plan_gate_failed\n' >"${RESULT_DIR}/classification.overall"
    exit 1
fi
set +e
execute_phase broadcast "${HARNESS_DIR}/sql/04_broadcast_exec.sql"
broadcast_rc=$?
set -e
collect_evidence broadcast "$broadcast_rc" || true
broadcast_class="$(classify_phase broadcast "$broadcast_rc")"
printf '%s\n' "$broadcast_class" >"${RESULT_DIR}/classification.broadcast"

sync_plan_stats shuffle "${HARNESS_DIR}/sql/03_shuffle_plan.sql"
if ! run_sql shuffle_plan "${HARNESS_DIR}/sql/03_shuffle_plan.sql"; then
    printf 'classification=INCONCLUSIVE\nreason=plan_process_exit\n' >"${RESULT_DIR}/classification.overall"
    exit 1
fi
if ! machine_plan_gate shuffle "${RESULT_DIR}/execution/shuffle_plan.out"; then
    printf 'classification=INCONCLUSIVE\nreason=plan_gate_failed\n' >"${RESULT_DIR}/classification.overall"
    exit 1
fi
set +e
execute_phase shuffle "${HARNESS_DIR}/sql/05_shuffle_exec.sql"
shuffle_rc=$?
set -e
collect_evidence shuffle "$shuffle_rc" || true
shuffle_class="$(classify_phase shuffle "$shuffle_rc")"
printf '%s\n' "$shuffle_class" >"${RESULT_DIR}/classification.shuffle"

if (( KEEP == 0 )); then
    run_sql cleanup "${HARNESS_DIR}/sql/99_cleanup.sql" || true
fi

printf 'result_dir=%s\nbroadcast_rc=%s\nshuffle_rc=%s\n' "$RESULT_DIR" "$broadcast_rc" "$shuffle_rc"
broadcast_result="$(sed -n 's/^classification=//p' "${RESULT_DIR}/classification.broadcast")"
shuffle_result="$(sed -n 's/^classification=//p' "${RESULT_DIR}/classification.shuffle")"
overall=INCONCLUSIVE
if [[ "$broadcast_result" == REPRODUCED && "$shuffle_result" == REPRODUCED ]]; then
	if [[ "$ACCEPTANCE_MODE" == fixed ]]; then
		overall=FIXED_ACCEPTED
	else
		overall=REPRODUCED
	fi
elif [[ "$broadcast_result" == NOT_REPRODUCED && "$shuffle_result" == REPRODUCED ]]; then
    overall=NOT_REPRODUCED
fi
printf 'classification=%s\n' "$overall" >"${RESULT_DIR}/classification.overall"
cat "${RESULT_DIR}/classification.overall" "${RESULT_DIR}/classification.broadcast" "${RESULT_DIR}/classification.shuffle"
