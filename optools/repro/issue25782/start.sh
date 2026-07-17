#!/usr/bin/env bash
set -euo pipefail
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)/lib.sh"

runtime_arg="${MO_25782_RUNTIME:-}"
while (($#)); do
    case "$1" in
        --runtime) (($# >= 2)) || die "--runtime requires an argument"; runtime_arg="$2"; shift 2 ;;
        -h|--help) printf 'usage: %s [--runtime ABSOLUTE_PATH]\n' "$0"; exit 0 ;;
        *) die "unknown argument: $1" ;;
    esac
done
load_runtime "${runtime_arg}"
[[ -x "${BINARY:-}" && "${BINARY}" = /* && ! -L "${BINARY}" ]] || die "BINARY must be an executable absolute non-symlink path"
binary_real="$(readlink -e -- "${BINARY}")"
readonly EXPECTED_COMMIT="${MO_25782_EXPECTED_COMMIT:-$(git -C "${REPO_ROOT}" rev-parse HEAD)}"
[[ "${EXPECTED_COMMIT}" =~ ^[0-9a-f]{9,40}$ ]] || die "MO_25782_EXPECTED_COMMIT must be a 9-40 digit lowercase git commit prefix"
command -v sha256sum >/dev/null 2>&1 || die "sha256sum is required"
binary_sha256="$(sha256sum -- "${binary_real}" | awk '{print $1}')"
[[ "${binary_sha256}" =~ ^[0-9a-f]{64}$ ]] || die "cannot hash BINARY"
if ! binary_version_output="$("${binary_real}" -version 2>&1)"; then
    die "BINARY -version failed"
fi
binary_commit="$(awk -F':[[:space:]]*' '/^[[:space:]]*Git commit ID:/ {print $2; exit}' <<<"${binary_version_output}")"
[[ "${binary_commit}" == "${EXPECTED_COMMIT}"* ]] ||
    die "BINARY commit mismatch: expected ${EXPECTED_COMMIT}, got ${binary_commit:-missing}"
printf '%s\n' "${binary_version_output}" >"${MO_25782_RUNTIME}/manifest/binary.version"
chmod 0400 -- "${MO_25782_RUNTIME}/manifest/binary.version"
command -v systemd-run >/dev/null 2>&1 || die "systemd-run is required"
command -v systemctl >/dev/null 2>&1 || die "systemctl is required"
systemctl --user show-environment >/dev/null 2>&1 || die "user systemd manager is unavailable"
precheck_ports "${PORTS_ALL}"
if [[ "${CGROUP_VERIFIABLE:-0}" = 1 ]]; then
    preflight_growth_gate >/dev/null
else
    log_msg "cgroup memory controller cannot be verified; starting smoke-only harness"
fi

run_id="$(date +%Y%m%d%H%M%S)-$$"
started_names=()
cleanup_started() {
    local i name unit
    for ((i=${#started_names[@]}-1; i>=0; i--)); do
        name="${started_names[i]}"
        if manifest_load "${name}" 2>/dev/null && [[ -n "${UNIT:-}" ]]; then
            unit="${UNIT}"
            systemctl --user stop "${unit}" >/dev/null 2>&1 || true
        fi
    done
}
trap 'rc=$?; if ((rc != 0)); then log_msg "start failed; cleaning started units in reverse order"; cleanup_started; fi; exit "$rc"' EXIT

wait_unit_pid() {
    local name="$1" timeout_s="${2:-30}" elapsed=0 pid active
    while ((elapsed < timeout_s)); do
        pid="$(systemctl --user show "${UNIT}" -p MainPID --value 2>/dev/null || true)"
        active="$(systemctl --user show "${UNIT}" -p ActiveState --value 2>/dev/null || true)"
        if [[ "${pid}" =~ ^[1-9][0-9]*$ ]] && [[ "${active}" = active ]] && [[ -r "/proc/${pid}/exe" ]]; then
            printf '%s\n' "${pid}"
            return 0
        fi
        [[ "${active}" = failed || "${active}" = inactive ]] && return 1
        sleep 1
        elapsed=$((elapsed + 1))
    done
    log_msg "${name} startup timed out; diagnostics follow"
    systemctl --user status --no-pager "${UNIT}" >&2 || true
    return 1
}

wait_port() {
    local name="$1" port="$2" timeout_s="${3:-60}" elapsed=0 rc
    while ((elapsed < timeout_s)); do
        if port_in_use "${port}" 2>/dev/null; then return 0; fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    log_msg "${name} port ${port} readiness timed out"
    ss -H -ltn 2>/dev/null || true
    return 1
}

start_one() {
    local name="$1" memory="$2" config="$3" debug_port="$4"
    local unit="mo-25782-${name}-${run_id}.service" log_file="${MO_25782_RUNTIME}/logs/${name}.log"
    local pid expected_start exe running_sha256 cmdline cg invocation_id exec_start nrestarts
    local cg_current cg_peak cg_max cg_swap_current cg_swap_max cg_oom cg_oom_kill
    [[ ! -f "${MO_25782_RUNTIME}/manifest/${name}.manifest" ]] || die "manifest exists for ${name}; runtime reuse refused"
    : >"${log_file}"; chmod 0600 -- "${log_file}"
    manifest_write "${name}" "state=planned" "UNIT=$(printf '%q' "${unit}")" \
        "MEMORY_MAX=$(printf '%q' "${memory}")" "CONFIG=$(printf '%q' "${config}")" \
        "EXE=$(printf '%q' "${binary_real}")" "BINARY_SHA256=${binary_sha256}" \
        "BINARY_COMMIT=$(printf '%q' "${binary_commit}")" "LOG_FILE=$(printf '%q' "${log_file}")"
    started_names+=("${name}")
    if ! systemd-run --user --unit="${unit}" --working-directory="${REPO_ROOT}" \
        --property="MemoryMax=${memory}" --property="MemorySwapMax=0" --property="KillMode=control-group" \
        --property="TimeoutStartSec=30" --property="StandardOutput=append:${log_file}" \
        --property="StandardError=append:${log_file}" "${binary_real}" -cfg "${config}" \
        -debug-http "127.0.0.1:${debug_port}"; then
        log_msg "systemd-run failed for ${name}"
        systemctl --user status --no-pager "${unit}" >&2 || true
        return 1
    fi
    UNIT="${unit}"
    pid="$(wait_unit_pid "${name}" 30)" || return 1
    expected_start="$(proc_starttime "${pid}")" || return 1
    exe="$(readlink -e -- "/proc/${pid}/exe")" || return 1
    [[ "${exe}" = "${binary_real}" ]] || die "${name}: running executable path changed"
    running_sha256="$(sha256sum -- "/proc/${pid}/exe" | awk '{print $1}')"
    [[ "${running_sha256}" = "${binary_sha256}" ]] || die "${name}: running executable hash changed"
    cmdline="$(tr '\0' ' ' <"/proc/${pid}/cmdline")"
    cg="$(systemctl --user show "${unit}" -p ControlGroup --value 2>/dev/null || true)"
    invocation_id="$(systemctl --user show "${unit}" -p InvocationID --value 2>/dev/null || true)"
    exec_start="$(systemctl --user show "${unit}" -p ExecMainStartTimestampMonotonic --value 2>/dev/null || true)"
    nrestarts="$(systemctl --user show "${unit}" -p NRestarts --value 2>/dev/null || true)"
    [[ -n "${cg}" ]] || die "${name}: missing cgroup path"
    [[ -n "${invocation_id}" && "${exec_start}" =~ ^[0-9]+$ && "${nrestarts}" =~ ^[0-9]+$ ]] ||
        die "${name}: missing systemd invocation identity"
    if [[ "${CGROUP_VERIFIABLE:-0}" = 1 ]]; then
        [[ -r "/sys/fs/cgroup${cg}/memory.max" && -r "/sys/fs/cgroup${cg}/memory.swap.max" ]] ||
            die "${name}: cannot verify cgroup memory controls"
        cgroup_max="$(cat "/sys/fs/cgroup${cg}/memory.max")"
        cgroup_swap="$(cat "/sys/fs/cgroup${cg}/memory.swap.max")"
        expected_max="$(memory_string_bytes "${memory}")"
        [[ "${cgroup_max}" = "${expected_max}" ]] || die "${name}: memory.max=${cgroup_max}, expected=${expected_max}"
        [[ "${cgroup_swap}" = 0 ]] || die "${name}: memory.swap.max=${cgroup_swap}, expected 0"
    fi
    cg_current="$(cat "/sys/fs/cgroup${cg}/memory.current" 2>/dev/null || true)"
    cg_peak="$(cat "/sys/fs/cgroup${cg}/memory.peak" 2>/dev/null || true)"
    cg_max="$(cat "/sys/fs/cgroup${cg}/memory.max" 2>/dev/null || true)"
    cg_swap_current="$(cat "/sys/fs/cgroup${cg}/memory.swap.current" 2>/dev/null || true)"
    cg_swap_max="$(cat "/sys/fs/cgroup${cg}/memory.swap.max" 2>/dev/null || true)"
    cg_oom="$(awk '$1 == "oom" {print $2}' "/sys/fs/cgroup${cg}/memory.events" 2>/dev/null || true)"
    cg_oom_kill="$(awk '$1 == "oom_kill" {print $2}' "/sys/fs/cgroup${cg}/memory.events" 2>/dev/null || true)"
    for value in "${cg_current}" "${cg_peak}" "${cg_max}" "${cg_swap_current}" "${cg_swap_max}" "${cg_oom}" "${cg_oom_kill}"; do
        [[ "${value}" =~ ^[0-9]+$ ]] || die "${name}: incomplete cgroup telemetry at start"
    done
    manifest_write "${name}" "state=running" "UNIT=$(printf '%q' "${unit}")" "MainPID=${pid}" \
        "PROC_STARTTIME=${expected_start}" "EXE=$(printf '%q' "${exe}")" "CMDLINE=$(printf '%q' "${cmdline}")" \
        "BINARY_SHA256=${binary_sha256}" "BINARY_COMMIT=$(printf '%q' "${binary_commit}")" \
        "MEMORY_MAX=$(printf '%q' "${memory}")" "CONFIG=$(printf '%q' "${config}")" "CGROUP=$(printf '%q' "${cg}")" \
        "LOG_FILE=$(printf '%q' "${log_file}")" "INVOCATION_ID=$(printf '%q' "${invocation_id}")" \
        "EXEC_MAIN_START_MONOTONIC=${exec_start}" "NRESTARTS_BASE=${nrestarts}" \
        "CGROUP_MEMORY_CURRENT_BASE=${cg_current}" "CGROUP_MEMORY_PEAK_BASE=${cg_peak}" \
        "CGROUP_MEMORY_MAX=${cg_max}" "CGROUP_SWAP_CURRENT_BASE=${cg_swap_current}" \
        "CGROUP_SWAP_MAX=${cg_swap_max}" "CGROUP_OOM_BASE=${cg_oom}" "CGROUP_OOM_KILL_BASE=${cg_oom_kill}"
}

start_one log "${MEMORY_LOG}" "${LOG_CONFIG}" 6061
manifest_load log; wait_port log 32001 90
start_one tn "${MEMORY_TN}" "${TN_CONFIG}" 6062
manifest_load tn; wait_port tn 19000 120
start_one cn1 "${MEMORY_CN1}" "${CN1_CONFIG}" 6063
manifest_load cn1; wait_port cn1 16001 180
start_one cn2 "${MEMORY_CN2}" "${CN2_CONFIG}" 6064
manifest_load cn2; wait_port cn2 16002 180

if [[ "${SMOKE_ONLY:-0}" = 1 ]]; then
    printf 'state=smoke-only\n' >"${MO_25782_RUNTIME}/start.state"
    chmod 0600 -- "${MO_25782_RUNTIME}/start.state"
    trap - EXIT
    printf 'runtime=%s\nstate=smoke-only\n' "${MO_25782_RUNTIME}"
    exit 0
fi

preflight_manifest_growth_gate >/dev/null

readiness_log="${MO_25782_RUNTIME}/logs/readiness.log"
metadata_ok=0
for ((metadata_try=0; metadata_try<180; metadata_try++)); do
    set +e
    cn1_select="$(mysql_exec 127.0.0.1 16001 'SELECT 1;' 2>&1)"; rc1=$?
    cn2_select="$(mysql_exec 127.0.0.1 16002 'SELECT 1;' 2>&1)"; rc2=$?
    replicas="$(mysql_exec 127.0.0.1 16001 'SHOW LOGSERVICE REPLICAS;' 2>&1)"; rc3=$?
    backends="$(mysql_exec 127.0.0.1 16001 'SHOW BACKEND SERVERS;' 2>&1)"; rc4=$?
    set -e
    {
        printf 'attempt=%s\n' "${metadata_try}"
        printf '%s\n%s\n%s\n%s\n' "${cn1_select}" "${cn2_select}" "${replicas}" "${backends}"
    } >"${readiness_log}"
    if ((rc1 == 0 && rc2 == 0 && rc3 == 0 && rc4 == 0)) &&
        grep -Fqi "${LOG_UUID}" <<<"${replicas}" &&
        grep -Fqi "${CN1_UUID}" <<<"${backends}" &&
        grep -Fqi "${CN2_UUID}" <<<"${backends}"; then
        metadata_ok=1
        break
    fi
    sleep 1
done
((metadata_ok == 1)) || { log_msg "CN SQL/HAKeeper metadata readiness timed out; see ${readiness_log}"; exit 1; }
mysql_exec 127.0.0.1 16001 'CREATE DATABASE IF NOT EXISTS issue25782_harness; CREATE TABLE IF NOT EXISTS issue25782_harness.lifecycle_probe (id INT PRIMARY KEY, v VARCHAR(32)); DELETE FROM issue25782_harness.lifecycle_probe; INSERT INTO issue25782_harness.lifecycle_probe VALUES (1, "ready"); SELECT COUNT(*) FROM issue25782_harness.lifecycle_probe;' >>"${readiness_log}" 2>&1 || { log_msg "DDL/DML readiness failed; see ${readiness_log}"; exit 1; }

start_one proxy "${MEMORY_PROXY}" "${PROXY_CONFIG}" 6065
manifest_load proxy; wait_port proxy 6001 120
mysql_proxy_exec 'SELECT 1;' >>"${readiness_log}" 2>&1 || { log_msg "Proxy SQL readiness failed; see ${readiness_log}"; exit 1; }
printf 'state=repro-ready\n' >"${MO_25782_RUNTIME}/start.state"
chmod 0600 -- "${MO_25782_RUNTIME}/start.state"
trap - EXIT
printf 'runtime=%s\nstate=repro-ready\n' "${MO_25782_RUNTIME}"
