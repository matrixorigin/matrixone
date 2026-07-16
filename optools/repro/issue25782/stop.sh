#!/usr/bin/env bash
set -euo pipefail
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)/lib.sh"

runtime_arg="${MO_25782_RUNTIME:-}"
timeout_s="${MO_25782_STOP_TIMEOUT:-30}"
while (($#)); do
    case "$1" in
        --runtime) (($# >= 2)) || die "--runtime requires an argument"; runtime_arg="$2"; shift 2 ;;
        --timeout) (($# >= 2)) || die "--timeout requires an argument"; timeout_s="$2"; shift 2 ;;
        -h|--help) printf 'usage: %s [--runtime ABSOLUTE_PATH] [--timeout SECONDS]\n' "$0"; exit 0 ;;
        *) die "unknown argument: $1" ;;
    esac
done
load_runtime "${runtime_arg}"
[[ "${timeout_s}" =~ ^[0-9]+$ ]] || die "timeout must be an integer"

# Rewrite a manifest in place while retaining every line which is not one of
# the keys being updated.  In particular, do not use manifest_write here: the
# stop manifest is the audit record for the whole run, including the original
# process and cgroup identity.
manifest_stop_write() {
    local name="$1"; shift
    local dst="${MO_25782_RUNTIME}/manifest/${name}.manifest"
    local tmp="${dst}.tmp.$$"
    local kv key line
    declare -A update_keys=()
    for kv in "$@"; do
        key="${kv%%=*}"
        [[ "${key}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || die "invalid manifest key: ${key}"
        update_keys["${key}"]=1
    done
    [[ -f "${dst}" && ! -L "${dst}" ]] || die "missing manifest for ${name}"
    umask 077
    {
        while IFS= read -r line || [[ -n "${line}" ]]; do
            key="${line%%=*}"
            if [[ "${line}" == *=* && ${update_keys["${key}"]+present} ]]; then
                continue
            fi
            printf '%s\n' "${line}"
        done <"${dst}"
        printf '%s\n' "$@"
    } >"${tmp}"
    chmod 0600 -- "${tmp}"
    mv -f -- "${tmp}" "${dst}"
}

# systemctl --value intentionally returns an empty string for a property that
# is unavailable.  Keep that distinction until the manifest is written, where
# it is represented explicitly as "missing".
show_prop() {
    local unit="$1" property="$2"
    systemctl --user show "${unit}" -p "${property}" --value 2>/dev/null || true
}

read_unit_props() {
    local unit="$1"
    SYS_ACTIVE="$(show_prop "${unit}" ActiveState)"
    SYS_INVOCATION="$(show_prop "${unit}" InvocationID)"
    SYS_CGROUP="$(show_prop "${unit}" ControlGroup)"
    SYS_MAINPID="$(show_prop "${unit}" MainPID)"
    SYS_EXEC_START="$(show_prop "${unit}" ExecMainStartTimestampMonotonic)"
    SYS_NRESTARTS="$(show_prop "${unit}" NRestarts)"
}

cg_value() {
    local file="$1"
    if [[ -r "${file}" ]]; then
        cat -- "${file}" 2>/dev/null || true
    else
        printf 'missing\n'
    fi
}

cg_event_value() {
    local file="$1" event="$2" value
    if [[ ! -r "${file}" ]]; then
        printf 'missing\n'
        return 0
    fi
    value="$(awk -v event="${event}" '$1 == event {print $2; found=1; exit} END {if (!found) print "missing"}' "${file}" 2>/dev/null || true)"
    [[ -n "${value}" ]] && printf '%s\n' "${value}" || printf 'missing\n'
}

read_cgroup_telemetry() {
    local cg="${1:-}" dir=""
    CG_MEMORY_CURRENT="missing"
    CG_MEMORY_PEAK="missing"
    CG_MEMORY_MAX="missing"
    CG_SWAP_CURRENT="missing"
    CG_SWAP_MAX="missing"
    CG_OOM="missing"
    CG_OOM_KILL="missing"
    # ControlGroup is supplied by systemd.  Reject malformed paths rather than
    # allowing a manifest/systemctl value to escape the cgroup mount.
    if [[ -n "${cg}" && "${cg}" = /* && "${cg}" != *..* ]]; then
        dir="/sys/fs/cgroup${cg}"
        if [[ -d "${dir}" ]]; then
            CG_MEMORY_CURRENT="$(cg_value "${dir}/memory.current")"
            CG_MEMORY_PEAK="$(cg_value "${dir}/memory.peak")"
            CG_MEMORY_MAX="$(cg_value "${dir}/memory.max")"
            CG_SWAP_CURRENT="$(cg_value "${dir}/memory.swap.current")"
            CG_SWAP_MAX="$(cg_value "${dir}/memory.swap.max")"
            CG_OOM="$(cg_event_value "${dir}/memory.events" oom)"
            CG_OOM_KILL="$(cg_event_value "${dir}/memory.events" oom_kill)"
        fi
    fi
}

manifest_value() {
    # Return the first non-empty value among the supplied shell variable names.
    local var value
    for var in "$@"; do
        value="${!var-}"
        if [[ -n "${value}" ]]; then
            printf '%s\n' "${value}"
            return 0
        fi
    done
    printf '\n'
}

q() { printf '%q' "$1"; }

verify_signal_identity() {
    local name="$1" unit="$2"
    local expected_pid expected_start expected_exe expected_cmd expected_cg expected_invocation expected_exec expected_restarts
    expected_pid="$(manifest_value MainPID MAIN_PID)"
    expected_start="${PROC_STARTTIME:-}"
    expected_exe="${EXE:-}"
    expected_cmd="${CMDLINE:-}"
    expected_cg="$(manifest_value CGROUP CONTROL_GROUP ControlGroup)"
    expected_invocation="$(manifest_value INVOCATION_ID InvocationID)"
    expected_exec="$(manifest_value EXEC_MAIN_START_MONOTONIC EXEC_MAIN_START_TIMESTAMP_MONOTONIC ExecMainStartTimestampMonotonic)"
    expected_restarts="$(manifest_value NRESTARTS_BASE NRESTARTS)"

    # These properties establish the exact service generation before either a
    # stop request or a kill request is sent.  NRestarts is checked when the
    # start manifest recorded it; unlike the generation fields it is optional
    # for older manifests, but an unavailable recorded value is unsafe.
    [[ -n "${SYS_INVOCATION}" ]] || die "${name}: missing systemd InvocationID; refusing to signal ${unit}"
    [[ -n "${SYS_CGROUP}" ]] || die "${name}: missing systemd ControlGroup; refusing to signal ${unit}"
    [[ "${SYS_MAINPID}" =~ ^[1-9][0-9]*$ ]] || die "${name}: missing systemd MainPID; refusing to signal ${unit}"
    [[ "${SYS_EXEC_START}" =~ ^[0-9]+$ ]] || die "${name}: missing systemd ExecMainStartTimestampMonotonic; refusing to signal ${unit}"
    if [[ -n "${expected_invocation}" && "${SYS_INVOCATION}" != "${expected_invocation}" ]]; then
        die "${name}: InvocationID mismatch; refusing to signal ${unit}"
    fi
    if [[ -n "${expected_cg}" && "${SYS_CGROUP}" != "${expected_cg}" ]]; then
        die "${name}: ControlGroup mismatch; refusing to signal ${unit}"
    fi
    if [[ -n "${expected_exec}" && "${SYS_EXEC_START}" != "${expected_exec}" ]]; then
        die "${name}: ExecMainStartTimestampMonotonic mismatch; refusing to signal ${unit}"
    fi
    if [[ -n "${expected_restarts}" ]]; then
        [[ -n "${SYS_NRESTARTS}" ]] || die "${name}: missing systemd NRestarts; refusing to signal ${unit}"
        [[ "${SYS_NRESTARTS}" = "${expected_restarts}" ]] ||
            die "${name}: NRestarts mismatch; refusing to signal ${unit}"
    fi

    # A unit name alone is not enough protection against a stale manifest.  The
    # process identity captured by start.sh must be complete and unchanged.
    [[ "${expected_pid}" =~ ^[1-9][0-9]*$ && "${expected_pid}" = "${SYS_MAINPID}" ]] ||
        die "${name}: MainPID mismatch; refusing to signal ${unit}"
    [[ -n "${expected_start}" && -n "${expected_exe}" && -n "${expected_cmd}" ]] ||
        die "${name}: incomplete process identity; refusing to signal ${unit}"
    proc_identity_matches "${expected_pid}" "${expected_start}" "${expected_exe}" "${expected_cmd}" ||
        die "${name}: process identity mismatch; refusing to signal ${unit}"
}

append_identity_and_telemetry() {
    local name="$1" unit="$2" state="$3"
    local -a updates=()
    # UNIT is intentionally not rewritten: retaining its original line makes
    # the unit selected by start.sh part of the immutable audit record.
    updates+=("state=${state}" "STOPPED_AT=$(q "$(date -Is)")")
    updates+=("FINAL_ACTIVE_STATE=$(q "${SYS_ACTIVE:-missing}")")
    updates+=("FINAL_INVOCATION_ID=$(q "${SYS_INVOCATION:-missing}")")
    updates+=("FINAL_CONTROL_GROUP=$(q "${SYS_CGROUP:-missing}")")
    updates+=("FINAL_MAINPID=$(q "${SYS_MAINPID:-missing}")")
    updates+=("FINAL_EXEC_MAIN_START_MONOTONIC=$(q "${SYS_EXEC_START:-missing}")")
    updates+=("FINAL_NRESTARTS=$(q "${SYS_NRESTARTS:-missing}")")
    updates+=("CGROUP_MEMORY_CURRENT_PRESTOP=$(q "${PRE_MEMORY_CURRENT:-missing}")")
    updates+=("CGROUP_MEMORY_PEAK_PRESTOP=$(q "${PRE_MEMORY_PEAK:-missing}")")
    updates+=("CGROUP_MEMORY_MAX_PRESTOP=$(q "${PRE_MEMORY_MAX:-missing}")")
    updates+=("CGROUP_SWAP_CURRENT_PRESTOP=$(q "${PRE_SWAP_CURRENT:-missing}")")
    updates+=("CGROUP_SWAP_MAX_PRESTOP=$(q "${PRE_SWAP_MAX:-missing}")")
    updates+=("CGROUP_OOM_PRESTOP=$(q "${PRE_OOM:-missing}")")
    updates+=("CGROUP_OOM_KILL_PRESTOP=$(q "${PRE_OOM_KILL:-missing}")")
    updates+=("CGROUP_MEMORY_CURRENT_FINAL=$(q "${CG_MEMORY_CURRENT:-missing}")")
    updates+=("CGROUP_MEMORY_PEAK_FINAL=$(q "${CG_MEMORY_PEAK:-missing}")")
    updates+=("CGROUP_MEMORY_MAX_FINAL=$(q "${CG_MEMORY_MAX:-missing}")")
    updates+=("CGROUP_SWAP_CURRENT_FINAL=$(q "${CG_SWAP_CURRENT:-missing}")")
    updates+=("CGROUP_SWAP_MAX_FINAL=$(q "${CG_SWAP_MAX:-missing}")")
    updates+=("CGROUP_OOM_FINAL=$(q "${CG_OOM:-missing}")")
    updates+=("CGROUP_OOM_KILL_FINAL=$(q "${CG_OOM_KILL:-missing}")")
    manifest_stop_write "${name}" "${updates[@]}"
}

stop_one() {
    local name="$1" unit active elapsed=0 final_cg
    # Avoid values from the previous unit when a manifest omits an optional
    # property.  The manifests are shell assignments generated by start.sh.
    unset UNIT MainPID MAIN_PID PROC_STARTTIME EXE CMDLINE CGROUP CONTROL_GROUP ControlGroup INVOCATION_ID InvocationID \
        EXEC_MAIN_START_MONOTONIC EXEC_MAIN_START_TIMESTAMP_MONOTONIC ExecMainStartTimestampMonotonic \
        NRESTARTS_BASE NRESTARTS || true
    manifest_load "${name}" || return 0
    unit="${UNIT:-}"
    [[ -n "${unit}" ]] || return 0

    read_unit_props "${unit}"
    active="${SYS_ACTIVE}"
    final_cg="${SYS_CGROUP:-${CGROUP:-}}"
    read_cgroup_telemetry "${final_cg}"
    PRE_MEMORY_CURRENT="${CG_MEMORY_CURRENT}"
    PRE_MEMORY_PEAK="${CG_MEMORY_PEAK}"
    PRE_MEMORY_MAX="${CG_MEMORY_MAX}"
    PRE_SWAP_CURRENT="${CG_SWAP_CURRENT}"
    PRE_SWAP_MAX="${CG_SWAP_MAX}"
    PRE_OOM="${CG_OOM}"
    PRE_OOM_KILL="${CG_OOM_KILL}"

    if [[ "${active}" != active && "${active}" != activating && "${active}" != deactivating ]]; then
        # Even an already-inactive unit gets an audit record.  No signal is
        # sent, so there is no generation identity requirement in this branch.
        read_unit_props "${unit}"
        read_cgroup_telemetry "${final_cg}"
        append_identity_and_telemetry "${name}" "${unit}" stopped
        return 0
    fi

    verify_signal_identity "${name}" "${unit}"
    systemctl --user stop "${unit}" --no-block >/dev/null 2>&1 || true

    # systemctl stop is asynchronous and already delivers the unit's normal
    # stop signal.  Give it the full grace period before any explicit signal;
    # during deactivation MainPID may legitimately become zero.
    while ((elapsed < timeout_s)); do
        active="$(show_prop "${unit}" ActiveState)"
        [[ "${active}" = inactive || "${active}" = failed || -z "${active}" ]] && break
        sleep 1
        elapsed=$((elapsed + 1))
    done
    active="$(show_prop "${unit}" ActiveState)"
    if [[ "${active}" = active || "${active}" = activating || "${active}" = deactivating ]]; then
        log_msg "${name}: graceful stop timed out; signaling exact unit ${unit} with TERM"
        read_unit_props "${unit}"
        verify_signal_identity "${name}" "${unit}"
        systemctl --user kill --kill-who=all --signal=TERM "${unit}" >/dev/null 2>&1 || true
        elapsed=0
        while ((elapsed < timeout_s)); do
            active="$(show_prop "${unit}" ActiveState)"
            [[ "${active}" = inactive || "${active}" = failed || -z "${active}" ]] && break
            sleep 1
            elapsed=$((elapsed + 1))
        done
    fi
    active="$(show_prop "${unit}" ActiveState)"
    if [[ "${active}" = active || "${active}" = activating || "${active}" = deactivating ]]; then
        log_msg "${name}: TERM timed out; escalating exact unit ${unit} to KILL"
        read_unit_props "${unit}"
        verify_signal_identity "${name}" "${unit}"
        systemctl --user kill --kill-who=all --signal=KILL "${unit}" >/dev/null 2>&1 || true
        elapsed=0
        while ((elapsed < timeout_s)); do
            active="$(show_prop "${unit}" ActiveState)"
            [[ "${active}" = inactive || "${active}" = failed || -z "${active}" ]] && break
            sleep 1
            elapsed=$((elapsed + 1))
        done
    fi
    active="$(show_prop "${unit}" ActiveState)"
    [[ "${active}" != active && "${active}" != activating && "${active}" != deactivating ]] ||
        die "${name}: unit ${unit} did not stop"

    # The cgroup often disappears as soon as the unit becomes inactive.  Keep
    # both pre-stop readings and explicit final "missing" values in that case.
    read_unit_props "${unit}"
    read_cgroup_telemetry "${final_cg}"
    append_identity_and_telemetry "${name}" "${unit}" stopped
}

for name in proxy cn2 cn1 tn log; do
    stop_one "${name}"
done
printf 'state=stopped\n' >"${MO_25782_RUNTIME}/start.state"
chmod 0600 -- "${MO_25782_RUNTIME}/start.state"
printf 'runtime=%s\nstate=stopped\n' "${MO_25782_RUNTIME}"
