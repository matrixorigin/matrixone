#!/usr/bin/env bash
set -euo pipefail
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)/lib.sh"

runtime_arg="${MO_25782_RUNTIME:-}"
evidence_phase=""
evidence_dir=""
while (($#)); do
    case "$1" in
        --runtime) (($# >= 2)) || die "--runtime requires an argument"; runtime_arg="$2"; shift 2 ;;
        --evidence) (($# >= 3)) || die "--evidence requires PHASE OUTDIR"; evidence_phase="$2"; evidence_dir="$3"; shift 3 ;;
        -h|--help) printf 'usage: %s [--runtime ABSOLUTE_PATH] [--evidence PHASE OUTDIR]\n' "$0"; exit 0 ;;
        *) die "unknown argument: $1" ;;
    esac
done
load_runtime "${runtime_arg}"

port_state() {
    local port="$1"
    if port_in_use "${port}" 2>/dev/null; then printf 'LISTEN'; else printf 'closed'; fi
}

show_one() {
    local name="$1" pid unit cg rss active state identity
    unset UNIT MainPID PROC_STARTTIME EXE CMDLINE CGROUP state || true
    if ! manifest_load "${name}" 2>/dev/null; then
        printf '%-6s state=absent\n' "${name}"
        return 0
    fi
    unit="${UNIT:-unknown}"
    pid="${MainPID:-0}"
    cg="${CGROUP:-unknown}"
    state="${state:-unknown}"
    active="$(systemctl --user show "${unit}" -p ActiveState --value 2>/dev/null || printf 'unknown')"
    rss="0"
    identity="unverified"
    if [[ "${pid}" =~ ^[1-9][0-9]*$ && -r "/proc/${pid}/status" ]]; then
        rss="$(awk '/^VmRSS:/ {print $2 "KiB"}' "/proc/${pid}/status")"
        rss="${rss:-0}"
        if [[ -n "${PROC_STARTTIME:-}" && -n "${EXE:-}" && -n "${CMDLINE:-}" ]] &&
            proc_identity_matches "${pid}" "${PROC_STARTTIME}" "${EXE}" "${CMDLINE}"; then
            identity="verified"
        else
            identity="MISMATCH"
        fi
    fi
    printf '%-6s state=%s active=%s PID=%s unit=%s cgroup=%s RSS=%s identity=%s\n' \
        "${name}" "${state}" "${active}" "${pid}" "${unit}" "${cg}" "${rss}" "${identity}"
}

if [[ -n "${evidence_phase}" ]]; then
    [[ "${evidence_dir}" = /* ]] || die "evidence output directory must be absolute"
    mkdir -p -- "${evidence_dir}"
    chmod 0700 -- "${evidence_dir}"
    out="${evidence_dir}/operators.${evidence_phase}.tsv"
    {
        printf 'instance_id\tcn_id\tinput_rows\tis_shuffle\tspill_rows\tspill_size\n'
    } >"${out}"
    chmod 0600 -- "${out}"
    printf '%s\n' "${out}"
    exit 0
fi

printf 'runtime=%s state=%s cgroup_verifiable=%s repro_allowed=%s\n' \
    "${MO_25782_RUNTIME}" "$(awk -F= '/^state=/{print $2}' "${MO_25782_RUNTIME}/start.state" 2>/dev/null || printf 'prepared')" \
    "${CGROUP_VERIFIABLE:-0}" "${REPRO_ALLOWED:-0}"
for name in log tn cn1 cn2 proxy; do show_one "${name}"; done
printf 'ports:'
for p in ${PORTS_ALL}; do printf ' %s=%s' "${p}" "$(port_state "${p}")"; done
printf '\n'
