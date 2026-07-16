#!/usr/bin/env bash
set -euo pipefail

readonly HARNESS_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
readonly REPO_ROOT="$(cd -- "${HARNESS_DIR}/../../.." && pwd -P)"
readonly BUILD_MEMORY_MAX="${BUILD_MEMORY_MAX:-6G}"
readonly HOST_RESERVE_KIB=$((4 * 1024 * 1024))
readonly BUILD_MAX_KIB=$((6 * 1024 * 1024))

die() {
    printf 'error: %s\n' "$*" >&2
    exit 1
}

[[ "${BUILD_MEMORY_MAX}" == "6G" ]] || die "BUILD_MEMORY_MAX must remain 6G for this safety-reviewed harness"
command -v systemd-run >/dev/null || die "systemd-run is required"
systemctl --user is-system-running >/dev/null || die "the user systemd manager is not running"
[[ -f /sys/fs/cgroup/cgroup.controllers ]] || die "cgroup v2 is required"
grep -qw memory /sys/fs/cgroup/cgroup.controllers || die "the cgroup v2 memory controller is unavailable"

mem_available_kib="$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)"
[[ "${mem_available_kib}" =~ ^[0-9]+$ ]] || die "cannot read MemAvailable"
(( mem_available_kib - BUILD_MAX_KIB >= HOST_RESERVE_KIB )) ||
    die "insufficient build headroom: MemAvailable=${mem_available_kib}KiB, need build cap plus 4GiB reserve"

run_id="$(date +%Y%m%d%H%M%S)-$$"
unit="mo-25782-build-${run_id}.service"
report_dir="${MO_25782_BUILD_REPORT_DIR:-/tmp/mo-25782-build-reports}"
mkdir -p -- "${report_dir}"
chmod 0700 -- "${report_dir}"
report="${report_dir}/${run_id}.log"

printf 'commit=%s\nunit=%s\nmemory_max=%s\nmem_available_before_kib=%s\n' \
    "$(git -C "${REPO_ROOT}" rev-parse HEAD)" "${unit}" "${BUILD_MEMORY_MAX}" "${mem_available_kib}" | tee "${report}"

set +e
systemd-run --user \
    --unit="${unit}" \
    --wait \
    --pipe \
    --collect \
    --working-directory="${REPO_ROOT}" \
    --setenv="PATH=${PATH}" \
    --setenv="HOME=${HOME}" \
    --setenv="GOPATH=$(go env GOPATH)" \
    --setenv="GOCACHE=$(go env GOCACHE)" \
    --property="MemoryMax=${BUILD_MEMORY_MAX}" \
    --property="MemorySwapMax=0" \
    --property="TimeoutStartSec=30min" \
    --property="KillMode=control-group" \
    /usr/bin/make -j2 build 2>&1 | tee -a "${report}"
rc=${PIPESTATUS[0]}
set -e

mem_available_after_kib="$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)"
printf 'exit_code=%s\nmem_available_after_kib=%s\n' "${rc}" "${mem_available_after_kib}" | tee -a "${report}"
(( rc == 0 )) || die "cgroup-isolated build failed; see ${report}"

binary_commit="$(${REPO_ROOT}/mo-service -version 2>&1 | sed -n 's/^  Git commit ID: //p' | head -1)"
source_commit="$(git -C "${REPO_ROOT}" rev-parse HEAD)"
[[ -n "${binary_commit}" ]] || die "cannot extract GitCommit from mo-service -version"
[[ "${source_commit}" == "${binary_commit}"* ]] ||
    die "binary/source mismatch: binary=${binary_commit}, source=${source_commit}"
printf 'binary_commit=%s\nstatus=PASS\nreport=%s\n' "${binary_commit}" "${report}" | tee -a "${report}"
