#!/usr/bin/env bash

# Run a controlled reproduction experiment for issue #26522. Keep the focused
# test in a fresh Go test process on every attempt: TestTableFeatures owns a
# package-global embedded cluster, so `go test -count` would not restart it.
set -euo pipefail

readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly MODE="${1:-}"
readonly FEATURE_ATTEMPTS="${2:-12}"
readonly ARTIFACT_DIR="${PROBE_ARTIFACT_DIR:-${ROOT_DIR}/issue-26522-probe-artifacts}"
readonly TAGS="matrixone_test"
readonly GO_MODULE_MODE="-mod=readonly"
readonly TEST_TIMEOUT="${UT_TIMEOUT:-40}"

if [[ "${MODE}" != "all" ]]; then
    echo "usage: $0 all [feature-attempts]" >&2
    exit 2
fi

if ! [[ "${FEATURE_ATTEMPTS}" =~ ^[1-9][0-9]*$ ]]; then
    echo "feature-attempts must be a positive integer, got '${FEATURE_ATTEMPTS}'" >&2
    exit 2
fi

cd "${ROOT_DIR}"
rm -rf "${ARTIFACT_DIR}"
mkdir -p "${ARTIFACT_DIR}/features"

readonly THIRDPARTIES_INSTALL_DIR="${ROOT_DIR}/thirdparties/install"
export GOWORK=off
export CGO_CFLAGS="-I${ROOT_DIR}/cgo -I${THIRDPARTIES_INSTALL_DIR}/include"
export CGO_LDFLAGS="-Wl,-rpath,${THIRDPARTIES_INSTALL_DIR}/lib:${ROOT_DIR}/cgo -L${THIRDPARTIES_INSTALL_DIR}/lib -L${ROOT_DIR}/cgo -lmo -lusearch_c -lm"
export LD_LIBRARY_PATH="${THIRDPARTIES_INSTALL_DIR}/lib:${ROOT_DIR}/cgo${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"

record_signatures() {
    local log_path="$1"
    local label="$2"
    local summary_path="${ARTIFACT_DIR}/signature-summary.txt"

    {
        echo "[${label}]"
        printf 'LocalTick backlog: '
        grep -Ec 'had [0-9]+ LocalTick msgs' "${log_path}" || true
        printf 'waitAnyShardReadyLocked: '
        grep -Ec 'waitAnyShardReadyLocked' "${log_path}" || true
        printf 'HAKeeper timeout: '
        grep -Eic '(hakeeper|HAKeeper).*(proposal|checker|heartbeat).*(timeout|deadline)|(proposal|checker|heartbeat).*(timeout|deadline).*(hakeeper|HAKeeper)' "${log_path}" || true
        printf 'connection reset by peer: '
        grep -Ec 'connection reset by peer' "${log_path}" || true
        echo
    } | tee -a "${summary_path}"
}

run_capture() {
    local label="$1"
    local log_path="$2"
    shift 2
    local started_at elapsed status

    started_at=$(date +%s)
    echo "==> ${label}"
    set +e
    "$@" >"${log_path}" 2>&1
    status=$?
    set -e
    elapsed=$(( $(date +%s) - started_at ))
    printf '%s: exit=%d duration=%ss\n' "${label}" "${status}" "${elapsed}" | tee -a "${ARTIFACT_DIR}/durations.txt"
    record_signatures "${log_path}" "${label}"

    if (( status != 0 )); then
        echo "${label} failed; last 200 log lines:"
        tail -n 200 "${log_path}"
    fi
    return "${status}"
}

prepare_native_dependencies() {
    echo "==> Preparing the same Go/CGo prerequisites used by UT"
    make clean
    make config
    make cgo
    make thirdparties
}

copy_ut_diagnostics() {
    local destination="${ARTIFACT_DIR}/full-ut"
    mkdir -p "${destination}"

    # Preserve the authoritative Go test JSON and failure diagnostics without
    # uploading an unconstrained scratch workspace from a shared runner.
    find "${ROOT_DIR}/scratch" -maxdepth 1 -type f -name '*-UT-*.out' \
        -exec cp -a {} "${destination}/" \; 2>/dev/null || true
    if [[ -d "${ROOT_DIR}/ut-report/failed" ]]; then
        cp -a "${ROOT_DIR}/ut-report/failed" "${destination}/"
    fi
    find "${ROOT_DIR}/ut-report" -maxdepth 1 -type f \
        \( -name 'top.txt' -o -name 'skipped.txt' -o -name 'no-test.txt' \) \
        -exec cp -a {} "${destination}/" \; 2>/dev/null || true
}

write_github_summary() {
    if [[ -z "${GITHUB_STEP_SUMMARY:-}" ]]; then
        return 0
    fi

    {
        echo '## Issue #26522 UT probe'
        echo
        echo '```text'
        [[ -f "${ARTIFACT_DIR}/durations.txt" ]] && cat "${ARTIFACT_DIR}/durations.txt"
        [[ -f "${ARTIFACT_DIR}/signature-summary.txt" ]] && cat "${ARTIFACT_DIR}/signature-summary.txt"
        echo '```'
    } >> "${GITHUB_STEP_SUMMARY}"
}

run_isolated_features() {
    local attempt log_path

    prepare_native_dependencies
    for (( attempt = 1; attempt <= FEATURE_ATTEMPTS; attempt++ )); do
        log_path="${ARTIFACT_DIR}/features/attempt-${attempt}.log"
        # `-count=1` and a new command process make each attempt start and stop
        # its own embedded cluster instead of reusing the package's sync.Once.
        go clean -testcache
        if ! run_capture "features attempt ${attempt}/${FEATURE_ATTEMPTS}" "${log_path}" \
            go test "${GO_MODULE_MODE}" -short -v -json -tags "${TAGS}" \
            -p 1 -count=1 -timeout "${TEST_TIMEOUT}m" -race \
            -run '^TestTableFeatures$' ./pkg/tests/features; then
            echo "Focused reproduction failed; full UT is intentionally not run."
            return 1
        fi
    done
}

run_production_ut() {
    local log_path="${ARTIFACT_DIR}/full-ut.log"

    # Match the production UT entrypoint exactly. It owns the light, exclusive,
    # heavy, and plan-race stages, including their current concurrency policy.
    make clean
    # The focused phase compiles this package. Clear its build output so the
    # production phase does not inherit a warmer Go build cache than normal CI.
    go clean -cache -testcache
    make config
    if ! run_capture "production UT (UT_PARALLEL=6)" "${log_path}" \
        make ut UT_PARALLEL=6 UT_TIMEOUT="${TEST_TIMEOUT}"; then
        copy_ut_diagnostics
        return 1
    fi
    copy_ut_diagnostics
}

{
    echo "Issue #26522 controlled UT probe"
    echo "commit: $(git rev-parse HEAD)"
    echo "feature attempts: ${FEATURE_ATTEMPTS}"
    echo "runner: ${RUNNER_NAME:-unknown}"
    echo
} | tee "${ARTIFACT_DIR}/metadata.txt"

trap write_github_summary EXIT
run_isolated_features
run_production_ut
