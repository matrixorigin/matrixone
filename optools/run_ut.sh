#!/bin/bash

# Copyright 2021 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o nounset

if (( $# == 0 )); then
    echo "Usage: $0 TestType SkipTest"
    echo "  TestType: UT|SCA"
    echo "  SkipTest: race"
    exit 1
fi

TEST_TYPE=$1
if [[ $# == 2 ]]; then 
    SKIP_TESTS=$2; 
else
    SKIP_TESTS="";
fi

shopt -s expand_aliases
source ./utilities.sh
source ./ut_tools.bash
source ./ut_process.bash
go version

BUILD_WKSP=$(dirname "$PWD") && cd $BUILD_WKSP


LOG="$G_TS-$TEST_TYPE.log"
UT_RUN_ID=${UT_RUN_ID:-"${G_TS}-${TEST_TYPE}"}
UT_TIMEOUT=${UT_TIMEOUT:-"15"}
UT_HARD_TIMEOUT=${UT_HARD_TIMEOUT:-"70m"}
UT_PARALLEL=${UT_PARALLEL:-"1"}
UT_SHARD=${UT_SHARD:-"all"}
UT_PREBUILD_EMBEDDED=${UT_PREBUILD_EMBEDDED:-"0"}
UT_OVERLAP_PLAN=${UT_OVERLAP_PLAN:-"1"}
# The light package wave does not own embedded-cluster fixtures.  On CI's
# single runner it can therefore overlap the exclusive issues package when
# enabled, but it uses its own conservative package budget so the two waves do
# not recreate the six-way race-test pressure that this scheduler removed.
UT_OVERLAP_LIGHT=${UT_OVERLAP_LIGHT:-"0"}
UT_OVERLAP_LIGHT_PARALLEL=${UT_OVERLAP_LIGHT_PARALLEL:-"2"}
# A helper may own two independent child process groups. Its trap gives each
# child a bounded TERM grace period, so the parent must retain the helper long
# enough for both children to finish before escalating to KILL.
UT_HELPER_TERM_GRACE_TICKS=${UT_HELPER_TERM_GRACE_TICKS:-"60"}
HEAVY_RACE_PARALLEL=${HEAVY_RACE_PARALLEL:-"3"}
PLAN_RACE_SHARDS=${PLAN_RACE_SHARDS:-"8"}
# Two shards cut the measured engine/test race runtime roughly in half while
# keeping the default heavy-stage memory/process budget bounded.
ENGINE_RACE_SHARDS=2
SCA_REPORT="$G_WKSP/$G_TS-SCA-Report.out"
UT_REPORT="$G_WKSP/$G_TS-UT-Report.out"
UT_DIAGNOSTIC_DIR="${BUILD_WKSP}/ut-report"
UT_STDERR="${UT_DIAGNOSTIC_DIR}/ut-stderr.log"
UT_CHECKPOINT="${UT_DIAGNOSTIC_DIR}/ut-checkpoint.log"
UT_FILTER="$G_WKSP/$G_TS-UT-Filter.out"
UT_COUNT="$G_WKSP/$G_TS-UT-Count.out"
CODE_COVERAGE="$G_WKSP/$G_TS-UT-Coverage.html"
RAW_COVERAGE="coverage.out"
IS_BUILD_FAIL=""
UT_TEST_STATUS=0
UT_SHARD_ROUTING_ERROR=0
PLAN_RACE_TEST_BINARY=""
PLAN_RACE_JOB_PID=""
PLAN_RACE_REPORT=""
CLUSTER_PREBUILD_JOB_PID=""
CLUSTER_PREBUILD_REPORT=""
ENGINE_RACE_TEST_BINARY=""
ENGINE_RACE_JOB_PID=""
ENGINE_RACE_REPORT=""
ENGINE_RACE_REPORT_READY=""
LIGHT_RACE_JOB_PID=""
LIGHT_RACE_REPORT=""
CURRENT_UT_PID=""
CURRENT_UT_COMMAND_STAGE=""
CURRENT_UT_COMMAND_LABEL=""
CURRENT_UT_STAGE="initializing"
CURRENT_UT_LABEL="startup"
UT_TERMINATING=0
TAGS="matrixone_test"
GO_MODULE_MODE="-mod=readonly"
# Static analysis owns vet in the separate SCA job. Running it again for every
# UT package duplicates work and increases race-test compile CPU/memory.
GO_TEST_VET_FLAGS="-vet=off"
# CI runs the checked-out MatrixOne module, never a caller's Go workspace.
export GOWORK=off

THIRDPARTIES_INSTALL_DIR=${BUILD_WKSP}/thirdparties/install
CGO_CFLAGS="-I${BUILD_WKSP}/cgo -I${THIRDPARTIES_INSTALL_DIR}/include"
CGO_LDFLAGS="-Wl,-rpath,${THIRDPARTIES_INSTALL_DIR}/lib:${BUILD_WKSP}/cgo -L${THIRDPARTIES_INSTALL_DIR}/lib -L${BUILD_WKSP}/cgo -lmo -lusearch_c -lm"
LD_LIBRARY_PATH="${THIRDPARTIES_INSTALL_DIR}/lib:${BUILD_WKSP}/cgo"

if [[ -n "${MO_CL_CUDA:-}" ]] ; then
    if [[ ${MO_CL_CUDA} == "1" ]] ; then
         if [[ -z "${CONDA_PREFIX:-}" ]] ; then
		 echo "CONDA_PREFIX environment variable not found"
		 exit 1
	 fi

         CUDA_HOME=/usr/local/cuda
         CGO_CFLAGS="${CGO_CFLAGS} -I${CUDA_HOME}/include -I${CONDA_PREFIX}/include"
         CGO_LDFLAGS="${CGO_LDFLAGS} -L${CUDA_HOME}/lib64/stubs -lcuda -L${CUDA_HOME}/lib64 -lcudart -L${CONDA_PREFIX}/lib -lcuvs -lcuvs_c  -lstdc++"
         LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${CUDA_HOME}/lib64:${CUDA_HOME}/extras/CUPTI/lib64:${CONDA_PREFIX}/lib"
	 TAGS="${TAGS},gpu"
    fi
fi

if [[ -f $SCA_REPORT ]]; then rm $SCA_REPORT; fi
if [[ -f $UT_REPORT ]]; then rm $UT_REPORT; fi
if [[ -f $UT_STDERR ]]; then rm $UT_STDERR; fi
if [[ -f $UT_CHECKPOINT ]]; then rm $UT_CHECKPOINT; fi
if [[ -f $UT_FILTER ]]; then rm $UT_FILTER; fi
if [[ -f $UT_COUNT ]]; then rm $UT_COUNT; fi
if ! mkdir -p "${BUILD_WKSP}/ut-report/failed/outputs"; then
    echo "failed to create UT diagnostic directory: ${BUILD_WKSP}/ut-report/failed/outputs" >&2
    exit 1
fi
: > "${UT_REPORT}"
: > "${UT_STDERR}"
: > "${UT_CHECKPOINT}"

function checkpoint_ut_event(){
    local event=$1
    local stage=${2:-${CURRENT_UT_STAGE}}
    local label=${3:-${CURRENT_UT_LABEL}}
    local status=${4:-}
    local detail=${5:-}
    local timestamp

    # Checkpoints are intentionally append-only.  A partially written final
    # line is still more useful after a hard runner kill than a last-state file
    # that was never flushed.
    timestamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    stage=${stage//$'\t'/ }
    label=${label//$'\t'/ }
    status=${status//$'\t'/ }
    detail=${detail//$'\t'/ }
    stage=${stage//$'\n'/ }
    label=${label//$'\n'/ }
    status=${status//$'\n'/ }
    detail=${detail//$'\n'/ }
    printf 'run_id=%s time=%s pid=%s event=%s stage=%s label=%s status=%s detail=%s\n' \
        "${UT_RUN_ID}" "${timestamp}" "$$" "${event}" "${stage}" "${label}" "${status}" "${detail}" \
        >> "${UT_CHECKPOINT}"
}

function mark_ut_stage(){
    local stage=$1
    local label=$2
    local event=${3:-start}
    local status=${4:-}
    local detail=${5:-}
    CURRENT_UT_STAGE=${stage}
    CURRENT_UT_LABEL=${label}
    checkpoint_ut_event "${event}" "${stage}" "${label}" "${status}" "${detail}"
}

# The parent retains the foreground command's PID even while joining another
# helper. Only this command writes UT_REPORT; helper reports are consumed after
# finish_ut_command has reaped it.
function start_ut_command(){
    if (( $# < 3 )) || [[ -n "${CURRENT_UT_PID}" ]]; then
        logger "ERR" "start_ut_command requires stage, label, command and no active writer"
        return 2
    fi
    local stage=$1
    local label=$2
    shift 2
    local saved_term_trap
    local term_pending=0
    CURRENT_UT_COMMAND_STAGE=${stage}
    CURRENT_UT_COMMAND_LABEL=${label}
    mark_ut_stage "${stage}" "${label}" start "" "${*}"
    saved_term_trap=$(trap -p TERM)
    trap 'term_pending=1' TERM
    set -m
    "$@" >> "${UT_REPORT}" 2>> "${UT_STDERR}" &
    CURRENT_UT_PID=$!
    set +m
    restore_ut_term_trap "${saved_term_trap}"
    checkpoint_ut_event "pid-start" "${stage}" "${label}" "" "child_pid=${CURRENT_UT_PID}"
    if (( term_pending != 0 )); then
        handle_ut_termination
    fi
}

function finish_ut_command(){
    local status=0
    wait "${CURRENT_UT_PID}" || status=$?
    CURRENT_UT_PID=""
    mark_ut_stage "${CURRENT_UT_COMMAND_STAGE}" "${CURRENT_UT_COMMAND_LABEL}" finish "${status}"
    return "${status}"
}

function run_ut_command(){
    start_ut_command "$@" || return $?
    finish_ut_command
}

function start_light_race(){
    if (( $# != 2 )); then
        logger "ERR" "start_light_race requires package scope and parallelism"
        return 2
    fi
    if [[ -n "${LIGHT_RACE_JOB_PID}" ]]; then
        logger "ERR" "start_light_race cannot start while another light helper is active"
        return 2
    fi

    local package_scope=$1
    local package_parallel=$2
    local saved_term_trap
    local term_pending=0
    LIGHT_RACE_REPORT="${G_WKSP}/${G_TS}-light-race-report.out"
    : > "${LIGHT_RACE_REPORT}"
    saved_term_trap=$(trap -p TERM)
    trap 'term_pending=1' TERM
    checkpoint_ut_event "start" "light" "light race-test packages" "" \
        "parallel=${package_parallel} background=true"
    set -m
    env LD_LIBRARY_PATH="${LD_LIBRARY_PATH}" \
        CGO_CFLAGS="${CGO_CFLAGS}" \
        CGO_LDFLAGS="${CGO_LDFLAGS}" \
        go test ${GO_MODULE_MODE} ${GO_TEST_VET_FLAGS} -short -v -json \
        -tags "${TAGS}" -p "${package_parallel}" -timeout "${UT_TIMEOUT}m" \
        -race ${package_scope} > "${LIGHT_RACE_REPORT}" 2>> "${UT_STDERR}" &
    LIGHT_RACE_JOB_PID=$!
    set +m
    restore_ut_term_trap "${saved_term_trap}"
    checkpoint_ut_event "pid-start" "light" "light race-test packages" "" \
        "child_pid=${LIGHT_RACE_JOB_PID} parallel=${package_parallel}"
    if (( term_pending != 0 )); then
        handle_ut_termination
    fi
}

function consume_light_race_report(){
    if [[ -z "${LIGHT_RACE_REPORT}" ]]; then
        return 0
    fi

    # The helper is the sole writer until its PID is reaped.  Hold this same
    # boundary while publishing its private JSON so a TERM cannot append a
    # prefix and then cause a second append from the cancellation path.
    local saved_term_trap
    local term_pending=0
    local append_status=0
    saved_term_trap=$(trap -p TERM)
    trap 'term_pending=1' TERM
    append_ut_report "${LIGHT_RACE_REPORT}" "${UT_REPORT}"
    append_status=$?
    if (( append_status != 0 )); then
        logger "ERR" "failed to consume light race report; preserving ${LIGHT_RACE_REPORT}"
        restore_ut_term_trap "${saved_term_trap}"
        if (( term_pending != 0 && UT_TERMINATING == 0 )); then
            handle_ut_termination
        fi
        return "${append_status}"
    fi
    rm -f "${LIGHT_RACE_REPORT}" "${LIGHT_RACE_REPORT}".*
    LIGHT_RACE_REPORT=""
    restore_ut_term_trap "${saved_term_trap}"
    if (( term_pending != 0 && UT_TERMINATING == 0 )); then
        handle_ut_termination
    fi
}

function finish_light_race(){
    local status=0
    local report_status=0
    if [[ -z "${LIGHT_RACE_JOB_PID}" ]]; then
        return 0
    fi
    wait "${LIGHT_RACE_JOB_PID}" || status=$?
    LIGHT_RACE_JOB_PID=""
    checkpoint_ut_event "finish" "light" "light race-test packages" "${status}"
    consume_light_race_report
    report_status=$?
    if (( report_status != 0 && status == 0 )); then
        status=${report_status}
    fi
    return "${status}"
}

function start_plan_race(){
    local package=$1
    local saved_term_trap
    local term_pending=0
    PLAN_RACE_TEST_BINARY="${G_WKSP}/${G_TS}-plan-race.test"
    PLAN_RACE_REPORT="${G_WKSP}/${G_TS}-plan-race-report.out"
    saved_term_trap=$(trap -p TERM)
    trap 'term_pending=1' TERM
    set -m
    run_plan_race_shards "${package}" &
    PLAN_RACE_JOB_PID=$!
    set +m
    restore_ut_term_trap "${saved_term_trap}"
    if (( term_pending != 0 )); then
        handle_ut_termination
    fi
}


function logger(){
    local level=$1
    local msg=$2
    local log=$LOG
    logger_base "$level" "$msg" "$log"
}

function report_active_ut_cases(){
    if [[ ! -s "${UT_REPORT}" ]]; then
        logger "ERR" "No Go test JSON is available to identify active UT cases"
        return 0
    fi

    logger "ERR" "Active or incomplete UT cases from ${UT_REPORT}:"
    awk -f "${BUILD_WKSP}/optools/active_ut_cases.awk" "${UT_REPORT}" |
        LC_ALL=C sort |
        sed 's/^/[active_ut_cases] /'
}

function report_cgroup_memory_usage(){
    local label=$1
    local relative_path=""
    local cgroup_path=""
    local events=""

    if [[ ! -r /proc/self/cgroup ]]; then
        return 0
    fi

    relative_path=$(awk -F: '$1 == "0" { print $3; exit }' /proc/self/cgroup)
    if [[ -n "${relative_path}" ]]; then
        cgroup_path="/sys/fs/cgroup${relative_path}"
        if [[ -r "${cgroup_path}/memory.peak" ]]; then
            events=$(tr '\n' ' ' < "${cgroup_path}/memory.events")
            logger "INF" "${label} cgroup memory: current=$(< "${cgroup_path}/memory.current") peak=$(< "${cgroup_path}/memory.peak") events=${events}"
            return 0
        fi
    fi

    relative_path=$(awk -F: '$2 ~ /(^|,)memory(,|$)/ { print $3; exit }' /proc/self/cgroup)
    cgroup_path="/sys/fs/cgroup/memory${relative_path}"
    if [[ -r "${cgroup_path}/memory.max_usage_in_bytes" ]]; then
        logger "INF" "${label} cgroup memory: current=$(< "${cgroup_path}/memory.usage_in_bytes") peak=$(< "${cgroup_path}/memory.max_usage_in_bytes") failcnt=$(< "${cgroup_path}/memory.failcnt")"
    fi
}

function consume_engine_race_report(){
    if [[ -z "${ENGINE_RACE_REPORT}" ]]; then
        return 0
    fi

    # A pending TERM may arrive after the helper has exited but while the
    # parent is appending its report.  Hold the same single-owner boundary for
    # both the normal path and the cancellation handler; otherwise the handler
    # could append the same JSON a second time.
    local saved_term_trap
    local term_pending=0
    local append_status=0
    saved_term_trap=$(trap -p TERM)
    trap 'term_pending=1' TERM
    # append_ut_report publishes an atomic destination only after every source
    # copy succeeds.  If TERM interrupts a copy, keep the source and marker so
    # cancellation diagnostics (or a safe retry) cannot lose the report.
    append_ut_report "${ENGINE_RACE_REPORT}" "${UT_REPORT}"
    append_status=$?
    if (( append_status != 0 )); then
        logger "ERR" "failed to consume engine race report; preserving ${ENGINE_RACE_REPORT}"
        restore_ut_term_trap "${saved_term_trap}"
        if (( term_pending != 0 && UT_TERMINATING == 0 )); then
            handle_ut_termination
        fi
        return "${append_status}"
    fi
    rm -f "${ENGINE_RACE_TEST_BINARY}" "${ENGINE_RACE_REPORT}" "${ENGINE_RACE_REPORT}".* \
        "${ENGINE_RACE_REPORT_READY}"
    ENGINE_RACE_TEST_BINARY=""
    ENGINE_RACE_REPORT=""
    ENGINE_RACE_REPORT_READY=""
    restore_ut_term_trap "${saved_term_trap}"
    if (( term_pending != 0 && UT_TERMINATING == 0 )); then
        handle_ut_termination
    fi
}

function consume_plan_race_report(){
    if [[ -z "${PLAN_RACE_REPORT}" ]]; then
        return 0
    fi

    local saved_term_trap
    local term_pending=0
    local append_status=0
    saved_term_trap=$(trap -p TERM)
    trap 'term_pending=1' TERM
    # Keep plan and engine report transfer on the same transactional path.  A
    # direct append could publish a prefix before a group TERM and then delete
    # the only source, making a retry duplicate or lose events.
    append_ut_report "${PLAN_RACE_REPORT}" "${UT_REPORT}"
    append_status=$?
    if (( append_status != 0 )); then
        logger "ERR" "failed to consume plan race report; preserving ${PLAN_RACE_REPORT}"
        restore_ut_term_trap "${saved_term_trap}"
        if (( term_pending != 0 && UT_TERMINATING == 0 )); then
            handle_ut_termination
        fi
        return "${append_status}"
    fi
    rm -f "${PLAN_RACE_REPORT}"
    PLAN_RACE_REPORT=""
    restore_ut_term_trap "${saved_term_trap}"
    if (( term_pending != 0 && UT_TERMINATING == 0 )); then
        handle_ut_termination
    fi
}

function handle_ut_termination(){
    trap - TERM
    if (( UT_TERMINATING != 0 )); then
        exit 143
    fi
    UT_TERMINATING=1
    checkpoint_ut_event "cancel" "${CURRENT_UT_STAGE}" "${CURRENT_UT_LABEL}" "143" \
        "current_pid=${CURRENT_UT_PID} light_pid=${LIGHT_RACE_JOB_PID} engine_pid=${ENGINE_RACE_JOB_PID} plan_pid=${PLAN_RACE_JOB_PID} prebuild_pid=${CLUSTER_PREBUILD_JOB_PID}"

    local pid
    # Notify every group before waiting, so the concurrent plan/helper cleanup
    # gets the same grace window as the foreground writer.
    for pid in "${CURRENT_UT_PID}" "${LIGHT_RACE_JOB_PID}" "${ENGINE_RACE_JOB_PID}" "${PLAN_RACE_JOB_PID}" "${CLUSTER_PREBUILD_JOB_PID}"; do
        [[ -n "${pid}" ]] && terminate_ut_process_group "${pid}" TERM
    done

    if [[ -n "${CURRENT_UT_PID}" ]]; then
        logger "ERR" "UT cancellation: stopping ${CURRENT_UT_LABEL} child ${CURRENT_UT_PID}"
        # Leave the outer timeout's kill-after budget for descendants that do
        # not honour TERM; do not block the diagnostic path indefinitely.
        wait_for_ut_process_group "${CURRENT_UT_PID}" 20
        wait "${CURRENT_UT_PID}" 2>/dev/null || true
        CURRENT_UT_PID=""
    fi
    if [[ -n "${LIGHT_RACE_JOB_PID}" ]]; then
        wait_for_ut_process_group "${LIGHT_RACE_JOB_PID}" "${UT_HELPER_TERM_GRACE_TICKS}"
        wait "${LIGHT_RACE_JOB_PID}" 2>/dev/null || true
        LIGHT_RACE_JOB_PID=""
    fi
    consume_light_race_report
    if [[ -n "${ENGINE_RACE_JOB_PID}" ]]; then
        wait_for_ut_process_group "${ENGINE_RACE_JOB_PID}" "${UT_HELPER_TERM_GRACE_TICKS}"
        wait "${ENGINE_RACE_JOB_PID}" 2>/dev/null || true
        ENGINE_RACE_JOB_PID=""
    fi
    if [[ -n "${PLAN_RACE_JOB_PID}" ]]; then
        wait_for_ut_process_group "${PLAN_RACE_JOB_PID}" "${UT_HELPER_TERM_GRACE_TICKS}"
        wait "${PLAN_RACE_JOB_PID}" 2>/dev/null || true
        PLAN_RACE_JOB_PID=""
    fi
    consume_plan_race_report
    if [[ -n "${CLUSTER_PREBUILD_JOB_PID}" ]]; then
        wait_for_ut_process_group "${CLUSTER_PREBUILD_JOB_PID}" "${UT_HELPER_TERM_GRACE_TICKS}"
        wait "${CLUSTER_PREBUILD_JOB_PID}" 2>/dev/null || true
        CLUSTER_PREBUILD_JOB_PID=""
    fi
    if [[ -n "${CLUSTER_PREBUILD_REPORT}" ]]; then
        if [[ -s "${CLUSTER_PREBUILD_REPORT}" ]]; then
            cat "${CLUSTER_PREBUILD_REPORT}" >> "${UT_STDERR}"
        fi
        rm -f "${CLUSTER_PREBUILD_REPORT}" "${CLUSTER_PREBUILD_REPORT}".* \
            "${G_WKSP}/${G_TS}-embedded-prebuild-"*.test
        CLUSTER_PREBUILD_REPORT=""
    fi
    consume_engine_race_report
    if [[ -n "${ENGINE_RACE_TEST_BINARY}" ]]; then
        rm -f "${ENGINE_RACE_TEST_BINARY}"
        ENGINE_RACE_TEST_BINARY=""
    fi
    if [[ -n "${PLAN_RACE_TEST_BINARY}" ]]; then
        rm -f "${PLAN_RACE_TEST_BINARY}"
        PLAN_RACE_TEST_BINARY=""
    fi
    logger "ERR" "UT runner received SIGTERM at stage=${CURRENT_UT_STAGE} label=${CURRENT_UT_LABEL}; reporting work without terminal Go test events"
    logger "ERR" "UT checkpoint: ${UT_CHECKPOINT}"
    logger "ERR" "UT stderr: ${UT_STDERR}"
    tail -n 40 "${UT_CHECKPOINT}" | sed 's/^/[ut_checkpoint] /' | tee -a "${LOG}"
    if [[ -s "${UT_STDERR}" ]]; then
        tail -n 40 "${UT_STDERR}" | sed 's/^/[ut_stderr] /' | tee -a "${LOG}"
    fi
    report_active_ut_cases
    exit 143
}

function run_engine_race_shards(){
    local engine_package=$1
    local engine_race_shards=$2
    local test_list="${G_WKSP}/${G_TS}-engine-race-tests.out"
    local build_log="${G_WKSP}/${G_TS}-engine-race-build.out"
    local metadata_file="${G_WKSP}/${G_TS}-engine-race-package.out"
    local engine_package_dir=""
    local engine_package_import=""
    local build_status=0
    local list_status=0
    local shard_status=0
    local wait_status=0
    local test_name=""
    local shard=0
    local test_count=0
    local pid=""
    local metadata_status=0
    local previous_term_trap=""
    local report_ready="${ENGINE_RACE_REPORT}.ready"
    local -a child_pids=(0)
    local -a shard_patterns
    local -a shard_counts
    local -a shard_pids
    local -a shard_reports

    if ! [[ "${engine_race_shards}" =~ ^[1-9][0-9]*$ ]] ||
        (( engine_race_shards > ENGINE_RACE_SHARDS )); then
        logger "ERR" "engine race shard count must be 1 or ${ENGINE_RACE_SHARDS}, got '${engine_race_shards}'"
        return 2
    fi

    checkpoint_ut_event "start" "engine" "${engine_package}" "" "shards=${engine_race_shards}"
    previous_term_trap=$(trap -p TERM)
    trap 'terminate_ut_process_groups 20 "${child_pids[@]}"; wait 2>/dev/null || true; rm -f "${metadata_file}"; exit 143' TERM
    set -m
    go list ${GO_MODULE_MODE} \
        -f '{{.Dir}}{{"\t"}}{{.ImportPath}}' "${engine_package}" \
        > "${metadata_file}" 2>&1 &
    child_pids=("$!")
    set +m
    wait "${child_pids[0]}"
    metadata_status=$?
    child_pids=(0)
    if (( metadata_status != 0 )) ||
        ! IFS=$'\t' read -r engine_package_dir engine_package_import < "${metadata_file}"; then
        logger "ERR" "Failed to resolve package metadata for ${engine_package}"
        tail -n 200 "${metadata_file}"
        rm -f "${metadata_file}"
        restore_ut_term_trap "${previous_term_trap}"
        set +m
        checkpoint_ut_event "finish" "engine" "${engine_package}" "${metadata_status}" "phase=discover"
        if (( metadata_status != 0 )); then
            return "${metadata_status}"
        fi
        return 2
    fi
    rm -f "${metadata_file}"

    : > "${ENGINE_RACE_REPORT}"
    rm -f "${report_ready}" "${ENGINE_RACE_REPORT}".*
    set -m
    LD_LIBRARY_PATH="${LD_LIBRARY_PATH}" \
        CGO_CFLAGS="${CGO_CFLAGS}" \
        CGO_LDFLAGS="${CGO_LDFLAGS}" \
        go test ${GO_MODULE_MODE} ${GO_TEST_VET_FLAGS} -short -race -tags "${TAGS}" \
        -p 1 -c -o "${ENGINE_RACE_TEST_BINARY}" "${engine_package}" > "${build_log}" 2>&1 &
    child_pids=("$!")
    set +m
    wait "${child_pids[0]}"
    build_status=$?
    child_pids=(0)
    if (( build_status != 0 )); then
        logger "ERR" "Failed to build race test binary for ${engine_package}"
        tail -n 200 "${build_log}"
        restore_ut_term_trap "${previous_term_trap}"
        set +m
        checkpoint_ut_event "finish" "engine" "${engine_package}" "${build_status}" "phase=build"
        return "${build_status}"
    fi

    set -m
    (
        cd "${engine_package_dir}" || exit 2
        LD_LIBRARY_PATH="${LD_LIBRARY_PATH}" \
            "${ENGINE_RACE_TEST_BINARY}" -test.short=true \
            -test.list='^(Test|Fuzz|Example)'
    ) > "${test_list}" 2>&1 &
    child_pids=("$!")
    set +m
    wait "${child_pids[0]}"
    list_status=$?
    child_pids=(0)
    if (( list_status != 0 )); then
        logger "ERR" "Failed to list tests for ${engine_package}"
        tail -n 200 "${test_list}"
        restore_ut_term_trap "${previous_term_trap}"
        set +m
        checkpoint_ut_event "finish" "engine" "${engine_package}" "${list_status}" "phase=list"
        return "${list_status}"
    fi

    for (( shard = 0; shard < engine_race_shards; shard++ )); do
        shard_patterns[shard]='^('
        shard_counts[shard]=0
        shard_reports[shard]="${ENGINE_RACE_REPORT}.$(( shard + 1 ))"
        : > "${shard_reports[shard]}"
    done

    # Alternate source-ordered top-level tests. This keeps adjacent tests from
    # the same large fixture file in different processes, automatically covers
    # newly added tests, and avoids a stale hand-maintained allowlist.
    while IFS= read -r test_name; do
        case "${test_name}" in
            Test*|Fuzz*|Example*) ;;
            *) continue ;;
        esac
        shard=$(( test_count % engine_race_shards ))
        if (( shard_counts[shard] > 0 )); then
            shard_patterns[shard]+='|'
        fi
        shard_patterns[shard]+="${test_name}"
        shard_counts[shard]=$(( shard_counts[shard] + 1 ))
        test_count=$(( test_count + 1 ))
    done < "${test_list}"

    if (( test_count == 0 )); then
        logger "ERR" "No tests discovered for ${engine_package}"
        restore_ut_term_trap "${previous_term_trap}"
        set +m
        return 2
    fi

    logger "INF" "Run ${test_count} tests in ${engine_package} across ${engine_race_shards} concurrent fresh race-detector processes"
    set -m
    for (( shard = 0; shard < engine_race_shards; shard++ )); do
        if (( shard_counts[shard] == 0 )); then
            continue
        fi
        shard_patterns[shard]+=')$'
        logger "INF" "Start ${engine_package} race shard $(( shard + 1 ))/${engine_race_shards} (${shard_counts[shard]} tests)"
        checkpoint_ut_event "start" "engine" "${engine_package} shard $(( shard + 1 ))/${engine_race_shards}" "" "tests=${shard_counts[shard]}"
        (
            cd "${engine_package_dir}" || exit 2
            LD_LIBRARY_PATH="${LD_LIBRARY_PATH}" \
                go tool test2json -t -p "${engine_package_import}" \
                "${ENGINE_RACE_TEST_BINARY}" -test.short=true -test.v=test2json \
                -test.paniconexit0=true -test.count=1 \
                -test.timeout="${UT_TIMEOUT}m" \
                -test.run="${shard_patterns[shard]}"
        ) > "${shard_reports[shard]}" &
        shard_pids[shard]=$!
        child_pids[shard]=${shard_pids[shard]}
    done
    set +m

    # Job control gives each shard (test2json and its test-binary child) a
    # distinct process group, so cancellation cannot strand a race process.
    for (( shard = 0; shard < engine_race_shards; shard++ )); do
        if (( shard_counts[shard] == 0 )); then
            continue
        fi
        pid=${shard_pids[shard]}
        wait "${pid}"
        wait_status=$?
        child_pids[shard]=0
        if (( wait_status != 0 )); then
            shard_status=1
            logger "ERR" "${engine_package} race shard $(( shard + 1 )) failed with status ${wait_status}"
        fi
        checkpoint_ut_event "finish" "engine" "${engine_package} shard $(( shard + 1 ))/${engine_race_shards}" "${wait_status}"
        cat "${shard_reports[shard]}" >> "${ENGINE_RACE_REPORT}"
    done
    # The marker is written only after every shard has been copied.  The parent
    # can therefore choose exactly one representation even if TERM arrives
    # between helper completion and parent report ownership.
    : > "${report_ready}"
    rm -f "${ENGINE_RACE_TEST_BINARY}" "${ENGINE_RACE_REPORT}".[0-9]*
    restore_ut_term_trap "${previous_term_trap}"
    ENGINE_RACE_TEST_BINARY=""
    checkpoint_ut_event "finish" "engine" "${engine_package}" "${shard_status}"
    return "${shard_status}"
}

function run_vet(){
    cd $BUILD_WKSP
    horiz_rule
    echo "#  BUILD WORKSPACE: $BUILD_WKSP"
    echo "#  SCA REPORT:      $SCA_REPORT"
    horiz_rule

    if [[ -f $SCA_REPORT ]]; then rm $SCA_REPORT; fi
    logger "INF" "Test is in progress... "
    LD_LIBRARY_PATH="${LD_LIBRARY_PATH}" CGO_CFLAGS="${CGO_CFLAGS}" CGO_LDFLAGS="${CGO_LDFLAGS}" go vet ${GO_MODULE_MODE} -tags "${TAGS}" -unsafeptr=false ./pkg/... 2>&1 | tee $SCA_REPORT
    logger "INF" "Refer to $SCA_REPORT for details"

}

function run_plan_race_shards(){
    local plan_package=$1
    local test_list="${G_WKSP}/${G_TS}-plan-race-tests.out"
    local build_log="${G_WKSP}/${G_TS}-plan-race-build.out"
    local metadata_file="${G_WKSP}/${G_TS}-plan-race-package.out"
    local plan_test_binary="${G_WKSP}/${G_TS}-plan-race.test"
    local plan_package_dir=""
    local plan_package_import=""
    local build_status=0
    local list_status=0
    local metadata_status=0
    local shard_status=0
    local shard_exit_status=0
    local test_name=""
    local shard=0
    local test_count=0
    local plan_child_pid=""
    local previous_term_trap=""
    local -a shard_patterns
    local -a shard_counts

    if ! [[ "${PLAN_RACE_SHARDS}" =~ ^[1-9][0-9]*$ ]] ||
        (( PLAN_RACE_SHARDS > 64 )); then
        logger "ERR" "PLAN_RACE_SHARDS must be an integer from 1 through 64, got '${PLAN_RACE_SHARDS}'"
        return 2
    fi

    # The plan phase may run concurrently with the heavy phase. Keep its
    # process-group cleanup local to this helper so a cancelled parent cannot
    # strand the test binary after the helper's shell exits.
    previous_term_trap=$(trap -p TERM)
    trap 'if [[ -n "${plan_child_pid}" ]]; then terminate_ut_process_groups 20 "${plan_child_pid}"; fi; wait 2>/dev/null || true; rm -f "${plan_test_binary}"; exit 143' TERM
    if [[ -z "${PLAN_RACE_REPORT}" ]]; then
        PLAN_RACE_REPORT="${G_WKSP}/${G_TS}-plan-race-report.out"
    fi
    : > "${PLAN_RACE_REPORT}"
    checkpoint_ut_event "start" "plan" "${plan_package}" "" "shards=${PLAN_RACE_SHARDS}"
    # Resolve both metadata fields through one cancellable child. Keeping the
    # PID in plan_child_pid makes TERM ownership identical to build and shard
    # execution.
    set -m
    go list ${GO_MODULE_MODE} \
        -f '{{.Dir}}{{"\t"}}{{.ImportPath}}' "${plan_package}" \
        > "${metadata_file}" 2>&1 &
    plan_child_pid=$!
    set +m
    wait "${plan_child_pid}"
    metadata_status=$?
    plan_child_pid=""
    if (( metadata_status != 0 )) ||
        ! IFS=$'\t' read -r plan_package_dir plan_package_import < "${metadata_file}"; then
        logger "ERR" "Failed to resolve package metadata for ${plan_package}"
        tail -n 200 "${metadata_file}"
        : > "${metadata_file}"
        restore_ut_term_trap "${previous_term_trap}"
        if (( metadata_status != 0 )); then
            return "${metadata_status}"
        fi
        return 2
    fi
    : > "${metadata_file}"

    # Compile and link the race-instrumented test binary once. Each shard still
    # runs in a fresh process, preserving race-detector and package-global
    # isolation without repeating the same link action for every shard.
    PLAN_RACE_TEST_BINARY="${plan_test_binary}"
    set -m
    LD_LIBRARY_PATH="${LD_LIBRARY_PATH}" \
        CGO_CFLAGS="${CGO_CFLAGS}" \
        CGO_LDFLAGS="${CGO_LDFLAGS}" \
        go test ${GO_MODULE_MODE} ${GO_TEST_VET_FLAGS} -short -race -tags "${TAGS}" \
        -p 1 -c -o "${plan_test_binary}" "${plan_package}" > "${build_log}" 2>&1 &
    plan_child_pid=$!
    set +m
    wait "${plan_child_pid}"
    build_status=$?
    plan_child_pid=""
    if (( build_status != 0 )); then
        logger "ERR" "Failed to build race test binary for ${plan_package}"
        tail -n 200 "${build_log}"
        rm -f "${plan_test_binary}"
        PLAN_RACE_TEST_BINARY=""
        checkpoint_ut_event "finish" "plan" "${plan_package}" "${build_status}" "phase=build"
        restore_ut_term_trap "${previous_term_trap}"
        return "${build_status}"
    fi

    # Test binaries normally execute with the package source directory as cwd.
    # Preserve that contract for both discovery and shard execution.
    set -m
    (
        cd "${plan_package_dir}" || exit 2
        LD_LIBRARY_PATH="${LD_LIBRARY_PATH}" \
            "${plan_test_binary}" -test.short=true \
            -test.list='^(Test|Fuzz|Example)'
    ) > "${test_list}" 2>&1 &
    plan_child_pid=$!
    set +m
    wait "${plan_child_pid}"
    list_status=$?
    plan_child_pid=""
    if (( list_status != 0 )); then
        logger "ERR" "Failed to list tests for ${plan_package}"
        tail -n 200 "${test_list}"
        rm -f "${plan_test_binary}"
        PLAN_RACE_TEST_BINARY=""
        checkpoint_ut_event "finish" "plan" "${plan_package}" "${list_status}" "phase=list"
        restore_ut_term_trap "${previous_term_trap}"
        return "${list_status}"
    fi

    for (( shard = 0; shard < PLAN_RACE_SHARDS; shard++ )); do
        shard_patterns[shard]='^('
        shard_counts[shard]=0
    done

    while IFS= read -r test_name; do
        case "${test_name}" in
            Test*|Fuzz*|Example*) ;;
            *) continue ;;
        esac
        shard=$(( test_count % PLAN_RACE_SHARDS ))
        if (( shard_counts[shard] > 0 )); then
            shard_patterns[shard]+='|'
        fi
        shard_patterns[shard]+="${test_name}"
        shard_counts[shard]=$(( shard_counts[shard] + 1 ))
        test_count=$(( test_count + 1 ))
    done < "${test_list}"

    if (( test_count == 0 )); then
        logger "ERR" "No tests discovered for ${plan_package}"
        rm -f "${plan_test_binary}"
        PLAN_RACE_TEST_BINARY=""
        checkpoint_ut_event "finish" "plan" "${plan_package}" "2" "phase=discover"
        restore_ut_term_trap "${previous_term_trap}"
        return 2
    fi

    logger "INF" "Run ${test_count} tests in ${plan_package} across ${PLAN_RACE_SHARDS} fresh race-detector processes"
    for (( shard = 0; shard < PLAN_RACE_SHARDS; shard++ )); do
        if (( shard_counts[shard] == 0 )); then
            continue
        fi
        shard_patterns[shard]+=')$'
        logger "INF" "Run ${plan_package} race shard $(( shard + 1 ))/${PLAN_RACE_SHARDS} (${shard_counts[shard]} tests)"
        mark_ut_stage "plan" "${plan_package} shard $(( shard + 1 ))/${PLAN_RACE_SHARDS}" start "" "tests=${shard_counts[shard]}"
        set -m
        (
            cd "${plan_package_dir}" || exit 2
            LD_LIBRARY_PATH="${LD_LIBRARY_PATH}" \
                go tool test2json -t -p "${plan_package_import}" \
                "${plan_test_binary}" -test.short=true -test.v=test2json \
                -test.paniconexit0=true -test.count=1 \
                -test.timeout="${UT_TIMEOUT}m" \
                -test.run="${shard_patterns[shard]}"
        ) >> "${PLAN_RACE_REPORT}" 2>> "${UT_STDERR}" &
        plan_child_pid=$!
        CURRENT_UT_PID=${plan_child_pid}
        checkpoint_ut_event "pid-start" "plan" "${plan_package} shard $(( shard + 1 ))/${PLAN_RACE_SHARDS}" "" "child_pid=${plan_child_pid} tests=${shard_counts[shard]}"
        wait "${plan_child_pid}"
        shard_exit_status=$?
        plan_child_pid=""
        CURRENT_UT_PID=""
        set +m
        mark_ut_stage "plan" "${plan_package} shard $(( shard + 1 ))/${PLAN_RACE_SHARDS}" finish "${shard_exit_status}"
        if (( shard_exit_status != 0 )); then
            shard_status=1
        fi
    done

    rm -f "${plan_test_binary}"
    PLAN_RACE_TEST_BINARY=""
    restore_ut_term_trap "${previous_term_trap}"
    checkpoint_ut_event "finish" "plan" "${plan_package}" "${shard_status}"
    return "${shard_status}"
}

function remove_packages_from_scope(){
    local scope=$1
    shift
    local package

    for package in "$@"; do
        scope=$(printf '%s\n' "${scope}" | grep -Fvx "${package}")
    done
    printf '%s\n' "${scope}"
}

function run_embedded_prebuild(){
    local package_scope=$1
    local package_parallel=$2
    local report_base=$3
    local package_index=0
    local package=""
    local output_path=""
    local package_report=""
    local child_pid=""
    local child_status=0
    local prebuild_status=0
    local active_count=0
    local -a child_pids=()
    local -a child_outputs=()
    local -a packages=()
    local previous_term_trap=""

    while IFS= read -r package; do
        [[ -n "${package}" ]] && packages+=("${package}")
    done <<< "${package_scope}"
    if (( ${#packages[@]} == 0 )); then
        return 0
    fi

    previous_term_trap=$(trap -p TERM)
    trap 'terminate_ut_process_groups 20 "${child_pids[@]}"; for package_report in "${child_outputs[@]}"; do if [[ -f "${package_report}" ]]; then cat "${package_report}" >> "${report_base}"; fi; done; wait 2>/dev/null || true; exit 143' TERM

    for package in "${packages[@]}"; do
        while (( active_count >= package_parallel )); do
            child_pid=${child_pids[0]}
            wait "${child_pid}" || child_status=$?
            if (( child_status != 0 )); then
                prebuild_status=1
            fi
            cat "${child_outputs[0]}" >> "${report_base}"
            child_pids=("${child_pids[@]:1}")
            child_outputs=("${child_outputs[@]:1}")
            active_count=$((active_count - 1))
            child_status=0
        done

        output_path="${G_WKSP}/${G_TS}-embedded-prebuild-${package_index}.test"
        package_report="${report_base}.${package_index}"
        : > "${package_report}"
        set -m
        env LD_LIBRARY_PATH="${LD_LIBRARY_PATH}" \
            CGO_CFLAGS="${CGO_CFLAGS}" \
            CGO_LDFLAGS="${CGO_LDFLAGS}" \
            go test ${GO_MODULE_MODE} ${GO_TEST_VET_FLAGS} -short -race \
            -tags "${TAGS}" -p 1 -timeout "${UT_TIMEOUT}m" \
            -c -o "${output_path}" "${package}" > "${package_report}" 2>&1 &
        child_pid=$!
        set +m
        child_pids+=("${child_pid}")
        child_outputs+=("${package_report}")
        active_count=$((active_count + 1))
        package_index=$((package_index + 1))
    done

    while (( active_count > 0 )); do
        child_pid=${child_pids[0]}
        wait "${child_pid}" || child_status=$?
        if (( child_status != 0 )); then
            prebuild_status=1
        fi
        cat "${child_outputs[0]}" >> "${report_base}"
        child_pids=("${child_pids[@]:1}")
        child_outputs=("${child_outputs[@]:1}")
        active_count=$((active_count - 1))
        child_status=0
    done
    restore_ut_term_trap "${previous_term_trap}"
    return "${prebuild_status}"
}

function start_embedded_prebuild(){
    local package_scope=$1
    local package_parallel=$2

    if [[ -z "${package_scope}" ]]; then
        return 0
    fi
    CLUSTER_PREBUILD_REPORT="${G_WKSP}/${G_TS}-embedded-prebuild.out"
    : > "${CLUSTER_PREBUILD_REPORT}"
    mark_ut_stage "embedded-prebuild" "compile embedded-cluster packages" start \
        "" "parallel=${package_parallel} compile_only=true"
    set -m
    run_embedded_prebuild "${package_scope}" "${package_parallel}" "${CLUSTER_PREBUILD_REPORT}" &
    CLUSTER_PREBUILD_JOB_PID=$!
    set +m
    checkpoint_ut_event "pid-start" "embedded-prebuild" "compile embedded-cluster packages" "" \
        "child_pid=${CLUSTER_PREBUILD_JOB_PID} parallel=${package_parallel} compile_only=true"
}

function finish_embedded_prebuild(){
    local prebuild_status=0
    if [[ -z "${CLUSTER_PREBUILD_JOB_PID}" ]]; then
        return 0
    fi
    wait "${CLUSTER_PREBUILD_JOB_PID}" || prebuild_status=$?
    CLUSTER_PREBUILD_JOB_PID=""
    mark_ut_stage "embedded-prebuild" "compile embedded-cluster packages" finish "${prebuild_status}"
    if (( prebuild_status != 0 )); then
        # The real embedded test command remains authoritative. A prebuild can
        # fail because of an environment-only test invocation; retain its
        # output for diagnosis but do not turn a later passing test red.
        logger "WRN" "embedded prebuild failed with status ${prebuild_status}; continuing with the authoritative test run"
        if [[ -s "${CLUSTER_PREBUILD_REPORT}" ]]; then
            tail -n 100 "${CLUSTER_PREBUILD_REPORT}" | sed 's/^/[embedded_prebuild] /'
        fi
    fi
    rm -f "${CLUSTER_PREBUILD_REPORT}" "${CLUSTER_PREBUILD_REPORT}".* \
        "${G_WKSP}/${G_TS}-embedded-prebuild-"*.test
    CLUSTER_PREBUILD_REPORT=""
}

function run_tests(){
    cd $BUILD_WKSP
    horiz_rule
    echo "#  BUILD WORKSPACE: $BUILD_WKSP"
    echo "#  SKIPPED TEST:    $SKIP_TESTS"
    echo "#  UT REPORT:       $UT_REPORT"
    echo "#  COVERAGE REPORT: $CODE_COVERAGE"
    echo "#  UT TIMEOUT:      $UT_TIMEOUT"
    echo "#  UT HARD TIMEOUT: $UT_HARD_TIMEOUT"
    echo "#  UT PARALLEL:     $UT_PARALLEL"
    echo "#  UT SHARD:        $UT_SHARD"
    echo "#  EMBEDDED PREBUILD: $UT_PREBUILD_EMBEDDED"
    echo "#  PLAN OVERLAP:    $UT_OVERLAP_PLAN"
    echo "#  LIGHT OVERLAP:   $UT_OVERLAP_LIGHT (parallel $UT_OVERLAP_LIGHT_PARALLEL)"
    echo "#  HELPER TERM GRACE: $UT_HELPER_TERM_GRACE_TICKS ticks"
    echo "#  CLUSTER ADMISSION: process lifecycle"
    echo "#  UT CHECKPOINT:   $UT_CHECKPOINT"
    echo "#  UT STDERR:       $UT_STDERR"
    echo "#  HEAVY RACE UT:   $HEAVY_RACE_PARALLEL total package slots"
    horiz_rule
    mark_ut_stage "routing" "validate shard and package partition" start

    if ! list_ut_shard_stages "${UT_SHARD}" >/dev/null; then
        logger "ERR" "UT_SHARD must be all, light, issues, embedded, or heavy-plan; got '${UT_SHARD}'"
        UT_TEST_STATUS=1
        mark_ut_stage "routing" "validate shard and package partition" finish 1
        return 0
    fi
    if ! validate_complete_partition \
        "UT stage" \
        "$(list_ut_shard_stages all)" \
        "$(list_ut_shard_stages light)" \
        "$(list_ut_shard_stages issues)" \
        "$(list_ut_shard_stages embedded)" \
        "$(list_ut_shard_stages heavy-plan)"; then
        logger "ERR" "Race-UT shard stages are not a complete disjoint partition"
        UT_TEST_STATUS=1
        mark_ut_stage "routing" "validate shard and package partition" finish 1
        return 0
    fi
    if [[ "${SKIP_TESTS}" == "race" && "${UT_SHARD}" != "all" ]]; then
        logger "ERR" "split UT shards require race mode; got SKIP_TESTS=race with UT_SHARD=${UT_SHARD}"
        UT_TEST_STATUS=1
        mark_ut_stage "routing" "validate shard and package partition" finish 1
        return 0
    fi
    if ! [[ "${UT_PREBUILD_EMBEDDED}" =~ ^[01]$ ]]; then
        logger "ERR" "UT_PREBUILD_EMBEDDED must be 0 or 1, got '${UT_PREBUILD_EMBEDDED}'"
        UT_TEST_STATUS=1
        mark_ut_stage "routing" "validate shard and package partition" finish 1
        return 0
    fi
    if ! [[ "${UT_OVERLAP_PLAN}" =~ ^[01]$ ]]; then
        logger "ERR" "UT_OVERLAP_PLAN must be 0 or 1, got '${UT_OVERLAP_PLAN}'"
        UT_TEST_STATUS=1
        mark_ut_stage "routing" "validate shard and package partition" finish 1
        return 0
    fi
    if ! [[ "${UT_OVERLAP_LIGHT}" =~ ^[01]$ ]]; then
        logger "ERR" "UT_OVERLAP_LIGHT must be 0 or 1, got '${UT_OVERLAP_LIGHT}'"
        UT_TEST_STATUS=1
        mark_ut_stage "routing" "validate shard and package partition" finish 1
        return 0
    fi
    if ! [[ "${UT_OVERLAP_LIGHT_PARALLEL}" =~ ^[1-9][0-9]*$ ]] ||
        (( UT_OVERLAP_LIGHT_PARALLEL > 64 )); then
        logger "ERR" "UT_OVERLAP_LIGHT_PARALLEL must be an integer from 1 through 64, got '${UT_OVERLAP_LIGHT_PARALLEL}'"
        UT_TEST_STATUS=1
        mark_ut_stage "routing" "validate shard and package partition" finish 1
        return 0
    fi
    if ! [[ "${UT_HELPER_TERM_GRACE_TICKS}" =~ ^[0-9]+$ ]] ||
        (( UT_HELPER_TERM_GRACE_TICKS < 41 || UT_HELPER_TERM_GRACE_TICKS > 240 )); then
        logger "ERR" "UT_HELPER_TERM_GRACE_TICKS must be between 41 and 240, got '${UT_HELPER_TERM_GRACE_TICKS}'"
        UT_TEST_STATUS=1
        mark_ut_stage "routing" "validate shard and package partition" finish 1
        return 0
    fi
    mark_ut_stage "routing" "validate shard and package partition" finish 0

    mark_ut_stage "prepare" "clean go test cache" start
    logger "INF" "Clean go test cache"
    local testcache_status=0
    go clean -testcache || testcache_status=$?
    mark_ut_stage "prepare" "clean go test cache" finish "${testcache_status}"
    if (( testcache_status != 0 )); then
        logger "ERR" "Failed to clean Go test cache"
        UT_TEST_STATUS=1
        return 0
    fi

    local test_scope
    mark_ut_stage "discovery" "resolve UT package scope" start
    if ! test_scope=$(go list ${GO_MODULE_MODE} ./...); then
        logger "ERR" "Failed to resolve the complete UT package scope"
        UT_TEST_STATUS=1
        mark_ut_stage "discovery" "resolve UT package scope" finish 1
        return 0
    fi
    mark_ut_stage "discovery" "resolve UT package scope" finish 0
    test_scope=$(printf '%s\n' "${test_scope}" | grep -v 'driver/aoe' | grep -v 'engine/aoe' | grep -v 'pkg/catalog')
    if [[ -z "${test_scope}" ]]; then
        logger "ERR" "The complete UT package scope is empty"
        UT_TEST_STATUS=1
        return 0
    fi
    local leave_out=$(egrep -lr  --include="*.go" 'Code generated by protoc-gen-gogo. DO NOT EDIT.' ./pkg/* | sort -u | xargs basename -a)
    logger "INF" "Ingore code coverage $(echo ${leave_out[@]}|tr " " "|")"
    local cover_profile='profile.raw'
    # Top-level cgo owns the complete thirdparty + CGo generation and stages
    # runtime libraries. A second standalone thirdparties invocation only
    # repeats the no-op scan and obscures that ownership contract.
    mark_ut_stage "build" "native CGo prerequisites" start
    make cgo
    local cgo_status=$?
    mark_ut_stage "build" "native CGo prerequisites" finish "${cgo_status}"
    if (( cgo_status != 0 )); then
        UT_TEST_STATUS=1
        return 0
    fi

    # Compile and link a CGo-transitive package through the same deterministic
    # CPU wrapper documented for local development. This catches drift between
    # libmo's declared native dependencies and the wrapper before the full UT
    # matrix obscures it among unrelated package output. GPU builds have a
    # separate, explicit CUDA/cuVS link contract.
    if [[ "${MO_CL_CUDA:-0}" != "1" ]]; then
        logger "INF" "Smoke test the deterministic CGo test wrapper"
        mark_ut_stage "build" "CGo wrapper smoke test" start
        if ! .agents/skills/mo-dev/scripts/mo-cgo-test \
            -count=1 -timeout=120s ./optools/testdata/mo_cgo_transitive; then
            logger "ERR" "Deterministic CGo test wrapper smoke failed"
            mark_ut_stage "build" "CGo wrapper smoke test" finish 1
            UT_TEST_STATUS=1
            return 0
        fi
        mark_ut_stage "build" "CGo wrapper smoke test" finish 0
    fi

    if [[ $SKIP_TESTS == 'race' ]]; then
        logger "INF" "Run UT packages with parallelism ${UT_PARALLEL} and process-lifecycle cluster admission"
        run_ut_command "all" "all packages" env LD_LIBRARY_PATH="${LD_LIBRARY_PATH}" CGO_CFLAGS="${CGO_CFLAGS}" CGO_LDFLAGS="${CGO_LDFLAGS}" go test ${GO_MODULE_MODE} ${GO_TEST_VET_FLAGS} -short -v -json -tags "${TAGS}" -p ${UT_PARALLEL} -timeout "${UT_TIMEOUT}m" $test_scope
        UT_TEST_STATUS=$?
    else
        logger "INF" "Run UT with race check"
        local plan_package
        local engine_package
        local hnsw_package
        local engine_test_scope
        local serial_test_scope
        local cluster_test_scope
        local resource_heavy_test_scope
        local light_test_scope
        local package
        local cluster_package_parallel=2
        local package_status=0
        local light_status=0
        local hnsw_status=0
        local serial_status=0
        local cluster_status=0
        local resource_heavy_status=0
        local engine_status=0
        local plan_status=0
        local report_status=0
        local resource_heavy_parallel=1
        local engine_race_parallel=1
        local shard_engine=1
        local engine_joined=0
        local light_started=0
        local overlap_light=0
        local light_parallel=${UT_OVERLAP_LIGHT_PARALLEL}

        if ! [[ "${UT_PARALLEL}" =~ ^[1-9][0-9]*$ ]]; then
            logger "ERR" "UT_PARALLEL must be a positive integer, got '${UT_PARALLEL}'"
            UT_TEST_STATUS=1
            return 0
        fi
        if (( light_parallel > UT_PARALLEL )); then
            light_parallel=${UT_PARALLEL}
            logger "INF" "Cap overlapping light package parallelism to UT_PARALLEL=${UT_PARALLEL}"
        fi

        if ! [[ "${HEAVY_RACE_PARALLEL}" =~ ^[1-9][0-9]*$ ]] ||
            (( HEAVY_RACE_PARALLEL > 64 )); then
            logger "ERR" "HEAVY_RACE_PARALLEL must be an integer from 1 through 64, got '${HEAVY_RACE_PARALLEL}'"
            UT_TEST_STATUS=1
            return 0
        fi
        if (( HEAVY_RACE_PARALLEL < cluster_package_parallel )); then
            cluster_package_parallel=${HEAVY_RACE_PARALLEL}
        fi

        if ! plan_package=$(go list ${GO_MODULE_MODE} ./pkg/sql/plan); then
            logger "ERR" "Failed to resolve ./pkg/sql/plan"
            UT_TEST_STATUS=1
            return 0
        fi
        if ! engine_package=$(go list ${GO_MODULE_MODE} ./pkg/vm/engine/test); then
            logger "ERR" "Failed to resolve ./pkg/vm/engine/test"
            UT_TEST_STATUS=1
            return 0
        fi
        if ! hnsw_package=$(go list ${GO_MODULE_MODE} ./pkg/vectorindex/hnsw); then
            logger "ERR" "Failed to resolve ./pkg/vectorindex/hnsw"
            UT_TEST_STATUS=1
            return 0
        fi

        # The main issues package keeps its shared base cluster alive for most
        # of the test process, so it retains an exclusive runner. The isolated
        # issues package has no shared base and safely belongs to the embedded
        # group: runner-wide admission still serializes each complete cluster
        # lifecycle. Former logservice/TAE members allocate independent ports
        # with collision retry and belong in the normal parallel scope.
        if ! serial_test_scope=$(go list ${GO_MODULE_MODE} \
            ./pkg/tests/issues); then
            logger "ERR" "Failed to resolve serial race-test packages"
            UT_TEST_STATUS=1
            return 0
        fi

        # Derive cluster owners from each race test binary's complete dependency
        # graph. This also catches packages that start a cluster through a test
        # helper, without relying on an incomplete directory allowlist.
        if ! cluster_test_scope=$(list_embedded_cluster_test_packages ${test_scope}); then
            logger "ERR" "Failed to discover embedded-cluster race-test packages"
            UT_TEST_STATUS=1
            return 0
        fi

        # Group precedence is exclusive > embedded cluster > resource heavy >
        # light. Keep every package in exactly one group even when its test
        # dependencies evolve.
        cluster_test_scope=$(remove_packages_from_scope \
            "${cluster_test_scope}" \
            "${plan_package}" \
            "${hnsw_package}" \
            ${serial_test_scope})

        # Dependency-based group precedence remains authoritative. If this
        # package ever starts owning an embedded cluster, keep it in that
        # serialized lifecycle group instead of running it a second time here.
        if printf '%s\n%s\n' "${serial_test_scope}" "${cluster_test_scope}" |
            grep -Fxq "${engine_package}"; then
            shard_engine=0
            engine_test_scope=""
            logger "INF" "Keep ${engine_package} in its higher-precedence race-test group"
        else
            engine_test_scope="${engine_package}"
        fi

        if ! resource_heavy_test_scope=$(go list ${GO_MODULE_MODE} \
            ./pkg/backup \
            ./pkg/fileservice \
            ./pkg/sql/plan/function \
            ./pkg/vm/engine/tae/db/test); then
            logger "ERR" "Failed to resolve resource-heavy race-test packages"
            UT_TEST_STATUS=1
            return 0
        fi
        resource_heavy_test_scope=$(remove_packages_from_scope \
            "${resource_heavy_test_scope}" \
            "${plan_package}" \
            "${engine_package}" \
            "${hnsw_package}" \
            ${serial_test_scope} \
            ${cluster_test_scope})

        light_test_scope=$(remove_packages_from_scope \
            "${test_scope}" \
            "${plan_package}" \
            "${engine_package}" \
            "${hnsw_package}" \
            ${serial_test_scope} \
            ${cluster_test_scope} \
            ${resource_heavy_test_scope})

        if ! validate_complete_partition \
            "UT package" \
            "${test_scope}" \
            "${light_test_scope}" \
            "${hnsw_package}" \
            "${serial_test_scope}" \
            "${cluster_test_scope}" \
            "${resource_heavy_test_scope}" \
            "${engine_test_scope}" \
            "${plan_package}"; then
            logger "ERR" "Race-UT package groups are not a complete disjoint partition"
            UT_TEST_STATUS=1
            return 0
        fi

        : > "${UT_REPORT}"
        # HNSW owns native worker pools inside its test binary. It must finish
        # before another race wave starts. When the bounded light/issues
        # overlap is eligible, run HNSW first, then let the low-budget light
        # helper overlap only the exclusive issues package.
        if (( UT_OVERLAP_LIGHT == 1 && UT_PARALLEL > 1 )) &&
            should_run_ut_stage light && should_run_ut_stage serial &&
            [[ -n "${light_test_scope}" ]]; then
            overlap_light=1
            if should_run_ut_stage hnsw; then
                logger "INF" "Run HNSW race-test package with exclusive runner CPU before light/issues overlap"
                run_ut_command "hnsw" "HNSW race-test package" env LD_LIBRARY_PATH="${LD_LIBRARY_PATH}" CGO_CFLAGS="${CGO_CFLAGS}" CGO_LDFLAGS="${CGO_LDFLAGS}" go test ${GO_MODULE_MODE} ${GO_TEST_VET_FLAGS} -short -v -json -tags "${TAGS}" -p 1 -timeout "${UT_TIMEOUT}m" -race "${hnsw_package}"
                hnsw_status=$?
            fi
            logger "INF" "Start light race-test packages in background with parallelism ${light_parallel}; overlap only with exclusive issues"
            if start_light_race "${light_test_scope}" "${light_parallel}"; then
                light_started=1
            else
                # A launch failure must not silently remove the light group
                # from the authoritative suite. Fall back to the original
                # foreground command so the package scope still executes.
                logger "ERR" "failed to start overlapping light race-test helper; retrying in foreground"
                run_ut_command "light" "light race-test packages" env LD_LIBRARY_PATH="${LD_LIBRARY_PATH}" CGO_CFLAGS="${CGO_CFLAGS}" CGO_LDFLAGS="${CGO_LDFLAGS}" go test ${GO_MODULE_MODE} ${GO_TEST_VET_FLAGS} -short -v -json -tags "${TAGS}" -p ${UT_PARALLEL} -timeout "${UT_TIMEOUT}m" -race $light_test_scope
                light_status=$?
                overlap_light=0
            fi
        else
            if should_run_ut_stage light && [[ -n "${light_test_scope}" ]]; then
                logger "INF" "Run light race-test packages with parallelism ${UT_PARALLEL}"
                run_ut_command "light" "light race-test packages" env LD_LIBRARY_PATH="${LD_LIBRARY_PATH}" CGO_CFLAGS="${CGO_CFLAGS}" CGO_LDFLAGS="${CGO_LDFLAGS}" go test ${GO_MODULE_MODE} ${GO_TEST_VET_FLAGS} -short -v -json -tags "${TAGS}" -p ${UT_PARALLEL} -timeout "${UT_TIMEOUT}m" -race $light_test_scope
                light_status=$?
            fi

            if should_run_ut_stage hnsw; then
                logger "INF" "Run HNSW race-test package with exclusive runner CPU"
                run_ut_command "hnsw" "HNSW race-test package" env LD_LIBRARY_PATH="${LD_LIBRARY_PATH}" CGO_CFLAGS="${CGO_CFLAGS}" CGO_LDFLAGS="${CGO_LDFLAGS}" go test ${GO_MODULE_MODE} ${GO_TEST_VET_FLAGS} -short -v -json -tags "${TAGS}" -p 1 -timeout "${UT_TIMEOUT}m" -race "${hnsw_package}"
                hnsw_status=$?
            fi
        fi

        # Compile the next embedded wave while the exclusive issues package is
        # exercising its already-started shared cluster. `go test -c` only
        # compiles and links; it does not execute TestMain or any test, so this
        # overlaps build/link work without creating another active cluster.
        if (( UT_PREBUILD_EMBEDDED == 1 )) &&
            (( overlap_light == 0 )) &&
            should_run_ut_stage serial && should_run_ut_stage embedded &&
            [[ -n "${cluster_test_scope}" ]]; then
            start_embedded_prebuild "${cluster_test_scope}" "${cluster_package_parallel}"
        fi

        if should_run_ut_stage serial; then
            logger "INF" "Run exclusive race-test packages serially"
            for package in ${serial_test_scope}; do
                logger "INF" "Run exclusive race-test package ${package}"
                run_ut_command "serial" "exclusive race-test package ${package}" env LD_LIBRARY_PATH="${LD_LIBRARY_PATH}" CGO_CFLAGS="${CGO_CFLAGS}" CGO_LDFLAGS="${CGO_LDFLAGS}" go test ${GO_MODULE_MODE} ${GO_TEST_VET_FLAGS} -short -v -json -tags "${TAGS}" -p 1 -timeout "${UT_TIMEOUT}m" -race "${package}"
                package_status=$?
                if (( package_status != 0 )); then
                    serial_status=1
                    logger "ERR" "Exclusive race-test package ${package} failed with status ${package_status}"
                fi
            done
        fi

        if (( light_started == 1 )); then
            finish_light_race
            light_status=$?
            report_cgroup_memory_usage "Light/issues overlap"
        fi

        # These packages link embedded clusters with substantial race-detector
        # memory. The runner-wide file-lock admission keeps complete cluster
        # lifecycles serialized across test binaries. Allow one additional
        # package process to overlap linking, setup, and non-cluster work without
        # returning to the six-way contention that starved HAKeeper.
        if should_run_ut_stage embedded; then
            finish_embedded_prebuild
            logger "INF" "Run embedded-cluster race-test packages with package parallelism ${cluster_package_parallel} and serialized cluster lifecycle admission"
            run_ut_command "embedded" "embedded-cluster race-test packages" env LD_LIBRARY_PATH="${LD_LIBRARY_PATH}" CGO_CFLAGS="${CGO_CFLAGS}" CGO_LDFLAGS="${CGO_LDFLAGS}" go test ${GO_MODULE_MODE} ${GO_TEST_VET_FLAGS} -short -v -json -tags "${TAGS}" -p "${cluster_package_parallel}" -timeout "${UT_TIMEOUT}m" -race $cluster_test_scope
            cluster_status=$?
        fi

        if should_run_ut_stage heavy && (( shard_engine == 1 )); then
            # engine/test is dominated by serial fixture lifecycles inside one
            # process. Build it once and split every discovered top-level test
            # across fresh race processes. The effective shard count and the
            # remaining go-test parallelism share HEAVY_RACE_PARALLEL as one
            # strict process budget. Low custom budgets use sequential waves.
            engine_race_parallel=${ENGINE_RACE_SHARDS}
            if (( engine_race_parallel > HEAVY_RACE_PARALLEL )); then
                engine_race_parallel=${HEAVY_RACE_PARALLEL}
            fi
            resource_heavy_parallel=$(( HEAVY_RACE_PARALLEL - engine_race_parallel ))
            if (( HEAVY_RACE_PARALLEL <= engine_race_parallel )); then
                resource_heavy_parallel=0
            fi
            ENGINE_RACE_TEST_BINARY="${G_WKSP}/${G_TS}-engine-race.test"
            ENGINE_RACE_REPORT="${G_WKSP}/${G_TS}-engine-race-report.out"
            ENGINE_RACE_REPORT_READY="${ENGINE_RACE_REPORT}.ready"

            if (( resource_heavy_parallel > 0 )); then
                set -m
                run_engine_race_shards "${engine_package}" "${engine_race_parallel}" &
                ENGINE_RACE_JOB_PID=$!
                set +m
            else
                resource_heavy_parallel=${HEAVY_RACE_PARALLEL}
            fi
        elif should_run_ut_stage heavy; then
            resource_heavy_parallel=${HEAVY_RACE_PARALLEL}
        fi

        if should_run_ut_stage heavy; then
            logger "INF" "Run remaining resource-heavy race-test packages with parallelism ${resource_heavy_parallel}"
            start_ut_command "heavy" "resource-heavy race-test packages" env LD_LIBRARY_PATH="${LD_LIBRARY_PATH}" CGO_CFLAGS="${CGO_CFLAGS}" CGO_LDFLAGS="${CGO_LDFLAGS}" go test ${GO_MODULE_MODE} ${GO_TEST_VET_FLAGS} -short -v -json -tags "${TAGS}" -p ${resource_heavy_parallel} -timeout "${UT_TIMEOUT}m" -race $resource_heavy_test_scope

            # Reuse the engine slots only after its helper has exited. At the
            # default budget, engine(2)+resource(1) becomes plan(1)+resource(1).
            # A failed engine still releases capacity: all tests must run and
            # its status remains authoritative. Defer report transfer until the
            # foreground writer stops; an atomic rename during its writes would
            # otherwise lose subsequent events written to the old inode.
            if [[ -n "${ENGINE_RACE_JOB_PID}" ]] &&
                (( UT_OVERLAP_PLAN == 1 )) && should_run_ut_stage plan; then
                wait "${ENGINE_RACE_JOB_PID}"
                engine_status=$?
                ENGINE_RACE_JOB_PID=""
                engine_joined=1
                logger "INF" "Engine finished; reuse released capacity for plan race tests"
                start_plan_race "${plan_package}"
            fi
            finish_ut_command
            resource_heavy_status=$?

            if (( shard_engine == 1 )); then
                if [[ -n "${ENGINE_RACE_JOB_PID}" ]]; then
                    wait "${ENGINE_RACE_JOB_PID}"
                    engine_status=$?
                    ENGINE_RACE_JOB_PID=""
                elif (( engine_joined == 0 )); then
                    # Keep the helper's process-group TERM trap scoped to a
                    # subshell even when a low budget requires sequential waves.
                    set -m
                    run_engine_race_shards "${engine_package}" "${engine_race_parallel}" &
                    ENGINE_RACE_JOB_PID=$!
                    set +m
                    wait "${ENGINE_RACE_JOB_PID}"
                    engine_status=$?
                    ENGINE_RACE_JOB_PID=""
                fi
                consume_engine_race_report
                report_status=$?
                if (( report_status != 0 )); then
                    # A report transfer failure is a failed UT stage, even if
                    # all test processes themselves exited successfully.  The
                    # source is intentionally retained for diagnostics.
                    engine_status=1
                fi
            fi

            report_cgroup_memory_usage "Resource-heavy UT"
        fi

        if should_run_ut_stage plan; then
            if [[ -n "${PLAN_RACE_JOB_PID}" ]]; then
                wait "${PLAN_RACE_JOB_PID}"
                plan_status=$?
                PLAN_RACE_JOB_PID=""
            else
                start_plan_race "${plan_package}"
                wait "${PLAN_RACE_JOB_PID}"
                plan_status=$?
                PLAN_RACE_JOB_PID=""
            fi
            consume_plan_race_report
            report_status=$?
            if (( report_status != 0 )); then
                plan_status=1
            fi
            rm -f "${PLAN_RACE_TEST_BINARY}"
            PLAN_RACE_TEST_BINARY=""
        fi

        if (( UT_SHARD_ROUTING_ERROR != 0 || light_status != 0 || hnsw_status != 0 || serial_status != 0 || cluster_status != 0 || resource_heavy_status != 0 || engine_status != 0 || plan_status != 0 )); then
            UT_TEST_STATUS=1
        fi
    fi

    # run_ut.sh intentionally does not use errexit because post-processing must
    # still run after a failed package. Preserve go test's status explicitly so
    # a report-parser failure can never replace the authoritative test result.
    if (( UT_TEST_STATUS != 0 )); then
        logger "ERR" "go test failed with status ${UT_TEST_STATUS}; raw report: ${UT_REPORT}"
        report_active_ut_cases
    fi

    # The caller must continue into ut_summary even when go test failed.
    return 0
}

function ut_summary(){
  local report_path="${BUILD_WKSP}/ut-report"
  local analysis_status=0
  local failed_output=""
  local setup_summary=""
  local setup_status=0

  # Keep the workflow's always-run report steps well-defined even when the
  # analyzer cannot parse a truncated/interleaved go test JSON stream.
  mkdir -p "${report_path}/failed/outputs"

  logger "INF" "UT checkpoint artifact: ${UT_CHECKPOINT}"
  if [[ -s "${UT_CHECKPOINT}" ]]; then
    tail -n 40 "${UT_CHECKPOINT}" | sed 's/^/[ut_checkpoint] /'
  fi

  if [[ -s "${UT_REPORT}" ]]; then
    setup_summary=$(python3 "${BUILD_WKSP}/optools/summarize_ut_setup.py" "${UT_REPORT}" 2>&1)
    setup_status=$?
    if (( setup_status == 0 )); then
      while IFS= read -r line; do
        [[ -n "${line}" ]] && logger "INF" "${line}"
      done <<< "${setup_summary}"
    else
      logger "WRN" "failed to summarize fixture setup timings: ${setup_summary}"
    fi
  fi

  if ! install_go_ut_analysis; then
    analysis_status=1
    logger "ERR" "failed to install go-ut-analysis"
  else
    go-ut-analysis test -f "${UT_REPORT}" --first 10 --report-path "${report_path}" --stdout=false
    analysis_status=$?
  fi

  if (( UT_TEST_STATUS != 0 || analysis_status != 0 )); then
    logger "ERR" "UT diagnostics: go-test=${UT_TEST_STATUS}, analysis=${analysis_status}"
    logger "ERR" "Last 200 Go build events (each truncated to 4096 bytes):"
    grep -E '"Action":"build-(output|fail)"' "${UT_REPORT}" | tail -n 200 | cut -c 1-4096
    logger "ERR" "Last 50 raw report lines (each truncated to 4096 bytes):"
    tail -n 50 "${UT_REPORT}" | cut -c 1-4096
    if [[ -s "${UT_STDERR}" ]]; then
      logger "ERR" "Last 50 UT stderr lines:"
      tail -n 50 "${UT_STDERR}" | cut -c 1-4096
    fi
  fi

  failed_output=$(find "${report_path}/failed/outputs" -type f -print -quit)
  if (( UT_TEST_STATUS == 0 && analysis_status == 0 )) &&
     [[ -z "${failed_output}" ]]; then
    logger "INF" "UNIT TESTING SUCCEEDED !!!"
  else
    logger "ERR" "UNIT TESTING FAILED: go-test=${UT_TEST_STATUS}, analysis=${analysis_status}"
    exit 1;
  fi
}

function post_test(){
    local aoe_test=$(find  pkg/vm/engine/aoe/test/* -type d -maxdepth 0)
    for dir in ${aoe_test[@]}; do
        logger "WRN" "Remove $dir"
        rm -rf $dir
    done
}

if [[ 'SCA' == $TEST_TYPE ]]; then
    horiz_rule
    echo "# Examining source code"
    horiz_rule
    run_vet
elif [[ 'UT' == $TEST_TYPE ]]; then
    trap handle_ut_termination TERM
    horiz_rule
    echo "# Running UT"
    horiz_rule
    run_tests

    horiz_rule
    echo "# Post testing"
    horiz_rule
    post_test

    ut_summary
else
    logger "ERR" "Wrong test type"
    exit 1
fi
    
exit 0
