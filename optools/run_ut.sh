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
go version

BUILD_WKSP=$(dirname "$PWD") && cd $BUILD_WKSP


LOG="$G_TS-$TEST_TYPE.log"
UT_TIMEOUT=${UT_TIMEOUT:-"15"}
UT_PARALLEL=${UT_PARALLEL:-"1"}
HEAVY_RACE_PARALLEL=${HEAVY_RACE_PARALLEL:-"3"}
PLAN_RACE_SHARDS=${PLAN_RACE_SHARDS:-"8"}
# Two shards cut the measured engine/test race runtime roughly in half while
# keeping the default heavy-stage memory/process budget bounded.
ENGINE_RACE_SHARDS=2
SCA_REPORT="$G_WKSP/$G_TS-SCA-Report.out"
UT_REPORT="$G_WKSP/$G_TS-UT-Report.out"
UT_FILTER="$G_WKSP/$G_TS-UT-Filter.out"
UT_COUNT="$G_WKSP/$G_TS-UT-Count.out"
CODE_COVERAGE="$G_WKSP/$G_TS-UT-Coverage.html"
RAW_COVERAGE="coverage.out"
IS_BUILD_FAIL=""
UT_TEST_STATUS=0
PLAN_RACE_TEST_BINARY=""
ENGINE_RACE_TEST_BINARY=""
ENGINE_RACE_JOB_PID=""
ENGINE_RACE_REPORT=""
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
if [[ -f $UT_FILTER ]]; then rm $UT_FILTER; fi
if [[ -f $UT_COUNT ]]; then rm $UT_COUNT; fi


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

function handle_ut_termination(){
    trap - TERM
    if [[ -n "${ENGINE_RACE_JOB_PID}" ]]; then
        kill -TERM "${ENGINE_RACE_JOB_PID}" 2>/dev/null || true
        wait "${ENGINE_RACE_JOB_PID}" 2>/dev/null || true
        ENGINE_RACE_JOB_PID=""
    fi
    if [[ -n "${ENGINE_RACE_REPORT}" ]]; then
        local partial_report
        for partial_report in "${ENGINE_RACE_REPORT}".*; do
            if [[ -f "${partial_report}" ]]; then
                cat "${partial_report}" >> "${UT_REPORT}"
            fi
        done
    fi
    if [[ -n "${ENGINE_RACE_TEST_BINARY}" ]]; then
        rm -f "${ENGINE_RACE_TEST_BINARY}"
        ENGINE_RACE_TEST_BINARY=""
    fi
    if [[ -n "${ENGINE_RACE_REPORT}" ]]; then
        rm -f "${ENGINE_RACE_REPORT}" "${ENGINE_RACE_REPORT}".*
        ENGINE_RACE_REPORT=""
    fi
    if [[ -n "${PLAN_RACE_TEST_BINARY}" ]]; then
        rm -f "${PLAN_RACE_TEST_BINARY}"
        PLAN_RACE_TEST_BINARY=""
    fi
    logger "ERR" "UT runner received SIGTERM; reporting work without terminal Go test events"
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

    trap 'for pid in "${child_pids[@]}"; do if (( pid > 0 )); then kill -TERM -- "-${pid}" 2>/dev/null || true; fi; done; wait; rm -f "${metadata_file}"; exit 143' TERM
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
        trap - TERM
        set +m
        return 2
    fi
    rm -f "${metadata_file}"

    : > "${ENGINE_RACE_REPORT}"
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
        trap - TERM
        set +m
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
        trap - TERM
        set +m
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
        trap - TERM
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
        cat "${shard_reports[shard]}" >> "${ENGINE_RACE_REPORT}"
    done
    trap - TERM

    rm -f "${ENGINE_RACE_TEST_BINARY}" "${ENGINE_RACE_REPORT}".*
    ENGINE_RACE_TEST_BINARY=""
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
    local plan_test_binary="${G_WKSP}/${G_TS}-plan-race.test"
    local plan_package_dir=""
    local plan_package_import=""
    local build_status=0
    local list_status=0
    local shard_status=0
    local test_name=""
    local shard=0
    local test_count=0
    local -a shard_patterns
    local -a shard_counts

    if ! [[ "${PLAN_RACE_SHARDS}" =~ ^[1-9][0-9]*$ ]] ||
        (( PLAN_RACE_SHARDS > 64 )); then
        logger "ERR" "PLAN_RACE_SHARDS must be an integer from 1 through 64, got '${PLAN_RACE_SHARDS}'"
        return 2
    fi

    if ! plan_package_dir=$(go list ${GO_MODULE_MODE} \
        -f '{{.Dir}}' "${plan_package}"); then
        logger "ERR" "Failed to resolve package metadata for ${plan_package}"
        return 2
    fi
    if ! plan_package_import=$(go list ${GO_MODULE_MODE} \
        -f '{{.ImportPath}}' "${plan_package}"); then
        logger "ERR" "Failed to resolve package import path for ${plan_package}"
        return 2
    fi

    # Compile and link the race-instrumented test binary once. Each shard still
    # runs in a fresh process, preserving race-detector and package-global
    # isolation without repeating the same link action for every shard.
    PLAN_RACE_TEST_BINARY="${plan_test_binary}"
    LD_LIBRARY_PATH="${LD_LIBRARY_PATH}" \
        CGO_CFLAGS="${CGO_CFLAGS}" \
        CGO_LDFLAGS="${CGO_LDFLAGS}" \
        go test ${GO_MODULE_MODE} ${GO_TEST_VET_FLAGS} -short -race -tags "${TAGS}" \
        -c -o "${plan_test_binary}" "${plan_package}" > "${build_log}" 2>&1
    build_status=$?
    if (( build_status != 0 )); then
        logger "ERR" "Failed to build race test binary for ${plan_package}"
        tail -n 200 "${build_log}"
        rm -f "${plan_test_binary}"
        PLAN_RACE_TEST_BINARY=""
        return "${build_status}"
    fi

    # Test binaries normally execute with the package source directory as cwd.
    # Preserve that contract for both discovery and shard execution.
    (
        cd "${plan_package_dir}" || exit 2
        LD_LIBRARY_PATH="${LD_LIBRARY_PATH}" \
            "${plan_test_binary}" -test.short=true \
            -test.list='^(Test|Fuzz|Example)'
    ) > "${test_list}" 2>&1
    list_status=$?
    if (( list_status != 0 )); then
        logger "ERR" "Failed to list tests for ${plan_package}"
        tail -n 200 "${test_list}"
        rm -f "${plan_test_binary}"
        PLAN_RACE_TEST_BINARY=""
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
        return 2
    fi

    logger "INF" "Run ${test_count} tests in ${plan_package} across ${PLAN_RACE_SHARDS} fresh race-detector processes"
    for (( shard = 0; shard < PLAN_RACE_SHARDS; shard++ )); do
        if (( shard_counts[shard] == 0 )); then
            continue
        fi
        shard_patterns[shard]+=')$'
        logger "INF" "Run ${plan_package} race shard $(( shard + 1 ))/${PLAN_RACE_SHARDS} (${shard_counts[shard]} tests)"
        (
            cd "${plan_package_dir}" || exit 2
            LD_LIBRARY_PATH="${LD_LIBRARY_PATH}" \
                go tool test2json -t -p "${plan_package_import}" \
                "${plan_test_binary}" -test.short=true -test.v=test2json \
                -test.paniconexit0=true -test.count=1 \
                -test.timeout="${UT_TIMEOUT}m" \
                -test.run="${shard_patterns[shard]}"
        ) >> "${UT_REPORT}"
        if (( $? != 0 )); then
            shard_status=1
        fi
    done

    rm -f "${plan_test_binary}"
    PLAN_RACE_TEST_BINARY=""
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

function run_tests(){
    cd $BUILD_WKSP
    horiz_rule
    echo "#  BUILD WORKSPACE: $BUILD_WKSP"
    echo "#  SKIPPED TEST:    $SKIP_TESTS"
    echo "#  UT REPORT:       $UT_REPORT"
    echo "#  COVERAGE REPORT: $CODE_COVERAGE"
    echo "#  UT TIMEOUT:      $UT_TIMEOUT"
    echo "#  UT PARALLEL:     $UT_PARALLEL"
    echo "#  CLUSTER ADMISSION: process lifecycle"
    echo "#  HEAVY RACE UT:   $HEAVY_RACE_PARALLEL package slots (+1 low-CPU engine shard)"
    horiz_rule

    logger "INF" "Clean go test cache"
    go clean -testcache

    local test_scope=$(go list ${GO_MODULE_MODE} ./... | grep -v 'driver/aoe' | grep -v 'engine/aoe' | grep -v 'pkg/catalog')
    local leave_out=$(egrep -lr  --include="*.go" 'Code generated by protoc-gen-gogo. DO NOT EDIT.' ./pkg/* | sort -u | xargs basename -a)
    logger "INF" "Ingore code coverage $(echo ${leave_out[@]}|tr " " "|")"
    local cover_profile='profile.raw'
    make cgo
    make thirdparties

    # Compile and link a CGo-transitive package through the same deterministic
    # CPU wrapper documented for local development. This catches drift between
    # libmo's declared native dependencies and the wrapper before the full UT
    # matrix obscures it among unrelated package output. GPU builds have a
    # separate, explicit CUDA/cuVS link contract.
    if [[ "${MO_CL_CUDA:-0}" != "1" ]]; then
        logger "INF" "Smoke test the deterministic CGo test wrapper"
        if ! .agents/skills/mo-dev/scripts/mo-cgo-test \
            -count=1 -timeout=120s ./optools/testdata/mo_cgo_transitive; then
            logger "ERR" "Deterministic CGo test wrapper smoke failed"
            exit 1
        fi
    fi

    if [[ $SKIP_TESTS == 'race' ]]; then
        logger "INF" "Run UT packages with parallelism ${UT_PARALLEL} and process-lifecycle cluster admission"
        LD_LIBRARY_PATH="${LD_LIBRARY_PATH}" CGO_CFLAGS="${CGO_CFLAGS}" CGO_LDFLAGS="${CGO_LDFLAGS}" go test ${GO_MODULE_MODE} ${GO_TEST_VET_FLAGS} -short -v -json -tags "${TAGS}" -p ${UT_PARALLEL} -timeout "${UT_TIMEOUT}m" $test_scope > $UT_REPORT
        UT_TEST_STATUS=$?
    else
        logger "INF" "Run UT with race check"
        local plan_package
        local engine_package
        local serial_test_scope
        local cluster_test_scope
        local resource_heavy_test_scope
        local light_test_scope
        local package
        local cluster_package_parallel=2
        local package_status=0
        local light_status=0
        local serial_status=0
        local cluster_status=0
        local resource_heavy_status=0
        local engine_status=0
        local plan_status=0
        local resource_heavy_parallel=1
        local engine_race_parallel=1
        local shard_engine=1

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

        # These packages need exclusive runner access. NewTestService callers
        # bind fixed ports, while the issues packages intentionally keep embedded
        # clusters alive for most of their test processes.
        if ! serial_test_scope=$(go list ${GO_MODULE_MODE} \
            ./pkg/logservice \
            ./pkg/vm/engine/tae/logstore \
            ./pkg/vm/engine/tae/logstore/driver/logservicedriver \
            ./pkg/tests/issues \
            ./pkg/tests/issues/isolated); then
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
            ${serial_test_scope})

        # Dependency-based group precedence remains authoritative. If this
        # package ever starts owning an embedded cluster, keep it in that
        # serialized lifecycle group instead of running it a second time here.
        if printf '%s\n%s\n' "${serial_test_scope}" "${cluster_test_scope}" |
            grep -Fxq "${engine_package}"; then
            shard_engine=0
            logger "INF" "Keep ${engine_package} in its higher-precedence race-test group"
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
            ${serial_test_scope} \
            ${cluster_test_scope})

        light_test_scope=$(remove_packages_from_scope \
            "${test_scope}" \
            "${plan_package}" \
            "${engine_package}" \
            ${serial_test_scope} \
            ${cluster_test_scope} \
            ${resource_heavy_test_scope})

        if [[ -n "${light_test_scope}" ]]; then
            logger "INF" "Run light race-test packages with parallelism ${UT_PARALLEL}"
            LD_LIBRARY_PATH="${LD_LIBRARY_PATH}" CGO_CFLAGS="${CGO_CFLAGS}" CGO_LDFLAGS="${CGO_LDFLAGS}" go test ${GO_MODULE_MODE} ${GO_TEST_VET_FLAGS} -short -v -json -tags "${TAGS}" -p ${UT_PARALLEL} -timeout "${UT_TIMEOUT}m" -race $light_test_scope > $UT_REPORT
            light_status=$?
        else
            : > "${UT_REPORT}"
        fi

        logger "INF" "Run exclusive race-test packages serially"
        for package in ${serial_test_scope}; do
            logger "INF" "Run exclusive race-test package ${package}"
            LD_LIBRARY_PATH="${LD_LIBRARY_PATH}" CGO_CFLAGS="${CGO_CFLAGS}" CGO_LDFLAGS="${CGO_LDFLAGS}" go test ${GO_MODULE_MODE} ${GO_TEST_VET_FLAGS} -short -v -json -tags "${TAGS}" -p 1 -timeout "${UT_TIMEOUT}m" -race "${package}" >> $UT_REPORT
            package_status=$?
            if (( package_status != 0 )); then
                serial_status=1
                logger "ERR" "Exclusive race-test package ${package} failed with status ${package_status}"
            fi
        done

        # These packages link embedded clusters with substantial race-detector
        # memory. The runner-wide file-lock admission keeps complete cluster
        # lifecycles serialized across test binaries. Allow one additional
        # package process to overlap linking, setup, and non-cluster work without
        # returning to the six-way contention that starved HAKeeper.
        logger "INF" "Run embedded-cluster race-test packages with package parallelism ${cluster_package_parallel} and serialized cluster lifecycle admission"
        LD_LIBRARY_PATH="${LD_LIBRARY_PATH}" CGO_CFLAGS="${CGO_CFLAGS}" CGO_LDFLAGS="${CGO_LDFLAGS}" go test ${GO_MODULE_MODE} ${GO_TEST_VET_FLAGS} -short -v -json -tags "${TAGS}" -p "${cluster_package_parallel}" -timeout "${UT_TIMEOUT}m" -race $cluster_test_scope >> $UT_REPORT
        cluster_status=$?

        if (( shard_engine == 1 )); then
            # engine/test is dominated by serial fixture lifecycles inside one
            # process. Build it once and split every discovered top-level test
            # across fresh race processes. The effective shard count and the
            # remaining go-test parallelism use one extra process over the old
            # package budget. Locally the two shards peak at 3.75 GB combined,
            # only 1.1 GB above the old single process, while cutting runtime
            # from 179s to 97s. Low custom budgets keep sequential waves.
            engine_race_parallel=${ENGINE_RACE_SHARDS}
            if (( engine_race_parallel > HEAVY_RACE_PARALLEL )); then
                engine_race_parallel=${HEAVY_RACE_PARALLEL}
            fi
            resource_heavy_parallel=$(( HEAVY_RACE_PARALLEL - engine_race_parallel + 1 ))
            if (( HEAVY_RACE_PARALLEL <= engine_race_parallel )); then
                resource_heavy_parallel=0
            fi
            ENGINE_RACE_TEST_BINARY="${G_WKSP}/${G_TS}-engine-race.test"
            ENGINE_RACE_REPORT="${G_WKSP}/${G_TS}-engine-race-report.out"

            if (( resource_heavy_parallel > 0 )); then
                run_engine_race_shards "${engine_package}" "${engine_race_parallel}" &
                ENGINE_RACE_JOB_PID=$!
            else
                resource_heavy_parallel=${HEAVY_RACE_PARALLEL}
            fi
        else
            resource_heavy_parallel=${HEAVY_RACE_PARALLEL}
        fi

        logger "INF" "Run remaining resource-heavy race-test packages with parallelism ${resource_heavy_parallel}"
        LD_LIBRARY_PATH="${LD_LIBRARY_PATH}" CGO_CFLAGS="${CGO_CFLAGS}" CGO_LDFLAGS="${CGO_LDFLAGS}" go test ${GO_MODULE_MODE} ${GO_TEST_VET_FLAGS} -short -v -json -tags "${TAGS}" -p ${resource_heavy_parallel} -timeout "${UT_TIMEOUT}m" -race $resource_heavy_test_scope >> $UT_REPORT
        resource_heavy_status=$?

        if (( shard_engine == 1 )); then
            if [[ -n "${ENGINE_RACE_JOB_PID}" ]]; then
                wait "${ENGINE_RACE_JOB_PID}"
                engine_status=$?
                ENGINE_RACE_JOB_PID=""
            else
                # Keep the helper's process-group TERM trap scoped to a
                # subshell even when a low budget requires sequential waves.
                run_engine_race_shards "${engine_package}" "${engine_race_parallel}" &
                ENGINE_RACE_JOB_PID=$!
                wait "${ENGINE_RACE_JOB_PID}"
                engine_status=$?
                ENGINE_RACE_JOB_PID=""
            fi
            if [[ -s "${ENGINE_RACE_REPORT}" ]]; then
                cat "${ENGINE_RACE_REPORT}" >> "${UT_REPORT}"
            fi
            rm -f "${ENGINE_RACE_TEST_BINARY}" "${ENGINE_RACE_REPORT}" "${ENGINE_RACE_REPORT}".*
            ENGINE_RACE_TEST_BINARY=""
            ENGINE_RACE_REPORT=""
        fi

        report_cgroup_memory_usage "Resource-heavy UT"

        run_plan_race_shards "${plan_package}"
        plan_status=$?

        if (( light_status != 0 || serial_status != 0 || cluster_status != 0 || resource_heavy_status != 0 || engine_status != 0 || plan_status != 0 )); then
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

  # Keep the workflow's always-run report steps well-defined even when the
  # analyzer cannot parse a truncated/interleaved go test JSON stream.
  mkdir -p "${report_path}/failed/outputs"

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
