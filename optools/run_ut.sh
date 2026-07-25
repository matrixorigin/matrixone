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
go version

BUILD_WKSP=$(dirname "$PWD") && cd $BUILD_WKSP


LOG="$G_TS-$TEST_TYPE.log"
UT_TIMEOUT=${UT_TIMEOUT:-"15"}
UT_PARALLEL=${UT_PARALLEL:-"1"}
HEAVY_RACE_PARALLEL=${HEAVY_RACE_PARALLEL:-"3"}
PLAN_RACE_SHARDS=${PLAN_RACE_SHARDS:-"8"}
UT_COVERAGE_DIR=${UT_COVERAGE_DIR:-""}
SCA_REPORT="$G_WKSP/$G_TS-SCA-Report.out"
UT_REPORT="$G_WKSP/$G_TS-UT-Report.out"
UT_FILTER="$G_WKSP/$G_TS-UT-Filter.out"
UT_COUNT="$G_WKSP/$G_TS-UT-Count.out"
IS_BUILD_FAIL=""
UT_TEST_STATUS=0
TAGS="matrixone_test"
GO_MODULE_MODE="-mod=readonly"
COVERAGE_ENABLED=0
declare -a COVERAGE_BASE_ARGS=()
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

if [[ -n "${UT_COVERAGE_DIR}" && "${SKIP_TESTS}" == "race" ]]; then
    echo "UT_COVERAGE_DIR requires the race-enabled UT path"
    exit 2
fi


function logger(){
    local level=$1
    local msg=$2
    local log=$LOG
    logger_base "$level" "$msg" "$log"
}

function verify_coverage_profile(){
    local profile_path=$1

    if (( COVERAGE_ENABLED )) && [[ ! -s "${profile_path}" ]]; then
        logger "ERR" "Missing or empty race coverage profile: ${profile_path}"
        return 1
    fi

    return 0
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

    # Listing does not run tests, so the process exits before race-detector
    # state can accumulate. Use -race here so race-tagged tests are included.
    LD_LIBRARY_PATH="${LD_LIBRARY_PATH}" \
        CGO_CFLAGS="${CGO_CFLAGS}" \
        CGO_LDFLAGS="${CGO_LDFLAGS}" \
        go test ${GO_MODULE_MODE} -short -race -tags "${TAGS}" \
        -list '^(Test|Fuzz|Example)' "${plan_package}" > "${test_list}"
    list_status=$?
    if (( list_status != 0 )); then
        logger "ERR" "Failed to list tests for ${plan_package}"
        tail -n 200 "${test_list}"
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
        return 2
    fi

    logger "INF" "Run ${test_count} tests in ${plan_package} across ${PLAN_RACE_SHARDS} fresh race-detector processes"
    for (( shard = 0; shard < PLAN_RACE_SHARDS; shard++ )); do
        if (( shard_counts[shard] == 0 )); then
            continue
        fi
        shard_patterns[shard]+=')$'
        logger "INF" "Run ${plan_package} race shard $(( shard + 1 ))/${PLAN_RACE_SHARDS} (${shard_counts[shard]} tests)"
        local -a shard_coverage_args=()
        if (( COVERAGE_ENABLED )); then
            shard_coverage_args=(
                "${COVERAGE_BASE_ARGS[@]}"
                "-coverprofile=${UT_COVERAGE_DIR}/ut-race-plan-${shard}.out"
            )
        fi
        LD_LIBRARY_PATH="${LD_LIBRARY_PATH}" \
            CGO_CFLAGS="${CGO_CFLAGS}" \
            CGO_LDFLAGS="${CGO_LDFLAGS}" \
            go test ${GO_MODULE_MODE} -short -v -json -tags "${TAGS}" \
            -p 1 -count=1 -timeout "${UT_TIMEOUT}m" -race \
            "${shard_coverage_args[@]}" \
            -run "${shard_patterns[shard]}" "${plan_package}" >> "${UT_REPORT}"
        if (( $? != 0 )); then
            shard_status=1
        fi
        if ! verify_coverage_profile "${UT_COVERAGE_DIR}/ut-race-plan-${shard}.out"; then
            shard_status=1
        fi
    done

    return "${shard_status}"
}

function run_tests(){
    cd $BUILD_WKSP
    horiz_rule
    echo "#  BUILD WORKSPACE: $BUILD_WKSP"
    echo "#  SKIPPED TEST:    $SKIP_TESTS"
    echo "#  UT REPORT:       $UT_REPORT"
    echo "#  UT COVERAGE DIR: ${UT_COVERAGE_DIR:-disabled}"
    echo "#  UT TIMEOUT:      $UT_TIMEOUT"
    echo "#  UT PARALLEL:     $UT_PARALLEL"
    echo "#  HEAVY RACE UT:   $HEAVY_RACE_PARALLEL"
    horiz_rule

    logger "INF" "Clean go test cache"
    go clean -testcache

    local test_scope=$(go list ${GO_MODULE_MODE} ./... | grep -v 'driver/aoe' | grep -v 'engine/aoe' | grep -v 'pkg/catalog')
    if [[ -n "${UT_COVERAGE_DIR}" ]]; then
        local coverage_scope
        local coverage_pkgs
        coverage_scope=$(go list ${GO_MODULE_MODE} ./... | grep -v 'driver\|engine/aoe\|engine/memEngine\|pkg/catalog')
        coverage_pkgs=$(echo "${coverage_scope}" | paste -sd, -)
        if [[ -z "${coverage_pkgs}" ]]; then
            logger "ERR" "Race coverage package scope is empty"
            UT_TEST_STATUS=1
            return 0
        fi
        if ! mkdir -p "${UT_COVERAGE_DIR}"; then
            logger "ERR" "Failed to create UT_COVERAGE_DIR: ${UT_COVERAGE_DIR}"
            UT_TEST_STATUS=1
            return 0
        fi
        if find "${UT_COVERAGE_DIR}" -maxdepth 1 -type f -name 'ut-race-*.out' -print -quit | grep -q .; then
            logger "ERR" "UT_COVERAGE_DIR already contains ut-race coverage profiles: ${UT_COVERAGE_DIR}"
            UT_TEST_STATUS=1
            return 0
        fi
        COVERAGE_BASE_ARGS=(
            "-covermode=atomic"
            "-coverpkg=${coverage_pkgs}"
        )
        COVERAGE_ENABLED=1
        logger "INF" "Race UT coverage enabled; profiles will be written to ${UT_COVERAGE_DIR}"
    fi
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
        logger "INF" "Run UT without race check"
        LD_LIBRARY_PATH="${LD_LIBRARY_PATH}" CGO_CFLAGS="${CGO_CFLAGS}" CGO_LDFLAGS="${CGO_LDFLAGS}" go test ${GO_MODULE_MODE} -short -v -json -tags "${TAGS}" -p ${UT_PARALLEL} -timeout "${UT_TIMEOUT}m"  $test_scope > $UT_REPORT
        UT_TEST_STATUS=$?
    else
        logger "INF" "Run UT with race check"
        local plan_package
        local fixed_port_test_scope
        local heavy_test_scope
        local light_test_scope
        local package
        local light_status=0
        local fixed_port_status=0
        local heavy_status=0
        local plan_status=0

        if ! [[ "${HEAVY_RACE_PARALLEL}" =~ ^[1-9][0-9]*$ ]] ||
            (( HEAVY_RACE_PARALLEL > 64 )); then
            logger "ERR" "HEAVY_RACE_PARALLEL must be an integer from 1 through 64, got '${HEAVY_RACE_PARALLEL}'"
            UT_TEST_STATUS=1
            return 0
        fi

        if ! plan_package=$(go list ${GO_MODULE_MODE} ./pkg/sql/plan); then
            logger "ERR" "Failed to resolve ./pkg/sql/plan"
            UT_TEST_STATUS=1
            return 0
        fi

        # NewTestService binds fixed ports. Its callers live in separate test
        # binaries, so a parallel package run can make them collide.
        if ! fixed_port_test_scope=$(go list ${GO_MODULE_MODE} \
            ./pkg/logservice \
            ./pkg/vm/engine/tae/logstore \
            ./pkg/vm/engine/tae/logstore/driver/logservicedriver); then
            logger "ERR" "Failed to resolve fixed-port race-test packages"
            UT_TEST_STATUS=1
            return 0
        fi

        if ! heavy_test_scope=$(go list ${GO_MODULE_MODE} \
            ./pkg/sql/plan/function \
            ./pkg/tests/issues \
            ./pkg/tests/dml \
            ./pkg/tests/shard \
            ./pkg/tests/partition \
            ./pkg/tests/txnexecutor); then
            logger "ERR" "Failed to resolve heavy race-test packages"
            UT_TEST_STATUS=1
            return 0
        fi

        light_test_scope="${test_scope}"
        for package in "${plan_package}" ${fixed_port_test_scope} ${heavy_test_scope}; do
            light_test_scope=$(printf '%s\n' "${light_test_scope}" | grep -Fvx "${package}")
        done

        if [[ -n "${light_test_scope}" ]]; then
            logger "INF" "Run light race-test packages with parallelism ${UT_PARALLEL}"
            local -a light_coverage_args=()
            if (( COVERAGE_ENABLED )); then
                light_coverage_args=(
                    "${COVERAGE_BASE_ARGS[@]}"
                    "-coverprofile=${UT_COVERAGE_DIR}/ut-race-light.out"
                )
            fi
            LD_LIBRARY_PATH="${LD_LIBRARY_PATH}" CGO_CFLAGS="${CGO_CFLAGS}" CGO_LDFLAGS="${CGO_LDFLAGS}" go test ${GO_MODULE_MODE} -short -v -json -tags "${TAGS}" -p ${UT_PARALLEL} -timeout "${UT_TIMEOUT}m" -race "${light_coverage_args[@]}" $light_test_scope > $UT_REPORT
            light_status=$?
            if ! verify_coverage_profile "${UT_COVERAGE_DIR}/ut-race-light.out"; then
                light_status=1
            fi
        else
            : > "${UT_REPORT}"
        fi

        logger "INF" "Run fixed-port race-test packages serially"
        local -a fixed_port_coverage_args=()
        if (( COVERAGE_ENABLED )); then
            fixed_port_coverage_args=(
                "${COVERAGE_BASE_ARGS[@]}"
                "-coverprofile=${UT_COVERAGE_DIR}/ut-race-fixed-port.out"
            )
        fi
        LD_LIBRARY_PATH="${LD_LIBRARY_PATH}" CGO_CFLAGS="${CGO_CFLAGS}" CGO_LDFLAGS="${CGO_LDFLAGS}" go test ${GO_MODULE_MODE} -short -v -json -tags "${TAGS}" -p 1 -timeout "${UT_TIMEOUT}m" -race "${fixed_port_coverage_args[@]}" $fixed_port_test_scope >> $UT_REPORT
        fixed_port_status=$?
        if ! verify_coverage_profile "${UT_COVERAGE_DIR}/ut-race-fixed-port.out"; then
            fixed_port_status=1
        fi

        logger "INF" "Run heavy race-test packages with parallelism ${HEAVY_RACE_PARALLEL}"
        local -a heavy_coverage_args=()
        if (( COVERAGE_ENABLED )); then
            heavy_coverage_args=(
                "${COVERAGE_BASE_ARGS[@]}"
                "-coverprofile=${UT_COVERAGE_DIR}/ut-race-heavy.out"
            )
        fi
        LD_LIBRARY_PATH="${LD_LIBRARY_PATH}" CGO_CFLAGS="${CGO_CFLAGS}" CGO_LDFLAGS="${CGO_LDFLAGS}" go test ${GO_MODULE_MODE} -short -v -json -tags "${TAGS}" -p ${HEAVY_RACE_PARALLEL} -timeout "${UT_TIMEOUT}m" -race "${heavy_coverage_args[@]}" $heavy_test_scope >> $UT_REPORT
        heavy_status=$?
        if ! verify_coverage_profile "${UT_COVERAGE_DIR}/ut-race-heavy.out"; then
            heavy_status=1
        fi

        run_plan_race_shards "${plan_package}"
        plan_status=$?

        if (( light_status != 0 || fixed_port_status != 0 || heavy_status != 0 || plan_status != 0 )); then
            UT_TEST_STATUS=1
        fi

    fi

    # run_ut.sh intentionally does not use errexit because post-processing must
    # still run after a failed package. Preserve go test's status explicitly so
    # a report-parser failure can never replace the authoritative test result.
    if (( UT_TEST_STATUS != 0 )); then
        logger "ERR" "go test failed with status ${UT_TEST_STATUS}; raw report: ${UT_REPORT}"
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

  if ! go install github.com/matrixorigin/go-ut-analysis@latest; then
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
