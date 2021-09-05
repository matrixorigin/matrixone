#!/bin/bash
#===============================================================================
#
#          FILE: run_bvt.sh
#   DESCRIPTION: 
#        AUTHOR: Matthew Li, lignay@me.com
#  ORGANIZATION: 
#       CREATED: 09/01/2021
#      REVISION:  ---
#===============================================================================

set -o nounset                                  # Treat unset variables as an error
#set -exuo pipefail

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

source $HOME/.bash_profile
source ./utilities.sh

BUILD_WKSP=$(dirname "$PWD") && cd $BUILD_WKSP

UT_TIMEOUT=15
LOG="$G_TS-$TEST_TYPE.log"
SCA_REPORT="$G_WKSP/$G_TS-SCA-Report.txt"
UT_REPORT="$G_WKSP/$G_TS-UT-Report.txt"
UT_FILTER="$G_WKSP/$G_TS-UT-Filter"
UT_COUNT="$G_WKSP/$G_TS-UT-Count"

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

function run_vet(){
    if [[ -f $SCA_REPORT ]]; then rm $SCA_REPORT; fi
	logger "INF" "Test is in progress... "
    go vet ./pkg/... 2>&1 | tee $SCA_REPORT
	logger "INF" "Refer to $SCA_REPORT for details"
}

function run_tests(){
	logger "INF" "Clean go test cache"
    go clean -testcache

    if [[ $SKIP_TESTS == 'race' ]]; then
        logger "INF" "Run UT without race check"
        go test -v -timeout "${UT_TIMEOUT}m" -v $(go list ./... | egrep -v "frontend") | tee $UT_REPORT
    else
        logger "INF" "Run UT with race check"
        go test -v -race -timeout "${UT_TIMEOUT}m" -v $(go list ./... | egrep -v "frontend") | tee $UT_REPORT
    fi
    egrep -a '^=== RUN *Test[^\/]*$|^\-\-\- PASS: *Test|^\-\-\- FAIL: *Test'  $UT_REPORT > $UT_FILTER
	logger "INF" "Refer to $UT_REPORT for details"
}

function ut_summary(){
    local total=$(cat "$UT_FILTER" | egrep '^=== RUN *Test' | wc -l | xargs)
    local pass=$(cat "$UT_FILTER" | egrep "^\-\-\- PASS: *Test" | wc -l | xargs)
    local fail=$(cat "$UT_FILTER" | egrep "^\-\-\- FAIL: *Test" | wc -l | xargs)
    local unknown=$(cat "$UT_FILTER" | sed '/^=== RUN/{x;p;x;}' | sed -n '/=== RUN/N;/--- /!p' | grep -v '^$' | wc -l | xargs)
    cat << EOF > $UT_COUNT
# Total: $total; Passed: $pass; Failed: $fail; Unknown: $unknown
# 
# FAILED CASES:
# $(cat "$UT_FILTER" | egrep "^\-\-\- FAIL: *Test" | xargs)
# 
# UNKNOWN CASES:
# $(cat "$UT_FILTER" | sed '/^=== RUN/{x;p;x;}' | sed -n '/=== RUN/N;/--- /!p' | grep -v '^$' | xargs)
EOF
    horiz_rule
    cat $UT_COUNT
    horiz_rule
    if (( $fail > 0 )) || (( $unknown > 0 )); then
      logger "INF" "UNIT TESTING FAILED !!!"
      exit 3
    else
      logger "INF" "UNIT TESTING SUCCEEDED !!!"
    fi
}

function teardown(){
	local aoe_test=$(find  pkg/vm/engine/aoe/test/* -type d -maxdepth 0)
	for dir in ${aoe_test[@]}; do
		logger "WRN" "Remove $dir"
		rm -rf $dir
	done
}
horiz_rule
echo "#  BUILD WORKSPACE: $BUILD_WKSP"
echo "#  UT REPORT:       $UT_REPORT"
echo "#  SCA REPORT:      $SCA_REPORT"
echo "#  SKIPPED TEST:    $SKIP_TESTS"
echo "#  CONTAINER ID:    $G_CONT_ID"
echo "#  UT TIMEOUT:      $UT_TIMEOUT"
horiz_rule

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
    echo "# Teardown UT"
    horiz_rule
	teardown    

    ut_summary || exit $?
else
    logger "ERR" "Wrong test type"
    exit 1
fi


exit 0
