#!/bin/bash

###################################################################
# Title	 : run_unit_test.sh
# Desc.  : Executing unit test cases.
# Author : Matthew Li (lignay@me.com)
###################################################################

function usage() {
	echo "Usage:"
    echo "./run_unit_test.sh VetTestReportName UnitTestReportName SkipTests"
    echo ""
    echo "Options:"
    echo "    VetTestReportName  vet testing report name"
    echo "    UnitTestReportName MatrixOne unit test report name"
    echo "    SkipTests          Skipping test list."
    echo ""
    echo "Example:"
    echo "    $0 vt_reports ut_reports race"
    exit 1
}

function msl() {
    str='*'
    num=80
    v=$(printf "%-${num}s" "$str")
    echo "${v// /*}"
}

function run_vet(){
    msl
    echo "* Examining Go source code"
    msl
    go vet ./pkg/... 2>&1 | tee $VET_RESULT
}

function run_tests(){
    msl
    echo "* Running UT"
    msl
    go clean -testcache

    [[ -f $UT_RESULT ]] && rm $UT_RESULT
    [[ -f $VET_RESULT ]] && rm $VET_RESULT
    [[ -f $UT_FILTER ]] && rm $UT_FILTER
    [[ -f $UT_COUNT ]] && rm $UT_COUNT

    if [[ $SKIP_TESTS == 'race' ]]; then
		echo "Run UT without race check"
        go test -v -timeout "${UT_TIMEOUT}m" -v $(go list ./... | egrep -v "frontend") | tee $UT_RESULT
    else
		echo "Run UT with race check"
        go test -v -race -timeout "${UT_TIMEOUT}m" -v $(go list ./... | egrep -v "frontend") | tee $UT_RESULT
    fi
    egrep -a '^=== RUN *Test[^\/]*$|^\-\-\- PASS: *Test|^\-\-\- FAIL: *Test'  $UT_RESULT > $UT_FILTER
}

function ut_summary(){
    local total=$(cat "$UT_FILTER" | egrep '^=== RUN *Test' | wc -l | xargs)
    local pass=$(cat "$UT_FILTER" | egrep "^\-\-\- PASS: *Test" | wc -l | xargs)
    local fail=$(cat "$UT_FILTER" | egrep "^\-\-\- FAIL: *Test" | wc -l | xargs)
    local unknown=$(cat "$UT_FILTER" | sed '/^=== RUN/{x;p;x;}' | sed -n '/=== RUN/N;/--- /!p' | grep -v '^$' | wc -l | xargs)
    cat << EOF > $UT_COUNT
Total: $total; Passed: $pass; Failed: $fail; Unknown: $unknown

FAILED CASES:
$(cat "$UT_FILTER" | egrep "^\-\-\- FAIL: *Test")

UNKNOWN CASES:
$(cat "$UT_FILTER" | sed '/^=== RUN/{x;p;x;}' | sed -n '/=== RUN/N;/--- /!p' | grep -v '^$')
EOF
    msl
    cat $UT_COUNT
    msl

    if (( $fail > 0 )) || (( $unknown > 0 )); then
      echo "Unit Testing FAILED !!!"
      exit 3
    else
      echo "Unit Testing SUCCEEDED !!!"
    fi
}

if (( $# < 3 )); then
    usage
fi

VET_RESULT=$1
UT_RESULT=$2
SKIP_TESTS=$3

BUILD_WKS="$(pwd)/../"
UT_TIMEOUT=15
UT_FILTER="/tmp/ut_filter"
UT_COUNT="/tmp/ut_count"

cd $BUILD_WKS

run_vet
run_tests
ut_summary || exit $?

exit 0
