#!/bin/bash

###################################################################
# Title	 : run_unit_test.sh
# Desc.  : Executing unit test cases.
# Usage  : run_unit_test.sh VetTestReportName UnitTestReportName
# Author : Matthew Li (lignay@me.com)
###################################################################

if [[ $# != 2 ]]; then
    echo "Usage: $0 VetTestReportName UnitTestReportName"
    exit 1
fi

VET_RESULT=$1
UT_RESULT=$2

BUILD_WKS="$(pwd)/../"
UT_TIMEOUT=15
UT_FILTER="ut_filter"
UT_COUNT="ut_count"

cd $BUILD_WKS
[[ -f $UT_RESULT ]] && rm $UT_RESULT
[[ -f $VET_RESULT ]] && rm $VET_RESULT

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
    go test -v -race -timeout "${UT_TIMEOUT}m" -v $(go list ./... | egrep -v "frontend") | tee $UT_RESULT
    egrep '^=== RUN *Test[^\/]*$|^\-\-\- PASS: *Test|^\-\-\- FAIL: *Test'  $UT_RESULT > $UT_FILTER
}

run_vet
run_tests
total=$(cat "$UT_FILTER" | egrep '^=== RUN *Test' | wc -l | xargs)
pass=$(cat "$UT_FILTER" | egrep "^\-\-\- PASS: *Test" | wc -l | xargs)
fail=$(cat "$UT_FILTER" | egrep "^\-\-\- FAIL: *Test" | wc -l | xargs)
unknown=$(( $total - $pass - $fail))
cat << EOF > $UT_COUNT
Total: $total; Passed: $pass; Failed: $fail; Unknown: $unknown

FAILED CASES:
$(egrep "^\-\-\- FAIL: *Test" $UT_FILTER)
EOF
msl
cat $UT_COUNT
msl

[[ -f $UT_FILTER ]] && rm $UT_FILTER
[[ -f $UT_COUNT ]] && rm $UT_COUNT

if (( $fail > 0 )) || (( $unknown > 0 )); then
  echo "Unit Testing FAILED !!!"
  exit 2
else
  echo "Unit Testing SUCCEEDED !!!"
fi

exit 0