#!/bin/bash

set -euo pipefail

SECONDS=0

# mv log to mount path
function packLog() {
    mv /matrixone-test/mo-service.log /matrixone-test/tester-log
    mv /matrixone-test/mo-tester/report /matrixone-test/tester-log
    mv /root/scratch /matrixone-test/tester-log

    duration=$SECONDS
    echo "$(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed."
}

function prepare() {
  mkdir /root/scratch
  echo ">>>>>>>>>>>>>>>>>>>>>>>> show locale"
  echo `locale`

  echo ">>>>>>>>>>>>>>>>>>>>>>>> show launch"
  echo "$LAUNCH"

  echo ">>>>>>>>>>>>>>>>>>>>>>> show go env"
  echo `go env`

  echo ">>>>>>>>>>>>>>>>>>>>>>>> clone mo-tester"
  git clone --depth=1 https://github.com/matrixorigin/mo-tester.git
}


function run_ut() {
  echo ">>>>>>>>>>>>>>>>>>>>>>>> run unit test"
  make ut UT_PARALLEL=${UT_PARALLEL}
}

function run_bvt() {
  echo ">>>>>>>>>>>>>>>>>>>>>>>> build mo service"
  make build

  echo ">>>>>>>>>>>>>>>>>>>>>>>> start mo service"
   ./optools/run_bvt.sh ./ "${LAUNCH}"

  echo ">>>>>>>>>>>>>>>>>>>>>>>> start bvt"
  if [[ "$LAUNCH" == "launch-tae-logservice" ]]; then
    echo "> test case: test/cases"
    cd mo-tester && ./run.sh -n -g -p /matrixone-test/test/cases 2>&1
  else
    # use test/distributed/cases as default test cases
    echo "> test case: test/distributed/cases"
    cd mo-tester && ./run.sh -n -g -p /matrixone-test/test/distributed/cases 2>&1
  fi
}

function bvt_ut() {
  trap "packLog" EXIT

  prepare

  if [[ "$ENABLE_UT" == "true" ]]; then
    echo ">>>>>>>>>>>>>>>>>>>>>>>> enabled ut"
    run_ut
  else
    echo ">>>>>>>>>>>>>>>>>>>>>>>> disabled ut"
  fi

  run_bvt
}

bvt_ut