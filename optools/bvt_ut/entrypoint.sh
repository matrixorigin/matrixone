#!/bin/bash

SECONDS=0

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
   ./optools/run_bvt.sh ./ ${LAUNCH}

  echo ">>>>>>>>>>>>>>>>>>>>>>>> start bvt"
  cd mo-tester && ./run.sh -n -g -p /matrixone-test/test/cases 2>&1
}

function bvt_ut() {
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
trap "packLog" EXIT
