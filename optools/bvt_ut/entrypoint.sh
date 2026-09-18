#!/bin/bash

set -euo pipefail

SECONDS=0

# Move logs to the mounted path without masking the original CI failure when
# preparation or compilation stops before one of the normal artifacts exists.
function packLog() {
    local log_dir=/matrixone-test/tester-log
    if ! mkdir -p "$log_dir"; then
        echo "failed to create $log_dir; preserving the original CI failure" >&2
        return 0
    fi
    for artifact in \
        /matrixone-test/mo-service.log \
        /matrixone-test/mo-tester/report \
        /root/scratch; do
        if [[ ! -e "$artifact" ]]; then
            continue
        fi
        if ! mv "$artifact" "$log_dir"; then
            echo "failed to archive $artifact" >&2
        fi
    done

    duration=$SECONDS
    echo "$(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed."
}

function prepare() {
  mkdir -p /root/scratch
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
   # Source the launcher so this entrypoint owns the service children until
   # mo-tester finishes. A standalone run_bvt.sh invocation still returns
   # with services alive for callers that launch the tester separately.
   source ./optools/run_bvt.sh ./ "${LAUNCH}"
   trap 'cleanup_all; packLog' EXIT

  echo ">>>>>>>>>>>>>>>>>>>>>>>> start bvt"
   # use test/distributed/cases as default test cases
  echo "> test case: test/distributed/cases"
  cd mo-tester && ./run.sh -n -g -o -p /matrixone-test/test/distributed/cases -e optimistic 2>&1
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
