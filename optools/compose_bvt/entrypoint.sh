#!/bin/bash

set -euo pipefail

SECONDS=0

# mv log to mount path
function packLog() {
    mv /mo-tester/report /test/

    duration=$SECONDS
    echo "$(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed."
}

function run_bvt() {
    trap packLog EXIT
    # wait for ready
    i=0
    while true; do
      if mysql -h cn0 -P 6001 -u dump -p111 --execute "show databases;"; then
        break;
      fi
      if [ $i -ge 300 ]; then
        echo "wait for $i seconds, mo init not finish, so exit 1"
        docker ps
        exit 1;
      fi
      i=$(($i+1))
      sleep 1
    done
    cd /mo-tester && ./run.sh -n -g -p /matrixone/test/distributed/cases/ -s /test/distributed/resources -e optimistic 2>&1
}

run_bvt
