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
    while [ $(mariadb -h cn0 -P 6001 -u dump -p111 --execute 'show databases;' 2>&1 | wc -l) -le 1 ]; do
        echo 'wait mo init finished'
        sleep 1
    done
    cd /mo-tester && ./run.sh -n -g -p /matrixone/test/distributed/cases/ -s /test/distributed/resources 2>&1
}

run_bvt