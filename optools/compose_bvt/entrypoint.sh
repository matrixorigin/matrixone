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
    while [ $(mysql -h cn0 -P 6001 -u dump -p111 --execute 'create database if not exists compose_test;use compose_test; create table if not exists compose_test_table(col1 int auto_increment primary key);show tables;' 2>&1 | tee /dev/stderr | grep 'compose_test_table' | wc -l) -lt 1 ]; do
      echo "wait mo init finished...$i"
      if [ $i -ge 300 ]; then
        echo "wait for $i seconds, mo init not finish, so exit 1"
        docker ps
        exit 1;
      fi
      i=$(($i+1))
      sleep 1
    done
    cd /mo-tester && ./run.sh -n -g -p /matrixone/test/distributed/cases/ -s /test/distributed/resources 2>&1
}

run_bvt
