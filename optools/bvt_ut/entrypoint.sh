#!/bin/bash

SECONDS=0

function packLog() {
    mv /matrixone-test/mo-service.log /matrixone-test/tester-log
    mv /matrixone-test/mo-tester/report /matrixone-test/tester-log
    mv /root/scratch /matrixone-test/tester-log

    duration=$SECONDS
    echo "$(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed."
}

echo ">>>>>>>>>>>>>>>>>>>>>>>> show locale"
echo `locale`

echo ">>>>>>>>>>>>>>>>>>>>>>>> clone mo-tester"
git clone --depth=1 https://github.com/matrixorigin/mo-tester.git


echo ">>>>>>>>>>>>>>>>>>>>>>>> run unit test"
make ut UT_PARALLEL=${UT_PARALLEL}

echo ">>>>>>>>>>>>>>>>>>>>>>>> build mo service"
make build 

echo ">>>>>>>>>>>>>>>>>>>>>>>> start mo service"
 ./optools/run_bvt.sh ./ ${LAUNCH}

echo ">>>>>>>>>>>>>>>>>>>>>>>> start bvt"
cd mo-tester && ./run.sh -n -g -p /matrixone-test/test/cases 2>&1

trap "packLog" EXIT
