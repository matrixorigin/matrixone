#!/bin/bash

set -e

MO_WORKSPACE=$1
COMPOSE_LAUNCH=$2

function export_logs() {
    cd ${MO_WORKSPACE}
    sed -i.bak 's/enable-sacrificing-freshness = true/enable-sacrificing-freshness = false/' ./etc/launch-tae-compose/config/cn-0.toml
    sed -i.bak 's/enable-sacrificing-freshness = true/enable-sacrificing-freshness = false/' ./etc/launch-tae-compose/config/cn-1.toml
    rm -f ./etc/launch-tae-compose/config/*.bak
    curl http://localhost:12345/debug/pprof/goroutine\?debug=2 -o docker-compose-log/cn-0-dump-stacks.log
    curl http://localhost:22345/debug/pprof/goroutine\?debug=2 -o docker-compose-log/cn-1-dump-stacks.log

}

function compose_bvt() {
    trap "export_logs" EXIT

    cd ${MO_WORKSPACE}
    sed -i.bak 's/enable-sacrificing-freshness = false/enable-sacrificing-freshness = true/' ./etc/launch-tae-compose/config/cn-0.toml
    sed -i.bak 's/enable-sacrificing-freshness = false/enable-sacrificing-freshness = true/' ./etc/launch-tae-compose/config/cn-1.toml

    docker compose -f etc/launch-tae-compose/compose.yaml --profile "${COMPOSE_LAUNCH}" up -d --build
    docker build -t matrixorigin/compose_tester:local -f optools/compose_bvt/Dockerfile.tester .
    docker run -it --name compose-tester --privileged --network launch-tae-compose_monet -v ${MO_WORKSPACE}/docker-compose-log:/test --rm matrixorigin/compose_tester:local
    exit 0
}

#create the dir for export logs
rm -rf ${MO_WORKSPACE}/docker-compose-log && mkdir -p ${MO_WORKSPACE}/docker-compose-log

compose_bvt

exit $?
