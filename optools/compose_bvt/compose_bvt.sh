#!/bin/bash

set -euo pipefail

if (( $# < 2 )); then
    echo "usage: $0 <matrixone-workspace> <compose-profile>" >&2
    exit 2
fi

MO_WORKSPACE=$1
COMPOSE_LAUNCH=$2
COMPOSE_FILE="${MO_WORKSPACE}/etc/launch-tae-compose/compose.yaml"

function export_logs() {
    cd -- "${MO_WORKSPACE}" || return 0
    curl --fail --silent --show-error "http://localhost:12345/debug/pprof/goroutine?debug=2" \
        -o docker-compose-log/cn-0-dump-stacks || true
    curl --fail --silent --show-error "http://localhost:22345/debug/pprof/goroutine?debug=2" \
        -o docker-compose-log/cn-1-dump-stacks || true
}

function cleanup_compose() {
    # Preserve the tester's exit status while making service teardown
    # best-effort. This trap owns every service started by this script,
    # including both CN-local Python workers.
    set +e
    export_logs
    docker compose -f "${COMPOSE_FILE}" --profile "${COMPOSE_LAUNCH}" down --remove-orphans

}

function compose_bvt() {
    trap cleanup_compose EXIT

    cd -- "${MO_WORKSPACE}"

    docker compose -f "${COMPOSE_FILE}" --profile "${COMPOSE_LAUNCH}" up -d --build
    docker build -t matrixorigin/compose-tester:local -f optools/compose_bvt/Dockerfile.tester .
    docker run -it --name compose-tester --privileged --network launch-tae-compose_monet \
        -v "${MO_WORKSPACE}/docker-compose-log:/test" --rm matrixorigin/compose-tester:local
}

#create the dir for export logs
python3 - "${MO_WORKSPACE}/docker-compose-log" <<'PY'
import pathlib
import shutil
import sys

target = pathlib.Path(sys.argv[1]).resolve()
if target == target.parent or target == pathlib.Path("/"):
    raise SystemExit(f"refusing unsafe log directory: {target}")
shutil.rmtree(target, ignore_errors=True)
target.mkdir(parents=True, exist_ok=True)
PY

compose_bvt
