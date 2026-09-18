#!/bin/bash

# Copyright 2021 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o nounset

if (( $# < 2 )); then
    echo "usage: $0 <matrixone-workspace> <launch-config> [proxy-args ...]" >&2
    exit 2
fi

MO_WORKSPACE=$1
LAUNCH=$2
shift 2
PROXY_ARGS=("$@")
LAUNCH_CONFIG=$LAUNCH
MO_PID=""
JSTFU_PID=""

function collect_descendants() {
    local parent="$1"
    local child
    for child in $(pgrep -P "$parent" 2>/dev/null || true); do
        BVT_CHILD_PIDS+=("$child")
        collect_descendants "$child"
    done
}

function cleanup_mo() {
    if [[ -z "$MO_PID" ]]; then
        return
    fi
    local pid="$MO_PID"
    local -a BVT_CHILD_PIDS=()
    collect_descendants "$pid"
    if kill -0 "$MO_PID" 2>/dev/null; then
        kill -TERM "$MO_PID" 2>/dev/null || true
        for _ in {1..30}; do
            if ! kill -0 "$MO_PID" 2>/dev/null; then
                break
            fi
            sleep 1
        done
        if kill -0 "$MO_PID" 2>/dev/null; then
            kill -KILL "$MO_PID" 2>/dev/null || true
        fi
    fi
    if ((${#BVT_CHILD_PIDS[@]} > 0)); then
        for child in "${BVT_CHILD_PIDS[@]}"; do
            kill -KILL "$child" 2>/dev/null || true
        done
    fi
    wait "$pid" 2>/dev/null || true
    MO_PID=""
}

function cleanup_jstfu() {
    if [[ -z "$JSTFU_PID" ]]; then
        return
    fi
    if kill -0 "$JSTFU_PID" 2>/dev/null; then
        kill -TERM "$JSTFU_PID" 2>/dev/null || true
        for _ in {1..15}; do
            if ! kill -0 "$JSTFU_PID" 2>/dev/null; then
                break
            fi
            sleep 1
        done
        if kill -0 "$JSTFU_PID" 2>/dev/null; then
            kill -KILL "$JSTFU_PID" 2>/dev/null || true
        fi
    fi
    wait "$JSTFU_PID" 2>/dev/null || true
    JSTFU_PID=""
}

function cleanup_all() {
    cleanup_jstfu
    cleanup_mo
}

trap 'cleanup_all; exit 130' INT
trap 'cleanup_all; exit 143' TERM

function launch_mo() {
    cd "$MO_WORKSPACE"
    # Ordinary launch BVT remains the single CI entry point, but its Python
    # cases opt into the worker-enabled manifest explicitly.  The generic
    # etc/launch manifest stays safe for users who start mo-service directly.
    LAUNCH_CONFIG=$LAUNCH
    if [[ "$LAUNCH" == "launch" ]]; then
        LAUNCH_CONFIG=launch-with-python-udf-worker
    fi
    if ((${#PROXY_ARGS[@]} > 0)); then
        ./mo-service -debug-http=:12345 -launch "./etc/${LAUNCH_CONFIG}/launch.toml" "${PROXY_ARGS[@]}" &>mo-service.log &
    else
        ./mo-service -debug-http=:12345 -launch "./etc/${LAUNCH_CONFIG}/launch.toml" &>mo-service.log &
    fi
    MO_PID=$!
}

# this will wait mo all system init completed
function wait_system_init() {
    for num in {1..300}  
    do
        if MYSQL_PWD=111 mysql --connect-timeout=2 -h 127.0.0.1 -P 6001 -u dump -e "show databases;"; then
            echo "ok, cost $num seconds"
            return 0
        fi
        sleep 1
    done 
    return 1
}

# MySQL readiness only proves that the CN frontend accepted connections.  The
# worker is a separately started service, so Python UDF DDL/execution can still
# race its Flight listener.  Keep this in the existing BVT launcher instead of
# adding a second CI job or making every SQL case retry its first call.
function wait_python_udf_worker() {
    if [[ "$LAUNCH_CONFIG" != "launch-with-python-udf-worker" ]]; then
        return 0
    fi
    for num in {1..120}
    do
        if [[ -n "$MO_PID" ]] && ! kill -0 "$MO_PID" 2>/dev/null; then
            echo "MatrixOne exited before Python UDF worker became ready" >&2
            tail -n 160 "$MO_WORKSPACE/mo-service.log" >&2 || true
            return 1
        fi
        if python3 - <<'PY'
import json
import signal

import pyarrow.flight as flight


def _timeout(_signum, _frame):
    raise TimeoutError("capability probe timed out")


signal.signal(signal.SIGALRM, _timeout)
signal.alarm(2)
try:
    client = flight.FlightClient("grpc://127.0.0.1:50051")
    reader = client.do_action(
        flight.Action("GetPythonCapabilities", b'{"protocol_version":1}')
    )
    result = next(iter(reader), None)
    if result is None:
        raise RuntimeError("empty capability response")
    response = json.loads(bytes(result.body))
    if response.get("protocol_version") != 1:
        raise RuntimeError("unsupported capability protocol version")
finally:
    signal.alarm(0)
PY
        then
            echo "Python UDF capability handshake is ready, cost $num seconds"
            return 0
        fi
        sleep 1
    done
    echo "Python UDF capability handshake did not become ready on 127.0.0.1:50051" >&2
    tail -n 160 "$MO_WORKSPACE/mo-service.log" >&2 || true
    return 1
}

# Start the jstfu datastream server (xtool/jstfu) that
# test/distributed/cases/datastream talks to on 127.0.0.1:4444.  In this
# launch deployment MO runs on the host, so a host jstfu is reachable from
# the CN.  Idempotent: the restart pass reuses the running instance and the
# already-built jar.  Java is guaranteed here (mo-tester itself needs it);
# the Maven wrapper bootstraps its own Maven, and the pom emits Java 8
# bytecode on any JDK.
function launch_jstfu() {
    cd "$MO_WORKSPACE"
    if bash -c 'exec 3<>/dev/tcp/127.0.0.1/4444' 2>/dev/null; then
        echo "jstfu already listening on :4444, skip"
        return 0
    fi
    if [ ! -f xtool/jstfu/target/jstfu.jar ]; then
        echo "building jstfu.jar"
        (cd xtool/jstfu && ./mvnw -q -B -DskipTests package) || {
            echo "jstfu build failed; datastream BVT cases will fail" >&2
            return 1
        }
    fi
    nohup ./optools/jstfu_bvt.sh "$MO_WORKSPACE/test/distributed/resources" 127.0.0.1:6001 &>jstfu.log &
    JSTFU_PID=$!
    for _ in {1..30}; do
        if bash -c 'exec 3<>/dev/tcp/127.0.0.1/4444' 2>/dev/null; then
            echo "jstfu ready on :4444"
            return 0
        fi
        sleep 1
    done
    echo "jstfu did not start; see jstfu.log" >&2
    cat jstfu.log >&2 || true
    return 1
}

launch_mo
if [[ "${SKIP_JSTFU:-false}" == "true" ]]; then
    echo "skip jstfu for this BVT suite"
else
    launch_jstfu
    status=$?
    if [[ "$status" -ne 0 ]]; then
        cleanup_all
        exit "$status"
    fi
fi
wait_system_init
status=$?
if [[ "$status" -ne 0 ]]; then
    cleanup_all
    exit 1
fi
wait_python_udf_worker
status=$?
if [[ "$status" -ne 0 ]]; then
    cleanup_all
    exit "$status"
fi

# This script is a launcher: its caller runs mo-tester after this command
# returns.  Keep the services alive on a successful return; signal and startup
# failure paths above own cleanup because no caller can use the deployment in
# those cases.
if [[ "${BASH_SOURCE[0]}" != "$0" ]]; then
    return 0
fi
exit 0
