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

MO_WORKSPACE=$1
LAUNCH=$2
PROXY=${3:-}

function launch_mo() {
    cd $MO_WORKSPACE
    ./mo-service -debug-http=:12345 -launch ./etc/$LAUNCH/launch.toml $PROXY &>mo-service.log &
}

# this will wait mo all system init completed
function wait_system_init() {
    for num in {1..300}  
    do
        if MYSQL_PWD=111 mysql -h 127.0.0.1 -P 6001 -u dump -e "show databases;"; then
            echo "ok, cost $num seconds"
            return 0
        fi
        sleep 1
    done 
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
    cd $MO_WORKSPACE
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
launch_jstfu || exit $?
wait_system_init
exit $?
