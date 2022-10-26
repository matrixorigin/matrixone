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
SYSTEM_INIT_COMPLETED=$MO_WORKSPACE/mo-data/local/cn/system_init_completed

function launch_mo() {
    cd $MO_WORKSPACE
    ./mo-service -launch ./etc/launch-tae-logservice/launch.toml &>mo-service.log &
}

# this will wait mo all system init completed
function wait_system_init() {
    for num in {1..300}  
    do  
        if [ -f "$SYSTEM_INIT_COMPLETED" ]; then
            echo "ok"
            return 0
        fi 
        echo "mo init not completed"
        sleep 1
    done 
    return 1
}

launch_mo
wait_system_init
exit $?
