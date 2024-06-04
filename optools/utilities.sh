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

set -o nounset                                  # Treat unset variables as an error

function logger_base() {
    local level=$1
    local msg=$2
	local log="$G_WKSP/$3"
    if [[ $# != 3 ]]; then
        echo "The number of argument is incorrect"
        exit 1
    fi
    local fn_stack=(${FUNCNAME[*]})
    local fn_name=""
    if [[ ${#fn_stack[*]} == 2 ]] && [[ 'mlog' == ${fn_stack[0]} ]]; then
        fn_name=${fn_stack[1]}
    else
        fn_name="${fn_stack[@]:0-2:1}"
    fi
 	if [[ ! -f $log ]]; then
		touch $log
	fi
    case $level in
        "ERR") echo -e "[$(date +"%Y-%m-%d %H:%M:%S %z" | cut -b 1-35)] [$fn_name] [ERR] $msg" | tee -a $log;;
        "WRN") echo -e "[$(date +"%Y-%m-%d %H:%M:%S %z" | cut -b 1-35)] [$fn_name] [WRN] $msg" | tee -a $log;;
        "INF") echo -e "[$(date +"%Y-%m-%d %H:%M:%S %z" | cut -b 1-35)] [$fn_name] [INF] $msg" | tee -a $log;;
        *) echo "Msg level is incorrect"; exit 1;;
    esac
}

function horiz_rule() {
    str='='
    num=60
    v=$(printf "%-${num}s" "$str")
    echo "#${v// /=}"
}

function get_container_id(){
	if [[ -f /proc/self/cgroup ]]; then
		cat /proc/self/cgroup | awk -F'/' '{print $3}' | head -n 1 | cut -c1-16  | xargs
	else
		echo "$(hostname)"
	fi
}

function cmd_timeout { 
    cmd="$1"; timeout="$2";
    grep -qP '^\d+$' <<< $timeout || timeout=60

    ( 
        eval "$cmd" &
        child=$!
        trap -- "" SIGTERM 
        (       
            sleep $timeout
            kill $child 2> /dev/null 
        ) &     
        wait $child
    )
}

G_TS=`date +%Y%m%d%H%M%S`
G_STAGE="${UT_WORKDIR:-$HOME}/scratch"
if [[ ! -d $G_STAGE ]]; then mkdir -p $G_STAGE; fi

G_CONT_ID=$(get_container_id)
G_WKSP=$G_STAGE/$G_CONT_ID
if [[ ! -d $G_WKSP ]]; then mkdir $G_WKSP; fi

OSTYPE_PREFIX=$(echo ${OSTYPE} | sed "s/[-(0-9)].*//g")
if [[ 'linux-gnu' == $OSTYPE_PREFIX ]]; then
    G_OSTYPE='Linux'
elif [[ 'darwin' == $OSTYPE_PREFIX ]]; then
    G_OSTYPE='MacOS'
else
	G_OSTYPE=$OSTYPE
fi
