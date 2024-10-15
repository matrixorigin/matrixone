#!/bin/bash

# Copyright 2023 Matrix Origin
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

## const
###############
pid=$$
ts=`date +%s`
out_log_count="/tmp/log_count.pid${pid}.ts${ts}"

max_try_conn=3
mod='mysql -h 127.0.0.1 -P 6001 -udump -p111 system -A'
metric_interval=60
count_threshold=1000

## function
###############

echo_proxy() {
    echo "[`date '+%F %T'`] $@"
}

show_env() {
    echo_proxy "This script is inspired by #9835"
    echo_proxy "arg count_threshold : $count_threshold"
    echo_proxy "arg metric_interval : $metric_interval"
    echo_proxy "out_log_count file  : $out_log_count"
}

check_mo_service_alive() {
    ## Action: try access mo {max_try_conn} times.
    ##         if failed, exit 0 with note: "failed to access mo-servcie ..."
    local ret=0
    for idx in `seq 1 $max_try_conn`;
    do
        echo_proxy "Try to access mo $idx times."
        $mod -Nse "select 1;" 1>/dev/null 2>&1
        ret=$?
        if [ $ret -eq 0 ]; then
            break
        fi
        # sleep 1 for retry
        sleep 1
    done
    if [ $ret -ne 0 ]; then
        echo_proxy "warning: failed to access mo-servcie through port 6001."
        exit 0
    fi
    echo_proxy "seccess to access mo-servcie through port 6001."
}

get_log_count() {
    # count log message per second (exclude level=debug record)
    #
    ### table system_metrics.metric example:
    # metric_name collecttime value   node    role    account type
    # mo_log_message_count    2023-08-03 15:08:08.591955  77  7c4dccb4-4d3c-41f8-b482-5251dc7a41bf    ALL sys error
    # mo_log_message_count    2023-08-03 15:08:08.591955  37  7c4dccb4-4d3c-41f8-b482-5251dc7a41bf    ALL sys info
    #
    ### calculation result example
    # collecttime	cnt_per_second	node	role	level
    # 2023-08-03 14:37:24.977181	35.78	7c4dccb4-4d3c-41f8-b482-5251dc7a41bf	ALL	error
    # 2023-08-03 14:37:24.977181	31.00	7c4dccb4-4d3c-41f8-b482-5251dc7a41bf	ALL	info
    # 2023-08-03 14:38:24.987134	21.02	7c4dccb4-4d3c-41f8-b482-5251dc7a41bf	ALL	error
    #
    local sql=`cat << EOF
select * from
(select collecttime, cast( value / $metric_interval as DECIMAL(38,2)) as cnt_per_second, node, role, type as level from system_metrics.metric
 where metric_name = 'mo_log_message_count' and type not in ('debug')) a
 where a.cnt_per_second > $count_threshold
 order by collecttime
EOF`
    echo_proxy "Query: $sql"
    $mod -e "$sql" > $out_log_count
}

check_log_count() {
    local rows=`wc -l $out_log_count | awk '{print $1}'`
    if [ "$rows" == "0" ]; then
        echo_proxy "log messages spitting out per second < threshold(val: $count_threshold): OK!"
        return 0
    fi

    echo_proxy "log messages spitting out per second threshold(val: $count_threshold)"
    echo_proxy "each rows show last $metric_interval secs status"
    echo_proxy
    cat $out_log_count
    echo
    return 1
}

show_log_info() {
    top_n=20
    cat $out_log_count | grep -v "collecttime" | while read _date _time _val _node _role _level; do
        echo_proxy "top $top_n log caller info, time-range [ (? - $metric_interval sec) ~ $_date $_time]"
        local sql="select cast(count(1) / $metric_interval as decimal(15,2)) cnt_per_sec, level, caller from system.log_info where timestamp between date_sub('$_date $_time', interval 60 second) and '$_date $_time' group by level, caller order by cnt_per_sec desc limit $top_n;"
        echo_proxy "Query: $sql"
        $mod -e "$sql"
        echo
    done
}

usage() {
    cat << EOF
Usage: $0 [cnt_threshold [metric_interval]]
like:  $0
  or   $0 1000
  or   $0 1000 60

options
    cnt_threshold   - int, log messages per second threshold
                      default: $count_threshold
    metric_interval - int, metric collected interval
                      default: $metric_interval
EOF
}

## main
################

if [ $# -eq 1 ]; then
    arg=$1
    if [ "$arg" == "-h" -o "$arg" == "--help" ]; then
        usage
        exit 1
    fi
    count_threshold=$arg
elif [ $# -eq 2 ]; then
    count_threshold=$1
    metric_interval=$2
fi

show_env
check_mo_service_alive
get_log_count
check_log_count
show_log_info
ret=$?
if [ "$ret" != "0" ]; then
    echo_proxy "log messages spitting out per second > threshold(val: $count_threshold): NOT ok!!!"
    exit 1
fi
