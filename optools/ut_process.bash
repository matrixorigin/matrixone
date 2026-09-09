#!/bin/bash

# Copyright 2026 Matrix Origin
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

function terminate_ut_process_group(){
    local pid=$1
    local signal=${2:-TERM}
    if [[ -z "${pid}" ]] || ! [[ "${pid}" =~ ^[1-9][0-9]*$ ]]; then
        return 0
    fi

    # run_ut_command and the helper runners enable job control before forking,
    # so the child pid is also the process-group id. Keep a pid fallback for
    # shells/runners that do not expose a separate group.
    kill -"${signal}" -- "-${pid}" 2>/dev/null || kill -"${signal}" "${pid}" 2>/dev/null || true
}

function ut_process_group_alive(){
    local pid=$1
    if [[ -z "${pid}" ]] || ! [[ "${pid}" =~ ^[1-9][0-9]*$ ]]; then
        return 1
    fi
    kill -0 -- "-${pid}" 2>/dev/null || kill -0 "${pid}" 2>/dev/null
}

function wait_for_ut_process_group(){
    local pid=$1
    local grace_ticks=${2:-20}
    local tick=0

    while (( tick < grace_ticks )) && ut_process_group_alive "${pid}"; do
        sleep 0.25
        tick=$((tick + 1))
    done
    if ut_process_group_alive "${pid}"; then
        logger "ERR" "UT cancellation: force stopping process group ${pid}"
        terminate_ut_process_group "${pid}" KILL
    fi
}

function terminate_ut_process_groups(){
    local grace_ticks=$1
    shift
    local pid

    # Send TERM to every owner before waiting. This gives independent groups a
    # common start time; a TERM-ignoring group is then force-killed within its
    # own bounded grace period instead of blocking another group from seeing
    # the signal.
    for pid in "$@"; do
        [[ "${pid}" =~ ^[1-9][0-9]*$ ]] && terminate_ut_process_group "${pid}" TERM
    done
    for pid in "$@"; do
        [[ "${pid}" =~ ^[1-9][0-9]*$ ]] && wait_for_ut_process_group "${pid}" "${grace_ticks}"
    done
}

function restore_ut_term_trap(){
    local saved_trap=${1:-}
    if [[ -n "${saved_trap}" ]]; then
        # `trap -p` returns a shell command captured from this script's own
        # trap table immediately before a helper installs its local trap.
        eval "${saved_trap}"
    else
        trap - TERM
    fi
}
