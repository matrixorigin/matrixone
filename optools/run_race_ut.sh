#!/usr/bin/env bash
# Copyright 2026 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eu
trap 'exit 130' INT
trap 'exit 143' TERM

if [[ $# -lt 5 || ! $2 =~ ^[1-9][0-9]*$ || ! $3 =~ ^[1-9][0-9]*$ ]]; then
    echo 'usage: run_race_ut.sh report parallel timeout_minutes tags package...' >&2
    exit 2
fi
report=$1
parallel=$2
timeout=$3
tags=$4
shift 4
regular=()
embedded=()
for package in "$@"; do
    case "$package" in
        github.com/matrixorigin/matrixone/pkg/embed|github.com/matrixorigin/matrixone/pkg/embed/*|github.com/matrixorigin/matrixone/pkg/tests/*|github.com/matrixorigin/matrixone/pkg/util/test)
            embedded+=("$package") ;;
        *) regular+=("$package") ;;
    esac
done

# 普通 race 包先完成，随后独占执行集群包，避免多个多 CN fixture
# 与其余 race 测试争用同一 runner。两阶段保留原参数和完整 JSON 结果。
: > "$report"
result=0
run_stage() {
    local stage_parallel=$1
    shift
    [[ $# -gt 0 ]] || return 0
    local status=0
    echo "race UT: packages=$# parallel=$stage_parallel" >&2
    go test -short -v -json -tags "$tags" -p "$stage_parallel" \
        -timeout "${timeout}m" -race "$@" >> "$report" || status=$?
    case "$status" in
        0) ;;
        1) result=1 ;; # 保留失败，仍收集尚未运行包的结果；不重试。
        *) exit "$status" ;; # 取消等异常终态不得继续准入下一阶段。
    esac
}
run_stage "$parallel" ${regular[@]+"${regular[@]}"}
run_stage 1 ${embedded[@]+"${embedded[@]}"}
exit "$result"
