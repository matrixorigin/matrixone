#!/usr/bin/env bash
# Run the CN-wide optimizer-hint controls on a fresh single-CN deployment.
# Use the BVT job's already-built image, never its live SQL endpoint or data.
set -euo pipefail

tester=$(cd "$1" && pwd)
case_file=$(cd "$(dirname "$2")" && pwd)/$(basename "$2")
[[ -s "$case_file" && -s "${case_file%.sql}.result" ]]
image=$(docker image inspect --format '{{.Id}}' "${MO_BVT_IMAGE:-matrixorigin/matrixone:latest}")
work=$(mktemp -d)
cid=''
cleanup() {
    local rc=$?
    if [[ -n "$cid" ]]; then
        if (( rc != 0 )); then docker logs --tail 100 "$cid" >&2 || :; fi
        docker rm -fv "$cid" >/dev/null || rc=1
    fi
    rm -rf "$work" || rc=1
    exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

cid=$(docker create -p 127.0.0.1::6001 -p 127.0.0.1::12345 --entrypoint /mo-service \
    "$image" -debug-http=:12345 -launch /etc/launch/launch.toml)
docker start "$cid" >/dev/null
sql_port=$(docker port "$cid" 6001/tcp); sql_port=${sql_port##*:}
debug_port=$(docker port "$cid" 12345/tcp); debug_port=${debug_port##*:}
[[ "$sql_port" =~ ^[1-9][0-9]*$ && "$debug_port" =~ ^[1-9][0-9]*$ ]]

# Both readiness conditions matter: SQL can accept connections before ISCP
# registers the query service. Poll conditions, not a fixed startup delay.
deadline=$((SECONDS + 120))
until docker logs "$cid" 2>&1 | grep -F 'ISCP-Task Start' >/dev/null &&
    MYSQL_PWD=111 mysql --no-defaults --protocol=TCP -h127.0.0.1 -P"$sql_port" \
        -udump --connect-timeout=2 -e 'select 1' >/dev/null 2>&1; do
    if (( SECONDS >= deadline )) || [[ $(docker inspect -f '{{.State.Running}}' "$cid") != true ]]; then
        echo 'Isolated ZoneMap CN failed to become ready' >&2
        exit 1
    fi
    sleep 1
done

# mo-tester resolves jars/config and writes logs relative to its workspace.
# Do not rewrite the shared mo.yml or send timeout diagnostics to its CN.
cd "$work"
mkdir log report
cp -R "$tester"/{lib,run.sh,run.yml} .
for f in target pprof.sh log4j.properties; do
    if [[ -e "$tester/$f" ]]; then cp -R "$tester/$f" .; fi
done
awk -v sql="$sql_port" -v debug="$debug_port" '
    /^  - addr:/      {print "  - addr: \"127.0.0.1:" sql "\""; n++; next}
    /^  serverIP:/    {print "  serverIP: \"127.0.0.1\""; h++; next}
    /^  port:/        {print "  port: " debug; p++; next}
    {print}
    END {if (n != 1 || h != 1 || p != 1) exit 1}
' "$tester/mo.yml" > mo.yml
rc=0
./run.sh -n -g -o -p "$case_file" || rc=$?
# Print the private report before cleanup. Some connection/setup failures can
# exit without a verdict; a zero process status by itself is not a pass.
if [[ -s report/report.txt ]]; then cat report/report.txt; fi
(( rc == 0 )) || exit "$rc"
[[ -s report/report.txt ]]
awk -v name="[$case_file]" '
    index($0, name) == 1 && /TOTAL *: *[1-9][0-9]*,/ && /FAILED *: *0,/ &&
        /IGNORED *: *0,/ && /ABNOR[A-Z]* *: *0,/ && /SUCCESS RATE *: *100%/ {ok=1}
    END {exit !ok}
' report/report.txt
