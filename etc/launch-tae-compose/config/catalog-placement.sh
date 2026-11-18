#!/usr/bin/env bash
# Fresh BVT cluster only: other CNs are held back until placement is verified.
set -euo pipefail
host=$1 port=$2 uuid=$3
[[ $uuid =~ ^[a-zA-Z0-9_-]+$ ]]
sql() { mysql --protocol=TCP --default-character-set=utf8mb4 --connect-timeout=3 -h "$host" -P "$port" -udump -NB -e "$1"; }
cron="mo_task.sys_cron_task"
tasks="mo_task.sys_async_task"
where_cron="task_metadata_id='StorageUsage'"
where_task="task_parent_id='StorageUsage'"
for ((i=0; i<120; i++)); do
    if [[ $(sql "select count(*) from $cron where $where_cron" 2>/dev/null) == 1 ]]; then break; fi
    sleep 1
done
[[ $(sql "select count(*) from $cron where $where_cron") == 1 ]]
# A reserved account label excludes this CN from normal proxy routing. The
# scheduler matches the same label literally, independently of the login user.
sql "select mo_ctl('cn','label','$uuid:account:bvt_catalog_full')"
# The first worker updates only its cron options and exits. Wait for the new
# instance: updating the cron definition alone does not move a running task.
for ((i=0; i<240; i++)); do
    if [[ $(sql "select count(*) from $tasks where $where_task and task_runner='$uuid' and task_status=1 and json_unquote(json_extract(task_metadata_option,'$.Labels.account'))='bvt_catalog_full'") == 1 ]]; then
        echo "StorageUsage runner: $uuid"
        exit 0
    fi
    sleep 1
done
echo "StorageUsage not running on the full CN" >&2
exit 1
