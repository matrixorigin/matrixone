#!/usr/bin/env bash
# Copyright 2026 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
set -euo pipefail
export LC_ALL=C
umask 077

if (($# < 2)); then
    echo "usage: $0 OUTPUT MYSQL_OPTIONS..." >&2
    echo "Use a protected --defaults-extra-file as the first MySQL option." >&2
    exit 2
fi
output=$1
shift
outdir=$(dirname -- "$output")
tmpdir=$(mktemp -d "${TMPDIR:-/tmp}/mo-upgrade-preflight.XXXXXXXX")
outtmp=
child=
spawning=0
cleanup() {
    trap '' HUP INT TERM
    if [[ -n $child ]]; then
        kill "$child" 2>/dev/null || true
        # A stopped child must be resumed to receive the termination signal.
        kill -CONT "$child" 2>/dev/null || true
        wait "$child" 2>/dev/null || true
    fi
    rm -rf -- "$tmpdir"
    if [[ -n $outtmp ]]; then rm -f -- "$outtmp"; fi
}
on_signal() {
    trap '' HUP INT TERM
    local status=$1 pid job_info job_pattern
    if ((spawning)); then
        # run_cmd is serial and owns the only background job. A signal may
        # arrive after Bash launches it but before child=$! has executed.
        pid=${!:-}
        if [[ -n $pid ]]; then
            job_info=$(jobs -l)
            job_pattern="^\\[[0-9]+\\][+-]?[[:space:]]+${pid}[[:space:]]+(Running|Stopped)([[:space:]]|$)"
            if [[ $job_info =~ $job_pattern ]]; then child=$pid; fi
        fi
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'on_signal 129' HUP
trap 'on_signal 130' INT
trap 'on_signal 143' TERM
outtmp=$(mktemp "$outdir/.mo-upgrade-decisions.XXXXXXXX")
export MO_CATALOG_PREFLIGHT_TMP=$tmpdir
run_cmd() {
    spawning=1
    "$@" <&0 &
    child=$!
    spawning=0
    local status=0
    wait "$child" || status=$?
    child=
    return "$status"
}

# The snapshot and edge reads form one statement: 4.2 advances its snapshot
# between statements even inside a transaction. Writers must still be drained.
run_cmd mysql "$@" --quick --raw --batch --skip-column-names --default-character-set=utf8mb4 --skip-force > "$tmpdir/raw" <<'SQL'
select 'A', cast(current_account_id() as varchar(32)), '{}';
select 'C', cast(count(*) as varchar(32)), '{}' from mo_catalog.mo_cdc_task;
select case when s.kind='user' then 'S' else 'X' end, substring(s.sname,13), json_object(
  'catalog_account_id','0', 'snapshot_id',cast(s.snapshot_id as varchar(36)),
  'name',s.sname, 'timestamp',cast(s.ts as varchar(32)),
  'level',cast(s.level as varchar(32)), 'account_name',s.account_name,
  'database_name',s.database_name, 'table_name',s.table_name,
  'object_id',cast(s.obj_id as varchar(32)), 'prior_kind',s.kind,
  'edge',null, 'decision',null)
from mo_catalog.mo_snapshots s
where (s.kind is null or s.kind<>'branch') and cast(s.level as varchar(32))='table'
  and s.sname regexp '^__mo_branch_(0|[1-9][0-9]{0,19})$'
  and (length(substring(s.sname,13))<20 or substring(s.sname,13)<='18446744073709551615')
union all
select 'E', cast(b.table_id as varchar(32)), json_object(
  'table_id',cast(b.table_id as varchar(32)), 'parent_id',cast(b.p_table_id as varchar(32)),
  'clone_ts',cast(b.clone_ts as varchar(32)), 'creator',cast(b.creator as varchar(32)),
  'level',b.level, 'table_deleted',b.table_deleted)
from mo_catalog.mo_branch_metadata b;
select 'Z', 'done', '{}';
SQL

# A successful MySQL result has a complete final line, even for an empty set.
last_byte=$(tail -c 1 "$tmpdir/raw" | od -An -tu1 | tr -d '[:space:]')
if [[ $last_byte != 10 ]]; then
    echo "incomplete MySQL output" >&2
    exit 1
fi
: > "$tmpdir/snapshots"
: > "$tmpdir/edges"
run_cmd awk -F '\t' '
        BEGIN { OFS="\t" }
        NF != 3 { print "malformed preflight row" > "/dev/stderr"; exit 1 }
        ended { print "rows after preflight completion" > "/dev/stderr"; exit 1 }
        $1 == "Z" { if (++ends != 1 || $2 != "done") exit 1; ended=1; next }
        $1 == "A" { if (++accounts != 1 || $2 != "0") exit 1; next }
        $1 == "C" { if (++counts != 1 || $2 !~ /^[0-9]+$/) exit 1; print $2 > (ENVIRON["MO_CATALOG_PREFLIGHT_TMP"] "/cdc-count"); next }
        $1 == "S" || $1 == "E" {
            if ($2 !~ /^(0|[1-9][0-9]*)$/ || length($2) > 20 || length($3) >= 262144) exit 1
            if ($1 == "S") print $2, $3 > (ENVIRON["MO_CATALOG_PREFLIGHT_TMP"] "/snapshots")
            else print $2, $3 > (ENVIRON["MO_CATALOG_PREFLIGHT_TMP"] "/edges")
            next
        }
        { print "unexpected snapshot kind or preflight tag" > "/dev/stderr"; exit 1 }
        END { if (accounts != 1 || counts != 1 || ends != 1) exit 1 }
    ' "$tmpdir/raw"
for input in snapshots edges; do
    run_cmd sort -S 16M --parallel=1 -T "$tmpdir" -t $'\t' -k1,1 "$tmpdir/$input" > "$tmpdir/$input.sorted"
    # Prefix forces string comparison: awk otherwise rounds large uint64 IDs.
    run_cmd awk -F '\t' 'NR > 1 && "x" $1 == "x" previous { exit 1 } { previous=$1 }' "$tmpdir/$input.sorted"
done
run_cmd join -t $'\t' -a 1 -e null -o '1.2,2.2' "$tmpdir/snapshots.sorted" "$tmpdir/edges.sorted" > "$tmpdir/joined"
printf '%s\n' '{"format":"matrixone-catalog-snapshot-decisions-v1","target":"4.0.7"}' > "$outtmp"
run_cmd jq -Rc '
    split("\t") as $parts |
    if ($parts | length) != 2 then error("malformed joined row") else . end |
    ($parts[0] | fromjson) as $snapshot | ($parts[1] | fromjson) as $edge |
    if ($snapshot | type) != "object" or $snapshot.catalog_account_id != "0" or
       $snapshot.prior_kind != "user" or $snapshot.level != "table" or
       ($snapshot.snapshot_id | type) != "string" or ($snapshot.name | type) != "string" or
       (($edge != null) and (($edge | type) != "object" or
        $edge.table_id != ($snapshot.name | ltrimstr("__mo_branch_")) or
        ($edge.parent_id | type) != "string" or ($edge.clone_ts | type) != "string" or
        ($edge.creator | type) != "string" or ($edge.level | type) != "string" or
        ($edge.table_deleted | type) != "boolean"))
    then error("invalid snapshot or edge")
    else $snapshot | .edge = $edge | .decision = null end
' < "$tmpdir/joined" >> "$outtmp"
run_cmd awk 'length($0) >= 262144 { exit 1 }' "$outtmp"
run_cmd wc -l < "$tmpdir/snapshots.sorted" > "$tmpdir/snapshot-count"
run_cmd wc -l < "$tmpdir/joined" > "$tmpdir/joined-count"
read -r snapshot_count < "$tmpdir/snapshot-count"
read -r joined_count < "$tmpdir/joined-count"
if [[ $snapshot_count != "$joined_count" ]]; then
    echo "snapshot merge was incomplete" >&2
    exit 1
fi
# Hard-link creation is atomic and fails if the destination already exists.
run_cmd ln -T -- "$outtmp" "$output"
read -r cdc_count < "$tmpdir/cdc-count"
echo "CDC tasks: $cdc_count; review and remove them as described in the upgrade guide." >&2
echo "Snapshot decision template written to $output; every decision requires administrator review." >&2
