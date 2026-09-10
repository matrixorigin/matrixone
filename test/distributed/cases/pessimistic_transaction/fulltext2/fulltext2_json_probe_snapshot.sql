-- #27926/#27941: a {snapshot=...} json_extract read must return the correct HISTORICAL
-- rows. A snapshot read takes the SAME covered/partial/skip path as a current read, measured
-- as of the snapshot: if the snapshot-bound generation has caught up to the source's last commit
-- as of S it probes; if it is behind it is completed with a table_changes tail up to S; and it
-- declines to a full scan on any uncertainty. Every path returns the exact historical rows, and
-- the coverage gate must NEVER fire a bare probe against a generation missing snapshot rows (that
-- would drop them). Because all three paths are exact, the result is deterministic no matter how
-- far the async index has built -- so this asserts RESULTS (not the plan, and with no readiness
-- poll). fulltext2 is AlwaysAsync.
set experimental_fulltext2_index = 1;
drop database if exists ft2_json_snap;
create database ft2_json_snap;
use ft2_json_snap;

create table t (id bigint primary key, j json);
create fulltext2 index ftj on t(j) with parser json;
insert into t values
 (1, '{"foo":"needle"}'),
 (2, '{"foo":"hay"}'),
 (3, '{"foo":"needle"}');

create snapshot ft2_json_snap_sp for account;

-- Diverge the current table from the snapshot: a new needle committed AFTER it.
insert into t values (4, '{"foo":"needle"}');

-- Snapshot read: the historical needles only (1,3). The post-snapshot row 4 is invisible,
-- and no pre-snapshot needle may be dropped -- proving the gate never probes a snapshot
-- generation that lacks the snapshot rows.
select id from t {snapshot='ft2_json_snap_sp'} where json_extract_string(j,'$.foo') = 'needle' order by id;

-- Current read: all needles (1,3,4). Row 4 is still catching up in the index, so the gate
-- declines and the table scan returns it -- exact regardless of the plan chosen.
select id from t where json_extract_string(j,'$.foo') = 'needle' order by id;

drop snapshot ft2_json_snap_sp;
drop database ft2_json_snap;
