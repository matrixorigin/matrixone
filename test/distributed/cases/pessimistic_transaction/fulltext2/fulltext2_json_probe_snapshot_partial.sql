-- #27926/#27941: a {snapshot=...} json_extract read whose index was BEHIND as of the snapshot must
-- still return the exact historical rows by completing the snapshot-bound index (bulk) arm with a
-- table_changes tail over (build_ts, S] -- the SAME partial path a current read uses, just measured
-- as of S. The gap row is committed to the base BEFORE the snapshot, so it belongs to the snapshot;
-- the index has not consumed it yet, so a bare probe would drop it and only the tail recovers it.
-- Which plan runs (covered probe / partial union / full scan as of S) depends on how far the async
-- index built before S, but every path is exact, so this asserts RESULTS -- no plan assertion, no
-- wall-clock. fulltext2 is AlwaysAsync.
set experimental_fulltext2_index = 1;
drop database if exists ft2_json_snap_partial;
create database ft2_json_snap_partial;
use ft2_json_snap_partial;

create table t (id bigint primary key, j json, content varchar(64));
create fulltext2 index ftj on t(j) with parser json;
insert into t values
 (1, '{"foo":"needle"}', 'c1'),
 (2, '{"foo":"hay"}', 'c2'),
 (3, '{"foo":"needle"}', 'c3');

-- Bake the initial rows so the pre-snapshot gap insert is a genuine gap (build_ts sits before it).
set @ftj = (select index_table_name from mo_catalog.mo_indexes where name = 'ftj' and algo_table_type = 'ftv2_index' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't') limit 1);
set @wait_ftj_sql = concat('select coalesce(max(chunk_id), -1) >= 0 as ready from `', database(), '`.`', @ftj, '` where index_id = ''cdc_tail'' and tag = 1');
prepare wait_ftj from @wait_ftj_sql;
-- @wait_expect(2, 120)
execute wait_ftj;
deallocate prepare wait_ftj;

-- Gap insert (row 4 needle, row 5 hay) committed to the base, THEN the snapshot: row 4 is in the
-- snapshot's base but not yet in the index as of S, so the snapshot read needs the tail to see it.
insert into t values (4, '{"foo":"needle"}', 'c4'), (5, '{"foo":"hay"}', 'c5');
create snapshot ft2_json_snap_partial_sp for account;

-- Post-snapshot divergence: row 6 (needle) committed AFTER S must be invisible to the snapshot read.
insert into t values (6, '{"foo":"needle"}', 'c6');

-- Snapshot read: historical needles as of S = 1,3,4 (row 4 recovered by the tail; row 6 excluded).
select id from t {snapshot='ft2_json_snap_partial_sp'} where json_extract_string(j,'$.foo') = 'needle' order by id;

-- Snapshot read with a second conjunct on a non-indexed column, kept on the base scan as of S: only
-- row 4 (content 'c4') matches among the historical needles.
select id from t {snapshot='ft2_json_snap_partial_sp'} where json_extract_string(j,'$.foo') = 'needle' and content = 'c4' order by id;

-- Current read: all needles including the post-snapshot row 6 = 1,3,4,6.
select id from t where json_extract_string(j,'$.foo') = 'needle' order by id;

drop snapshot ft2_json_snap_partial_sp;
drop database ft2_json_snap_partial;
