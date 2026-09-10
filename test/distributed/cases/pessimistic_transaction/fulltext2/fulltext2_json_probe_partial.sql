-- #27926/#27941: a CURRENT-read json_extract probe against a BEHIND fulltext2 index must return
-- the correct rows by completing the index (bulk) arm with a table_changes tail over
-- (build_ts, snapshot], NOT by declining to a full scan. Which plan runs (covered probe / partial
-- union / table scan) depends on how far the async index has built, but the RESULT is identical, so
-- this asserts RESULTS -- no plan assertion, no wall-clock. The base scan re-checks every WHERE
-- conjunct on current values, so a stale index arm can never leak an updated or deleted row.
set experimental_fulltext2_index = 1;
drop database if exists ft2_json_partial;
create database ft2_json_partial;
use ft2_json_partial;

create table t (id bigint primary key, j json, content varchar(64));
create fulltext2 index ftj on t(j) with parser json;
insert into t values
 (1, '{"foo":"needle"}', 'c1'),
 (2, '{"foo":"hay"}', 'c2'),
 (3, '{"foo":"needle"}', 'c3');

-- Bake the initial rows into the index so the later writes form a genuine gap (covered base +
-- table_changes tail). Readiness is gated on the cdc_tail chunk -- rows consumed, watermark written
-- in the same ISCP transaction -- so there is no wall-clock arithmetic and no timezone dependence.
set @ftj = (select index_table_name from mo_catalog.mo_indexes where name = 'ftj' and algo_table_type = 'ftv2_index' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't') limit 1);
set @wait_ftj_sql = concat('select coalesce(max(chunk_id), -1) >= 0 as ready from `', database(), '`.`', @ftj, '` where index_id = ''cdc_tail'' and tag = 1');
prepare wait_ftj from @wait_ftj_sql;
-- @wait_expect(2, 120)
execute wait_ftj;
deallocate prepare wait_ftj;

-- Gap insert: row 4 (needle) is caught by the tail; row 5 (hay) is excluded.
insert into t values (4, '{"foo":"needle"}', 'c4'), (5, '{"foo":"hay"}', 'c5');
select id from t where json_extract_string(j,'$.foo') = 'needle' order by id;

-- Cross-arm dedup: update a BAKED needle (row 1) to a different-but-still-needle value in the gap.
-- Row 1 is now in BOTH arms -- the index (bulk) arm still holds its old needle posting, and the
-- table_changes tail carries its new needle value -- so UNION ALL plus the group-by dedup must
-- collapse it. It must appear exactly ONCE (a dedup regression would return id 1 twice).
update t set j = '{"foo":"needle","v":2}' where id = 1;
select id from t where json_extract_string(j,'$.foo') = 'needle' order by id;

-- Update match->nomatch: baked row 3 (needle) becomes hay. The stale index arm still holds 3 as a
-- needle, but the base re-check on current values drops it.
update t set j = '{"foo":"hay"}' where id = 3;
select id from t where json_extract_string(j,'$.foo') = 'needle' order by id;

-- Update nomatch->match: row 2 (hay) becomes needle. Its new value appears in the table_changes
-- tail, so it is included.
update t set j = '{"foo":"needle"}' where id = 2;
select id from t where json_extract_string(j,'$.foo') = 'needle' order by id;

-- Delete-in-gap: baked row 1 (needle) is deleted. MVCC at the read snapshot drops it even though
-- the index still holds its posting.
delete from t where id = 1;
select id from t where json_extract_string(j,'$.foo') = 'needle' order by id;

-- Multi-column: a second WHERE conjunct on a non-indexed column stays on the base scan. Row 7 is a
-- gap needle but content <> 'c6', so the base re-check drops it -- only row 6 matches.
insert into t values (6, '{"foo":"needle"}', 'c6'), (7, '{"foo":"needle"}', 'other');
select id from t where json_extract_string(j,'$.foo') = 'needle' and content = 'c6' order by id;

drop database ft2_json_partial;
