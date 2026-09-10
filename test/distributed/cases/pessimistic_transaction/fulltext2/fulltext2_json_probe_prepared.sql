-- #27926/#27941: a PREPARED json_extract statement must not go stale across EXECUTEs. Its
-- covered/partial/skip decision reflects the async index's freshness at plan-build time, which no
-- schema version tracks, so cross-transaction plan reuse would freeze it: a covered probe cached
-- while the index was current, reused after the index falls behind, would drop rows committed in
-- the gap. The planner flags such a plan to rebuild on every EXECUTE, so each execution re-derives
-- coverage from the current snapshot. This asserts RESULTS -- deterministic regardless of how far
-- the async index has built (partial fills the gap, or a caught-up index covers it), and it fails
-- only if the stale-reuse regression returns (a re-EXECUTE dropping the gap rows).
set experimental_fulltext2_index = 1;
drop database if exists ft2_json_prep;
create database ft2_json_prep;
use ft2_json_prep;

create table t (id bigint primary key, j json);
create fulltext2 index ftj on t(j) with parser json;
insert into t values
 (1, '{"foo":"needle"}'),
 (2, '{"foo":"hay"}'),
 (3, '{"foo":"needle"}');

-- Bake the initial rows so the first EXECUTE caches a COVERED plan (the exact plan whose reuse the
-- fix must invalidate). Readiness is gated on the cdc_tail chunk -- consumed rows and watermark
-- written in one ISCP transaction -- so there is no wall-clock arithmetic and no timezone reliance.
set @ftj = (select index_table_name from mo_catalog.mo_indexes where name = 'ftj' and algo_table_type = 'ftv2_index' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't') limit 1);
set @wait_ftj_sql = concat('select coalesce(max(chunk_id), -1) >= 0 as ready from `', database(), '`.`', @ftj, '` where index_id = ''cdc_tail'' and tag = 1');
prepare wait_ftj from @wait_ftj_sql;
-- @wait_expect(2, 120)
execute wait_ftj;
deallocate prepare wait_ftj;

prepare p from 'select id from t where json_extract_string(j,''$.foo'') = ''needle'' order by id';

-- First EXECUTE while the index is current: caches the covered plan; returns the baked needles.
execute p;

-- Commit a gap row the index has not consumed, then re-EXECUTE the SAME prepared statement. A stale
-- reused covered plan would drop row 4; the rebuild returns it (via the partial tail, or the index
-- if it has caught up).
insert into t values (4, '{"foo":"needle"}');
execute p;

-- A second gap batch (5 needle, 6 hay): the re-EXECUTE must include 5 and still exclude 6.
insert into t values (5, '{"foo":"needle"}'), (6, '{"foo":"hay"}');
execute p;

deallocate prepare p;
drop database ft2_json_prep;
