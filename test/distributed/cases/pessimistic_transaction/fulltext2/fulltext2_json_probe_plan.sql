-- #27926: assert the fulltext2 json_extract probe appears in the plan of a
-- current read. The index is created first and rows written after, so the ISCP
-- CDC tail consumes them asynchronously and advances the watermark. Readiness is
-- gated on the cdc_tail chunk (rows consumed, watermark written in the same ISCP
-- transaction); the probe is then asserted via @regex on EXPLAIN, so the check
-- uses no wall-clock arithmetic and does not depend on client/server timezone.
set experimental_fulltext2_index = 1;
drop database if exists ft2_json_probe_plan;
create database ft2_json_probe_plan;
use ft2_json_probe_plan;

create table t (id bigint primary key, j json);
create fulltext2 index ftj on t(j) with parser json;
insert into t values
 (1, '{"foo":"needle"}'),
 (2, '{"foo":"hay"}'),
 (3, '{"foo":"needle"}'),
 (4, '{"n":42}');

-- Wait for the CDC tail to consume the rows. The tail chunk and the watermark are
-- written in the same ISCP transaction, so a visible cdc_tail chunk proves the
-- rows are indexed and the watermark is fresh (coverage holds at the default delay).
set @ftj = (select index_table_name from mo_catalog.mo_indexes where name = 'ftj' and algo_table_type = 'ftv2_index' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't') limit 1);
set @wait_ftj_sql = concat('select coalesce(max(chunk_id), -1) >= 0 as ready from `', database(), '`.`', @ftj, '` where index_id = ''cdc_tail'' and tag = 1');
prepare wait_ftj from @wait_ftj_sql;
-- @wait_expect(2, 120)
execute wait_ftj;
deallocate prepare wait_ftj;

-- The plan contains the fulltext2_search probe; the original json_extract
-- predicate stays above it as a retained Filter Cond. Polled as insurance against
-- residual coverage delay under load.
-- @separator:table
-- @wait_expect(2, 120)
-- @regex("Table Function on fulltext2_search", true)
explain select id from t where json_extract_string(j,'$.foo') = 'needle';

-- and the results stay exact (the probe is a superset; the predicate re-checks).
select id from t where json_extract_string(j,'$.foo') = 'needle' order by id;

-- a numeric range probe fires too (coverage already holds)
-- @separator:table
-- @regex("Table Function on fulltext2_search", true)
explain select id from t where json_extract_float64(j,'$.n') > 10;
select id from t where json_extract_float64(j,'$.n') > 10 order by id;

drop database ft2_json_probe_plan;
