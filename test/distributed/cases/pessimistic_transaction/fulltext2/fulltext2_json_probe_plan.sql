-- Regression for #27926: the fulltext2 json_extract index probe must actually
-- FIRE on an ordinary CURRENT read. Before the fix the coverage gate could never
-- open for a current read against an always-async json index, so the plan
-- silently fell back to Table Scan + Filter:
--   (1) the mo_iscp_log coverage lookup ran under the canceled planning context
--       (proc.Ctx) and errored with "context canceled" -> fail-closed; and
--   (2) it demanded the watermark reach wall-clock now, which an async watermark
--       (it chases the clock with a built-in lag) can never satisfy.
-- The probe is a superset prefilter with the original predicate retained, so the
-- rows were always correct -- which is exactly why the equivalence-only test in
-- cases/fulltext2/fulltext2_json_probe.sql never caught this. This case asserts
-- the PLAN.
--
-- The index is created FIRST and rows are written AFTER (the issue's exact
-- scenario), so the writes travel the ISCP CDC tail. fulltext2 is always-async,
-- so that tail is polled before the plan is asserted (a fixed sleep would be
-- flaky). Once the tail lands the watermark is live and within the coverage
-- staleness window, so the probe fires.
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

-- Wait for the CDC tail so the ISCP watermark is live and non-empty (coverage
-- requires a live job with a watermark). The tail chunk and the watermark are
-- written in the same ISCP transaction, so a visible cdc_tail chunk proves the
-- watermark has advanced.
set @ftj = (select index_table_name from mo_catalog.mo_indexes where name = 'ftj' and algo_table_type = 'ftv2_index' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't') limit 1);
set @wait_ftj_sql = concat('select coalesce(max(chunk_id), -1) >= 0 as ready from `', database(), '`.`', @ftj, '` where index_id = ''cdc_tail'' and tag = 1');
prepare wait_ftj from @wait_ftj_sql;
-- @wait_expect(2, 120)
execute wait_ftj;
deallocate prepare wait_ftj;

-- The plan MUST now contain the fulltext2_search probe (the fix). The original
-- json_extract predicate stays above it as a retained Filter Cond.
-- @separator:table
-- @regex("Table Function on fulltext2_search", true)
explain select id from t where json_extract_string(j,'$.foo') = 'needle';

-- and the results stay exact (the probe is a superset; the predicate re-checks).
select id from t where json_extract_string(j,'$.foo') = 'needle' order by id;

-- a numeric range probe fires too
-- @separator:table
-- @regex("Table Function on fulltext2_search", true)
explain select id from t where json_extract_float64(j,'$.n') > 10;
select id from t where json_extract_float64(j,'$.n') > 10 order by id;

drop database ft2_json_probe_plan;
