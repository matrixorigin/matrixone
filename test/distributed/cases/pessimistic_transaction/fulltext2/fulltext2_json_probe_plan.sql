-- #27926: assert the fulltext2 json_extract probe appears in the plan of a
-- current read. Rows are inserted before CREATE INDEX so the build populates the
-- whole index and commits a covering watermark within the blocking DDL; coverage
-- then holds at the default delay with no async gap. The planner compares HLC
-- timestamps, so the assertion needs no wall-clock arithmetic in the test.
set experimental_fulltext2_index = 1;
drop database if exists ft2_json_probe_plan;
create database ft2_json_probe_plan;
use ft2_json_probe_plan;

create table t (id bigint primary key, j json);
insert into t values
 (1, '{"foo":"needle"}'),
 (2, '{"foo":"hay"}'),
 (3, '{"foo":"needle"}'),
 (4, '{"n":42}');
create fulltext2 index ftj on t(j) with parser json;

-- The plan contains the fulltext2_search probe; the original json_extract
-- predicate stays above it as a retained Filter Cond. Polled as CI insurance in
-- case the build watermark commits asynchronously under load.
-- @separator:table
-- @wait_expect(2, 60)
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
