-- #27926: the fulltext2 json_extract probe must decline to a Table Scan when
-- coverage cannot be proven, and still return correct rows. A transaction-local
-- (uncommitted) write to the source table cannot have reached the async index,
-- so SourceCommitTS fails closed and the probe is not used. This is deterministic
-- -- no dependence on CDC timing or which CN serves the read.
set experimental_fulltext2_index = 1;
drop database if exists ft2_json_probe_fallback;
create database ft2_json_probe_fallback;
use ft2_json_probe_fallback;

create table t (id bigint primary key, j json);
create fulltext2 index ftj on t(j) with parser json;
insert into t values (1, '{"foo":"needle"}'), (2, '{"foo":"hay"}');

-- Inside a transaction with an uncommitted write to t, the coverage check fails
-- closed, so the plan has no fulltext2_search probe (Table Scan), and the query
-- still returns the uncommitted matching row.
begin;
insert into t values (3, '{"foo":"needle"}');
-- @separator:table
-- @regex("Table Function on fulltext2_search", false)
explain select id from t where json_extract_string(j,'$.foo') = 'needle';
select id from t where json_extract_string(j,'$.foo') = 'needle' order by id;
commit;

-- after commit the results stay exact regardless of which plan is chosen.
select id from t where json_extract_string(j,'$.foo') = 'needle' order by id;

drop database ft2_json_probe_fallback;
