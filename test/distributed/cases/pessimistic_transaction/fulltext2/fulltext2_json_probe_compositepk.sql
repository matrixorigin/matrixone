-- #27926/#27941: a json_extract fulltext2 probe on a table with a COMPOSITE primary key must not
-- error. table_changes emits only non-hidden source columns, so the hidden composite pk column
-- (__mo_cpkey_col) cannot anchor a partial tail; decideJSONProbe therefore declines to a full scan
-- rather than a partial plan that would hard-error at the splice. Result is correct regardless of
-- how far the async index has built (covered probe, or full scan when behind).
set experimental_fulltext2_index = 1;
drop database if exists ft2_json_cpk;
create database ft2_json_cpk;
use ft2_json_cpk;

create table t (a bigint, b bigint, j json, primary key(a, b));
create fulltext2 index ftj on t(j) with parser json;
insert into t values
 (1, 1, '{"foo":"needle"}'),
 (2, 2, '{"foo":"hay"}'),
 (3, 3, '{"foo":"needle"}');

-- Queried immediately, the index is behind; with a composite pk the partial path is declined, so
-- this is a full scan (not a runtime error) and returns the needles.
select a from t where json_extract_string(j,'$.foo') = 'needle' order by a;

-- A committed gap row is still returned (full scan sees it) -- proving the decline path stays
-- correct, not just non-erroring.
insert into t values (4, 4, '{"foo":"needle"}');
select a from t where json_extract_string(j,'$.foo') = 'needle' order by a;

drop database ft2_json_cpk;
