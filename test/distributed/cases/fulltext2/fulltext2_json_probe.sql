-- The json word breaker indexes each leaf as a (tag, value) tuple, and the
-- optimizer turns json_extract_string/json_extract_float64 comparisons into a
-- probe on that index. The probe is only a PREFILTER: the original predicate is
-- retained and re-evaluated, so an indexed table must return exactly what an
-- unindexed one does. Every case below asserts that equivalence.

drop database if exists ft2_json_probe;
create database ft2_json_probe;
use ft2_json_probe;

set experimental_fulltext2_index = 1;

create table t (id int primary key, j json);
-- t_plain holds identical rows with NO index: the oracle.
create table t_plain (id int primary key, j json);

insert into t values
 (1, '{"a":{"foo":"bar"},"n":10}'),
 (2, '{"a":{"foo":"baz"},"n":20}'),
 (3, '{"foo":"bar","n":30}'),
 (4, '{"other":"bar","n":40}'),
 (5, '{"foo":"qux","n":25.5}'),
 (6, '{"foo":"3.14","n":-7}'),
 (7, '{"foo":3.14,"n":0}'),
 (8, '{"tags":["bar","zed"],"n":100}'),
 (9, '{"foo":null,"n":null}'),
 (10, '{"deep":{"deeper":{"foo":"bar"}},"n":5}');
insert into t_plain select * from t;

create fulltext2 index ftj on t(j) with parser json;
show create table t;

-- ---------------------------------------------------------------- equality
select id from t where json_extract_string(j,'$.foo') = 'bar' order by id;
select id from t_plain where json_extract_string(j,'$.foo') = 'bar' order by id;

-- a nested path: only the trailing key is indexed, so this is served too
select id from t where json_extract_string(j,'$.deep.deeper.foo') = 'bar' order by id;
select id from t_plain where json_extract_string(j,'$.deep.deeper.foo') = 'bar' order by id;

-- a numeric leaf reached through json_extract_string: the constant is probed
-- under BOTH encodings, so row 7 (the NUMBER 3.14) must not be lost
select id from t where json_extract_string(j,'$.foo') = '3.14' order by id;
select id from t_plain where json_extract_string(j,'$.foo') = '3.14' order by id;

-- numeric equality reaches an integer leaf (all numbers normalize to float64)
select id from t where json_extract_float64(j,'$.n') = 10 order by id;
select id from t_plain where json_extract_float64(j,'$.n') = 10 order by id;

-- array elements are indexed under the enclosing key
select id from t where json_extract_string(j,'$.tags[0]') = 'bar' order by id;
select id from t_plain where json_extract_string(j,'$.tags[0]') = 'bar' order by id;

-- no match at all
select id from t where json_extract_string(j,'$.foo') = 'nothing' order by id;
select id from t_plain where json_extract_string(j,'$.foo') = 'nothing' order by id;

-- ---------------------------------------------------------------- ranges
select id from t where json_extract_float64(j,'$.n') > 15 order by id;
select id from t_plain where json_extract_float64(j,'$.n') > 15 order by id;

select id from t where json_extract_float64(j,'$.n') >= 25.5 order by id;
select id from t_plain where json_extract_float64(j,'$.n') >= 25.5 order by id;

select id from t where json_extract_float64(j,'$.n') < 10 order by id;
select id from t_plain where json_extract_float64(j,'$.n') < 10 order by id;

select id from t where json_extract_float64(j,'$.n') <= 20 order by id;
select id from t_plain where json_extract_float64(j,'$.n') <= 20 order by id;

-- negative bound, and a bound no row sits on
select id from t where json_extract_float64(j,'$.n') > -7 order by id;
select id from t_plain where json_extract_float64(j,'$.n') > -7 order by id;

-- reversed operand order is the same predicate
select id from t where 15 < json_extract_float64(j,'$.n') order by id;
select id from t_plain where 15 < json_extract_float64(j,'$.n') order by id;

-- a string inequality: the probe unions the string range with every numeric
-- term, so the numeric leaf (row 7) is still reachable
select id from t where json_extract_string(j,'$.foo') > 'a' order by id;
select id from t_plain where json_extract_string(j,'$.foo') > 'a' order by id;

-- ---------------------------------------------------------------- combined
-- the probe must not disturb an ANDed non-json predicate
select id from t where json_extract_string(j,'$.foo') = 'bar' and id > 1 order by id;
select id from t_plain where json_extract_string(j,'$.foo') = 'bar' and id > 1 order by id;

-- OR is NOT probed (the branch need not hold), and must stay correct
select id from t where json_extract_string(j,'$.foo') = 'bar' or id = 2 order by id;
select id from t_plain where json_extract_string(j,'$.foo') = 'bar' or id = 2 order by id;

-- NOT is not probed either
select id from t where not (json_extract_string(j,'$.foo') = 'bar') order by id;
select id from t_plain where not (json_extract_string(j,'$.foo') = 'bar') order by id;

-- NOTE: rows inserted AFTER the index exists
-- incremental (ISCP) build, so their visibility is timing dependent and is not
-- asserted here — a fixed sleep would make this case flaky. That the incremental
-- build emits terms byte-identical to the CREATE build is covered
-- deterministically by TestCreateAndIscpAgreeOnTerms in pkg/fulltext2.

drop database if exists ft2_json_probe;
