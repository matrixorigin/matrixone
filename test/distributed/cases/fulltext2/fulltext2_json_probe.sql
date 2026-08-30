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

-- json_extract_string is NULL for a numeric leaf, so this matches ONLY row 6
-- (the string "3.14") and NOT row 7 (the number 3.14). The two extractors are
-- disjoint on leaf type, which is why the probe needs one encoding, not two.
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

-- a string inequality ranges over the STRING encoding only. That is exact, not a
-- compromise: json_extract_string is NULL for a numeric leaf, so row 7
-- ({"foo":3.14}) cannot satisfy the predicate and need not be reachable.
select id from t where json_extract_string(j,'$.foo') > 'a' order by id;
select id from t_plain where json_extract_string(j,'$.foo') > 'a' order by id;

-- string range with both ends bounded by real values in the data
select id from t where json_extract_string(j,'$.foo') >= 'bar' and json_extract_string(j,'$.foo') <= 'baz' order by id;
select id from t_plain where json_extract_string(j,'$.foo') >= 'bar' and json_extract_string(j,'$.foo') <= 'baz' order by id;

-- a strict inequality on a bound a row sits exactly on: the probe includes the
-- boundary term (both range ends are inclusive) and the retained predicate is
-- what removes the row, so this must NOT return row 2
select id from t where json_extract_float64(j,'$.n') > 20 order by id;
select id from t_plain where json_extract_float64(j,'$.n') > 20 order by id;

-- ------------------------------------------------- range dedup / wide ranges
-- Each row holds MANY leaves under one key, so a wide range reaches every row
-- once per matching element. The streaming probe walks one term at a time, so
-- each document must still arrive exactly once -- a repeated pk would multiply
-- rows through the index join and silently inflate the answer.
create table w (id int primary key, j json);
create table w_plain (id int primary key, j json);
insert into w values
 (1, '{"v":[1,2,3,4,5,6,7,8,9,10]}'),
 (2, '{"v":[11,12,13,14,15,16,17,18,19,20]}'),
 (3, '{"v":[100,200,300]}'),
 (4, '{"v":["a","b","c","d"]}'),
 (5, '{"v":[-1,-2,-3]}');
insert into w_plain select * from w;
create fulltext2 index wtj on w(j) with parser json;

-- a sweep wide enough to touch every numeric element of every row
select id from w where json_extract_float64(j,'$.v[0]') > -1000 order by id;
select id from w_plain where json_extract_float64(j,'$.v[0]') > -1000 order by id;

select count(*) from w where json_extract_float64(j,'$.v[0]') > -1000;
select count(*) from w_plain where json_extract_float64(j,'$.v[0]') > -1000;

-- a narrower range, still multi-element per row
select id from w where json_extract_float64(j,'$.v[0]') < 5 order by id;
select id from w_plain where json_extract_float64(j,'$.v[0]') < 5 order by id;

-- a string range under the SAME key finds the string row and no numeric one:
-- the two leaf encodings occupy disjoint stretches of the term space
select id from w where json_extract_string(j,'$.v[0]') >= 'a' order by id;
select id from w_plain where json_extract_string(j,'$.v[0]') >= 'a' order by id;

-- and the mirrored bound is empty rather than sweeping up the numeric rows
select id from w where json_extract_string(j,'$.v[0]') < 'a' order by id;
select id from w_plain where json_extract_string(j,'$.v[0]') < 'a' order by id;

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
