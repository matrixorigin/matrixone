-- @suite
-- This suite asserts SQL semantics only and is intentionally topology-agnostic:
-- the same file runs in standalone and multi-CN BVT jobs.
-- @setup
drop database if exists right_single_rf;
create database right_single_rf;
use right_single_rf;

create table big_pk(id bigint primary key, v bigint, flag int);
-- Keep the probe comfortably above the default small-scan estimate so these
-- semantic cases exercise right-SINGLE RF instead of sitting on its cutoff.
insert into big_pk
select result, result * 10, result % 2 from generate_series(1, 20000) g;

create table small_lookup(id bigint primary key, lookup_id bigint, min_v bigint);
insert into small_lookup values
    (1, 1, 0),
    (2, 5000, 49999),
    (3, 30000, 0),
    (4, null, 0);

-- @case
-- @desc: right-SINGLE exact-IN keeps match, missing and NULL preserved rows
-- @label:bvt
select s.id, (select b.v from big_pk b where b.id = s.lookup_id) as scalar_v
from small_lookup s
order by s.id;

-- @case
-- @desc: leading cluster-key RF prunes ranges while a non-PK row filter preserves exact semantics
-- @label:bvt
create table big_cluster(id bigint, cluster_key bigint, v bigint) cluster by(cluster_key);
insert into big_cluster
select result, result, result * 10 from generate_series(1, 20000) g;
select s.id, (select b.v from big_cluster b where b.cluster_key = s.lookup_id) as scalar_v
from small_lookup s
order by s.id;

-- @case
-- @desc: empty preserved/build input returns promptly with correct scalar semantics
-- @label:bvt
create table empty_lookup(id bigint primary key, lookup_id bigint);
select e.id, (select b.v from big_pk b where b.id = e.lookup_id) as scalar_v
from empty_lookup e
order by e.id;

-- @case
-- @desc: residual predicates remain join conditions after RF key extraction
-- @label:bvt
select s.id, (select b.v from big_pk b
              where b.id = s.lookup_id and b.v > s.min_v) as scalar_v
from small_lookup s
order by s.id;

-- @case
-- @desc: duplicate matches still raise the SINGLE cardinality error
-- @label:bvt
create table inner_dup(id bigint, v bigint);
insert into inner_dup values (1, 10), (1, 11);
select s.id, (select d.v from inner_dup d where d.id = s.lookup_id) as scalar_v
from small_lookup s
where s.id = 1;

-- @case
-- @desc: full composite PK correlation remains exact
-- @label:bvt
create table big_composite(a bigint, b bigint, v bigint, primary key(a, b));
-- The same margin makes the leading composite-prefix case deterministic.
insert into big_composite
select result, result + 1, result * 100 from generate_series(1, 20000) g;
create table small_composite(id bigint primary key, a bigint, b bigint);
insert into small_composite values (1, 1, 2), (2, 5000, 5001), (3, 30000, 30001);
select s.id, (select b.v from big_composite b where b.a = s.a and b.b = s.b) as scalar_v
from small_composite s
order by s.id;

-- @case
-- @desc: leading composite-PK RF still reports duplicate scalar matches
-- @label:bvt
insert into big_composite values (1, 3, 101);
select s.id, (select b.v from big_composite b where b.a = s.a) as scalar_v
from small_composite s
where s.id = 1;

-- @case
-- @desc: UPDATE SET correlated scalar preserves affected rows and missing NULL
-- @label:bvt
create table update_target(id bigint primary key, lookup_id bigint, v bigint);
insert into update_target values (1, 1, -1), (2, 5000, -1), (3, 30000, -1);
update update_target t
set v = (select b.v from big_pk b where b.id = t.lookup_id);
select * from update_target order by id;

-- @case
-- @desc: INSERT SELECT correlated scalar remains correct
-- @label:bvt
create table insert_target(id bigint primary key, v bigint);
insert into insert_target
select s.id, (select b.v from big_pk b where b.id = s.lookup_id)
from small_lookup s;
select * from insert_target order by id;

-- @case
-- @desc: DELETE scalar predicate preserves DML semantics
-- @label:bvt
create table delete_target(id bigint primary key, lookup_id bigint);
insert into delete_target values (1, 1), (2, 2), (3, 30000), (4, null);
delete from delete_target t
where (select b.flag from big_pk b where b.id = t.lookup_id) = 1;
select * from delete_target order by id;

-- @case
-- @desc: a branch-level LIMIT 0 prunes the complete RF topology without invalidating UNION ALL
-- @label:bvt
select *
from (
    (select s.id, (select b.v from big_pk b where b.id = s.lookup_id) as scalar_v
     from small_lookup s
     limit 0)
    union all
    (select 99, 990)
) u
order by id;

-- @teardown
drop database right_single_rf;
