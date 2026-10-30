-- @suite

-- @case
-- @desc: GREATEST/LEAST with BOOL mixed with numeric or string arguments: TRUE/FALSE act as 1/0 (MySQL semantics)
-- @label:bvt

drop database if exists func_least_greatest_bool;
create database func_least_greatest_bool;
use func_least_greatest_bool;

select greatest(2, true) as a, least(2, true) as b, greatest(true, 0) as c, least(false, 3) as d, greatest(-1, false) as e;
select greatest(1e0, true) as a, least(0.5e0, true) as b, greatest(true, 2.5e0) as c;
select greatest('a', true) as a, least('a', true) as b, greatest('0', false) as c;
select greatest(null, true, 2) as a, least(true, null) as b, greatest(true, false) as c;

create table t(id int primary key, i int, u bigint unsigned, d double, s varchar(10), b bool);
insert into t values (1, 5, 7, 2.5, 'x', true), (2, -5, 0, 0.25, '0', false), (3, null, null, null, null, null), (4, 0, 1, -1, '', true);
select id, greatest(i, b) as gi, least(i, b) as li, greatest(u, b) as gu, least(u, b) as lu from t order by id;
select id, greatest(d, b) as gd, least(d, b) as ld, greatest(s, b) as gs, least(s, b) as ls from t order by id;
select id from t where greatest(i, b) = 1 order by id;
select id, greatest(i, b, d) as g3, least(b, u) as l2, least(b, i, d) as l3 from t order by id;

drop database if exists func_least_greatest_bool;
