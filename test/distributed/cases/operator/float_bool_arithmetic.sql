-- @suite

-- @case
-- @desc: FLOAT/DOUBLE with a BOOL operand in arithmetic and comparison: TRUE/FALSE act as 1/0, NULL stays NULL (MySQL semantics)
-- @label:bvt

drop database if exists float_bool_arithmetic;
create database float_bool_arithmetic;
use float_bool_arithmetic;

create table tf(id int primary key, f float, d double, b bool);
insert into tf values (1, 1.5, 2.5, true), (2, 0, 0, false), (3, null, null, null), (4, -3, -4.5, true);

select id, d + true as p1, true + d as p2, d - true as m1, true - d as m2, d * true as t1, false * d as t2 from tf order by id;
select id, d / true as q1, true / d as q2, d % true as r1, d div true as i1, true div d as i2 from tf order by id;
select id, d = true as eq, true = d as eq2, d <> true as ne, d > true as gt, d < true as lt, d >= true as ge, d <= false as le, d <=> b as nse from tf order by id;
select id, f + b as p, f - b as m, f * b as t, f / b as q, f = b as eq, f > b as gt, b < f as lt from tf order by id;
select 1e0 + true as a, true + 2.5e0 as b, 3e0 / true as c, 5e0 % true as e, 5e0 div true as g, 1e0 = true as h, 0e0 = false as i;
select cast(1.5 as float) + true as a, cast(1.5 as double) - false as b, cast(0.5 as float) < true as c;
select id from tf where d > b order by id;
select id from tf where f = b order by id;
select nullif(1e0, true) as a, nullif(2e0, true) as b, 1e0 in (true, 3e0) as c;

drop database if exists float_bool_arithmetic;
