-- @suite

-- @case
-- @desc: DECIMAL with a BOOL operand in arithmetic and comparison: TRUE/FALSE act as 1/0, NULL stays NULL (MySQL semantics)
-- @label:bvt

drop database if exists decimal_bool_arithmetic;
create database decimal_bool_arithmetic;
use decimal_bool_arithmetic;

create table td(id int primary key, d decimal(10,2), d2 decimal(30,4));
insert into td values (1, 1.50, 1.50), (2, 0, 0), (3, null, null);

select id, d + true as p1, true + d as p2, d - true as m1, true - d as m2, d * true as t1, false * d as t2 from td order by id;
select id, d / true as q1, true / d as q2, d % true as r1, true % d as r2, d div true as i1 from td order by id;
select id, d = true as eq, true = d as eq2, d <> true as ne, d > true as gt, d < true as lt, d >= true as ge, d <= false as le from td order by id;
select id, d2 + true as p, d2 - false as m, d2 * true as t, d2 / true as q, d2 % true as r, d2 = true as eq, d2 > false as gt from td order by id;
select cast(1.5 as decimal(10,2)) + true as a, true + cast(1.5 as decimal(10,2)) as b, cast(1.5 as decimal(10,2)) - true as c, cast(1.5 as decimal(10,2)) * true as e;
select cast(1.5 as decimal(10,2)) = true as a, cast(1.5 as decimal(10,2)) > true as b, cast(0.5 as decimal(10,2)) < true as c;
select 1.5 + true as a, true + 2.25 as b, 1.5 div true as c, 1.5 = true as e, 1.5 in (true, 2) as f;
select id from td where d > true order by id;
select id from td where d = false order by id;
select cast(true as decimal(10,2)) as t, cast(false as decimal(5,0)) as f, cast(true as decimal(30,4)) as t128, cast(true as decimal(60,4)) as t256;
create table ti(a decimal(10,2));
insert into ti values (true), (false), (null);
select a from ti order by a;

drop database if exists decimal_bool_arithmetic;
