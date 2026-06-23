-- DOUBLE PRECISION is the SQL-standard synonym for DOUBLE (float64).

drop table if exists dp;
create table dp(a double precision, b double precision(10,2), c double);
-- normalizes to `double` in the catalog, like MySQL.
show create table dp;

insert into dp values (3.1415926535897, 1234.5678, 2.71828);
insert into dp values (-1.5e10, 0.01, 0.00001);
select * from dp order by a;

-- behaves as float64: arithmetic and aggregation.
select a + c as s, a * 2 as dbl from dp order by 1;
select sum(c) as sc from dp;

-- DOUBLE PRECISION as a cast target via CAST, CONVERT and the :: operator.
select cast('3.14' as double precision) as cd;
select convert('3.14', double precision) as cv;
select '3.14' :: double precision as cc;
select abs(cast(-2.5 as double precision)) as ab;
select cast(7 as double precision(10,2)) as cdp;

drop table if exists dp;

-- DOUBLE and DOUBLE PRECISION are the same type.
drop table if exists dp2;
create table dp2(a double precision);
insert into dp2 values (1.25);
insert into dp2 select cast(2.5 as double) from dp2;
select * from dp2 order by a;
drop table if exists dp2;
