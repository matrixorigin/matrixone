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

-- It really is float64: the 0.1 + 0.2 IEEE-754 artifact (decimal would give 0.3),
-- and the double magnitude range round-trips.
select cast(0.1 as double precision) + cast(0.2 as double precision) as f64_artifact;
drop table if exists dpb;
create table dpb(v double precision);
insert into dpb values (1.7976931348623157e308), (2.2250738585072014e-308), (0);
select v > 0 as positive from dpb order by v desc;
drop table if exists dpb;

-- Same M/D validation as DOUBLE (errors).
create table dperr(a double precision(300));
create table dperr(a double precision(10,40));
create table dperr(a double precision(5,10));

-- PRECISION is now a reserved word (matching MySQL): using it as an
-- identifier or alias is a syntax error.
create table dpres(precision int);
select 1 as precision;

-- Composes with UNSIGNED / ZEROFILL and column constraints (normalizes to double).
drop table if exists dpc;
create table dpc(a double precision unsigned, b double precision zerofill,
                 c double precision not null default 1.5 primary key);
show create table dpc;
insert into dpc(a, b, c) values (1.5, 2.5, 3.5);
select * from dpc;
drop table if exists dpc;

-- ALTER TABLE add / modify column with DOUBLE PRECISION.
drop table if exists dpa;
create table dpa(x int);
alter table dpa add column y double precision;
alter table dpa modify column x double precision(8,3);
show create table dpa;
drop table if exists dpa;
