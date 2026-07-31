-- @suite

-- @case
-- @desc: MySQL-compatible quantified subqueries with VALUES ROW and TABLE
-- @label:bvt
drop database if exists mysql_compat_model10c;
create database mysql_compat_model10c;
use mysql_compat_model10c;

create table t (id int primary key, v int);
create table tv (v int);
create table tv_pair (id int, v int);
create table tv_empty (v int);
create table tv_null (v int);
insert into t values (1,10),(2,20),(3,30),(4,null);
insert into tv values (20),(30);
insert into tv_pair values (2,20);
insert into tv_null values (20),(null);

select id from t where v > any (values row(15), row(25)) order by id;
select id from t where v = any (table tv) order by id;
select id from t where v in (values row(20), row(30)) order by id;
select id from t where v in (table tv) order by id;
select id from t where exists (values row(null)) order by id;
select count(*) as cnt from t where exists (table tv_empty);
select count(*) as cnt from t where v = any (table tv_empty);
select id from t where id < 4 and v > all (table tv_empty) order by id;
select id from t where (v = any (table tv_null)) is null order by id;
select id from t where v > all (values row(15), row(25)) order by id;
select id from t where v > any (values row(15), row(25) order by column_0 desc limit 1) order by id;
select id from t where v <= some (table tv) order by id;
select id from t where v = any (table tv order by v desc limit 1) order by id;
select id from t where (id, v) = any (table tv_pair) order by id;
select id from t where (id, v) = any (values row(2, 20)) order by id;
select id from t where v = any (table tv union values row(10)) order by id;
select id from t where v = any (values row(10) union table tv) order by id;
select id from t where v = any ((table tv order by v desc limit 1) union values row(10)) order by id;
select id from t where v = any ((values row(10), row(20) order by column_0 desc limit 1) union table tv) order by id;
select id from t where v = any (table tv intersect values row(20)) order by id;
select id from t where v = any (values row(10), row(20), row(30) except table tv) order by id;
select id from t where v = any (values row(10) union values row(20) intersect table tv) order by id;
select count(*) as cnt from (table tv union all values row(20)) as u;

drop database mysql_compat_model10c;
