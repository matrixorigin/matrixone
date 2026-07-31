-- @suite

-- @case
-- @desc: MySQL rejects window functions in UPDATE assignments and CHECK constraints unless MATRIXONE_NATIVE is enabled
-- @label:bvt

drop database if exists mysql_compat_window_invalid_context;
create database mysql_compat_window_invalid_context;
use mysql_compat_window_invalid_context;

set @old_sql_mode = @@session.sql_mode;
set session sql_mode = '';

create table t (id int primary key, grp int, v int, rn int default 0);
insert into t values (1,1,10,0),(2,1,20,0),(3,2,30,0),(4,2,40,0);

-- MySQL-compatible mode rejects both remaining invalid contexts.
update t set rn = row_number() over (order by id);
create table check_bad (
  id int primary key,
  v int,
  check (row_number() over (order by v) > 0)
);

-- MatrixOne native mode intentionally preserves support for these contexts.
set session sql_mode = 'MATRIXONE_NATIVE';
update t set rn = row_number() over (order by id);
select id, rn from t order by id;
create table check_bad (
  id int primary key,
  v int,
  check (row_number() over (order by v) > 0)
);

drop table check_bad;
drop table t;
set session sql_mode = @old_sql_mode;
drop database mysql_compat_window_invalid_context;
