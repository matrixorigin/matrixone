-- @suite
-- @case
-- @desc: YEAR assignment honors strict SQL mode
-- @label:bvt

drop database if exists year_strict_sqlmode;
create database year_strict_sqlmode;
use year_strict_sqlmode;
set @old_sql_mode = @@session.sql_mode;

create table t (id int primary key, y year);

-- Strict assignments reject invalid four-digit YEAR values even when the
-- destination column is nullable.
set session sql_mode = 'STRICT_TRANS_TABLES';
insert into t values (1, 1901), (2, 1900), (3, 2155), (4, 2156);
insert into t values (2, '2156');
select count(*) from t;

-- Non-strict assignments adjust invalid YEAR values to 0000, not SQL NULL.
set session sql_mode = '';
insert into t values (3, 1900), (4, '2156');
select id, y + 0, y is null from t order by id;

-- IGNORE applies the same 0000 adjustment under strict mode.
truncate table t;
set session sql_mode = 'STRICT_TRANS_TABLES';
insert ignore into t values (5, 1900), (6, '2156');
select id, y + 0, y is null from t order by id;

set session sql_mode = @old_sql_mode;
drop database year_strict_sqlmode;
