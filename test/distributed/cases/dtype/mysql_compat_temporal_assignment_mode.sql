-- @suite

-- @case
-- @desc: Temporal assignment and calendar calculations must use execution-time SQL mode
-- @label:bvt

drop database if exists mysql_compat_temporal_assignment_mode;
create database mysql_compat_temporal_assignment_mode;
use mysql_compat_temporal_assignment_mode;

set @old_sql_mode = @@session.sql_mode;
set session sql_mode = 'ALLOW_INVALID_DATES';

drop table if exists source_temporal;
drop table if exists target_temporal;
create table source_temporal (
    id int primary key,
    d date,
    dt datetime(6)
);
create table target_temporal (
    id int auto_increment primary key,
    d date,
    dt datetime(6),
    ts timestamp(6)
);
insert into source_temporal values (1, '2024-02-30', '2024-04-31 12:34:56');
insert into target_temporal (id, d, ts) values (1, '2024-01-01', '2024-01-01 00:00:00');

-- SQL-level INSERT ... SELECT and UPDATE must take the permissive runtime cast.
set session sql_mode = '';
insert into target_temporal (id, ts)
select 2, d from source_temporal where id = 1;
update target_temporal t
join source_temporal s on s.id = 1
set t.d = s.dt
where t.id = 1;
select id, d, ts from target_temporal order by id;

-- A literal in a cached PREPARE must be revalidated on every EXECUTE.
set session sql_mode = 'ALLOW_INVALID_DATES';
prepare prepared_date from 'insert into target_temporal (d) values (''2024-02-30'')';
prepare prepared_datetime from 'insert into target_temporal (dt) values (''2024-04-31 12:34:56'')';
set session sql_mode = '';
-- @regex("parsedate",true)
execute prepared_date;
-- @regex("invalid datetime value",true)
execute prepared_datetime;
set session sql_mode = 'ALLOW_INVALID_DATES';
execute prepared_date;
execute prepared_datetime;
set session sql_mode = '';
-- @regex("parsedate",true)
execute prepared_date;
-- @regex("invalid datetime value",true)
execute prepared_datetime;
deallocate prepare prepared_date;
deallocate prepare prepared_datetime;
set session sql_mode = 'ALLOW_INVALID_DATES';
select count(*) as prepared_rows from target_temporal;
select count(*) as prepared_invalid_dates
from target_temporal where d = '2024-02-30';
select count(*) as prepared_invalid_datetimes
from target_temporal where dt = '2024-04-31 12:34:56';

-- Tagged invalid dates must normalize overflow fields for calendar arithmetic.
set session sql_mode = 'ALLOW_INVALID_DATES';
select weekday(cast('2024-02-30' as date)) as weekday_overflow,
       extract(week from cast('2024-04-31' as date)) as extract_week_overflow,
       dayofyear(cast('2024-04-31' as date)) as dayofyear_overflow;

drop table target_temporal;
drop table source_temporal;
set session sql_mode = @old_sql_mode;
drop database mysql_compat_temporal_assignment_mode;
