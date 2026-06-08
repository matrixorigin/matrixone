-- @suite

-- @case
-- @desc:test date_trunc function
-- @label:bvt
drop database if exists date_trunc_db;
create database date_trunc_db;
use date_trunc_db;
set time_zone = '+00:00';
select date_trunc('year', cast('2024-05-16 12:34:56.123456' as datetime));
select date_trunc('quarter', cast('2024-05-16 12:34:56.123456' as datetime));
select date_trunc('month', cast('2024-05-16 12:34:56.123456' as datetime));
select date_trunc('week', cast('2024-05-19 12:34:56.123456' as datetime));
select date_trunc('day', cast('2024-05-16 12:34:56.123456' as datetime));
select date_trunc('hour', cast('2024-05-16 12:34:56.123456' as datetime));
select date_trunc('minute', cast('2024-05-16 12:34:56.123456' as datetime));
select date_trunc('second', cast('2024-05-16 12:34:56.123456' as datetime));
select date_trunc('year', cast('2024-05-16 12:34:56.123456' as timestamp)) as ts_year;
select date_trunc('quarter', cast('2024-05-16 12:34:56.123456' as timestamp)) as ts_quarter;
select date_trunc('month', cast('2024-05-16 12:34:56.123456' as timestamp)) as ts_month;
select date_trunc('week', cast('2024-05-19 12:34:56.123456' as timestamp)) as ts_week;
select date_trunc('day', cast('2024-05-16 12:34:56.123456' as timestamp)) as ts_day;
select date_trunc('hour', cast('2024-05-16 12:34:56.123456' as timestamp)) as ts_hour;
select date_trunc('minute', cast('2024-05-16 12:34:56.123456' as timestamp)) as ts_minute;
select date_trunc('second', cast('2024-05-16 12:34:56.123456' as timestamp)) as ts_second;
select date_trunc('YEAR', cast('2024-05-16 12:34:56.123456' as timestamp)) as ts_upper_year;
select date_trunc('Hour', cast('2024-05-16 12:34:56.123456' as timestamp)) as ts_mixed_hour;
select date_trunc('week', cast('2025-01-01 12:34:56.123456' as timestamp)) as ts_week_year_boundary;
select date_trunc('year', cast('2024-05-16' as date)) as d_year;
select date_trunc('quarter', cast('2024-05-16' as date)) as d_quarter;
select date_trunc('month', cast('2024-05-16' as date));
select date_trunc('week', cast('2025-01-01' as date)) as d_week_year_boundary;
select date_trunc('day', cast('2024-05-16' as date)) as d_day;
select date_trunc('hour', cast(null as timestamp)) as ts_null;
select date_trunc('hour', null) as null_dt;
select date_trunc(null, cast('2024-05-16 12:34:56.123456' as datetime)) as unit_null_dt;
select date_trunc('hour', cast('2024-05-16' as date));
select date_trunc('minute', cast('2024-05-16' as date));
select date_trunc('second', cast('2024-05-16' as date));
select date_trunc('bogus', cast('2024-05-16 12:34:56' as datetime));
select date_trunc('bogus', cast('2024-05-16 12:34:56' as timestamp));
select date_trunc('day', '2024-05-16 12:34:56');
select date_trunc('day', 1);
select date_trunc('day');
select date_trunc('day', cast('2024-05-16 12:34:56' as datetime), cast('2024-05-16 12:34:56' as datetime));
create table date_trunc_readings(d date, dt datetime, ts timestamp, value int);
insert into date_trunc_readings values ('2024-05-16', '2024-05-16 12:01:00', '2024-05-16 12:01:00', 1), ('2024-05-16', '2024-05-16 12:45:00', '2024-05-16 12:45:00', 2), ('2024-05-16', '2024-05-16 13:01:00', '2024-05-16 13:01:00', 3);
select date_trunc('month', d), date_trunc('day', dt), date_trunc('hour', ts) from date_trunc_readings order by value limit 1;
select date_trunc('hour', ts) as bucket, sum(value) as total from date_trunc_readings group by bucket order by bucket;
drop database date_trunc_db;
