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
select date_trunc('month', cast('2024-05-16' as date));
select date_trunc('hour', cast('2024-05-16' as date));
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
