-- A text user variable in +/- INTERVAL arithmetic is read as a date/time
-- string, like a literal or DATE_ADD(@v, ...) (issue #31708).
set @dt = '2026-01-15';
select @dt + interval 1 day;
select interval 1 day + @dt;
select @dt - interval 1 month;
select (@dt) + interval 2 hour;
select @dt + interval 1 day = date_add(@dt, interval 1 day);
set @ts = '2026-01-15 10:20:30';
select @ts + interval 90 minute, @ts - interval 1 second;
set @bad = 'abc';
select @bad + interval 1 day;
set @n = null;
select @n + interval 1 day;
drop database if exists uvar_interval;
create database uvar_interval;
use uvar_interval;
create table t (id int primary key, d date);
insert into t values (1, '2026-01-14'), (2, '2026-01-16');
select id from t where d < @dt + interval 1 day order by id;
drop database uvar_interval;
