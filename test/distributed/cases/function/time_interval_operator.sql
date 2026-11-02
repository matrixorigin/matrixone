-- TIME +/- INTERVAL binds like DATE_ADD/DATE_SUB on a TIME value (MySQL).
select cast('10:00:00' as time) + interval 1 hour;
select interval 90 minute + cast('10:00:00' as time);
select cast('10:00:00' as time) - interval 30 second;
select cast('10:00:00' as time) + interval '1:30' hour_minute;
select cast('23:30:00' as time) + interval 1 hour, date_add(cast('23:30:00' as time), interval 1 hour);
select cast(null as time) + interval 1 hour;
drop database if exists time_interval_op;
create database time_interval_op;
use time_interval_op;
create table t (id int primary key, t time);
insert into t values (1, '08:15:00'), (2, '01:00:00'), (3, null);
select id, t + interval 45 minute, t - interval 2 hour, interval 1 second + t from t order by id;
select id from t where t + interval 1 hour > '09:00:00' order by id;
drop database time_interval_op;
