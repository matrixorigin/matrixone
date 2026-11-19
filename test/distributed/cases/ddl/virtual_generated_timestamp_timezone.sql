-- Issue #28140: a virtual DATE(TIMESTAMP) uses the reader's time zone.
drop database if exists issue28140_virtual_tz;
create database issue28140_virtual_tz;
use issue28140_virtual_tz;
create table t (id int primary key, ts timestamp(6) not null,
    d date as (date(ts)) virtual, s date as (date(ts)) stored, key kd(d));
set time_zone='+00:00';
insert into t(id,ts) values (1,'2024-02-29 23:30:00.123456');
set time_zone='+08:00';
select count(*) as matched from t where d='2024-03-01' and d=date(ts) and s='2024-02-29';
select count(*) as matched from t force index(kd) where d='2024-03-01';
select count(*) as matched from t ignore index(kd) where d='2024-03-01';
set time_zone='+00:00';
select count(*) as matched from t force index(kd) where d='2024-02-29';
drop database issue28140_virtual_tz;
