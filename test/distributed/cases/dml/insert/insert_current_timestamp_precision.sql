-- @suit
-- @case
-- @desc:test for INSERT with current_timestamp precision
-- @label:bvt

drop database if exists test_timestamp_precision;
create database test_timestamp_precision;
use test_timestamp_precision;

-- Test current_timestamp with different precision scales
drop table if exists t_timestamp_precision;
create table t_timestamp_precision (id int primary key, ts0 timestamp(0), ts3 timestamp(3), ts6 timestamp(6));
insert into t_timestamp_precision values (1, current_timestamp(0), current_timestamp(3), current_timestamp(6));
select id, extract(microsecond from ts0) as ts0_micro, extract(microsecond from ts3) % 1000 as ts3_micro_remainder, case when extract(microsecond from ts0) = 0 then 'PASS' else 'FAIL' end as ts0_check, case when extract(microsecond from ts3) % 1000 = 0 then 'PASS' else 'FAIL' end as ts3_check from t_timestamp_precision;
drop table t_timestamp_precision;

-- Test DATETIME with current_timestamp
drop table if exists t_datetime_precision;
create table t_datetime_precision (id int primary key, dt0 datetime(0), dt3 datetime(3), dt6 datetime(6));
insert into t_datetime_precision values (1, current_timestamp(0), current_timestamp(3), current_timestamp(6));
select id, extract(microsecond from dt0) as dt0_micro, extract(microsecond from dt3) % 1000 as dt3_micro_remainder, case when extract(microsecond from dt0) = 0 then 'PASS' else 'FAIL' end as dt0_check, case when extract(microsecond from dt3) % 1000 = 0 then 'PASS' else 'FAIL' end as dt3_check from t_datetime_precision;
drop table t_datetime_precision;

-- Test NOW() function (alias of current_timestamp)
drop table if exists t_now_precision;
create table t_now_precision (id int primary key, dt0 datetime(0), dt3 datetime(3));
insert into t_now_precision values (1, now(0), now(3));
select id, extract(microsecond from dt0) as dt0_micro, extract(microsecond from dt3) % 1000 as dt3_micro_remainder, case when extract(microsecond from dt0) = 0 then 'PASS' else 'FAIL' end as dt0_check, case when extract(microsecond from dt3) % 1000 = 0 then 'PASS' else 'FAIL' end as dt3_check from t_now_precision;
drop table t_now_precision;

-- Test SYSDATE() function
drop table if exists t_sysdate_precision;
create table t_sysdate_precision (id int primary key, ts0 timestamp(0), ts1 timestamp(1), ts2 timestamp(2));
insert into t_sysdate_precision values (1, sysdate(0), sysdate(1), sysdate(2));
select id, extract(microsecond from ts0) as ts0_micro, extract(microsecond from ts1) % 100000 as ts1_remainder, extract(microsecond from ts2) % 10000 as ts2_remainder, case when extract(microsecond from ts0) = 0 then 'PASS' else 'FAIL' end as ts0_check, case when extract(microsecond from ts1) % 100000 = 0 then 'PASS' else 'FAIL' end as ts1_check, case when extract(microsecond from ts2) % 10000 = 0 then 'PASS' else 'FAIL' end as ts2_check from t_sysdate_precision;
drop table t_sysdate_precision;

-- Test INSERT with column default values
drop table if exists t_default_precision;
create table t_default_precision (id int primary key, dt0 datetime(0) default current_timestamp(0), dt3 datetime(3) default current_timestamp(3));
insert into t_default_precision (id) values (1);
select id, extract(microsecond from dt0) as dt0_micro, extract(microsecond from dt3) % 1000 as dt3_micro_remainder, case when extract(microsecond from dt0) = 0 then 'PASS' else 'FAIL' end as dt0_check, case when extract(microsecond from dt3) % 1000 = 0 then 'PASS' else 'FAIL' end as dt3_check from t_default_precision;
drop table t_default_precision;

-- Test with multiple inserts to ensure consistency
drop table if exists t_multi_insert;
create table t_multi_insert (id int primary key auto_increment, ts0 timestamp(0));
insert into t_multi_insert (ts0) values (current_timestamp(0));
insert into t_multi_insert (ts0) values (current_timestamp(0));
insert into t_multi_insert (ts0) values (current_timestamp(0));
select count(*) as total_rows, sum(case when extract(microsecond from ts0) = 0 then 1 else 0 end) as correct_precision_count, case when count(*) = sum(case when extract(microsecond from ts0) = 0 then 1 else 0 end) then 'PASS' else 'FAIL' end as all_check from t_multi_insert;
drop table t_multi_insert;

-- Test edge case: scale 4 and 5
drop table if exists t_scale_45;
create table t_scale_45 (id int primary key, ts4 timestamp(4), ts5 timestamp(5));
insert into t_scale_45 values (1, current_timestamp(4), current_timestamp(5));
select id, extract(microsecond from ts4) % 100 as ts4_remainder, extract(microsecond from ts5) % 10 as ts5_remainder, case when extract(microsecond from ts4) % 100 = 0 then 'PASS' else 'FAIL' end as ts4_check, case when extract(microsecond from ts5) % 10 = 0 then 'PASS' else 'FAIL' end as ts5_check from t_scale_45;
drop table t_scale_45;

-- Test precision mismatch: insert higher precision into lower precision columns
drop table if exists t_repro2;
create table t_repro2 (dt0 datetime(0), ts0 timestamp(0));
insert into t_repro2 values (current_timestamp(6), current_timestamp(3));
select extract(microsecond from dt0) as d0, extract(microsecond from ts0) t0 from t_repro2;
drop table t_repro2;

drop database test_timestamp_precision;
