drop database if exists test;
create database test;
use test;

create table t1(a int primary key);
create table t2(a int);
insert into t2 select * from generate_series(1, 819200)g;

begin;
replace into t1 select a from t2 where a mod 11 = 0;
replace into t1 select a from t2 where a mod 13 = 0;
replace into t1 select a from t2 where a mod 17 = 0;
rollback;

-- issue #24472: REPLACE INTO SELECT from a large source should not choose
-- shuffle DEDUP JOIN when OldColCapture is required.
create table t_replace_shuffle_capture(a int primary key, b int, marker int);
begin;
replace into t_replace_shuffle_capture select a, a, 24472 from t2;
rollback;

drop table if exists t1;
drop table if exists t_replace_shuffle_capture;
drop table if exists t2;
drop database if exists test;
