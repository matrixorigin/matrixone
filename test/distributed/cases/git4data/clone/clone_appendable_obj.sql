drop database if exists test;
create database test;
use test;

create table t1(a int primary key, b int);
insert into t1 select *,* from generate_series(1, 10)g;
delete from t1 where a mod 2 = 0;

drop snapshot if exists sp;
create snapshot sp for table test t1;

insert into t1 select *,* from generate_series(11, 20)g;
delete from t1 where a mod 2 = 0;

drop snapshot if exists sp2;
create snapshot sp2 for table test t1;

delete from t1 where a mod 3 = 0;

-- @ignore:0
select mo_ctl("dn", "flush", "test.t1");

create table t2 clone t1 {snapshot = "sp"};
select * from t2 order by a asc;

create table t3 clone t1 {snapshot = "sp2"};
select * from t3 order by a asc;

create table t4 clone t1;
select * from t4 order by a asc;

drop database test;
drop snapshot sp;
drop snapshot sp2;