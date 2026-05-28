drop database if exists test;
create database test;
use test;

-- Schema evolution: target table has an extra column compared to base
create table t0(a int, b int, primary key(a));
insert into t0 values(1,1),(2,2),(3,3);
create snapshot sp0 for table test t0;

data branch create table t1 from t0{snapshot="sp0"};
alter table t1 add column c int default 0;
update t1 set c=10 where a=1;
insert into t1 values(4,4,40);
create snapshot sp1 for table test t1;

-- DIFF should succeed: t1 has [a,b,c], t0 has [a,b]
data branch diff t1{snapshot="sp1"} against t0{snapshot="sp0"};

drop snapshot sp1;
drop snapshot sp0;
drop table t0;
drop table t1;
drop database test;
