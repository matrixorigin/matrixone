drop database if exists test;
create database test;
use test;

-- case 1: no LCA
--  i. delete
create table t1(a int, b int, primary key(a));
insert into t1 select *,* from generate_series(1, 8192)g;
delete from t1 where a in (9, 99, 999);

create table t2(a int, b int, primary key(a));
insert into t2 select *,* from generate_series(1, 8192)g;

snapshot diff t2 against t1;

delete from t2 where a in (9, 99, 999);
snapshot diff t2 against t1;

drop table t1;
drop table t2;

--  ii. update
create table t1(a int, b int, primary key(a));
insert into t1 select *,* from generate_series(1, 8192)g;
update t1 set b = b+1 where a in (9, 99, 999);

create table t2(a int, b int, primary key(a));
insert into t2 select *,* from generate_series(1, 8192)g;

snapshot diff t2 against t1;

update t2 set b = b+1 where a in (9, 99, 999);
snapshot diff t2 against t1;

drop table t1;
drop table t2;

-- case 2: has LCA t0 (not t1 nor t2)
--  i. delete on LCA
create table t0(a int, b int, primary key(a));
insert into t0 values(1,1),(2,2),(3,3);

create table t1 clone t0;
insert into t1 values(4,4);
delete from t1 where a = 3;

create table t2 clone t0;
insert into t2 values(5, 5);
snapshot diff t2 against t1;

delete from t2 where a = 3;
snapshot diff t2 against t1;

drop table t0;
drop table t1;
drop table t2;

--  ii. update on LCA
create table t0(a int, b int, primary key(a));
insert into t0 values(1,1),(2,2),(3,3);

create table t1 clone t0;
insert into t1 values(4,4);
update t1 set b = b + 1 where a = 3;

create table t2 clone t0;
insert into t2 values(5,5);
snapshot diff t2 against t1;

update t2 set b = b + 1 where a = 3;
snapshot diff t2 against t1;

drop table t0;
drop table t1;
drop table t2;

drop database test;