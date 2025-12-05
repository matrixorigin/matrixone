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

data branch diff t2 against t1;

delete from t2 where a in (9, 99, 999);
data branch diff t2 against t1;

drop table t1;
drop table t2;

--  ii. update
create table t1(a int, b int, primary key(a));
insert into t1 select *,* from generate_series(1, 8192)g;
update t1 set b = b+1 where a in (9, 99, 999);

create table t2(a int, b int, primary key(a));
insert into t2 select *,* from generate_series(1, 8192)g;

data branch diff t2 against t1;

update t2 set b = b+1 where a in (9, 99, 999);
data branch diff t2 against t1;

drop table t1;
drop table t2;

-- case 2: has LCA t0 (not t1 nor t2)
--  i. delete on LCA
create table t0(a int, b int, primary key(a));
insert into t0 values(1,1),(2,2),(3,3);

data branch create table t1 from t0;
insert into t1 values(4,4);
delete from t1 where a = 3;

data branch create table t2 from t0;
insert into t2 values(5, 5);
data branch diff t2 against t1;

delete from t2 where a = 3;
data branch diff t2 against t1;

drop table t0;
drop table t1;
drop table t2;

--  ii. update on LCA
create table t0(a int, b int, primary key(a));
insert into t0 values(1,1),(2,2),(3,3);

data branch create table t1 from t0;
insert into t1 values(4,4);
update t1 set b = b + 1 where a = 3;

data branch create table t2 from t0;
insert into t2 values(5,5);
data branch diff t2 against t1;

update t2 set b = b + 1 where a = 3;
data branch diff t2 against t1;

drop table t0;
drop table t1;
drop table t2;

-- case 3: t1 is the LCA
--  i. delete on LCA
create table t1(a int, b int, primary key(a));
insert into t1 select *,* from generate_series(1,100)g;

data branch create table t2 from t1;
insert into t2 select *,* from generate_series(101,200)g;
delete from t2 where a >= 100;

data branch diff t2 against t1;

drop table t1;
drop table t2;

-- case 4: mix
create table t0(a int, b int, c int, primary key(a, b));
insert into t0 select *,*,* from generate_series(1, 8192)g;

data branch create table t1 from t0;
update t1 set c = c + 1 where a in (1, 100, 1000);
insert into t1 values(9000, 9000, 9000);

data branch create table t2 from t0;
update t2 set c = c + 1 where a in (2, 200, 1000);
insert into t2 values(9001, 9001, 9001);

data branch diff t2 against t1;

drop database test;