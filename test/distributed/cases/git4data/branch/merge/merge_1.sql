drop database if exists test;
create database test;
use test;

-- case 1: no lca
--  i. simple merge
create table t1 (a int, b int, primary key(a));
insert into t1 values (1,1),(3,3),(5,5);

create table t2 (a int, b int, primary key(a));
insert into t2 values(1,1),(2,2),(4,4);

data branch diff t2 against t1;
data branch merge t2 into t1;

select * from t1 order by a asc;
data branch diff t2 against t1;

drop table t1;
drop table t2;

--  ii. mix merge
create table t1 (a int, b int, primary key(a));
insert into t1 values (1,1),(3,3),(5,5);
update t1 set b = b + 1 where a in (1, 5);

create table t2 (a int, b int, primary key(a));
insert into t2 values(1,1),(2,2),(4,4);
update t2 set b = b + 1 where a in (1, 4);

data branch diff t2 against t1;
data branch merge t2 into t1;

select * from t1 order by a asc;
data branch diff t2 against t1;

drop table t1;
drop table t2;

-- case 2: has lca t0
--  i. simple merge
create table t0 (a int, b int, primary key(a));
insert into t0 values (1,1),(2,2),(3,3);

data branch create table t1 from t0;
insert into t1 values(4,4);

data branch create table t2 from t0;
insert into t2 values(5,5);

data branch diff t2 against t1;
data branch merge t2 into t1;
select * from t1 order by a asc;
data branch diff t2 against t1;

drop table t0;
drop table t1;
drop table t2;

-- case 3: t1 is the lca
--  i. simple merge
create table t1(a int, b int, primary key(a));
insert into t1 values(1,1),(2,2);

data branch create table t2 from t1;
insert into t2 values(3,3),(4,4);

data branch diff t2 against t1;
data branch merge t2 into t1;
select * from t1 order by a asc;
data branch diff t2 against t1;

drop table t1;
drop table t2;

--  ii. mix merge
create table t1(a int, b int, primary key(a));
insert into t1 values(1,1),(2,2);

data branch create table t2 from t1;
insert into t2 values(3,3),(4,4);
update t2 set b = b + 1 where a in (1,3);

data branch diff t2 against t1;
data branch merge t2 into t1;
select * from t1 order by a asc;
data branch diff t2 against t1;

drop table t1;
drop table t2;

-- case 4: t2 is the lca
--  i. simple merge
create table t2 (a int, b int, primary key(a));
insert into t2 values(1,1),(2,2);

data branch create table t1 from t2;
insert into t1 values(3,3),(4,4);

data branch diff t2 against t1;
data branch merge t2 into t1;
select * from t1 order by a asc;
data branch diff t2 against t1;

drop table t1;
drop table t2;

--  ii. mix merge
create table t2 (a int, b int, primary key(a));
insert into t2 values(1,1),(2,2);

data branch create table t1 from t2;
insert into t1 values(3,3),(4,4);

insert into t2 values (5,5);

data branch diff t2 against t1;
data branch merge t2 into t1;
select * from t1 order by a asc;
data branch diff t2 against t1;

drop table t1;
drop table t2;

drop database test;
