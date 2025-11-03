drop database if exists test;
create database test;
use test;

-- case 1: t0 is the LCA
create table t0(a int, b int, primary key(a));
insert into t0 values(1,1),(2,2);

create table t1 clone t0;
insert into t1 values(3, 3);

create table t2 clone t0;
insert into t2 values(3, 4);

data branch diff t2 against t1;
data branch merge t2 into t1;
data branch merge t2 into t1 when conflict skip;
select * from t1 order by a asc;
data branch merge t2 into t1 when conflict accept;
select * from t1 order by a asc;

drop table t0;
drop table t1;
drop table t2;

-- case 2: has no LCA
create table t1 (a int, b int, primary key(a));
insert into t1 values(1,1),(2,2);

create table t2 (a int, b int, primary key(a));
insert into t2 values(1,2),(3,3);

data branch diff t2 against t1;
data branch merge t2 into t1;
data branch merge t2 into t1 when conflict skip;
select * from t1 order by a asc;
data branch merge t2 into t1 when conflict accept;
select * from t1 order by a asc;

drop table t1;
drop table t2;

-- case 3: t1 is the LCA
create table t1 (a int, b int, primary key(a));
insert into t1 values(1,1),(2,2);

create table t2 clone t1;
update t2 set b = b + 2 where a = 1;
update t1 set b = b + 1 where a = 1;

data branch diff t2 against t1;
data branch merge t2 into t1;
data branch merge t2 into t1 when conflict skip;
select * from t1 order by a asc;
data branch merge t2 into t1 when conflict accept;
select * from t1 order by a asc;

drop table t1;
drop table t2;

-- case 4: t2 is the LCA
create table t2 (a int, b int, primary key(a));
insert into t2 values(1,1),(2,2);

create table t1 clone t2;
update t1 set b = b + 1 where a = 1;
update t2 set b = b + 2 where a = 1;

data branch diff t2 against t1;
data branch merge t2 into t1;
data branch merge t2 into t1 when conflict skip;
select * from t1 order by a asc;
data branch merge t2 into t1 when conflict accept;
select * from t1 order by a asc;

drop table t1;
drop table t2;

drop database test;




