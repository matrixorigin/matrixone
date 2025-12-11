drop database if exists test;
create database test;
use test;

-- case 1: t0 is the LCA
create table t0(a int, b int, primary key(a));
insert into t0 values(1,1),(2,2);

data branch create table t1 from t0;
insert into t1 values(3, 3);

data branch create table t2 from t0;
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

data branch create table t2 from t1;
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

data branch create table t1 from t2;
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


-- case 4: complex merge
create table t0(a int primary key, b varchar(10));
insert into t0 select *, "t0" from generate_series(1, 100)g;

data branch create table t1 from t0;
update t1 set b = "t1" where a in (1, 20, 40, 60, 80, 100);

data branch create table t2 from t0;
update t2 set b = "t2" where a in (2, 22, 42, 62, 82);

data branch create table t3 from t0;
update t3 set b = "t3" where a in (3, 23, 43, 63, 83);

data branch merge t1 into t0;
select count(*) as cnt, b from t0 group by b order by cnt;

data branch merge t2 into t0;
select count(*) as cnt, b from t0 group by b order by cnt;

data branch merge t3 into t0;
select count(*) as cnt, b from t0 group by b order by cnt;

drop database test;




