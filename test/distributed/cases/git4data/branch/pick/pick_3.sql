drop database if exists test;
create database test;
use test;

-- ================================================================
-- PICK Test 3: Mixed operations (INSERT + UPDATE)
-- ================================================================

-- case 1: pick updated rows from branch
create table t0 (a int, b int, primary key(a));
insert into t0 values (1,1),(2,2),(3,3),(4,4),(5,5);

data branch create table t1 from t0;
data branch create table t2 from t0;

-- t2 makes various changes
insert into t2 values (6,6),(7,7);
update t2 set b = 20 where a = 2;
update t2 set b = 40 where a = 4;

-- diff shows all changes
data branch diff t2 against t1;

-- pick only the updates (pk=2,4) — not the inserts (pk=6,7)
data branch pick t2 into t1 keys(2,4);
select * from t1 order by a asc;

-- pick the inserts too
data branch pick t2 into t1 keys(6,7);
select * from t1 order by a asc;

drop table t0;
drop table t1;
drop table t2;

-- case 2: pick subset of changes — selective cherry-pick
create table t0 (a int, b varchar(20), primary key(a));
insert into t0 select *, 'base' from generate_series(1,10)g;

data branch create table t1 from t0;
data branch create table t2 from t0;

-- t2: update odd numbers
update t2 set b = 'updated' where a in (1,3,5,7,9);

-- t1 stays the same as t0

-- pick only a=1 and a=5 from t2 into t1
data branch pick t2 into t1 keys(1,5);
select * from t1 order by a asc;

-- verify: a=3,7,9 still different
data branch diff t2 against t1;

drop table t0;
drop table t1;
drop table t2;

-- case 3: pick from multiple inserts — large key list
create table t1 (a int, b int, primary key(a));
insert into t1 values (1,1);

create table t2 (a int, b int, primary key(a));
insert into t2 values (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10);

-- pick even numbers only
data branch pick t2 into t1 keys(2,4,6,8,10);
select * from t1 order by a asc;

-- verify remaining diff
data branch diff t2 against t1;

drop table t1;
drop table t2;

drop database test;
