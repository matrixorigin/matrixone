drop database if exists test;
create database test;
use test;

-- ================================================================
-- PICK Test 5: KEYS with subquery
-- ================================================================

-- case 1: keys from subquery — select specific PKs
create table t0 (a int, b int, primary key(a));
insert into t0 values (1,1),(2,2),(3,3);

data branch create table t1 from t0;
data branch create table t2 from t0;

insert into t2 values (4,4),(5,5),(6,6),(7,7);

-- pick keys selected by a subquery: pick even numbers > 3
data branch pick t2 into t1 keys(select a from t2 where a > 3 and a % 2 = 0);
select * from t1 order by a asc;

-- verify: pk=5,7 still in diff
data branch diff t2 against t1;

-- pick remaining via subquery
data branch pick t2 into t1 keys(select a from t2 where a in (5,7));
select * from t1 order by a asc;

drop table t0;
drop table t1;
drop table t2;

-- case 2: keys from another table
create table t1 (a int, b int, primary key(a));
insert into t1 values (1,1);

create table t2 (a int, b int, primary key(a));
insert into t2 values (1,1),(2,2),(3,3),(4,4),(5,5);

-- helper table with keys to pick
create table pick_keys (k int);
insert into pick_keys values (2),(4);

data branch pick t2 into t1 keys(select k from pick_keys);
select * from t1 order by a asc;

drop table pick_keys;
drop table t1;
drop table t2;

drop database test;
