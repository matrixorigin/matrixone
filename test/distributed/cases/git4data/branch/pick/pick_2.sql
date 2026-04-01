drop database if exists test;
create database test;
use test;

-- ================================================================
-- PICK Test 2: Conflict handling (FAIL, SKIP, ACCEPT)
-- ================================================================

-- case 1: conflict with LCA — both sides insert same PK with different values
create table t0 (a int, b int, primary key(a));
insert into t0 values (1,1),(2,2);

data branch create table t1 from t0;
insert into t1 values (3,30);

data branch create table t2 from t0;
insert into t2 values (3,40);

-- pick pk=3: should conflict (both inserted pk=3 with different values)
-- default is FAIL
data branch pick t2 into t1 keys(3);

-- pick with SKIP: keeps t1's value
data branch pick t2 into t1 keys(3) when conflict skip;
select * from t1 order by a asc;

-- pick with ACCEPT: overwrites with t2's value
data branch pick t2 into t1 keys(3) when conflict accept;
select * from t1 order by a asc;

drop table t0;
drop table t1;
drop table t2;

-- case 2: conflict without LCA — overlapping PKs
create table t1 (a int, b int, primary key(a));
insert into t1 values (1,10),(2,20);

create table t2 (a int, b int, primary key(a));
insert into t2 values (1,100),(3,30);

-- pick pk=1: conflicts because pk=1 exists in both with different values
data branch pick t2 into t1 keys(1);

-- pick pk=1 with skip: keeps t1 value
data branch pick t2 into t1 keys(1) when conflict skip;
select * from t1 order by a asc;

-- pick pk=1 with accept: takes t2 value
data branch pick t2 into t1 keys(1) when conflict accept;
select * from t1 order by a asc;

-- pick pk=3: no conflict (new key)
data branch pick t2 into t1 keys(3);
select * from t1 order by a asc;

drop table t1;
drop table t2;

-- case 3: conflict from update on both sides
create table t0 (a int, b int, primary key(a));
insert into t0 values (1,1),(2,2),(3,3);

data branch create table t1 from t0;
update t1 set b = 10 where a = 1;

data branch create table t2 from t0;
update t2 set b = 20 where a = 1;

-- pick pk=1: both updated, should conflict
data branch pick t2 into t1 keys(1);

-- skip: keep t1's update (b=10)
data branch pick t2 into t1 keys(1) when conflict skip;
select * from t1 order by a asc;

-- accept: take t2's update (b=20)
data branch pick t2 into t1 keys(1) when conflict accept;
select * from t1 order by a asc;

drop table t0;
drop table t1;
drop table t2;

-- ----------------------------------------------------------------
-- case 4: conflict with NULL values in non-PK columns
-- ----------------------------------------------------------------

create table t0 (a int, b int, c varchar(20), primary key(a));
insert into t0 values (1, 10, 'hello'), (2, 20, NULL), (3, NULL, 'world');

data branch create table t1 from t0;
data branch create table t2 from t0;

-- both sides insert pk=4 with different NULL patterns
insert into t1 values (4, 100, NULL);
insert into t2 values (4, NULL, 'new');

-- conflict: same PK, different values
data branch pick t2 into t1 keys(4) when conflict fail;

data branch pick t2 into t1 keys(4) when conflict skip;
select * from t1 where a = 4;
-- expect: (4, 100, NULL) — kept t1 value

data branch pick t2 into t1 keys(4) when conflict accept;
select * from t1 where a = 4;
-- expect: (4, NULL, 'new') — took t2 value

drop table t0;
drop table t1;
drop table t2;

-- ----------------------------------------------------------------
-- case 5: larger scale conflicts — batch with mixed outcomes
-- ----------------------------------------------------------------

create table t0 (a int, b int, primary key(a));
insert into t0 select *, 0 from generate_series(1, 20) g;

data branch create table t1 from t0;
data branch create table t2 from t0;

-- t1: update even numbers
update t1 set b = a * 10 where a % 2 = 0;

-- t2: update multiples of 3 + insert new rows
update t2 set b = a * 100 where a % 3 = 0;
insert into t2 values (21, 21), (22, 22), (23, 23);

-- pk=6,12,18: both sides updated — source wins (update conflicts not detected)
-- pk=21,22,23: new inserts from t2
data branch pick t2 into t1 keys(6, 12, 18, 21, 22, 23) when conflict skip;
select * from t1 where a in (6, 12, 18, 21, 22, 23) order by a asc;
-- expect: 6→600, 12→1200, 18→1800 (src wins), 21→21, 22→22, 23→23

data branch pick t2 into t1 keys(6) when conflict accept;
select * from t1 where a = 6;
-- expect: (6, 600) — already t2's value

drop table t0;
drop table t1;
drop table t2;

drop database test;
