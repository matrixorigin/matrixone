drop database if exists test;
create database test;
use test;

-- ================================================================
-- PICK Test 6: Edge cases
-- ================================================================

-- case 1: pick non-existent key — should be no-op
create table t0 (a int, b int, primary key(a));
insert into t0 values (1,1),(2,2);

data branch create table t1 from t0;
data branch create table t2 from t0;

insert into t2 values (3,3);

-- pick pk=99 which doesn't exist in t2's changes
data branch pick t2 into t1 keys(99);
select * from t1 order by a asc;

-- pick pk=3 which does exist
data branch pick t2 into t1 keys(3);
select * from t1 order by a asc;

drop table t0;
drop table t1;
drop table t2;

-- case 2: pick all changed keys — equivalent to merge
create table t0 (a int, b int, primary key(a));
insert into t0 values (1,1),(2,2);

data branch create table t1 from t0;
data branch create table t2 from t0;

insert into t2 values (3,3),(4,4),(5,5);

-- pick all three new keys = same as merge
data branch pick t2 into t1 keys(3,4,5);
select * from t1 order by a asc;

-- diff should be empty now
data branch diff t2 against t1;

drop table t0;
drop table t1;
drop table t2;

-- case 3: pick single key
create table t1 (a int, b int, primary key(a));
insert into t1 values (1,1);

create table t2 (a int, b int, primary key(a));
insert into t2 values (1,1),(2,2),(3,3);

data branch pick t2 into t1 keys(2);
select * from t1 order by a asc;

drop table t1;
drop table t2;

-- case 4: repeated picks of same key
create table t0 (a int, b int, primary key(a));
insert into t0 values (1,1),(2,2);

data branch create table t1 from t0;
data branch create table t2 from t0;

insert into t2 values (3,3);

-- first pick
data branch pick t2 into t1 keys(3);
select * from t1 order by a asc;

-- second pick of same key — should be no-op (already merged)
data branch pick t2 into t1 keys(3) when conflict skip;
select * from t1 order by a asc;

drop table t0;
drop table t1;
drop table t2;

-- case 5: pick with snapshots
create table t1 (a int, b varchar(10), primary key(a));
insert into t1 values (1,'a'),(2,'b'),(3,'c');
create snapshot sp1 for table test t1;

create table t2 like t1;
insert into t2 values (1,'a'),(2,'b'),(4,'d'),(5,'e');
create snapshot sp2 for table test t2;

-- pick pk=4 from t2 into t1
data branch pick t2{snapshot="sp2"} into t1{snapshot="sp1"} keys(4);
select * from t1 order by a asc;

drop snapshot sp1;
drop snapshot sp2;
drop table t1;
drop table t2;

drop database test;
