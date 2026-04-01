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

-- ----------------------------------------------------------------
-- case 6: snapshot chain — pick from historical state
-- ----------------------------------------------------------------

drop snapshot if exists sp_v1;
drop snapshot if exists sp_v2;
drop snapshot if exists sp_v3;

create table t1 (a int, b varchar(20), primary key(a));
insert into t1 values (1, 'v0'), (2, 'v0'), (3, 'v0');

create snapshot sp_v1 for account sys;

update t1 set b = 'v1' where a = 1;
insert into t1 values (4, 'v1');

create snapshot sp_v2 for account sys;

update t1 set b = 'v2' where a = 2;
delete from t1 where a = 3;
insert into t1 values (5, 'v2');

create snapshot sp_v3 for account sys;

-- current state: {(1,'v1'),(2,'v2'),(4,'v1'),(5,'v2')} — pk=3 deleted

create table t2 (a int, b varchar(20), primary key(a));

-- pick from v1 snapshot: all values are 'v0'
data branch pick t1{snapshot=sp_v1} into t2 keys(1, 2, 3);
select * from t2 order by a;
-- expect: (1,'v0'),(2,'v0'),(3,'v0')

drop table t2;

-- pick from v2 snapshot: pk=1 is 'v1', pk=4 exists
create table t2 (a int, b varchar(20), primary key(a));
data branch pick t1{snapshot=sp_v2} into t2 keys(1, 4);
select * from t2 order by a;
-- expect: (1,'v1'),(4,'v1')

drop snapshot sp_v1;
drop snapshot sp_v2;
drop snapshot sp_v3;
drop table t1;
drop table t2;

-- ----------------------------------------------------------------
-- case 7: larger scale with snapshot — 100 rows
-- ----------------------------------------------------------------

drop snapshot if exists sp_100;

create table t1 (a int, b int, primary key(a));
insert into t1 select *, result * 3 from generate_series(1, 100) g;

create snapshot sp_100 for account sys;

-- modify heavily after snapshot
delete from t1 where a > 80;
update t1 set b = 0 where a <= 20;

create table t2 (a int, b int, primary key(a));

-- pick from snapshot: should get original values
data branch pick t1{snapshot=sp_100} into t2 keys(1, 50, 100);
select * from t2 order by a;
-- expect: (1,3),(50,150),(100,300) — original values before modifications

drop snapshot sp_100;
drop table t1;
drop table t2;

-- ----------------------------------------------------------------
-- case 8: pick into table with existing data — no conflicts
-- ----------------------------------------------------------------

create table t1 (a int, b int, primary key(a));
insert into t1 values (1,1),(2,2),(3,3),(4,4),(5,5);

create table t2 (a int, b int, primary key(a));
insert into t2 values (10,10),(20,20),(30,30);

-- pick from t1 into t2 — different PKs, no conflicts
data branch pick t1 into t2 keys(1, 3, 5);
select * from t2 order by a;
-- expect: {1,3,5,10,20,30}

select count(*) from t2;
-- expect: 6

drop table t1;
drop table t2;

drop database test;
