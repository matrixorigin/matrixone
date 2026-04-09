drop database if exists test;
create database test;
use test;

-- ================================================================
-- PICK Test 1: Basic PICK with all LCA topologies
-- ================================================================

-- case 1: no LCA — pick specific rows from independent tables
create table t1 (a int, b int, primary key(a));
insert into t1 values (1,1),(3,3),(5,5);

create table t2 (a int, b int, primary key(a));
insert into t2 values (1,1),(2,2),(4,4);

-- diff to see what exists
data branch diff t2 against t1;

-- pick only pk=2 from t2 into t1
data branch pick t2 into t1 keys(2);
select * from t1 order by a asc;

-- pick pk=4 from t2 into t1
data branch pick t2 into t1 keys(4);
select * from t1 order by a asc;

-- verify diff after picks
data branch diff t2 against t1;

drop table t1;
drop table t2;

-- case 2: has LCA (t0 is the common ancestor)
create table t0 (a int, b int, primary key(a));
insert into t0 values (1,1),(2,2),(3,3);

data branch create table t1 from t0;
insert into t1 values (4,4);

data branch create table t2 from t0;
insert into t2 values (5,5),(6,6),(7,7);

-- diff to see divergence
data branch diff t2 against t1;

-- pick only pk=5 and pk=7 from t2 into t1 (skip pk=6)
data branch pick t2 into t1 keys(5,7);
select * from t1 order by a asc;

-- verify: pk=6 should still be in diff
data branch diff t2 against t1;

drop table t0;
drop table t1;
drop table t2;

-- case 3: t1 is the LCA (dst is ancestor of src)
create table t1 (a int, b int, primary key(a));
insert into t1 values (1,1),(2,2);

data branch create table t2 from t1;
insert into t2 values (3,3),(4,4),(5,5);

-- pick only pk=3 from t2 into t1
data branch pick t2 into t1 keys(3);
select * from t1 order by a asc;

-- verify: pk=4,5 still in diff
data branch diff t2 against t1;

drop table t1;
drop table t2;

-- case 4: t2 is the LCA (src is ancestor of dst)
create table t2 (a int, b int, primary key(a));
insert into t2 values (1,1),(2,2);

data branch create table t1 from t2;
insert into t1 values (3,3),(4,4);

-- t2 has no new data relative to t1, but t1 has new data
-- pick should be a no-op since t2 has nothing new
data branch diff t2 against t1;

-- pick non-existing key: should be a no-op
data branch pick t2 into t1 keys(99);
select * from t1 order by a asc;

drop table t1;
drop table t2;

-- ----------------------------------------------------------------
-- case 5: snapshot-based src picking with LCA
-- ----------------------------------------------------------------
-- Take a snapshot of t2, then modify t2 further.
-- Pick from t2{snapshot=sp1} to freeze the source state.

drop snapshot if exists sp_src;

create table t0 (a int, b int, primary key(a));
insert into t0 values (1,1),(2,2),(3,3);

data branch create table t1 from t0;
data branch create table t2 from t0;

insert into t2 values (4,4),(5,5);
create snapshot sp_src for account sys;

-- further changes on t2 after snapshot
update t2 set b = 50 where a = 5;
insert into t2 values (6,6);

-- pick from snapshot: t2 at sp_src has (5,5) not (5,50), and no pk=6
data branch pick t2{snapshot=sp_src} into t1 keys(4,5,6);
select * from t1 order by a asc;
-- expect: {1,2,3,4,5} — pk=5 has b=5 (snapshot value), pk=6 absent

drop snapshot sp_src;
drop table t0;
drop table t1;
drop table t2;

-- ----------------------------------------------------------------
-- case 6: medium-scale pick — 100 rows, selective keys
-- ----------------------------------------------------------------

create table t0 (a int, b varchar(20), primary key(a));
insert into t0 select *, 'base' from generate_series(1, 50) g;

data branch create table t1 from t0;
data branch create table t2 from t0;

-- t2: insert 50 new rows + update 10 existing
insert into t2 select *, 'new' from generate_series(51, 100) g;
update t2 set b = 'changed' where a <= 10;

-- pick only pk=55,60,65,70,75,80,85,90,95,100 (10 new rows)
data branch pick t2 into t1 keys(55,60,65,70,75,80,85,90,95,100);
select count(*) from t1;
-- expect: 60

-- pick one of the updates
data branch pick t2 into t1 keys(5);
select * from t1 where a = 5;
-- expect: (5,'changed')

-- verify diff still has remaining rows
data branch diff t2 against t1;

drop table t0;
drop table t1;
drop table t2;

-- ----------------------------------------------------------------
-- case 7: no-LCA with snapshot src, larger data
-- ----------------------------------------------------------------

drop snapshot if exists sp_freeze;

create table t1 (a int, b int, primary key(a));
insert into t1 select *, result * 10 from generate_series(1, 30) g;

create snapshot sp_freeze for account sys;

-- modify after snapshot
delete from t1 where a > 25;
update t1 set b = 999 where a = 1;

create table t2 (a int, b int, primary key(a));

-- pick from frozen snapshot: should have all 30 rows, original values
data branch pick t1{snapshot=sp_freeze} into t2 keys(1,10,20,30);
select * from t2 order by a asc;
-- expect: (1,10),(10,100),(20,200),(30,300) — original values

drop snapshot sp_freeze;
drop table t1;
drop table t2;

drop database test;
