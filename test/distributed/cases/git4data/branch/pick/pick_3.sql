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

-- ----------------------------------------------------------------
-- case 4: DELETE + INSERT + UPDATE mix — medium scale
-- ----------------------------------------------------------------

create table t0 (a int, b varchar(20), primary key(a));
insert into t0 select *, 'orig' from generate_series(1, 30) g;

data branch create table t1 from t0;
data branch create table t2 from t0;

-- t2: mixed operations
delete from t2 where a in (5, 10, 15, 20, 25, 30);
update t2 set b = 'modified' where a in (1, 2, 3, 4);
insert into t2 values (31, 'new31'), (32, 'new32'), (33, 'new33');

-- pick the deletes + one insert + one update
data branch pick t2 into t1 keys(5, 10, 15, 31, 1);
select * from t1 where a in (1, 5, 10, 15, 31) order by a asc;
-- expect: (1,'modified'), (31,'new31'); pk=5,10,15 deleted

select count(*) from t1;
-- expect: 28 (30 original - 3 deleted + 1 inserted)

-- verify remaining diff
data branch diff t2 against t1;

drop table t0;
drop table t1;
drop table t2;

-- ----------------------------------------------------------------
-- case 5: large batch pick — 200 rows, pick 50
-- ----------------------------------------------------------------

create table t1 (a int, b int, primary key(a));
insert into t1 select *, 0 from generate_series(1, 100) g;

create table t2 (a int, b int, primary key(a));
insert into t2 select *, result * 2 from generate_series(1, 200) g;

-- pick every 4th row from the extension range
data branch pick t2 into t1 keys(
    104, 108, 112, 116, 120, 124, 128, 132, 136, 140,
    144, 148, 152, 156, 160, 164, 168, 172, 176, 180,
    184, 188, 192, 196, 200
);
select count(*) from t1;
-- expect: 125 (100 + 25 picked)

-- verify a specific picked row
select * from t1 where a = 200;
-- expect: (200, 400)

drop table t1;
drop table t2;

drop database test;
