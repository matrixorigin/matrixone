drop database if exists test;
create database test;
use test;

-- ================================================================
-- PICK Test 11: Composite primary key (multi-column PK)
-- ================================================================

-- case 1: literal tuple keys — composite PK (int, varchar)
create table t0 (id int, name varchar(20), val int, primary key(id, name));
insert into t0 values (1,'alice',10),(2,'bob',20),(3,'charlie',30);

data branch create table t1 from t0;
data branch create table t2 from t0;

insert into t2 values (4,'dave',40),(5,'eve',50),(6,'frank',60);

-- pick specific composite keys
data branch pick t2 into t1 keys((4,'dave'),(6,'frank'));
select * from t1 order by id, name;

-- verify: (5,'eve') still in diff
data branch diff t2 against t1;

-- pick remaining
data branch pick t2 into t1 keys((5,'eve'));
select * from t1 order by id, name;

-- all synced now
data branch diff t2 against t1;

drop table t0;
drop table t1;
drop table t2;

-- case 2: subquery keys — composite PK (int, int)
create table t0 (a int, b int, c varchar(20), primary key(a, b));
insert into t0 values (1,1,'base'),(2,2,'base');

data branch create table t1 from t0;
data branch create table t2 from t0;

insert into t2 values (3,3,'new'),(4,4,'new'),(5,5,'new'),(6,6,'new');

-- pick using subquery selecting both PK columns
data branch pick t2 into t1 keys(select a, b from t2 where a >= 4 and a % 2 = 0);
select * from t1 order by a, b;

-- verify: (3,3) and (5,5) still in diff
data branch diff t2 against t1;

drop table t0;
drop table t1;
drop table t2;

-- case 3: composite PK with no LCA (independent tables)
create table t1 (x int, y int, z int, primary key(x, y));
insert into t1 values (1,1,10),(2,2,20);

create table t2 (x int, y int, z int, primary key(x, y));
insert into t2 values (3,3,30),(4,4,40),(5,5,50);

data branch pick t2 into t1 keys((3,3),(5,5));
select * from t1 order by x, y;

drop table t1;
drop table t2;

-- case 4: composite PK — pick all via subquery
create table t0 (a int, b varchar(10), c int, primary key(a, b));
insert into t0 values (1,'x',100);

data branch create table t1 from t0;
data branch create table t2 from t0;

insert into t2 values (2,'y',200),(3,'z',300);

-- pick all new rows
data branch pick t2 into t1 keys(select a, b from t2 where a > 1);
select * from t1 order by a, b;

-- verify fully synced
data branch diff t2 against t1;

drop table t0;
drop table t1;
drop table t2;

-- case 5: composite PK — empty keys (no-op)
create table t0 (a int, b int, c int, primary key(a, b));
insert into t0 values (1,1,10),(2,2,20);

data branch create table t1 from t0;
data branch create table t2 from t0;

insert into t2 values (3,3,30);

-- subquery returns nothing
data branch pick t2 into t1 keys(select a, b from t2 where a > 100);
select * from t1 order by a, b;
-- expect unchanged: {(1,1),(2,2)}

drop table t0;
drop table t1;
drop table t2;

-- case 6: composite PK — snapshot-based pick
create table t0 (a int, b int, c int, primary key(a, b));
insert into t0 values (1,1,10);

data branch create table t1 from t0;
data branch create table t2 from t0;

insert into t2 values (2,2,20),(3,3,30);

create snapshot sp_before for account sys;

-- add more to t2 after snapshot
insert into t2 values (4,4,40);

-- pick from t2 at snapshot time — should not see (4,4,40)
data branch pick t2{snapshot=sp_before} into t1 keys((2,2),(3,3),(4,4));
select * from t1 order by a, b;
-- expect: {(1,1),(2,2),(3,3)} — (4,4) was not in snapshot

drop snapshot sp_before;
drop table t0;
drop table t1;
drop table t2;

drop database test;
