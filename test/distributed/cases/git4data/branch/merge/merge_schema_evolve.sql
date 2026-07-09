drop database if exists test;
create database test;
use test;

-- =====================================================
-- Case 1: basic schema evolution merge - target has extra column
--   base:   [a, b]
--   target: [a, b, c]
-- =====================================================
create table t0(a int, b int, primary key(a));
insert into t0 values(1,1),(2,2),(3,3);
create snapshot sp0 for table test t0;

data branch create table t1 from t0{snapshot="sp0"};
alter table t1 add column c int default 0;
update t1 set c=10 where a=1;
insert into t1 values(4,4,40);
create snapshot sp1 for table test t1;

-- MERGE should succeed: only common columns (a, b) are written to base.
data branch merge t1 into t0;

-- Verify: t0 gets a=4 inserted, c is not written (t0 has no column c).
select * from t0 order by a;

drop snapshot sp1;
drop snapshot sp0;
drop table t0;
drop table t1;

-- =====================================================
-- Case 2: merge with INSERT + UPDATE on common column
--   t1 updates b (common) and adds a new row -> both applied to t0
-- =====================================================
create table t0(a int, b int, primary key(a));
insert into t0 values(1,1),(2,2),(3,3);
create snapshot sp0 for table test t0;

data branch create table t1 from t0{snapshot="sp0"};
alter table t1 add column c int default 0;
update t1 set b=99, c=10 where a=1;
insert into t1 values(4,4,40);
create snapshot sp1 for table test t1;

data branch merge t1 into t0 when conflict accept;

-- t0: a=1 b updated to 99, a=4 inserted
select * from t0 order by a;

drop snapshot sp1;
drop snapshot sp0;
drop table t0;
drop table t1;

-- =====================================================
-- Case 3: merge with DELETE
--   t1 deletes a row -> merge deletes it from t0
-- =====================================================
create table t0(a int, b int, primary key(a));
insert into t0 values(1,1),(2,2),(3,3);
create snapshot sp0 for table test t0;

data branch create table t1 from t0{snapshot="sp0"};
alter table t1 add column c int default 0;
delete from t1 where a=2;
create snapshot sp1 for table test t1;

data branch merge t1 into t0;

-- t0: a=2 deleted
select * from t0 order by a;

drop snapshot sp1;
drop snapshot sp0;
drop table t0;
drop table t1;

-- =====================================================
-- Case 4: merge with composite PK + extra column
--   base:   [a, b, c]   PK(a, b)
--   target: [a, b, c, d]
-- =====================================================
create table t0(a int, b int, c int, primary key(a,b));
insert into t0 values(1,1,10),(2,2,20);
create snapshot sp0 for table test t0;

data branch create table t1 from t0{snapshot="sp0"};
alter table t1 add column d int default 0;
update t1 set c=99 where a=1 and b=1;
insert into t1 values(3,3,30,300);
create snapshot sp1 for table test t1;

data branch merge t1 into t0 when conflict accept;

-- t0: (1,1) c updated to 99, (3,3) inserted, d is not written
select * from t0 order by a;

drop snapshot sp1;
drop snapshot sp0;
drop table t0;
drop table t1;

-- =====================================================
-- Case 5: merge is idempotent - merging twice produces no new changes
-- =====================================================
create table t0(a int, b int, primary key(a));
insert into t0 values(1,1),(2,2);
create snapshot sp0 for table test t0;

data branch create table t1 from t0{snapshot="sp0"};
alter table t1 add column c int default 0;
insert into t1 values(3,3,30);
create snapshot sp1 for table test t1;

data branch merge t1 into t0;
select * from t0 order by a;

-- Second merge: no diff, no changes
data branch merge t1 into t0;
select * from t0 order by a;

drop snapshot sp1;
drop snapshot sp0;
drop table t0;
drop table t1;

-- =====================================================
-- Case 6: target-only column sits between common columns
--   base:   [a, b]
--   target: [a, c, b]
-- =====================================================
create table t0(a int, b int, primary key(a));
insert into t0 values(1,1),(2,2),(3,3);
create snapshot sp0 for table test t0;

data branch create table t1 from t0{snapshot="sp0"};
alter table t1 add column c int default 0 after a;
update t1 set b=99 where a=1;
update t1 set c=88 where a=2;
insert into t1(a,c,b) values(4,40,4);
create snapshot sp1 for table test t1;

data branch merge t1 into t0 when conflict accept;
select * from t0 order by a;

drop snapshot sp1;
drop snapshot sp0;
drop table t0;
drop table t1;

-- =====================================================
-- Case 7: cluster-by hidden helper columns stay out of SQL/apply columns
-- =====================================================
create table t0(a int, b int) cluster by(a,b);
create snapshot sp0 for table test t0;

data branch create table t1 from t0{snapshot="sp0"};
alter table t1 add column c int default 0 after a;
create snapshot sp1 for table test t1;

data branch merge t1 into t0 when conflict accept;
select * from t0 order by a;

drop snapshot sp1;
drop snapshot sp0;
drop table t0;
drop table t1;

drop database test;
