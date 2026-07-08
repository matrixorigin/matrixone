drop database if exists test;
create database test;
use test;

-- =====================================================
-- Case 1: basic schema evolution - target has one extra column
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

-- DIFF should succeed: only common columns (a, b) are compared.
-- Row a=4 is new in t1; rows a=1,2,3 have matching common columns.
data branch diff t1{snapshot="sp1"} against t0{snapshot="sp0"};

drop snapshot sp1;
drop snapshot sp0;
drop table t0;
drop table t1;

-- =====================================================
-- Case 2: multiple extra columns on target
--   base:   [a, b]
--   target: [a, b, c, d]
-- =====================================================
create table t0(a int, b int, primary key(a));
insert into t0 values(1,1),(2,2);
create snapshot sp0 for table test t0;

data branch create table t1 from t0{snapshot="sp0"};
alter table t1 add column c int default 0;
alter table t1 add column d varchar(20) default 'x';
insert into t1 values(3,3,30,'new');
create snapshot sp1 for table test t1;

data branch diff t1{snapshot="sp1"} against t0{snapshot="sp0"};

drop snapshot sp1;
drop snapshot sp0;
drop table t0;
drop table t1;

-- =====================================================
-- Case 3: extra column + UPDATE on a common column
--   t1 updates b (common col) -> diff should show UPDATE
-- =====================================================
create table t0(a int, b int, primary key(a));
insert into t0 values(1,1),(2,2);
create snapshot sp0 for table test t0;

data branch create table t1 from t0{snapshot="sp0"};
alter table t1 add column c int default 0;
update t1 set b=99 where a=1;
create snapshot sp1 for table test t1;

-- a=1: common col b differs (1 vs 99) -> UPDATE
-- a=2: no change
data branch diff t1{snapshot="sp1"} against t0{snapshot="sp0"};

drop snapshot sp1;
drop snapshot sp0;
drop table t0;
drop table t1;

-- =====================================================
-- Case 4: extra column + UPDATE only on target-only column
--   t1 updates c (target-only) -> diff should show NO change
-- =====================================================
create table t0(a int, b int, primary key(a));
insert into t0 values(1,1),(2,2);
create snapshot sp0 for table test t0;

data branch create table t1 from t0{snapshot="sp0"};
alter table t1 add column c int default 0;
update t1 set c=99 where a=1;
create snapshot sp1 for table test t1;

-- Common columns (a, b) match for all rows -> empty diff
data branch diff t1{snapshot="sp1"} against t0{snapshot="sp0"};

drop snapshot sp1;
drop snapshot sp0;
drop table t0;
drop table t1;

-- =====================================================
-- Case 5: extra column + DELETE on target
--   t1 deletes a row -> diff should show DELETE
-- =====================================================
create table t0(a int, b int, primary key(a));
insert into t0 values(1,1),(2,2),(3,3);
create snapshot sp0 for table test t0;

data branch create table t1 from t0{snapshot="sp0"};
alter table t1 add column c int default 0;
delete from t1 where a=2;
create snapshot sp1 for table test t1;

-- a=2: t1 deleted it, t0 still has it -> DELETE (c is null, projected from base)
data branch diff t1{snapshot="sp1"} against t0{snapshot="sp0"};

drop snapshot sp1;
drop snapshot sp0;
drop table t0;
drop table t1;

-- =====================================================
-- Case 6: composite primary key + extra column
--   base:   [a, b, c]   PK(a, b)
--   target: [a, b, c, d]
-- =====================================================
create table t0(a int, b int, c int, primary key(a,b));
insert into t0 values(1,1,10),(2,2,20);
create snapshot sp0 for table test t0;

data branch create table t1 from t0{snapshot="sp0"};
alter table t1 add column d int default 0;
insert into t1 values(3,3,30,300);
create snapshot sp1 for table test t1;

data branch diff t1{snapshot="sp1"} against t0{snapshot="sp0"};

drop snapshot sp1;
drop snapshot sp0;
drop table t0;
drop table t1;

-- =====================================================
-- Case 7: type mismatch on a common column -> error
--   base:   [a, b int]
--   target: [a, b varchar]
-- =====================================================
create table t0(a int, b int, primary key(a));
insert into t0 values(1,1),(2,2);
create snapshot sp0 for table test t0;

create table t1(a int, b varchar(20), primary key(a));
insert into t1 values(1,'1'),(2,'2');
create snapshot sp1 for table test t1;

-- @regex("schema compatibility check: column 'b' exists in both schemas but has different types", true)
data branch diff t1{snapshot="sp1"} against t0{snapshot="sp0"};

drop snapshot sp1;
drop snapshot sp0;
drop table t0;
drop table t1;

-- =====================================================
-- Case 8: PK column removed from target -> error
--   base:   [a, b]   PK(a)
--   target: [b]      (a was dropped, no PK)
-- =====================================================
create table t0(a int, b int, primary key(a));
insert into t0 values(1,1),(2,2);
create snapshot sp0 for table test t0;

data branch create table t1 from t0{snapshot="sp0"};
alter table t1 drop column a;
create snapshot sp1 for table test t1;

-- @regex("schema compatibility check: primary key column 'a' is not present in common columns", true)
data branch diff t1{snapshot="sp1"} against t0{snapshot="sp0"};

drop snapshot sp1;
drop snapshot sp0;
drop table t0;
drop table t1;

drop database test;
