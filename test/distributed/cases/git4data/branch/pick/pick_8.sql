drop database if exists test;
create database test;
use test;

-- ================================================================
-- PICK Test 8: DELETE scenarios
-- Tests c/d/e/f from the 6 fundamental PICK scenarios:
--   c) src doesn't have pk, dst has pk → should be ignored
--   d) src deleted pk, dst deleted pk → should be ignored
--   e) src deleted pk, dst unchanged → delete propagation
--   f) src has pk, dst deleted pk → re-insert from source
-- ================================================================

-- ----------------------------------------------------------------
-- case 1 (scenario c): src doesn't have pk, dst has pk → ignore
-- Setup: LCA has pk=1,2,3.  dst adds pk=4.  src unchanged.
-- PICK keys(4) should be a no-op because pk=4 doesn't exist in src.
-- ----------------------------------------------------------------
create table t0 (a int, b int, primary key(a));
insert into t0 values (1,1),(2,2),(3,3);

data branch create table t1 from t0;
-- t1 (dst) adds a new row
insert into t1 values (4,40);

data branch create table t2 from t0;
-- t2 (src) makes no changes

-- before pick: t1 should have pk=1,2,3,4
select * from t1 order by a asc;

-- pick keys(4): src doesn't have pk=4 → should be ignored
data branch pick t2 into t1 keys(4);

-- after pick: t1 should still have pk=1,2,3,4 (unchanged)
select * from t1 order by a asc;

drop table t0;
drop table t1;
drop table t2;

-- ----------------------------------------------------------------
-- case 2 (scenario d): both sides deleted same pk → ignore
-- Setup: LCA has pk=1,2,3.  Both branches delete pk=2.
-- PICK keys(2) should be a no-op (both deleted, nothing to transfer).
-- ----------------------------------------------------------------
create table t0 (a int, b int, primary key(a));
insert into t0 values (1,1),(2,2),(3,3);

data branch create table t1 from t0;
delete from t1 where a = 2;

data branch create table t2 from t0;
delete from t2 where a = 2;

-- before pick: t1 has pk=1,3
select * from t1 order by a asc;

-- pick keys(2): both deleted → no-op
data branch pick t2 into t1 keys(2);

-- after pick: t1 should still have pk=1,3
select * from t1 order by a asc;

drop table t0;
drop table t1;
drop table t2;

-- ----------------------------------------------------------------
-- case 3 (scenario e): src deleted pk, dst unchanged → delete dst
-- Setup: LCA has pk=1,2,3.  src deletes pk=2.  dst unchanged.
-- PICK keys(2) should delete pk=2 from dst.
-- ----------------------------------------------------------------
create table t0 (a int, b int, primary key(a));
insert into t0 values (1,1),(2,2),(3,3);

data branch create table t1 from t0;
-- t1 (dst) makes no changes

data branch create table t2 from t0;
-- t2 (src) deletes pk=2
delete from t2 where a = 2;

-- before pick: t1 has pk=1,2,3
select * from t1 order by a asc;

-- pick keys(2): src deleted pk=2 → should delete from dst
data branch pick t2 into t1 keys(2);

-- after pick: t1 should have pk=1,3 (pk=2 deleted)
select * from t1 order by a asc;

drop table t0;
drop table t1;
drop table t2;

-- ----------------------------------------------------------------
-- case 4 (scenario f): src has pk, dst deleted pk → ACCEPT re-inserts
-- Setup: LCA has pk=1,2,3. dst deletes pk=2. src updates pk=2.
-- Default/FAIL treats this as a conflict; ACCEPT re-inserts pk=2 with src's value.
-- ----------------------------------------------------------------
create table t0 (a int, b int, primary key(a));
insert into t0 values (1,1),(2,2),(3,3);

data branch create table t1 from t0;
-- t1 (dst) deletes pk=2
delete from t1 where a = 2;

data branch create table t2 from t0;
-- t2 (src) updates pk=2 to a new value
update t2 set b = 200 where a = 2;

-- before pick: t1 has pk=1,3 (pk=2 deleted)
select * from t1 order by a asc;

data branch pick t2 into t1 keys(2) when conflict fail;
data branch pick t2 into t1 keys(2) when conflict accept;

-- after ACCEPT: t1 should have pk=1,2,3 with pk=2 having b=200 (re-inserted)
select * from t1 order by a asc;

drop table t0;
drop table t1;
drop table t2;

-- ----------------------------------------------------------------
-- case 5 (scenario e variant): src deleted, dst has unrelated changes
-- Setup: LCA has pk=1,2,3,4,5.  src deletes pk=3.
--        dst updates pk=1 and adds pk=6.
-- PICK keys(3) should only delete pk=3 from dst, not touch pk=1,6.
-- ----------------------------------------------------------------
create table t0 (a int, b int, primary key(a));
insert into t0 values (1,1),(2,2),(3,3),(4,4),(5,5);

data branch create table t1 from t0;
-- t1 (dst) makes its own changes
update t1 set b = 10 where a = 1;
insert into t1 values (6,6);

data branch create table t2 from t0;
-- t2 (src) deletes pk=3
delete from t2 where a = 3;

-- before pick
select * from t1 order by a asc;

-- pick keys(3): src deleted → propagate delete to dst
data branch pick t2 into t1 keys(3);

-- after pick: pk=3 removed, but pk=1(b=10) and pk=6 untouched
select * from t1 order by a asc;

drop table t0;
drop table t1;
drop table t2;

-- ----------------------------------------------------------------
-- case 6 (scenario c variant): src doesn't have pk, dst inserted
-- pk after LCA. No LCA version of the topology.
-- Setup: t1 and t2 are independent (no LCA).
--        t1 has pk=1,2,3.  t2 has pk=1,2.
-- PICK keys(3) from t2 into t1: pk=3 not in t2 → should be ignored.
-- ----------------------------------------------------------------
create table t1 (a int, b int, primary key(a));
insert into t1 values (1,1),(2,2),(3,3);

create table t2 (a int, b int, primary key(a));
insert into t2 values (1,1),(2,2);

-- pick keys(3) from t2 into t1: t2 doesn't have pk=3
data branch pick t2 into t1 keys(3);

-- t1 should remain unchanged
select * from t1 order by a asc;

drop table t1;
drop table t2;

drop database test;
