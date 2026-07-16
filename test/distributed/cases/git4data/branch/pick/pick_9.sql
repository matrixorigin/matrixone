drop database if exists test;
create database test;
use test;

-- ================================================================
-- PICK Test 9: BETWEEN SNAPSHOT — time-range narrowing
-- ================================================================

-- ----------------------------------------------------------------
-- case a: BETWEEN SNAPSHOT picks only changes within the time range
-- ----------------------------------------------------------------
-- Timeline:
--   create t0 → {1,2,3}
--   branch t1 from t0
--   snapshot sp1
--   insert into t1 values (4,4),(5,5)   ← between sp1 and sp2
--   snapshot sp2
--   insert into t1 values (6,6),(7,7)   ← after sp2
--
-- PICK t1 INTO t0 BETWEEN SNAPSHOT sp1 AND sp2
-- → should pick only pk=4,5 (not 6,7)

drop snapshot if exists sp1;
drop snapshot if exists sp2;

create table t0 (a int, b int, primary key(a));
insert into t0 values (1,1),(2,2),(3,3);

data branch create table t1 from t0;

create snapshot sp1 for account sys;

insert into t1 values (4,4),(5,5);

create snapshot sp2 for account sys;

insert into t1 values (6,6),(7,7);

-- before pick: t0 has {1,2,3}, t1 has {1,2,3,4,5,6,7}
data branch pick t1 into t0 between snapshot sp1 and sp2;
select * from t0 order by a asc;
-- expect: {1,2,3,4,5}  (6,7 are outside sp1..sp2)

-- verify: diff still shows pk=6,7
data branch diff t1 against t0;

drop snapshot sp1;
drop snapshot sp2;
drop table t0;
drop table t1;

-- ----------------------------------------------------------------
-- case b: BETWEEN SNAPSHOT + KEYS — intersection of time and PKs
-- ----------------------------------------------------------------
-- Same timeline as case a, but additionally restrict to keys(4).
-- Only pk=4 is both in the time range AND in the key set.

drop snapshot if exists sp1;
drop snapshot if exists sp2;

create table t0 (a int, b int, primary key(a));
insert into t0 values (1,1),(2,2),(3,3);

data branch create table t1 from t0;

create snapshot sp1 for account sys;

insert into t1 values (4,4),(5,5);

create snapshot sp2 for account sys;

insert into t1 values (6,6),(7,7);

data branch pick t1 into t0 between snapshot sp1 and sp2 keys(4);
select * from t0 order by a asc;
-- expect: {1,2,3,4}  (5 in range but not in keys; 6,7 outside range)

drop snapshot sp1;
drop snapshot sp2;
drop table t0;
drop table t1;

-- ----------------------------------------------------------------
-- case c: Empty range — no changes between snapshots → no-op
-- ----------------------------------------------------------------

drop snapshot if exists sp1;
drop snapshot if exists sp2;

create table t0 (a int, b int, primary key(a));
insert into t0 values (1,1),(2,2),(3,3);

data branch create table t1 from t0;

create snapshot sp1 for account sys;
-- no changes on t1 between sp1 and sp2
create snapshot sp2 for account sys;

insert into t1 values (4,4);

data branch pick t1 into t0 between snapshot sp1 and sp2;
select * from t0 order by a asc;
-- expect: {1,2,3}  (pk=4 is after sp2, not picked)

drop snapshot sp1;
drop snapshot sp2;
drop table t0;
drop table t1;

-- ----------------------------------------------------------------
-- case d: BETWEEN SNAPSHOT with DELETE in range
-- ----------------------------------------------------------------
-- Timeline:
--   t0 = {1,2,3,4,5}, branch t1
--   snapshot sp1
--   delete from t1 where a=3   ← between sp1 and sp2
--   insert into t1 values (6,6)← between sp1 and sp2
--   snapshot sp2
--
-- PICK t1 INTO t0 BETWEEN SNAPSHOT sp1 AND sp2
-- → should propagate delete of pk=3, insert of pk=6

drop snapshot if exists sp1;
drop snapshot if exists sp2;

create table t0 (a int, b int, primary key(a));
insert into t0 values (1,1),(2,2),(3,3),(4,4),(5,5);

data branch create table t1 from t0;

create snapshot sp1 for account sys;

delete from t1 where a=3;
insert into t1 values (6,6);

create snapshot sp2 for account sys;

data branch pick t1 into t0 between snapshot sp1 and sp2;
select * from t0 order by a asc;
-- expect: {1,2,4,5,6}  (pk=3 deleted, pk=6 inserted)

drop snapshot sp1;
drop snapshot sp2;
drop table t0;
drop table t1;

-- ----------------------------------------------------------------
-- case e: BETWEEN SNAPSHOT with conflict — WHEN CONFLICT SKIP
-- ----------------------------------------------------------------
-- Timeline:
--   t0 = {1}, branch t1, branch t2
--   snapshot sp1
--   insert t1 (2,200), insert t2 (2,20)  ← conflict on pk=2
--   insert t1 (3,3)
--   snapshot sp2
--
-- PICK t1 INTO t2 BETWEEN SNAPSHOT sp1 AND sp2 WHEN CONFLICT SKIP
-- → pk=2 conflicts (both inserted with different values), skip; pk=3 succeeds

drop snapshot if exists sp1;
drop snapshot if exists sp2;

create table t0 (a int, b int, primary key(a));
insert into t0 values (1,1);

data branch create table t1 from t0;
data branch create table t2 from t0;

create snapshot sp1 for account sys;

insert into t1 values (2,200);
insert into t2 values (2,20);
insert into t1 values (3,3);

create snapshot sp2 for account sys;

data branch pick t1 into t2 between snapshot sp1 and sp2 when conflict skip;
select * from t2 order by a asc;
-- expect: {(1,1),(2,20),(3,3)}  (pk=2 conflict skipped, keeps dst value; pk=3 picked)

drop snapshot sp1;
drop snapshot sp2;
drop table t0;
drop table t1;
drop table t2;

-- ----------------------------------------------------------------
-- case f: BETWEEN SNAPSHOT with no LCA (independent tables)
-- ----------------------------------------------------------------
-- Independent tables have no LCA, collect range is [0, sp].
-- BETWEEN narrows to [sp1.next, sp2].

drop snapshot if exists sp1;
drop snapshot if exists sp2;

create table t1 (a int, b int, primary key(a));
insert into t1 values (1,1),(2,2);

create snapshot sp1 for account sys;

insert into t1 values (3,3),(4,4);

create snapshot sp2 for account sys;

insert into t1 values (5,5);

create table t2 (a int, b int, primary key(a));

data branch pick t1 into t2 between snapshot sp1 and sp2;
select * from t2 order by a asc;
-- expect: {3,4}  (only changes between sp1 and sp2; 1,2 before sp1; 5 after sp2)

drop snapshot sp1;
drop snapshot sp2;
drop table t1;
drop table t2;

-- ----------------------------------------------------------------
-- case g: BETWEEN upper bound after ALTER uses the bounded row image
-- ----------------------------------------------------------------
-- The same PK changes in the old physical generation and again after
-- ALTER.  PICK must hydrate the old change at sp_schema_to, not from the
-- current source row written after that snapshot.

drop snapshot if exists sp_schema_from;
drop snapshot if exists sp_schema_to;

create table t0 (a int, b int, primary key(a));
insert into t0 values (1,1);

data branch create table t1 from t0;

create snapshot sp_schema_from for account sys;
update t1 set b=10 where a=1;
alter table t1 add column c int default 7;
create snapshot sp_schema_to for account sys;
update t1 set b=20 where a=1;

data branch pick t1 into t0 between snapshot sp_schema_from and sp_schema_to;
select * from t0 order by a asc;
-- expect destination b=10 at the upper bound; current source b=20 is later
select * from t1 order by a asc;

drop snapshot sp_schema_from;
drop snapshot sp_schema_to;
drop table t0;
drop table t1;

-- ----------------------------------------------------------------
-- case h: BETWEEN upper bound before ALTER scans only the old generation
-- ----------------------------------------------------------------
-- The current physical table does not exist at sp_schema_to.  PICK must
-- resolve the old physical generation and apply its b=15 row image.

drop snapshot if exists sp_schema_from;
drop snapshot if exists sp_schema_to;

create table t0 (a int, b int, primary key(a));
insert into t0 values (1,1);

data branch create table t1 from t0;

create snapshot sp_schema_from for account sys;
update t1 set b=15 where a=1;
create snapshot sp_schema_to for account sys;
alter table t1 add column c int default 7;
update t1 set b=25 where a=1;

data branch pick t1 into t0 between snapshot sp_schema_from and sp_schema_to;
select * from t0 order by a asc;
-- expect destination b=15; ALTER and b=25 are outside the range
select * from t1 order by a asc;

drop snapshot sp_schema_from;
drop snapshot sp_schema_to;
drop table t0;
drop table t1;

-- ----------------------------------------------------------------
-- case i: no-LCA BETWEEN across ALTER excludes materialization
-- ----------------------------------------------------------------
-- t2 is schema-compatible with current t1 but independent of its DAG.
-- Only a=1 changes inside the bounded old-generation window; a=2 must
-- keep its independent destination value even though ALTER copies it.

drop snapshot if exists sp_nolca_from;
drop snapshot if exists sp_nolca_to;

create table t0 (a int, b int, primary key(a));
insert into t0 values (1,1),(2,2);
data branch create table t1 from t0;

create table t2 (a int, b int, c int default 7, primary key(a));
insert into t2 values (1,1,7),(2,200,7);

create snapshot sp_nolca_from for account sys;
update t1 set b=15 where a=1;
create snapshot sp_nolca_to for account sys;
alter table t1 add column c int default 7;
update t1 set b=25 where a=1;

data branch pick t1 into t2 between snapshot sp_nolca_from and sp_nolca_to when conflict accept;
select * from t2 order by a asc;
-- expect a=1 at bounded value 15 with c=NULL (column not yet present);
-- a=2 remains independent value 200
select * from t1 order by a asc;

drop snapshot sp_nolca_from;
drop snapshot sp_nolca_to;
drop table t2;
drop table t1;
drop table t0;

-- ----------------------------------------------------------------
-- case j: no-LCA BETWEEN hydrates through a dropped middle generation
-- ----------------------------------------------------------------
-- The upper-bound generation is itself replaced by a later ALTER. Rows from
-- the first generation still need its endpoint image, so the reader fallback
-- must use the relation opened at sp_double_to rather than current catalog.

drop snapshot if exists sp_double_from;
drop snapshot if exists sp_double_to;

create table t0 (a int, b int, primary key(a));
insert into t0 values (1,1),(2,2);
data branch create table t1 from t0;

create table t2 (a int, b int, c int default 7, d int default 9, primary key(a));
insert into t2 values (1,100,70,90),(2,200,71,91);

create snapshot sp_double_from for account sys;
update t1 set b=10 where a=1;
alter table t1 add column c int default 7;
update t1 set b=15,c=8 where a=1;
create snapshot sp_double_to for account sys;
alter table t1 add column d int default 9;
update t1 set b=25,c=18,d=19 where a=1;

data branch pick t1 into t2 between snapshot sp_double_from and sp_double_to when conflict accept;
select * from t2 order by a asc;
-- expect bounded middle-generation row; d did not exist at the upper bound
select * from t1 order by a asc;

drop snapshot sp_double_from;
drop snapshot sp_double_to;
drop table t2;
drop table t1;
drop table t0;

drop database test;
