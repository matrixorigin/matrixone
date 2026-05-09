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

drop database test;
