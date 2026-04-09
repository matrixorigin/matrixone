drop database if exists test;
create database test;
use test;

-- ================================================================
-- PICK Test 10: BETWEEN SNAPSHOT edge cases
-- ================================================================

-- ----------------------------------------------------------------
-- case a: STRING snapshot names (quoted)
-- ----------------------------------------------------------------

drop snapshot if exists sp_from;
drop snapshot if exists sp_to;

create table t0 (a int, b int, primary key(a));
insert into t0 values (1,1);

data branch create table t1 from t0;

create snapshot sp_from for account sys;

insert into t1 values (2,2),(3,3);

create snapshot sp_to for account sys;

insert into t1 values (4,4);

data branch pick t1 into t0 between snapshot 'sp_from' and 'sp_to';
select * from t0 order by a asc;
-- expect: {1,2,3}

drop snapshot sp_from;
drop snapshot sp_to;
drop table t0;
drop table t1;

-- ----------------------------------------------------------------
-- case b: BETWEEN SNAPSHOT + WHEN CONFLICT ACCEPT
-- ----------------------------------------------------------------
-- Both sides insert pk=2 with different values, ACCEPT takes source.

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

data branch pick t1 into t2 between snapshot sp1 and sp2 when conflict accept;
select * from t2 order by a asc;
-- expect: {(1,1),(2,200),(3,3)}  (pk=2 conflict accepted, source value 200 wins)

drop snapshot sp1;
drop snapshot sp2;
drop table t0;
drop table t1;
drop table t2;

-- ----------------------------------------------------------------
-- case c: BETWEEN SNAPSHOT + WHEN CONFLICT FAIL
-- ----------------------------------------------------------------
-- Should raise an error on the conflict.

drop snapshot if exists sp1;
drop snapshot if exists sp2;

create table t0 (a int, b int, primary key(a));
insert into t0 values (1,1);

data branch create table t1 from t0;
data branch create table t2 from t0;

create snapshot sp1 for account sys;

insert into t1 values (2,200);
insert into t2 values (2,20);

create snapshot sp2 for account sys;

data branch pick t1 into t2 between snapshot sp1 and sp2 when conflict fail;

-- verify t2 unchanged after FAIL
select * from t2 order by a asc;
-- expect error above, then: {(1,1),(2,20)}

drop snapshot sp1;
drop snapshot sp2;
drop table t0;
drop table t1;
drop table t2;

-- ----------------------------------------------------------------
-- case d: BETWEEN + KEYS with STRING snapshot names and composite filter
-- ----------------------------------------------------------------
-- Only pick pk=5 out of changes {4,5,6} in time range.

drop snapshot if exists sp1;
drop snapshot if exists sp2;

create table t0 (a int, b int, primary key(a));
insert into t0 values (1,1),(2,2),(3,3);

data branch create table t1 from t0;

create snapshot sp1 for account sys;

insert into t1 values (4,4),(5,5),(6,6);

create snapshot sp2 for account sys;

insert into t1 values (7,7);

data branch pick t1 into t0 between snapshot 'sp1' and 'sp2' keys(5);
select * from t0 order by a asc;
-- expect: {1,2,3,5}

drop snapshot sp1;
drop snapshot sp2;
drop table t0;
drop table t1;

-- ----------------------------------------------------------------
-- case e: Multiple picks with BETWEEN on same table
-- ----------------------------------------------------------------
-- Two consecutive picks with different snapshot ranges.

drop snapshot if exists sp1;
drop snapshot if exists sp2;
drop snapshot if exists sp3;

create table t0 (a int, b int, primary key(a));
insert into t0 values (1,1);

data branch create table t1 from t0;

create snapshot sp1 for account sys;
insert into t1 values (2,2);
create snapshot sp2 for account sys;
insert into t1 values (3,3);
create snapshot sp3 for account sys;
insert into t1 values (4,4);

-- first pick: range [sp1, sp2] → pk=2
data branch pick t1 into t0 between snapshot sp1 and sp2;
select * from t0 order by a asc;
-- expect: {1,2}

-- second pick: range [sp2, sp3] → pk=3
data branch pick t1 into t0 between snapshot sp2 and sp3;
select * from t0 order by a asc;
-- expect: {1,2,3}

drop snapshot sp1;
drop snapshot sp2;
drop snapshot sp3;
drop table t0;
drop table t1;

-- ----------------------------------------------------------------
-- case f: Mutual exclusion — src with snapshot option + BETWEEN is error
-- ----------------------------------------------------------------

drop snapshot if exists sp1;
drop snapshot if exists sp2;

create table t0 (a int, b int, primary key(a));
create snapshot sp1 for account sys;
create snapshot sp2 for account sys;

data branch pick t0 {snapshot = sp1} into t0 between snapshot sp1 and sp2;

drop snapshot sp1;
drop snapshot sp2;
drop table t0;

drop database test;
