drop database if exists test;
create database test;
use test;

-- =====================================================
-- Case 0: exact regression for issue #24549
-- =====================================================
create table a(id int primary key, f0 double);
data branch create table b from a;
alter table b add column f1 double default 0.0;
data branch diff b against a;
data branch merge b into a;
drop table a;
drop table b;

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
update t1 set b=22 where a=2;
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
--   base:   [`select`, `line item`, c]   composite PK with quoted/reserved names
--   target: [`select`, `line item`, c, d]
-- =====================================================
create table t0(`select` int, `line item` int, c int, primary key(`select`,`line item`));
insert into t0 values(1,1,10),(2,2,20);
create snapshot sp0 for table test t0;

data branch create table t1 from t0{snapshot="sp0"};
-- The UPDATE belongs to the pre-ALTER physical generation. COPY ALTER rebuilds
-- __mo_cpkey_col with a new Seqnum; DIFF must still map that derived key.
update t1 set c=11 where `select`=1 and `line item`=1;
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

-- @regex("schema compatibility check: target primary key columns .* do not match base primary key columns", true)
data branch diff t1{snapshot="sp1"} against t0{snapshot="sp0"};

drop snapshot sp1;
drop snapshot sp0;
drop table t0;
drop table t1;

-- =====================================================
-- Case 9: target-only column sits between common columns
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

-- a=1 common column b changed; a=2 only target-only c changed; a=4 inserted.
data branch diff t1{snapshot="sp1"} against t0{snapshot="sp0"};

drop snapshot sp1;
drop snapshot sp0;
drop table t0;
drop table t1;

-- =====================================================
-- Case 10: cluster-by + added column reaches the fake-PK rejection boundary
-- =====================================================
-- MatrixOne does not allow an explicit primary key together with CLUSTER BY.
-- A legal cluster-by table therefore uses the fake PK, and adding a target-only
-- column is rejected before DIFF output can expose any hidden helper column.
create table t0(a int, b int) cluster by(a,b);
insert into t0 values(1,1),(2,2);

data branch create table t1 from t0;
alter table t1 add column c int default 0 after a;

-- @regex("schema compatibility check: target-only columns require an explicit primary key", true)
data branch diff t1 against t0;

drop table t0;
drop table t1;

-- =====================================================
-- Case 11: base-only non-PK column is rejected
-- =====================================================
create table t0(a int, b int, primary key(a));
insert into t0 values(1,1),(2,2);
create snapshot sp0 for table test t0;

data branch create table t1 from t0{snapshot="sp0"};
alter table t1 drop column b;
create snapshot sp1 for table test t1;

-- @regex("schema compatibility check: base column 'b' is not present in target schema", true)
data branch diff t1{snapshot="sp1"} against t0{snapshot="sp0"};

drop snapshot sp1;
drop snapshot sp0;
drop table t0;
drop table t1;

-- =====================================================
-- Case 12: common-column UPDATE before ALTER is preserved
-- =====================================================
create table t0(a int primary key, b int);
insert into t0 values(1,1),(2,2);
data branch create table t1 from t0;
update t1 set b=11 where a=1;
alter table t1 add column c int default 0;
data branch diff t1 against t0;
drop table t0;
drop table t1;

-- =====================================================
-- Case 13: DELETE before ADD FIRST is preserved
-- =====================================================
create table t0(a int primary key, b int);
insert into t0 values(1,1),(2,2);
data branch create table t1 from t0;
delete from t1 where a=2;
alter table t1 add column c int default 0 first;
data branch diff t1 against t0;
drop table t0;
drop table t1;

-- =====================================================
-- Case 14: base-side UPDATE after target ALTER keeps commit_ts
-- =====================================================
create table t0(a int primary key, b int);
insert into t0 values(1,1),(2,2);
data branch create table t1 from t0;
alter table t1 add column c int default 0;
update t0 set b=77 where a=1;
data branch diff t1 against t0;
drop table t0;
drop table t1;

-- =====================================================
-- Case 15: fake-PK hash is incompatible across added columns
-- =====================================================
create table t0(a int, b int);
insert into t0 values(1,1);
data branch create table t1 from t0;
alter table t1 add column c int default 0;
-- @regex("schema compatibility check: target-only columns require an explicit primary key", true)
data branch diff t1 against t0;
drop table t0;
drop table t1;

-- =====================================================
-- Case 16: same PK updated before and after ALTER is emitted once
-- =====================================================
create table t0(a int primary key, b int);
insert into t0 values(1,1),(2,2);
data branch create table t1 from t0;
update t1 set b=11 where a=1;
alter table t1 add column c int default 7;
update t1 set b=12 where a=1;
data branch diff t1 against t0;
drop table t0;
drop table t1;

-- =====================================================
-- Case 17: ALTER on live data-branch lineage rejects explicit transactions
-- =====================================================
create table t0(a int primary key, b int);
insert into t0 values(1,1),(2,2);
data branch create table t1 from t0;
begin;
update t1 set b=11 where a=1;
-- @regex("ALTER on a data-branch lineage is not supported inside an explicit transaction", true)
alter table t1 add column c int default 0;
rollback;
data branch diff t1 against t0;
drop table t0;
drop table t1;

-- =====================================================
-- Case 18: snapshot diff spans an ALTER physical generation
-- =====================================================
create table t0(a int primary key, b int);
insert into t0 values(1,1),(2,2);
data branch create table t1 from t0;
drop snapshot if exists s0;
drop snapshot if exists s1;
create snapshot s0 for account sys;
update t1 set b=11 where a=1;
alter table t1 add column c int default 7;
update t1 set b=22 where a=2;
create snapshot s1 for account sys;
data branch diff t1{snapshot="s1"} against t1{snapshot="s0"};
drop snapshot s1;
drop snapshot s0;
drop table t0;
drop table t1;

-- =====================================================
-- Case 19: branch-from-snapshot records the snapshot physical parent
-- =====================================================
create table t0(a int primary key, b int);
insert into t0 values(1,1),(2,2);
data branch create table t1 from t0;
drop snapshot if exists s0;
create snapshot s0 for account sys;
update t1 set b=11 where a=1;
alter table t1 add column c int default 7;
data branch create table t2 from t1{snapshot="s0"};
update t2 set b=22 where a=2;
-- t2 must fork from t1's old physical ID captured by s0, not current t1.
data branch diff t1 against t2;
drop snapshot s0;
drop table t2;
drop table t1;
drop table t0;

-- =====================================================
-- Case 20: root ALTER is retained for a future snapshot branch
-- =====================================================
-- No branch metadata exists when s0 is created.  ALTER must still retain
-- old->new lineage because the user snapshot can become a branch parent.
create table t0(a int primary key, b int);
insert into t0 values(1,1),(2,2);
drop snapshot if exists s0;
create snapshot s0 for account sys;
update t0 set b=11 where a=1;
alter table t0 add column c int default 7;
data branch create table t1 from t0{snapshot="s0"};
update t1 set b=22 where a=2;
data branch diff t0 against t1;
drop snapshot s0;
drop table t1;
drop table t0;

-- =====================================================
-- Case 21: historical fake-PK generations fail closed
-- =====================================================
-- Endpoint schemas match, but old fake PKs hash (a,b) while endpoint fake
-- PKs hash (a,b,c); historical rows cannot be matched safely.
create table t0(a int, b int);
insert into t0 values(1,1);
data branch create table t1 from t0;
update t1 set b=11 where a=1;
alter table t0 add column c int default 0;
alter table t1 add column c int default 0;
-- @regex("historical data branch fake primary key schema differs from the endpoint schema", true)
data branch diff t1 against t0;
drop table t1;
drop table t0;

-- =====================================================
-- Case 22: target-only historical type change with no writes is compatible
-- =====================================================
create table t0(a int primary key, b int);
insert into t0 values(1,1),(2,2);
data branch create table t1 from t0;
alter table t1 add column c int default 7;
alter table t1 modify column c varchar(20);
data branch diff t1 against t0;
drop table t1;
drop table t0;

-- =====================================================
-- Case 23: target-only historical values survive endpoint hydration
-- =====================================================
create table t0(a int primary key, b int);
insert into t0 values(1,1),(2,2);
data branch create table t1 from t0;
alter table t1 add column c int default 7;
update t1 set b=11, c=70 where a=1;
alter table t1 modify column c varchar(20);
data branch diff t1 against t0;
drop table t1;
drop table t0;

-- =====================================================
-- Case 24: DROP/ADD with the same name is a new column identity
-- =====================================================
create table t0(a int primary key, b int);
insert into t0 values(1,1);
data branch create table t1 from t0;
update t1 set b=5 where a=1;
alter table t1 drop column b;
alter table t1 add column b int default 0;
-- @regex("schema compatibility check: column 'b' has different identity", true)
data branch diff t1 against t0;
drop table t1;
drop table t0;

-- =====================================================
-- Case 25: incompatible target-only LCA columns are not probed
-- =====================================================
create table t0(a int primary key, b int, c int);
insert into t0 values(1,1,1);
data branch create table t1 from t0;
data branch create table t2 from t0;
alter table t1 modify column c varchar(20);
alter table t2 drop column c;
update t1 set b=11, c='x' where a=1;
data branch diff t1 against t2;
drop table t2;
drop table t1;
drop table t0;

drop database test;
