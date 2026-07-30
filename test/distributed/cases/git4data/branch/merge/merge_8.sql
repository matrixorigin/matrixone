-- Linear branch chain t0 -> t1 -> t2 merge coverage.
-- Verifies `data branch merge` in a chain shape (grandparent ->
-- parent -> child). The existing merge/*.sql only covers flat
-- forks (t0 -> {t1, t2}) or single-edge parent/child merges.
-- A chain merge is distinctive because:
--   * merging the GRANDCHILD directly into the GRANDPARENT
--     (t2 into t0) forces the LCA probe to walk through t1's
--     clone edge to identify t0 as the LCA;
--   * a cascade merge (t2 -> t1, then t1 -> t0) must leave the
--     intermediate branch's metadata consistent so later diffs
--     still resolve;
--   * conflict detection (skip / accept) has to work the same way
--     across multiple chain depths.

drop database if exists test_chain_merge;
create database test_chain_merge;
use test_chain_merge;

-- ============================================================
-- Case 1: cascade merge up the chain, single primary key.
-- Write-on-leaf-only, then merge t2 -> t1, then merge t1 -> t0.
-- After each merge, verify the parent contains the merged rows
-- and that subsequent diffs are empty.
-- ============================================================
create table t0(a int primary key, b int);
insert into t0 values (1, 1), (2, 2), (3, 3);

data branch create table t1 from t0;
data branch create table t2 from t1;

insert into t2 values (4, 4), (5, 5);
update t2 set b = b + 100 where a = 1;
delete from t2 where a = 2;

data branch diff t2 against t1 output summary;
data branch merge t2 into t1;
select * from t1 order by a;
data branch diff t2 against t1 output summary;

-- now t1 carries t2's changes; cascade up to t0.
data branch diff t1 against t0 output summary;
data branch merge t1 into t0;
select * from t0 order by a;
data branch diff t1 against t0 output summary;

-- After full cascade, diff t2 against t0 must also be empty.
data branch diff t2 against t0 output summary;

drop table t2;
drop table t1;
drop table t0;

-- ============================================================
-- Case 2: direct merge grandchild -> grandparent (t2 into t0)
-- without going through t1. Writes only on leaf.
-- Verifies the LCA probe identifies t0 as LCA across 2 clone
-- edges, and the merge correctly applies t2's mutations to t0.
-- t1 remains untouched and its diff to t0 should still reflect
-- only what was on t1 beforehand (nothing in this case).
-- ============================================================
create table t0(a int primary key, b int);
insert into t0 values (1, 1), (2, 2), (3, 3);

data branch create table t1 from t0;
data branch create table t2 from t1;

insert into t2 values (10, 10), (11, 11);
update t2 set b = b + 1000 where a = 1;

data branch diff t2 against t0 output summary;
data branch merge t2 into t0;

select * from t0 order by a;

-- After merging t2 into t0, the grandchild->grandparent diff
-- becomes empty, but t2 vs t1 still shows t2's mutations (t1
-- was NOT touched by this merge).
data branch diff t2 against t0 output summary;
data branch diff t2 against t1 output summary;

-- t1 vs t0: now t0 has the rows that t2 added; t1 doesn't. So
-- t1 vs t0 should surface those as t0-side INSERTs.
data branch diff t1 against t0 output summary;
data branch diff t1 against t0 output count;

drop table t2;
drop table t1;
drop table t0;

-- ============================================================
-- Case 3: DML on every level of the chain, then a direct
-- grandchild->grandparent merge. Verifies conflict handling
-- when t0 itself has been mutated after branching.
-- ============================================================
create table t0(a int primary key, b int);
insert into t0 values (1, 1), (2, 2), (3, 3);

data branch create table t1 from t0;
insert into t1 values (4, 4);

data branch create table t2 from t1;
insert into t2 values (5, 5);
update t2 set b = b + 10 where a = 1;

-- Now mutate t0 at a row that t2 has also modified -> CONFLICT.
update t0 set b = b + 100 where a = 1;
insert into t0 values (50, 50);

data branch diff t2 against t0 output summary;

-- Default merge should fail on conflict (row a=1 has differing
-- values on both sides). Use conflict=skip first, then accept.
data branch merge t2 into t0 when conflict skip;
select * from t0 order by a;

-- Accept overwrites t0's value with t2's.
data branch merge t2 into t0 when conflict accept;
select * from t0 order by a;

-- After accept, t2 vs t0 should be empty (t2 side fully applied).
data branch diff t2 against t0 output summary;

-- t1 vs t0: t1 has a=4 insert only; t0 now has everything from
-- t2 plus a=50 that came from t0's own insert.
data branch diff t1 against t0 output summary;

drop table t2;
drop table t1;
drop table t0;

-- ============================================================
-- Case 4: DML on middle branch AFTER grandchild was taken.
-- t2 was branched from t1, THEN t1 got more writes. Merging
-- t2 into t0 must attribute middle-branch writes correctly:
-- t1's post-t2 writes should NOT appear in t0 (we're merging
-- t2, not t1), but since t2 was branched BEFORE those writes
-- existed, they're invisible to t2 anyway.
-- ============================================================
create table t0(a int primary key, b varchar(8));
insert into t0 values (1, 't0'), (2, 't0'), (3, 't0');

data branch create table t1 from t0;
insert into t1 values (4, 't1');
update t1 set b = 't1u' where a = 2;

data branch create table t2 from t1;
insert into t2 values (5, 't2');
update t2 set b = 't2u' where a = 3;

-- AFTER t2 is branched: continue mutating t1.
update t1 set b = 't1late' where a = 1;
insert into t1 values (6, 't1late');

-- Now merge t2 into t0. Expected:
--   * a=1 unchanged in t2 (because 't1late' happened AFTER t2
--     was branched) -> t2's view of a=1 is still 't0'. No diff
--     on a=1 side.
--   * a=2 value 't1u' was present at t2's branch time.
--   * a=3 was updated by t2 to 't2u'.
--   * a=4, a=5 are inserts that reached t2.
--   * a=6 is a t1-only insert post-t2-branch; it should NOT be
--     merged into t0.
data branch diff t2 against t0 output summary;
data branch merge t2 into t0;
select * from t0 order by a;

-- Sanity: t1 is untouched by the above merge.
data branch diff t1 against t0 output summary;
data branch diff t1 against t0 output count;

drop table t2;
drop table t1;
drop table t0;

-- ============================================================
-- Case 5: Composite PK chain, cascade merge with interleaved DML.
-- ============================================================
create table t0(region varchar(4), k int, v int, primary key (region, k));
insert into t0 values ('eu', 1, 10), ('eu', 2, 20), ('us', 1, 100);

data branch create table t1 from t0;
insert into t1 values ('us', 2, 200);

data branch create table t2 from t1;
update t2 set v = v + 1 where region = 'eu' and k = 1;
delete from t2 where region = 'us' and k = 2;
insert into t2 values ('ap', 1, 999);

-- Merge t2 up into t1.
data branch diff t2 against t1 output summary;
data branch merge t2 into t1;
select * from t1 order by region, k;

-- Then cascade t1 into t0.
data branch diff t1 against t0 output summary;
data branch merge t1 into t0;
select * from t0 order by region, k;

-- Full chain now consistent.
data branch diff t2 against t0 output summary;
data branch diff t2 against t1 output summary;

drop table t2;
drop table t1;
drop table t0;

-- ============================================================
-- Case 6: Snapshot-scoped cascade merge.
-- Take snapshots at various time points along the chain, then
-- verify diff via snapshots still correctly reports state pre-
-- and post-merge. (Merging itself is unscoped; snapshots are
-- used only to verify the time-travel view.)
-- ============================================================
create table t0(a int primary key, b int);
insert into t0 values (1, 1), (2, 2);
drop snapshot if exists c6_t0_sp0;
create snapshot c6_t0_sp0 for table test_chain_merge t0;

data branch create table t1 from t0{snapshot="c6_t0_sp0"};
insert into t1 values (3, 3);
drop snapshot if exists c6_t1_sp0;
create snapshot c6_t1_sp0 for table test_chain_merge t1;

data branch create table t2 from t1{snapshot="c6_t1_sp0"};
insert into t2 values (4, 4);
update t2 set b = b + 10 where a = 1;
drop snapshot if exists c6_t2_sp0;
create snapshot c6_t2_sp0 for table test_chain_merge t2;

-- Diff t2@sp0 vs t0@sp0 BEFORE any merge.
data branch diff t2{snapshot="c6_t2_sp0"} against t0{snapshot="c6_t0_sp0"} output summary;

-- Now cascade merge.
data branch merge t2 into t1;
data branch merge t1 into t0;

-- Live diff after cascade: all tables equal.
data branch diff t2 against t0 output summary;
data branch diff t1 against t0 output summary;

-- Historical snapshots are immutable — replaying the diff against
-- the old snapshots should still show the pre-merge state.
data branch diff t2{snapshot="c6_t2_sp0"} against t0{snapshot="c6_t0_sp0"} output summary;
data branch diff t2{snapshot="c6_t2_sp0"} against t0{snapshot="c6_t0_sp0"} output count;

drop snapshot c6_t0_sp0;
drop snapshot c6_t1_sp0;
drop snapshot c6_t2_sp0;
drop table t2;
drop table t1;
drop table t0;

-- ============================================================
-- Case 7: Conflict modes along the chain. Writes on t0 AFTER
-- branching collide with writes on t2 on the same PK.
-- Verifies when conflict skip and when conflict accept both
-- behave as expected across 2 clone edges.
-- ============================================================
create table t0(a int primary key, b int, tag varchar(8));
insert into t0 values (1, 1, 'orig'), (2, 2, 'orig');

data branch create table t1 from t0;
data branch create table t2 from t1;

-- Simultaneously diverge both ends.
update t0 set b = 111, tag = 't0-won' where a = 1;
update t2 set b = 222, tag = 't2-won' where a = 1;
insert into t0 values (10, 10, 't0-ins');
insert into t2 values (20, 20, 't2-ins');

data branch diff t2 against t0 output summary;

-- skip keeps t0's own row on conflict (a=1).
data branch merge t2 into t0 when conflict skip;
select * from t0 order by a;

-- Re-run with accept to overwrite.
data branch merge t2 into t0 when conflict accept;
select * from t0 order by a;

-- After accept, t2 vs t0 must be empty.
data branch diff t2 against t0 output summary;

drop table t2;
drop table t1;
drop table t0;

-- ============================================================
-- Case 8: no-PK (fake PK) chain merge sanity.
-- Merge via a fake-PK all the way up the chain. Only the leaf
-- is written to; cascade merge should migrate the rows up.
-- ============================================================
create table t0(a int, b int);
insert into t0 select *, * from generate_series(1, 20) g;

data branch create table t1 from t0;
data branch create table t2 from t1;

insert into t2 values (21, 21), (22, 22);
update t2 set b = b + 1 where a in (5, 10);
delete from t2 where a in (1, 2);

data branch merge t2 into t1;
data branch merge t1 into t0;

select count(*) as total_rows from t0;
select * from t0 where a in (5, 10, 21, 22) order by a;
select count(*) as deleted_rows from t0 where a in (1, 2);

-- No-PK merge converges all diffs.
data branch diff t2 against t0 output summary;
data branch diff t1 against t0 output summary;

drop table t2;
drop table t1;
drop table t0;

-- ============================================================
-- Case 9: Intermediate branch already merged upward, then a
-- later grandchild merge. Order-of-merges regression.
--
-- Timeline:
--   T1. create t0, branch t1, branch t2
--   T2. write on t1
--   T3. merge t1 into t0
--   T4. write on t2
--   T5. merge t2 into t0
-- Verifies the branch metadata stays consistent after step T3,
-- so step T5's LCA probe for (t2 vs t0) still resolves.
-- ============================================================
create table t0(a int primary key, b int);
insert into t0 values (1, 1), (2, 2);

data branch create table t1 from t0;
data branch create table t2 from t1;

-- T2: writes on t1 only.
insert into t1 values (3, 3);
update t1 set b = b + 100 where a = 1;

-- T3: merge t1 into t0.
data branch merge t1 into t0;
select * from t0 order by a;

-- T4: writes on t2 (which was branched from t1 BEFORE t1 was
-- merged; t2 never saw t1's mutations).
insert into t2 values (4, 4);
update t2 set b = b + 1000 where a = 2;

-- T5: merge t2 into t0. LCA probe must still find t0 as LCA.
data branch diff t2 against t0 output summary;
data branch merge t2 into t0 when conflict accept;
select * from t0 order by a;

-- Verify t2 fully merged (except any skipped rows by accept
-- semantics — here accept means t2 wins).
data branch diff t2 against t0 output summary;

drop table t2;
drop table t1;
drop table t0;

drop database test_chain_merge;
