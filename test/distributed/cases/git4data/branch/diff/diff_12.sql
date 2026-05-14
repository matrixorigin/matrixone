-- Linear branch chain t0 -> t1 -> t2 diff coverage.
-- Verifies `data branch diff` across non-adjacent levels of a chain
-- (e.g. t2 against t0, t0 against t2). The chain-shape exercises a
-- different LCA path than a flat fork (t0 -> {t1, t2}) because the
-- DAG walk must traverse through the intermediate t1 edge to find
-- the correct branch TS. We also interleave insert/update/delete on
-- t0, t1, t2 at multiple time points, mix snapshot-scoped diffs, and
-- include a longer chain (t0 -> t1 -> t2 -> t3) and a no-PK variant
-- sanity check.

drop database if exists test_chain_diff;
create database test_chain_diff;
use test_chain_diff;

-- ============================================================
-- Case 1: pure chain, writes only on leaf t2.
-- Verifies that with no writes on t0/t1, diff t2 vs t0 equals diff
-- t2 vs t1 on the leaf-origin rows, and both base sides are clean.
-- ============================================================
create table t0(a int primary key, b int);
insert into t0 values (1, 1), (2, 2), (3, 3);

data branch create table t1 from t0;
data branch create table t2 from t1;

insert into t2 values (4, 4), (5, 5);
update t2 set b = b + 100 where a = 1;
delete from t2 where a = 2;

-- chain: t0 -> t1 -> t2; LCA for each pair:
--   t2 vs t0 : t0 itself is the LCA (lcaRight)
--   t2 vs t1 : t1 itself is the LCA (lcaRight)
--   t1 vs t0 : t0 itself is the LCA (lcaRight)   -- no writes on t1, should be empty

data branch diff t2 against t0 output summary;
data branch diff t2 against t0 output count;
data branch diff t2 against t0;

data branch diff t2 against t1 output summary;
data branch diff t2 against t1 output count;
data branch diff t2 against t1;

data branch diff t1 against t0 output summary;

-- reverse direction sanity
data branch diff t0 against t2 output summary;
data branch diff t0 against t2 output count;

drop table t2;
drop table t1;
drop table t0;

-- ============================================================
-- Case 2: writes on the middle branch t1 both before and after t2
-- has been taken. Chain: t0 -> (t1 grows) -> t2 -> (t1 grows more).
-- Key check: the LCA of (t2, t0) is t0 with branch TS = CloneTS(t1).
-- Only changes AFTER that TS on each partition are diffed. Pre-t2
-- t1 writes live in t1's partition; post-t2 t1 writes are attributed
-- to t1's side when diffed against t0.
-- ============================================================
create table t0(a int primary key, b int);
insert into t0 values (1, 10), (2, 20), (3, 30);

data branch create table t1 from t0;
insert into t1 values (4, 40);
data branch create table t2 from t1;

-- now diverge t1 and t2 separately
update t1 set b = b + 1 where a = 2;
insert into t1 values (5, 50);
delete from t1 where a = 3;

update t2 set b = b + 100 where a = 1;
insert into t2 values (7, 70);

-- t2 vs t0: LCA=t0, branch TS = CloneTS(t1). t2 side reports its
-- own writes replayed from CloneTS(t1)+1; t1's writes do not leak
-- onto t2's side.
data branch diff t2 against t0 output summary;
data branch diff t2 against t0 output count;
data branch diff t2 against t0;

-- t2 vs t1: LCA=t1 itself (lcaRight in the immediate parent sense).
-- Both sides have post-t2-clone writes.
data branch diff t2 against t1 output summary;
data branch diff t2 against t1;

-- t1 vs t0: LCA=t0, branch TS = CloneTS(t1). All of t1's writes
-- surface on t1 side.
data branch diff t1 against t0 output summary;
data branch diff t1 against t0 output count;

drop table t2;
drop table t1;
drop table t0;

-- ============================================================
-- Case 3: writes on the ROOT t0 after t1 and t2 are branched.
-- Verifies that mutations on t0 after cloning do NOT leak into
-- the branch side of the diff.
-- ============================================================
create table t0(a int primary key, b int);
insert into t0 values (1, 1), (2, 2), (3, 3);

data branch create table t1 from t0;
data branch create table t2 from t1;

-- Write on root AFTER t1 and t2 exist.
update t0 set b = b + 1000 where a = 1;
insert into t0 values (10, 10);
delete from t0 where a = 3;

-- And write on t2 as well.
update t2 set b = b - 1 where a = 2;
insert into t2 values (20, 20);

-- t2 vs t0 : t0 is the LCA. Both sides diverge. Expect mirror-style
-- output (t0 mutations on t0 side, t2 mutations on t2 side).
data branch diff t2 against t0 output summary;
data branch diff t2 against t0 output count;
data branch diff t2 against t0;

-- t1 vs t0 : t1 has no writes, so only t0 side is non-empty.
data branch diff t1 against t0 output summary;
data branch diff t0 against t1 output summary;

-- t2 vs t1 : t1 is the LCA. Only t2 side should show changes.
data branch diff t2 against t1 output summary;
data branch diff t2 against t1;

drop table t2;
drop table t1;
drop table t0;

-- ============================================================
-- Case 4: fully interleaved DML at multiple time points on t0/t1/t2.
-- This is the most complex scenario: writes on every level mixed
-- with snapshots, executed at several times along the chain.
-- Snapshots capture the state at each vantage point so that later
-- diffs can exercise snapshot-scoped + cross-level combinations.
-- ============================================================
create table t0(a int primary key, b int, tag varchar(16));
insert into t0 values (1, 1, 't0'), (2, 2, 't0'), (3, 3, 't0');
drop snapshot if exists c4_t0_sp0;
create snapshot c4_t0_sp0 for table test_chain_diff t0;

-- Branch t1 off t0, write on t1.
data branch create table t1 from t0{snapshot="c4_t0_sp0"};
insert into t1 values (4, 4, 't1');
update t1 set tag = 't1-u' where a = 1;
drop snapshot if exists c4_t1_sp0;
create snapshot c4_t1_sp0 for table test_chain_diff t1;

-- Branch t2 off t1 (at a specific snapshot).
data branch create table t2 from t1{snapshot="c4_t1_sp0"};
insert into t2 values (5, 5, 't2');
update t2 set tag = 't2-u' where a = 2;
delete from t2 where a = 3;
drop snapshot if exists c4_t2_sp0;
create snapshot c4_t2_sp0 for table test_chain_diff t2;

-- More writes on t1 AFTER t2 was branched.
update t1 set tag = 't1-u2' where a = 4;
insert into t1 values (6, 6, 't1');
drop snapshot if exists c4_t1_sp1;
create snapshot c4_t1_sp1 for table test_chain_diff t1;

-- More writes on t0 AFTER both t1 and t2 exist.
update t0 set tag = 't0-u' where a = 2;
insert into t0 values (7, 7, 't0');
drop snapshot if exists c4_t0_sp1;
create snapshot c4_t0_sp1 for table test_chain_diff t0;

-- More writes on t2.
update t2 set tag = 't2-u2' where a = 4;
insert into t2 values (8, 8, 't2');
drop snapshot if exists c4_t2_sp1;
create snapshot c4_t2_sp1 for table test_chain_diff t2;

-- CURRENT-state diffs across all pairs.
data branch diff t2 against t0 output summary;
data branch diff t2 against t0 output count;

data branch diff t2 against t1 output summary;
data branch diff t2 against t1 output count;

data branch diff t1 against t0 output summary;
data branch diff t1 against t0 output count;

data branch diff t0 against t2 output summary;
data branch diff t1 against t2 output summary;

-- Snapshot-scoped diffs — verify the chain LCA math holds when
-- replaying at a specific time point on each side.
data branch diff t2{snapshot="c4_t2_sp0"} against t0{snapshot="c4_t0_sp0"} output summary;
data branch diff t2{snapshot="c4_t2_sp0"} against t0{snapshot="c4_t0_sp0"} output count;

data branch diff t2{snapshot="c4_t2_sp0"} against t1{snapshot="c4_t1_sp0"} output summary;

data branch diff t2{snapshot="c4_t2_sp1"} against t0{snapshot="c4_t0_sp1"} output summary;

-- Mixed-snapshot: newer t2 snapshot against older t0 snapshot, i.e.
-- diff a future state of the grandchild against an older state of
-- the grandparent. This is the nastiest LCA scenario because the
-- snapshots sit on opposite sides of the intermediate branch points.
data branch diff t2{snapshot="c4_t2_sp1"} against t0{snapshot="c4_t0_sp0"} output summary;
data branch diff t2{snapshot="c4_t2_sp1"} against t0{snapshot="c4_t0_sp0"} output count;

drop snapshot c4_t0_sp0;
drop snapshot c4_t0_sp1;
drop snapshot c4_t1_sp0;
drop snapshot c4_t1_sp1;
drop snapshot c4_t2_sp0;
drop snapshot c4_t2_sp1;
drop table t2;
drop table t1;
drop table t0;

-- ============================================================
-- Case 5: 4-level chain t0 -> t1 -> t2 -> t3 (depth sanity).
-- Verifies the DAG walk remains stable when diff skips 2 edges.
-- ============================================================
create table t0(a int primary key, b int);
insert into t0 values (1, 1), (2, 2);

data branch create table t1 from t0;
insert into t1 values (3, 3);

data branch create table t2 from t1;
insert into t2 values (4, 4);
update t2 set b = b + 100 where a = 1;

data branch create table t3 from t2;
insert into t3 values (5, 5);
delete from t3 where a = 2;
update t3 set b = b + 1000 where a = 4;

-- Diff across every pair of levels.
data branch diff t3 against t0 output summary;
data branch diff t3 against t0 output count;
data branch diff t3 against t0;

data branch diff t3 against t1 output summary;
data branch diff t3 against t1 output count;

data branch diff t3 against t2 output summary;
data branch diff t3 against t2;

data branch diff t2 against t0 output summary;
data branch diff t2 against t1 output summary;
data branch diff t1 against t0 output summary;

-- Reverse direction for the 3-edge skip.
data branch diff t0 against t3 output summary;
data branch diff t0 against t3 output count;

drop table t3;
drop table t2;
drop table t1;
drop table t0;

-- ============================================================
-- Case 6: no-PK (fake PK) variant of the linear chain.
-- Verifies that the fake-PK hash path also handles the chain.
-- ============================================================
create table t0(a int, b int);
insert into t0 select *, * from generate_series(1, 50) g;

data branch create table t1 from t0;
insert into t1 values (51, 51), (52, 52);

data branch create table t2 from t1;
update t2 set b = b + 1 where a in (1, 10, 20, 30, 40, 51);
insert into t2 values (53, 53);
delete from t2 where a in (2, 3);

data branch diff t2 against t0 output summary;
data branch diff t2 against t0 output count;

data branch diff t2 against t1 output summary;
data branch diff t2 against t1 output count;

data branch diff t1 against t0 output summary;

drop table t2;
drop table t1;
drop table t0;

-- ============================================================
-- Case 7: chain diff after intermediate flush/checkpoint/GC.
-- Scaled down from diff_9 but on the chain shape. Verifies that
-- even after flush + checkpoint, diff across non-adjacent chain
-- levels (t2 vs t0) is still correct.
-- ============================================================
create table t0(a int primary key, b int);
insert into t0 select *, * from generate_series(1, 2000) g;

data branch create table t1 from t0;
update t1 set b = b + 1 where a mod 337 = 0;
insert into t1 values (2001, 2001);

data branch create table t2 from t1;
update t2 set b = b + 2 where a mod 191 = 0;
delete from t2 where a in (7, 77, 777);
insert into t2 values (2002, 2002);

-- @ignore:0
select mo_ctl('dn', 'flush', 'test_chain_diff.t0');
-- @ignore:0
select mo_ctl('dn', 'flush', 'test_chain_diff.t1');
-- @ignore:0
select mo_ctl('dn', 'flush', 'test_chain_diff.t2');
-- @ignore:0
select mo_ctl('dn', 'globalcheckpoint', '');

data branch diff t2 against t0 output summary;
data branch diff t2 against t0 output count;

data branch diff t2 against t1 output summary;

data branch diff t1 against t0 output summary;

drop table t2;
drop table t1;
drop table t0;

-- ============================================================
-- Case 8: composite PK linear chain, diff with snapshot on both
-- sides at different chain depths.
-- ============================================================
create table t0(region varchar(4), k int, v int, primary key (region, k));
insert into t0 values ('eu', 1, 10), ('eu', 2, 20), ('us', 1, 100);
drop snapshot if exists c8_t0_sp0;
create snapshot c8_t0_sp0 for table test_chain_diff t0;

data branch create table t1 from t0;
update t1 set v = v + 1 where region = 'eu' and k = 1;
insert into t1 values ('us', 2, 200);
drop snapshot if exists c8_t1_sp0;
create snapshot c8_t1_sp0 for table test_chain_diff t1;

data branch create table t2 from t1;
delete from t2 where region = 'eu' and k = 2;
update t2 set v = v + 100 where region = 'us' and k = 1;
insert into t2 values ('ap', 1, 999);
drop snapshot if exists c8_t2_sp0;
create snapshot c8_t2_sp0 for table test_chain_diff t2;

-- Current state diffs.
data branch diff t2 against t0 output summary;
data branch diff t2 against t0;

-- Snapshot-scoped: snapshot of t2 against the ORIGINAL snapshot of
-- t0 (before the clone existed) — verifies the LCA can be resolved
-- purely via snapshot references without touching live tables.
data branch diff t2{snapshot="c8_t2_sp0"} against t0{snapshot="c8_t0_sp0"} output summary;
data branch diff t2{snapshot="c8_t2_sp0"} against t0{snapshot="c8_t0_sp0"} output count;

data branch diff t2{snapshot="c8_t2_sp0"} against t1{snapshot="c8_t1_sp0"} output summary;

drop snapshot c8_t0_sp0;
drop snapshot c8_t1_sp0;
drop snapshot c8_t2_sp0;
drop table t2;
drop table t1;
drop table t0;

-- ============================================================
-- Case 9: canonical "t1 -> t2 -> t3" with IUD on every branch,
-- then diff across non-adjacent levels. This is the textbook
-- complex scenario: full insert/update/delete on t2, then branch
-- t3 off t2, full insert/update/delete on t3, then diff t3 vs t1.
-- Also cross-level: diff t3 vs t2, t2 vs t1, and the reverse
-- pairings, plus output count / summary / row-level variants.
-- ============================================================
create table t1(a int primary key, b int);
insert into t1 values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5);

data branch create table t2 from t1;
update t2 set b = b + 10 where a = 1;
insert into t2 values (6, 6), (7, 7);
delete from t2 where a = 2;

data branch create table t3 from t2;
update t3 set b = b + 100 where a in (3, 6);
insert into t3 values (8, 8), (9, 9);
delete from t3 where a in (4, 7);

-- Adjacent-level diffs (LCA is the immediate parent).
data branch diff t2 against t1 output summary;
data branch diff t2 against t1 output count;
data branch diff t2 against t1;

data branch diff t3 against t2 output summary;
data branch diff t3 against t2 output count;
data branch diff t3 against t2;

-- Cross-level diff (skips 1 edge): grandchild against grandparent.
-- LCA resolves to t1 itself (lcaRight), branch TS = CloneTS(t2).
-- Only t3's mutations relative to t2's state at clone time surface.
data branch diff t3 against t1 output summary;
data branch diff t3 against t1 output count;
data branch diff t3 against t1;

-- Reverse direction (same LCA, lcaLeft).
data branch diff t1 against t3 output summary;
data branch diff t1 against t3 output count;
data branch diff t1 against t3;

-- Ancestor-chain diff sanity.
data branch diff t1 against t2 output summary;

-- Writes on t1 AFTER t2 and t3 were branched. Verifies that
-- mutations on the root do NOT leak into t3's side.
update t1 set b = b + 1000 where a = 5;
insert into t1 values (10, 10);
delete from t1 where a = 1;

data branch diff t3 against t1 output summary;
data branch diff t3 against t1 output count;
data branch diff t3 against t1;

data branch diff t2 against t1 output summary;
data branch diff t2 against t1 output count;

-- Writes on t2 AFTER t3 was branched. Verifies t3's view is
-- unaffected by later t2 mutations, and that t2 vs t1 absorbs
-- both pre-t3 and post-t3 t2 mutations.
insert into t2 values (20, 20);
update t2 set b = b + 5 where a = 6;
delete from t2 where a = 7;

data branch diff t3 against t2 output summary;
data branch diff t3 against t1 output summary;
data branch diff t2 against t1 output summary;
data branch diff t2 against t1;

drop table t3;
drop table t2;
drop table t1;

-- ============================================================
-- Case 10: stress — t1 -> t2 -> t3 -> t4 with IUD on every level
-- and snapshots captured at every level, then diff across every
-- pair of levels (6 pairs, both directions). Covers the DAG walk
-- at depth 3 (cross 3 edges) and all intermediate combinations.
-- Uses composite PK to exercise the compositeKind hash path.
-- ============================================================
create table t1(region varchar(4), k int, v int, primary key (region, k));
insert into t1 values ('eu', 1, 10), ('eu', 2, 20), ('us', 1, 100), ('us', 2, 200);
drop snapshot if exists c10_t1_sp0;
create snapshot c10_t1_sp0 for table test_chain_diff t1;

data branch create table t2 from t1{snapshot="c10_t1_sp0"};
update t2 set v = v + 1 where region = 'eu' and k = 1;
insert into t2 values ('ap', 1, 999);
delete from t2 where region = 'us' and k = 2;
drop snapshot if exists c10_t2_sp0;
create snapshot c10_t2_sp0 for table test_chain_diff t2;

data branch create table t3 from t2{snapshot="c10_t2_sp0"};
update t3 set v = v + 2 where region = 'eu' and k = 2;
insert into t3 values ('sa', 1, 777);
delete from t3 where region = 'us' and k = 1;
drop snapshot if exists c10_t3_sp0;
create snapshot c10_t3_sp0 for table test_chain_diff t3;

data branch create table t4 from t3{snapshot="c10_t3_sp0"};
update t4 set v = v + 3 where region = 'ap' and k = 1;
insert into t4 values ('na', 1, 555);
delete from t4 where region = 'eu' and k = 1;
drop snapshot if exists c10_t4_sp0;
create snapshot c10_t4_sp0 for table test_chain_diff t4;

-- Adjacent-level diffs.
data branch diff t2 against t1 output summary;
data branch diff t3 against t2 output summary;
data branch diff t4 against t3 output summary;

-- Skip-1-edge diffs.
data branch diff t3 against t1 output summary;
data branch diff t3 against t1 output count;
data branch diff t3 against t1;

data branch diff t4 against t2 output summary;
data branch diff t4 against t2 output count;
data branch diff t4 against t2;

-- Skip-2-edge diff (cross 3 clone edges).
data branch diff t4 against t1 output summary;
data branch diff t4 against t1 output count;
data branch diff t4 against t1;

-- Reverse skip-2-edge.
data branch diff t1 against t4 output summary;
data branch diff t1 against t4 output count;

-- Snapshot-scoped variant: each pair replayed at its own snapshot
-- taken right after the level's IUD was applied.
data branch diff t4{snapshot="c10_t4_sp0"} against t1{snapshot="c10_t1_sp0"} output summary;
data branch diff t4{snapshot="c10_t4_sp0"} against t1{snapshot="c10_t1_sp0"} output count;

data branch diff t4{snapshot="c10_t4_sp0"} against t2{snapshot="c10_t2_sp0"} output summary;
data branch diff t3{snapshot="c10_t3_sp0"} against t1{snapshot="c10_t1_sp0"} output summary;

-- Write further on every level AFTER all branches exist, then
-- re-diff to confirm deeper levels remain unaffected by root
-- mutations and vice versa.
insert into t1 values ('af', 1, 888);
update t1 set v = v + 10000 where region = 'us' and k = 1;

insert into t2 values ('af', 2, 889);
delete from t2 where region = 'ap' and k = 1;

update t3 set v = v + 20000 where region = 'eu' and k = 2;

data branch diff t4 against t1 output summary;
data branch diff t4 against t2 output summary;
data branch diff t4 against t3 output summary;

-- Historical snapshots must NOT see any of the post-branch writes.
data branch diff t4{snapshot="c10_t4_sp0"} against t1{snapshot="c10_t1_sp0"} output summary;

drop snapshot c10_t1_sp0;
drop snapshot c10_t2_sp0;
drop snapshot c10_t3_sp0;
drop snapshot c10_t4_sp0;
drop table t4;
drop table t3;
drop table t2;
drop table t1;

-- ============================================================
-- Case 11: t1 -> t2 followed by IUD on t1 (post-branch) — the
-- focus scenario. After t2 is cloned, every kind of write is
-- applied to t1, and then diff is taken in both directions.
-- The LCA is t1 itself (lcaRight / lcaLeft). The t1 side of the
-- diff must surface ALL post-branch t1 writes; the t2 side
-- surfaces only t2's own writes. Also verifies:
--   * an UPDATE on t1 of a row that t2 has also updated with a
--     different value (both sides report UPDATE);
--   * a DELETE on t1 of a row that t2 has kept unchanged
--     (t1 side reports DELETE, t2 side reports the baseline row
--     as INSERT — this is the divergent-base scenario);
--   * an INSERT on t1 of a PK that t2 has also inserted with a
--     different value (both sides report INSERT on that PK).
-- ============================================================
create table t1(a int primary key, b int);
insert into t1 values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5);

data branch create table t2 from t1;
-- IUD on t2 first.
update t2 set b = b + 10 where a = 1;
insert into t2 values (6, 6), (7, 7);
delete from t2 where a = 2;

data branch diff t2 against t1 output summary;
data branch diff t2 against t1;

-- NOW do IUD on t1 (post-branch).
--   * a=1: t1 sets b=-1; t2 had set b=11. Divergent UPDATE.
--   * a=2: t2 deleted it; t1 updates it. Conflict on the
--     resurrected-vs-deleted row.
--   * a=3: t1 deletes it; t2 kept it unchanged (baseline value).
--   * a=4: t1 updates it; t2 never touched it.
--   * a=5: t1 deletes it; t2 never touched it.
--   * a=6: t2 inserted (6,6); t1 inserts (6,600). Double-insert
--     on same PK with different values.
--   * a=100: t1 inserts; t2 doesn't know about it.
--   * a=7: t2 inserted (7,7); t1 inserts (7,7). Double-insert
--     on same PK with same value (should reconcile).
update t1 set b = -1 where a = 1;
update t1 set b = 222 where a = 2;
delete from t1 where a = 3;
update t1 set b = 444 where a = 4;
delete from t1 where a = 5;
insert into t1 values (6, 600);
insert into t1 values (100, 100);
insert into t1 values (7, 7);

-- Diff after all the t1 post-branch IUD.
data branch diff t2 against t1 output summary;
data branch diff t2 against t1 output count;
data branch diff t2 against t1;

-- Reverse.
data branch diff t1 against t2 output summary;
data branch diff t1 against t2 output count;
data branch diff t1 against t2;

-- Do a SECOND round of IUD on t1 (further post-branch), confirm
-- the diff reflects the latest state consistently.
update t1 set b = -2 where a = 1;
insert into t1 values (8, 800);
delete from t1 where a = 100;
update t1 set b = 888 where a = 6;

data branch diff t2 against t1 output summary;
data branch diff t2 against t1;

drop table t2;
drop table t1;

-- ============================================================
-- Case 12: alternating rounds of IUD on t1 and t2 after branch.
-- Pattern:
--   T0. t1 has initial rows.
--   T1. data branch create t2 from t1.
--   T2. IUD on t2 round A.
--   T3. IUD on t1 round A.
--   T4. IUD on t2 round B (possibly on rows t1 just touched).
--   T5. IUD on t1 round B.
-- Then diff t2 vs t1. Verifies that the diff's t1-side tracks
-- ALL of t1's post-branch writes (rounds A + B) and t2-side tracks
-- ALL of t2's writes — and that the two streams are reconciled
-- even when they keep clobbering each other's rows.
-- ============================================================
create table t1(a int primary key, b int, note varchar(16));
insert into t1 values (1, 1, 'init'), (2, 2, 'init'), (3, 3, 'init'),
                      (4, 4, 'init'), (5, 5, 'init');

data branch create table t2 from t1;

-- T2: t2 round A.
update t2 set b = 10, note = 't2-A' where a = 1;
delete from t2 where a = 2;
insert into t2 values (10, 10, 't2-A');

-- T3: t1 round A (touches rows t2 already touched, and new rows).
update t1 set b = 100, note = 't1-A' where a = 1;
update t1 set b = 200, note = 't1-A' where a = 2;
delete from t1 where a = 3;
insert into t1 values (20, 20, 't1-A');

-- Mid-point diff after round A on both sides.
data branch diff t2 against t1 output summary;
data branch diff t2 against t1;

-- T4: t2 round B — further diverge.
update t2 set b = 11, note = 't2-B' where a = 1;
update t2 set b = 44, note = 't2-B' where a = 4;
insert into t2 values (11, 11, 't2-B');
delete from t2 where a = 10;

-- T5: t1 round B.
update t1 set b = 111, note = 't1-B' where a = 1;
insert into t1 values (21, 21, 't1-B');
delete from t1 where a = 20;
update t1 set b = 5555, note = 't1-B' where a = 5;

-- Final diff — both sides have two rounds of IUD.
data branch diff t2 against t1 output summary;
data branch diff t2 against t1 output count;
data branch diff t2 against t1;

data branch diff t1 against t2 output summary;
data branch diff t1 against t2 output count;

drop table t2;
drop table t1;

drop database test_chain_diff;
