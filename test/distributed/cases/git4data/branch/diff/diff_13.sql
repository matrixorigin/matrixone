-- Giant multi-fork branch tree diff coverage.
--
-- Tree shape (19 nodes, depth 4, 10 leaves):
--
--                         t0  (root, L1)
--                    /    |    \
--                   t1    t2    t3        (L2)
--                 /  \    |    /  \
--               t4   t5  t6  t7   t8      (L3)
--              / \  / \  / \ / \  / \
--            t9 t10 t11 t12 t13 t14 t15 t16 t17 t18   (L4, 10 leaves)
--
-- Per-node leaf-to-parent mapping:
--   t1 -> t0     t2 -> t0     t3 -> t0
--   t4 -> t1     t5 -> t1     t6 -> t2     t7 -> t3     t8 -> t3
--   t9,t10  -> t4
--   t11,t12 -> t5
--   t13,t14 -> t6    (t6 has two leaves even though t2 only forked once)
--   t15,t16 -> t7
--   t17,t18 -> t8
--
-- Test plan:
--   1. build the tree in top-down order;
--   2. after each `data branch create`, immediately do at least one
--      insert, one update, one delete on that node;
--   3. finally do IUD on t0;
--   4. take 10 diffs across various tree cross-sections:
--      a. leaf vs root          (skip 3 edges)
--      b. leaf vs sibling leaf  (share grandparent)
--      c. leaf vs cousin leaf   (share great-grandparent t0)
--      d. intermediate vs root
--      e. leaf vs its own parent (direct)
--      f. leaf vs its own grandparent (skip 1)
--      g. leaf vs non-ancestor intermediate
--      h. two intermediates with different depth
--      i. sibling intermediates
--      j. reverse-direction of one of the above, for symmetry.
--
-- Row encoding scheme so each node has a unique footprint:
--   Every table starts with rows (1..20). Each node Tk with k>=1
--   does:
--      insert (100+k, 100+k)      -- unique PK per node
--      update set b = k*10000 where a = k  -- stamps a predictable row
--      delete from Tk where a = 20 - k     -- stable but different row
-- t0 finally does:
--      insert (200, 200)
--      update set b = 99999 where a = 1
--      delete from t0 where a = 19

drop database if exists test_tree_diff;
create database test_tree_diff;
use test_tree_diff;

-- ============================================================
-- Root and all seed rows.
-- ============================================================
create table t0(a int primary key, b int);
insert into t0 select *, * from generate_series(1, 20) g;

-- ============================================================
-- L2: t1, t2, t3 from t0 (each followed by IUD).
-- ============================================================
data branch create table t1 from t0;
insert into t1 values (101, 101);
update t1 set b = 10000 where a = 1;
delete from t1 where a = 19;

data branch create table t2 from t0;
insert into t2 values (102, 102);
update t2 set b = 20000 where a = 2;
delete from t2 where a = 18;

data branch create table t3 from t0;
insert into t3 values (103, 103);
update t3 set b = 30000 where a = 3;
delete from t3 where a = 17;

-- ============================================================
-- L3: t4,t5 from t1; t6 from t2; t7,t8 from t3.
-- ============================================================
data branch create table t4 from t1;
insert into t4 values (104, 104);
update t4 set b = 40000 where a = 4;
delete from t4 where a = 16;

data branch create table t5 from t1;
insert into t5 values (105, 105);
update t5 set b = 50000 where a = 5;
delete from t5 where a = 15;

data branch create table t6 from t2;
insert into t6 values (106, 106);
update t6 set b = 60000 where a = 6;
delete from t6 where a = 14;

data branch create table t7 from t3;
insert into t7 values (107, 107);
update t7 set b = 70000 where a = 7;
delete from t7 where a = 13;

data branch create table t8 from t3;
insert into t8 values (108, 108);
update t8 set b = 80000 where a = 8;
delete from t8 where a = 12;

-- ============================================================
-- L4 (10 leaves):
--   t9,t10  from t4
--   t11,t12 from t5
--   t13,t14 from t6
--   t15,t16 from t7
--   t17,t18 from t8
-- ============================================================
data branch create table t9 from t4;
insert into t9 values (109, 109);
update t9 set b = 90000 where a = 9;
delete from t9 where a = 11;

data branch create table t10 from t4;
insert into t10 values (110, 110);
update t10 set b = 100000 where a = 10;
delete from t10 where a = 20;

data branch create table t11 from t5;
insert into t11 values (111, 111);
update t11 set b = 110000 where a = 1;
delete from t11 where a = 11;

data branch create table t12 from t5;
insert into t12 values (112, 112);
update t12 set b = 120000 where a = 2;
delete from t12 where a = 12;

data branch create table t13 from t6;
insert into t13 values (113, 113);
update t13 set b = 130000 where a = 3;
delete from t13 where a = 13;

data branch create table t14 from t6;
insert into t14 values (114, 114);
update t14 set b = 140000 where a = 4;
delete from t14 where a = 14;

data branch create table t15 from t7;
insert into t15 values (115, 115);
update t15 set b = 150000 where a = 5;
delete from t15 where a = 15;

data branch create table t16 from t7;
insert into t16 values (116, 116);
update t16 set b = 160000 where a = 6;
delete from t16 where a = 16;

data branch create table t17 from t8;
insert into t17 values (117, 117);
update t17 set b = 170000 where a = 7;
delete from t17 where a = 17;

data branch create table t18 from t8;
insert into t18 values (118, 118);
update t18 set b = 180000 where a = 8;
delete from t18 where a = 18;

-- ============================================================
-- Finally, IUD on t0 itself.
-- ============================================================
insert into t0 values (200, 200);
update t0 set b = 99999 where a = 1;
delete from t0 where a = 19;

-- ============================================================
-- Sanity: tree metadata query — verify the DAG has 19 nodes.
-- ============================================================
select count(*) as tree_nodes from mo_catalog.mo_branch_metadata m
  join mo_catalog.mo_tables t
    on t.rel_id = m.table_id
 where t.reldatabase = 'test_tree_diff' and t.relname like 't%';

-- ============================================================
-- 10 diff pairs. Each one exercises a structurally distinct
-- cross-section of the tree, so together they stress every
-- relevant LCA code path.
-- ============================================================

-- (1) leaf vs root: skip 3 edges (L4 -> L1). LCA=t0, tarBranchTS=CloneTS(t1).
data branch diff t9 against t0 output summary;
data branch diff t9 against t0 output count;
data branch diff t9 against t0;

-- (2) leaf vs sibling leaf: share immediate grandparent. LCA=t4 (both
-- t9,t10 branched from t4). lcaOther with tar/baseBranchTS from
-- CloneTS(t9) and CloneTS(t10) respectively.
data branch diff t9 against t10 output summary;
data branch diff t9 against t10 output count;
data branch diff t9 against t10;

-- (3) leaf vs cousin leaf: share great-grandparent t1. t9 under
-- t4, t11 under t5. LCA=t1.
data branch diff t9 against t11 output summary;
data branch diff t9 against t11 output count;
data branch diff t9 against t11;

-- (4) leaf vs cross-subtree cousin: t9 (under t1) vs t13 (under
-- t2). LCA=t0. The LCA walk must traverse opposite subtrees.
data branch diff t9 against t13 output summary;
data branch diff t9 against t13 output count;
data branch diff t9 against t13;

-- (5) intermediate vs root: t4 vs t0. LCA=t0 (lcaRight).
data branch diff t4 against t0 output summary;
data branch diff t4 against t0 output count;
data branch diff t4 against t0;

-- (6) leaf vs own parent: direct child/parent. LCA=t4.
data branch diff t9 against t4 output summary;
data branch diff t9 against t4 output count;
data branch diff t9 against t4;

-- (7) leaf vs own grandparent: skip 1 edge. LCA=t1.
data branch diff t9 against t1 output summary;
data branch diff t9 against t1 output count;
data branch diff t9 against t1;

-- (8) leaf vs non-ancestor intermediate of a different subtree:
-- t9 (under t1) vs t6 (under t2). LCA=t0. Non-trivial: neither
-- side is ancestor of the other, and they live in different L2
-- subtrees.
data branch diff t9 against t6 output summary;
data branch diff t9 against t6 output count;
data branch diff t9 against t6;

-- (9) intermediate vs intermediate at different depth: t4 (L3,
-- under t1) vs t3 (L2). LCA=t0.
data branch diff t4 against t3 output summary;
data branch diff t4 against t3 output count;
data branch diff t4 against t3;

-- (10) reverse direction of a deep cross-subtree pair: t18 vs t9.
-- Both are L4 leaves, under completely different L2 subtrees
-- (t18 under t3, t9 under t1). LCA=t0. Plus we test the reverse
-- of (4) for asymmetry verification.
data branch diff t18 against t9 output summary;
data branch diff t18 against t9 output count;
data branch diff t18 against t9;

-- Reverse of (4) as an additional symmetry check.
data branch diff t13 against t9 output summary;

-- ============================================================
-- Bonus: three-way summary sanity across the two sibling pairs
-- under t4. Ensures that for symmetric structures the diff
-- reports are mirror images.
-- ============================================================
data branch diff t10 against t9 output summary;
data branch diff t12 against t11 output summary;
data branch diff t14 against t13 output summary;

-- ============================================================
-- Bonus: a `columns` projection on the deepest pair to confirm
-- projection still works at depth 3.
-- ============================================================
data branch diff t9 against t0 columns (a);
data branch diff t9 against t0 columns (b);

-- ============================================================
-- Cleanup: delete in topological order (leaves first, then
-- intermediates, then root).
-- ============================================================
drop table t9;
drop table t10;
drop table t11;
drop table t12;
drop table t13;
drop table t14;
drop table t15;
drop table t16;
drop table t17;
drop table t18;

drop table t4;
drop table t5;
drop table t6;
drop table t7;
drop table t8;

drop table t1;
drop table t2;
drop table t3;

drop table t0;

drop database test_tree_diff;
