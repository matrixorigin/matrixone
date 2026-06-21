-- =====================================================================
-- vector_cagra.sql — CAGRA index sync build, search and DDL lifecycle
--
-- GPU REQUIRED. Covers the synchronous (no ASYNC keyword) CREATE INDEX
-- path: cagra_create runs inline in the user txn, the cuVS CAGRA graph
-- is built before CREATE INDEX returns, and search is immediately
-- available — no CDC catch-up sleep needed.
--
-- CAGRA is an approximate index: only the top-1 exact-match neighbor is
-- guaranteed stable (the graph build is thread/FP order dependent so
-- lower-rank approximate neighbors can vary). Every search below probes
-- a vector that exactly matches an indexed row, so top-1 is deterministic.
--
-- For top-1 to actually be deterministic on this 20-row toy set the graph
-- must be dense enough that no node is unreachable from the search seeds:
-- graph_degree=8 / intermediate_graph_degree=16 fully connect the 20 nodes,
-- and itopk_size=32 (> row count) makes the search visit every reachable
-- node, so the zero-distance exact match is always returned. (A sparse
-- graph_degree=4 graph leaves corner nodes unreachable -> recall@1 misses.)
-- =====================================================================

SET experimental_cagra_index = 1;
SET cagra_threads_build = 7;
SET cagra_max_index_capacity = 99999;

drop database if exists cagra_sync;
create database cagra_sync;
use cagra_sync;

create table t (id bigint primary key, v vecf32(8));
insert into t values
    ( 1, '[1,1,1,1,1,1,1,1]'),     ( 2, '[2,2,2,2,2,2,2,2]'),
    ( 3, '[3,3,3,3,3,3,3,3]'),     ( 4, '[4,4,4,4,4,4,4,4]'),
    ( 5, '[5,5,5,5,5,5,5,5]'),     ( 6, '[6,6,6,6,6,6,6,6]'),
    ( 7, '[7,7,7,7,7,7,7,7]'),     ( 8, '[8,8,8,8,8,8,8,8]'),
    ( 9, '[9,9,9,9,9,9,9,9]'),     (10, '[10,10,10,10,10,10,10,10]'),
    (11, '[11,11,11,11,11,11,11,11]'), (12, '[12,12,12,12,12,12,12,12]'),
    (13, '[13,13,13,13,13,13,13,13]'), (14, '[14,14,14,14,14,14,14,14]'),
    (15, '[15,15,15,15,15,15,15,15]'), (16, '[16,16,16,16,16,16,16,16]'),
    (17, '[17,17,17,17,17,17,17,17]'), (18, '[18,18,18,18,18,18,18,18]'),
    (19, '[19,19,19,19,19,19,19,19]'), (20, '[20,20,20,20,20,20,20,20]');

-- Sync default (no ASYNC): cagra_create runs inline, blocks until built.
create index ix using cagra on t (v)
    op_type 'vector_l2_ops' intermediate_graph_degree=16 graph_degree=8 itopk_size=32;

-- DDL surface: the index shows up on the table and in the catalog.
show create table t;
desc t;
select name, type, algo, algo_table_type from mo_catalog.mo_indexes
    where table_id = (select rel_id from mo_catalog.mo_tables
                      where relname='t' and reldatabase='cagra_sync')
    order by algo_table_type;

-- Storage layout: sync build writes the model to tag=0 only (no overflow).
set @stbl = (select index_table_name from mo_catalog.mo_indexes
    where table_id=(select rel_id from mo_catalog.mo_tables
                    where relname='t' and reldatabase='cagra_sync')
      and name='ix' and algo_table_type='cagra_index');
set @q = concat('select distinct tag from `', @stbl, '` order by tag');
prepare s from @q; execute s; deallocate prepare s;

-- Search: each probe exactly matches one indexed row → deterministic top-1.
select id from t order by l2_distance(v, '[1,1,1,1,1,1,1,1]') limit 1;
select id from t order by l2_distance(v, '[10,10,10,10,10,10,10,10]') limit 1;
select id from t order by l2_distance(v, '[15,15,15,15,15,15,15,15]') limit 1;
select id from t order by l2_distance(v, '[20,20,20,20,20,20,20,20]') limit 1;

-- DROP INDEX removes the catalog rows; the table reverts to no secondary key.
drop index ix on t;
show create table t;
select count(*) from mo_catalog.mo_indexes
    where table_id = (select rel_id from mo_catalog.mo_tables
                      where relname='t' and reldatabase='cagra_sync')
      and algo='cagra';

-- Re-create and search again to prove the lifecycle is repeatable.
create index ix using cagra on t (v)
    op_type 'vector_l2_ops' intermediate_graph_degree=16 graph_degree=8 itopk_size=32;
select id from t order by l2_distance(v, '[7,7,7,7,7,7,7,7]') limit 1;

drop database cagra_sync;
