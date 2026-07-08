-- =====================================================================
-- vector_ivfpq.sql — IVF-PQ index sync build, search and DDL lifecycle
--
-- GPU REQUIRED. Covers the synchronous (no ASYNC keyword) CREATE INDEX
-- path: ivfpq_create runs inline in the user txn, the cuVS IVF-PQ
-- centroids + codebook are built before CREATE INDEX returns, and search
-- is immediately available — no CDC catch-up sleep needed.
--
-- IVF-PQ is a quantized + clustered approximate index: only the top-1
-- exact-match neighbor is guaranteed stable. Every search below probes a
-- vector that exactly matches an indexed row, so top-1 is deterministic.
--
-- For top-1 to actually be deterministic the PQ residual must be tiny so the
-- approximate distance can't invert the zero-distance row at k=1 (there is no
-- exact re-rank — search returns the raw PQ top-1). Two levers do that here:
--   * lists=10 over 20 rows -> ~2 rows/cell, so the residual each PQ code must
--     encode spans ~2 (vs ~10 with lists=2); the absolute PQ error shrinks ~5x
--     and the boundary probe [1] no longer collapses onto a neighbour.
--   * m=8 over a vecf32(8) = one sub-quantizer per dimension (pq_len=1) with
--     256 levels (bits_per_code=8), so 1..20 reconstruct near-exactly.
-- kmeans_train_percent=100 is required because lists=10 needs >=10 training
-- rows to seed 10 centroids (37% of 20 = 7 < 10 would degenerate), and
-- probe_limit=16 (>=lists) scans every cell so the coarse quantizer never
-- drops the matching row.
-- =====================================================================

SET experimental_ivfpq_index = 1;
SET ivfpq_threads_build = 6;
SET ivfpq_max_index_capacity = 99999;
SET kmeans_train_percent = 100;
SET kmeans_max_iteration = 12;
SET probe_limit = 16;

drop database if exists ivfpq_sync;
create database ivfpq_sync;
use ivfpq_sync;

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

-- Sync default (no ASYNC): ivfpq_create runs inline, blocks until built.
create index ix using ivfpq on t (v)
    op_type 'vector_l2_ops' lists=10 m=8 bits_per_code=8;

-- DDL surface: the index shows up on the table and in the catalog.
show create table t;
desc t;
select name, type, algo, algo_table_type from mo_catalog.mo_indexes
    where table_id = (select rel_id from mo_catalog.mo_tables
                      where relname='t' and reldatabase='ivfpq_sync')
    order by algo_table_type;

-- Storage layout: sync build writes the model to tag=0 only (no overflow).
set @stbl = (select index_table_name from mo_catalog.mo_indexes
    where table_id=(select rel_id from mo_catalog.mo_tables
                    where relname='t' and reldatabase='ivfpq_sync')
      and name='ix' and algo_table_type='ivfpq_index');
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
                      where relname='t' and reldatabase='ivfpq_sync')
      and algo='ivfpq';

-- Re-create and search again to prove the lifecycle is repeatable.
create index ix using ivfpq on t (v)
    op_type 'vector_l2_ops' lists=10 m=8 bits_per_code=8;
select id from t order by l2_distance(v, '[7,7,7,7,7,7,7,7]') limit 1;

drop database ivfpq_sync;
