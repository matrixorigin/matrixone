-- =====================================================================
-- vector_ivfpq_replicated.sql — IVF-PQ index in REPLICATED distribution mode
--
-- GPU REQUIRED. Exercises the REPLICATED dispatch path for IVF-PQ: a full
-- copy of the index (centroids + PQ codebook over all rows) is built on every
-- (logical) GPU and searches are load-balanced across the replicas. On a
-- single-GPU host we present N logical GPUs (all mapped to physical device 0)
-- via the test-only session variable gpu_multi_simulation — see
-- pkg/vectorindex.SimulateDevices. With the per-replica maps keyed by logical
-- rank (not device id), the N replicas coexist on device 0.
--
-- Determinism: same 20-row data, exact-match probes, and recall-robust params
-- (lists=10, m=8, kmeans_train_percent=100, probe_limit) as vector_ivfpq.sql.
-- Each replica is a full copy, so every probe returns its unique zero-distance
-- row regardless of which replica served it.
-- =====================================================================

SET experimental_ivfpq_index = 1;
SET ivfpq_threads_build = 6;
SET ivfpq_max_index_capacity = 99999;
SET kmeans_train_percent = 100;
SET kmeans_max_iteration = 12;
SET probe_limit = 16;

-- 2 replicas, both on physical device 0
SET gpu_multi_simulation = 2;

drop database if exists ivfpq_replicated;
create database ivfpq_replicated;
use ivfpq_replicated;

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

create index ix using ivfpq on t (v)
    op_type 'vector_l2_ops' lists=10 m=8 bits_per_code=8
    distribution_mode 'replicated';

-- The distribution_mode round-trips through SHOW CREATE TABLE and the catalog.
show create table t;
select algo, algo_table_type, algo_params from mo_catalog.mo_indexes
    where table_id = (select rel_id from mo_catalog.mo_tables
                      where relname='t' and reldatabase='ivfpq_replicated')
      and name='ix' and algo_table_type='ivfpq_index';

-- Search: each probe exactly matches one indexed row → deterministic top-1,
-- whichever replica handles the query.
select id from t order by l2_distance(v, '[1,1,1,1,1,1,1,1]') limit 1;
select id from t order by l2_distance(v, '[10,10,10,10,10,10,10,10]') limit 1;
select id from t order by l2_distance(v, '[15,15,15,15,15,15,15,15]') limit 1;
select id from t order by l2_distance(v, '[20,20,20,20,20,20,20,20]') limit 1;

drop database ivfpq_replicated;

SET gpu_multi_simulation = 0;
