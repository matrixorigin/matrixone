-- =====================================================================
-- vector_ivfpq_metric.sql — IVFPQ across all supported distance metrics
--
-- GPU REQUIRED. Verifies every metric mapped by pkg/vectorindex/metric for the
-- cuvs backend builds + searches on IVFPQ: vector_l2_ops, vector_l2sq_ops,
-- vector_ip_ops (inner product), vector_cosine_ops. (vector_l1_ops is rejected by
-- the CREATE INDEX validator and is intentionally not exercised here.)
--
-- Data: row id=1 is a dominant, unique-direction vector; querying with it makes
-- id=1 the unique nearest under L2, L2sq, cosine AND inner-product, so the top-1
-- is deterministic for every metric. Each search also returns the score:
--   * l2 / l2sq / cosine  -> 0   (exact self-match)
--   * inner_product       -> -1292  (NEGATED on the C++ side to match MO's
--                                    inner_product = -dot convention; smaller = nearer)
-- =====================================================================

SET experimental_ivfpq_index = 1;
SET ivfpq_threads_build = 6;
SET ivfpq_max_index_capacity = 99999;
SET kmeans_train_percent = 100;
SET probe_limit = 16;

drop database if exists ivfpq_metric;
create database ivfpq_metric;
use ivfpq_metric;

create table t (id bigint primary key, v vecf32(8));
insert into t values
    ( 1, '[16,15,14,13,12,11,10,9]'),
    ( 2, '[1,0,0,0,0,0,0,0]'),     ( 3, '[2,0,0,0,0,0,0,0]'),
    ( 4, '[3,0,0,0,0,0,0,0]'),     ( 5, '[4,0,0,0,0,0,0,0]'),
    ( 6, '[0,1,0,0,0,0,0,0]'),     ( 7, '[0,2,0,0,0,0,0,0]'),
    ( 8, '[0,0,3,0,0,0,0,0]'),     ( 9, '[0,0,0,4,0,0,0,0]'),
    (10, '[1,1,0,0,0,0,0,0]'),     (11, '[2,2,0,0,0,0,0,0]'),
    (12, '[0,0,1,1,0,0,0,0]'),     (13, '[3,0,3,0,0,0,0,0]'),
    (14, '[1,2,3,0,0,0,0,0]'),     (15, '[0,4,0,2,0,0,0,0]'),
    (16, '[5,1,0,0,0,0,0,0]'),     (17, '[1,0,5,0,0,0,0,0]'),
    (18, '[2,0,0,5,0,0,0,0]'),     (19, '[0,3,0,0,4,0,0,0]'),
    (20, '[6,0,0,0,0,1,0,0]');

-- ---- vector_l2_ops (l2_distance) ----
create index ix using ivfpq on t (v) op_type 'vector_l2_ops' lists=2 m=8 bits_per_code=8;
select algo, algo_table_type, algo_params from mo_catalog.mo_indexes
    where table_id = (select rel_id from mo_catalog.mo_tables
                      where relname='t' and reldatabase='ivfpq_metric')
      and name='ix' and algo_table_type='ivfpq_index';
select id, l2_distance(v, '[16,15,14,13,12,11,10,9]') as score from t order by score asc limit 1;
drop index ix on t;

-- ---- vector_l2sq_ops (l2_distance_sq) ----
create index ix using ivfpq on t (v) op_type 'vector_l2sq_ops' lists=2 m=8 bits_per_code=8;
select algo, algo_table_type, algo_params from mo_catalog.mo_indexes
    where table_id = (select rel_id from mo_catalog.mo_tables
                      where relname='t' and reldatabase='ivfpq_metric')
      and name='ix' and algo_table_type='ivfpq_index';
select id, l2_distance_sq(v, '[16,15,14,13,12,11,10,9]') as score from t order by score asc limit 1;
drop index ix on t;

-- ---- vector_ip_ops (inner_product) ----
create index ix using ivfpq on t (v) op_type 'vector_ip_ops' lists=2 m=8 bits_per_code=8;
select algo, algo_table_type, algo_params from mo_catalog.mo_indexes
    where table_id = (select rel_id from mo_catalog.mo_tables
                      where relname='t' and reldatabase='ivfpq_metric')
      and name='ix' and algo_table_type='ivfpq_index';
select id, inner_product(v, '[16,15,14,13,12,11,10,9]') as score from t order by score asc limit 1;
drop index ix on t;

-- ---- vector_cosine_ops (cosine_distance) ----
create index ix using ivfpq on t (v) op_type 'vector_cosine_ops' lists=2 m=8 bits_per_code=8;
select algo, algo_table_type, algo_params from mo_catalog.mo_indexes
    where table_id = (select rel_id from mo_catalog.mo_tables
                      where relname='t' and reldatabase='ivfpq_metric')
      and name='ix' and algo_table_type='ivfpq_index';
select id, cosine_distance(v, '[16,15,14,13,12,11,10,9]') as score from t order by score asc limit 1;
drop index ix on t;

drop database ivfpq_metric;
