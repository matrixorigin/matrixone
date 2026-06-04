-- =====================================================================
-- vector_ivfpq_quantization.sql — IVF-PQ with float16 / int8 quantization
--
-- GPU REQUIRED. The vectors stay vecf32; the QUANTIZATION clause sets the
-- internal storage type the IVF-PQ codebook is built over:
--   * float16 — bit-level f32→f16 conversion (2x memory, near-lossless).
--   * int8    — a LEARNED scalar quantizer samples the data for min/max
--               and maps the range to 256 levels (4x memory, lossy).
--
-- Two databases, one per quantization. Each builds a sync IVF-PQ index and
-- asserts (a) the QUANTIZATION option round-trips through the catalog and
-- (b) exact-match search returns the right row.
--
-- Determinism: the dataset is integers 1..20. In float16 every value is
-- exact; in int8 the quantizer trains on [1,20] so each integer maps to a
-- distinct level (~13 levels apart) — so the exact-match probe is always
-- the unique zero-distance top-1 under both quantizations. (Wide-range
-- data would collapse adjacent int8 levels; keep quantization probes on a
-- tight, well-separated range.)
-- =====================================================================

SET experimental_ivfpq_index = 1;
SET ivfpq_threads_build = 6;
SET ivfpq_max_index_capacity = 99999;
SET kmeans_train_percent = 100;
SET kmeans_max_iteration = 12;
SET probe_limit = 16;

-- =====================================================================
-- float16 quantization
-- =====================================================================
drop database if exists ivfpq_q_f16;
create database ivfpq_q_f16;
use ivfpq_q_f16;

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
    QUANTIZATION 'float16';

-- The quantization option round-trips through SHOW CREATE TABLE and the
-- catalog algo_params.
show create table t;
select algo, algo_table_type, algo_params from mo_catalog.mo_indexes
    where table_id = (select rel_id from mo_catalog.mo_tables
                      where relname='t' and reldatabase='ivfpq_q_f16')
      and name='ix' and algo_table_type='ivfpq_index';

select id from t order by l2_distance(v, '[1,1,1,1,1,1,1,1]') limit 1;
select id from t order by l2_distance(v, '[10,10,10,10,10,10,10,10]') limit 1;
select id from t order by l2_distance(v, '[15,15,15,15,15,15,15,15]') limit 1;
select id from t order by l2_distance(v, '[20,20,20,20,20,20,20,20]') limit 1;

drop database ivfpq_q_f16;

-- =====================================================================
-- int8 quantization
-- =====================================================================
drop database if exists ivfpq_q_int8;
create database ivfpq_q_int8;
use ivfpq_q_int8;

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
    QUANTIZATION 'int8';

show create table t;
select algo, algo_table_type, algo_params from mo_catalog.mo_indexes
    where table_id = (select rel_id from mo_catalog.mo_tables
                      where relname='t' and reldatabase='ivfpq_q_int8')
      and name='ix' and algo_table_type='ivfpq_index';

select id from t order by l2_distance(v, '[1,1,1,1,1,1,1,1]') limit 1;
select id from t order by l2_distance(v, '[10,10,10,10,10,10,10,10]') limit 1;
select id from t order by l2_distance(v, '[15,15,15,15,15,15,15,15]') limit 1;
select id from t order by l2_distance(v, '[20,20,20,20,20,20,20,20]') limit 1;

drop database ivfpq_q_int8;
