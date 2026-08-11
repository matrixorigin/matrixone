-- =====================================================================
-- vector_ivfpq_quantizer_train_limit.sql — the QUANTIZER_TRAIN_LIMIT option
--
-- GPU REQUIRED. QUANTIZER_TRAIN_LIMIT sets how many rows are strided-sampled
-- to train the int8/uint8 scalar quantizer (default 1000). This case asserts
-- the option parses, builds, and round-trips through BOTH SHOW CREATE TABLE
-- and the catalog algo_params, for int8 and uint8. (Recall/search accuracy is
-- covered by vector_ivfpq_quantization.sql; ivfpq top-k is approximate, so this
-- case only checks the deterministic option plumbing.)
-- =====================================================================

SET experimental_ivfpq_index = 1;
SET ivfpq_threads_build = 6;
SET ivfpq_max_index_capacity = 99999;

-- =====================================================================
-- int8 with quantizer_train_limit
-- =====================================================================
drop database if exists ivfpq_qtl_int8;
create database ivfpq_qtl_int8;
use ivfpq_qtl_int8;

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
    QUANTIZATION 'int8' QUANTIZER_TRAIN_LIMIT 5000;

-- quantizer_train_limit round-trips through SHOW CREATE TABLE ...
show create table t;
-- ... and the catalog algo_params.
select algo, algo_table_type, algo_params from mo_catalog.mo_indexes
    where table_id = (select rel_id from mo_catalog.mo_tables
                      where relname='t' and reldatabase='ivfpq_qtl_int8')
      and name='ix' and algo_table_type='ivfpq_index';
-- index built and table is queryable
select count(*) from t;

drop database ivfpq_qtl_int8;

-- =====================================================================
-- uint8 with a small quantizer_train_limit
-- =====================================================================
drop database if exists ivfpq_qtl_uint8;
create database ivfpq_qtl_uint8;
use ivfpq_qtl_uint8;

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
    QUANTIZATION 'uint8' QUANTIZER_TRAIN_LIMIT 200;

show create table t;
select algo, algo_table_type, algo_params from mo_catalog.mo_indexes
    where table_id = (select rel_id from mo_catalog.mo_tables
                      where relname='t' and reldatabase='ivfpq_qtl_uint8')
      and name='ix' and algo_table_type='ivfpq_index';
select count(*) from t;

drop database ivfpq_qtl_uint8;
