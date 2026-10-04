-- =====================================================================
-- vector_cagra_quantizer_train_limit.sql — the QUANTIZER_TRAIN_LIMIT option
--
-- GPU REQUIRED. QUANTIZER_TRAIN_LIMIT sets how many rows are strided-sampled
-- to train the int8/uint8 scalar quantizer (default 1000). Asserts the option
-- parses, builds, and round-trips through SHOW CREATE TABLE and the catalog
-- algo_params. (Search accuracy is covered by vector_cagra_quantization.sql.)
-- =====================================================================

SET experimental_cagra_index = 1;
SET cagra_threads_build = 7;
SET cagra_max_index_capacity = 99999;

drop database if exists cagra_qtl_int8;
create database cagra_qtl_int8;
use cagra_qtl_int8;

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

create index ix using cagra on t (v)
    op_type 'vector_l2_ops' intermediate_graph_degree=16 graph_degree=8 itopk_size=32
    QUANTIZATION 'int8' QUANTIZER_TRAIN_LIMIT 5000;

show create table t;
select algo, algo_table_type, algo_params from mo_catalog.mo_indexes
    where table_id = (select rel_id from mo_catalog.mo_tables
                      where relname='t' and reldatabase='cagra_qtl_int8')
      and name='ix' and algo_table_type='cagra_index';
select count(*) from t;

drop database cagra_qtl_int8;
