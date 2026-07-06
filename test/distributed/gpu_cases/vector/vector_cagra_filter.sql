-- =====================================================================
-- vector_cagra_filter.sql — CAGRA pre-filtered search via INCLUDE columns
--
-- GPU REQUIRED. Covers the INCLUDE-column pre-filter path across ALL FOUR
-- supported INCLUDE column types — int32, int64, float32, float64 (VARCHAR is
-- rejected; see vector_gpu_negative.sql). A WHERE predicate on any of them, or
-- on several combined, is pushed into the GPU search (predsJSON) and restricts
-- the ANN candidate set before ranking. Verifies the INCLUDE list round-trips
-- and single- + multi-column predicates change the nearest neighbor.
--
-- Data: id=i -> [i]*8; c_i32=i (int), c_i64=i*10 (bigint), c_f32=i.25 (float),
-- c_f64=i.5 (double); all monotone so each predicate band has a deterministic
-- nearest. Query [12]*8:
--   * c_i32 < 10                    -> id 9   (int32; id 12 excluded)
--   * c_i64 >= 100                  -> id 12  (int64; exact self-match passes)
--   * c_f32 > 15.25                 -> id 16  (float32; id 12 excluded)
--   * c_f64 = 5.5                   -> id 5   (float64 equality; single row)
--   * c_i32 >= 10 AND c_f64 < 15.5  -> id 12  (int32 + float64 band 10..14)
--   * c_i64 < 100 AND c_f32 > 5.25  -> id 9   (int64 + float32 band 6..9)
-- =====================================================================

SET experimental_cagra_index = 1;
SET cagra_threads_build = 7;
SET cagra_max_index_capacity = 99999;

drop database if exists cagra_filter;
create database cagra_filter;
use cagra_filter;

create table t (id bigint primary key, v vecf32(8), c_i32 int, c_i64 bigint, c_f32 float, c_f64 double);
insert into t values
    (1, '[1,1,1,1,1,1,1,1]', 1, 10, 1.25, 1.5),
    (2, '[2,2,2,2,2,2,2,2]', 2, 20, 2.25, 2.5),
    (3, '[3,3,3,3,3,3,3,3]', 3, 30, 3.25, 3.5),
    (4, '[4,4,4,4,4,4,4,4]', 4, 40, 4.25, 4.5),
    (5, '[5,5,5,5,5,5,5,5]', 5, 50, 5.25, 5.5),
    (6, '[6,6,6,6,6,6,6,6]', 6, 60, 6.25, 6.5),
    (7, '[7,7,7,7,7,7,7,7]', 7, 70, 7.25, 7.5),
    (8, '[8,8,8,8,8,8,8,8]', 8, 80, 8.25, 8.5),
    (9, '[9,9,9,9,9,9,9,9]', 9, 90, 9.25, 9.5),
    (10, '[10,10,10,10,10,10,10,10]', 10, 100, 10.25, 10.5),
    (11, '[11,11,11,11,11,11,11,11]', 11, 110, 11.25, 11.5),
    (12, '[12,12,12,12,12,12,12,12]', 12, 120, 12.25, 12.5),
    (13, '[13,13,13,13,13,13,13,13]', 13, 130, 13.25, 13.5),
    (14, '[14,14,14,14,14,14,14,14]', 14, 140, 14.25, 14.5),
    (15, '[15,15,15,15,15,15,15,15]', 15, 150, 15.25, 15.5),
    (16, '[16,16,16,16,16,16,16,16]', 16, 160, 16.25, 16.5),
    (17, '[17,17,17,17,17,17,17,17]', 17, 170, 17.25, 17.5),
    (18, '[18,18,18,18,18,18,18,18]', 18, 180, 18.25, 18.5),
    (19, '[19,19,19,19,19,19,19,19]', 19, 190, 19.25, 19.5),
    (20, '[20,20,20,20,20,20,20,20]', 20, 200, 20.25, 20.5);

create index ix using cagra on t (v) op_type 'vector_l2_ops' intermediate_graph_degree=16 graph_degree=8 itopk_size=32 INCLUDE (c_i32, c_i64, c_f32, c_f64);

show create table t;
select algo, algo_table_type, algo_params from mo_catalog.mo_indexes
    where table_id = (select rel_id from mo_catalog.mo_tables
                      where relname='t' and reldatabase='cagra_filter')
      and name='ix' and algo_table_type='cagra_index';

select id from t where c_i32 < 10 order by l2_distance(v, '[12,12,12,12,12,12,12,12]') asc limit 1;
select id from t where c_i64 >= 100 order by l2_distance(v, '[12,12,12,12,12,12,12,12]') asc limit 1;
select id from t where c_f32 > 15.25 order by l2_distance(v, '[12,12,12,12,12,12,12,12]') asc limit 1;
select id from t where c_f64 = 5.5 order by l2_distance(v, '[12,12,12,12,12,12,12,12]') asc limit 1;
select id from t where c_i32 >= 10 and c_f64 < 15.5 order by l2_distance(v, '[12,12,12,12,12,12,12,12]') asc limit 1;
select id from t where c_i64 < 100 and c_f32 > 5.25 order by l2_distance(v, '[12,12,12,12,12,12,12,12]') asc limit 1;

drop database cagra_filter;
