-- =====================================================================
-- vector_ivfpq_filter_quant.sql — IVFPQ INCLUDE-column pre-filter combined
-- with quantization and a vecf16 base column.
--
-- GPU REQUIRED. vector_ivfpq_filter.sql already covers the INCLUDE pre-filter
-- over a plain vecf32 base (quantization 'float32'). This file proves the SAME
-- predsJSON pre-filter path stays correct when the index storage is compressed
-- or the base column is half:
--   * f32 base + QUANTIZATION 'float16'  — supported (query quantized to T)
--   * f32 base + QUANTIZATION 'int8'     — supported (learned scalar quantizer)
--   * f32 base + QUANTIZATION 'uint8'    — supported
--   * vecf16 base, direct (no QUANTIZATION) — supported (native half query)
--   * vecf16 base + QUANTIZATION 'int8'  + filter — supported (the native half
--                                          query is quantized to int8 inside
--                                          cuVS via search_quantize_with_filter)
--   * vecf16 base + QUANTIZATION 'uint8' + filter — supported (same path)
-- Every storage routes the SAME predsJSON pre-filter through the const-B*
-- quantize search, so the expected nearest neighbor per predicate is identical.
--
-- Data/predicates are identical to vector_ivfpq_filter.sql so the expected
-- nearest neighbor per predicate is unchanged across every storage:
--   id=i -> [i]*8; c_i32=i, c_i64=i*10, c_f32=i.25, c_f64=i.5 (all monotone).
-- Query [12]*8:
--   * c_i32 < 10                    -> id 9
--   * c_i64 >= 100                  -> id 12
--   * c_f32 > 15.25                 -> id 16
--   * c_f64 = 5.5                   -> id 5
--   * c_i32 >= 10 AND c_f64 < 15.5  -> id 12
--   * c_i64 < 100 AND c_f32 > 5.25  -> id 9
--
-- Determinism note: integers 1..20 are exact in float16 and the int8/uint8
-- quantizer trains on [1,20] so each integer maps to a distinct level; each
-- predicate band keeps a unique nearest. Do NOT widen the range under int8/
-- uint8 (adjacent levels would collapse and the top-1 would become ambiguous).
-- =====================================================================

SET experimental_ivfpq_index = 1;
SET ivfpq_threads_build = 6;
SET ivfpq_max_index_capacity = 99999;
SET kmeans_train_percent = 100;
SET probe_limit = 16;

-- =====================================================================
-- f32 base + QUANTIZATION 'float16' + INCLUDE pre-filter
-- =====================================================================
drop database if exists ivfpq_fq_f16;
create database ivfpq_fq_f16;
use ivfpq_fq_f16;

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

create index ix using ivfpq on t (v) op_type 'vector_l2_ops' lists=2 m=8 bits_per_code=8 QUANTIZATION 'float16' INCLUDE (c_i32, c_i64, c_f32, c_f64);

show create table t;
select id from t where c_i32 < 10 order by l2_distance(v, '[12,12,12,12,12,12,12,12]') asc limit 1;
select id from t where c_i64 >= 100 order by l2_distance(v, '[12,12,12,12,12,12,12,12]') asc limit 1;
select id from t where c_f32 > 15.25 order by l2_distance(v, '[12,12,12,12,12,12,12,12]') asc limit 1;
select id from t where c_f64 = 5.5 order by l2_distance(v, '[12,12,12,12,12,12,12,12]') asc limit 1;
select id from t where c_i32 >= 10 and c_f64 < 15.5 order by l2_distance(v, '[12,12,12,12,12,12,12,12]') asc limit 1;
select id from t where c_i64 < 100 and c_f32 > 5.25 order by l2_distance(v, '[12,12,12,12,12,12,12,12]') asc limit 1;

drop database ivfpq_fq_f16;

-- =====================================================================
-- f32 base + QUANTIZATION 'int8' + INCLUDE pre-filter
-- =====================================================================
drop database if exists ivfpq_fq_int8;
create database ivfpq_fq_int8;
use ivfpq_fq_int8;

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

create index ix using ivfpq on t (v) op_type 'vector_l2_ops' lists=2 m=8 bits_per_code=8 QUANTIZATION 'int8' INCLUDE (c_i32, c_i64, c_f32, c_f64);

show create table t;
select id from t where c_i32 < 10 order by l2_distance(v, '[12,12,12,12,12,12,12,12]') asc limit 1;
select id from t where c_i64 >= 100 order by l2_distance(v, '[12,12,12,12,12,12,12,12]') asc limit 1;
select id from t where c_f32 > 15.25 order by l2_distance(v, '[12,12,12,12,12,12,12,12]') asc limit 1;
select id from t where c_f64 = 5.5 order by l2_distance(v, '[12,12,12,12,12,12,12,12]') asc limit 1;
select id from t where c_i32 >= 10 and c_f64 < 15.5 order by l2_distance(v, '[12,12,12,12,12,12,12,12]') asc limit 1;
select id from t where c_i64 < 100 and c_f32 > 5.25 order by l2_distance(v, '[12,12,12,12,12,12,12,12]') asc limit 1;

drop database ivfpq_fq_int8;

-- =====================================================================
-- f32 base + QUANTIZATION 'uint8' + INCLUDE pre-filter
-- =====================================================================
drop database if exists ivfpq_fq_uint8;
create database ivfpq_fq_uint8;
use ivfpq_fq_uint8;

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

create index ix using ivfpq on t (v) op_type 'vector_l2_ops' lists=2 m=8 bits_per_code=8 QUANTIZATION 'uint8' INCLUDE (c_i32, c_i64, c_f32, c_f64);

show create table t;
select id from t where c_i32 < 10 order by l2_distance(v, '[12,12,12,12,12,12,12,12]') asc limit 1;
select id from t where c_i64 >= 100 order by l2_distance(v, '[12,12,12,12,12,12,12,12]') asc limit 1;
select id from t where c_f32 > 15.25 order by l2_distance(v, '[12,12,12,12,12,12,12,12]') asc limit 1;
select id from t where c_f64 = 5.5 order by l2_distance(v, '[12,12,12,12,12,12,12,12]') asc limit 1;
select id from t where c_i32 >= 10 and c_f64 < 15.5 order by l2_distance(v, '[12,12,12,12,12,12,12,12]') asc limit 1;
select id from t where c_i64 < 100 and c_f32 > 5.25 order by l2_distance(v, '[12,12,12,12,12,12,12,12]') asc limit 1;

drop database ivfpq_fq_uint8;

-- =====================================================================
-- vecf16 base, direct (no QUANTIZATION) + INCLUDE pre-filter
-- The query literal is cast to vecf16(8) so the half query path is exercised.
-- =====================================================================
drop database if exists ivfpq_fq_f16base;
create database ivfpq_fq_f16base;
use ivfpq_fq_f16base;

create table t (id bigint primary key, v vecf16(8), c_i32 int, c_i64 bigint, c_f32 float, c_f64 double);
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

create index ix using ivfpq on t (v) op_type 'vector_l2_ops' lists=2 m=8 bits_per_code=8 INCLUDE (c_i32, c_i64, c_f32, c_f64);

show create table t;
select id from t where c_i32 < 10 order by l2_distance(v, cast('[12,12,12,12,12,12,12,12]' as vecf16(8))) asc limit 1;
select id from t where c_i64 >= 100 order by l2_distance(v, cast('[12,12,12,12,12,12,12,12]' as vecf16(8))) asc limit 1;
select id from t where c_f32 > 15.25 order by l2_distance(v, cast('[12,12,12,12,12,12,12,12]' as vecf16(8))) asc limit 1;
select id from t where c_f64 = 5.5 order by l2_distance(v, cast('[12,12,12,12,12,12,12,12]' as vecf16(8))) asc limit 1;
select id from t where c_i32 >= 10 and c_f64 < 15.5 order by l2_distance(v, cast('[12,12,12,12,12,12,12,12]' as vecf16(8))) asc limit 1;
select id from t where c_i64 < 100 and c_f32 > 5.25 order by l2_distance(v, cast('[12,12,12,12,12,12,12,12]' as vecf16(8))) asc limit 1;

drop database ivfpq_fq_f16base;

-- =====================================================================
-- vecf16 base + QUANTIZATION 'int8' + INCLUDE pre-filter
-- The native half query is quantized to int8 inside cuVS (the const-B*
-- search_quantize_with_filter path); same predicates and nearest neighbors
-- as every storage above.
-- =====================================================================
drop database if exists ivfpq_fq_f16int8;
create database ivfpq_fq_f16int8;
use ivfpq_fq_f16int8;

create table t (id bigint primary key, v vecf16(8), c_i32 int, c_i64 bigint, c_f32 float, c_f64 double);
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

create index ix using ivfpq on t (v) op_type 'vector_l2_ops' lists=2 m=8 bits_per_code=8 QUANTIZATION 'int8' INCLUDE (c_i32, c_i64, c_f32, c_f64);

show create table t;
select id from t where c_i32 < 10 order by l2_distance(v, cast('[12,12,12,12,12,12,12,12]' as vecf16(8))) asc limit 1;
select id from t where c_i64 >= 100 order by l2_distance(v, cast('[12,12,12,12,12,12,12,12]' as vecf16(8))) asc limit 1;
select id from t where c_f32 > 15.25 order by l2_distance(v, cast('[12,12,12,12,12,12,12,12]' as vecf16(8))) asc limit 1;
select id from t where c_f64 = 5.5 order by l2_distance(v, cast('[12,12,12,12,12,12,12,12]' as vecf16(8))) asc limit 1;
select id from t where c_i32 >= 10 and c_f64 < 15.5 order by l2_distance(v, cast('[12,12,12,12,12,12,12,12]' as vecf16(8))) asc limit 1;
select id from t where c_i64 < 100 and c_f32 > 5.25 order by l2_distance(v, cast('[12,12,12,12,12,12,12,12]' as vecf16(8))) asc limit 1;

drop database ivfpq_fq_f16int8;

-- =====================================================================
-- vecf16 base + QUANTIZATION 'uint8' + INCLUDE pre-filter (same path as int8)
-- =====================================================================
drop database if exists ivfpq_fq_f16uint8;
create database ivfpq_fq_f16uint8;
use ivfpq_fq_f16uint8;

create table t (id bigint primary key, v vecf16(8), c_i32 int, c_i64 bigint, c_f32 float, c_f64 double);
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

create index ix using ivfpq on t (v) op_type 'vector_l2_ops' lists=2 m=8 bits_per_code=8 QUANTIZATION 'uint8' INCLUDE (c_i32, c_i64, c_f32, c_f64);

show create table t;
select id from t where c_i32 < 10 order by l2_distance(v, cast('[12,12,12,12,12,12,12,12]' as vecf16(8))) asc limit 1;
select id from t where c_i64 >= 100 order by l2_distance(v, cast('[12,12,12,12,12,12,12,12]' as vecf16(8))) asc limit 1;
select id from t where c_f32 > 15.25 order by l2_distance(v, cast('[12,12,12,12,12,12,12,12]' as vecf16(8))) asc limit 1;
select id from t where c_f64 = 5.5 order by l2_distance(v, cast('[12,12,12,12,12,12,12,12]' as vecf16(8))) asc limit 1;
select id from t where c_i32 >= 10 and c_f64 < 15.5 order by l2_distance(v, cast('[12,12,12,12,12,12,12,12]' as vecf16(8))) asc limit 1;
select id from t where c_i64 < 100 and c_f32 > 5.25 order by l2_distance(v, cast('[12,12,12,12,12,12,12,12]' as vecf16(8))) asc limit 1;

drop database ivfpq_fq_f16uint8;
