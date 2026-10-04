-- =====================================================================
-- vector_cagra_f16.sql — CAGRA over a vecf16 (half) BASE column
--
-- GPU REQUIRED. Unlike vector_cagra_quantization.sql (vecf32 base, the
-- QUANTIZATION clause only changes internal storage), here the COLUMN itself
-- is vecf16 — the native base/query type is half end-to-end:
--   * direct   — no QUANTIZATION: the index stores half natively (Q == base).
--   * int8     — vecf16 base quantized half->int8 via the native half-source
--                scalar quantizer (no f32 detour).
--   * uint8    — same, half->uint8.
--
-- Three databases, one per storage. Each builds a sync CAGRA index and asserts
-- (a) the vecf16 column + index round-trip through SHOW CREATE TABLE and
-- (b) exact-match search returns the right row. The query literal is cast to
-- vecf16(8) so the half query path is exercised.
--
-- Determinism: integers 1..20 — every value is exact in half, and the int8/
-- uint8 quantizer trains on [1,20] so each integer maps to a distinct level;
-- the exact-match probe is always the unique top-1. Do not widen the range
-- under int8/uint8 (adjacent levels would collapse).
-- =====================================================================

SET experimental_cagra_index = 1;
SET cagra_threads_build = 7;
SET cagra_max_index_capacity = 99999;

-- =====================================================================
-- vecf16 base, direct (no QUANTIZATION — stored as half)
-- =====================================================================
drop database if exists cagra_f16_direct;
create database cagra_f16_direct;
use cagra_f16_direct;

create table t (id bigint primary key, v vecf16(8));
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
    op_type 'vector_l2_ops' intermediate_graph_degree=16 graph_degree=8 itopk_size=32;

show create table t;
select id from t order by l2_distance(v, cast('[1,1,1,1,1,1,1,1]' as vecf16(8))) limit 1;
select id from t order by l2_distance(v, cast('[10,10,10,10,10,10,10,10]' as vecf16(8))) limit 1;
select id from t order by l2_distance(v, cast('[15,15,15,15,15,15,15,15]' as vecf16(8))) limit 1;
select id from t order by l2_distance(v, cast('[20,20,20,20,20,20,20,20]' as vecf16(8))) limit 1;

drop database cagra_f16_direct;

-- =====================================================================
-- vecf16 base, QUANTIZATION int8 (native half->int8)
-- =====================================================================
drop database if exists cagra_f16_int8;
create database cagra_f16_int8;
use cagra_f16_int8;

create table t (id bigint primary key, v vecf16(8));
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
    QUANTIZATION 'int8';

show create table t;
select id from t order by l2_distance(v, cast('[1,1,1,1,1,1,1,1]' as vecf16(8))) limit 1;
select id from t order by l2_distance(v, cast('[10,10,10,10,10,10,10,10]' as vecf16(8))) limit 1;
select id from t order by l2_distance(v, cast('[15,15,15,15,15,15,15,15]' as vecf16(8))) limit 1;
select id from t order by l2_distance(v, cast('[20,20,20,20,20,20,20,20]' as vecf16(8))) limit 1;

drop database cagra_f16_int8;

-- =====================================================================
-- vecf16 base, QUANTIZATION uint8 (native half->uint8)
-- =====================================================================
drop database if exists cagra_f16_uint8;
create database cagra_f16_uint8;
use cagra_f16_uint8;

create table t (id bigint primary key, v vecf16(8));
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
    QUANTIZATION 'uint8';

show create table t;
select id from t order by l2_distance(v, cast('[1,1,1,1,1,1,1,1]' as vecf16(8))) limit 1;
select id from t order by l2_distance(v, cast('[10,10,10,10,10,10,10,10]' as vecf16(8))) limit 1;
select id from t order by l2_distance(v, cast('[15,15,15,15,15,15,15,15]' as vecf16(8))) limit 1;
select id from t order by l2_distance(v, cast('[20,20,20,20,20,20,20,20]' as vecf16(8))) limit 1;

drop database cagra_f16_uint8;
