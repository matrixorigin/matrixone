-- =====================================================================
-- vector_gpu_negative.sql — validation / guard rails for GPU vector indexes
--
-- GPU REQUIRED. Documents the rejections that protect the cuvs (CAGRA / IVF-PQ)
-- backend from unsupported configurations. Each statement below is expected to
-- FAIL with the captured error; the .result records the exact message so a
-- regression in the validators is caught:
--   * op_type 'vector_l1_ops'      — not in the cuvs op-type allow-list
--   * op_type 'vector_bogus_ops'   — unknown op_type
--   * vecf64 column                — cuvs has no float64; only VECF32 allowed
--   * QUANTIZATION 'float64'       — cuvs quantization is f32/f16/int8/uint8 only
--   * dimension mismatch at search — query dim must equal the column dim
-- =====================================================================

SET experimental_cagra_index = 1;
SET experimental_ivfpq_index = 1;

drop database if exists gpu_negative;
create database gpu_negative;
use gpu_negative;

create table t (id bigint primary key, v vecf32(8), lbl varchar(16));
insert into t values
    (1, '[1,1,1,1,1,1,1,1]', 'x'),     (2, '[2,2,2,2,2,2,2,2]', 'x'),
    (3, '[3,3,3,3,3,3,3,3]', 'x'),     (4, '[4,4,4,4,4,4,4,4]', 'x'),
    (5, '[5,5,5,5,5,5,5,5]', 'x'),     (6, '[6,6,6,6,6,6,6,6]', 'x'),
    (7, '[7,7,7,7,7,7,7,7]', 'x'),     (8, '[8,8,8,8,8,8,8,8]', 'x'),
    (9, '[9,9,9,9,9,9,9,9]', 'x'),     (10, '[10,10,10,10,10,10,10,10]', 'x');
create table tf (id bigint primary key, v vecf64(8));

-- L1 is not supported by the cuvs backend (rejected by the op_type validator).
create index ix using cagra on t (v) op_type 'vector_l1_ops';
create index ix using ivfpq on t (v) op_type 'vector_l1_ops' lists=2 m=8 bits_per_code=8;

-- Unknown op_type.
create index ix using cagra on t (v) op_type 'vector_bogus_ops';

-- cuvs has no float64 — a vecf64 column cannot host a CAGRA / IVF-PQ index.
create index ixf using cagra on tf (v) op_type 'vector_l2_ops';
create index ixf using ivfpq on tf (v) op_type 'vector_l2_ops' lists=2 m=8 bits_per_code=8;

-- Unsupported QUANTIZATION value.
create index ixq using cagra on t (v) op_type 'vector_l2_ops' QUANTIZATION 'float64';

-- VARCHAR is not a supported INCLUDE column type (only int32/int64/float32/float64).
create index ixv using cagra on t (v) op_type 'vector_l2_ops' INCLUDE (lbl);

-- A valid index, then a query whose vector dimension differs from the column.
create index ixok using cagra on t (v)
    op_type 'vector_l2_ops' intermediate_graph_degree=8 graph_degree=4 itopk_size=16;
select id from t order by l2_distance(v, '[1,2,3]') asc limit 1;

drop database gpu_negative;
