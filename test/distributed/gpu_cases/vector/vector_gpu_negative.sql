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
--   * QUANTIZATION 'bf16'          — no GPU bfloat16 storage; must not silent-fallback to f32
--   * int8/uint8 + ip/cosine       — affine quantizer breaks dot-product/angle (L2-only) at CREATE
--   * REINDEX QUANTIZATION          — the (quantization, op_type) pair is gated via the per-algo
--                                     ValidQuantization hook on the merged config: bad values
--                                     (bf16/float64) and int8/uint8 on a non-L2 index are rejected
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

-- QUANTIZATION 'bf16' has no GPU bfloat16 storage (cuvs has no bfloat16 index or
-- quantizer). It passes the downcast width guard (bf16 is 2 bytes <= f32's 4),
-- so it must be rejected explicitly rather than silently building f32 storage.
create index ixbq using cagra on t (v) op_type 'vector_l2_ops' QUANTIZATION 'bf16';
create index ixbq using ivfpq on t (v) op_type 'vector_l2_ops' lists=2 m=8 bits_per_code=8 QUANTIZATION 'bf16';

-- VARCHAR is not a supported INCLUDE column type (only int32/int64/float32/float64).
create index ixv using cagra on t (v) op_type 'vector_l2_ops' INCLUDE (lbl);

-- A valid index, then a query whose vector dimension differs from the column.
create index ixok using cagra on t (v)
    op_type 'vector_l2_ops' intermediate_graph_degree=8 graph_degree=4 itopk_size=16;
select id from t order by l2_distance(v, '[1,2,3]') asc limit 1;

-- Base-column type guard: only vecf32 / vecf16 are valid base columns;
-- vecbf16 (like int8/uint8) is rejected.
create table tbf (id bigint primary key, v vecbf16(8));
create index ixbf using cagra on tbf (v) op_type 'vector_l2_ops';
create index ixbf using ivfpq on tbf (v) op_type 'vector_l2_ops' lists=2 m=8 bits_per_code=8;

-- QUANTIZATION is downcast-only: a vecf16 base (2 bytes/element) cannot be
-- upcast to float32 storage (4 bytes/element).
create table th (id bigint primary key, v vecf16(8));
create index ixup using cagra on th (v) op_type 'vector_l2_ops' QUANTIZATION 'float32';
create index ixup using ivfpq on th (v) op_type 'vector_l2_ops' lists=2 m=8 bits_per_code=8 QUANTIZATION 'float32';

-- int8/uint8 QUANTIZATION is L2-only. The scalar quantizer applies a per-element
-- affine map q(x)=scalar*x+offset; the constant offset is a translation that
-- cancels in an L2 difference but NOT in a dot product (biases IP by component
-- sum) or norm (rotates cosine angles). So int8/uint8 + inner-product / cosine
-- returns wrong rankings and is rejected. (L2 is fine; the scale is corrected
-- on search.)
create index ixqi using cagra on t (v) op_type 'vector_ip_ops' QUANTIZATION 'int8';
create index ixqi using cagra on t (v) op_type 'vector_cosine_ops' QUANTIZATION 'int8';
create index ixqi using ivfpq on t (v) op_type 'vector_ip_ops' lists=2 m=8 bits_per_code=8 QUANTIZATION 'uint8';

-- REINDEX gates the (quantization, op_type) pair through the per-algo
-- ValidQuantization hook, evaluated on the MERGED config: the value must be a
-- cuvs storage name (float32/float16/int8/uint8 — bf16/float64 rejected), and
-- int8/uint8 require L2. op_type is immutable across a reindex, so the merged
-- op_type is the index's stored inner-product — hence int8 is rejected here too.
-- Built on its own table, since t already has a CAGRA index on v and two CAGRA
-- indexes may not share a column.
create table tre (id bigint primary key, v vecf32(8));
insert into tre values
    (1, '[1,1,1,1,1,1,1,1]'), (2, '[2,2,2,2,2,2,2,2]'), (3, '[3,3,3,3,3,3,3,3]'),
    (4, '[4,4,4,4,4,4,4,4]'), (5, '[5,5,5,5,5,5,5,5]'), (6, '[6,6,6,6,6,6,6,6]'),
    (7, '[7,7,7,7,7,7,7,7]'), (8, '[8,8,8,8,8,8,8,8]'), (9, '[9,9,9,9,9,9,9,9]'),
    (10, '[10,10,10,10,10,10,10,10]');
create index ixre using cagra on tre (v) op_type 'vector_ip_ops'
    intermediate_graph_degree=8 graph_degree=4 itopk_size=16;
alter table tre alter reindex ixre cagra QUANTIZATION 'int8';
alter table tre alter reindex ixre cagra QUANTIZATION 'bf16';
alter table tre alter reindex ixre cagra QUANTIZATION 'float64';

drop database gpu_negative;
