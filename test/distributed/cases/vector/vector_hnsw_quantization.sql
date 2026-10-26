-- HNSW QUANTIZATION: the element type the index stores. MO converts every vector and query to
-- it before usearch sees them; the base column keeps its type.
--   float32 / float16 / bf16 / float64: every op_type; float64 only on a vecf64 column.
--   int8 / uint8: L2 only. The build trains one [min,max] range from a SAMPLE of the column,
--   stored in the metadata table, that maps the vectors onto the integer range; distances
--   come back in the column's units.
-- vecf16 / vecbf16 / vecint8 / vecuint8 columns are indexed as they are; a vecint8/vecuint8
-- column needs no range.
-- QUANTIZATION is set at CREATE INDEX; a REINDEX keeps it.
-- Each index-served top-k is checked against the same rows with no index.
drop database if exists hnsw_quantization;
create database hnsw_quantization;
use hnsw_quantization;

set experimental_hnsw_index = 1;

create table t_ref(id bigint primary key, v vecf32(4));
insert into t_ref values
    (1,'[1,1,1,1]'),(2,'[3,3,3,3]'),(3,'[5,5,5,5]'),
    (4,'[50,50,50,50]'),(5,'[52,52,52,52]'),(6,'[54,54,54,54]');
select id from t_ref order by l2_distance(v,'[2,2,2,2]') limit 3;
-- vectors of distinct directions, the reference for cosine
create table t_cos(id bigint primary key, v vecf32(4));
insert into t_cos values
    (1,'[1,0,0,0]'),(2,'[0.9,0.3,0,0]'),(3,'[0,1,0,0]'),
    (4,'[0,0,1,0]'),(5,'[0,0,0.9,0.3]'),(6,'[0.5,0.5,0.5,0.5]');
select id from t_cos order by cosine_distance(v,'[0.95,0.2,0,0]') limit 3;

-- ---------------- float16 --------------------------------------------------
create table t_f16(id bigint primary key, v vecf32(4));
insert into t_f16 select * from t_ref;
create index i_f16 using hnsw on t_f16(v) op_type 'vector_l2_ops' quantization 'float16';
show create table t_f16;
-- @regex("Vector Index Scan on", true)
explain select id from t_f16 order by l2_distance(v,'[2,2,2,2]') limit 3;
select id from t_f16 order by l2_distance(v,'[2,2,2,2]') limit 3;

-- ---------------- bf16 -----------------------------------------------------
create table t_bf16(id bigint primary key, v vecf32(4));
insert into t_bf16 select * from t_cos;
create index i_bf16 using hnsw on t_bf16(v) op_type 'vector_cosine_ops' quantization 'BF16';
show create table t_bf16;
select id from t_bf16 order by cosine_distance(v,'[0.95,0.2,0,0]') limit 3;

-- ---------------- int8 / uint8 (L2) ----------------------------------------
create table t_i8(id bigint primary key, v vecf32(4));
insert into t_i8 select * from t_ref;
create index i_i8 using hnsw on t_i8(v) quantization 'int8';
show create table t_i8;
-- @regex("Vector Index Scan on", true)
explain select id from t_i8 order by l2_distance(v,'[2,2,2,2]') limit 3;
select id from t_i8 order by l2_distance(v,'[2,2,2,2]') limit 3;
select id from t_i8 order by l2_distance(v,'[51,51,51,51]') limit 2;

create table t_u8(id bigint primary key, v vecf32(4));
insert into t_u8 select * from t_ref;
create index i_u8 using hnsw on t_u8(v) op_type 'vector_l2_ops' quantization 'uint8';
show create table t_u8;
select id from t_u8 order by l2_distance(v,'[2,2,2,2]') limit 3;

-- int8/uint8 need L2; unknown values are rejected
create index i_bad using hnsw on t_ref(v) op_type 'vector_cosine_ops' quantization 'int8';
create index i_bad using hnsw on t_ref(v) op_type 'vector_ip_ops' quantization 'uint8';
create index i_bad using hnsw on t_ref(v) op_type 'vector_l2_ops' quantization 'int4';

-- ---------------- float64 / vecf64 ------------------------------------------
-- float64 cannot widen a vecf32 column
create index i_bad using hnsw on t_ref(v) op_type 'vector_l2_ops' quantization 'float64';
create table t_f64(id bigint primary key, v vecf64(4));
insert into t_f64 select id, cast(v as vecf64(4)) from t_ref;
create index i_f64 using hnsw on t_f64(v) op_type 'vector_l2_ops' quantization 'float32';
show create table t_f64;
select id from t_f64 order by l2_distance(v,'[2,2,2,2]') limit 3;

-- ---------------- narrow columns, indexed as they are ------------------------
create table t_cf16(id bigint primary key, v vecf16(4));
insert into t_cf16 select id, cast(v as vecf16(4)) from t_ref;
create index i_cf16 using hnsw on t_cf16(v) op_type 'vector_l2_ops';
-- @regex("Vector Index Scan on", true)
explain select id from t_cf16 order by l2_distance(v,'[2,2,2,2]') limit 3;
select id from t_cf16 order by l2_distance(v,'[2,2,2,2]') limit 3;

create table t_cbf16(id bigint primary key, v vecbf16(4));
insert into t_cbf16 select id, cast(v as vecbf16(4)) from t_cos;
create index i_cbf16 using hnsw on t_cbf16(v) op_type 'vector_cosine_ops';
select id from t_cbf16 order by cosine_distance(v,'[0.95,0.2,0,0]') limit 3;

create table t_ci8(id bigint primary key, v vecint8(4));
insert into t_ci8 select id, cast(v as vecint8(4)) from t_ref;
create index i_ci8 using hnsw on t_ci8(v) op_type 'vector_l2_ops';
-- @regex("Vector Index Scan on", true)
explain select id from t_ci8 order by l2_distance(v,'[2,2,2,2]') limit 3;
select id from t_ci8 order by l2_distance(v,'[2,2,2,2]') limit 3;
select id from t_ci8 order by l2_distance(v,'[51,51,51,51]') limit 2;

create table t_cu8(id bigint primary key, v vecuint8(4));
insert into t_cu8 select id, cast(v as vecuint8(4)) from t_ref;
create index i_cu8 using hnsw on t_cu8(v) op_type 'vector_ip_ops';
select id from t_ref order by inner_product(v,'[1,1,1,1]') limit 2;
select id from t_cu8 order by inner_product(v,'[1,1,1,1]') limit 2;

-- a vecf16 column narrowed further to int8
create table t_cf16_i8(id bigint primary key, v vecf16(4));
insert into t_cf16_i8 select * from t_cf16;
create index i_cf16_i8 using hnsw on t_cf16_i8(v) quantization 'int8';
select id from t_cf16_i8 order by l2_distance(v,'[2,2,2,2]') limit 3;
-- a vecint8 column cannot be widened
create index i_bad using hnsw on t_ci8(v) quantization 'float16';

-- ---------------- REINDEX keeps QUANTIZATION -------------------------------
alter table t_f16 alter reindex i_f16 hnsw quantization 'bf16';
alter table t_f16 alter reindex i_f16 hnsw ef_search = 64;
show create table t_f16;
select id from t_f16 order by l2_distance(v,'[2,2,2,2]') limit 3;

-- ---------------- CDC adds rows to the int8 index with its trained range -----
-- While the index lags the insert the query is planned as a full scan; wait until it is
-- served by the index again, so the new row comes from the int8 index.
insert into t_i8 values (7,'[2,2,2,2]');
-- @wait_expect(2, 60)
explain select id from t_i8 order by l2_distance(v,'[2,2,2,2]') limit 1;
select id from t_i8 order by l2_distance(v,'[2,2,2,2]') limit 1;

insert into t_ci8 values (7,'[2,2,2,2]');
-- @wait_expect(2, 60)
explain select id from t_ci8 order by l2_distance(v,'[2,2,2,2]') limit 1;
select id from t_ci8 order by l2_distance(v,'[2,2,2,2]') limit 1;

drop database hnsw_quantization;
