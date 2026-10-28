-- HNSW QUANTIZATION, narrow vector columns and exact prefiltered search.
-- Each section proves the claim of docs/design/20261026-hnsw-quantization.md it is headed by.
-- Each index-served top-k is checked against the same rows with no index.
drop database if exists hnsw_quantization;
create database hnsw_quantization;
use hnsw_quantization;

set experimental_hnsw_index = 1;

-- A CN that does not run the ISCP sync keeps its cached HNSW model until the cross-CN
-- freshness sweep (default about 10 minutes). Shorten the sweep on every CN so the
-- waits below converge; restore the default at exit.
select mo_ctl('cn', 'SetVectorIndexFreshnessInterval', '1s') not like '%failed=[%' as all_cn_updated;

create table t_ref(id bigint primary key, v vecf32(4));
insert into t_ref values
    (1,'[1,1,1,1]'),(2,'[3,3,3,3]'),(3,'[5,5,5,5]'),
    (4,'[50,50,50,50]'),(5,'[52,52,52,52]'),(6,'[54,54,54,54]');
select id from t_ref order by l2_distance(v,'[2,2,2,3]') limit 3;
-- vectors of distinct directions, the reference for cosine
create table t_cos(id bigint primary key, v vecf32(4));
insert into t_cos values
    (1,'[1,0,0,0]'),(2,'[0.9,0.3,0,0]'),(3,'[0,1,0,0]'),
    (4,'[0,0,1,0]'),(5,'[0,0,0.9,0.3]'),(6,'[0.5,0.5,0.5,0.5]');
select id from t_cos order by cosine_distance(v,'[0.95,0.2,0,0]') limit 3;

-- ---------------- C1: accepted values, reported by SHOW CREATE TABLE --------
create table t_f16(id bigint primary key, v vecf32(4));
insert into t_f16 select * from t_ref;
create index i_f16 using hnsw on t_f16(v) op_type 'vector_l2_ops' quantization 'float16';
show create table t_f16;
create table t_bf16(id bigint primary key, v vecf32(4));
insert into t_bf16 select * from t_cos;
create index i_bf16 using hnsw on t_bf16(v) op_type 'vector_cosine_ops' quantization 'BF16';
show create table t_bf16;
create table t_f32(id bigint primary key, v vecf32(4));
insert into t_f32 select * from t_ref;
create index i_f32 using hnsw on t_f32(v) quantization 'Float32';
show create table t_f32;
create table t_i8(id bigint primary key, v vecf32(4));
insert into t_i8 select * from t_ref;
create index i_i8 using hnsw on t_i8(v) quantization 'int8';
show create table t_i8;
create table t_u8(id bigint primary key, v vecf32(4));
insert into t_u8 select * from t_ref;
create index i_u8 using hnsw on t_u8(v) op_type 'vector_l2_ops' quantization 'uint8';
show create table t_u8;
create table t_f64(id bigint primary key, v vecf64(4));
insert into t_f64 select id, cast(v as vecf64(4)) from t_ref;
create index i_f64 using hnsw on t_f64(v) op_type 'vector_l2_ops' quantization 'float64';
show create table t_f64;
create table t_f64n(id bigint primary key, v vecf64(4));
insert into t_f64n select * from t_f64;
create index i_f64n using hnsw on t_f64n(v) op_type 'vector_l2_ops' quantization 'float32';
show create table t_f64n;
create index i_bad using hnsw on t_ref(v) op_type 'vector_l2_ops' quantization 'int4';

-- ---------------- C2: narrow columns indexed as they are ---------------------
create table t_cf16(id bigint primary key, v vecf16(4));
insert into t_cf16 select id, cast(v as vecf16(4)) from t_ref;
create index i_cf16 using hnsw on t_cf16(v) op_type 'vector_l2_ops';
-- @regex("Vector Index Scan on", true)
explain select id from t_cf16 order by l2_distance(v,'[2,2,2,3]') limit 3;
select id from t_cf16 order by l2_distance(v,'[2,2,2,3]') limit 3;

create table t_cbf16(id bigint primary key, v vecbf16(4));
insert into t_cbf16 select id, cast(v as vecbf16(4)) from t_cos;
create index i_cbf16 using hnsw on t_cbf16(v) op_type 'vector_cosine_ops';
-- @regex("Vector Index Scan on", true)
explain select id from t_cbf16 order by cosine_distance(v,'[0.95,0.2,0,0]') limit 3;
select id from t_cbf16 order by cosine_distance(v,'[0.95,0.2,0,0]') limit 3;

create table t_ci8(id bigint primary key, v vecint8(4));
insert into t_ci8 select id, cast(v as vecint8(4)) from t_ref;
create index i_ci8 using hnsw on t_ci8(v) op_type 'vector_l2_ops';
-- @regex("Vector Index Scan on", true)
explain select id from t_ci8 order by l2_distance(v,'[2,2,2,3]') limit 3;
select id from t_ci8 order by l2_distance(v,'[2,2,2,3]') limit 3;
select id from t_ci8 order by l2_distance(v,'[52,52,52,51]') limit 2;

create table t_cu8(id bigint primary key, v vecuint8(4));
insert into t_cu8 select id, cast(v as vecuint8(4)) from t_ref;
create index i_cu8 using hnsw on t_cu8(v) op_type 'vector_ip_ops';
-- @regex("Vector Index Scan on", true)
explain select id from t_cu8 order by inner_product(v,'[1,1,1,1]') limit 2;
select id from t_ref order by inner_product(v,'[1,1,1,1]') limit 2;
select id from t_cu8 order by inner_product(v,'[1,1,1,1]') limit 2;

-- ---------------- C3: no widening --------------------------------------------
create index i_bad using hnsw on t_ref(v) op_type 'vector_l2_ops' quantization 'float64';
create index i_bad using hnsw on t_cf16(v) quantization 'float32';
create index i_bad using hnsw on t_ci8(v) quantization 'float16';
create index i_bad using hnsw on t_cu8(v) quantization 'bf16';
-- equal or narrower is accepted: vecf16 -> int8, vecuint8 -> int8
create table t_cf16_i8(id bigint primary key, v vecf16(4));
insert into t_cf16_i8 select * from t_cf16;
create index i_cf16_i8 using hnsw on t_cf16_i8(v) quantization 'int8';
select id from t_cf16_i8 order by l2_distance(v,'[2,2,2,3]') limit 3;
create table t_cu8_i8(id bigint primary key, v vecuint8(4));
insert into t_cu8_i8 select * from t_cu8;
create index i_cu8_i8 using hnsw on t_cu8_i8(v) quantization 'int8';
-- @regex("Vector Index Scan on", true)
explain select id from t_cu8_i8 order by l2_distance(v,'[52,52,52,51]') limit 2;
select id from t_cu8_i8 order by l2_distance(v,'[52,52,52,51]') limit 2;

-- ---------------- C4: int8 / uint8 need L2 -----------------------------------
create index i_bad using hnsw on t_ref(v) op_type 'vector_cosine_ops' quantization 'int8';
create index i_bad using hnsw on t_ref(v) op_type 'vector_ip_ops' quantization 'uint8';
create table t_sq(id bigint primary key, v vecf32(4));
insert into t_sq select * from t_ref;
create index i_sq using hnsw on t_sq(v) op_type 'vector_l2sq_ops' quantization 'uint8';
-- @regex("Vector Index Scan on", true)
explain select id from t_sq order by l2_distance_sq(v,'[2,2,2,3]') limit 3;
select id from t_sq order by l2_distance_sq(v,'[2,2,2,3]') limit 3;

-- ---------------- C5: index top-k equals the exact top-k ---------------------
-- @regex("Vector Index Scan on", true)
explain select id from t_f16 order by l2_distance(v,'[2,2,2,3]') limit 3;
select id from t_f16 order by l2_distance(v,'[2,2,2,3]') limit 3;
-- @regex("Vector Index Scan on", true)
explain select id from t_bf16 order by cosine_distance(v,'[0.95,0.2,0,0]') limit 3;
select id from t_bf16 order by cosine_distance(v,'[0.95,0.2,0,0]') limit 3;
select id from t_f32 order by l2_distance(v,'[2,2,2,3]') limit 3;
-- @regex("Vector Index Scan on", true)
explain select id from t_i8 order by l2_distance(v,'[2,2,2,3]') limit 3;
select id from t_i8 order by l2_distance(v,'[2,2,2,3]') limit 3;
select id from t_i8 order by l2_distance(v,'[52,52,52,51]') limit 2;
-- @regex("Vector Index Scan on", true)
explain select id from t_u8 order by l2_distance(v,'[2,2,2,3]') limit 3;
select id from t_u8 order by l2_distance(v,'[2,2,2,3]') limit 3;
select id from t_f64 order by l2_distance(v,'[2,2,2,3]') limit 3;
select id from t_f64n order by l2_distance(v,'[2,2,2,3]') limit 3;

-- ---------------- C6: int8 / uint8 distances are in the column's units -------
-- the trained range is [1,54]: one step is 53/255, so the error of a 4-d L2 distance is
-- below 4 * (53/255) < 1
select id, l2_distance(v,'[2.3,2,2,2]') d from t_ref order by d limit 3;
select q.id, abs(q.d - l2_distance(r.v,'[2.3,2,2,2]')) < 1 within_step
    from (select id, l2_distance(v,'[2.3,2,2,2]') d from t_i8 order by d limit 3) q
    join t_ref r on r.id = q.id order by q.d;
select q.id, abs(q.d - l2_distance(r.v,'[2.3,2,2,2]')) < 1 within_step
    from (select id, l2_distance(v,'[2.3,2,2,2]') d from t_u8 order by d limit 3) q
    join t_ref r on r.id = q.id order by q.d;
-- a vecint8 column is stored as it is: its distances are exact
select id, l2_distance(v,'[2,2,2,3]') d from t_ci8 order by d limit 3;

-- ---------------- C7: incremental sync uses the index's range ----------------
-- While the index lags a change the query is served by the index as it was; wait until
-- the synced rows are returned.
insert into t_i8 values (7,'[2,2,2,3]');
explain select id from t_i8 order by l2_distance(v,'[2,2,2,3]') limit 1;
-- @wait_expect(2, 60)
select id from t_i8 order by l2_distance(v,'[2,2,2,3]') limit 1;
delete from t_i8 where id = 7;
explain select id from t_i8 order by l2_distance(v,'[2.3,2,2,2]') limit 1;
-- @wait_expect(2, 60)
select id from t_i8 order by l2_distance(v,'[2.3,2,2,2]') limit 1;

insert into t_ci8 values (7,'[2,2,2,3]');
explain select id from t_ci8 order by l2_distance(v,'[2,2,2,3]') limit 1;
-- @wait_expect(2, 60)
select id from t_ci8 order by l2_distance(v,'[2,2,2,3]') limit 1;

-- an int8 index created on an empty table trains its range from the first rows synced
create table t_empty(id bigint primary key, v vecf32(4));
create index i_empty using hnsw on t_empty(v) quantization 'int8';
insert into t_empty select * from t_ref;
explain select id from t_empty order by l2_distance(v,'[2,2,2,3]') limit 3;
-- @wait_expect(2, 60)
select id from t_empty order by l2_distance(v,'[2,2,2,3]') limit 3;
select id from t_empty order by l2_distance(v,'[52,52,52,51]') limit 2;

-- ---------------- C8: an out-of-range row saturates --------------------------
-- [1000,...] is far outside [1,54]: it is stored as the top of the range, like id 6
insert into t_i8 values (8,'[1000,1000,1000,1000]');
explain select id from t_i8 order by l2_distance(v,'[1000,1000,1000,1000]') limit 2;
-- @wait_expect(2, 60)
select count(*) from (select id from t_i8 order by l2_distance(v,'[1000,1000,1000,1000]') limit 2) q where id in (6, 8);
select id, d from (select id, l2_distance(v,'[1000,1000,1000,1000]') d from t_i8 order by d limit 2) q order by id;

-- ---------------- C9: REINDEX keeps the quantization and retrains the range --
alter table t_f16 alter reindex i_f16 hnsw quantization 'bf16';
alter table t_i8 alter reindex i_i8 hnsw quantization 'uint8';
alter table t_f16 alter reindex i_f16 hnsw ef_search = 64;
show create table t_f16;
select id from t_f16 order by l2_distance(v,'[2,2,2,3]') limit 3;
alter table t_i8 alter reindex i_i8 hnsw;
show create table t_i8;
-- the range now reaches 1000: id 8 is ordered apart from id 6
-- @regex("Vector Index Scan on", true)
explain select id from t_i8 order by l2_distance(v,'[1000,1000,1000,1000]') limit 1;
select id from t_i8 order by l2_distance(v,'[1000,1000,1000,1000]') limit 1;
select id from t_i8 order by l2_distance(v,'[60,60,60,60]') limit 1;

-- ---------------- C10: copy ALTER, CLONE and TRUNCATE keep the quantization --
create table t_alt(id bigint primary key, v vecf32(4));
insert into t_alt select * from t_ref;
create index i_alt using hnsw on t_alt(v) quantization 'int8';
alter table t_alt add column z int;
show create table t_alt;
explain select id from t_alt order by l2_distance(v,'[2,2,2,3]') limit 3;
-- @wait_expect(2, 60)
select id from t_alt order by l2_distance(v,'[2,2,2,3]') limit 3;

create table t_cln clone t_u8;
show create table t_cln;
explain select id from t_cln order by l2_distance(v,'[2,2,2,3]') limit 3;
-- @wait_expect(2, 60)
select id from t_cln order by l2_distance(v,'[2,2,2,3]') limit 3;

truncate table t_u8;
insert into t_u8 select * from t_ref where id > 1;
show create table t_u8;
explain select id from t_u8 order by l2_distance(v,'[2,2,2,3]') limit 3;
-- @wait_expect(2, 60)
select id from t_u8 order by l2_distance(v,'[2,2,2,3]') limit 3;

-- ---------------- C11: the exact-search threshold ----------------------------
create table t_g(id bigint primary key, c int, v vecf32(2));
insert into t_g select result, if(result % 500 = 0, 1, 0),
    cast(concat('[', cast(result % 50 as char), ',', cast(floor(result / 50) as char), ']') as vecf32(2))
    from generate_series(1, 5000) gs;
create index i_g using hnsw on t_g(v) m = 4 ef_construction = 8 ef_search = 4;
select @@hnsw_prefilter_exact_threshold;
set hnsw_prefilter_exact_threshold = -1;
set hnsw_prefilter_exact_threshold = 1000000001;
set hnsw_prefilter_exact_threshold = 1000000000;
select @@hnsw_prefilter_exact_threshold;
set hnsw_prefilter_exact_threshold = default;
-- @regex("PrefilterExactThreshold: 5000", true)
explain verbose select id from t_g where c = 1 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
set hnsw_prefilter_exact_threshold = 0;
-- @regex("PrefilterExactThreshold", false)
explain verbose select id from t_g where c = 1 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';

-- ---------------- C12: within the threshold the prefiltered page is exact ----
select id from t_g where c = 1 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
set hnsw_prefilter_exact_threshold = default;
-- @regex("Vector Index Scan on", true)
explain select id from t_g where c = 1 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from t_g where c = 1 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from t_g where c = 1 order by l2_distance(v,'[24,51]') limit 3 by rank with option 'mode=force';
select id from t_g where c = 1 order by l2_distance(v,'[24,51]') limit 3 by rank with option 'mode=pre';
set hnsw_prefilter_exact_threshold = default;

-- ---------------- C13: only an int8 / uint8 index has a dict table ------------
select t.relname, count(*) from mo_catalog.mo_indexes i join mo_catalog.mo_tables t on i.table_id = t.rel_id
    where t.reldatabase = 'hnsw_quantization' and i.algo_table_type = 'hnsw_dict' group by t.relname order by t.relname;

-- ---------------- C15: max_index_capacity splits the index into models -------
select @@hnsw_max_index_capacity;
create table t_cap(id bigint primary key, v vecf32(4));
insert into t_cap select * from t_ref;
create index i_cap using hnsw on t_cap(v) quantization 'int8' max_index_capacity 2;
show create table t_cap;
-- @regex("Vector Index Scan on", true)
explain select id from t_cap order by l2_distance(v,'[2,2,2,3]') limit 3;
select id from t_cap order by l2_distance(v,'[2,2,2,3]') limit 3;
select id from t_cap order by l2_distance(v,'[52,52,52,51]') limit 2;
insert into t_cap values (7,'[2,2,2,3]'),(8,'[53,53,53,53]');
explain select id from t_cap order by l2_distance(v,'[2,2,2,3]') limit 1;
-- @wait_expect(2, 60)
select id from t_cap order by l2_distance(v,'[2,2,2,3]') limit 1;
select id from t_cap order by l2_distance(v,'[53,53,53,53]') limit 1;

-- ---------------- C14: an element the stored type cannot hold is never stored -----
-- CREATE INDEX fails on it, REINDEX leaves the row out of the index and records it, and a
-- search with it fails
create table t_big(id bigint primary key, v vecf32(4));
insert into t_big values (1,'[1,1,1,1]'),(2,'[100000,1,1,1]');
create index i_big using hnsw on t_big(v) quantization 'float16';
delete from t_big where id = 2;
create index i_big using hnsw on t_big(v) quantization 'float16';
insert into t_big values (2,'[100000,1,1,1]');
alter table t_big alter reindex i_big hnsw;
-- @ignore:3
show index ignored from t_big;
select id from t_f16 order by l2_distance(v,'[100000,1,1,1]') limit 1;

select mo_ctl('cn', 'SetVectorIndexFreshnessInterval', '0') not like '%failed=[%' as all_cn_restored;
drop database hnsw_quantization;
