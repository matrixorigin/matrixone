-- GPU REQUIRED. Rows a CAGRA or IVF-PQ index cannot store (#31583). Each section proves the
-- claim of docs/design/20261028-index-ignored-rows.md it is headed by.
SET experimental_cagra_index = 1;
SET experimental_ivfpq_index = 1;
SET cagra_threads_build = 7;
SET ivfpq_threads_build = 6;
SET cagra_max_index_capacity = 99999;
SET ivfpq_max_index_capacity = 99999;
SET kmeans_train_percent = 100;
SET probe_limit = 16;
select mo_ctl('cn', 'SetVectorIndexFreshnessInterval', '1s') not like '%failed=[%' as all_cn_updated;

-- =====================================================================
-- cagra: rows the index cannot store
-- =====================================================================
drop database if exists cagra_ignored;
create database cagra_ignored;
use cagra_ignored;
create table t(id bigint primary key, v vecf32(8), c int);
insert into t select result, cast(concat('[', result%17, ',', result%13, ',', result%11, ',', result%7, ',', result%5, ',', result%3, ',', result%19, ',', result%23, ']') as vecf32(8)), result from generate_series(1,300) g;
insert into t values (9999,'[100000,1,1,1,1,1,1,1]',9999);

-- ---------------- I11: what the index cannot store -----
-- float16 cannot hold 100000: by default the build fails and leaves no index
create index ix using cagra on t(v) op_type 'vector_l2_ops' intermediate_graph_degree=16 graph_degree=8 itopk_size=32 quantization 'float16';
select count(*) from mo_catalog.mo_indexes where name = 'ix' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = 'cagra_ignored' and relname = 't');
-- float32 and int8 storage hold it
create index ix using cagra on t(v) op_type 'vector_l2_ops' intermediate_graph_degree=16 graph_degree=8 itopk_size=32;
show index ignored from t;
drop index ix on t;
create index ix using cagra on t(v) op_type 'vector_l2_ops' intermediate_graph_degree=16 graph_degree=8 itopk_size=32 quantization 'int8';
show index ignored from t;
drop index ix on t;
-- a vector whose L2 distances overflow float32 cannot be stored by any storage type
create table n(id bigint primary key, v vecf32(8));
insert into n select id, v from t where id <= 300;
insert into n values (9999,'[1e20,1,1,1,1,1,1,1]');
create index ixn using cagra on n(v) op_type 'vector_l2_ops' intermediate_graph_degree=16 graph_degree=8 itopk_size=32;
create index ixn using cagra on n(v) op_type 'vector_l2_ops' intermediate_graph_degree=16 graph_degree=8 itopk_size=32 max_bad_records 1;
-- @ignore:4
show index ignored from n;
select id from n order by l2_distance(v, '[1,1,1,1,1,1,1,1]') limit 1;
-- a vecf16 base holds no value either rule rejects
create table h(id bigint primary key, v vecf16(8));
insert into h select id, cast(v as vecf16(8)) from t where id <= 300;
insert into h values (9999,'[65000,1,1,1,1,1,1,1]');
create index ixh using cagra on h(v) op_type 'vector_l2_ops' intermediate_graph_degree=16 graph_degree=8 itopk_size=32;
show index ignored from h;

-- ---------------- I12: the limit, as for HNSW -----
-- within the limit the row is left out and recorded, and the index answers
create index ix using cagra on t(v) op_type 'vector_l2_ops' intermediate_graph_degree=16 graph_degree=8 itopk_size=32 quantization 'float16' max_bad_records 1;
-- @ignore:4
show index ignored from t;
select id from t order by l2_distance(v, '[1,1,1,1,1,1,1,1]') limit 1;
-- one row more through incremental sync stops the index
insert into t values (10000,'[1,-90000,1,1,1,1,1,1]',10000);
-- @ignore:6
-- @wait_expect(2, 60)
show index status from t;
-- ALTER REINDEX with a higher limit rebuilds the index and restarts it
alter table t alter reindex ix cagra max_bad_records = 10;
-- @ignore:4
-- @wait_expect(2, 60)
show index ignored from t;
-- @ignore:6
-- @wait_expect(2, 60)
show index status from t;
-- an ignored row updated to a storable value, and one deleted, leave the ignored rows
update t set v = '[2,2,2,2,2,2,2,2]' where id = 9999;
delete from t where id = 10000;
-- @wait_expect(2, 60)
show index ignored from t;
-- @wait_expect(2, 60)
select id from t order by l2_distance(v, '[2,2,2,2,2,2,2,2]') limit 2;
-- a build that can store none of its rows fails within the limit
create table z(id bigint primary key, v vecf32(8));
insert into z values (1,'[100000,1,1,1,1,1,1,1]'),(2,'[1,100000,1,1,1,1,1,1]');
create index iz using cagra on z(v) op_type 'vector_l2_ops' intermediate_graph_degree=16 graph_degree=8 itopk_size=32 quantization 'float16' max_bad_records 10;
-- an asynchronous build records within the limit, and stops past it
create table a(id bigint primary key, v vecf32(8));
insert into a select id, v from n where id <= 300;
insert into a values (9999,'[100000,1,1,1,1,1,1,1]');
create index ia using cagra on a(v) op_type 'vector_l2_ops' intermediate_graph_degree=16 graph_degree=8 itopk_size=32 quantization 'float16' max_bad_records 1 async;
-- @ignore:4
-- @wait_expect(2, 60)
show index ignored from a;
create table b(id bigint primary key, v vecf32(8));
insert into b select id, v from a;
create index ib using cagra on b(v) op_type 'vector_l2_ops' intermediate_graph_degree=16 graph_degree=8 itopk_size=32 quantization 'float16' async;
-- @ignore:6
-- @wait_expect(2, 60)
show index status from b;

-- ---------------- I13: left-out rows never give cuVS a sub-index it cannot build -----
-- 215 rows and 3 left out: the count plans 100 + 100 + 18 rows, but only 15 of
-- the last sub-index can be stored, fewer than cuVS builds; those go to the brute-force tail
create table s(id bigint primary key, v vecf32(8), c int);
insert into s select id, v, c from t where id <= 215;
insert into s values (5001,'[100000,1,1,1,1,1,1,1]',5001),(5002,'[100000,2,1,1,1,1,1,1]',5002),(5003,'[100000,3,1,1,1,1,1,1]',5003);
create index isx using cagra on s(v) op_type 'vector_l2_ops' intermediate_graph_degree=16 graph_degree=8 itopk_size=32 max_index_capacity 100 quantization 'float16' max_bad_records 3 include (c);
-- @ignore:4
show index ignored from s;
select id from s order by l2_distance(v, (select v from s where id = 215)) limit 1;
select id from s where c = 105 order by l2_distance(v, (select v from s where id = 105)) limit 1;
select id from s where c = 215 order by l2_distance(v, (select v from s where id = 215)) limit 1;

-- ---------------- I9: a restore follows the limit of the index it restores -----
-- t is within its limit, b is stopped by it
insert into t values (10000,'[100000,9,1,1,1,1,1,1]',10000);
-- @ignore:4
-- @wait_expect(2, 60)
show index ignored from t;
create table tc clone t;
create table bc clone b;
-- @ignore:4
-- @wait_expect(2, 60)
show index ignored from tc;
-- @wait_expect(2, 60)
select id from tc order by l2_distance(v, '[1,1,1,1,1,1,1,1]') limit 1;
-- @ignore:6
-- @wait_expect(2, 60)
show index status from bc;

drop database cagra_ignored;

-- =====================================================================
-- ivfpq: rows the index cannot store
-- =====================================================================
drop database if exists ivfpq_ignored;
create database ivfpq_ignored;
use ivfpq_ignored;
create table t(id bigint primary key, v vecf32(8), c int);
insert into t select result, cast(concat('[', result%17, ',', result%13, ',', result%11, ',', result%7, ',', result%5, ',', result%3, ',', result%19, ',', result%23, ']') as vecf32(8)), result from generate_series(1,300) g;
insert into t values (9999,'[100000,1,1,1,1,1,1,1]',9999);

-- ---------------- I11: what the index cannot store -----
-- float16 cannot hold 100000: by default the build fails and leaves no index
create index ix using ivfpq on t(v) op_type 'vector_l2_ops' lists=4 m=8 bits_per_code=8 quantization 'float16';
select count(*) from mo_catalog.mo_indexes where name = 'ix' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = 'ivfpq_ignored' and relname = 't');
-- float32 and int8 storage hold it
create index ix using ivfpq on t(v) op_type 'vector_l2_ops' lists=4 m=8 bits_per_code=8;
show index ignored from t;
drop index ix on t;
create index ix using ivfpq on t(v) op_type 'vector_l2_ops' lists=4 m=8 bits_per_code=8 quantization 'int8';
show index ignored from t;
drop index ix on t;
-- a vector whose L2 distances overflow float32 cannot be stored by any storage type
create table n(id bigint primary key, v vecf32(8));
insert into n select id, v from t where id <= 300;
insert into n values (9999,'[1e20,1,1,1,1,1,1,1]');
create index ixn using ivfpq on n(v) op_type 'vector_l2_ops' lists=4 m=8 bits_per_code=8;
create index ixn using ivfpq on n(v) op_type 'vector_l2_ops' lists=4 m=8 bits_per_code=8 max_bad_records 1;
-- @ignore:4
show index ignored from n;
select id from n order by l2_distance(v, '[1,1,1,1,1,1,1,1]') limit 1;
-- a vecf16 base holds no value either rule rejects
create table h(id bigint primary key, v vecf16(8));
insert into h select id, cast(v as vecf16(8)) from t where id <= 300;
insert into h values (9999,'[65000,1,1,1,1,1,1,1]');
create index ixh using ivfpq on h(v) op_type 'vector_l2_ops' lists=4 m=8 bits_per_code=8;
show index ignored from h;

-- ---------------- I12: the limit, as for HNSW -----
-- within the limit the row is left out and recorded, and the index answers
create index ix using ivfpq on t(v) op_type 'vector_l2_ops' lists=4 m=8 bits_per_code=8 quantization 'float16' max_bad_records 1;
-- @ignore:4
show index ignored from t;
select id from t order by l2_distance(v, '[1,1,1,1,1,1,1,1]') limit 1;
-- one row more through incremental sync stops the index
insert into t values (10000,'[1,-90000,1,1,1,1,1,1]',10000);
-- @ignore:6
-- @wait_expect(2, 60)
show index status from t;
-- ALTER REINDEX with a higher limit rebuilds the index and restarts it
alter table t alter reindex ix ivfpq max_bad_records = 10;
-- @ignore:4
-- @wait_expect(2, 60)
show index ignored from t;
-- @ignore:6
-- @wait_expect(2, 60)
show index status from t;
-- an ignored row updated to a storable value, and one deleted, leave the ignored rows
update t set v = '[2,2,2,2,2,2,2,2]' where id = 9999;
delete from t where id = 10000;
-- @wait_expect(2, 60)
show index ignored from t;
-- @wait_expect(2, 60)
select id from t order by l2_distance(v, '[2,2,2,2,2,2,2,2]') limit 2;
-- a build that can store none of its rows fails within the limit
create table z(id bigint primary key, v vecf32(8));
insert into z values (1,'[100000,1,1,1,1,1,1,1]'),(2,'[1,100000,1,1,1,1,1,1]');
create index iz using ivfpq on z(v) op_type 'vector_l2_ops' lists=4 m=8 bits_per_code=8 quantization 'float16' max_bad_records 10;
-- an asynchronous build records within the limit, and stops past it
create table a(id bigint primary key, v vecf32(8));
insert into a select id, v from n where id <= 300;
insert into a values (9999,'[100000,1,1,1,1,1,1,1]');
create index ia using ivfpq on a(v) op_type 'vector_l2_ops' lists=4 m=8 bits_per_code=8 quantization 'float16' max_bad_records 1 async;
-- @ignore:4
-- @wait_expect(2, 60)
show index ignored from a;
create table b(id bigint primary key, v vecf32(8));
insert into b select id, v from a;
create index ib using ivfpq on b(v) op_type 'vector_l2_ops' lists=4 m=8 bits_per_code=8 quantization 'float16' async;
-- @ignore:6
-- @wait_expect(2, 60)
show index status from b;

-- ---------------- I13: left-out rows never give cuVS a sub-index it cannot build -----
-- 202 rows and 3 left out: the count plans 100 + 100 + 5 rows, but only 2 of
-- the last sub-index can be stored, fewer than cuVS builds; those go to the brute-force tail
create table s(id bigint primary key, v vecf32(8), c int);
insert into s select id, v, c from t where id <= 202;
insert into s values (5001,'[100000,1,1,1,1,1,1,1]',5001),(5002,'[100000,2,1,1,1,1,1,1]',5002),(5003,'[100000,3,1,1,1,1,1,1]',5003);
create index isx using ivfpq on s(v) op_type 'vector_l2_ops' lists=4 m=8 bits_per_code=8 max_index_capacity 100 quantization 'float16' max_bad_records 3 include (c);
-- @ignore:4
show index ignored from s;
select id from s order by l2_distance(v, (select v from s where id = 202)) limit 1;
select id from s where c = 102 order by l2_distance(v, (select v from s where id = 102)) limit 1;
select id from s where c = 202 order by l2_distance(v, (select v from s where id = 202)) limit 1;

-- ---------------- I9: a restore follows the limit of the index it restores -----
-- t is within its limit, b is stopped by it
insert into t values (10000,'[100000,9,1,1,1,1,1,1]',10000);
-- @ignore:4
-- @wait_expect(2, 60)
show index ignored from t;
create table tc clone t;
create table bc clone b;
-- @ignore:4
-- @wait_expect(2, 60)
show index ignored from tc;
-- @wait_expect(2, 60)
select id from tc order by l2_distance(v, '[1,1,1,1,1,1,1,1]') limit 1;
-- @ignore:6
-- @wait_expect(2, 60)
show index status from bc;

drop database ivfpq_ignored;
