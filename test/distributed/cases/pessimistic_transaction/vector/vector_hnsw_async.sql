
SET experimental_hnsw_index = 1;

drop database if exists hnsw_cdc;
create database if not exists hnsw_cdc;
use hnsw_cdc;

-- Empty-index first-generation replay. Admit INSERT -> UPDATE on one PK and
-- INSERT -> DELETE on another immediately after CREATE INDEX, before observing
-- any ready generation. This retains the original race-sensitive lifecycle
-- without a sleep; metadata publication is the barrier and exact ANN results
-- reject a generation that lost or reordered either CDC transition.
create table t0_f64(a bigint primary key, b vecf64(3));
create index idx0_f64 using hnsw on t0_f64(b) op_type "vector_l2_ops" M 64 EF_CONSTRUCTION 200 EF_SEARCH 200 ASYNC;
insert into t0_f64 values (0, '[1,2,3]');
update t0_f64 set b = '[4,5,6]' where a = 0;
insert into t0_f64 values (1, '[2,3,4]');
delete from t0_f64 where a = 1;
insert into t0_f64 values (2, '[100,100,100]');

set @t0_f64_meta = (select index_table_name from mo_catalog.mo_indexes where name = 'idx0_f64' and algo = 'hnsw' and algo_table_type = 'hnsw_meta' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't0_f64') limit 1);
set @wait_t0_sql = concat('select count(*) = 1 and min(filesize) > 0 as f64_empty_ready from `', database(), '`.`', @t0_f64_meta, '`');
prepare wait_t0 from @wait_t0_sql;
-- @metacmp(false)
-- @wait_expect(1, 120)
execute wait_t0;
deallocate prepare wait_t0;

select case_name, a, b from ( select 'f64-empty-update' as case_name, a, b from (select * from t0_f64 order by l2_distance(b, '[4,5,6]') limit 1) q union all select 'f64-empty-delete', a, b from (select * from t0_f64 order by l2_distance(b, '[2,3,4]') limit 1) q union all select 'f64-empty-insert', a, b from (select * from t0_f64 order by l2_distance(b, '[100,100,100]') limit 1) q ) readiness order by case_name;
drop table t0_f64;

-- f32 and f64 exercise distinct HNSW type paths while sharing each
-- readiness barrier. Build a baseline generation first, then make INSERT,
-- UPDATE, and DELETE one committed CDC delta for both consumers.
create table t1_f32(a bigint primary key, b vecf32(3), c int, key c_k(c));
create table t1_f64(a bigint primary key, b vecf64(3), c int, key c_k(c));
insert into t1_f32 values (0, '[1,2,3]', 1), (1, '[2,3,4]', 1), (2, '[100,100,100]', 2);
insert into t1_f64 values (0, '[1,2,3]', 1), (1, '[2,3,4]', 1), (2, '[100,100,100]', 2);
create index idx_f32 using hnsw on t1_f32(b) op_type "vector_l2_ops" M 64 EF_CONSTRUCTION 200 EF_SEARCH 200 ASYNC;
create index idx_f64 using hnsw on t1_f64(b) op_type "vector_l2_ops" M 64 EF_CONSTRUCTION 200 EF_SEARCH 200 ASYNC;

set @t1_f32_meta = (select index_table_name from mo_catalog.mo_indexes where name = 'idx_f32' and algo = 'hnsw' and algo_table_type = 'hnsw_meta' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't1_f32') limit 1);
set @t1_f64_meta = (select index_table_name from mo_catalog.mo_indexes where name = 'idx_f64' and algo = 'hnsw' and algo_table_type = 'hnsw_meta' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't1_f64') limit 1);
set @wait_t1_initial_sql = concat('select (select count(*) from `', database(), '`.`', @t1_f32_meta, '`) = 1 as f32_ready, (select count(*) from `', database(), '`.`', @t1_f64_meta, '`) = 1 as f64_ready');
prepare wait_t1_initial from @wait_t1_initial_sql;
-- @metacmp(false)
-- @wait_expect(2, 120)
execute wait_t1_initial;
deallocate prepare wait_t1_initial;

set @capture_t1_sql = concat('select (select checksum from `', database(), '`.`', @t1_f32_meta, '` limit 1), (select checksum from `', database(), '`.`', @t1_f64_meta, '` limit 1) into @t1_f32_before, @t1_f64_before');
prepare capture_t1 from @capture_t1_sql;
execute capture_t1;
deallocate prepare capture_t1;

start transaction;
insert into t1_f32 values (3, '[500,500,500]', 3);
insert into t1_f64 values (3, '[500,500,500]', 3);
delete from t1_f32 where a = 1;
delete from t1_f64 where a = 1;
update t1_f32 set b = '[1000,1000,1000]' where a = 0;
update t1_f64 set b = '[1000,1000,1000]' where a = 0;
commit;

-- A changed metadata generation proves the single committed delta (and thus
-- all three mutation kinds) reached each consumer before any HNSW model is read.
set @wait_t1_delta_sql = concat('select (select checksum <> @t1_f32_before from `', database(), '`.`', @t1_f32_meta, '` limit 1) as f32_ready, (select checksum <> @t1_f64_before from `', database(), '`.`', @t1_f64_meta, '` limit 1) as f64_ready');
prepare wait_t1_delta from @wait_t1_delta_sql;
-- @metacmp(false)
-- @wait_expect(2, 120)
execute wait_t1_delta;
deallocate prepare wait_t1_delta;

-- Exact INSERT/UPDATE/DELETE oracles for both types. The surviving far row
-- makes stale DELETE and UPDATE generations fail rather than merely joining a
-- stale pk back to the current base-table value.
select case_name, a, b, c from ( select 'f32-update' as case_name, a, b, c from (select * from t1_f32 order by l2_distance(b, '[1000,1000,1000]') limit 1) q union all select 'f32-delete', a, b, c from (select * from t1_f32 order by l2_distance(b, '[2,3,4]') limit 1) q union all select 'f32-insert', a, b, c from (select * from t1_f32 order by l2_distance(b, '[500,500,500]') limit 1) q union all select 'f64-update', a, b, c from (select * from t1_f64 order by l2_distance(b, '[1000,1000,1000]') limit 1) q union all select 'f64-delete', a, b, c from (select * from t1_f64 order by l2_distance(b, '[2,3,4]') limit 1) q union all select 'f64-insert', a, b, c from (select * from t1_f64 order by l2_distance(b, '[500,500,500]') limit 1) q ) readiness order by case_name;

drop table t1_f32;
drop table t1_f64;

-- Keep the 128-dimensional f64 path at its minimum two endpoint rows. The f32
-- path retains the bulk LOAD lifecycle needed by issue #22794: build on the
-- first 10k snapshot, then merge a second 10k CDC generation. This removes a
-- redundant f64 10k build while preserving type, dimension, and endpoint
-- coverage.
create table t2_f32(a bigint primary key, b vecf32(128));
create table t2_f64(a bigint primary key, b vecf64(128));
create index idx2_f64 using hnsw on t2_f64(b) op_type "vector_l2_ops" M 64 EF_CONSTRUCTION 200 EF_SEARCH 200 ASYNC;

load data infile {'filepath'='$resources/vector/sift128_base_10k.csv.gz', 'compression'='gzip'} into table t2_f32 fields terminated by ':' parallel 'true';
insert into t2_f64 select a, cast(b as vecf64(128)) from t2_f32 where a in (0, 9999);
create index idx2_f32 using hnsw on t2_f32(b) op_type "vector_l2_ops" M 64 EF_CONSTRUCTION 200 EF_SEARCH 200 ASYNC;

set @t2_f32_meta = (select index_table_name from mo_catalog.mo_indexes where name = 'idx2_f32' and algo = 'hnsw' and algo_table_type = 'hnsw_meta' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't2_f32') limit 1);
set @t2_f64_meta = (select index_table_name from mo_catalog.mo_indexes where name = 'idx2_f64' and algo = 'hnsw' and algo_table_type = 'hnsw_meta' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't2_f64') limit 1);
select (select count(*) from t2_f32) as f32_rows, (select count(*) from t2_f64) as f64_rows;

-- Random 128-dimensional 10k models are materially larger than 100 KiB. This
-- deterministic lower bound rejects an absent or empty generation without
-- depending on the exact serialized size.
set @wait_t2_build_sql = concat('select (select count(*) = 1 and min(filesize) > 100000 from `', database(), '`.`', @t2_f32_meta, '`) as f32_ready, (select count(*) = 1 and min(filesize) > 0 from `', database(), '`.`', @t2_f64_meta, '`) as f64_ready');
prepare wait_t2_build from @wait_t2_build_sql;
-- @metacmp(false)
-- @wait_expect(2, 120)
execute wait_t2_build;
deallocate prepare wait_t2_build;

-- Read the f64 model after its generation is visible. Keep the f32 model cache
-- cold until its second generation is complete: cross-CN cache refresh is
-- intentionally eventual, so warming the 10k model would make the later 20k
-- oracle observe that stale model for the cache TTL rather than test CDC.
select case_name, a, b from ( select 'f64-last' as case_name, a, b from (select * from t2_f64 order by l2_distance(b, "[14, 2, 0, 0, 0, 2, 42, 55, 9, 1, 0, 0, 18, 100, 77, 32, 89, 1, 0, 0, 19, 85, 15, 68, 52, 4, 0, 0, 0, 0, 2, 28, 34, 13, 5, 12, 49, 40, 39, 37, 24, 2, 0, 0, 34, 83, 88, 28, 119, 20, 0, 0, 41, 39, 13, 62, 119, 16, 2, 0, 0, 0, 10, 42, 9, 46, 82, 79, 64, 19, 2, 5, 10, 35, 26, 53, 84, 32, 34, 9, 119, 119, 21, 3, 3, 11, 17, 14, 119, 25, 8, 5, 0, 0, 11, 22, 23, 17, 42, 49, 17, 12, 5, 5, 12, 78, 119, 90, 27, 0, 4, 2, 48, 92, 112, 85, 15, 0, 2, 7, 50, 36, 15, 11, 1, 0, 0, 7]") limit 1) q union all select 'f64-first', a, b from (select * from t2_f64 order by l2_distance(b, "[0, 16, 35, 5, 32, 31, 14, 10, 11, 78, 55, 10, 45, 83, 11, 6, 14, 57, 102, 75, 20, 8, 3, 5, 67, 17, 19, 26, 5, 0, 1, 22, 60, 26, 7, 1, 18, 22, 84, 53, 85, 119, 119, 4, 24, 18, 7, 7, 1, 81, 106, 102, 72, 30, 6, 0, 9, 1, 9, 119, 72, 1, 4, 33, 119, 29, 6, 1, 0, 1, 14, 52, 119, 30, 3, 0, 0, 55, 92, 111, 2, 5, 4, 9, 22, 89, 96, 14, 1, 0, 1, 82, 59, 16, 20, 5, 25, 14, 11, 4, 0, 0, 1, 26, 47, 23, 4, 0, 0, 4, 38, 83, 30, 14, 9, 4, 9, 17, 23, 41, 0, 0, 2, 8, 19, 25, 23, 1]") limit 1) q ) readiness order by case_name;

-- Regression for closed issue #22794: update an already materialized f32 model
-- with a second bulk CDC load, then prove both the generation transition and
-- the endpoints that exist only in the delta dataset.
set @capture_t2_f32_sql = concat('select checksum into @t2_f32_before from `', database(), '`.`', @t2_f32_meta, '` limit 1');
prepare capture_t2_f32 from @capture_t2_f32_sql;
execute capture_t2_f32;
deallocate prepare capture_t2_f32;

load data infile {'filepath'='$resources/vector/sift128_base_10k_2.csv.gz', 'compression'='gzip'} into table t2_f32 fields terminated by ':' parallel 'true';
select count(*) as f32_rows_after_delta from t2_f32;

set @wait_t2_f32_delta_sql = concat('select checksum <> @t2_f32_before as f32_delta_ready from `', database(), '`.`', @t2_f32_meta, '` limit 1');
prepare wait_t2_f32_delta from @wait_t2_f32_delta_sql;
-- @metacmp(false)
-- @wait_expect(1, 120)
execute wait_t2_f32_delta;
deallocate prepare wait_t2_f32_delta;

-- A generation can become visible before every parallel CDC fragment has
-- joined the model. Poll the black-box endpoint oracle as the completion gate.
-- @wait_expect(2, 120)
select case_name, a from ( select 'f32-delta-first' as case_name, a from (select a from t2_f32 order by l2_distance(b, "[59, 0, 0, 1, 1, 1, 5, 100, 41, 0, 0, 4, 57, 34, 31, 115, 4, 0, 0, 12, 30, 33, 43, 85, 21, 0, 0, 14, 25, 9, 10, 60, 99, 11, 0, 0, 0, 0, 10, 55, 68, 1, 0, 3, 115, 65, 42, 115, 32, 3, 0, 4, 13, 21, 104, 115, 81, 15, 15, 23, 9, 2, 21, 75, 43, 20, 1, 0, 10, 2, 2, 20, 52, 35, 32, 61, 79, 8, 7, 41, 50, 106, 96, 20, 8, 2, 11, 39, 115, 48, 53, 11, 3, 0, 2, 43, 35, 11, 0, 1, 13, 7, 0, 1, 115, 58, 54, 29, 1, 2, 0, 3, 32, 115, 99, 34, 1, 0, 0, 0, 35, 15, 52, 44, 9, 0, 0, 18]") limit 1) q union all select 'f32-delta-last', a from (select a from t2_f32 order by l2_distance(b, "[0, 0, 0, 0, 0, 101, 82, 4, 2, 0, 0, 0, 3, 133, 133, 8, 46, 1, 2, 13, 15, 29, 87, 50, 22, 1, 0, 16, 25, 6, 18, 49, 5, 2, 0, 2, 3, 59, 70, 19, 18, 2, 0, 11, 42, 37, 30, 13, 133, 13, 4, 53, 28, 3, 8, 42, 77, 6, 11, 103, 36, 0, 0, 32, 7, 15, 59, 27, 2, 0, 2, 5, 14, 5, 55, 52, 51, 3, 2, 5, 133, 21, 10, 38, 26, 1, 0, 64, 71, 3, 10, 118, 53, 5, 6, 28, 33, 26, 73, 15, 0, 0, 0, 22, 13, 15, 133, 133, 4, 0, 0, 15, 107, 62, 46, 91, 9, 1, 7, 16, 28, 4, 0, 27, 33, 4, 15, 25]") limit 1) q ) readiness order by case_name;

-- Once the delta generation is visible, prove it retained both endpoints from
-- the original 10k model as well as admitting the two delta endpoints above.
-- @wait_expect(2, 120)
select case_name, a from ( select 'f32-base-first-after-delta' as case_name, a from (select a from t2_f32 order by l2_distance(b, "[0, 16, 35, 5, 32, 31, 14, 10, 11, 78, 55, 10, 45, 83, 11, 6, 14, 57, 102, 75, 20, 8, 3, 5, 67, 17, 19, 26, 5, 0, 1, 22, 60, 26, 7, 1, 18, 22, 84, 53, 85, 119, 119, 4, 24, 18, 7, 7, 1, 81, 106, 102, 72, 30, 6, 0, 9, 1, 9, 119, 72, 1, 4, 33, 119, 29, 6, 1, 0, 1, 14, 52, 119, 30, 3, 0, 0, 55, 92, 111, 2, 5, 4, 9, 22, 89, 96, 14, 1, 0, 1, 82, 59, 16, 20, 5, 25, 14, 11, 4, 0, 0, 1, 26, 47, 23, 4, 0, 0, 4, 38, 83, 30, 14, 9, 4, 9, 17, 23, 41, 0, 0, 2, 8, 19, 25, 23, 1]") limit 1) q union all select 'f32-base-last-after-delta', a from (select a from t2_f32 order by l2_distance(b, "[14, 2, 0, 0, 0, 2, 42, 55, 9, 1, 0, 0, 18, 100, 77, 32, 89, 1, 0, 0, 19, 85, 15, 68, 52, 4, 0, 0, 0, 0, 2, 28, 34, 13, 5, 12, 49, 40, 39, 37, 24, 2, 0, 0, 34, 83, 88, 28, 119, 20, 0, 0, 41, 39, 13, 62, 119, 16, 2, 0, 0, 0, 10, 42, 9, 46, 82, 79, 64, 19, 2, 5, 10, 35, 26, 53, 84, 32, 34, 9, 119, 119, 21, 3, 3, 11, 17, 14, 119, 25, 8, 5, 0, 0, 11, 22, 23, 17, 42, 49, 17, 12, 5, 5, 12, 78, 119, 90, 27, 0, 4, 2, 48, 92, 112, 85, 15, 0, 2, 7, 50, 36, 15, 11, 1, 0, 0, 7]") limit 1) q ) retained order by case_name;

drop table t2_f32;
drop table t2_f64;

-- end t2


drop database hnsw_cdc;
