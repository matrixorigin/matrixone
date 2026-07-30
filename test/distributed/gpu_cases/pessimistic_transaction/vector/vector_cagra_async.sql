-- =====================================================================
-- vector_cagra_async.sql — CAGRA async build + ISCP CDC INSERT/DELETE/UPDATE
--
-- GPU REQUIRED. Covers the ASYNC CREATE INDEX path: cagra_create is
-- deferred to the first CDC iteration (stashed as ConsumerInfo.InitSQL),
-- and a batch of DML flows through the ISCP CDC pipeline into the CAGRA
-- storage table's tag=1 brute-force overflow.
--
-- The model is cached in memory and only refreshed when the CDC consumer
-- catches up, so — like vector_hnsw_async — ALL of the INSERT/DELETE/
-- UPDATE ops run first, then a single SELECT SLEEP(30) lets the consumer
-- (10s sync interval) apply the whole batch, and only then do we search.
--
-- Determinism notes (CAGRA is an approximate index):
--   * Exact-match probes always return that row as top-1 — used to verify
--     INSERT and UPDATE (new vec replaces old).
--   * The deleted-sentinel probe [105,...] resolves to id=100: once id=105
--     is gone, id=100 lives in the exact brute-force overflow and is the
--     unique nearest neighbor by a wide margin, so it is stable regardless
--     of graph approximation.
--   * id=3 (deleted) is verified via COUNT(*) only — its integer neighbors
--     are L2-equidistant, so a search over them would not be reproducible.
-- =====================================================================

SET experimental_cagra_index = 1;
SET cagra_threads_build = 7;
SET cagra_max_index_capacity = 99999;

drop database if exists cagra_cdc;
create database cagra_cdc;
use cagra_cdc;

create table t (id bigint primary key, v vecf32(8));
insert into t values
    (1, '[1,1,1,1,1,1,1,1]'),  (2, '[2,2,2,2,2,2,2,2]'),
    (3, '[3,3,3,3,3,3,3,3]'),  (4, '[4,4,4,4,4,4,4,4]'),
    (5, '[5,5,5,5,5,5,5,5]'),  (6, '[6,6,6,6,6,6,6,6]'),
    (7, '[7,7,7,7,7,7,7,7]'),  (8, '[8,8,8,8,8,8,8,8]'),
    (9, '[9,9,9,9,9,9,9,9]'),  (10, '[10,10,10,10,10,10,10,10]');

-- Async build: cagra_create deferred to first CDC iteration via InitSQL.
create index ix using cagra on t (v)
    op_type 'vector_l2_ops' intermediate_graph_degree=8 graph_degree=4 ASYNC;
show create table t;

-- Batch of DML — all applied before any search. The 10 initial rows go
-- through the InitSQL build (tag=0); everything below rides the CDC tail
-- into the tag=1 overflow.
insert into t values (100, '[100,100,100,100,100,100,100,100]');
insert into t values (105, '[105,105,105,105,105,105,105,105]');
insert into t values (300, '[300,300,300,300,300,300,300,300]');
insert into t values (700, '[700,700,700,700,700,700,700,700]');
delete from t where id=105;
delete from t where id=3;
update t set v = '[500,500,500,500,500,500,500,500]' where id=5;
update t set v = '[305,305,305,305,305,305,305,305]' where id=300;
insert into t values (800, '[800,800,800,800,800,800,800,800]');

-- Single wait for the whole batch to flow through CDC.
select sleep(30);

-- Surviving inserted sentinels — exact match → that row.
select id from t order by l2_distance(v, '[100,100,100,100,100,100,100,100]') limit 1;
select id from t order by l2_distance(v, '[700,700,700,700,700,700,700,700]') limit 1;
select id from t order by l2_distance(v, '[800,800,800,800,800,800,800,800]') limit 1;

-- Updated rows — exact match on the NEW value returns the moved row.
select id from t order by l2_distance(v, '[500,500,500,500,500,500,500,500]') limit 1;
select id from t order by l2_distance(v, '[305,305,305,305,305,305,305,305]') limit 1;

-- Deleted sentinel — probe its old value; id=105 is gone so the unique
-- nearest survivor id=100 (exact overflow) comes back instead.
select id from t order by l2_distance(v, '[105,105,105,105,105,105,105,105]') limit 1;

-- Storage layout: tag=0 (model from initial build) + tag=1 (CDC overflow).
set @stbl = (select index_table_name from mo_catalog.mo_indexes
    where table_id=(select rel_id from mo_catalog.mo_tables
                    where relname='t' and reldatabase='cagra_cdc')
      and name='ix' and algo_table_type='cagra_index');
set @q = concat('select distinct tag from `', @stbl, '` order by tag');
prepare s from @q; execute s; deallocate prepare s;

-- Row count: 10 initial + 5 inserts (100,105,300,700,800) - 2 deletes
-- (105,3) = 13. Confirms id=3 and id=105 are gone.
select count(*) from t;

drop database cagra_cdc;
