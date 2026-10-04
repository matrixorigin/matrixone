-- =====================================================================
-- vector_cagra_f16_async.sql — CAGRA vecf16 base + ISCP CDC INSERT/DELETE/UPDATE
--
-- GPU REQUIRED. The vecf16 twin of vector_cagra_async.sql. It proves the
-- ongoing CDC ingestion path carries a vecf16 base column NATIVELY (2 bytes
-- per element, no f32 widening): the iscp CuvsCdcWriter extracts the source
-- column as []types.Float16, encodes each event record at 2*dim bytes, and
-- CagraSync.AppendRecords steps the stream by that same width; the search-side
-- replayEventChunks[cuvs.Float16] then reinterprets the bytes back to half and
-- feeds the f16 brute-force overflow.
--
-- ASYNC CREATE INDEX: cagra_create is deferred to the first CDC iteration
-- (stashed as ConsumerInfo.InitSQL). The 10 initial rows build the tag=0
-- model (native half); every DML below rides the CDC tail into the tag=1
-- overflow. Like vector_cagra_async / vector_hnsw_async, ALL ops run first,
-- then a single SELECT SLEEP(30) lets the 10s-interval consumer apply the
-- whole batch, and only then do we search.
--
-- The query literal is cast to vecf16(8) so the native half query path is
-- exercised (CagraSearch over base B == cuvs.Float16).
--
-- Determinism notes (CAGRA is an approximate index; values are exact in f16):
--   * Every sentinel value (100..800, 305, 500, 105) is an integer < 2048, so
--     it is represented exactly in float16 — no rounding ambiguity.
--   * Exact-match probes always return that row as top-1 — verifies INSERT and
--     UPDATE (new vec replaces old).
--   * The deleted-sentinel probe [105,...] resolves to id=100: once id=105 is
--     gone, id=100 lives in the exact f16 overflow and is the unique nearest
--     neighbor by a wide margin, stable regardless of graph approximation.
--   * id=3 (deleted) is verified via COUNT(*) only — its integer neighbors are
--     L2-equidistant, so a search over them would not be reproducible.
-- =====================================================================

SET experimental_cagra_index = 1;
SET cagra_threads_build = 7;
SET cagra_max_index_capacity = 99999;

drop database if exists cagra_f16_cdc;
create database cagra_f16_cdc;
use cagra_f16_cdc;

create table t (id bigint primary key, v vecf16(8));
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
-- through the InitSQL build (tag=0); everything below rides the f16 CDC tail
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
select id from t order by l2_distance(v, cast('[100,100,100,100,100,100,100,100]' as vecf16(8))) limit 1;
select id from t order by l2_distance(v, cast('[700,700,700,700,700,700,700,700]' as vecf16(8))) limit 1;
select id from t order by l2_distance(v, cast('[800,800,800,800,800,800,800,800]' as vecf16(8))) limit 1;

-- Updated rows — exact match on the NEW value returns the moved row.
select id from t order by l2_distance(v, cast('[500,500,500,500,500,500,500,500]' as vecf16(8))) limit 1;
select id from t order by l2_distance(v, cast('[305,305,305,305,305,305,305,305]' as vecf16(8))) limit 1;

-- Deleted sentinel — probe its old value; id=105 is gone so the unique
-- nearest survivor id=100 (exact f16 overflow) comes back instead.
select id from t order by l2_distance(v, cast('[105,105,105,105,105,105,105,105]' as vecf16(8))) limit 1;

-- Storage layout: tag=0 (model from initial build) + tag=1 (CDC overflow).
set @stbl = (select index_table_name from mo_catalog.mo_indexes
    where table_id=(select rel_id from mo_catalog.mo_tables
                    where relname='t' and reldatabase='cagra_f16_cdc')
      and name='ix' and algo_table_type='cagra_index');
set @q = concat('select distinct tag from `', @stbl, '` order by tag');
prepare s from @q; execute s; deallocate prepare s;

-- Row count: 10 initial + 5 inserts (100,105,300,700,800) - 2 deletes
-- (105,3) = 13. Confirms id=3 and id=105 are gone.
select count(*) from t;

drop database cagra_f16_cdc;
