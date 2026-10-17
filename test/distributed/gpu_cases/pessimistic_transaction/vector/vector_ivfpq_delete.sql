-- =====================================================================
-- vector_ivfpq_delete.sql — IVFPQ soft-delete: search excludes deleted rows
--
-- GPU REQUIRED. Builds a sync IVFPQ index, deletes a row, waits for the
-- committed CDC tail, then uses one bounded exact-match readiness query to
-- observe the delete in the index's per-device deleted bitset. The original
-- searches then confirm the deleted row is excluded and the next survivor is
-- returned.
-- Lives under pessimistic_transaction/ because, like the async cases, it
-- depends on CDC catch-up.
--
-- Data: id=i -> [v]*8 with v doubling (10,20,40,...,5120) so a deleted row has a
-- UNIQUE nearest survivor (no equidistant tie). Delete id=5 ([160]*8):
--   * query [160]*8  -> id 4 ([80], the unique nearest survivor; id 6 [320] is farther)
--   * query [1280]*8 -> id 8 (untouched row still found)
-- =====================================================================

SET experimental_ivfpq_index = 1;
SET ivfpq_threads_build = 6;
SET ivfpq_max_index_capacity = 99999;
SET kmeans_train_percent = 100;
SET probe_limit = 16;

drop database if exists ivfpq_delete;
create database ivfpq_delete;
use ivfpq_delete;

create table t (id bigint primary key, v vecf32(8));
insert into t values
    (1, '[10,10,10,10,10,10,10,10]'),     (2, '[20,20,20,20,20,20,20,20]'),
    (3, '[40,40,40,40,40,40,40,40]'),     (4, '[80,80,80,80,80,80,80,80]'),
    (5, '[160,160,160,160,160,160,160,160]'),     (6, '[320,320,320,320,320,320,320,320]'),
    (7, '[640,640,640,640,640,640,640,640]'),     (8, '[1280,1280,1280,1280,1280,1280,1280,1280]'),
    (9, '[2560,2560,2560,2560,2560,2560,2560,2560]'),     (10, '[5120,5120,5120,5120,5120,5120,5120,5120]');

create index ix using ivfpq on t (v) op_type 'vector_l2_ops' lists=2 m=8 bits_per_code=8;

-- Baseline: the exact row is the top-1 before deletion.
select id from t order by l2_distance(v, '[160,160,160,160,160,160,160,160]') asc limit 1;

-- Capture the committed tail generation before the delete. The first readiness
-- gate below must observe a newer chunk before any vector probe can run.
set @stbl = (
    select index_table_name from mo_catalog.mo_indexes
    where name = 'ix' and algo = 'ivfpq' and algo_table_type = 'ivfpq_index'
      and table_id in (
          select rel_id from mo_catalog.mo_tables
          where reldatabase = database() and relname = 't'
      )
    limit 1
);
set @capture_tail_sql = concat(
    'select coalesce(max(chunk_id), -1) into @tail_baseline from `',
    database(), '`.`', @stbl,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare capture_tail from @capture_tail_sql;
execute capture_tail;
deallocate prepare capture_tail;

-- Delete it; CDC must propagate to the deleted bitset before search reflects it.
delete from t where id = 5;

select count(*) from t;
-- First wait for the committed CDC tail. This storage-only poll keeps the
-- exact vector probes from re-caching a pre-commit index snapshot.
set @wait_tail_sql = concat(
    'select coalesce(max(chunk_id), -1) > @tail_baseline as cdc_tail_ready from `',
    database(), '`.`', @stbl,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare wait_tail from @wait_tail_sql;
-- @wait_expect(1, 60)
execute wait_tail;
deallocate prepare wait_tail;

-- Then observe post-commit cache invalidation through exact vector probes.
-- The gate returns one row only after every probe has its expected result.
-- @wait_expect(1, 60)
select 1 as ready
from (
    select 'delete-160' as probe_name, q.id as actual_id, 4 as expected_id
    from (select id from t order by l2_distance(v, '[160,160,160,160,160,160,160,160]') asc limit 1) q
    union all
    select 'untouched-1280' as probe_name, q.id as actual_id, 8 as expected_id
    from (select id from t order by l2_distance(v, '[1280,1280,1280,1280,1280,1280,1280,1280]') asc limit 1) q
) readiness
having count(*) = 2
   and sum(case when actual_id = expected_id then 1 else 0 end) = 2;


-- Deleted row is gone -> next unique survivor (id 4); an untouched row is unaffected.
select id from t order by l2_distance(v, '[160,160,160,160,160,160,160,160]') asc limit 1;
select id from t order by l2_distance(v, '[1280,1280,1280,1280,1280,1280,1280,1280]') asc limit 1;

drop database ivfpq_delete;
