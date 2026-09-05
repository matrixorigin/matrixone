-- Regression for #27927 (IVF-PQ): a vector search on a named snapshot must read the
-- HISTORICAL index, not the current one.
--
-- GPU REQUIRED. Lives under pessimistic_transaction/ because it depends on the ISCP CDC
-- consumer appending the post-snapshot row into the index's cdc_tail overflow.
--
-- Before the fix the ivfpq_search TVF loaded the index through the shared veccache under
-- the CURRENT txn, so a `{snapshot=...}` top-k returned the CURRENT generation's nearest
-- neighbour; that row does not exist in the snapshotted base table, the JOIN dropped it,
-- and the query came back EMPTY. The fix threads the snapshot TS into the TVF, clones the
-- read txn at that TS, and caches the historical index under a TS-suffixed key.
--
-- Determinism: the probe [1.01 x8] is an EXACT match for the post-snapshot row 21 (current
-- answer, served exactly from the brute-force cdc_tail overflow) and is 100x nearer to row
-- 1 than to row 2 in the snapshotted generation (0.028 vs 2.80), so the historical top-1
-- survives PQ quantization error. Only top-1 is asserted -- see gpu_cases/README.md.
SET experimental_ivfpq_index = 1;
SET ivfpq_threads_build = 6;
SET kmeans_train_percent = 100;
SET kmeans_max_iteration = 12;
SET probe_limit = 16;

drop database if exists ivfpq_snapshot_case;
create database ivfpq_snapshot_case;
use ivfpq_snapshot_case;

create table t (id bigint primary key, v vecf32(8));
insert into t values
    ( 1, '[1,1,1,1,1,1,1,1]'),         ( 2, '[2,2,2,2,2,2,2,2]'),
    ( 3, '[3,3,3,3,3,3,3,3]'),         ( 4, '[4,4,4,4,4,4,4,4]'),
    ( 5, '[5,5,5,5,5,5,5,5]'),         ( 6, '[6,6,6,6,6,6,6,6]'),
    ( 7, '[7,7,7,7,7,7,7,7]'),         ( 8, '[8,8,8,8,8,8,8,8]'),
    ( 9, '[9,9,9,9,9,9,9,9]'),         (10, '[10,10,10,10,10,10,10,10]'),
    (11, '[11,11,11,11,11,11,11,11]'), (12, '[12,12,12,12,12,12,12,12]'),
    (13, '[13,13,13,13,13,13,13,13]'), (14, '[14,14,14,14,14,14,14,14]'),
    (15, '[15,15,15,15,15,15,15,15]'), (16, '[16,16,16,16,16,16,16,16]'),
    (17, '[17,17,17,17,17,17,17,17]'), (18, '[18,18,18,18,18,18,18,18]'),
    (19, '[19,19,19,19,19,19,19,19]'), (20, '[20,20,20,20,20,20,20,20]');

-- Sync build (no ASYNC): the index is complete before CREATE INDEX returns, so the
-- snapshot below captures a fully populated generation.
create index ix using ivfpq on t (v)
    op_type 'vector_l2_ops' lists=10 m=8 bits_per_code=8
    kmeans_train_percent 100 kmeans_max_iteration 20 max_index_capacity 100;

-- Storage table name, for the CDC-tail readiness probe below.
set @idx = (select index_table_name from mo_catalog.mo_indexes
    where name = 'ix' and algo = 'ivfpq' and algo_table_type = 'ivfpq_index'
      and table_id in (select rel_id from mo_catalog.mo_tables
                       where reldatabase = database() and relname = 't')
    limit 1);

create snapshot ivfpq_snapshot_case_sp for account;

-- The post-snapshot row is the current-generation nearest neighbour for the probe below.
insert into t values (21, '[1.01,1.01,1.01,1.01,1.01,1.01,1.01,1.01]');

-- Wait for the ISCP CDC consumer to append the row into the cdc_tail overflow, so the
-- CURRENT index really does differ from the snapshotted one. Polled on the index storage
-- table, never on a fixed sleep.
set @wait_sql = concat('select count(*) > 0 as ready from `', database(), '`.`', @idx,
                       '` where index_id = ''cdc_tail''');
prepare wait_cdc from @wait_sql;
-- @wait_expect(1, 180)
execute wait_cdc;
deallocate prepare wait_cdc;

-- Current view: id 21 is the exact match.
select id from t order by l2_distance(v,'[1.01,1.01,1.01,1.01,1.01,1.01,1.01,1.01]') limit 1;
-- Snapshot force mode (exact, index bypassed): historical nearest is id 1.
select id from t {snapshot='ivfpq_snapshot_case_sp'} order by l2_distance(v,'[1.01,1.01,1.01,1.01,1.01,1.01,1.01,1.01]') limit 1 by rank with option 'mode=force';
-- Snapshot IVF-PQ path (the fix): historical index -> id 1 (was EMPTY before the fix).
select id from t {snapshot='ivfpq_snapshot_case_sp'} order by l2_distance(v,'[1.01,1.01,1.01,1.01,1.01,1.01,1.01,1.01]') limit 1;
-- The current-index answer is unchanged by the snapshot query above: the historical index
-- is cached under its own TS-suffixed key and never displaces the current entry.
select id from t order by l2_distance(v,'[1.01,1.01,1.01,1.01,1.01,1.01,1.01,1.01]') limit 1;

drop snapshot ivfpq_snapshot_case_sp;
drop database ivfpq_snapshot_case;
SET experimental_ivfpq_index = 0;
