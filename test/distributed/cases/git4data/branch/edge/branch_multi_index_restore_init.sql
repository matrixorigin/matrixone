-- A DATA BRANCH restores every derived index on the new base-table ID. When
-- two restore jobs start from the same watermark, each InitSQL must run in its
-- own ISCP iteration before either index is queried.
set experimental_fulltext2_index = 1;
set experimental_hnsw_index = 1;

drop database if exists issue27950_branch_indexes;
create database issue27950_branch_indexes;
use issue27950_branch_indexes;

create table base_t(
  id bigint primary key,
  body text,
  v vecf32(3)
);
insert into base_t values
  (1, 'alpha seed', '[1,0,0]'),
  (2, 'beta seed', '[0,1,0]');
create fulltext2 index ft on base_t(body) max_index_capacity 64;
create index hv using hnsw on base_t(v) op_type 'vector_l2_ops';
alter table base_t alter reindex ft fulltext2 force_sync;
alter table base_t alter reindex hv hnsw force_sync;

data branch create table leaf_t from base_t;
update leaf_t set body = 'branch token', v = '[9,0,0]' where id = 1;
insert into leaf_t values (3, 'branch insert', '[0,0,0]');

set @leaf_id = (
  select rel_id from mo_catalog.mo_tables
  where reldatabase = database() and relname = 'leaf_t'
);
set @leaf_ft2_index = (
  select index_table_name from mo_catalog.mo_indexes
  where table_id = @leaf_id and name = 'ft'
    and algo = 'fulltext2' and algo_table_type = 'ftv2_index'
  limit 1
);
set @leaf_ft2_ready_sql = concat(
  'select coalesce(max(chunk_id), -1) >= 0 as ready from `', database(), '`.`',
  @leaf_ft2_index, '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare wait_leaf_ft2 from @leaf_ft2_ready_sql;
set @leaf_hnsw_meta = (
  select index_table_name from mo_catalog.mo_indexes
  where table_id = @leaf_id and name = 'hv'
    and algo = 'hnsw' and algo_table_type = 'hnsw_meta'
  limit 1
);
set @leaf_hnsw_ready_sql = concat(
  'select count(*) > 0 as ready from `', database(), '`.`',
  @leaf_hnsw_meta, '`'
);
prepare wait_leaf_hnsw from @leaf_hnsw_ready_sql;

-- FULLTEXT2 publishes cdc_tail only after durable hidden storage is ready;
-- HNSW publishes metadata only after its FORCE_SYNC RestoreInitSQL completes.
-- @wait_expect(1, 60)
execute wait_leaf_ft2;
deallocate prepare wait_leaf_ft2;
-- @wait_expect(1, 60)
execute wait_leaf_hnsw;
deallocate prepare wait_leaf_hnsw;

-- Neither restore job may retain the permanent scheduler error caused by a
-- shared Init iteration. The hidden-table waits above are the readiness guard
-- before proving the optimizer paths.
select count(*) from mo_catalog.mo_iscp_log
where table_id = @leaf_id and drop_at is null
  and job_name in ('index_ft', 'index_hv')
  and job_state = 4;

-- These assertions prove the fixed special-index paths, not a base-table
-- fallback. The independent distance expression remains the exact oracle.
-- @separator:table
-- @regex("Table Function on fulltext2_search", true)
explain select id from leaf_t
where match(body) against('+branch' in boolean mode) order by id;
select id from leaf_t
where match(body) against('+branch' in boolean mode) order by id;

-- @separator:table
-- @regex("Table Function on hnsw_search", true)
explain select id from leaf_t
order by l2_distance(v, '[0,0,0]') limit 2;
select id from leaf_t
order by l2_distance(v, '[0,0,0]') limit 2;
select id from leaf_t
order by (l2_distance(v, '[0,0,0]') + 0) limit 2;
data branch diff leaf_t against base_t output count;

drop database issue27950_branch_indexes;
set experimental_fulltext2_index = 0;
set experimental_hnsw_index = 0;
