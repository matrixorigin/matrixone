-- A DATA BRANCH restores every derived index on the new base-table ID. When
-- two restore jobs start from the same watermark, each InitSQL must run in its
-- own ISCP iteration and both indexes must eventually serve the branch writes.
set experimental_fulltext2_index = 1;
set experimental_hnsw_index = 1;

-- This serial BVT starts with the default cache cadence. Shorten the existing
-- cross-CN freshness sweep so a warm reader can observe CDC updates within the
-- bounded waits below; restore the default at exit. Check partial RPC failures.
select mo_ctl('cn', 'SetVectorIndexFreshnessInterval', '1s') not like '%failed=[%' as all_cn_updated;

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
-- These assertions prove the fixed special-index paths, not a base-table
-- fallback. The independent distance expression remains the exact oracle.
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
explain select id from leaf_t
where match(body) against('+branch' in boolean mode) order by id;
-- Wait for the complete result, including the post-branch INSERT. A durable
-- CDC tail alone can precede that write or leave a remote reader's cache stale.
-- @wait_expect(1, 60)
select id from leaf_t
where match(body) against('+branch' in boolean mode) order by id;

-- HNSW may safely fall back until its rebuilt generation covers both writes.
-- @wait_expect(1, 60)
-- @separator:table
-- @regex("Vector Index Scan on", true)
explain select id from leaf_t
order by l2_distance(v, '[0,0,0]') limit 2;
select id from leaf_t
order by l2_distance(v, '[0,0,0]') limit 2;
select id from leaf_t
order by (l2_distance(v, '[0,0,0]') + 0) limit 2;
data branch diff leaf_t against base_t output count;

-- Check after both public readiness conditions, when the restore jobs have run.
select count(*) from mo_catalog.mo_iscp_log
where table_id = @leaf_id and drop_at is null
  and job_name in ('index_ft', 'index_hv')
  and job_state = 4;

select mo_ctl('cn', 'SetVectorIndexFreshnessInterval', '0') not like '%failed=[%' as all_cn_restored;

drop database issue27950_branch_indexes;
set experimental_fulltext2_index = 0;
set experimental_hnsw_index = 0;
