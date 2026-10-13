-- Regression for #27927: an HNSW vector search on a named snapshot must read the
-- HISTORICAL index, not the current one. Before the fix the hnsw_search TVF loaded the
-- current index (via the shared cache under the current txn), so a `{snapshot=...}` top-k
-- whose current-generation candidates were all inserted after the snapshot returned EMPTY
-- (the snapshotted base table filtered them out, with no historical replacement). The fix
-- threads the snapshot TS into the TVF, clones the read txn at that TS, and caches the
-- historical index under a TS-suffixed key. hnsw is async; readiness is polled on the index
-- metadata table (its publication / checksum), never on a fixed sleep.
set experimental_hnsw_index = 1;
drop database if exists hnsw_snapshot_case;
create database hnsw_snapshot_case;
use hnsw_snapshot_case;

create table t(id bigint primary key, v vecf32(3));
insert into t values (1,'[0,0,0]'), (2,'[1,1,1]');
create index h using hnsw on t(v) op_type 'vector_l2_ops';

set @meta = (select index_table_name from mo_catalog.mo_indexes
    where name = 'h' and algo = 'hnsw' and algo_table_type = 'hnsw_meta'
      and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't')
    limit 1);

-- Wait for the base index to publish BEFORE the snapshot, so the snapshot captures a
-- populated generation; then capture its metadata checksum baseline.
set @wait_base_sql = concat('select count(*) >= 1 as ready from `', database(), '`.`', @meta, '`');
prepare wait_base from @wait_base_sql;
-- @wait_expect(1, 120)
execute wait_base;
deallocate prepare wait_base;
set @cap_sql = concat('select coalesce(min(checksum), '''') into @meta_before from `', database(), '`.`', @meta, '`');
prepare cap from @cap_sql;
execute cap;
deallocate prepare cap;

create snapshot hnsw_snapshot_case_sp for account;

-- Insert a row that becomes the current-generation nearest neighbor for the probe below.
insert into t values (3,'[0.01,0.01,0.01]');

-- Wait for the current index generation to reflect the insert (metadata checksum changed).
set @wait_upd_sql = concat('select count(*) > 0 as ready from `', database(), '`.`', @meta, '` where checksum <> @meta_before');
prepare wait_upd from @wait_upd_sql;
-- @wait_expect(1, 120)
execute wait_upd;
deallocate prepare wait_upd;

-- Current view: id3 is the exact match.
select id from t order by l2_distance(v,'[0.01,0.01,0.01]') limit 1;
-- Snapshot force mode (exact, no index): historical nearest is id1.
select id from t {snapshot='hnsw_snapshot_case_sp'} order by l2_distance(v,'[0.01,0.01,0.01]') limit 1 by rank with option 'mode=force';
-- Snapshot HNSW path (the fix): historical index -> id1 (was empty before the fix).
select id from t {snapshot='hnsw_snapshot_case_sp'} order by l2_distance(v,'[0.01,0.01,0.01]') limit 1;

drop snapshot hnsw_snapshot_case_sp;
drop database hnsw_snapshot_case;
