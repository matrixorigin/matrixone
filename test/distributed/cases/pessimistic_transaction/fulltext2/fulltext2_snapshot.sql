-- Regression for #27941 (fulltext2): a MATCH on a named snapshot must read the
-- HISTORICAL index, not the current one. Before the fix the fulltext2_search TVF
-- loaded the index through the shared cache under the current txn, so a
-- `{snapshot=...} MATCH` returned current-index results. The fix threads the snapshot
-- TS into the TVF, clones the read txn at that TS, and caches the historical index
-- under a TS-suffixed key (so it never serves/pollutes the current-index entry, and
-- concurrent same-snapshot queries still share one load). fulltext2 is AlwaysAsync;
-- readiness is polled on the durable cdc_tail chunk (never poll MATCH -- that can pin
-- a stale per-CN cache).
set experimental_fulltext2_index = 1;
drop database if exists ft2_snap_case;
create database ft2_snap_case;
use ft2_snap_case;

create table t(id bigint primary key, body text);
insert into t values (1,'historic alpha'),(2,'shared token');
create fulltext2 index idx on t(body);

set @ft2_index = (
    select index_table_name from mo_catalog.mo_indexes
    where name = 'idx' and algo = 'fulltext2' and algo_table_type = 'ftv2_index'
      and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 't')
    limit 1
);

-- Wait for the base index to sync the historic rows BEFORE the snapshot, so the
-- snapshot captures a populated (not lagging) index; then capture the tail baseline.
-- Poll the BASE chunk (tag = 0), not cdc_tail: the initial build of an index over an
-- already-populated table writes one tag=0 row and NO tail, so a cdc_tail predicate here
-- can never become true and is either a no-op (if it expects the not-ready answer) or a
-- guaranteed timeout. cdc_tail only appears once later DML flows through CDC, which is
-- what wait_upd below polls for.
set @wait_base_sql = concat(
    'select count(*) > 0 as ready from `', database(), '`.`', @ft2_index,
    '` where tag = 0');
prepare wait_base from @wait_base_sql;
-- @wait_expect(1, 120)
execute wait_base;
deallocate prepare wait_base;
set @capture_sql = concat(
    'select coalesce(max(chunk_id), -1) into @tail_baseline from `', database(), '`.`', @ft2_index,
    '` where index_id = ''cdc_tail'' and tag = 1');
prepare capture from @capture_sql;
execute capture;
deallocate prepare capture;

create snapshot ft2_snap_case_sp for account;

-- Diverge the current index from the snapshot.
update t set body='current beta' where id=1;
insert into t values (3,'current beta');

-- Wait for the update to advance the tail PAST the baseline (current index diverged).
set @wait_upd_sql = concat(
    'select coalesce(max(chunk_id), -1) > @tail_baseline as ready from `', database(), '`.`', @ft2_index,
    '` where index_id = ''cdc_tail'' and tag = 1');
prepare wait_upd from @wait_upd_sql;
-- @wait_expect(1, 120)
execute wait_upd;
deallocate prepare wait_upd;

-- Current index (control): 'alpha' gone, 'beta' => id1,id3.
select id from t where match(body) against('+alpha' in boolean mode);
select id from t where match(body) against('+beta' in boolean mode) order by id;

-- Snapshot index (the fix): historical -- 'alpha' => id1 'historic alpha', 'beta' => empty.
select id, body from t {snapshot='ft2_snap_case_sp'} where match(body) against('+alpha' in boolean mode);
select id from t {snapshot='ft2_snap_case_sp'} where match(body) against('+beta' in boolean mode);

drop snapshot ft2_snap_case_sp;
drop database ft2_snap_case;
