-- Regression for #27964: renaming (or dropping) a fulltext2 INCLUDE column left
-- stale metadata and broke incremental (CDC) indexing, because fulltext2 never
-- implemented the ALTER-column plan hooks. The fix (pkg/fulltext2/plugin/plan/
-- alter.go, mirroring ivfflat) rewrites the INCLUDE metadata name and triggers an
-- index rebuild so post-rename inserts are indexed again. fulltext2 is AlwaysAsync,
-- so readiness is polled on the durable cdc_tail chunk (never poll MATCH directly).
set experimental_fulltext2_index = 1;
drop database if exists ft2_include_rename;
create database ft2_include_rename;
use ft2_include_rename;

create table renamed (id bigint primary key, body text, status int, prio int);
create fulltext2 index ft_ren on renamed (body) include (status, prio);
insert into renamed values (1, 'alpha original', 10, 100), (2, 'beta only', 20, 200);

-- Wait for the initial CDC sync of the pre-rename rows.
set @ren_ft2_index = (
    select index_table_name from mo_catalog.mo_indexes
    where name = 'ft_ren' and algo = 'fulltext2' and algo_table_type = 'ftv2_index'
      and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'renamed')
    limit 1
);
set @wait_initial_sql = concat(
    'select coalesce(max(chunk_id), -1) >= 0 as ready from `', database(), '`.`', @ren_ft2_index,
    '` where index_id = ''cdc_tail'' and tag = 1');
prepare wait_initial from @wait_initial_sql;
-- @wait_expect(1, 120)
execute wait_initial;
deallocate prepare wait_initial;

-- Rename an INCLUDE column. This rebuilds the index (COPY), re-registering a new
-- physical index table, so re-resolve it afterwards.
alter table renamed rename column status to state;

-- Metadata fix (synchronous, deterministic): INCLUDE now shows `state`, not `status`.
show create table renamed;

-- Wait for the rebuild to settle on the (re-resolved) index table, then capture a
-- baseline chunk_id. The incremental insert below must advance chunk_id PAST this
-- baseline; polling `>= 0` alone would pass on the rebuild's own chunk and let
-- MATCH run before the incremental row synced (mirrors fulltext2_async's
-- before-mutation baseline pattern).
set @ren_ft2_index2 = (
    select index_table_name from mo_catalog.mo_indexes
    where name = 'ft_ren' and algo = 'fulltext2' and algo_table_type = 'ftv2_index'
      and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'renamed')
    limit 1
);
set @wait_rebuild_sql = concat(
    'select coalesce(max(chunk_id), -1) >= 0 as ready from `', database(), '`.`', @ren_ft2_index2,
    '` where index_id = ''cdc_tail'' and tag = 1');
prepare wait_rebuild from @wait_rebuild_sql;
-- @wait_expect(1, 120)
execute wait_rebuild;
deallocate prepare wait_rebuild;
set @capture_baseline_sql = concat(
    'select coalesce(max(chunk_id), -1) into @tail_baseline from `', database(), '`.`', @ren_ft2_index2,
    '` where index_id = ''cdc_tail'' and tag = 1');
prepare capture_baseline from @capture_baseline_sql;
execute capture_baseline;
deallocate prepare capture_baseline;

-- Incremental insert AFTER the baseline: its chunk must land strictly above it.
insert into renamed values (3, 'alpha incremental', 30, 300);
set @wait_incr_sql = concat(
    'select coalesce(max(chunk_id), -1) > @tail_baseline as ready from `', database(), '`.`', @ren_ft2_index2,
    '` where index_id = ''cdc_tail'' and tag = 1');
prepare wait_incr from @wait_incr_sql;
-- @wait_expect(1, 120)
execute wait_incr;
deallocate prepare wait_incr;

-- End-to-end fix: post-rename incremental row is searchable (bug returned only 1).
select id from renamed where match(body) against('alpha') order by id;
-- INCLUDE column is served under its new name from the rebuilt index.
select id, state from renamed where match(body) against('alpha') order by id;

-- Dropping an INCLUDE column drops the index (ivfflat parity), no stale reference.
alter table renamed drop column prio;
show create table renamed;

-- Regression: renaming a NON-include, non-key column leaves the index intact.
drop table if exists plain;
create table plain (id bigint primary key, body text, note int);
create fulltext2 index ft_plain on plain (body) include (note);
insert into plain values (1, 'gamma text', 7);
alter table plain rename column id to pk;
show create table plain;

drop database ft2_include_rename;
