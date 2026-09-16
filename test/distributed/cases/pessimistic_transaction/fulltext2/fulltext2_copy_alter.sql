-- Regression for #28837 (fulltext2): an UNRELATED COPY ALTER (ADD COLUMN, which does not
-- touch the indexed column) must not leave the FULLTEXT2 index empty. cloneUnaffectedIndexes
-- marks fulltext2 SkipWholeIndex, so the ALTER clones the table to a NEW id with an empty
-- index and rebuilds it from the CDC log. Before the fix the consumer wrote only a tag=1
-- cdc_tail (no tag=0 base) and the querying CN kept its warm doc-less cache, so MATCH stayed
-- empty for minutes until a reindex/restart. The fix rebuilds a real tag=0 base at copy time
-- (AlterCopyInitSQL REINDEX FORCE_SYNC) and refreshes the idle cache on the CDC flush
-- (cache.RemoveIdle). Readiness is polled on the REPLACEMENT index's durable tag=0 base and
-- tag=1 tail (never poll MATCH directly for that -- it can pin a stale per-CN cache); those
-- durable rows are exactly the signal the fix produces and the bug does not.
--
-- #28985 (multi-CN read-your-writes): the copy-alter warms a base-only generation on the query
-- CN, and cross-CN cache refresh is EVENTUAL by design -- the periodic IsStale sweep (~10m by
-- default; RemoveIdle evicts only the CDC-consumer CN -- see the won't-fix note in
-- pkg/vectorindex/cache/cache.go). Rather than skip the tail assertion, this case PROVES the
-- eventual mechanism converges: at the START it lowers the periodic sweep cadence cluster-wide via
-- mo_ctl (moadmin-only, broadcast to all CNs, not persisted), so the SWEEP -- not any manual
-- eviction -- refreshes stale entries in seconds; it restores the default at the end. BVT runs
-- serially, so this global knob is safe, and a short interval is benign to any later case (it only
-- refreshes caches sooner). At the very end it also directly exercises the manual EvictVectorIndexCache
-- mo_ctl and confirms via GetVectorIndexCacheInfo that no CN holds the entry afterwards.
--
-- This is a MULTI-CN test (it runs in the pessimistic multi-CN BVT suite). That is what makes the
-- GetVectorIndexCacheInfo=0 poll a genuine proof of the cross-CN sweep: cached is summed over ALL
-- CNs, and RemoveIdle only evicts the CDC-consumer CN, so a non-consumer CN's stale entry can be
-- cleared only by the periodic sweep. The sum can reach 0 only once the sweep has evicted the
-- non-consumer CN(s) -- RemoveIdle alone cannot zero it.
set experimental_fulltext2_index = 1;

-- Lower the periodic cross-CN freshness sweep cadence for the duration of this case. Everything
-- below relies on the sweep (at this shortened cadence) to converge -- that is what proves the
-- mechanism. mo_ctl's Result carries per-CN addresses, so ignore its (unstable) output column.
-- @ignore:0
select mo_ctl('cn', 'SetVectorIndexFreshnessInterval', '1s');

drop database if exists ft2_copy_alter_case;
create database ft2_copy_alter_case;
use ft2_copy_alter_case;

-- Force-build over an already-populated table: CREATE builds a tag=0 base from source.
create table docs(id bigint primary key, body text);
insert into docs values
  (1,'quantum physics is deep'),(2,'classical mechanics only'),
  (3,'quantum computing rocks'),(4,'organic chemistry notes'),(5,'a quantum leap forward');
create fulltext2 index ftidx on docs(body);

set @ft2_index = (
    select index_table_name from mo_catalog.mo_indexes
    where name = 'ftidx' and algo = 'fulltext2' and algo_table_type = 'ftv2_index'
      and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'docs')
    limit 1
);
-- The synchronous CREATE build wrote one tag=0 base row; confirm before searching.
set @wait_base_sql = concat(
    'select count(*) > 0 as ready from `', database(), '`.`', @ft2_index, '` where tag = 0');
prepare wait_base from @wait_base_sql;
-- @wait_expect(1, 120)
execute wait_base;
deallocate prepare wait_base;

-- Base-table oracle vs MATCH BEFORE the alter: the index is queryable and agrees.
select id from docs where body like '%quantum%' order by id;
select id from docs where match(body) against('quantum') order by id;

-- The UNRELATED COPY ALTER: adds a column the fulltext2 index does not cover.
alter table docs add column extra int;

-- The ALTER cloned docs to a new table id, so the fulltext2 index has a NEW hidden table.
-- Re-resolve it and wait on ITS tag=0 base. Pre-fix the CDC consumer writes only tag=1, so
-- this base never appears and the wait times out (reproducing #28837); post-fix REINDEX writes it.
set @ft2_index2 = (
    select index_table_name from mo_catalog.mo_indexes
    where name = 'ftidx' and algo = 'fulltext2' and algo_table_type = 'ftv2_index'
      and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'docs')
    limit 1
);
set @wait_base2_sql = concat(
    'select count(*) > 0 as ready from `', database(), '`.`', @ft2_index2, '` where tag = 0');
prepare wait_base2 from @wait_base2_sql;
-- @wait_expect(1, 120)
execute wait_base2;
deallocate prepare wait_base2;

-- MATCH after the copy alter == the base-table oracle. This warms a base-only generation on the
-- query CN's cache. An empty result here is the #28837 bug.
select id from docs where body like '%quantum%' order by id;
select id from docs where match(body) against('quantum') order by id;

-- The unrelated column is present and MATCH composes with a predicate on it.
show create table docs;
select id, extra from docs where match(body) against('quantum') order by id;

-- After the REINDEX rebuilt the tag=0 base, ordinary CDC must keep flowing: a fresh INSERT
-- (a term absent from the base) must arrive in the tag=1 cdc_tail. The copy-alter registered the
-- CDC with startFromNow=true, so the first normal iteration does NOT replay the copied rows -- it
-- carries only the new rows. Wait on the durable tag=1 tail, never MATCH (which can pin a stale
-- per-CN cache).
insert into docs(id, body, extra) values (100,'neutrino oscillation study',1),(101,'neutrino detector array',1);
set @wait_tail_sql = concat(
    'select count(*) > 0 as ready from `', database(), '`.`', @ft2_index2, '` where tag = 1');
prepare wait_tail from @wait_tail_sql;
-- @wait_expect(1, 120)
execute wait_tail;
deallocate prepare wait_tail;

-- The insert made the warm base-only entry STALE. Wait -- deterministically, not on a timer --
-- until the periodic freshness sweep (running at the shortened 1s cadence set at the top) has
-- EVICTED the stale replacement-index entry on EVERY CN. GetVectorIndexCacheInfo returns
-- {cached, cns}: cached sums, across all CNs, how many cache entries hold @ft2_index2. json_extract
-- pulls the numeric $.result.cached total (cns is deployment-dependent, so it is not compared) and
-- the poll drives that total to 0, i.e. evicted everywhere. This is placement-independent (it
-- inspects all CNs, never trusts one) and never re-warms the cache (it only reads it). Under the default
-- ~10m cadence it would never reach 0 in the window (the #28985 bug); the shortened cadence makes
-- the sweep converge in seconds. Once every CN has dropped the stale entry, the next MATCH
-- cold-reloads base+tail on whichever CN serves it.
set @wait_sweep_sql = concat(
    'select json_extract(mo_ctl(''cn'', ''GetVectorIndexCacheInfo'', ''', @ft2_index2, '''), ''$.result.cached'') as cached');
prepare wait_sweep from @wait_sweep_sql;
-- @wait_expect(1, 120)
execute wait_sweep;
deallocate prepare wait_sweep;

-- New rows arrived via the CDC tail and are searchable; the base still serves the copied rows.
-- The entry was just evicted everywhere, so this first MATCH cold-reloads base+tail on any CN.
select id from docs where match(body) against('neutrino') order by id;
select id from docs where match(body) against('quantum') order by id;

-- The operator workflow before evicting: list the cached keys to discover the exact key. The two
-- MATCHes above re-warmed @ft2_index2, so GetVectorIndexCacheKeys must now list it. The full key
-- list is not clean across BVT cases (dynamic hidden-table names, other cases' cached indexes, and
-- possibly snapshot generations of this same index), so assert only the stable fact that
-- @ft2_index2 appears AT LEAST ONCE: LIKE returns 1 whether it is listed once (base only) or more
-- (base + snapshot generations), and it matches this index's unique table name only.
set @list_sql = concat(
    'select mo_ctl(''cn'', ''GetVectorIndexCacheKeys'', '''') like ''%', @ft2_index2, '%'' as found');
prepare list_keys from @list_sql;
execute list_keys;
deallocate prepare list_keys;

-- Directly exercise the manual eviction command: the two MATCHes above re-warmed the entry on
-- whichever CN(s) served them. EvictVectorIndexCache broadcasts to every CN and drops @ft2_index2
-- synchronously; its Result {evicted, cns} depends on which CNs had warmed it, so ignore that
-- output column. Immediately afterward GetVectorIndexCacheInfo must report cached=0 on every CN --
-- nothing queries in between to re-warm it, so this is deterministic.
set @evict_sql = concat('select mo_ctl(''cn'', ''EvictVectorIndexCache'', ''', @ft2_index2, ''')');
prepare do_evict from @evict_sql;
-- @ignore:0
execute do_evict;
deallocate prepare do_evict;
set @check_info_sql = concat(
    'select json_extract(mo_ctl(''cn'', ''GetVectorIndexCacheInfo'', ''', @ft2_index2, '''), ''$.result.cached'') as cached');
prepare check_info from @check_info_sql;
execute check_info;
deallocate prepare check_info;

-- Restore the default freshness cadence so later cases run with the normal ~10m bound.
-- @ignore:0
select mo_ctl('cn', 'SetVectorIndexFreshnessInterval', '0');

drop database ft2_copy_alter_case;
