-- @skip:issue#28985
-- Skipped: flaky in multi-CN CI. The final section asserts that a warm per-CN fulltext2 cache
-- reflects a subsequent CDC tail flush, but cross-CN cache refresh is EVENTUAL by design (the
-- ~10m IsStale pull sweep; RemoveIdle evicts only the consumer CN -- see the won't-fix note in
-- pkg/vectorindex/cache/cache.go), so a MATCH routed to a non-consumer CN drops the new rows.
-- Tracked in #28985; unskip once the case is made multi-CN-deterministic (or scoped single-CN).
-- Regression for #28837 (fulltext2): an UNRELATED COPY ALTER (ADD COLUMN, which does not
-- touch the indexed column) must not leave the FULLTEXT2 index empty. cloneUnaffectedIndexes
-- marks fulltext2 SkipWholeIndex, so the ALTER clones the table to a NEW id with an empty
-- index and rebuilds it from the CDC log. Before the fix the consumer wrote only a tag=1
-- cdc_tail (no tag=0 base) and the querying CN kept its warm doc-less cache, so MATCH stayed
-- empty for minutes until a reindex/restart. The fix rebuilds a real tag=0 base at copy time
-- (AlterCopyInitSQL REINDEX FORCE_SYNC) and refreshes the idle cache on the CDC flush
-- (cache.RemoveIdle). Readiness is polled on the REPLACEMENT index's durable tag=0 base
-- (never poll MATCH -- that can pin a stale per-CN cache); that base is exactly the signal
-- the fix produces and the bug does not, so pre-fix this case times out at the second wait.
set experimental_fulltext2_index = 1;
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

-- Best-effort PREWARM of the replacement index: if it runs before the async REINDEX FORCE_SYNC
-- builds the tag=0 base, it loads a base-less generation into this CN's cache. This is a smoke
-- probe only -- a BVT cannot hold the REINDEX, so the base may already be published and the
-- count(*)>=0 wrapper (always 1) passes either way. The DETERMINISTIC proof that a base-less
-- generation is served empty, NOT retained, and reloaded to the base without a CDC flush or
-- housekeeping sweep is the unit test TestCacheNotCacheEmptyReloadsBase in pkg/vectorindex/cache.
select count(*) >= 0 as prewarmed from docs where match(body) against('quantum');

-- The ALTER cloned docs to a new table id, so the fulltext2 index has a NEW hidden table.
-- Re-resolve it and wait on ITS tag=0 base. Pre-fix the CDC consumer writes only tag=1, so
-- this base never appears and the wait times out (reproducing #28837); post-fix REINDEX
-- writes it.
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

-- MATCH after the copy alter == the base-table oracle. This is the first MATCH on the new
-- index, so the CN cache loads fresh. An empty result here is the #28837 bug.
select id from docs where body like '%quantum%' order by id;
select id from docs where match(body) against('quantum') order by id;

-- The unrelated column is present and MATCH composes with a predicate on it.
show create table docs;
select id, extra from docs where match(body) against('quantum') order by id;

-- Independently check that ordinary CDC keeps flowing after COPY ALTER. Use a second
-- table whose REPLACEMENT index has never been searched: the assertions above warmed
-- docs' base generation, and a CDC writer on another CN only evicts its OWN cache.
-- Waiting for a durable tail does not refresh an already-warm remote generation.
-- Keep the original rebuild/prewarm assertions above instead of assuming immediate
-- cross-CN cache coherence here (the same isolation is used in fulltext2_async.sql).
create table docs_cdc(id bigint primary key, body text);
insert into docs_cdc values
  (1,'quantum physics is deep'),(2,'classical mechanics only'),
  (3,'quantum computing rocks'),(4,'organic chemistry notes'),(5,'a quantum leap forward');
create fulltext2 index ftidx on docs_cdc(body);
alter table docs_cdc add column extra int;
set @ft2_cdc_index = (
    select index_table_name from mo_catalog.mo_indexes
    where name = 'ftidx' and algo = 'fulltext2' and algo_table_type = 'ftv2_index'
      and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'docs_cdc')
    limit 1
);
set @wait_cdc_base_sql = concat(
    'select count(*) > 0 as ready from `', database(), '`.`', @ft2_cdc_index, '` where tag = 0');
prepare wait_cdc_base from @wait_cdc_base_sql;
-- @wait_expect(1, 120)
execute wait_cdc_base;
deallocate prepare wait_cdc_base;

-- After the REINDEX rebuilt this tag=0 base, ordinary CDC must keep flowing: a fresh INSERT
-- (a term absent from the base) must arrive in the tag=1 cdc_tail and become searchable. The
-- copy-alter registered the CDC with startFromNow=true, so the first normal iteration does NOT
-- replay the copied rows (that would need an empty ts=0 watermark) -- it carries only the new
-- rows. MATCH then composes the base (copied rows) with the tail (new rows). Wait on the
-- durable tag=1 tail, never MATCH (which can pin a stale per-CN cache).
set @capture_tail_sql = concat(
    'select coalesce(max(chunk_id), -1) into @tail_before_insert from `', database(), '`.`', @ft2_cdc_index,
    '` where index_id = ''cdc_tail'' and tag = 1');
prepare capture_tail from @capture_tail_sql;
execute capture_tail;
deallocate prepare capture_tail;
insert into docs_cdc(id, body, extra) values (100,'neutrino oscillation study',1),(101,'neutrino detector array',1);
set @wait_tail_sql = concat(
    'select coalesce(max(chunk_id), -1) > ', @tail_before_insert,
    ' as ready from `', database(), '`.`', @ft2_cdc_index, '` where index_id = ''cdc_tail'' and tag = 1');
prepare wait_tail from @wait_tail_sql;
-- @wait_expect(1, 120)
execute wait_tail;
deallocate prepare wait_tail;

-- New rows arrived via the CDC tail; the base still serves the copied rows.
select id from docs_cdc where body like '%neutrino%' order by id;
select id from docs_cdc where match(body) against('neutrino') order by id;
select id from docs_cdc where match(body) against('quantum') order by id;

drop database ft2_copy_alter_case;
