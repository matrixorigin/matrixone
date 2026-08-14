-- Execution-level proof for the clone/restore path of a fulltext2 index (ported
-- from pessimistic_transaction/fulltext fulltext_restore_clone). fulltext2 is
-- AlwaysAsync (CDC-backed, no ASYNC keyword). This drives the real surface:
--   (a) CREATE TABLE ... CLONE of a table carrying a fulltext2 index succeeds and
--       the CLONED index still answers MATCH ... AGAINST;
--   (b) the background CDC maintenance is RE-ARMED on the clone (RestoreInitSQL):
--       rows inserted AFTER the clone become matchable;
--   (c) CREATE SNAPSHOT + RESTORE DATABASE rebuilds the fulltext2 index from the
--       restored rows and it answers MATCH;
--   (d) the experimental_fulltext2_index gate is AUTO-SKIPPED during clone/restore
--       (an existing index is replayed even with the flag toggled off).
--
-- MATCH ... AGAINST requires a fulltext index (no full-scan fallback), so every
-- match that returns a row is served by a working index. Boolean mode is used so
-- results don't depend on relevancy thresholds.

drop database if exists ft2_restore;
create database ft2_restore;
use ft2_restore;

-- CREATE FULLTEXT2 INDEX is gated behind experimental_fulltext2_index (default off).
set experimental_fulltext2_index = 1;

-- Source: fulltext2 index over unique tokens (unambiguous matches).
create table src(id int primary key, body text, FULLTEXT2 ftidx(body));
insert into src values
  (1,'alpha keyword'),(2,'beta keyword'),
  (3,'gamma topic'),(4,'delta topic');

-- Wait for the initial CDC sync before the first MATCH. Do not poll MATCH here:
-- fulltext2's per-CN search cache is not cross-invalidated by the CDC consumer,
-- so an early empty MATCH would pin a stale snapshot for later queries. Poll the
-- hidden storage table instead; its cdc_tail row is the CDC build's durable
-- readiness signal.
set @src_ft2_index = (
    select index_table_name
    from mo_catalog.mo_indexes
    where name = 'ftidx'
      and algo = 'fulltext2'
      and algo_table_type = 'ftv2_index'
      and table_id in (
          select rel_id
          from mo_catalog.mo_tables
          where reldatabase = database() and relname = 'src'
      )
    limit 1
);
set @src_ft2_ready_sql = concat(
    'select coalesce(max(chunk_id), -1) >= 0 as ready from `', database(), '`.`', @src_ft2_index,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare wait_src_ft2 from @src_ft2_ready_sql;
-- @wait_expect(2, 30)
execute wait_src_ft2;
deallocate prepare wait_src_ft2;
select id from src where match(body) against('alpha' in boolean mode) order by id;

-- ================= CLONE (gate auto-skipped: flag toggled OFF) =================
set experimental_fulltext2_index = 0;

-- (a) clone succeeds and copies the rows + fulltext2 index definition
create table dst clone src;
select count(*) from dst;
show create table dst;

set @dst_ft2_index = (
    select index_table_name
    from mo_catalog.mo_indexes
    where name = 'ftidx'
      and algo = 'fulltext2'
      and algo_table_type = 'ftv2_index'
      and table_id in (
          select rel_id
          from mo_catalog.mo_tables
          where reldatabase = database() and relname = 'dst'
      )
    limit 1
);
-- (c) maintenance re-armed: insert a row AFTER the clone. Done BEFORE any dst MATCH
-- so the clone's per-CN index cache is first loaded fresh (with the new row present).
-- fulltext2's index cache is not cross-CN invalidated by the CDC consumer, so a dst
-- MATCH issued before this insert settles would pin a pre-insert snapshot and the
-- 'epsilon' query would read it stale on multi-CN.
insert into dst values (5,'epsilon fresh');

-- The clone copies the source tail. Wait until the destination tail advances past
-- the source tail, which proves the post-clone insert was persisted without relying
-- on a hard-coded chunk number.
set @dst_ft2_ready_sql = concat(
    'select coalesce(max(d.chunk_id), -1) > coalesce((select max(s.chunk_id) from `',
    database(), '`.`', @src_ft2_index,
    '` s where s.index_id = ''cdc_tail'' and s.tag = 1), -1) as ready from `',
    database(), '`.`', @dst_ft2_index,
    '` d where d.index_id = ''cdc_tail'' and d.tag = 1'
);
prepare wait_dst_ft2 from @dst_ft2_ready_sql;
-- @wait_expect(2, 45)
execute wait_dst_ft2;
deallocate prepare wait_dst_ft2;
-- (b) the cloned index answers MATCH on the copied rows ...
select id from dst where match(body) against('beta' in boolean mode) order by id;
select id from dst where match(body) against('topic' in boolean mode) order by id;
-- (c) ... and on the row inserted after the clone
select id from dst where match(body) against('epsilon' in boolean mode) order by id;
-- pre-existing cloned rows remain matchable too
select id from dst where match(body) against('alpha' in boolean mode) order by id;

-- ================= SNAPSHOT / RESTORE (gate auto-skipped, flag still OFF) =======
create snapshot ft2_sp for account;

-- mutate after the snapshot: drop the indexed source table
drop table src;

-- restore the database from the snapshot -> restores the rows and the existing
-- fulltext2 index (flag is OFF, but restore replays the existing index).
restore database sys.ft2_restore {snapshot="ft2_sp"};

-- Wait before the first MATCH after restore until the copied CDC tail is visible,
-- so the cache is loaded from the complete restored index.
set @restored_ft2_index = (
    select index_table_name
    from mo_catalog.mo_indexes
    where name = 'ftidx'
      and algo = 'fulltext2'
      and algo_table_type = 'ftv2_index'
      and table_id in (
          select rel_id
          from mo_catalog.mo_tables
          where reldatabase = database() and relname = 'src'
      )
    limit 1
);
set @restored_ft2_ready_sql = concat(
    'select coalesce(max(chunk_id), -1) >= 0 as ready from `', database(), '`.`', @restored_ft2_index,
    '` where index_id = ''cdc_tail'' and tag = 1'
);
prepare wait_restored_ft2 from @restored_ft2_ready_sql;
-- @wait_expect(2, 30)
execute wait_restored_ft2;
deallocate prepare wait_restored_ft2;
select id from src where match(body) against('alpha' in boolean mode) order by id;
select id from src where match(body) against('keyword' in boolean mode) order by id;

drop snapshot ft2_sp;
drop database ft2_restore;
