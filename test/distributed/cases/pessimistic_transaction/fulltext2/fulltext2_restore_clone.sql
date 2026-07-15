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

-- let the async CDC build finish, then confirm the source index answers MATCH
select sleep(30);
select id from src where match(body) against('alpha' in boolean mode) order by id;

-- ================= CLONE (gate auto-skipped: flag toggled OFF) =================
set experimental_fulltext2_index = 0;

-- (a) clone succeeds and copies the rows + fulltext2 index definition
create table dst clone src;
select count(*) from dst;
show create table dst;

-- let RestoreInitSQL's re-registered CDC settle on the clone
select sleep(30);

-- (b) the cloned index answers MATCH
select id from dst where match(body) against('beta' in boolean mode) order by id;
select id from dst where match(body) against('topic' in boolean mode) order by id;

-- (c) maintenance re-armed: a row inserted AFTER the clone becomes matchable
insert into dst values (5,'epsilon fresh');
select sleep(45);
select id from dst where match(body) against('epsilon' in boolean mode) order by id;
-- pre-existing cloned rows remain matchable too
select id from dst where match(body) against('alpha' in boolean mode) order by id;

-- ================= SNAPSHOT / RESTORE (gate auto-skipped, flag still OFF) =======
create snapshot ft2_sp for account;

-- mutate after the snapshot: drop the indexed source table
drop table src;

-- restore the database from the snapshot -> rebuilds the fulltext2 index from the
-- restored rows (flag is OFF, but restore replays the existing index)
restore database sys.ft2_restore {snapshot="ft2_sp"};

-- the restored index answers MATCH
select sleep(30);
select id from src where match(body) against('alpha' in boolean mode) order by id;
select id from src where match(body) against('keyword' in boolean mode) order by id;

drop snapshot ft2_sp;
drop database ft2_restore;
