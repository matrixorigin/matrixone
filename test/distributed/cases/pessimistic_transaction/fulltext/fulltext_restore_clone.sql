-- Execution-level proof for the clone/restore path of an ASYNC fulltext index
-- (the CDC-backed maintenance path re-registered by Scope.RestoreTable, whose
-- fulltext RestoreInitSQL is the no-op "SELECT 1" with startFromNow=true).
--
-- This drives the real surface, not just metadata/wiring:
--   (a) CREATE TABLE ... CLONE of a table carrying an ASYNC fulltext index
--       succeeds;
--   (b) the CLONED inverted index still answers MATCH ... AGAINST;
--   (c) the background maintenance path is RE-ARMED on the clone: rows inserted
--       AFTER the clone are picked up by the re-registered CDC and become
--       matchable.
--
-- Why a non-empty MATCH proves the index is live: MATCH ... AGAINST requires a
-- fulltext index (a full-table-scan fallback is unsupported and errors), so
-- every match that returns a row below is served by a working inverted index.
-- Boolean mode is used so results don't depend on IDF / relevancy thresholds.

drop database if exists ft_restore;
create database ft_restore;
use ft_restore;

-- Source: ASYNC fulltext index over unique tokens (unambiguous matches).
create table src(id int primary key, body text, FULLTEXT ftidx(body) ASYNC);
insert into src values
  (1,'alpha keyword'),(2,'beta keyword'),
  (3,'gamma topic'),(4,'delta topic');

-- let the async build finish, then confirm the source index answers MATCH
select sleep(30);
select id from src where match(body) against('alpha' in boolean mode) order by id;

-- (a) clone succeeds and copies the rows + fulltext index definition
create table dst clone src;
select count(*) from dst;
show create table dst;

-- let RestoreTable's re-registered CDC settle on the clone
select sleep(30);

-- (b) the cloned inverted index answers MATCH (no full-scan fallback exists)
select id from dst where match(body) against('beta' in boolean mode) order by id;
select id from dst where match(body) against('topic' in boolean mode) order by id;

-- (c) maintenance re-armed: a row inserted AFTER the clone becomes matchable
insert into dst values (5,'epsilon fresh');
select sleep(45);
-- the post-clone token is matched via the cloned index -> CDC path is live
select id from dst where match(body) against('epsilon' in boolean mode) order by id;
-- pre-existing cloned rows remain matchable too
select id from dst where match(body) against('alpha' in boolean mode) order by id;

drop database ft_restore;
