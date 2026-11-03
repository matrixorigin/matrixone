-- #30402: fulltext2_threads_search sizes the multi-segment PARALLEL top-k search.
-- It is a perf-only hint: any worker count yields identical ranked results. The worker
-- count is bound once when an index generation is first LOADED into the cache, so to
-- take the serial AND the parallel path this test loads TWO separate indexes, each first
-- queried under its own setting -- not one warm cache re-read under a changed knob.
--
-- Two conditions are required for the query to actually reach the parallel SearchBoolean
-- top-k fan-out (and its cross-segment floor/skip) rather than the serial no-LIMIT
-- streaming path:
--   1. a competitive ORDER BY score DESC LIMIT, so the limit pushes down and the TVF runs
--      the bounded top-k search instead of streaming every match;
--   2. a small max_postings_capacity, so the base seals into MANY segments and the
--      multi-segment fan-out is genuinely exercised (one segment would run serially);
--   3. a COVERED index (INCLUDE column, pk + score projection), so there is no base-table
--      join and no post-filter over-fetch: the search keeps exactly LIMIT 3 of the 6
--      matches and the top-k truncation and cross-segment floor happen inside the search.
-- 'rare' appears in a MINORITY of docs (ids 1..6) at DISTINCT term frequencies, so its
-- idf is positive and the top-k is strictly ordered (no ties) -- a deterministic
-- (id, score) list to compare across the serial and parallel loads.
set experimental_fulltext2_index = 1;
drop database if exists ft2_threads;
create database ft2_threads;
use ft2_threads;

-- default is 0 (resolves to the deployment's effective parallelism), settable in [0,1024].
select @@fulltext2_threads_search as default_val;
set fulltext2_threads_search = 1024;
select @@fulltext2_threads_search as after_set;

-- SERIAL load: the knob is 1 BEFORE t1's index is first queried, so its cached generation
-- is built with one worker and the top-k query runs the serial walk. max_postings_capacity
-- 8 (~5-11 postings/doc) seals the ~80 postings into several tag=0 segments.
set fulltext2_threads_search = 1;
create table t1(id bigint primary key, body text, prio int);
insert into t1 values
  (1,'rare common data text word more',10),
  (2,'rare rare common data text word more',20),
  (3,'rare rare rare common data text word more',30),
  (4,'rare rare rare rare common data text word more',40),
  (5,'rare rare rare rare rare common data text word more',50),
  (6,'rare rare rare rare rare rare common data text word more',60),
  (7,'common data text word more',70),
  (8,'common data text word more',80),
  (9,'common data text word more',90),
  (10,'common data text word more',100),
  (11,'common data text word more',110),
  (12,'common data text word more',120);
create fulltext2 index ft on t1(body) include(prio) max_postings_capacity 8;
-- @regex("Fulltext Index Scan on ft", true)
-- @regex("Join", false)
-- @regex("Candidate Limit: 3", true)
explain select id, match(body) against('rare' in bm25 mode) as score
  from t1 where match(body) against('rare' in bm25 mode)
  order by score desc limit 3;
select id, match(body) against('rare' in bm25 mode) as score
  from t1 where match(body) against('rare' in bm25 mode)
  order by score desc limit 3;

-- PARALLEL load: a SEPARATE index first queried while the knob is 8, so its generation is
-- built parallel over the same many segments. The ranked (id, score) list must be
-- IDENTICAL to the serial load above.
set fulltext2_threads_search = 8;
create table t2(id bigint primary key, body text, prio int);
insert into t2 values
  (1,'rare common data text word more',10),
  (2,'rare rare common data text word more',20),
  (3,'rare rare rare common data text word more',30),
  (4,'rare rare rare rare common data text word more',40),
  (5,'rare rare rare rare rare common data text word more',50),
  (6,'rare rare rare rare rare rare common data text word more',60),
  (7,'common data text word more',70),
  (8,'common data text word more',80),
  (9,'common data text word more',90),
  (10,'common data text word more',100),
  (11,'common data text word more',110),
  (12,'common data text word more',120);
create fulltext2 index ft on t2(body) include(prio) max_postings_capacity 8;
-- @regex("Fulltext Index Scan on ft", true)
-- @regex("Join", false)
-- @regex("Candidate Limit: 3", true)
explain select id, match(body) against('rare' in bm25 mode) as score
  from t2 where match(body) against('rare' in bm25 mode)
  order by score desc limit 3;
select id, match(body) against('rare' in bm25 mode) as score
  from t2 where match(body) against('rare' in bm25 mode)
  order by score desc limit 3;

set fulltext2_threads_search = 0;
drop database ft2_threads;
