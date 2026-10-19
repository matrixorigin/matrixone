-- #30402: fulltext2_threads_search sizes the multi-segment parallel top-k search.
-- It is a perf-only hint: any worker count yields identical ranked results. The worker
-- count is bound once when an index generation is first LOADED into the cache (warm-cache
-- ownership), so to actually take the serial AND the parallel path this test loads TWO
-- separate indexes, each first queried under its own setting -- not one warm cache re-read
-- under a changed knob (which would reuse the first load's value and prove nothing).
set experimental_fulltext2_index = 1;
drop database if exists ft2_threads;
create database ft2_threads;
use ft2_threads;

-- default is 0 (resolves to the deployment's effective parallelism), settable in [0,1024].
select @@fulltext2_threads_search as default_val;
set fulltext2_threads_search = 1024;
select @@fulltext2_threads_search as after_set;

-- SERIAL load: the knob is 1 BEFORE t1's index is first queried, so its cached generation
-- is built with one worker and the query actually runs the serial walk.
set fulltext2_threads_search = 1;
create table t1(id bigint primary key, body text);
insert into t1 values
  (1,'apple banana cherry'),(2,'banana cherry date'),(3,'cherry date elder'),
  (4,'apple elder fig'),(5,'banana fig grape');
create fulltext2 index ft on t1(body);
select id, match(body) against('banana cherry' in bm25 mode) as score
  from t1 where match(body) against('banana cherry' in bm25 mode)
  order by score desc, id;

-- PARALLEL load: a SEPARATE index first queried while the knob is 8, so its generation is
-- built parallel. The ranked (id, score) list must be IDENTICAL to the serial load above.
set fulltext2_threads_search = 8;
create table t2(id bigint primary key, body text);
insert into t2 values
  (1,'apple banana cherry'),(2,'banana cherry date'),(3,'cherry date elder'),
  (4,'apple elder fig'),(5,'banana fig grape');
create fulltext2 index ft on t2(body);
select id, match(body) against('banana cherry' in bm25 mode) as score
  from t2 where match(body) against('banana cherry' in bm25 mode)
  order by score desc, id;

set fulltext2_threads_search = 0;
drop database ft2_threads;
