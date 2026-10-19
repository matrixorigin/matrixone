-- #30402: fulltext2_threads_search session variable sizes the multi-segment parallel
-- top-k search. It is a perf-only hint: any value yields identical ranked results.
set experimental_fulltext2_index = 1;
drop database if exists ft2_threads;
create database ft2_threads;
use ft2_threads;

-- default is 0 (use GOMAXPROCS), and the variable is settable in [0,1024].
select @@fulltext2_threads_search as default_val;
set fulltext2_threads_search = 4;
select @@fulltext2_threads_search as after_set;

create table t(id bigint primary key, body text);
insert into t values
  (1,'apple banana cherry'),(2,'banana cherry date'),(3,'cherry date elder'),
  (4,'apple elder fig'),(5,'banana fig grape');
create fulltext2 index ft on t(body);

-- parallel path (threads=4): IN BM25 MODE routes to the parallel multi-segment search.
select id, match(body) against('banana cherry' in bm25 mode) as score
  from t where match(body) against('banana cherry' in bm25 mode)
  order by score desc, id;

-- serial (threads=1): identical ranking.
set fulltext2_threads_search = 1;
select id, match(body) against('banana cherry' in bm25 mode) as score
  from t where match(body) against('banana cherry' in bm25 mode)
  order by score desc, id;

set fulltext2_threads_search = 0;
drop database ft2_threads;
