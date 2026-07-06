-- WAND "retrieval" fulltext index with fulltext_max_index_capacity forcing MULTIPLE
-- tag=0 base sub-indexes. capacity=2 over 6 docs => 3 capacity-bounded base
-- sub-indexes (each its own index_id, all in the one hidden store). Queries whose
-- matches SPAN sub-indexes (apple: docs 1 & 4; cherry: docs 2 & 3) verify the load
-- path enumerates + composes every sub-index and search runs across all of them.
drop database if exists ft_multibase;
create database ft_multibase;
use ft_multibase;
set fulltext_max_index_capacity = 2;
create table t (id bigint primary key, txt text);
insert into t values
 (1,'apple banana'),
 (2,'banana cherry'),
 (3,'cherry date'),
 (4,'date apple'),
 (5,'elderberry fig'),
 (6,'fig grape');
create fulltext index ft on t(txt) with parser retrieval;

-- wait for the async (CDC/ISCP) build to split + persist the sub-indexes
select sleep(20);

-- matches span sub-indexes {1,2},{3,4},{5,6}:
select id from t where match(txt) against('apple' in retrieval mode) order by id;
select id from t where match(txt) against('cherry' in retrieval mode) order by id;
select id from t where match(txt) against('banana' in retrieval mode) order by id;
select id from t where match(txt) against('fig' in retrieval mode) order by id;

drop database ft_multibase;
