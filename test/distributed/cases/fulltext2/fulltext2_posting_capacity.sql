-- fulltext2 max_postings_capacity: a segment seals on whichever cap (docs OR
-- postings) is reached first. max_index_capacity (docs) alone is a poor memory
-- proxy — a doc can hold one token or thousands — so max_postings_capacity bounds
-- per-segment build memory by term-occurrence count. Here a TINY posting cap forces
-- the tag=0 base to split into several segments; the multi-segment split is
-- transparent to MATCH (the Index spans segments), so results are unchanged.

drop database if exists ft2_postcap;
create database ft2_postcap;
use ft2_postcap;
set experimental_fulltext2_index = 1;

drop table if exists docs;
create table docs(id bigint primary key, body text);
insert into docs values
 (0,'the quick brown fox jumps'),
 (1,'a quick brown dog runs'),
 (2,'the lazy fox sleeps here'),
 (3,'brown bear and lazy cat'),
 (4,'quick quick quick fox fox');

-- max_postings_capacity 8: ~5 postings/doc => a fresh segment seals about every 2
-- docs, so the base is built as multiple tag=0 sub-indexes rather than one.
create fulltext2 index ft on docs(body) max_index_capacity 1000000 max_postings_capacity 8;

-- Both caps persist in the index's algo_params (SHOW CREATE renders only the parser
-- for a fulltext family index, so assert the params via mo_indexes).
select distinct algo, algo_params from mo_catalog.mo_indexes where name = 'ft';

-- Queries span every sealed segment (multi-segment split is transparent).
select id from docs where match(body) against('quick brown fox') order by id;
select id from docs where match(body) against('brown') order by id;
select id from docs where match(body) against('+quick +fox' in boolean mode) order by id;
select id from docs where match(body) against('lazy') order by id;

-- REBUILD (no MERGE) re-derives the base from source under the same posting cap.
alter table docs alter reindex ft fulltext2;
select id from docs where match(body) against('brown') order by id;

-- ALTER REINDEX can RE-BOUND the posting cap: the override is merged into algo_params
-- and the rebuild seals at the new value (max_index_capacity stays as-is).
alter table docs alter reindex ft fulltext2 max_postings_capacity 4000000;
select distinct algo_params from mo_catalog.mo_indexes where name = 'ft' and table_id = (select rel_id from mo_catalog.mo_tables where relname = 'docs');
select id from docs where match(body) against('brown') order by id;

-- A generous posting cap keeps the base as a single segment; results identical.
drop table if exists docs2;
create table docs2(id bigint primary key, body text);
insert into docs2 values
 (0,'the quick brown fox jumps'),
 (1,'a quick brown dog runs'),
 (2,'the lazy fox sleeps here');
create fulltext2 index ft on docs2(body) max_postings_capacity 8000000;
select distinct algo_params from mo_catalog.mo_indexes where name = 'ft' and table_id = (select rel_id from mo_catalog.mo_tables where relname = 'docs2');
select id from docs2 where match(body) against('brown') order by id;

-- Guard: max_postings_capacity must be > 0.
create table bad(id bigint primary key, body text);
create fulltext2 index ftbad on bad(body) max_postings_capacity 0;

drop database ft2_postcap;
