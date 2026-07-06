-- ALTER REINDEX for a WAND "retrieval" fulltext index. Rebuilds the tag=0 base
-- synchronously from the current source rows (folding in the tag=1 CDC tail), honoring
-- the current fulltext_max_index_capacity. Exercises the REINDEX grammar
-- (ALTER ... REINDEX <idx> FULLTEXT), the DDL dispatch gate, and the sync rebuild.
drop database if exists ft_reindex;
create database ft_reindex;
use ft_reindex;
set fulltext_max_index_capacity = 2;
create table t (id bigint primary key, txt text);
insert into t values (1,'apple banana'),(2,'banana cherry'),(3,'cherry date'),(4,'date apple');
create fulltext index ft on t(txt) with parser retrieval;
-- sync build: searchable immediately
select id from t where match(txt) against('apple' in retrieval mode) order by id;
-- more rows flow through CDC into the tag=1 tail
insert into t values (5,'fig grape'),(6,'grape apple');
select sleep(20);
select id from t where match(txt) against('apple' in retrieval mode) order by id;
-- reindex: rebuild tag=0 from all current rows (tail folded in), results unchanged
alter table t alter reindex ft fulltext;
select id from t where match(txt) against('apple' in retrieval mode) order by id;
drop database ft_reindex;
