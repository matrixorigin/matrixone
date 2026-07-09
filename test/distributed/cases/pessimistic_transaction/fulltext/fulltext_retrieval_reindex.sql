-- ALTER REINDEX for a WAND "retrieval" fulltext index: rebuilds the tag=0 base
-- SYNCHRONOUSLY from the current SOURCE rows (so no CDC-tail wait is needed), honoring
-- the index's max_index_capacity.
--
-- Multi-CN note: the per-CN search cache is not cross-CN invalidated on a CDC/reindex
-- write, so a MATCH issued *before* the reindex would cache a stale copy on the querying
-- CN and later read stale. We therefore issue NO MATCH until after the reindex — the
-- first MATCH is the querying CN's first load, so it reads the freshly rebuilt base.
drop database if exists ft_reindex;
create database ft_reindex;
use ft_reindex;
create table t (id bigint primary key, txt text);
insert into t values (1,'apple banana'),(2,'banana cherry'),(3,'cherry date'),(4,'date apple');
create fulltext index ft on t(txt) with parser retrieval max_index_capacity=2;
-- more rows (these would flow through CDC into the tail, but the reindex below rebuilds
-- the base directly from source, so they are picked up with no CDC wait)
insert into t values (5,'fig grape'),(6,'grape apple');
-- reindex: rebuild tag=0 from ALL current source rows (5,6 included)
alter table t alter reindex ft fulltext;
select id from t where match(txt) against('apple' in retrieval mode) order by id;
-- reindex can change max_index_capacity: repartition the base at the new value; same docs
alter table t alter reindex ft fulltext max_index_capacity=3;
select id from t where match(txt) against('apple' in retrieval mode) order by id;
-- a non-fulltext reindex option is rejected
alter table t alter reindex ft fulltext lists=5;
drop database ft_reindex;
