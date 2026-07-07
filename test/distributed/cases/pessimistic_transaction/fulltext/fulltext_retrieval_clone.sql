-- CREATE TABLE ... CLONE of a WAND "retrieval" fulltext index. The block-clone copies
-- all three hidden tables (postings + ft_index tag=0 base + ft_meta); Scope.RestoreTable
-- then rebuilds the tag=0 base from the cloned rows via the plugin RestoreInitSQL
-- (ALTER ... REINDEX ... FULLTEXT FORCE_SYNC, run as the re-armed CDC's InitSQL),
-- discarding the seed+clone duplicate. Post-clone rows flow in via the re-armed CDC.
drop database if exists ft_clone;
create database ft_clone;
use ft_clone;
set fulltext_max_index_capacity = 2;
create table t (id bigint primary key, txt text);
insert into t values (1,'apple banana'),(2,'banana cherry'),(3,'cherry date'),(4,'date apple');
create fulltext index ft on t(txt) with parser retrieval;
select id from t where match(txt) against('apple' in retrieval mode) order by id;
create table t2 clone t;
-- wait for RestoreTable's async reindex (InitSQL) to rebuild the cloned tag=0 base
select sleep(30);
-- (a) cloned + rebuilt tag=0 base answers MATCH
select id from t2 where match(txt) against('apple' in retrieval mode) order by id;
-- (b) CDC re-armed on the clone: a post-clone insert becomes matchable
insert into t2 values (5,'apple mango');
select sleep(30);
select id from t2 where match(txt) against('apple' in retrieval mode) order by id;
drop database ft_clone;
