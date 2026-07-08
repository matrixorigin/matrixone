-- CREATE TABLE ... CLONE of a WAND "retrieval" fulltext index. The block-clone copies
-- all three hidden tables (postings + ft_index tag=0 base + ft_meta); Scope.RestoreTable
-- then rebuilds the tag=0 base from the cloned rows via the plugin RestoreInitSQL
-- (ALTER ... REINDEX ... FULLTEXT FORCE_SYNC, run as the re-armed CDC's InitSQL),
-- discarding the seed+clone duplicate. Post-clone rows flow in via the re-armed CDC.
-- The clone rebuild AND the post-clone CDC insert are both async; issue both up front,
-- then a SINGLE settle wait, and check the converged state.
drop database if exists ft_clone;
create database ft_clone;
use ft_clone;
create table t (id bigint primary key, txt text);
insert into t values (1,'apple banana'),(2,'banana cherry'),(3,'cherry date'),(4,'date apple');
create fulltext index ft on t(txt) with parser retrieval max_index_capacity=2;
select id from t where match(txt) against('apple' in retrieval mode) order by id;

-- clone, then a post-clone insert on the re-armed CDC, both async
create table t2 clone t;
insert into t2 values (5,'apple mango');

-- single settle wait for the clone's async reindex + the post-clone CDC insert
select sleep(60);

-- converged: cloned+rebuilt base (1,4) plus the post-clone insert (5)
select id from t2 where match(txt) against('apple' in retrieval mode) order by id;
select id from t2 where match(txt) against('mango' in retrieval mode) order by id;
drop database ft_clone;
