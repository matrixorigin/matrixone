-- CREATE TABLE ... CLONE of a bm25 index: the clone's index is rebuilt from the
-- cloned rows via the re-armed CDC's InitSQL (RestoreInitSQL), and post-clone
-- rows flow in via CDC. Ported from fulltext_retrieval_clone.
drop database if exists bm25_clone;
create database bm25_clone;
use bm25_clone;
create table t (id bigint primary key, txt text);
insert into t values (1,'apple banana'),(2,'banana cherry'),(3,'cherry date'),(4,'date apple');
create index ft using bm25 on t(txt) with parser gojieba max_index_capacity=2;
select id from t where bm25(txt) against('apple') order by id;
create table t2 clone t;
insert into t2 values (5,'apple mango');
select sleep(60);
select id from t2 where bm25(txt) against('apple') order by id;
select id from t2 where bm25(txt) against('mango') order by id;
drop database bm25_clone;
