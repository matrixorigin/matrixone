-- ALTER ... REINDEX ... BM25 rebuilds the whole index from source (re-tokenize),
-- and can change max_index_capacity; a non-bm25 option (lists) is rejected.
-- Ported from fulltext_retrieval_reindex (with parser retrieval -> using bm25;
-- reindex ... fulltext -> reindex ... bm25; IN RETRIEVAL MODE -> default).
drop database if exists bm25_reindex;
create database bm25_reindex;
use bm25_reindex;
create table t (id bigint primary key, txt text);
insert into t values (1,'apple banana'),(2,'banana cherry'),(3,'cherry date'),(4,'date apple');
create index ft using bm25 on t(txt) with parser gojieba max_index_capacity=2;
insert into t values (5,'fig grape'),(6,'grape apple');
alter table t alter reindex ft bm25;
select id from t where bm25(txt) against('apple') order by id;
alter table t alter reindex ft bm25 max_index_capacity=3;
select id from t where bm25(txt) against('apple') order by id;
alter table t alter reindex ft bm25 lists=5;
drop database bm25_reindex;
