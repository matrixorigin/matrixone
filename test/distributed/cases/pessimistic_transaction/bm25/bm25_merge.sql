-- ALTER ... REINDEX ... BM25 MERGE folds the tag=1 CdcTail into the tag=0 base
-- (incremental compaction, no re-tokenize). Ported from fulltext_retrieval_merge
-- (bm25 part only; the classic-fulltext table is dropped).
drop database if exists bm25_merge;
create database bm25_merge;
use bm25_merge;
create table t (id bigint primary key, txt text);
insert into t values (1,'apple red'),(2,'apple green'),(3,'apple blue');
create index ft using bm25 on t(txt) with parser gojieba;
insert into t values (4,'apple yellow');
delete from t where id=2;
select sleep(30);
alter table t alter reindex ft bm25 merge;
insert into t values (5,'apple pink');
update t set txt='apple orange' where id=4;
select sleep(30);
alter table t alter reindex ft bm25 merge;
select id from t where match(txt) against('apple') order by id;
select id from t where match(txt) against('green') order by id;
select id from t where match(txt) against('orange') order by id;
select id from t where match(txt) against('yellow') order by id;
drop database bm25_merge;
