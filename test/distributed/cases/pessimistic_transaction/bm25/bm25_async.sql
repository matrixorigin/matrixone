-- bm25 ranked-retrieval index: post-create DML (INSERT/DELETE) flows into the
-- tag=1 CdcTail via the WAND sinker and is visible after the CDC settles.
-- Ported from pessimistic_transaction/fulltext/fulltext_retrieval_async.sql
-- (create fulltext ... with parser retrieval -> create index ... using bm25;
-- IN RETRIEVAL MODE -> default ranked mode).
drop database if exists bm25_async;
create database bm25_async;
use bm25_async;
create table t (id bigint primary key, txt text);
create index ft using bm25 on t(txt) with parser gojieba;
insert into t values (1, '营养 早餐'), (2, '视频 文案');
insert into t values (3, '营养 健康 食谱');
delete from t where id = 1;
select sleep(30);
select id from t where match(txt) against('营养') order by id;
select id from t where match(txt) against('视频') order by id;
select id from t where match(txt) against('健康') order by id;
drop database bm25_async;
