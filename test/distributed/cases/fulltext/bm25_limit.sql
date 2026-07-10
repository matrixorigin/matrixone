-- bm25 ranked top-K: LIMIT pushdown returns the highest-scored docs. Ported from
-- fulltext_retrieval_limit.sql (IN RETRIEVAL MODE -> default).
drop database if exists bm25_limit;
create database bm25_limit;
use bm25_limit;
create table t (id bigint primary key, txt text);
insert into t values
 (1, '营养 天气 城市'),
 (2, '营养 早餐 城市'),
 (3, '营养 早餐 视频'),
 (4, '视频 天气 城市'),
 (5, '教育 成长 学习');
create index ft using bm25 on t(txt) with parser gojieba;
select id from t where bm25(txt) against('营养 早餐 视频') order by id;
select id from t where bm25(txt) against('营养 早餐 视频') limit 1;
select id from t where bm25(txt) against('营养 早餐 视频') limit 2;
select id from t where bm25(txt) against('营养 早餐 视频') limit 3;
select id from t where bm25(txt) against('营养 早餐 视频') limit 10;
select id from t where bm25(txt) against('营养') order by id;
select id from t where bm25(txt) against('不存在') limit 5;
drop database bm25_limit;
