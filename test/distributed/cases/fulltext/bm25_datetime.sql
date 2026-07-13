-- bm25 index on a DATETIME primary key (encodePk supports it). Ported from
-- fulltext_retrieval_datetime.sql.
drop database if exists bm25_datetime;
create database bm25_datetime;
use bm25_datetime;
set experimental_bm25_index = 1;
create table t (id datetime primary key, txt text);
insert into t values
 ('2020-06-01 10:00:01', '孩子 营养 早餐 视频 文案'),
 ('2020-06-01 10:00:02', '营养 早餐 健康 食谱'),
 ('2020-06-01 10:00:03', '视频 文案 创作 技巧'),
 ('2020-06-01 10:00:04', '孩子 教育 成长');
create index ft using bm25 on t(txt) with parser gojieba;
select id from t where bm25(txt) against('营养 早餐');
select id from t where bm25(txt) against('视频 文案');
select id from t where bm25(txt) against('教育');
select id from t where bm25(txt) against('不存在的词');
drop database bm25_datetime;
