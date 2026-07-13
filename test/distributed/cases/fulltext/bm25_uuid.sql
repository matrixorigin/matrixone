-- bm25 index on a UUID primary key (encodePk supports it). Ported from
-- fulltext_retrieval_uuid.sql.
drop database if exists bm25_uuid;
create database bm25_uuid;
use bm25_uuid;
set experimental_bm25_index = 1;
create table t (id uuid primary key, txt text);
insert into t values
 ('00000000-0000-0000-0000-000000000001', '孩子 营养 早餐 视频 文案'),
 ('00000000-0000-0000-0000-000000000002', '营养 早餐 健康 食谱'),
 ('00000000-0000-0000-0000-000000000003', '视频 文案 创作 技巧'),
 ('00000000-0000-0000-0000-000000000004', '孩子 教育 成长');
create index ft using bm25 on t(txt) with parser gojieba;
select id from t where bm25(txt) against('营养 早餐');
select id from t where bm25(txt) against('视频 文案');
select id from t where bm25(txt) against('教育');
select id from t where bm25(txt) against('不存在的词');
drop database bm25_uuid;
