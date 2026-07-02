-- WAND "retrieval" fulltext index: jieba-tokenized, BM25, in-operator top-K.
-- The index is always async (built from postings via ISCP at CREATE), so a
-- sleep lets the background build finish before querying.
drop database if exists ft_retrieval;
create database ft_retrieval;
use ft_retrieval;

create table t (id bigint primary key, txt text);
insert into t values
 (1, '孩子 营养 早餐 视频 文案'),
 (2, '营养 早餐 健康 食谱'),
 (3, '视频 文案 创作 技巧'),
 (4, '孩子 教育 成长');

create fulltext index ft on t(txt) with parser retrieval;
-- the parser is recorded on the index
show create table t;

-- wait for the async (CDC/ISCP) initial build from postings
select sleep(20);

-- ranked disjunctive top-K (results ordered by id for a deterministic check)
select id from t where match(txt) against('营养 早餐') order by id;
select id from t where match(txt) against('视频 文案') order by id;
select id from t where match(txt) against('教育') order by id;
select id from t where match(txt) against('不存在的词') order by id;

drop database ft_retrieval;
