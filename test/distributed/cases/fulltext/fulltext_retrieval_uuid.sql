-- WAND "retrieval" fulltext index with a UUID primary key. The SQL-based CDC
-- delivers a uuid pk as its canonical TEXT string; the WAND codec stores that text
-- and decodes it back to a uuid for the doc_id -> source join. Same corpus/queries
-- as fulltext_retrieval; only the pk type differs.
drop database if exists ft_retrieval_uuid;
create database ft_retrieval_uuid;
use ft_retrieval_uuid;

create table t (id uuid primary key, txt text);
insert into t values
 ('00000000-0000-0000-0000-000000000001', '孩子 营养 早餐 视频 文案'),
 ('00000000-0000-0000-0000-000000000002', '营养 早餐 健康 食谱'),
 ('00000000-0000-0000-0000-000000000003', '视频 文案 创作 技巧'),
 ('00000000-0000-0000-0000-000000000004', '孩子 教育 成长');

create fulltext index ft on t(txt) with parser retrieval;
show create table t;

-- wait for the async (CDC/ISCP) initial build from postings
select sleep(20);

-- ranked disjunctive top-K (ordered by id for a deterministic check)
select id from t where match(txt) against('营养 早餐') order by id;
select id from t where match(txt) against('视频 文案') order by id;
select id from t where match(txt) against('教育') order by id;
select id from t where match(txt) against('不存在的词') order by id;

drop database ft_retrieval_uuid;
