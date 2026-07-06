-- WAND "retrieval" fulltext index with a DATETIME primary key. The ISCP CDC now
-- delivers a datetime pk in its NATIVE form (extractRowFromVector ReprNative); the
-- WAND codec stores it as fixed-width raw bytes (encodePk) and decodes it back to a
-- datetime for the doc_id -> source join. Same corpus/queries as fulltext_retrieval;
-- only the pk type differs.
drop database if exists ft_retrieval_datetime;
create database ft_retrieval_datetime;
use ft_retrieval_datetime;

create table t (id datetime primary key, txt text);
insert into t values
 ('2020-06-01 10:00:01', '孩子 营养 早餐 视频 文案'),
 ('2020-06-01 10:00:02', '营养 早餐 健康 食谱'),
 ('2020-06-01 10:00:03', '视频 文案 创作 技巧'),
 ('2020-06-01 10:00:04', '孩子 教育 成长');

create fulltext index ft on t(txt) with parser retrieval;
show create table t;

-- wait for the async (CDC/ISCP) initial build from postings
select sleep(20);

-- ranked disjunctive top-K (ordered by id for a deterministic check)
select id from t where match(txt) against('营养 早餐') order by id;
select id from t where match(txt) against('视频 文案') order by id;
select id from t where match(txt) against('教育') order by id;
select id from t where match(txt) against('不存在的词') order by id;

drop database ft_retrieval_datetime;
