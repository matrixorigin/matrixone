-- WAND "retrieval" fulltext index is always async: INSERT/DELETE after CREATE
-- flow through the ISCP sinker into tag=1 CdcTail frames and become searchable
-- (each build evicts the search cache so results appear without the idle TTL).
drop database if exists ft_ret_async;
create database ft_ret_async;
use ft_ret_async;

create table t (id bigint primary key, txt text);
create fulltext index ft on t(txt) with parser retrieval;

-- initial rows maintained via CDC (the postings/tag=0 build is empty at create)
insert into t values (1, '营养 早餐'), (2, '视频 文案');
select sleep(20);
select id from t where match(txt) against('营养') order by id;

-- CDC INSERT: a new row is picked up by the sinker (tag=1 segment frame)
insert into t values (3, '营养 健康 食谱');
select sleep(20);
select id from t where match(txt) against('营养') order by id;

-- CDC DELETE: the row is removed via a tag=1 delete frame
delete from t where id = 1;
select sleep(20);
select id from t where match(txt) against('营养') order by id;

drop database ft_ret_async;
