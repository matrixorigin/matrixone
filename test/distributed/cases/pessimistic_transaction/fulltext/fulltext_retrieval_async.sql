-- WAND "retrieval" fulltext index is always async: INSERT/DELETE after CREATE flow
-- through the ISCP sinker into tag=1 CdcTail frames and become searchable. All async
-- writes are issued up front, then a SINGLE settle wait, so the checks observe the final
-- converged state (inserts visible, delete applied) without racing each intermediate step.
drop database if exists ft_ret_async;
create database ft_ret_async;
use ft_ret_async;

create table t (id bigint primary key, txt text);
create fulltext index ft on t(txt) with parser retrieval;

-- all CDC writes up front: initial inserts, a follow-up insert, and a delete of id=1
insert into t values (1, '营养 早餐'), (2, '视频 文案');
insert into t values (3, '营养 健康 食谱');
delete from t where id = 1;

-- single settle wait for the ISCP sinker to converge the tag=1 tail
select sleep(60);

-- final converged state: id=1 deleted, id=2/3 inserted and searchable
select id from t where match(txt) against('营养') order by id;
select id from t where match(txt) against('视频') order by id;
select id from t where match(txt) against('健康') order by id;

drop database ft_ret_async;
