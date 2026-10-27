-- docs/cn/vector_search.md, "异步维护的向量索引": an ASYNC IVFFLAT index is searched as it
-- is until ISCP applies a write; a synchronous IVFFLAT index reflects the transaction's own
-- writes at once. The transaction's uncommitted writes hold the async index behind
-- deterministically. Each index query is paired with mode=force as the oracle.
set experimental_ivf_index = 1;
drop database if exists vector_async_index_lag;
create database vector_async_index_lag;
use vector_async_index_lag;

create table ta (id bigint primary key, v vecf32(2));
insert into ta select result, concat('[', result, ',', result, ']') from generate_series(1, 20) g;
create index ia using ivfflat on ta(v) lists = 2 op_type 'vector_l2_ops' ASYNC;
-- @wait_expect(1, 120)
select id from ta order by l2_distance(v, '[0,0]') limit 2 by rank with option 'mode=post';

begin;
insert into ta values (100, '[0,0]');
delete from ta where id = 1;
update ta set v = '[100,100]' where id = 2;
-- @regex("Vector Index Scan on ia", true)
-- @regex("Sort Key: l2_distance", false)
explain select id from ta order by l2_distance(v, '[0,0]') limit 2 by rank with option 'mode=post';
-- 100 is not in the index, 1 is dropped by the join, 2 keeps its indexed distance: one row.
select id, v, l2_distance(v, '[0,0]') as d from ta order by d limit 2 by rank with option 'mode=post';
select id, v, l2_distance(v, '[0,0]') as d from ta order by d limit 2 by rank with option 'mode=force';
rollback;

create table ts (id bigint primary key, v vecf32(2));
insert into ts select result, concat('[', result, ',', result, ']') from generate_series(1, 20) g;
create index iss using ivfflat on ts(v) lists = 2 op_type 'vector_l2_ops';

begin;
insert into ts values (100, '[0,0]');
delete from ts where id = 1;
update ts set v = '[100,100]' where id = 2;
-- @regex("Vector Index Scan on iss", true)
explain select id from ts order by l2_distance(v, '[0,0]') limit 2 by rank with option 'mode=post';
-- The synchronous index already holds the transaction's writes: same rows as mode=force.
select id, v, l2_distance(v, '[0,0]') as d from ts order by d limit 2 by rank with option 'mode=post';
select id, v, l2_distance(v, '[0,0]') as d from ts order by d limit 2 by rank with option 'mode=force';
rollback;

drop database vector_async_index_lag;
set experimental_ivf_index = 0;
