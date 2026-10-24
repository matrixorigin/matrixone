-- #29889 / #30987: the HNSW index is maintained asynchronously by ISCP. The query
-- searches the index and never falls back to a full table scan unless mode=force is
-- requested. When a committed row becomes visible through the index is not asserted:
-- it needs ISCP to apply it and the CN's index cache to load the new version.
set experimental_hnsw_index = 1;

drop database if exists issue_29889_hnsw;
create database issue_29889_hnsw;
use issue_29889_hnsw;

create table t (id bigint primary key, v vecf32(2));
insert into t values (1, '[10,10]'), (2, '[20,20]');
create index idx_hnsw using hnsw on t(v) op_type 'vector_l2_ops';

-- Warm the initial generation before adding the row that exercises the gap.
select id from t order by l2_distance(v, '[0,0]') limit 1;

insert into t values (3, '[0,0]');

-- mode=force scans the table and sees the row without waiting.
select id from t order by l2_distance(v, '[0,0]') limit 1 by rank with option 'mode=force';
-- The plan stays on the index in every state below: a brute-force plan sorts by
-- l2_distance over a table scan and has no Vector Index Scan.
-- @regex("Vector Index Scan on idx_hnsw", true)
-- @regex("Sort Key: l2_distance", false)
explain select id from t order by l2_distance(v, '[0,0]') limit 1;

-- Committed update and delete the index has not applied yet.
update t set v = '[1,1]' where id = 2;
delete from t where id = 1;
-- @regex("Vector Index Scan on idx_hnsw", true)
-- @regex("Sort Key: l2_distance", false)
explain select id from t order by l2_distance(v, '[0,0]') limit 1;

-- The transaction has written the source table.
begin;
insert into t values (4, '[2,2]');
-- @regex("Vector Index Scan on idx_hnsw", true)
-- @regex("Sort Key: l2_distance", false)
explain select id from t order by l2_distance(v, '[0,0]') limit 1;
update t set v = '[3,3]' where id = 4;
-- @regex("Vector Index Scan on idx_hnsw", true)
-- @regex("Sort Key: l2_distance", false)
explain select id from t order by l2_distance(v, '[0,0]') limit 1;
delete from t where id = 4;
-- @regex("Vector Index Scan on idx_hnsw", true)
-- @regex("Sort Key: l2_distance", false)
explain select id from t order by l2_distance(v, '[0,0]') limit 1;
rollback;

-- With a filter on a non-indexed column.
-- @regex("Vector Index Scan on idx_hnsw", true)
-- @regex("Sort Key: l2_distance", false)
explain select id from t where id > 0 order by l2_distance(v, '[0,0]') limit 1;

-- mode=force is the only brute-force plan.
-- @regex("Sort Key: l2_distance", true)
-- @regex("Vector Index Scan", false)
explain select id from t order by l2_distance(v, '[0,0]') limit 1 by rank with option 'mode=force';

-- Each statement of docs/cn/vector_search.md "异步维护的向量索引", checked from SQL for HNSW.
-- A transaction's uncommitted writes are never in the index, so they hold the index
-- behind the source deterministically. Each index query is paired with mode=force.
create table t2 (id bigint primary key, v vecf32(2));
insert into t2 values (1, '[1,1]'),(2, '[2,2]'),(3, '[3,3]'),(4, '[4,4]'),(5, '[5,5]'),(6, '[6,6]'),(7, '[7,7]'),(8, '[8,8]'),(9, '[9,9]'),(10, '[10,10]'),(11, '[11,11]'),(12, '[12,12]'),(13, '[13,13]'),(14, '[14,14]'),(15, '[15,15]'),(16, '[16,16]'),(17, '[17,17]'),(18, '[18,18]'),(19, '[19,19]'),(20, '[20,20]');
create index idx_hnsw2 using hnsw on t2(v) op_type 'vector_l2_ops';
-- @regex("Vector Index Scan on idx_hnsw2", true)
-- @regex("Sort Key: l2_distance", false)
explain select id from t2 order by l2_distance(v, '[0,0]') limit 1;
select id, l2_distance(v, '[0,0]') as d from t2 order by d limit 3;

-- A row not in the index is not returned; mode=force returns it.
begin;
insert into t2 values (21, '[0,0]');
select id from t2 order by l2_distance(v, '[0,0]') limit 1;
select id from t2 order by l2_distance(v, '[0,0]') limit 1 by rank with option 'mode=force';
rollback;

-- Rows deleted from the source but still in the index are dropped by the join, so the
-- page has fewer than LIMIT rows although 5 rows remain; mode=force fills it.
begin;
delete from t2 where id <= 15;
-- @regex("Vector Index Scan on idx_hnsw2", true)
-- @regex("Sort Key: l2_distance", false)
explain select id from t2 order by l2_distance(v, '[0,0]') limit 2;
select count(*) from t2;
select id from t2 order by l2_distance(v, '[0,0]') limit 2;
select id from t2 order by l2_distance(v, '[0,0]') limit 2 by rank with option 'mode=force';
rollback;

-- A row whose vector changed is ranked and returned with the indexed vector's distance;
-- mode=force uses the current vector.
begin;
update t2 set v = '[100,100]' where id = 1;
select id, v, l2_distance(v, '[0,0]') as d from t2 order by d limit 2;
select id, v, l2_distance(v, '[0,0]') as d from t2 order by d limit 2 by rank with option 'mode=force';
rollback;

-- Committed writes behave the same until ISCP applies them; when that happens is not
-- asserted. The index is never bypassed: mode=force sees the committed row at once.
insert into t2 values (22, '[0,0]');
select id from t2 order by l2_distance(v, '[0,0]') limit 1 by rank with option 'mode=force';
-- @regex("Vector Index Scan on idx_hnsw2", true)
-- @regex("Sort Key: l2_distance", false)
explain select id from t2 order by l2_distance(v, '[0,0]') limit 1;

drop database issue_29889_hnsw;
set experimental_hnsw_index = 0;
