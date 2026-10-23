-- #29889 / #30987: the HNSW index is maintained asynchronously, so a committed row
-- becomes visible through the index once ISCP applies it. The query keeps using the
-- index; it never falls back to a full table scan unless mode=force is requested.
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

-- @wait_expect(1, 60)
select id from t order by l2_distance(v, '[0,0]') limit 1;
-- mode=force scans the table and sees the row without waiting.
select id from t order by l2_distance(v, '[0,0]') limit 1 by rank with option 'mode=force';
-- @regex("Index Search Scan|Vector Index Scan|hnsw",true)
explain select id from t order by l2_distance(v, '[0,0]') limit 1;

-- Rows this transaction deleted are still in the index: the search keeps room for each of
-- them, so deleting the 15 nearest rows inside the transaction still returns row 16.
create table t2 (id bigint primary key, v vecf32(2));
insert into t2 values (1, '[1,1]'),(2, '[2,2]'),(3, '[3,3]'),(4, '[4,4]'),(5, '[5,5]'),(6, '[6,6]'),(7, '[7,7]'),(8, '[8,8]'),(9, '[9,9]'),(10, '[10,10]'),(11, '[11,11]'),(12, '[12,12]'),(13, '[13,13]'),(14, '[14,14]'),(15, '[15,15]'),(16, '[16,16]'),(17, '[17,17]'),(18, '[18,18]'),(19, '[19,19]'),(20, '[20,20]');
create index idx_hnsw2 using hnsw on t2(v) op_type 'vector_l2_ops';
begin;
delete from t2 where id <= 15;
select id from t2 order by l2_distance(v, '[0,0]') limit 1;
select id from t2 order by l2_distance(v, '[0,0]') limit 2;
rollback;
select id from t2 order by l2_distance(v, '[0,0]') limit 1;

drop database issue_29889_hnsw;
set experimental_hnsw_index = 0;
