-- =====================================================================
-- vector_cagra_ddl.sql — DDL/DML lifecycle on a CAGRA-indexed table
--
-- GPU REQUIRED. Exercises SQL operations against a table carrying a CAGRA
-- vector index. ALTER / TRUNCATE rewrite the base table and the index is
-- repopulated through the ISCP/CDC stream, so each such op is followed by
-- SELECT SLEEP(30) before the verifying search (the index is transiently empty
-- until CDC catches up). Lives under pessimistic_transaction/ for that reason.
--
-- Steps (query [5]*8 -> id 5 at every stable point):
--   1. baseline search
--   2. ALTER TABLE ADD COLUMN  -> CDC rebuild -> search recovers
--   3. ALTER TABLE DROP COLUMN -> CDC rebuild -> search recovers
--   4. TRUNCATE TABLE          -> index emptied (count 0)
--   5. re-INSERT               -> CDC rebuild -> search recovers
--   6. reindex (DROP + CREATE INDEX, synchronous) -> search works immediately
-- =====================================================================

SET experimental_cagra_index = 1;
SET cagra_threads_build = 7;
SET cagra_max_index_capacity = 99999;

drop database if exists cagra_ddl;
create database cagra_ddl;
use cagra_ddl;

create table t (id bigint primary key, v vecf32(8), tag int default 0);
insert into t values
    (1, '[1,1,1,1,1,1,1,1]', 0),
    (2, '[2,2,2,2,2,2,2,2]', 0),
    (3, '[3,3,3,3,3,3,3,3]', 0),
    (4, '[4,4,4,4,4,4,4,4]', 0),
    (5, '[5,5,5,5,5,5,5,5]', 0),
    (6, '[6,6,6,6,6,6,6,6]', 0),
    (7, '[7,7,7,7,7,7,7,7]', 0),
    (8, '[8,8,8,8,8,8,8,8]', 0),
    (9, '[9,9,9,9,9,9,9,9]', 0),
    (10, '[10,10,10,10,10,10,10,10]', 0),
    (11, '[11,11,11,11,11,11,11,11]', 0),
    (12, '[12,12,12,12,12,12,12,12]', 0),
    (13, '[13,13,13,13,13,13,13,13]', 0),
    (14, '[14,14,14,14,14,14,14,14]', 0),
    (15, '[15,15,15,15,15,15,15,15]', 0),
    (16, '[16,16,16,16,16,16,16,16]', 0),
    (17, '[17,17,17,17,17,17,17,17]', 0),
    (18, '[18,18,18,18,18,18,18,18]', 0),
    (19, '[19,19,19,19,19,19,19,19]', 0),
    (20, '[20,20,20,20,20,20,20,20]', 0);

create index ix using cagra on t (v) op_type 'vector_l2_ops' intermediate_graph_degree=16 graph_degree=8 itopk_size=32;

-- 1. baseline
select id from t order by l2_distance(v, '[5,5,5,5,5,5,5,5]') asc limit 1;

-- 2. ALTER ADD COLUMN -> CDC rebuild
alter table t add column extra int default 7;
select sleep(30);
select id from t order by l2_distance(v, '[5,5,5,5,5,5,5,5]') asc limit 1;

-- 3. ALTER DROP COLUMN -> CDC rebuild
alter table t drop column extra;
select sleep(30);
select id from t order by l2_distance(v, '[5,5,5,5,5,5,5,5]') asc limit 1;

-- 4. TRUNCATE -> index emptied
truncate table t;
select count(*) from t;

-- 5. re-INSERT -> CDC rebuild
insert into t values
    (1, '[1,1,1,1,1,1,1,1]', 0),
    (2, '[2,2,2,2,2,2,2,2]', 0),
    (3, '[3,3,3,3,3,3,3,3]', 0),
    (4, '[4,4,4,4,4,4,4,4]', 0),
    (5, '[5,5,5,5,5,5,5,5]', 0),
    (6, '[6,6,6,6,6,6,6,6]', 0),
    (7, '[7,7,7,7,7,7,7,7]', 0),
    (8, '[8,8,8,8,8,8,8,8]', 0),
    (9, '[9,9,9,9,9,9,9,9]', 0),
    (10, '[10,10,10,10,10,10,10,10]', 0),
    (11, '[11,11,11,11,11,11,11,11]', 0),
    (12, '[12,12,12,12,12,12,12,12]', 0),
    (13, '[13,13,13,13,13,13,13,13]', 0),
    (14, '[14,14,14,14,14,14,14,14]', 0),
    (15, '[15,15,15,15,15,15,15,15]', 0),
    (16, '[16,16,16,16,16,16,16,16]', 0),
    (17, '[17,17,17,17,17,17,17,17]', 0),
    (18, '[18,18,18,18,18,18,18,18]', 0),
    (19, '[19,19,19,19,19,19,19,19]', 0),
    (20, '[20,20,20,20,20,20,20,20]', 0);
select sleep(30);
select count(*) from t;
select id from t order by l2_distance(v, '[5,5,5,5,5,5,5,5]') asc limit 1;

-- 6. reindex (synchronous drop + recreate)
drop index ix on t;
create index ix using cagra on t (v) op_type 'vector_l2_ops' intermediate_graph_degree=16 graph_degree=8 itopk_size=32;
select id from t order by l2_distance(v, '[5,5,5,5,5,5,5,5]') asc limit 1;

drop database cagra_ddl;
