-- =====================================================================
-- vector_ivfpq_ddl.sql — DDL/DML lifecycle on a IVFPQ-indexed table
--
-- GPU REQUIRED. Exercises SQL operations against a table carrying a IVFPQ
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

SET experimental_ivfpq_index = 1;
SET ivfpq_threads_build = 6;
SET ivfpq_max_index_capacity = 99999;
SET kmeans_train_percent = 100;
SET probe_limit = 16;

drop database if exists ivfpq_ddl;
create database ivfpq_ddl;
use ivfpq_ddl;

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

create index ix using ivfpq on t (v) op_type 'vector_l2_ops' lists=2 m=8 bits_per_code=8;

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
create index ix using ivfpq on t (v) op_type 'vector_l2_ops' lists=2 m=8 bits_per_code=8;
select id from t order by l2_distance(v, '[5,5,5,5,5,5,5,5]') asc limit 1;

drop database ivfpq_ddl;
