-- Serialized account-RESTORE of IVF-PQ and CAGRA — one index reindexed at a time.
--
-- The combined vector_ivfpq_cagra_account_restore puts both indexes in ONE
-- account, so `restore account` registers a CDC task per index and the CDC
-- scheduler fires both InitSQL reindexes (ALTER … REINDEX … FORCE_SYNC) at the
-- same instant → two concurrent cuVS GPU builds → crash.
--
-- This case isolates each algorithm in its OWN account and restores them
-- sequentially: each restore's single reindex is given a sleep(30) to finish
-- (and the account is dropped) before the next account is set up and restored,
-- so the GPU only ever runs one reindex at a time. GPU-only.

-- ========================== IVF-PQ ==========================
drop account if exists acc_ivfpq;
create account acc_ivfpq ADMIN_NAME 'admin1' IDENTIFIED BY 'test123';

-- @session:id=2&user=acc_ivfpq:admin1&password=test123
set experimental_ivfpq_index = 1;
create database vdb;
use vdb;
create table t_ivfpq (a bigint primary key, v vecf32(8));
insert into t_ivfpq values
    ( 1, '[1,1,1,1,1,1,1,1]'),         ( 2, '[2,2,2,2,2,2,2,2]'),
    ( 3, '[3,3,3,3,3,3,3,3]'),         ( 4, '[4,4,4,4,4,4,4,4]'),
    ( 5, '[5,5,5,5,5,5,5,5]'),         ( 6, '[6,6,6,6,6,6,6,6]'),
    ( 7, '[7,7,7,7,7,7,7,7]'),         ( 8, '[8,8,8,8,8,8,8,8]'),
    ( 9, '[9,9,9,9,9,9,9,9]'),         (10, '[10,10,10,10,10,10,10,10]'),
    (11, '[11,11,11,11,11,11,11,11]'), (12, '[12,12,12,12,12,12,12,12]'),
    (13, '[13,13,13,13,13,13,13,13]'), (14, '[14,14,14,14,14,14,14,14]'),
    (15, '[15,15,15,15,15,15,15,15]'), (16, '[16,16,16,16,16,16,16,16]'),
    (17, '[17,17,17,17,17,17,17,17]'), (18, '[18,18,18,18,18,18,18,18]'),
    (19, '[19,19,19,19,19,19,19,19]'), (20, '[20,20,20,20,20,20,20,20]');
-- k-means + capacity given as CREATE INDEX options so they persist into
-- algo_params and the account-restore reindex reproduces the create-time build
-- (kmeans_train_percent=100 trains on every row → deterministic exact-match search).
create index ix using ivfpq on t_ivfpq (v)
    op_type 'vector_l2_ops' lists=10 m=8 bits_per_code=8
    kmeans_train_percent 100 kmeans_max_iteration 20 max_index_capacity 100;
select a from t_ivfpq order by l2_distance(v, '[1,1,1,1,1,1,1,1]') asc limit 1;
select a from t_ivfpq order by l2_distance(v, '[20,20,20,20,20,20,20,20]') asc limit 1;
-- @session

drop snapshot if exists ivfpq_snap;
create snapshot ivfpq_snap for account acc_ivfpq;

-- @session:id=2&user=acc_ivfpq:admin1&password=test123
set experimental_ivfpq_index = 0;
drop database vdb;
-- @session

restore account acc_ivfpq{snapshot="ivfpq_snap"} to account acc_ivfpq;

-- @session:id=2&user=acc_ivfpq:admin1&password=test123
use vdb;
-- probe_limit >= lists makes ivfpq scan every cell, so a limit-1 query for an
-- exact-match vector returns that row deterministically (mirrors vector_ivfpq).
set probe_limit = 16;
select count(*) from t_ivfpq;
show create table t_ivfpq;
select sleep(30);
select a from t_ivfpq order by l2_distance(v, '[1,1,1,1,1,1,1,1]') asc limit 1;
select a from t_ivfpq order by l2_distance(v, '[20,20,20,20,20,20,20,20]') asc limit 1;
-- @session

drop snapshot if exists ivfpq_snap;
drop account if exists acc_ivfpq;

-- ========================== CAGRA ==========================
drop account if exists acc_cagra;
create account acc_cagra ADMIN_NAME 'admin1' IDENTIFIED BY 'test123';

-- @session:id=3&user=acc_cagra:admin1&password=test123
set experimental_cagra_index = 1;
create database vdb;
use vdb;
create table t_cagra (a bigint primary key, v vecf32(8));
insert into t_cagra values
    ( 1, '[1,1,1,1,1,1,1,1]'),         ( 2, '[2,2,2,2,2,2,2,2]'),
    ( 3, '[3,3,3,3,3,3,3,3]'),         ( 4, '[4,4,4,4,4,4,4,4]'),
    ( 5, '[5,5,5,5,5,5,5,5]'),         ( 6, '[6,6,6,6,6,6,6,6]'),
    ( 7, '[7,7,7,7,7,7,7,7]'),         ( 8, '[8,8,8,8,8,8,8,8]'),
    ( 9, '[9,9,9,9,9,9,9,9]'),         (10, '[10,10,10,10,10,10,10,10]'),
    (11, '[11,11,11,11,11,11,11,11]'), (12, '[12,12,12,12,12,12,12,12]'),
    (13, '[13,13,13,13,13,13,13,13]'), (14, '[14,14,14,14,14,14,14,14]'),
    (15, '[15,15,15,15,15,15,15,15]'), (16, '[16,16,16,16,16,16,16,16]'),
    (17, '[17,17,17,17,17,17,17,17]'), (18, '[18,18,18,18,18,18,18,18]'),
    (19, '[19,19,19,19,19,19,19,19]'), (20, '[20,20,20,20,20,20,20,20]');
create index ix using cagra on t_cagra (v)
    op_type 'vector_l2_ops' intermediate_graph_degree=16 graph_degree=8 itopk_size=32
    max_index_capacity 100;
-- @session

drop snapshot if exists cagra_snap;
create snapshot cagra_snap for account acc_cagra;

-- @session:id=3&user=acc_cagra:admin1&password=test123
set experimental_cagra_index = 0;
drop database vdb;
-- @session

restore account acc_cagra{snapshot="cagra_snap"} to account acc_cagra;

-- @session:id=3&user=acc_cagra:admin1&password=test123
use vdb;
-- cagra's itopk_size=32 (> row count) visits every node, so a limit-1 query for
-- an exact-match vector is deterministic (mirrors vector_cagra).
select count(*) from t_cagra;
show create table t_cagra;
select sleep(30);
select a from t_cagra order by l2_distance(v, '[1,1,1,1,1,1,1,1]') asc limit 1;
select a from t_cagra order by l2_distance(v, '[20,20,20,20,20,20,20,20]') asc limit 1;
-- @session

drop snapshot if exists cagra_snap;
drop account if exists acc_cagra;
