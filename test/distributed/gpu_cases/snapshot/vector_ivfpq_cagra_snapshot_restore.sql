-- IVF-PQ and CAGRA vector indexes survive snapshot + restore.
--
-- GPU-only: the index rebuilds on a GPU, so this case lives in gpu_cases.
-- Mirrors the data-level snapshot/restore pattern of
-- cases/snapshot/fulltext_snapshot_restore.sql: snapshot the table, mutate it,
-- then delete + re-insert the snapshot view and confirm the secondary index is
-- rebuilt to the snapshot state and still answers vector search. The
-- experimental flags are reset after each section.

drop database if exists vector_gpu_snap_db;
create database vector_gpu_snap_db;
use vector_gpu_snap_db;

-- ============================== IVF-PQ ==============================
set experimental_ivfpq_index = 1;

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

-- kmeans_train_percent given as a CREATE INDEX option so it persists into
-- algo_params and the index rebuild reproduces the create-time build.
create index ix using ivfpq on t_ivfpq (v)
    op_type 'vector_l2_ops' lists=10 m=8 bits_per_code=8
    kmeans_train_percent 100 kmeans_max_iteration 20 max_index_capacity 100;

-- index definition exists before the snapshot
show create table t_ivfpq;

-- snapshot the table state (20 rows), then mutate after the snapshot
create snapshot ivfpq_snap for account sys;
insert into t_ivfpq values (21, '[21,21,21,21,21,21,21,21]');

-- restore the snapshot view: wipe current rows and re-insert the snapshot
delete from t_ivfpq;
insert into t_ivfpq select * from vector_gpu_snap_db.t_ivfpq {snapshot = 'ivfpq_snap'};

-- the re-inserted rows reach the index via the async CDC tail; wait for it to
-- catch up so the search reflects the restored data.
select sleep(30);

-- verify: snapshot state (20 rows, row 21 gone), index def intact, searchable
select count(*) from t_ivfpq;
show create table t_ivfpq;
select a from t_ivfpq order by l2_distance(v, '[1,1,1,1,1,1,1,1]') asc limit 3;

drop snapshot if exists ivfpq_snap;
set experimental_ivfpq_index = 0;

-- ============================== CAGRA ==============================
set experimental_cagra_index = 1;

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

show create table t_cagra;

create snapshot cagra_snap for account sys;
insert into t_cagra values (21, '[21,21,21,21,21,21,21,21]');

delete from t_cagra;
insert into t_cagra select * from vector_gpu_snap_db.t_cagra {snapshot = 'cagra_snap'};

-- wait for the async CDC tail to catch up before searching
select sleep(30);

select count(*) from t_cagra;
show create table t_cagra;
select a from t_cagra order by l2_distance(v, '[1,1,1,1,1,1,1,1]') asc limit 3;

drop snapshot if exists cagra_snap;
set experimental_cagra_index = 0;

drop database vector_gpu_snap_db;
