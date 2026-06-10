-- Full account RESTORE of IVF-PQ / CAGRA indexes — the real restore path that
-- recreates the table AND replays CREATE INDEX in a background context. GPU-only.
--
-- Flow (mirrors cases/snapshot/snapshot_restore_view.sql): a sub-account owns a
-- GPU vector index; sys snapshots the account; the account drops its database;
-- sys runs `restore account ... to account ...`, which re-creates the table and
-- its ivfpq/cagra index from the snapshot. Because restore runs as a background
-- re-entry (IsFrontend()=false), the experimental-flag gate is skipped — the
-- restore succeeds and rebuilds the index even though the account turned the
-- flag off before the restore.

drop account if exists acc_vec;
create account acc_vec ADMIN_NAME 'admin1' IDENTIFIED BY 'test123';

-- @session:id=2&user=acc_vec:admin1&password=test123
set experimental_ivfpq_index = 1;
set experimental_cagra_index = 1;
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
create index ix using ivfpq on t_ivfpq (v)
    op_type 'vector_l2_ops' lists=10 m=8 bits_per_code=8;

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
    op_type 'vector_l2_ops' intermediate_graph_degree=16 graph_degree=8 itopk_size=32;
-- @session

-- sys takes an account-level snapshot
create snapshot vec_snap for account acc_vec;

-- @session:id=2&user=acc_vec:admin1&password=test123
-- turn the flag off and drop everything, to prove restore does not depend on it
set experimental_ivfpq_index = 0;
set experimental_cagra_index = 0;
drop database vdb;
-- @session

-- sys restores the whole account: replays the table + ivfpq/cagra CREATE INDEX
-- in a background context (experimental-flag gate skipped on background re-entry)
restore account acc_vec{snapshot="vec_snap"} to account acc_vec;

-- @session:id=2&user=acc_vec:admin1&password=test123
use vdb;
-- both tables + index defs are back
select count(*) from t_ivfpq;
show create table t_ivfpq;
select count(*) from t_cagra;
show create table t_cagra;
-- restore replays the data through the async CDC tail (not a synchronous
-- rebuild / model-blob copy), so wait for it to catch up before searching.
select sleep(30);
select a from t_ivfpq order by l2_distance(v, '[1,1,1,1,1,1,1,1]') asc limit 3;
select a from t_cagra order by l2_distance(v, '[1,1,1,1,1,1,1,1]') asc limit 3;
-- @session

drop snapshot if exists vec_snap;
drop account if exists acc_vec;
