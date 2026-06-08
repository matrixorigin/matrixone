-- GPU-only vector indexes (IVF-PQ, CAGRA) on a CPU-only build.
--
-- Even with the experimental feature flag enabled, CREATE INDEX for these
-- GPU-backed algorithms is exercised on a CPU-only mo-service (built without
-- the `gpu` tag, no GPU device). This pins the end-to-end behavior of the new
-- index-plugin dispatch + experimental-flag gate on the CPU-reachable path:
-- the flag is settable, the plugin dispatch is reached, and CREATE INDEX
-- surfaces its CPU outcome (expected: a clean error, not a crash).

drop database if exists vector_gpu_cpu_db;
create database vector_gpu_cpu_db;
use vector_gpu_cpu_db;

-- ============================== IVF-PQ ==============================
select @@experimental_ivfpq_index;   -- default: off (0)
set experimental_ivfpq_index = 1;
select @@experimental_ivfpq_index;   -- enabled (1)

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

-- Expected to fail on a CPU-only build (the IVF-PQ index is GPU-backed).
create index ix using ivfpq on t_ivfpq (v)
    op_type 'vector_l2_ops' lists=10 m=8 bits_per_code=8;

-- reset the experimental flag after use
set experimental_ivfpq_index = 0;

-- ============================== CAGRA ==============================
select @@experimental_cagra_index;   -- default: off (0)
set experimental_cagra_index = 1;
select @@experimental_cagra_index;   -- enabled (1)

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

-- Expected to fail on a CPU-only build (the CAGRA index is GPU-backed).
create index ix using cagra on t_cagra (v)
    op_type 'vector_l2_ops' intermediate_graph_degree=16 graph_degree=8 itopk_size=32;

-- reset the experimental flag after use
set experimental_cagra_index = 0;

drop database vector_gpu_cpu_db;
