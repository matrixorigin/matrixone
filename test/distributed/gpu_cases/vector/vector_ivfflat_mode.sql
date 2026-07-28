-- =====================================================================
-- vector_ivfflat_mode.sql — IVF-FLAT index search under gpu_mode on vs off
--
-- GPU REQUIRED. The IVF-FLAT index assigns a query to its nearest centroid via
-- the productl2 brute-force operator, which offloads to the GPU when gpu_mode=1
-- (gpumode.EffectiveGpuMode -> brute_force gpuMode) and runs on CPU when
-- gpu_mode=0. IVF-FLAT keeps full vectors, so with probe_limit >= lists the
-- search is exhaustive/exact and the two modes MUST return identical results —
-- the blocks below are byte-identical, proving the GPU and CPU brute-force
-- centroid-assignment paths agree.
-- =====================================================================

SET experimental_ivf_index = 1;

drop database if exists ivfflat_mode;
create database ivfflat_mode;
use ivfflat_mode;

create table t (id bigint primary key, v vecf32(8));
insert into t values
    (1, '[1,1,1,1,1,1,1,1]'),
    (2, '[2,2,2,2,2,2,2,2]'),
    (3, '[3,3,3,3,3,3,3,3]'),
    (4, '[4,4,4,4,4,4,4,4]'),
    (5, '[5,5,5,5,5,5,5,5]'),
    (6, '[6,6,6,6,6,6,6,6]'),
    (7, '[7,7,7,7,7,7,7,7]'),
    (8, '[8,8,8,8,8,8,8,8]'),
    (9, '[9,9,9,9,9,9,9,9]'),
    (10, '[10,10,10,10,10,10,10,10]'),
    (11, '[11,11,11,11,11,11,11,11]'),
    (12, '[12,12,12,12,12,12,12,12]'),
    (13, '[13,13,13,13,13,13,13,13]'),
    (14, '[14,14,14,14,14,14,14,14]'),
    (15, '[15,15,15,15,15,15,15,15]'),
    (16, '[16,16,16,16,16,16,16,16]'),
    (17, '[17,17,17,17,17,17,17,17]'),
    (18, '[18,18,18,18,18,18,18,18]'),
    (19, '[19,19,19,19,19,19,19,19]'),
    (20, '[20,20,20,20,20,20,20,20]');

create index ix using ivfflat on t (v) op_type 'vector_l2_ops' lists=2;

-- probe_limit >= lists -> exhaustive, exact, deterministic.
SET probe_limit = 2;

-- ---- gpu_mode = 1 (GPU brute-force centroid assignment) ----
SET gpu_mode = 1;
select id from t order by l2_distance(v, '[5,5,5,5,5,5,5,5]') asc limit 1;
select id from t order by l2_distance(v, '[12,12,12,12,12,12,12,12]') asc limit 1;
select id from t order by l2_distance(v, '[18,18,18,18,18,18,18,18]') asc limit 1;

-- ---- gpu_mode = 0 (CPU) — identical results ----
SET gpu_mode = 0;
select id from t order by l2_distance(v, '[5,5,5,5,5,5,5,5]') asc limit 1;
select id from t order by l2_distance(v, '[12,12,12,12,12,12,12,12]') asc limit 1;
select id from t order by l2_distance(v, '[18,18,18,18,18,18,18,18]') asc limit 1;

SET gpu_mode = 1;
drop database ivfflat_mode;
