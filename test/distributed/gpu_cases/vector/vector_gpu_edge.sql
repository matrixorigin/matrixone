-- =====================================================================
-- vector_gpu_edge.sql — GPU vector index edge cases (NULL vectors, duplicates)
--
-- GPU REQUIRED. Sync CAGRA index over data that includes:
--   * NULL vectors  — rows with v IS NULL are skipped by the index build
--                     (they stay in the table but are not indexed/searched).
--   * duplicate vectors — two rows share the same vector; the build still
--                     succeeds and a query on a UNIQUE vector is unaffected.
-- Probes query unique, non-null rows so the exact-match top-1 is deterministic
-- (probing a NULL/duplicated vector would be ambiguous and is avoided).
-- =====================================================================

SET experimental_cagra_index = 1;
SET cagra_threads_build = 7;
SET cagra_max_index_capacity = 99999;

drop database if exists gpu_edge;
create database gpu_edge;
use gpu_edge;

create table t (id bigint primary key, v vecf32(8));
-- ids 5,11,17 are NULL (skipped by the index); id 21 duplicates id 12's vector.
insert into t values
    ( 1, '[1,1,1,1,1,1,1,1]'),     ( 2, '[2,2,2,2,2,2,2,2]'),
    ( 3, '[3,3,3,3,3,3,3,3]'),     ( 4, '[4,4,4,4,4,4,4,4]'),
    ( 5, NULL),                    ( 6, '[6,6,6,6,6,6,6,6]'),
    ( 7, '[7,7,7,7,7,7,7,7]'),     ( 8, '[8,8,8,8,8,8,8,8]'),
    ( 9, '[9,9,9,9,9,9,9,9]'),     (10, '[10,10,10,10,10,10,10,10]'),
    (11, NULL),                    (12, '[12,12,12,12,12,12,12,12]'),
    (13, '[13,13,13,13,13,13,13,13]'), (14, '[14,14,14,14,14,14,14,14]'),
    (15, '[15,15,15,15,15,15,15,15]'), (16, '[16,16,16,16,16,16,16,16]'),
    (17, NULL),                    (18, '[18,18,18,18,18,18,18,18]'),
    (19, '[19,19,19,19,19,19,19,19]'), (20, '[20,20,20,20,20,20,20,20]'),
    (21, '[12,12,12,12,12,12,12,12]');

create index ix using cagra on t (v)
    op_type 'vector_l2_ops' intermediate_graph_degree=16 graph_degree=8 itopk_size=32;

-- All 21 rows are in the table; 3 NULLs are simply not indexed.
select count(*) from t;
select count(*) from t where v is null;

-- Unique, non-null probes -> deterministic exact-match top-1.
select id from t order by l2_distance(v, '[7,7,7,7,7,7,7,7]') asc limit 1;
select id from t order by l2_distance(v, '[16,16,16,16,16,16,16,16]') asc limit 1;
-- A probe adjacent to a NULL row (id 11 is NULL) still resolves to a valid row.
select id from t order by l2_distance(v, '[13,13,13,13,13,13,13,13]') asc limit 1;

drop database gpu_edge;
