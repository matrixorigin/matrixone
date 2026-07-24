-- =====================================================================
-- vector_ivfpq_postfilter.sql — IVF-PQ search filtering on a NON-INCLUDE column
--
-- GPU REQUIRED. A WHERE predicate on a column that is NOT in the index INCLUDE
-- list cannot be pushed into the GPU bitset pre-filter. Instead the planner runs
-- the ANN search to get a candidate window, then JOINs + filters the predicate
-- at the database (post-filter): ivfpq_search (candidate window) INNER JOIN
-- (table scan Filter: <non-include pred>) -> Sort -> Limit.
--
-- Methodology: first take the UNFILTERED ranked result, then verify the
-- post-filtered result equals exactly the unfiltered rows that satisfy the
-- predicate. The candidate window grows with the query LIMIT, so a LIMIT >= row
-- count makes the window cover every row -> the post-filter is exact.
--
-- Data: id=i -> [i]*8; c_inc=i (INCLUDE, int), c_noinc=100+i (NOT included).
-- Query [12]*8. With LIMIT 20 the window is all 20 rows, so post-filter is exact:
--   * c_noinc < 105            -> i in 1..4  -> 4,3,2,1   (nearest-first)
--   * c_noinc >= 116           -> i in 16..20 -> 16,17,18,19,20
--   * c_noinc = 102            -> id 2        (far row still found, full window)
--   * c_inc <= 14 (PRE) AND c_noinc >= 108 (POST) -> i in 8..14
-- =====================================================================

SET experimental_ivfpq_index = 1;
SET ivfpq_threads_build = 6;
SET ivfpq_max_index_capacity = 99999;
SET kmeans_train_percent = 100;
SET probe_limit = 16;

drop database if exists ivfpq_postfilter;
create database ivfpq_postfilter;
use ivfpq_postfilter;

create table t (id bigint primary key, v vecf32(8), c_inc int, c_noinc int);
insert into t values
    (1, '[1,1,1,1,1,1,1,1]', 1, 101),
    (2, '[2,2,2,2,2,2,2,2]', 2, 102),
    (3, '[3,3,3,3,3,3,3,3]', 3, 103),
    (4, '[4,4,4,4,4,4,4,4]', 4, 104),
    (5, '[5,5,5,5,5,5,5,5]', 5, 105),
    (6, '[6,6,6,6,6,6,6,6]', 6, 106),
    (7, '[7,7,7,7,7,7,7,7]', 7, 107),
    (8, '[8,8,8,8,8,8,8,8]', 8, 108),
    (9, '[9,9,9,9,9,9,9,9]', 9, 109),
    (10, '[10,10,10,10,10,10,10,10]', 10, 110),
    (11, '[11,11,11,11,11,11,11,11]', 11, 111),
    (12, '[12,12,12,12,12,12,12,12]', 12, 112),
    (13, '[13,13,13,13,13,13,13,13]', 13, 113),
    (14, '[14,14,14,14,14,14,14,14]', 14, 114),
    (15, '[15,15,15,15,15,15,15,15]', 15, 115),
    (16, '[16,16,16,16,16,16,16,16]', 16, 116),
    (17, '[17,17,17,17,17,17,17,17]', 17, 117),
    (18, '[18,18,18,18,18,18,18,18]', 18, 118),
    (19, '[19,19,19,19,19,19,19,19]', 19, 119),
    (20, '[20,20,20,20,20,20,20,20]', 20, 120);

-- Only c_inc is pushed into the GPU pre-filter; c_noinc is post-filtered.
create index ix using ivfpq on t (v) op_type 'vector_l2_ops' lists=2 m=8 bits_per_code=8 INCLUDE (c_inc);

show create table t;

-- (1) UNFILTERED ranked baseline (window covers all rows at LIMIT 20).
select id, c_noinc from t order by l2_distance(v, '[12,12,12,12,12,12,12,12]') asc limit 20;

-- (2) POST-FILTER on the non-INCLUDE column — must equal the baseline rows that
--     satisfy the predicate, in the same distance order.
select id from t where c_noinc < 105 order by l2_distance(v, '[12,12,12,12,12,12,12,12]') asc limit 20;
select id from t where c_noinc >= 116 order by l2_distance(v, '[12,12,12,12,12,12,12,12]') asc limit 20;
select id from t where c_noinc = 102 order by l2_distance(v, '[12,12,12,12,12,12,12,12]') asc limit 20;

-- (3) MIXED: c_inc is pushed (pre-filter), c_noinc is post-filtered. Both apply.
select id from t where c_inc <= 14 and c_noinc >= 108 order by l2_distance(v, '[12,12,12,12,12,12,12,12]') asc limit 20;

-- (4) Small LIMIT shrinks the candidate window: a far post-filter match (id 2,
--     c_noinc=102) falls outside the window and is not returned (approximate).
select id from t where c_noinc = 102 order by l2_distance(v, '[12,12,12,12,12,12,12,12]') asc limit 1;

drop database ivfpq_postfilter;
