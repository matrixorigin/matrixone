-- =============================================================================
-- Vector Search Adaptive Mode Test Suite
-- Tests for Phase 1-5 of the adaptive mode optimization
-- =============================================================================

create database if not exists test_retry;
use test_retry;

-- =============================================================================
-- Phase 1: mode=auto Syntax and Basic Execution
-- Verify that mode=auto is accepted and executes correctly
-- =============================================================================

drop table if exists t_phase1;
create table t_phase1(id int primary key, vec vecf32(3), category int);

insert into t_phase1 values (1, '[1,0,0]', 1);
insert into t_phase1 values (2, '[0,1,0]', 1);
insert into t_phase1 values (3, '[0,0,1]', 2);
insert into t_phase1 values (4, '[1,1,0]', 2);
insert into t_phase1 values (5, '[1,0,1]', 3);

create index idx_phase1 using ivfflat on t_phase1(vec) lists=2 op_type 'vector_l2_ops';
set experimental_ivf_index = 1;

-- Test 1.1: mode=auto syntax is accepted
-- Expectation: Returns closest vector to [0,0,0]
select id from t_phase1 order by l2_distance(vec, '[0,0,0]') limit 1 by rank with option 'mode=auto';

-- Test 1.2: mode=auto with filter
-- Expectation: Returns id 1 or 2 (category=1, closest to [0,0,0])
select id from t_phase1 where category = 1 order by l2_distance(vec, '[0,0,0]') limit 1 by rank with option 'mode=auto';

-- Test 1.3: Compare auto with explicit modes - results should be equivalent
-- mode=pre (guaranteed correct)
select id from t_phase1 where category = 1 order by l2_distance(vec, '[0,0,0]') limit 2 by rank with option 'mode=pre';
-- mode=auto should return same results (may use different path internally)
select id from t_phase1 where category = 1 order by l2_distance(vec, '[0,0,0]') limit 2 by rank with option 'mode=auto';

drop table t_phase1;

-- =============================================================================
-- Phase 2: Smart Mode Selection Based on Statistics
-- Tests for automatic selection of force mode for small datasets
-- =============================================================================

drop table if exists t_phase2_small;
create table t_phase2_small(id int primary key, vec vecf32(3), rare_col int);

-- Very small dataset (5 rows) - should trigger force mode
insert into t_phase2_small values (1, '[1,0,0]', 0);
insert into t_phase2_small values (2, '[0,1,0]', 0);
insert into t_phase2_small values (3, '[0,0,1]', 0);
insert into t_phase2_small values (4, '[1,1,1]', 1);  -- rare value
insert into t_phase2_small values (5, '[2,2,2]', 0);

create index idx_phase2 using ivfflat on t_phase2_small(vec) lists=2 op_type 'vector_l2_ops';

-- Run analyze to update statistics
-- analyze table t_phase2_small;

-- Test 2.1: Small dataset with rare filter value
-- Expectation: Auto mode should detect small dataset and return correct result
-- Even with very selective filter (rare_col=1), should find id=4
select id, rare_col from t_phase2_small where rare_col = 1 order by l2_distance(vec, '[0,0,0]') limit 1 by rank with option 'mode=auto';

-- Test 2.2: Verify result matches force mode
select id, rare_col from t_phase2_small where rare_col = 1 order by l2_distance(vec, '[0,0,0]') limit 1 by rank with option 'mode=force';

drop table t_phase2_small;

-- =============================================================================
-- Phase 3 & 4: Dynamic Over-fetch and Nprobe Adjustment
-- Tests for selectivity-based parameter optimization
-- =============================================================================

drop table if exists t_phase34;
create table t_phase34(id int primary key, vec vecf32(3), filter_col int);

-- Create a larger dataset with skewed distribution
-- 95% of rows have filter_col = 0, 5% have filter_col = 1
-- This creates high selectivity scenario

-- Cluster 1: Dense cluster near origin, mostly filter_col = 0
insert into t_phase34 values (1, '[0.1,0.1,0.1]', 0);
insert into t_phase34 values (2, '[0.2,0.2,0.2]', 0);
insert into t_phase34 values (3, '[0.3,0.3,0.3]', 0);
insert into t_phase34 values (4, '[0.4,0.4,0.4]', 0);
insert into t_phase34 values (5, '[0.5,0.5,0.5]', 0);
insert into t_phase34 values (6, '[0.6,0.6,0.6]', 0);
insert into t_phase34 values (7, '[0.7,0.7,0.7]', 0);
insert into t_phase34 values (8, '[0.8,0.8,0.8]', 0);
insert into t_phase34 values (9, '[0.9,0.9,0.9]', 0);
insert into t_phase34 values (10, '[1.0,1.0,1.0]', 1);  -- rare value in cluster

-- Cluster 2: Another cluster, mostly filter_col = 0
insert into t_phase34 values (11, '[5.1,5.1,5.1]', 0);
insert into t_phase34 values (12, '[5.2,5.2,5.2]', 0);
insert into t_phase34 values (13, '[5.3,5.3,5.3]', 0);
insert into t_phase34 values (14, '[5.4,5.4,5.4]', 0);
insert into t_phase34 values (15, '[5.5,5.5,5.5]', 1);  -- rare value in cluster

create index idx_phase34 using ivfflat on t_phase34(vec) lists=3 op_type 'vector_l2_ops';
set probe_limit = 1;

-- Test 3.1: Low selectivity filter (filter_col=0, 87% of data)
-- Post mode should work fine with normal over-fetch
select id from t_phase34 where filter_col = 0 order by l2_distance(vec, '[0,0,0]') limit 3 by rank with option 'mode=auto';

-- Test 3.2: High selectivity filter (filter_col=1, 13% of data)
-- Auto mode should increase over-fetch factor or nprobe
select id from t_phase34 where filter_col = 1 order by l2_distance(vec, '[0,0,0]') limit 1 by rank with option 'mode=auto';

-- Verify result is correct (should be id=10, closest with filter_col=1)
select id from t_phase34 where filter_col = 1 order by l2_distance(vec, '[0,0,0]') limit 1 by rank with option 'mode=pre';

drop table t_phase34;

-- =============================================================================
-- Phase 5: Adaptive Fallback (Post -> Pre Retry)
-- Tests for automatic retry when post mode returns insufficient results
-- =============================================================================

drop table if exists t_retry;
create table t_retry(id int primary key, vec vecf32(3), filter_col int);

-- Insert data
-- Cluster 1: Rows near [0,0,0], filter_col = 0
-- These will be the "nearest" cluster, but will be filtered out.
insert into t_retry values (1, '[0.1,0.1,0.1]', 0);
insert into t_retry values (2, '[0.11,0.11,0.11]', 0);
insert into t_retry values (3, '[0.12,0.12,0.12]', 0);
insert into t_retry values (4, '[0.13,0.13,0.13]', 0);
insert into t_retry values (5, '[0.14,0.14,0.14]', 0);
insert into t_retry values (6, '[0.15,0.15,0.15]', 0);
insert into t_retry values (7, '[0.16,0.16,0.16]', 0);
insert into t_retry values (8, '[0.17,0.17,0.17]', 0);
insert into t_retry values (9, '[0.18,0.18,0.18]', 0);
insert into t_retry values (10, '[0.19,0.19,0.19]', 0);
insert into t_retry values (11, '[0.2,0.2,0.2]', 0);
insert into t_retry values (12, '[0.21,0.21,0.21]', 0);
insert into t_retry values (13, '[0.22,0.22,0.22]', 0);
insert into t_retry values (14, '[0.23,0.23,0.23]', 0);
insert into t_retry values (15, '[0.24,0.24,0.24]', 0);
insert into t_retry values (16, '[0.25,0.25,0.25]', 0);
insert into t_retry values (17, '[0.26,0.26,0.26]', 0);
insert into t_retry values (18, '[0.27,0.27,0.27]', 0);
insert into t_retry values (19, '[0.28,0.28,0.28]', 0);
insert into t_retry values (20, '[0.29,0.29,0.29]', 0);

-- Cluster 2: 1 row at [10,10,10], filter_col = 1 (Target)
-- This is far away, so its cluster won't be searched with nprobe=1.
-- But it satisfies the filter.
insert into t_retry values (999, '[10,10,10]', 1);

-- Create Index with lists=5. 
-- With 21 points, K-Means should easily separate the [10,10,10] point into its own cluster or a far cluster.
create index idx using ivfflat on t_retry(vec) lists=5 op_type 'vector_l2_ops';

-- Set variables
set experimental_ivf_index = 1;
-- Force low recall
set probe_limit = 1;

-- Test 5.1: mode = post 
-- Expectation: Empty result. 
-- Explanation: 
-- 1. Query [0,0,0] finds closest cluster (Cluster 1).
-- 2. Scans Cluster 1 rows (id 1-20).
-- 3. Filters by filter_col=1. All fail.
-- 4. Returns empty.
select id, filter_col from t_retry where filter_col = 1 order by l2_distance(vec, '[0,0,0]') limit 1 by rank with option 'mode=post';

-- Test 5.2: mode = auto (Fallback Test)
-- Expectation: Returns id 999.
-- Explanation:
-- 1. Tries 'post' mode logic first (same as above).
-- 2. Result count (0) < Limit (1).
-- 3. Triggers retry with 'pre' mode (AST rewrite: mode=auto -> mode=pre).
-- 4. 'pre' mode pushes filter_col=1 down.
-- 5. Finds id 999.
select id, filter_col from t_retry where filter_col = 1 order by l2_distance(vec, '[0,0,0]') limit 1 by rank with option 'mode=auto';

-- Test 5.3: mode = auto with sufficient results (No Fallback)
-- Modify the query to allow filter_col=0.
-- Expectation: Returns id 1 (closest).
-- Explanation: 'post' mode finds results, no retry needed.
select id, filter_col from t_retry where filter_col = 0 order by l2_distance(vec, '[0,0,0]') limit 1 by rank with option 'mode=auto';

-- Test 5.4: mode = auto with limit > 1
-- Expectation: Should return at least 'limit' rows if available
select id, filter_col from t_retry where filter_col = 0 order by l2_distance(vec, '[0,0,0]') limit 5 by rank with option 'mode=auto';

-- Test 5.5: Verify auto mode equals pre mode for fallback scenario
-- Both should return id 999
select id, filter_col from t_retry where filter_col = 1 order by l2_distance(vec, '[0,0,0]') limit 1 by rank with option 'mode=pre';

drop table t_retry;

-- =============================================================================
-- Edge Cases and Boundary Tests
-- =============================================================================

drop table if exists t_edge;
create table t_edge(id int primary key, vec vecf32(3), status int);

insert into t_edge values (1, '[1,0,0]', 1);
insert into t_edge values (2, '[0,1,0]', 2);
insert into t_edge values (3, '[0,0,1]', 3);

create index idx_edge using ivfflat on t_edge(vec) lists=2 op_type 'vector_l2_ops';

-- Test E.1: No matching rows
-- Expectation: Empty result (no retry can help)
select id from t_edge where status = 999 order by l2_distance(vec, '[0,0,0]') limit 1 by rank with option 'mode=auto';

-- Test E.2: Limit exceeds available rows
-- Expectation: Returns all matching rows (2 rows with status != 2)
select id from t_edge where status != 2 order by l2_distance(vec, '[0,0,0]') limit 10 by rank with option 'mode=auto';

-- Test E.3: No filter condition
-- Expectation: Normal vector search, no filter selectivity issues
select id from t_edge order by l2_distance(vec, '[0,0,0]') limit 2 by rank with option 'mode=auto';

drop table t_edge;
-- =============================================================================
-- Phase 6: Session Variable enable_vector_auto_mode_by_default
-- =============================================================================

drop table if exists t_phase6;
create table t_phase6(id int primary key, vec vecf32(3), filter_col int);
insert into t_phase6 values (1, '[1,0,0]', 0);
insert into t_phase6 values (2, '[0,1,0]', 0);
insert into t_phase6 values (3, '[0,0,1]', 0);
insert into t_phase6 values (999, '[10,10,10]', 1);
create index idx_phase6 using ivfflat on t_phase6(vec) lists=2 op_type 'vector_l2_ops';

set experimental_ivf_index = 1;
set probe_limit = 1;
set enable_vector_auto_mode_by_default = 1;

-- Test 6.1: Default auto mode (should trigger fallback/retry internally)
-- Even though no mode is specified in SQL, it SHOULD return 999 because auto mode is default
select id from t_phase6 where filter_col = 1 order by l2_distance(vec, '[0,0,0]') limit 1;

-- Test 6.2: Override session default with explicit mode
-- Explicit 'post' should return empty despite session default being 'auto'
select id from t_phase6 where filter_col = 1 order by l2_distance(vec, '[0,0,0]') limit 1 by rank with option 'mode=post';

set enable_vector_auto_mode_by_default = 0;
-- Test 6.3: Back to default (post)
select id from t_phase6 where filter_col = 1 order by l2_distance(vec, '[0,0,0]') limit 1;

drop table t_phase6;
drop database test_retry;
