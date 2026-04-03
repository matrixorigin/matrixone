-- Test: Inline filter optimization for TABLE_SCAN
-- When a TABLE_SCAN has static FilterList but no RuntimeFilterProbeList,
-- the filter is embedded directly into the TableScan operator.
-- This eliminates the separate Filter and one Projection operator from the pipeline.
--
-- Expected phyplan changes:
--   Before: output → projection → projection → filter → tablescan
--   After:  output → projection → tablescan

drop database if exists inline_test_db;
create database inline_test_db;
use inline_test_db;

-- Create test table: col_a (output), col_b (filter-only), col_c (output)
create table t1 (
    col_a int,
    col_b int,
    col_c varchar(20)
);

insert into t1 values
    (1,  5,  'row1'),
    (2,  10, 'row2'),
    (3,  15, 'row3'),
    (4,  20, 'row4'),
    (5,  25, 'row5'),
    (6,  30, 'row6'),
    (7,  35, 'row7'),
    (8,  40, 'row8'),
    (9,  45, 'row9'),
    (10, 50, 'row10');

-- Case 1: Filter-only column (col_b) not in SELECT list
-- Verify correctness: only rows where col_b > 20 should pass
select col_a, col_c from t1 where col_b > 20 order by col_a;

-- Case 2: Phyplan should NOT have a separate "filter" operator
-- The filter is embedded in tablescan, and one projection layer is removed
-- @regex("tablescan", true)
-- @regex("filter", false)
explain phyplan select col_a, col_c from t1 where col_b > 20;

-- Case 3: All rows filtered out
select col_a, col_c from t1 where col_b > 100;

-- Case 4: No rows filtered out
select col_a, col_c from t1 where col_b > 0 order by col_a;

-- Case 5: Multiple filter conditions, some filter-only
select col_a from t1 where col_b > 15 and col_c like 'row%' order by col_a;

-- Case 6: Phyplan for multiple filter conditions
-- @regex("tablescan", true)
-- @regex("filter", false)
explain phyplan select col_a from t1 where col_b > 15 and col_c like 'row%';

-- Case 7: SELECT * (all columns in output, but still no separate filter)
-- @regex("tablescan", true)
-- @regex("filter", false)
explain phyplan select * from t1 where col_b > 20;

-- Case 8: Verify result correctness for SELECT *
select * from t1 where col_b > 20 order by col_a;

-- Case 9: Expression in WHERE clause
select col_a, col_c from t1 where col_b + 5 > 30 order by col_a;

-- Case 10: Aggregate with filter-only column
select count(*), sum(col_a) from t1 where col_b > 20;

-- Cleanup
drop table t1;
drop database inline_test_db;
