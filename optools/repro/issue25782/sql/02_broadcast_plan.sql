USE issue25782;
set @@join_spill_mem=1000;
select @@join_spill_mem;

-- Patch only planner statistics.  The physical data and predicate are the
-- same as in 03_shuffle_plan.sql.  Keep min/max/null metadata consistent on
-- both sides so the optimizer sees an overlapping key range; the physical
-- rows remain the fixed 132,096-row data set from 01_setup.sql.
SELECT table_cnt
FROM table_stats('issue25782.issue25782_build', 'patch',
                 '{"table_cnt": 1024, "block_number": 1,
                   "accurate_object_number": 1, "ndv_map": {"k": 1024},
                   "min_val_map": {"k": 868928},
                   "max_val_map": {"k": 1131072},
                   "null_cnt_map": {"k": 0}}') g;
SELECT table_cnt
FROM table_stats('issue25782.issue25782_probe', 'patch',
                 '{"table_cnt": 40000000, "block_number": 6000,
                   "accurate_object_number": 1, "ndv_map": {"k": 40000000},
                   "min_val_map": {"k": 868928},
                   "max_val_map": {"k": 1131072},
                   "null_cnt_map": {"k": 0}}') g;

SELECT 'BROADCAST_PLAN' AS marker;
EXPLAIN SELECT count(*)
FROM issue25782_probe AS p
LEFT JOIN issue25782_build AS b ON p.k = b.k
WHERE p.k >= 1;
SELECT 'BROADCAST_PHYPLAN' AS marker;
EXPLAIN PHYPLAN SELECT count(*)
FROM issue25782_probe AS p
LEFT JOIN issue25782_build AS b ON p.k = b.k
WHERE p.k >= 1;
