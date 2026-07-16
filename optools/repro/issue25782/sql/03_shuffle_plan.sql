USE issue25782;
set @@join_spill_mem=1000;
select @@join_spill_mem;

-- The data is unchanged; only statistics are raised above the optimizer's
-- shuffle thresholds.  This is the positive control for the same HashBuild.
SELECT table_cnt
FROM table_stats('issue25782.issue25782_build', 'patch',
                 '{"table_cnt": 6000000, "block_number": 2000,
                   "accurate_object_number": 1, "ndv_map": {"k": 6000000},
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

SELECT 'SHUFFLE_PLAN' AS marker;
EXPLAIN SELECT count(*)
FROM issue25782_probe AS p
LEFT JOIN issue25782_build AS b ON p.k = b.k
WHERE p.k >= 1;
SELECT 'SHUFFLE_PHYPLAN' AS marker;
EXPLAIN PHYPLAN SELECT count(*)
FROM issue25782_probe AS p
LEFT JOIN issue25782_build AS b ON p.k = b.k
WHERE p.k >= 1;
