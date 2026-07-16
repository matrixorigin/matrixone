USE issue25782;
set @@join_spill_mem=1000;
select @@join_spill_mem;

-- Must remain byte-for-byte equivalent to 04_broadcast_exec.sql.
SELECT count(*)
FROM issue25782_probe AS p
LEFT JOIN issue25782_build AS b ON p.k = b.k
WHERE p.k >= 1;
EXPLAIN PHYPLAN ANALYZE SELECT count(*)
FROM issue25782_probe AS p
LEFT JOIN issue25782_build AS b ON p.k = b.k
WHERE p.k >= 1;
