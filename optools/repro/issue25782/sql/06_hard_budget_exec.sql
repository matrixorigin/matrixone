USE issue25782;
-- Keep the policy threshold far above this bounded dataset. Any spill observed
-- here must therefore come from hard budget admission, not join_spill_mem.
set @@join_spill_mem=1073741824;
select @@join_spill_mem;

SELECT count(*)
FROM issue25782_probe AS p
LEFT JOIN issue25782_build AS b ON p.k = b.k
WHERE p.k >= 1;
EXPLAIN PHYPLAN ANALYZE SELECT count(*)
FROM issue25782_probe AS p
LEFT JOIN issue25782_build AS b ON p.k = b.k
WHERE p.k >= 1;
