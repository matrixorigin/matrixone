-- =====================================================================
-- vector_int8_overflow_scale.sql — int8/uint8 quantized index + CDC overflow
-- must merge on the SAME distance scale (break-review CRITICAL fix).
--
-- GPU REQUIRED. A 1-byte (int8/uint8) main index computes L2 over the quantized
-- vectors, i.e. scalar^2 * true_L2 (scalar = 255/(max-min)). The base-typed CDC
-- overflow brute force computes true (base-scale) L2. Before the fix,
-- mergeMultiResults compared the two raw, so a moderately-distant overflow row
-- (small base-scale distance) out-ranked the true-nearest main row (large
-- scalar^2-scaled distance). The fix dequantizes the main distances by
-- 1/scalar^2 inside transform_distance so both tiers share the base scale.
--
-- Values in [0.2, 2.0] => scalar ~= 255/1.8 ~= 141, scalar^2 ~= 20000. Main rows
-- id 1..10 at [i*0.2]*8. Overflow row id 999 at [1.0]*8 (true dist 4.5 from the
-- query, ~225x farther than id 1). Query [0.25]*8: the TRUE nearest is id 1
-- (dist ~0.02). Correct top-1 (with the fix) is 1; the bug returned 999.
-- =====================================================================

SET experimental_cagra_index = 1;
SET experimental_ivfpq_index = 1;
SET cagra_threads_build = 7;

-- ---------------------------------------------------------------------
-- CAGRA, QUANTIZATION 'int8'
-- ---------------------------------------------------------------------
drop database if exists int8scale_cagra;
create database int8scale_cagra;
use int8scale_cagra;
create table t (id bigint primary key, v vecf32(8));
insert into t values
 (1,'[0.2,0.2,0.2,0.2,0.2,0.2,0.2,0.2]'),(2,'[0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4]'),
 (3,'[0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6]'),(4,'[0.8,0.8,0.8,0.8,0.8,0.8,0.8,0.8]'),
 (5,'[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0]'),(6,'[1.2,1.2,1.2,1.2,1.2,1.2,1.2,1.2]'),
 (7,'[1.4,1.4,1.4,1.4,1.4,1.4,1.4,1.4]'),(8,'[1.6,1.6,1.6,1.6,1.6,1.6,1.6,1.6]'),
 (9,'[1.8,1.8,1.8,1.8,1.8,1.8,1.8,1.8]'),(10,'[2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0]');
create index ix using cagra on t (v) op_type 'vector_l2_ops'
    intermediate_graph_degree=8 graph_degree=4 itopk_size=16 QUANTIZATION 'int8';
-- no overflow yet -> id 1
select id from t order by l2_distance(v, '[0.25,0.25,0.25,0.25,0.25,0.25,0.25,0.25]') asc limit 1;
-- add the overflow row, then the true nearest is STILL id 1 (not 999)
insert into t values (999, '[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0]');
select sleep(30);
select id from t order by l2_distance(v, '[0.25,0.25,0.25,0.25,0.25,0.25,0.25,0.25]') asc limit 1;
select id from t order by l2_distance(v, '[0.25,0.25,0.25,0.25,0.25,0.25,0.25,0.25]') asc limit 3;
drop database int8scale_cagra;

-- ---------------------------------------------------------------------
-- IVF-PQ, QUANTIZATION 'uint8' (same scale issue; +128 shift also cancels in L2)
-- ---------------------------------------------------------------------
drop database if exists int8scale_ivfpq;
create database int8scale_ivfpq;
use int8scale_ivfpq;
create table t (id bigint primary key, v vecf32(8));
insert into t values
 (1,'[0.2,0.2,0.2,0.2,0.2,0.2,0.2,0.2]'),(2,'[0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4]'),
 (3,'[0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6]'),(4,'[0.8,0.8,0.8,0.8,0.8,0.8,0.8,0.8]'),
 (5,'[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0]'),(6,'[1.2,1.2,1.2,1.2,1.2,1.2,1.2,1.2]'),
 (7,'[1.4,1.4,1.4,1.4,1.4,1.4,1.4,1.4]'),(8,'[1.6,1.6,1.6,1.6,1.6,1.6,1.6,1.6]'),
 (9,'[1.8,1.8,1.8,1.8,1.8,1.8,1.8,1.8]'),(10,'[2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0]');
create index ix using ivfpq on t (v) op_type 'vector_l2_ops'
    lists=2 m=2 bits_per_code=8 QUANTIZATION 'uint8';
insert into t values (999, '[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0]');
select sleep(30);
-- IVF-PQ recall on this tiny PQ config picks among the near main ids (1..4)
-- non-deterministically, so the exact top-1 is not stable. The scale bug is
-- about the moderately-distant overflow row 999 (true dist 4.5) out-ranking the
-- near main rows, so assert the stable invariant directly: 999 must NEVER appear
-- in the top results. Pre-fix this returned 1 (999 was top-1); with the
-- dequant fix it is 0.
select count(*) as top1_is_overflow from
  (select id from t order by l2_distance(v, '[0.25,0.25,0.25,0.25,0.25,0.25,0.25,0.25]') asc limit 1) x
  where x.id = 999;
select count(*) as top3_has_overflow from
  (select id from t order by l2_distance(v, '[0.25,0.25,0.25,0.25,0.25,0.25,0.25,0.25]') asc limit 3) x
  where x.id = 999;
drop database int8scale_ivfpq;

SET experimental_cagra_index = 0;
SET experimental_ivfpq_index = 0;
