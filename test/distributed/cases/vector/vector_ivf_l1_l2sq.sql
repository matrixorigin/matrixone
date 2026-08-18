-- #25966: IVFFLAT accepted two op_types that could not be used.
--   * vector_l1_ops: CREATE INDEX succeeded but l1_distance did not exist in SQL,
--     so neither the user query nor the index's own generated search SQL could
--     name the metric.
--   * vector_l2sq_ops: CREATE INDEX succeeded but the planner matched a single
--     canonical op_type per distance function, so the index was never chosen.
-- Both op_types must now build an index the optimizer actually uses.
drop database if exists ivf_l1_l2sq;
create database ivf_l1_l2sq;
use ivf_l1_l2sq;

-- l1_distance is a first-class SQL function over every vector element type.
-- |1-10|+|2-20|+|3-30| = 54 in each width.
select l1_distance(cast('[1,2,3]' as vecf32(3)), cast('[10,20,30]' as vecf32(3))) as l1_f32;
select l1_distance(cast('[1,2,3]' as vecf64(3)), cast('[10,20,30]' as vecf64(3))) as l1_f64;
select l1_distance(cast('[1,2,3]' as vecbf16(3)), cast('[10,20,30]' as vecbf16(3))) as l1_bf16;
select l1_distance(cast('[1,2,3]' as vecf16(3)), cast('[10,20,30]' as vecf16(3))) as l1_f16;
select l1_distance(cast('[1,2,3]' as vecint8(3)), cast('[10,20,30]' as vecint8(3))) as l1_int8;
select l1_distance(cast('[1,2,3]' as vecuint8(3)), cast('[10,20,30]' as vecuint8(3))) as l1_uint8;

-- Dimension mismatch is an error, like the other distance functions.
select l1_distance(cast('[1,2,3]' as vecf32(3)), cast('[1,2]' as vecf32(2)));

-- Two well-separated clusters so the ranking is unambiguous whichever
-- centroid a probe lands in.
create table t(a int primary key, v vecf32(4));
insert into t values
    (1,'[1,1,1,1]'),(2,'[3,3,3,3]'),(3,'[5,5,5,5]'),
    (4,'[50,50,50,50]'),(5,'[52,52,52,52]'),(6,'[54,54,54,54]');

-- 1) vector_l1_ops: buildable AND queryable through l1_distance.
create index ti_l1 using ivfflat on t(v) lists=2 op_type 'vector_l1_ops';

-- @separator:table
-- @regex("Table Function on ivf_search", true)
explain select a from t order by l1_distance(v,'[1,1,1,1]') limit 3;

select a from t order by l1_distance(v,'[1,1,1,1]') limit 3;
select a from t order by l1_distance(v,'[54,54,54,54]') limit 3;

-- The distance itself is the Manhattan distance, not L2: |3-1|*4 = 8.
select a, l1_distance(v,'[1,1,1,1]') as d from t order by d limit 2;

-- A different metric must NOT be answered by the L1 index (no ivf_search).
-- @separator:table
-- @regex("Table Function on ivf_search", false)
explain select a from t order by l2_distance(v,'[1,1,1,1]') limit 3;

alter table t drop index ti_l1;

-- 2) vector_l2sq_ops: the same index vector_l2_ops builds, so BOTH
--    l2_distance and l2_distance_sq must be served by it.
create index ti_l2sq using ivfflat on t(v) lists=2 op_type 'vector_l2sq_ops';

-- @separator:table
-- @regex("Table Function on ivf_search", true)
explain select a from t order by l2_distance_sq(v,'[1,1,1,1]') limit 3;

select a from t order by l2_distance_sq(v,'[1,1,1,1]') limit 3;

-- @separator:table
-- @regex("Table Function on ivf_search", true)
explain select a from t order by l2_distance(v,'[54,54,54,54]') limit 3;

select a from t order by l2_distance(v,'[54,54,54,54]') limit 3;

-- The score is reported in the units the QUERY asked for, not the index's:
-- l2_distance_sq(v,[1,1,1,1]) for [3,3,3,3] is 16, l2_distance is 4.
select a, l2_distance_sq(v,'[1,1,1,1]') as dsq from t order by dsq limit 2;
select a, l2_distance(v,'[1,1,1,1]') as d from t order by d limit 2;

-- op_type survives the DDL round trip.
-- @separator:table
show create table t;

-- An index built with the canonical vector_l2_ops still answers both, unchanged.
alter table t drop index ti_l2sq;
create index ti_l2 using ivfflat on t(v) lists=2 op_type 'vector_l2_ops';

-- @separator:table
-- @regex("Table Function on ivf_search", true)
explain select a from t order by l2_distance_sq(v,'[1,1,1,1]') limit 3;

select a from t order by l2_distance_sq(v,'[1,1,1,1]') limit 3;
select a from t order by l2_distance(v,'[54,54,54,54]') limit 3;

-- 3) An unknown op_type is still rejected at CREATE INDEX.
alter table t drop index ti_l2;
create index ti_bad using ivfflat on t(v) lists=2 op_type 'vector_bogus_ops';

drop database ivf_l1_l2sq;
