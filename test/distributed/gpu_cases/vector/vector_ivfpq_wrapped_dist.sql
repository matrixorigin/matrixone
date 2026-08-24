-- =====================================================================
-- vector_ivfpq_wrapped_dist.sql — GPU regression for issue #26961 on IVF-PQ.
--
-- GPU REQUIRED. The #26961 planner crash ("cannot find column reference /
-- Missing Column: t.v") reproduced on IVFFLAT, where the base vector column is
-- pruned; IVF-PQ keeps the base table scan so it never orphaned. This locks in
-- that the SHARED SELECT-side distance rewrite (replaceDistFnExprsWithScoreCol /
-- sameQueryVector) keeps IVF-PQ correct when the SELECT-side distance is WRAPPED
-- by a scalar (CAST/ROUND/arithmetic) or bound to an alias while the unwrapped
-- distance is the ORDER BY key.
--
-- IVF-PQ is approximate: only the top-1 exact-match neighbor is deterministic.
-- Every query probes a vector that exactly matches an indexed row and uses
-- LIMIT 1, so the wrapped distance of the returned row is 0 (deterministic).
-- lists=10 / m=8 (pq_len=1, 256 levels) + kmeans_train_percent=100 +
-- probe_limit=16 make the exact match stable (see vector_ivfpq.sql).
-- =====================================================================

SET experimental_ivfpq_index = 1;
SET ivfpq_threads_build = 6;
SET kmeans_train_percent = 100;
SET probe_limit = 16;

drop database if exists ivfpq_wrapped;
create database ivfpq_wrapped;
use ivfpq_wrapped;

create table t (id bigint primary key, v vecf32(8));
insert into t values
(1,'[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'),
(2,'[0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9]'),
(3,'[0.9,0.8,0.7,0.6,0.5,0.4,0.3,0.2]'),
(4,'[0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5]'),
(5,'[0.11,0.21,0.31,0.41,0.51,0.61,0.71,0.81]');
create index idx using ivfpq on t(v) lists=10 op_type 'vector_l2_ops' m=8;

-- direct distance in SELECT (baseline: must keep working)
select id from t order by l2_distance(v,'[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]') limit 1;

-- CAST-wrapped distance in SELECT, plain distance in ORDER BY (the #26961 shape)
select id, cast(l2_distance(v,'[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]') as decimal(10,3)) as d
from t order by l2_distance(v,'[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]') limit 1;

-- ROUND-wrapped distance in SELECT
select id, round(l2_distance(v,'[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'),3) as d
from t order by l2_distance(v,'[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]') limit 1;

-- arithmetic-wrapped distance in SELECT
select id, round(l2_distance(v,'[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]')+1,3) as d
from t order by l2_distance(v,'[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]') limit 1;

-- alias in ORDER BY (distance aliased in SELECT, ORDER BY references the alias)
select id, round(l2_distance(v,'[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'),3) as d from t order by d limit 1;

-- CAST-wrapped distance ALSO used as the ORDER BY key
select id from t order by cast(l2_distance(v,'[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]') as decimal(10,3)) limit 1;

-- Distance to a DIFFERENT vector in SELECT while ORDER BY uses the index vector:
-- must show the TRUE distance to the OTHER vector (NOT be silently rewritten to
-- the index score), still ordered by the index vector. Guards sameQueryVector.
select id, round(l2_distance(v,'[0.9,0.8,0.7,0.6,0.5,0.4,0.3,0.2]'),4) as d_to_b
from t order by l2_distance(v,'[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]') limit 1;

drop database ivfpq_wrapped;
