-- Regression for issue #26961: IVFFLAT top-k must work when the SELECT-side distance
-- expression is WRAPPED by a scalar function (CAST / ROUND / arithmetic) or bound to an
-- alias, while the unwrapped distance remains the ORDER BY key. Previously this failed with
-- "cannot find column reference Missing Column: t.v" during planner column remapping.
create database if not exists ivf_wrapped;
use ivf_wrapped;
set experimental_ivf_index=1;

create table t (id int primary key, v vecf32(4));
create index idx using ivfflat on t(v) lists=2 op_type 'vector_l2_ops';
insert into t values
(1,'[0.10,0.20,0.30,0.40]'),
(2,'[0.20,0.30,0.40,0.50]'),
(3,'[0.90,0.80,0.70,0.60]'),
(4,'[0.50,0.50,0.50,0.50]'),
(5,'[0.11,0.21,0.31,0.41]');

-- direct distance in SELECT (baseline: must keep working)
select id from t order by l2_distance(v,'[0.15,0.25,0.35,0.45]') limit 3;

-- CAST-wrapped distance in SELECT, plain distance in ORDER BY (the #26961 repro)
select id, cast(l2_distance(v,'[0.15,0.25,0.35,0.45]') as decimal(10,3)) as d
from t order by l2_distance(v,'[0.15,0.25,0.35,0.45]') limit 3;

-- ROUND-wrapped distance in SELECT
select id, round(l2_distance(v,'[0.15,0.25,0.35,0.45]'),3) as d
from t order by l2_distance(v,'[0.15,0.25,0.35,0.45]') limit 3;

-- arithmetic-wrapped distance in SELECT
select id, round(l2_distance(v,'[0.15,0.25,0.35,0.45]')+1,3) as d
from t order by l2_distance(v,'[0.15,0.25,0.35,0.45]') limit 3;

-- alias in ORDER BY (distance aliased in SELECT, ORDER BY references the alias)
select id, round(l2_distance(v,'[0.15,0.25,0.35,0.45]'),3) as d from t order by d limit 3;

-- CAST-wrapped distance ALSO used as the ORDER BY key
select id from t order by cast(l2_distance(v,'[0.15,0.25,0.35,0.45]') as decimal(10,3)) limit 3;

-- Distance to a DIFFERENT vector in SELECT while ORDER BY uses the index vector: this must show the
-- TRUE distance to the OTHER vector (NOT be silently rewritten to the index score), still ordered by
-- the index vector. Guards the correctness of the distance-to-score rewrite.
select id, round(l2_distance(v,'[0.90,0.80,0.70,0.60]'),4) as d_to_b
from t order by l2_distance(v,'[0.15,0.25,0.35,0.45]') limit 3;

drop database ivf_wrapped;
