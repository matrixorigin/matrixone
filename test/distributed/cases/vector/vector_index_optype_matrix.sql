-- op_type -> distance-function resolution matrix for the CPU-reachable vector
-- index algorithms (#25966). Every op_type an index can be BUILT with must be
-- one a query can actually USE, for the distance functions it serves:
--
--   vector_l2_ops     -> l2_distance, l2_distance_sq
--   vector_l2sq_ops   -> l2_distance, l2_distance_sq   (same index; the sqrt is
--                        chosen from the QUERY's function, not the op_type)
--   vector_l1_ops     -> l1_distance          (ivfflat only; usearch has no L1)
--   vector_ip_ops     -> inner_product
--   vector_cosine_ops -> cosine_distance
--
-- Each block asserts three things: the served function reaches the index
-- (ivf_search / hnsw_search in the plan), a non-served function does not, and
-- the SCORE the index returns equals the brute-force distance — t_ref is the
-- same rows with no index, so the two projections must print identical values.
-- The score is what the rewrite substitutes for the distance expression, so a
-- wrong metric or a missing squared->L2 transform shows up as a value diff.
drop database if exists vec_optype_matrix;
create database vec_optype_matrix;
use vec_optype_matrix;

set experimental_ivf_index = 1;

create table t(a bigint primary key, v vecf32(4));
insert into t values
    (1,'[1,1,1,1]'),(2,'[3,3,3,3]'),(3,'[5,5,5,5]'),
    (4,'[50,50,50,50]'),(5,'[52,52,52,52]'),(6,'[54,54,54,54]');

-- Unindexed twin: the brute-force reference for every score comparison below.
create table t_ref(a bigint primary key, v vecf32(4));
insert into t_ref select a, v from t;

-- Cosine needs distances that are both exact in binary AND distinct, so index and
-- brute force can be compared without rounding and without an ambiguous tie order:
-- parallel -> 0, orthogonal -> 1, antiparallel -> 2.
create table tc(a bigint primary key, v vecf32(4));
insert into tc values (1,'[1,0,0,0]'),(2,'[0,1,0,0]'),(3,'[-1,0,0,0]');
create table tc_ref(a bigint primary key, v vecf32(4));
insert into tc_ref select a, v from tc;

-- ---------------- IVF-FLAT: vector_l2_ops -----------------------------------
create index ivf_l2 using ivfflat on t(v) lists=2 op_type 'vector_l2_ops';

-- @separator:table
-- @regex("Table Function on ivf_search", true)
explain select a from t order by l2_distance(v,'[1,1,1,1]') limit 3;
-- @separator:table
-- @regex("Table Function on ivf_search", true)
explain select a from t order by l2_distance_sq(v,'[1,1,1,1]') limit 3;
-- @separator:table
-- @regex("Table Function on ivf_search", false)
explain select a from t order by l1_distance(v,'[1,1,1,1]') limit 3;
-- @separator:table
-- @regex("Table Function on ivf_search", false)
explain select a from t order by inner_product(v,'[1,1,1,1]') limit 3;
-- @separator:table
-- @regex("Table Function on ivf_search", false)
explain select a from t order by cosine_distance(v,'[1,1,1,1]') limit 3;

-- score validity: index score == brute force, for BOTH L2 forms.
select a, l2_distance(v,'[1,1,1,1]') as d from t order by l2_distance(v,'[1,1,1,1]') limit 3;
select a, l2_distance(v,'[1,1,1,1]') as d from t_ref order by l2_distance(v,'[1,1,1,1]') limit 3;
select a, l2_distance_sq(v,'[1,1,1,1]') as dsq from t order by l2_distance_sq(v,'[1,1,1,1]') limit 3;
select a, l2_distance_sq(v,'[1,1,1,1]') as dsq from t_ref order by l2_distance_sq(v,'[1,1,1,1]') limit 3;

-- The squared->L2 transform is pinned by the two comparisons above: the index stores
-- squared L2, so an l2_distance query that comes back equal to brute force can only
-- have been sqrt-ed. (The direct score^2 - l2_distance_sq probe is done on the HNSW
-- block below; wrapping the distance in another expression does not survive the
-- IVF-FLAT rewrite's column remapping today.)

alter table t drop index ivf_l2;

-- ---------------- IVF-FLAT: vector_l2sq_ops ---------------------------------
create index ivf_l2sq using ivfflat on t(v) lists=2 op_type 'vector_l2sq_ops';

-- @separator:table
-- @regex("Table Function on ivf_search", true)
explain select a from t order by l2_distance_sq(v,'[1,1,1,1]') limit 3;
-- @separator:table
-- @regex("Table Function on ivf_search", true)
explain select a from t order by l2_distance(v,'[1,1,1,1]') limit 3;
-- @separator:table
-- @regex("Table Function on ivf_search", false)
explain select a from t order by cosine_distance(v,'[1,1,1,1]') limit 3;

select a, l2_distance_sq(v,'[1,1,1,1]') as dsq from t order by l2_distance_sq(v,'[1,1,1,1]') limit 3;
select a, l2_distance_sq(v,'[1,1,1,1]') as dsq from t_ref order by l2_distance_sq(v,'[1,1,1,1]') limit 3;
select a, l2_distance(v,'[1,1,1,1]') as d from t order by l2_distance(v,'[1,1,1,1]') limit 3;
select a, l2_distance(v,'[1,1,1,1]') as d from t_ref order by l2_distance(v,'[1,1,1,1]') limit 3;

-- Same transform, on the l2sq-named index: it stores the same squared metric, so
-- l2_distance off it must still equal the brute-force square root (above).

alter table t drop index ivf_l2sq;

-- ---------------- IVF-FLAT: vector_l1_ops -----------------------------------
create index ivf_l1 using ivfflat on t(v) lists=2 op_type 'vector_l1_ops';

-- @separator:table
-- @regex("Table Function on ivf_search", true)
explain select a from t order by l1_distance(v,'[1,1,1,1]') limit 3;
-- @separator:table
-- @regex("Table Function on ivf_search", false)
explain select a from t order by l2_distance(v,'[1,1,1,1]') limit 3;

-- L1 is NOT squared, so no transform may be applied: sum|a-b| = 8 for [3,3,3,3].
select a, l1_distance(v,'[1,1,1,1]') as d from t order by l1_distance(v,'[1,1,1,1]') limit 3;
select a, l1_distance(v,'[1,1,1,1]') as d from t_ref order by l1_distance(v,'[1,1,1,1]') limit 3;
select a, l1_distance(v,'[54,54,54,54]') as d from t order by l1_distance(v,'[54,54,54,54]') limit 3;
select a, l1_distance(v,'[54,54,54,54]') as d from t_ref order by l1_distance(v,'[54,54,54,54]') limit 3;

alter table t drop index ivf_l1;

-- ---------------- IVF-FLAT: vector_ip_ops -----------------------------------
create index ivf_ip using ivfflat on t(v) lists=2 op_type 'vector_ip_ops';

-- @separator:table
-- @regex("Table Function on ivf_search", true)
explain select a from t order by inner_product(v,'[1,1,1,1]') limit 3;
-- @separator:table
-- @regex("Table Function on ivf_search", false)
explain select a from t order by l2_distance(v,'[1,1,1,1]') limit 3;

select a, inner_product(v,'[1,1,1,1]') as d from t order by inner_product(v,'[1,1,1,1]') limit 3;
select a, inner_product(v,'[1,1,1,1]') as d from t_ref order by inner_product(v,'[1,1,1,1]') limit 3;

alter table t drop index ivf_ip;

-- ---------------- IVF-FLAT: vector_cosine_ops -------------------------------
create index ivf_cos using ivfflat on tc(v) lists=2 op_type 'vector_cosine_ops';

-- @separator:table
-- @regex("Table Function on ivf_search", true)
explain select a from tc order by cosine_distance(v,'[1,0,0,0]') limit 3;
-- @separator:table
-- @regex("Table Function on ivf_search", false)
explain select a from tc order by l2_distance(v,'[1,0,0,0]') limit 3;

select a, cosine_distance(v,'[1,0,0,0]') as d from tc order by cosine_distance(v,'[1,0,0,0]') limit 3;
select a, cosine_distance(v,'[1,0,0,0]') as d from tc_ref order by cosine_distance(v,'[1,0,0,0]') limit 3;

alter table tc drop index ivf_cos;

set experimental_ivf_index = 0;

-- ---------------- HNSW ------------------------------------------------------
-- usearch backs HNSW and has no L1 metric, so vector_l1_ops must be rejected at
-- CREATE INDEX rather than building an index no query can use.
set experimental_hnsw_index = 1;
create table h(a bigint primary key, v vecf32(4));
insert into h values
    (1,'[1,1,1,1]'),(2,'[3,3,3,3]'),(3,'[5,5,5,5]'),
    (4,'[50,50,50,50]'),(5,'[52,52,52,52]'),(6,'[54,54,54,54]');

create table hc(a bigint primary key, v vecf32(4));
insert into hc select a, v from tc;

create index h_l1 using hnsw on h(v) op_type 'vector_l1_ops';

create index h_l2 using hnsw on h(v) op_type 'vector_l2_ops';
-- @separator:table
-- @regex("hnsw_search", true)
explain select a from h order by l2_distance(v,'[1,1,1,1]') limit 3;
-- @separator:table
-- @regex("hnsw_search", true)
explain select a from h order by l2_distance_sq(v,'[1,1,1,1]') limit 3;
-- @separator:table
-- @regex("hnsw_search", false)
explain select a from h order by cosine_distance(v,'[1,1,1,1]') limit 3;

select a, l2_distance(v,'[1,1,1,1]') as d from h order by l2_distance(v,'[1,1,1,1]') limit 3;
select a, l2_distance(v,'[1,1,1,1]') as d from t_ref order by l2_distance(v,'[1,1,1,1]') limit 3;
select a, round(l2_distance(v,'[1,1,1,1]') * l2_distance(v,'[1,1,1,1]')
             - l2_distance_sq(v,'[1,1,1,1]'), 6) as sqrt_check
  from h order by l2_distance(v,'[1,1,1,1]') limit 3;

alter table h drop index h_l2;

create index h_l2sq using hnsw on h(v) op_type 'vector_l2sq_ops';
-- @separator:table
-- @regex("hnsw_search", true)
explain select a from h order by l2_distance_sq(v,'[1,1,1,1]') limit 3;
-- @separator:table
-- @regex("hnsw_search", true)
explain select a from h order by l2_distance(v,'[1,1,1,1]') limit 3;

select a, l2_distance_sq(v,'[1,1,1,1]') as dsq from h order by l2_distance_sq(v,'[1,1,1,1]') limit 3;
select a, l2_distance_sq(v,'[1,1,1,1]') as dsq from t_ref order by l2_distance_sq(v,'[1,1,1,1]') limit 3;

alter table h drop index h_l2sq;

create index h_cos using hnsw on hc(v) op_type 'vector_cosine_ops';
-- @separator:table
-- @regex("hnsw_search", true)
explain select a from hc order by cosine_distance(v,'[1,0,0,0]') limit 3;

select a, cosine_distance(v,'[1,0,0,0]') as d from hc order by cosine_distance(v,'[1,0,0,0]') limit 3;
select a, cosine_distance(v,'[1,0,0,0]') as d from tc_ref order by cosine_distance(v,'[1,0,0,0]') limit 3;

alter table hc drop index h_cos;

-- vector_ip_ops: usearch's IP metric is 1 - a·b while inner_product is -a·b, so the
-- index score has to be rescaled to the SQL convention. Before that transform every
-- HNSW ip score came back exactly 1 higher than brute force — the ordering was right,
-- so only these value comparisons catch it.
create index h_ip using hnsw on h(v) op_type 'vector_ip_ops';
-- @separator:table
-- @regex("hnsw_search", true)
explain select a from h order by inner_product(v,'[1,1,1,1]') limit 3;

select a, inner_product(v,'[1,1,1,1]') as d from h order by inner_product(v,'[1,1,1,1]') limit 3;
select a, inner_product(v,'[1,1,1,1]') as d from t_ref order by inner_product(v,'[1,1,1,1]') limit 3;

alter table h drop index h_ip;

set experimental_hnsw_index = 0;

drop database vec_optype_matrix;
