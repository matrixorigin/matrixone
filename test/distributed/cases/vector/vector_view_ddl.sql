-- The view-definition plugin hook added for #27027 is consulted for EVERY index algorithm,
-- so it must be able to refuse a fulltext view without touching vector ones. A vector index
-- is an optimization, not a precondition: l2_distance and friends are real kernels, so a
-- view whose plan never reaches the index still runs as a brute-force scan and sort.
-- Rejecting those would break ordinary working views, which is why the vector plugins
-- return nil from the hook and why this case exists.
set experimental_ivf_index = 1;
set experimental_hnsw_index = 1;

drop database if exists vec_view_ddl;
create database vec_view_ddl;
use vec_view_ddl;

create table t (id bigint primary key, v vecf32(4));
insert into t values
    (1,'[1,1,1,1]'),(2,'[3,3,3,3]'),(3,'[5,5,5,5]'),
    (4,'[50,50,50,50]'),(5,'[52,52,52,52]'),(6,'[54,54,54,54]');

-- ---------------- no index at all: the view is created AND queryable -----------
create view v_nodix as select id from t order by l2_distance(v,'[1,1,1,1]') limit 3;
select id from v_nodix order by id;

create view v_nodix_score as
select id, l2_distance(v,'[1,1,1,1]') as d from t order by l2_distance(v,'[1,1,1,1]') limit 3;
select id from v_nodix_score order by id;

-- a distance in the projection with no ORDER BY at all
create view v_dist_only as select id, cosine_distance(v,'[1,1,1,1]') as d from t;
select count(*) as n from v_dist_only;

-- ---------------- an index whose metric does not match the query ---------------
-- The planner cannot use an l2 index for a cosine ORDER BY; it falls back to a scan, and
-- the view must still be accepted and correct.
create index ix using ivfflat on t(v) lists=2 op_type 'vector_l2_ops';
create view v_wrong_metric as select id from t order by cosine_distance(v,'[1,1,1,1]') limit 2;
select count(*) as n from v_wrong_metric;

-- ---------------- with a matching index the view still works -------------------
create view v_match as select id from t order by l2_distance(v,'[1,1,1,1]') limit 3;
select id from v_match order by id;

-- and it genuinely reaches the index rather than scanning
-- @separator:table
-- @regex("Table Function on ivf_search", true)
explain select id from v_match;

-- ---------------- a CONSUMER above the view falls back to a scan ---------------
-- Selecting straight from the view reaches ivf_search (asserted above), but putting a
-- consumer above it -- an outer ORDER BY, or a join -- currently makes the rewrite miss,
-- and the plan falls back to a full scan plus sort. Correct rows, silently no index.
--
-- Same root cause as the fulltext half of #27027 (a consumer above the Top-K defeats the
-- rewrite), differing only in consequence: fulltext has no fallback so the view is
-- unrunnable, while a vector index is an optimization so the query merely gets slow. That
-- is why the vector plugins do not refuse these definitions -- which is what this file
-- guards.
--
-- Only the ROWS are asserted here, deliberately. Whether these shapes reach the index is
-- the planner's business and is fixed by the sort-anchored rewrite (#25967 / #25974),
-- which asserts ivf_search for exactly these shapes in vector_view_consumers. Pinning the
-- current fallback here too would mean one branch or the other lands with a red test.
select id from v_match order by id;
select v_match.id from v_match join t m on m.id = v_match.id order by v_match.id;

drop database vec_view_ddl;
