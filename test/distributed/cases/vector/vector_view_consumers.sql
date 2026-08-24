-- A VIEW is a consumer boundary, like the CTE shapes in vector_ivf_topk_consumers.
-- Before the sort-anchored rewrite (#25967 / #25974) a vector Top-K reached the index when
-- selected from directly, but putting anything above it -- an outer ORDER BY, or a join --
-- made the rewrite miss and the plan fell back to a full scan plus sort. Correct rows,
-- silently no index, which is invisible from the result set and only shows in EXPLAIN.
--
-- A view makes that boundary permanent: the fallback is baked into every query the view
-- ever serves. So each EXPLAIN below asserts VECTOR_INDEX_SCAN is present, and each query is
-- cross-checked against the same Top-K computed WITHOUT the index (mode=force) so a plan
-- that reaches the index but returns the wrong rows still fails.
--
-- The fulltext half of the same root cause is #27027: there the placeholder has no
-- implementation at all, so the view is not slow but unrunnable, and view DDL refuses it.
set experimental_ivf_index = 1;

drop database if exists vec_view_consumers;
create database vec_view_consumers;
use vec_view_consumers;

create table t_ivf(id bigint primary key, v vecf32(4), tag int);
insert into t_ivf values
    (1,'[1,1,1,1]',10),(2,'[3,3,3,3]',20),(3,'[5,5,5,5]',30),
    (4,'[50,50,50,50]',40),(5,'[52,52,52,52]',50),(6,'[54,54,54,54]',60);
create index i_ivf using ivfflat on t_ivf(v) lists=2 op_type 'vector_l2_ops';

create table meta(id bigint primary key, name varchar(20));
insert into meta values (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f');

create view knn as
    select id, l2_distance(v,'[1,1,1,1]') as d from t_ivf
    order by l2_distance(v,'[1,1,1,1]') limit 3;

-- ---------------- control: selecting from the view directly --------------------
-- @separator:table
-- @regex("Vector Index Scan", true)
explain select id, d from knn;

select id, d from knn order by id;

-- ---------------- #25967 shape: an outer ORDER BY above the view ---------------
-- @separator:table
-- @regex("Vector Index Scan", true)
explain select id, d from knn order by d;

select id, d from knn order by d;

-- the same Top-K without the index: rows and scores must be identical
with force_knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_ivf
    order by l2_distance(v,'[1,1,1,1]') limit 3 by rank with option 'mode=force'
) select id, d from force_knn order by d;

-- ordering the other way must still be honoured
select id, d from knn order by d desc;

-- ---------------- #25974 shape: a join against the view ------------------------
-- @separator:table
-- @regex("Vector Index Scan", true)
explain select knn.id, knn.d, m.name from knn join meta m on m.id = knn.id;

select knn.id, knn.d, m.name from knn join meta m on m.id = knn.id order by knn.d;

with force_knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_ivf
    order by l2_distance(v,'[1,1,1,1]') limit 3 by rank with option 'mode=force'
) select force_knn.id, force_knn.d, m.name
  from force_knn join meta m on m.id = force_knn.id order by force_knn.d;

-- ---------------- an aggregate above the view ----------------------------------
-- @separator:table
-- @regex("Vector Index Scan", true)
explain select count(*) as n from knn;

select count(*) as n from knn;

-- ---------------- a filter above the view --------------------------------------
-- @separator:table
-- @regex("Vector Index Scan", true)
explain select id from knn where d > 0;

select id from knn where d > 0 order by id;

-- ---------------- a view that projects no distance -----------------------------
create view knn_ids as
    select id from t_ivf order by l2_distance(v,'[1,1,1,1]') limit 3;

-- @separator:table
-- @regex("Vector Index Scan", true)
explain select id from knn_ids order by id;

select id from knn_ids order by id;

drop database vec_view_consumers;
