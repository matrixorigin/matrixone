-- HNSW half of the Top-K consumer coverage (see vector_ivf_topk_consumers.sql for
-- IVFFLAT). A vector Top-K must keep using the index when something CONSUMES it: an
-- outer ORDER BY (#25967) or a relational JOIN (#25974). Both used to disable the
-- rewrite and silently fall back to a full scan with an exact sort -- correct results,
-- but the index unused, invisible from the result set alone. Every EXPLAIN asserts
-- hnsw_search is present, and every query is checked against t_ref, the same rows with
-- no index, so a plan that reaches the index but returns the wrong rows still fails.
drop database if exists hnsw_topk_consumers;
create database hnsw_topk_consumers;
use hnsw_topk_consumers;

set experimental_hnsw_index = 1;

create table t_hnsw(id bigint primary key, v vecf32(4), tag int);
insert into t_hnsw values
    (1,'[1,1,1,1]',10),(2,'[3,3,3,3]',20),(3,'[5,5,5,5]',30),
    (4,'[50,50,50,50]',40),(5,'[52,52,52,52]',50),(6,'[54,54,54,54]',60);
create index i_hnsw using hnsw on t_hnsw(v) op_type 'vector_l2_ops';

-- Unindexed twin: HNSW has no mode=force, so the brute-force reference is a copy of the
-- same rows with no index on them.
create table t_ref(id bigint primary key, v vecf32(4), tag int);
insert into t_ref select id, v, tag from t_hnsw;

create table meta(id bigint primary key, name varchar(20));
insert into meta values (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f');

-- ---------------- control: no consumer ---------------------------------------
-- @separator:table
-- @regex("Table Function on hnsw_search", true)
explain select id, l2_distance(v,'[1,1,1,1]') as d from t_hnsw order by l2_distance(v,'[1,1,1,1]') limit 3;

-- ---------------- #25967: outer ORDER BY -------------------------------------
-- @separator:table
-- @regex("Table Function on hnsw_search", true)
explain with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_hnsw
    order by l2_distance(v,'[1,1,1,1]') limit 3
) select id, d from knn order by d;

with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_hnsw
    order by l2_distance(v,'[1,1,1,1]') limit 3
) select id, d from knn order by d;

-- same Top-K without the index: the rows and scores must be identical.
with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_ref
    order by l2_distance(v,'[1,1,1,1]') limit 3
) select id, d from knn order by d;

-- The outer ordering must survive the rewrite. A fix that simply skipped the outer sort
-- to find the Top-K would silently return ascending rows here.
with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_hnsw
    order by l2_distance(v,'[1,1,1,1]') limit 3
) select id, d from knn order by d desc;

-- Ordering by a different column than the Top-K metric must also be respected.
with knn as (
    select id, tag, l2_distance(v,'[1,1,1,1]') as d from t_hnsw
    order by l2_distance(v,'[1,1,1,1]') limit 3
) select id, tag from knn order by tag desc;

-- ---------------- #25974: JOIN consumer --------------------------------------
-- @separator:table
-- @regex("Table Function on hnsw_search", true)
explain with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_hnsw
    order by l2_distance(v,'[1,1,1,1]') limit 3
) select k.id, m.name from knn k left join meta m on k.id = m.id;

with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_hnsw
    order by l2_distance(v,'[1,1,1,1]') limit 3
) select k.id, k.d, m.name from knn k left join meta m on k.id = m.id order by k.d;

with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_ref
    order by l2_distance(v,'[1,1,1,1]') limit 3
) select k.id, k.d, m.name from knn k left join meta m on k.id = m.id order by k.d;

-- @separator:table
-- @regex("Table Function on hnsw_search", true)
explain with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_hnsw
    order by l2_distance(v,'[1,1,1,1]') limit 3
) select k.id, m.name from knn k inner join meta m on k.id = m.id;

with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_hnsw
    order by l2_distance(v,'[1,1,1,1]') limit 3
) select k.id, m.name from knn k inner join meta m on k.id = m.id order by k.id;

-- ---------------- aggregate consumer -----------------------------------------
-- count(*) over a Top-K prunes the derived projection to nothing while the sort still
-- carries its original ORDER BY column position. Indexing that pruned list blind panicked
-- the CN ("index out of range [N] with length 0") once this shape started reaching the
-- rewrite, so keep an aggregate consumer covered on this algorithm too.
select count(*) as n from (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_hnsw
    order by l2_distance(v,'[1,1,1,1]') limit 3
) t;

select count(*) as n from (
    select id from t_hnsw where tag >= 20
    order by l2_distance(v,'[1,1,1,1]') limit 3
) t;

-- ---------------- both consumers at once -------------------------------------
-- @separator:table
-- @regex("Table Function on hnsw_search", true)
explain with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_hnsw
    order by l2_distance(v,'[1,1,1,1]') limit 3
) select k.id, m.name from knn k left join meta m on k.id = m.id order by k.d desc;

with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_hnsw
    order by l2_distance(v,'[1,1,1,1]') limit 3
) select k.id, m.name from knn k left join meta m on k.id = m.id order by k.d desc;

-- ---------------- filtered inner Top-K, secondary index present --------------
-- The scan guard is algorithm-independent (detectVectorGuardFromSort), but HNSW reaches
-- it through a different context builder, so cover it on this side too: without a guard
-- entry the regular-index rule consumes the scan before the sort anchor runs.
create table h_guard(id bigint primary key, v vecf32(4), tag int);
insert into h_guard values
    (1,'[1,1,1,1]',1),(2,'[3,3,3,3]',1),(3,'[5,5,5,5]',1),
    (4,'[50,50,50,50]',2),(5,'[52,52,52,52]',2),(6,'[54,54,54,54]',2);
create index hg_vec using hnsw on h_guard(v) op_type 'vector_l2_ops';
create index hg_tag on h_guard(tag);
create table h_guard_ref(id bigint primary key, v vecf32(4), tag int);
insert into h_guard_ref select id, v, tag from h_guard;

-- @separator:table
-- @regex("Table Function on hnsw_search", true)
explain with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from h_guard
    where tag = 1 order by l2_distance(v,'[1,1,1,1]') limit 3
) select k.id, m.name from knn k join meta m on k.id = m.id;

with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from h_guard
    where tag = 1 order by l2_distance(v,'[1,1,1,1]') limit 3
) select k.id, k.d, m.name from knn k join meta m on k.id = m.id order by k.d;

with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from h_guard_ref
    where tag = 1 order by l2_distance(v,'[1,1,1,1]') limit 3
) select k.id, k.d, m.name from knn k join meta m on k.id = m.id order by k.d;

set experimental_hnsw_index = 0;
-- ---------------- projection without the pk (column pruning) ----------------
-- The search table function declares pkid/score (plus INCLUDE columns) but the planner
-- projects only what the query reads. A score-only SELECT prunes pkid, and the runtime
-- used to write its int64 pk into whatever vector sat at position 0 -- the float64 score
-- -- panicking the CN with "interface conversion: interface {} is int64, not float64".
-- Pre-existing on main and reachable from plain SQL, so keep a case on it.
-- @separator:table
-- @regex("Table Function on hnsw_search", true)
explain select l2_distance(v,'[1,1,1,1]') as d from t_hnsw order by l2_distance(v,'[1,1,1,1]') limit 3;

select l2_distance(v,'[1,1,1,1]') as d from t_hnsw order by l2_distance(v,'[1,1,1,1]') limit 3;

select count(*) as n from (
    select l2_distance(v,'[1,1,1,1]') as d from t_hnsw order by l2_distance(v,'[1,1,1,1]') limit 3
) x;

drop database hnsw_topk_consumers;
