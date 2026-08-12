-- =====================================================================
-- CAGRA half of the Top-K consumer coverage. GPU REQUIRED.
-- See cases/vector/vector_ivf_topk_consumers.sql for IVFFLAT and
-- vector_hnsw_topk_consumers.sql for HNSW.
--
-- A vector Top-K must keep using the index when something CONSUMES it: an outer
-- ORDER BY (#25967) or a relational JOIN (#25974). Both used to disable the rewrite
-- and silently fall back to a full scan with an exact sort -- correct results, but
-- the index unused and nothing in the result set to show it. Every EXPLAIN below
-- asserts cagra_search is present, and every query is checked against t_ref, the
-- same rows with no index, so a plan that reaches the index but returns the wrong
-- rows still fails.
--
-- CAGRA is approximate. The probe vector exactly matches an indexed row and the
-- clusters here are far apart ([1..5] vs [50..54]), so the returned top-3 is stable
-- for this toy set (same construction as vector_cagra.sql).
-- =====================================================================

SET experimental_cagra_index = 1;
SET cagra_threads_build = 7;

drop database if exists cagra_topk_consumers;
create database cagra_topk_consumers;
use cagra_topk_consumers;

create table t_cagra(id bigint primary key, v vecf32(4), tag int);
insert into t_cagra values
    (1,'[1,1,1,1]',10),(2,'[3,3,3,3]',20),(3,'[5,5,5,5]',30),
    (4,'[50,50,50,50]',40),(5,'[52,52,52,52]',50),(6,'[54,54,54,54]',60);
create index i_cagra using cagra on t_cagra(v) op_type 'vector_l2_ops';

-- Unindexed twin: CAGRA has no mode=force, so the brute-force reference is a copy of
-- the same rows with no index on them.
create table t_ref(id bigint primary key, v vecf32(4), tag int);
insert into t_ref select id, v, tag from t_cagra;

create table meta(id bigint primary key, name varchar(20));
insert into meta values (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f');

-- ---------------- control: no consumer ---------------------------------------
-- @separator:table
-- @regex("Table Function on cagra_search", true)
explain select id, l2_distance(v,'[1,1,1,1]') as d from t_cagra order by l2_distance(v,'[1,1,1,1]') limit 3;

-- ---------------- #25967: outer ORDER BY -------------------------------------
-- @separator:table
-- @regex("Table Function on cagra_search", true)
explain with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_cagra
    order by l2_distance(v,'[1,1,1,1]') limit 3
) select id, d from knn order by d;

with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_cagra
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
    select id, l2_distance(v,'[1,1,1,1]') as d from t_cagra
    order by l2_distance(v,'[1,1,1,1]') limit 3
) select id, d from knn order by d desc;

-- Ordering by a different column than the Top-K metric must also be respected.
with knn as (
    select id, tag, l2_distance(v,'[1,1,1,1]') as d from t_cagra
    order by l2_distance(v,'[1,1,1,1]') limit 3
) select id, tag from knn order by tag desc;

-- ---------------- #25974: JOIN consumer --------------------------------------
-- @separator:table
-- @regex("Table Function on cagra_search", true)
explain with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_cagra
    order by l2_distance(v,'[1,1,1,1]') limit 3
) select k.id, m.name from knn k left join meta m on k.id = m.id;

with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_cagra
    order by l2_distance(v,'[1,1,1,1]') limit 3
) select k.id, k.d, m.name from knn k left join meta m on k.id = m.id order by k.d;

with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_ref
    order by l2_distance(v,'[1,1,1,1]') limit 3
) select k.id, k.d, m.name from knn k left join meta m on k.id = m.id order by k.d;

-- @separator:table
-- @regex("Table Function on cagra_search", true)
explain with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_cagra
    order by l2_distance(v,'[1,1,1,1]') limit 3
) select k.id, m.name from knn k inner join meta m on k.id = m.id;

with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_cagra
    order by l2_distance(v,'[1,1,1,1]') limit 3
) select k.id, m.name from knn k inner join meta m on k.id = m.id order by k.id;

-- ---------------- aggregate consumer -----------------------------------------
-- count(*) over a Top-K prunes the derived projection to nothing while the sort still
-- carries its original ORDER BY column position. Indexing that pruned list blind panicked
-- the CN ("index out of range [N] with length 0") once this shape started reaching the
-- rewrite, so keep an aggregate consumer covered on this algorithm too.
select count(*) as n from (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_cagra
    order by l2_distance(v,'[1,1,1,1]') limit 3
) t;

select count(*) as n from (
    select id from t_cagra where tag >= 20
    order by l2_distance(v,'[1,1,1,1]') limit 3
) t;

-- ---------------- both consumers at once -------------------------------------
-- @separator:table
-- @regex("Table Function on cagra_search", true)
explain with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_cagra
    order by l2_distance(v,'[1,1,1,1]') limit 3
) select k.id, m.name from knn k left join meta m on k.id = m.id order by k.d desc;

with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_cagra
    order by l2_distance(v,'[1,1,1,1]') limit 3
) select k.id, m.name from knn k left join meta m on k.id = m.id order by k.d desc;

drop database cagra_topk_consumers;
