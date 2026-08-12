-- A vector Top-K must keep using the index when something CONSUMES it: an outer
-- ORDER BY (#25967) or a relational JOIN (#25974). Both used to disable the IVFFLAT /
-- HNSW rewrite and silently fall back to a full scan with an exact sort -- correct
-- results, but the index unused, which is a large regression on real vector tables and
-- invisible from the result set. Every EXPLAIN below therefore asserts ivf_search is
-- present, and every query is checked against the same Top-K computed without the index
-- (mode=force) so a plan that reaches the index but returns the wrong rows still fails.
drop database if exists ivf_topk_consumers;
create database ivf_topk_consumers;
use ivf_topk_consumers;

set experimental_ivf_index = 1;

create table t_ivf(id bigint primary key, v vecf32(4), tag int);
insert into t_ivf values
    (1,'[1,1,1,1]',10),(2,'[3,3,3,3]',20),(3,'[5,5,5,5]',30),
    (4,'[50,50,50,50]',40),(5,'[52,52,52,52]',50),(6,'[54,54,54,54]',60);
create index i_ivf using ivfflat on t_ivf(v) lists=2 op_type 'vector_l2_ops';

create table meta(id bigint primary key, name varchar(20));
insert into meta values (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f');

-- ---------------- control: no consumer ---------------------------------------
-- A Top-K whose parent IS a project must still be anchored there, not by the SORT entry
-- point added for the consumer shapes below. The sort anchor has no project to read
-- column requirements from, so it disables the index-only scan: catching that needs the
-- ABSENCE of a base table scan, not just the presence of ivf_search. Without this
-- assertion the anchor arbitration can regress and every plain Top-K silently grows a
-- join back to the base table while all the consumer assertions still pass.
-- @separator:table
-- @regex("Table Function on ivf_search", true)
-- @regex("Table Scan on", false)
explain select id, l2_distance(v,'[1,1,1,1]') as d from t_ivf order by l2_distance(v,'[1,1,1,1]') limit 3;

-- @separator:table
-- @regex("Table Function on ivf_search", true)
-- @regex("Table Scan on", false)
explain select id from t_ivf order by l2_distance(v,'[1,1,1,1]') limit 3;

-- ---------------- #25967: outer ORDER BY -------------------------------------
-- @separator:table
-- @regex("Table Function on ivf_search", true)
explain with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_ivf
    order by l2_distance(v,'[1,1,1,1]') limit 3
) select id, d from knn order by d;

with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_ivf
    order by l2_distance(v,'[1,1,1,1]') limit 3
) select id, d from knn order by d;

-- same Top-K without the index: the rows and scores must be identical.
with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_ivf
    order by l2_distance(v,'[1,1,1,1]') limit 3 by rank with option 'mode=force'
) select id, d from knn order by d;

-- The outer ordering must survive the rewrite. A fix that simply skipped the outer sort
-- to find the Top-K would silently return ascending rows here.
with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_ivf
    order by l2_distance(v,'[1,1,1,1]') limit 3
) select id, d from knn order by d desc;

-- Ordering by a different column than the Top-K metric must also be respected.
with knn as (
    select id, tag, l2_distance(v,'[1,1,1,1]') as d from t_ivf
    order by l2_distance(v,'[1,1,1,1]') limit 3
) select id, tag from knn order by tag desc;

-- ---------------- #25974: JOIN consumer --------------------------------------
-- @separator:table
-- @regex("Table Function on ivf_search", true)
explain with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_ivf
    order by l2_distance(v,'[1,1,1,1]') limit 3
) select k.id, m.name from knn k left join meta m on k.id = m.id;

with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_ivf
    order by l2_distance(v,'[1,1,1,1]') limit 3
) select k.id, k.d, m.name from knn k left join meta m on k.id = m.id order by k.d;

with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_ivf
    order by l2_distance(v,'[1,1,1,1]') limit 3 by rank with option 'mode=force'
) select k.id, k.d, m.name from knn k left join meta m on k.id = m.id order by k.d;

-- @separator:table
-- @regex("Table Function on ivf_search", true)
explain with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_ivf
    order by l2_distance(v,'[1,1,1,1]') limit 3
) select k.id, m.name from knn k inner join meta m on k.id = m.id;

with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_ivf
    order by l2_distance(v,'[1,1,1,1]') limit 3
) select k.id, m.name from knn k inner join meta m on k.id = m.id order by k.id;

-- ---------------- aggregate consumer -----------------------------------------
-- count(*) over a Top-K prunes the derived projection to nothing while the sort still
-- carries its original ORDER BY column position. Indexing that pruned list blind panicked
-- the CN ("index out of range [N] with length 0") once this shape started reaching the
-- rewrite, so keep an aggregate consumer covered.
select count(*) as n from (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_ivf
    order by l2_distance(v,'[1,1,1,1]') limit 3
) t;

select count(*) as n from (
    select id from t_ivf where tag >= 20
    order by l2_distance(v,'[1,1,1,1]') limit 3
) t;

-- ---------------- both consumers at once -------------------------------------
-- @separator:table
-- @regex("Table Function on ivf_search", true)
explain with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_ivf
    order by l2_distance(v,'[1,1,1,1]') limit 3
) select k.id, m.name from knn k left join meta m on k.id = m.id order by k.d desc;

with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_ivf
    order by l2_distance(v,'[1,1,1,1]') limit 3
) select k.id, m.name from knn k left join meta m on k.id = m.id order by k.d desc;

-- ---------------- filtered inner Top-K, secondary index present --------------
-- applyIndices is post-order: without a guard entry for the SORT anchor,
-- applyIndicesForFilters rewrites this scan into a secondary-index join before the
-- vector rewrite ever runs, and resolveScanNodeWithIndex then finds a JOIN instead of a
-- TABLE_SCAN -- the Top-K silently falls back to a full scan + exact sort. Every other
-- case in this file has a filter-free inner query and cannot catch that.
create table t_guard(id bigint primary key, v vecf32(4), tag int);
insert into t_guard values
    (1,'[1,1,1,1]',1),(2,'[3,3,3,3]',1),(3,'[5,5,5,5]',1),
    (4,'[50,50,50,50]',2),(5,'[52,52,52,52]',2),(6,'[54,54,54,54]',2);
create index g_vec using ivfflat on t_guard(v) lists=2 op_type 'vector_l2_ops';
create index g_tag on t_guard(tag);

-- @separator:table
-- @regex("Table Function on ivf_search", true)
explain with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_guard
    where tag = 1 order by l2_distance(v,'[1,1,1,1]') limit 3
) select k.id, m.name from knn k join meta m on k.id = m.id;

with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_guard
    where tag = 1 order by l2_distance(v,'[1,1,1,1]') limit 3
) select k.id, k.d, m.name from knn k join meta m on k.id = m.id order by k.d;

with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_guard
    where tag = 1 order by l2_distance(v,'[1,1,1,1]') limit 3 by rank with option 'mode=force'
) select k.id, k.d, m.name from knn k join meta m on k.id = m.id order by k.d;

-- The reference above is index-FREE by definition: mode=force makes the rewrite bail
-- ("Disable vector index, force full table scan"). So it proves the ROWS are right but
-- exercises no index path. pre/post/auto are the index-using modes and they differ
-- precisely on filtered queries -- run each against the same reference.
-- @separator:table
-- @regex("Table Function on ivf_search", true)
explain with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_guard
    where tag = 1 order by l2_distance(v,'[1,1,1,1]') limit 3 by rank with option 'mode=pre'
) select k.id, m.name from knn k join meta m on k.id = m.id;

with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_guard
    where tag = 1 order by l2_distance(v,'[1,1,1,1]') limit 3 by rank with option 'mode=pre'
) select k.id, k.d from knn k order by k.d;

-- @separator:table
-- @regex("Table Function on ivf_search", true)
explain with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_guard
    where tag = 1 order by l2_distance(v,'[1,1,1,1]') limit 3 by rank with option 'mode=post'
) select k.id, m.name from knn k join meta m on k.id = m.id;

with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_guard
    where tag = 1 order by l2_distance(v,'[1,1,1,1]') limit 3 by rank with option 'mode=post'
) select k.id, k.d from knn k order by k.d;

-- @separator:table
-- @regex("Table Function on ivf_search", true)
explain with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_guard
    where tag = 1 order by l2_distance(v,'[1,1,1,1]') limit 3 by rank with option 'mode=auto'
) select k.id, m.name from knn k join meta m on k.id = m.id;

with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_guard
    where tag = 1 order by l2_distance(v,'[1,1,1,1]') limit 3 by rank with option 'mode=auto'
) select k.id, k.d from knn k order by k.d;

-- same, consumed by an outer ORDER BY instead of a join
-- @separator:table
-- @regex("Table Function on ivf_search", true)
explain with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_guard
    where tag = 1 order by l2_distance(v,'[1,1,1,1]') limit 3
) select id, d from knn order by d desc;

with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_guard
    where tag = 1 order by l2_distance(v,'[1,1,1,1]') limit 3
) select id, d from knn order by d desc;

-- ---------------- two Top-Ks in one query ------------------------------------
-- Each sort-anchored rewrite publishes its column remap into the SHARED idxColMap, so two
-- Top-Ks in one statement can collide there. Both must be rewritten and both must keep
-- their own rows.
-- @separator:table
-- @regex("Table Function on ivf_search", true)
explain with a as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_ivf
    order by l2_distance(v,'[1,1,1,1]') limit 2
), b as (
    select id, l2_distance(v,'[54,54,54,54]') as d from t_ivf
    order by l2_distance(v,'[54,54,54,54]') limit 2
) select a.id as near_id, b.id as far_id from a join b on a.id <> b.id order by near_id, far_id;

with a as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_ivf
    order by l2_distance(v,'[1,1,1,1]') limit 2
), b as (
    select id, l2_distance(v,'[54,54,54,54]') as d from t_ivf
    order by l2_distance(v,'[54,54,54,54]') limit 2
) select a.id as near_id, b.id as far_id from a join b on a.id <> b.id order by near_id, far_id;

-- ---------------- LIMIT + OFFSET inside the consumed CTE ---------------------
-- The candidate budget and the result pagination are separate; a consumer must still see
-- the offset applied exactly once.
-- @separator:table
-- @regex("Table Function on ivf_search", true)
explain with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_ivf
    order by l2_distance(v,'[1,1,1,1]') limit 2 offset 1
) select id, d from knn order by d;

with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_ivf
    order by l2_distance(v,'[1,1,1,1]') limit 2 offset 1
) select id, d from knn order by d;

with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_ivf
    order by l2_distance(v,'[1,1,1,1]') limit 2 offset 1 by rank with option 'mode=force'
) select id, d from knn order by d;

-- ---------------- projection without the pk (column pruning) ----------------
-- The search table function declares pkid/score (plus INCLUDE columns) but the planner
-- projects only what the query reads. A score-only SELECT prunes pkid, and the runtime
-- used to write its int64 pk into whatever vector sat at position 0 -- the float64 score
-- -- panicking the CN with "interface conversion: interface {} is int64, not float64".
-- Pre-existing on main and reachable from plain SQL, so keep a case on it.
-- @separator:table
-- @regex("Table Function on ivf_search", true)
explain select l2_distance(v,'[1,1,1,1]') as d from t_ivf order by l2_distance(v,'[1,1,1,1]') limit 3;

select l2_distance(v,'[1,1,1,1]') as d from t_ivf order by l2_distance(v,'[1,1,1,1]') limit 3;

select count(*) as n from (
    select l2_distance(v,'[1,1,1,1]') as d from t_ivf order by l2_distance(v,'[1,1,1,1]') limit 3
) x;

-- ---------------- index-only scan under a consumer ---------------------------
-- An index-only scan drops the base table and answers straight from ivf_search. It needs
-- a projection bounding which base columns can still be read; a sort-anchored rewrite has
-- no project above the Top-K, so the Top-K's OWN projection is that bound -- a consumer
-- can only read what the derived table exposes. Assert the base scan is gone, not merely
-- that ivf_search appears: without the bound these shapes keep a join back to the table.
-- @separator:table
-- @regex("Table Function on ivf_search", true)
-- @regex("Table Scan on", false)
explain with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_ivf
    order by l2_distance(v,'[1,1,1,1]') limit 3
) select id, d from knn order by d;

-- and the rows must still match the unindexed Top-K.
with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_ivf
    order by l2_distance(v,'[1,1,1,1]') limit 3
) select id, d from knn order by d;

with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_ivf
    order by l2_distance(v,'[1,1,1,1]') limit 3 by rank with option 'mode=force'
) select id, d from knn order by d;

-- The bound must REFUSE when the Top-K exposes a column the index does not cover: `tag`
-- is neither the pk nor an INCLUDE column, so dropping the base scan would lose it. The
-- base scan must survive here -- this is the assertion that keeps the optimization honest.
-- @separator:table
-- @regex("Table Function on ivf_search", true)
-- @regex("Table Scan on", true)
explain with knn as (
    select id, tag, l2_distance(v,'[1,1,1,1]') as d from t_ivf
    order by l2_distance(v,'[1,1,1,1]') limit 3
) select id, tag, d from knn order by d;

with knn as (
    select id, tag, l2_distance(v,'[1,1,1,1]') as d from t_ivf
    order by l2_distance(v,'[1,1,1,1]') limit 3
) select id, tag, d from knn order by d;

with knn as (
    select id, tag, l2_distance(v,'[1,1,1,1]') as d from t_ivf
    order by l2_distance(v,'[1,1,1,1]') limit 3 by rank with option 'mode=force'
) select id, tag, d from knn order by d;

-- #25974 join consumer: the Top-K side goes index-only, so the user's own join binds
-- directly to the table function's pkid instead of to a second scan of the base table.
-- @separator:table
-- @regex("Table Function on ivf_search", true)
-- @regex("mo_ivf_alias_0.pkid", true)
explain with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_ivf
    order by l2_distance(v,'[1,1,1,1]') limit 3
) select knn.id, knn.d, m.name from knn join meta m on m.id = knn.id order by knn.d;

with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_ivf
    order by l2_distance(v,'[1,1,1,1]') limit 3
) select knn.id, knn.d, m.name from knn join meta m on m.id = knn.id order by knn.d;

with knn as (
    select id, l2_distance(v,'[1,1,1,1]') as d from t_ivf
    order by l2_distance(v,'[1,1,1,1]') limit 3 by rank with option 'mode=force'
) select knn.id, knn.d, m.name from knn join meta m on m.id = knn.id order by knn.d;

drop database ivf_topk_consumers;
