-- =====================================================================
-- vector_cagra_delete.sql — CAGRA soft-delete: search excludes deleted rows
--
-- GPU REQUIRED. Builds a sync CAGRA index, deletes a row, and uses one bounded
-- exact-match readiness query to wait until the ISCP/CDC consumer propagates
-- the delete to the index's per-device deleted bitset. The original searches
-- then confirm the deleted row is excluded and the next survivor is returned.
-- Lives under pessimistic_transaction/ because, like the async cases, it
-- depends on CDC catch-up.
--
-- Data: id=i -> [v]*8 with v doubling (10,20,40,...,5120) so a deleted row has a
-- UNIQUE nearest survivor (no equidistant tie). Delete id=5 ([160]*8):
--   * query [160]*8  -> id 4 ([80], the unique nearest survivor; id 6 [320] is farther)
--   * query [1280]*8 -> id 8 (untouched row still found)
-- =====================================================================

SET experimental_cagra_index = 1;
SET cagra_threads_build = 7;
SET cagra_max_index_capacity = 99999;

drop database if exists cagra_delete;
create database cagra_delete;
use cagra_delete;

create table t (id bigint primary key, v vecf32(8));
insert into t values
    (1, '[10,10,10,10,10,10,10,10]'),     (2, '[20,20,20,20,20,20,20,20]'),
    (3, '[40,40,40,40,40,40,40,40]'),     (4, '[80,80,80,80,80,80,80,80]'),
    (5, '[160,160,160,160,160,160,160,160]'),     (6, '[320,320,320,320,320,320,320,320]'),
    (7, '[640,640,640,640,640,640,640,640]'),     (8, '[1280,1280,1280,1280,1280,1280,1280,1280]'),
    (9, '[2560,2560,2560,2560,2560,2560,2560,2560]'),     (10, '[5120,5120,5120,5120,5120,5120,5120,5120]');

create index ix using cagra on t (v) op_type 'vector_l2_ops' intermediate_graph_degree=8 graph_degree=4 itopk_size=16;

-- Baseline: the exact row is the top-1 before deletion.
select id from t order by l2_distance(v, '[160,160,160,160,160,160,160,160]') asc limit 1;

-- Delete it; CDC must propagate to the deleted bitset before search reflects it.
delete from t where id = 5;

select count(*) from t;
-- One readiness gate covers all CDC-dependent probes. It returns one row
-- only after every probe has its expected search result.
-- @wait_expect(1, 60)
select 1 as ready
from (
    select 'delete-160' as probe_name, q.id as actual_id, 4 as expected_id
    from (select id from t order by l2_distance(v, '[160,160,160,160,160,160,160,160]') asc limit 1) q
    union all
    select 'untouched-1280' as probe_name, q.id as actual_id, 8 as expected_id
    from (select id from t order by l2_distance(v, '[1280,1280,1280,1280,1280,1280,1280,1280]') asc limit 1) q
) readiness
having count(*) = 2
   and sum(case when actual_id = expected_id then 1 else 0 end) = 2;


-- Deleted row is gone -> next unique survivor (id 4); an untouched row is unaffected.
select id from t order by l2_distance(v, '[160,160,160,160,160,160,160,160]') asc limit 1;
select id from t order by l2_distance(v, '[1280,1280,1280,1280,1280,1280,1280,1280]') asc limit 1;

drop database cagra_delete;
