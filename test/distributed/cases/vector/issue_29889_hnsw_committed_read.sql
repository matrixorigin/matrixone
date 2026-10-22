-- Regression for #29889: a committed row must not disappear when the async
-- HNSW generation has not consumed the latest source-table commit yet.
set experimental_hnsw_index = 1;

drop database if exists issue_29889_hnsw;
create database issue_29889_hnsw;
use issue_29889_hnsw;

create table t (id bigint primary key, v vecf32(2));
insert into t values (1, '[10,10]'), (2, '[20,20]');
create index idx_hnsw using hnsw on t(v) op_type 'vector_l2_ops';

-- Warm the initial generation before adding the row that exercises the gap.
select id from t order by l2_distance(v, '[0,0]') limit 1;

insert into t values (3, '[0,0]');

-- The exact source scan fallback must return the just-committed nearest row,
-- even if the resident HNSW generation is still behind.
select id from t order by l2_distance(v, '[0,0]') limit 1;

drop database issue_29889_hnsw;
set experimental_hnsw_index = 0;
