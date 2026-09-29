-- HNSW cosine boundary for #29082 (cosine ANN restored).
-- Cosine now uses native usearch ANN. HNSW cosine assumes caller-normalized vectors: a
-- normalized query is served by the index; a zero/subnormal query vector -- whose float32
-- squared norm underflows, so usearch cannot score it to the SQL cosine_distance contract --
-- is rejected fail-fast instead of leaking a wrong score.
drop database if exists hnsw_cosine_boundary;
create database hnsw_cosine_boundary;
use hnsw_cosine_boundary;

set experimental_hnsw_index = 1;

-- All rows are unit vectors (cosine is defined for normalized data).
create table h_norm(id bigint primary key, v vecf32(3));
insert into h_norm values
    (1, '[1,0,0]'),
    (2, '[0,1,0]'),
    (3, '[0,0,1]'),
    (4, '[0.70710677,0.70710677,0]'),
    (5, '[0.57735026,0.57735026,0.57735026]');
create index h_cos using hnsw on h_norm(v) op_type 'vector_cosine_ops';
alter table h_norm alter reindex h_cos hnsw force_sync;

-- A normalized query is served by the cosine index (native ANN), not the exact scan.
-- @separator:table
-- @regex("hnsw_search", true)
explain select id, cosine_distance(v, '[1,0,0]') as d
  from h_norm order by cosine_distance(v, '[1,0,0]') limit 3;

-- ... and returns the correct nearest neighbours.
select id, round(cosine_distance(v, '[1,0,0]'), 6) as d
  from h_norm order by cosine_distance(v, '[1,0,0]') limit 3;

-- A zero query vector has no direction; usearch's float32 cosine cannot score it, so the
-- index-served query is rejected fail-fast.
select id from h_norm order by cosine_distance(v, '[0,0,0]') limit 3;

-- A subnormal query (float32 squared-norm underflow) is likewise rejected.
select id from h_norm order by cosine_distance(v, '[1e-20,1e-20,1e-20]') limit 3;

-- The scalar cosine_distance (no index: ORDER BY id) is unaffected and keeps its
-- zero-vector convention (distance 1 for every row).
select id, cosine_distance(v, '[0,0,0]') as d from h_norm order by id;

set experimental_hnsw_index = 0;
drop database hnsw_cosine_boundary;
