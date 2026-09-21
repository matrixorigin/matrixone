-- HNSW cosine boundary regression for #29082.
-- usearch's raw cosine score can return 0 for zero-vs-zero and +/-Inf for tiny
-- finite vectors. These queries exercise the public SQL consumers whose result
-- sets/aggregates change when those values leak out of the index path.
drop database if exists hnsw_cosine_boundary;
create database hnsw_cosine_boundary;
use hnsw_cosine_boundary;

set experimental_hnsw_index = 1;

create table h_src(id bigint primary key, v vecf32(3), label varchar(20));
insert into h_src values
    (1, '[0,0,0]', 'zero'),
    (2, '[1e-20,1e-20,1e-20]', 'tiny'),
    (3, '[1,1,1]', 'unit'),
    (4, '[2,1,1]', 'skew'),
    (5, '[-1,-1,-1]', 'opposite');
create index h_cos using hnsw on h_src(v) op_type 'vector_cosine_ops';
alter table h_src alter reindex h_cos hnsw force_sync;

create table h_labels(id bigint primary key, name varchar(20));
insert into h_labels values
    (1, 'zero'), (2, 'tiny'), (3, 'unit'), (4, 'skew'), (5, 'opposite');

-- The planner must keep cosine on the exact SQL path. A post-search clamp would
-- be too late because HNSW has already selected its global top-K candidates.
-- @separator:table
-- @regex("hnsw_search", false)
explain select id, cosine_distance(v, '[1e-20,1e-20,1e-20]') as d
  from h_src
 order by cosine_distance(v, '[1e-20,1e-20,1e-20]')
 limit 3;

-- This is the result-set failure on the unfixed HNSW path: the tiny query's
-- projected scores can be negative or -Inf even though the scalar contract is
-- finite and non-negative.
-- Positive scaling preserves direction: d([2,1,1], [t,t,t]) = 1-4/sqrt(18)
-- for t > 0, including t=1e-20. The expected value must not preserve the old
-- float32 squared-norm underflow error (0.057188 instead of 0.057191).
select id, round(cosine_distance(v, '[1e-20,1e-20,1e-20]'), 6) as d
  from h_src
 order by cosine_distance(v, '[1e-20,1e-20,1e-20]')
 limit 3;
select id, cosine_distance(v, '[1e-20,1e-20,1e-20]') as raw_d
  from h_src
 order by cosine_distance(v, '[1e-20,1e-20,1e-20]')
 limit 3;

-- Zero-vector convention: every zero-norm pair has SQL distance 1, including
-- zero-vs-zero. The tiny query also must remain finite and non-negative.
select id, cosine_distance(v, '[0,0,0]') as zero_query_d
  from h_src order by id;
select id, cosine_distance(v, '[0,0,0]') as zero_query_d
  from h_src
 order by cosine_distance(v, '[0,0,0]'), id
 limit 3;
select id, round(cosine_distance(v, '[1e-20,1e-20,1e-20]'), 6) as tiny_query_d
  from h_src order by id;

-- Derived filtering and aggregation must see the scalar contract, not -Inf or
-- a negative score from the index implementation.
with ranked as (
    select id, cosine_distance(v, '[1e-20,1e-20,1e-20]') as d
      from h_src
     order by cosine_distance(v, '[1e-20,1e-20,1e-20]'), id
     limit 3
)
select count(*) as negative_scores
  from ranked q
 where d < 0;
with ranked as (
    select id, cosine_distance(v, '[1e-20,1e-20,1e-20]') as d
      from h_src
     order by cosine_distance(v, '[1e-20,1e-20,1e-20]'), id
     limit 3
)
select min(d) as min_score, max(d) as max_score
  from ranked q;

-- Pagination and a relational consumer use the same exact distances.
with ranked as (
    select id, cosine_distance(v, '[1e-20,1e-20,1e-20]') as d
      from h_src
     order by cosine_distance(v, '[1e-20,1e-20,1e-20]'), id
     limit 4
)
select id, round(d, 6) as d from ranked order by d, id limit 2 offset 1;

with ranked as (
    select id, cosine_distance(v, '[1e-20,1e-20,1e-20]') as d
      from h_src
     order by cosine_distance(v, '[1e-20,1e-20,1e-20]'), id
     limit 3
)
select r.id, round(r.d, 6) as d, l.name
  from ranked r join h_labels l on r.id = l.id
 order by r.d, r.id;

set experimental_hnsw_index = 0;
drop database hnsw_cosine_boundary;
