-- GPU REQUIRED. docs/sql/vector-distance-functions.md, CAGRA and IVF-PQ rows: each op_type
-- serves its SQL functions; CAGRA returns the scalar function's value in the float32 domain
-- (inner_product negated); IVF-PQ answers from compressed codes; neither accepts l1.
drop database if exists vec_distance_semantics_gpu;
create database vec_distance_semantics_gpu;
use vec_distance_semantics_gpu;
set experimental_cagra_index = 1;
set experimental_ivfpq_index = 1;
create table t (id bigint primary key, v vecf32(3));
insert into t values (1,'[0.5,0.5,-3]'),(2,'[-0.5,-1,2]'),(3,'[-2,-3,-1]'),(4,'[-0.5,3,0.5]'),(5,'[-3,1,0.5]'),(6,'[2,-2,-2]'),(7,'[-2,-0.5,-0.5]'),(8,'[-3,1,1]'),(9,'[2,4,6]');

create index ix using cagra on t(v) op_type 'vector_l2_ops';
-- @separator:table
-- @regex("Vector Index Scan on ix \[l2_distance\]", true)
explain select id from t order by l2_distance(v, '[1,2,3]') limit 3;
select id, round(l2_distance(v, '[1,2,3]'), 4) d from t order by l2_distance(v, '[1,2,3]') limit 3;
-- @separator:table
-- @regex("Vector Index Scan on ix \[l2_distance_sq\]", true)
explain select id from t order by l2_distance_sq(v, '[1,2,3]') limit 3;
select id, round(l2_distance_sq(v, '[1,2,3]'), 4) d from t order by l2_distance_sq(v, '[1,2,3]') limit 3;
drop index ix on t;
create index ix using cagra on t(v) op_type 'vector_l2sq_ops';
-- @separator:table
-- @regex("Vector Index Scan on ix \[l2_distance\]", true)
explain select id from t order by l2_distance(v, '[1,2,3]') limit 3;
select id, round(l2_distance(v, '[1,2,3]'), 4) d from t order by l2_distance(v, '[1,2,3]') limit 3;
drop index ix on t;
create index ix using cagra on t(v) op_type 'vector_ip_ops';
-- @separator:table
-- @regex("Vector Index Scan on ix \[inner_product\]", true)
explain select id from t order by inner_product(v, '[1,2,3]') limit 3;
select id, round(inner_product(v, '[1,2,3]'), 4) d from t order by inner_product(v, '[1,2,3]') limit 3;
drop index ix on t;
create index ix using cagra on t(v) op_type 'vector_cosine_ops';
-- @separator:table
-- @regex("Vector Index Scan on ix \[cosine_distance\]", true)
explain select id from t order by cosine_distance(v, '[1,2,3]') limit 3;
-- row 9 has the query's direction: its distance is 0 up to float32 rounding
select id, abs(cosine_distance(v, '[1,2,3]')) < 1e-6 near_zero from t order by cosine_distance(v, '[1,2,3]') limit 1;
select id, round(cosine_distance(v, '[1,2,3]'), 4) d from t order by cosine_distance(v, '[1,2,3]') limit 3;
drop index ix on t;
create index ix using cagra on t(v) op_type 'vector_l1_ops';

create index ix using ivfpq on t(v) lists = 2 m = 3 op_type 'vector_ip_ops';
-- @separator:table
-- @regex("Vector Index Scan on ix \[inner_product\]", true)
explain select id from t order by inner_product(v, '[1,2,3]') limit 3;
drop index ix on t;
create index ix using ivfpq on t(v) lists = 2 m = 3 op_type 'vector_l1_ops';

drop database vec_distance_semantics_gpu;
