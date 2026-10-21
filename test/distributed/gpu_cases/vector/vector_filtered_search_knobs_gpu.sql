-- GPU REQUIRED. BY RANK search knobs on cagra and ivfpq (#28988). The 500 nearest rows
-- fail the filter: the default candidate budget returns no row, itopk 1000 returns a full
-- page (cagra; its graph build is not deterministic) or ids 501..510 (ivfpq). nprobe and
-- max_nprobe apply to ivfpq only.
drop database if exists vector_filtered_search_knobs_gpu;
create database vector_filtered_search_knobs_gpu;
use vector_filtered_search_knobs_gpu;

set experimental_cagra_index = 1;
set experimental_ivfpq_index = 1;

create table t_cagra(id bigint primary key, category int not null, v vecf32(8));
insert into t_cagra select result, if(result <= 500, 0, 1), cast(concat('[', result, ',0,0,0,0,0,0,0]') as vecf32(8)) from generate_series(1, 1000, 1) g;
create index i_cagra using cagra on t_cagra(v) op_type 'vector_l2_ops' intermediate_graph_degree=16 graph_degree=8;

create table t_ivfpq(id bigint primary key, category int not null, v vecf32(8));
insert into t_ivfpq select id, category, v from t_cagra;
create index i_ivfpq using ivfpq on t_ivfpq(v) op_type 'vector_l2_ops' lists=2 m=8 bits_per_code=8;

-- cagra
select count(*) from (select id from t_cagra where category = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10) x;
select count(*) from (select id from t_cagra where category = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'itopk=1000') x;
-- @separator:table
-- @regex("Internal Top-K: 1000", true)
explain select id from t_cagra where category = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'itopk=1000';
select id from t_cagra where category = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'nprobe=2';
select id from t_cagra where category = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'max_nprobe=2';

-- ivfpq
select count(*) from (select id from t_ivfpq where category = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10) x;
select group_concat(id order by id) from (select id from t_ivfpq where category = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'nprobe=2', 'itopk=1000') x;
-- @separator:table
-- @regex("Internal Top-K: 1000, Max Internal Top-K: 2000, Max NProbe: 2", true)
explain select id from t_ivfpq where category = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'nprobe=2', 'itopk=1000', 'max_itopk=2000', 'max_nprobe=2';

-- iterative applies to cagra and ivfpq: category is not an INCLUDE column here, so it goes
-- through membership and the page fills; mode=include is an alias of mode=iterative
select count(*) from (select id from t_cagra where category = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=iterative') x;
select count(*) from (select id from t_cagra where category = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=include') x;
select count(*) from (select id from t_ivfpq where category = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=iterative') x;
select count(*) from (select id from t_ivfpq where category = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=iterative') x where x.id <= 500;
select count(*) from (select id from t_ivfpq where category = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=include') x;

drop database vector_filtered_search_knobs_gpu;
