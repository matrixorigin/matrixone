-- Rank modes by index capability (#28988). mode=pre, mode=iterative and mode=auto need the
-- membership capability, which ivfflat and hnsw have; mode=include is ivfflat only.
-- mode=post, mode=force and no clause are accepted on both.
drop database if exists vector_filtered_search_modes;
create database vector_filtered_search_modes;
use vector_filtered_search_modes;

set experimental_hnsw_index = 1;
set experimental_ivf_index = 1;

create table t_hnsw(id bigint primary key, v vecf32(2), category int);
insert into t_hnsw select result, cast(concat('[', result, ',0]') as vecf32(2)), if(result <= 50, 0, 1) from generate_series(1, 100, 1) g;
create index i_hnsw using hnsw on t_hnsw(v) op_type 'vector_l2_ops';

create table t_ivf(id bigint primary key, v vecf32(2), category int);
insert into t_ivf select id, v, category from t_hnsw;
create index i_ivf using ivfflat on t_ivf(v) lists = 2 op_type 'vector_l2_ops';

-- hnsw: pre searches the qualifying keys (ids 51..53); auto is accepted
-- @separator:table
-- @regex("Join Type: SEMI", true)
explain select id from t_hnsw where category = 1 order by l2_distance(v, '[0,0]') limit 3 by rank with option 'mode=pre';
select id from t_hnsw where category = 1 order by l2_distance(v, '[0,0]') limit 3 by rank with option 'mode=pre';
select id from t_hnsw where category = 1 order by l2_distance(v, '[0,0]') limit 3 by rank with option 'mode=auto';
-- hnsw: iterative is accepted, include is ivfflat only
select id from t_hnsw where category = 1 order by l2_distance(v, '[0,0]') limit 3 by rank with option 'mode=iterative';
select id from t_hnsw where category = 1 order by l2_distance(v, '[0,0]') limit 3 by rank with option 'mode=include';

-- hnsw: post, force and no clause are accepted
-- @separator:table
-- @regex("Vector Index Scan on", true)
explain select id from t_hnsw where category = 1 order by l2_distance(v, '[0,0]') limit 3 by rank with option 'mode=post';
-- @separator:table
-- @regex("Vector Index Scan on", false)
explain select id from t_hnsw where category = 1 order by l2_distance(v, '[0,0]') limit 3 by rank with option 'mode=force';
select id from t_hnsw where category = 1 order by l2_distance(v, '[0,0]') limit 3 by rank with option 'mode=force';
-- @separator:table
-- @regex("Vector Index Scan on", true)
explain select id from t_hnsw where category = 1 order by l2_distance(v, '[0,0]') limit 3;

-- ivfflat: pre and auto are accepted
-- @separator:table
-- @regex("Join Type: SEMI", true)
explain select id from t_ivf where category = 1 order by l2_distance(v, '[0,0]') limit 3 by rank with option 'mode=pre';
select id from t_ivf where category = 1 order by l2_distance(v, '[0,0]') limit 3 by rank with option 'mode=pre';
select id from t_ivf where category = 1 order by l2_distance(v, '[0,0]') limit 3 by rank with option 'mode=auto';
-- ivfflat: iterative and include are accepted
select id from t_ivf where category = 1 order by l2_distance(v, '[0,0]') limit 3 by rank with option 'mode=iterative';
select id from t_ivf where category = 1 order by l2_distance(v, '[0,0]') limit 3 by rank with option 'mode=include';

drop database vector_filtered_search_modes;
