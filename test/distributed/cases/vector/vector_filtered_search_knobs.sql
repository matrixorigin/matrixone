-- BY RANK search knobs (#28988). itopk sets the internal top-k of the index search: the
-- issue fixture (the 500 nearest rows fail the filter) returns no rows with the default
-- candidate budget and ids 501..510 with itopk covering the index. Session variables
-- default the knobs, and a SET after PREPARE applies to the next EXECUTE. nprobe and
-- max_nprobe apply to IVF indexes only.
drop database if exists vector_filtered_search_knobs;
create database vector_filtered_search_knobs;
use vector_filtered_search_knobs;

set experimental_hnsw_index = 1;
set experimental_ivf_index = 1;

create table t_hnsw(id bigint primary key, category int not null, v vecf32(2));
insert into t_hnsw select result, if(result <= 500, 0, 1), cast(concat('[', result, ',0]') as vecf32(2)) from generate_series(1, 1000, 1) g;
create index i_hnsw using hnsw on t_hnsw(v) op_type 'vector_l2_ops';

create table t_ivf(id bigint primary key, category int not null, v vecf32(2));
insert into t_ivf select id, category, v from t_hnsw;
create index i_ivf using ivfflat on t_ivf(v) lists = 4 op_type 'vector_l2_ops';

-- hnsw: the default budget returns no row; itopk 1000 returns the qualifying nearest rows
select count(*) from (select id from t_hnsw where category = 1 order by l2_distance(v, '[0,0]') limit 10) x;
select group_concat(id order by id) from (select id from t_hnsw where category = 1 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'itopk=1000') x;
select group_concat(id order by id) from (select id from t_hnsw where category = 1 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=post', 'itopk=1000') x;
-- @separator:table
-- @regex("Internal Top-K: 1000, Max Internal Top-K: 5000", true)
explain select id from t_hnsw where category = 1 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'itopk=1000', 'max_itopk=5000';
-- max_itopk lowers itopk
select count(*) from (select id from t_hnsw where category = 1 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'max_itopk=20') x;

-- session variable after PREPARE
prepare s from 'select group_concat(id order by id) from (select id from t_hnsw where category = 1 order by l2_distance(v, ''[0,0]'') limit 10) x';
execute s;
set vector_itopk = 1000;
execute s;
-- the BY RANK option wins over the session variable
select count(*) from (select id from t_hnsw where category = 1 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'itopk=20') x;
set vector_itopk = 0;
execute s;
deallocate prepare s;

-- ivfflat: nprobe probes every list, itopk covers the index
-- @separator:table
-- @regex("NProbe: 4", true)
explain select id from t_ivf where category = 1 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'nprobe=4', 'itopk=1000';
select group_concat(id order by id) from (select id from t_ivf where category = 1 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'nprobe=4', 'itopk=1000') x;
-- @separator:table
-- @regex("Max NProbe: 8", true)
explain select id from t_ivf where category = 1 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'nprobe=2', 'max_nprobe=8';

-- nprobe and max_nprobe are IVF knobs
select id from t_hnsw where category = 1 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'nprobe=4';
select id from t_hnsw where category = 1 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'max_nprobe=4';

-- invalid values
select id from t_hnsw order by l2_distance(v, '[0,0]') limit 10 by rank with option 'itopk=0';
select id from t_hnsw order by l2_distance(v, '[0,0]') limit 10 by rank with option 'itopk=abc';
select id from t_hnsw order by l2_distance(v, '[0,0]') limit 10 by rank with option 'itopk=100', 'max_itopk=50';
select id from t_ivf order by l2_distance(v, '[0,0]') limit 10 by rank with option 'nprobe=8', 'max_nprobe=4';

drop database vector_filtered_search_knobs;
