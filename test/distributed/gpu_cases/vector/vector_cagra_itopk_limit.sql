-- GPU REQUIRED. cagra internal top-k limit 1024 (#28988). 3000 rows; 5 rows pass the
-- primary-key filter, so iterative never fills LIMIT 10 and widens to its cap 1024.
-- itopk or max_itopk above 1024 is rejected, from the SQL option and the session variable.
drop database if exists vector_cagra_itopk_limit;
create database vector_cagra_itopk_limit;
use vector_cagra_itopk_limit;

set experimental_cagra_index = 1;

create table t(id bigint primary key, v vecf32(8));
insert into t select result, cast(concat('[', result, ',0,0,0,0,0,0,0]') as vecf32(8)) from generate_series(1, 3000, 1) g;
create index i using cagra on t(v) op_type 'vector_l2_ops' intermediate_graph_degree=16 graph_degree=8;

select group_concat(id order by id) from (select id from t where id <= 5 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=iterative') x;
select group_concat(id order by id) from (select id from t where id <= 5 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=iterative', 'max_itopk=1024') x;
select group_concat(id order by id) from (select id from t where id <= 5 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'itopk=1024') x;

select id from t where id <= 5 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'itopk=1025';
select id from t where id <= 5 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=iterative', 'max_itopk=2000';

set vector_itopk = 2000;
select id from t where id <= 5 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10;
set vector_itopk = 0;
set vector_max_itopk = 2000;
select id from t where id <= 5 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=iterative';
set vector_max_itopk = 0;

drop database vector_cagra_itopk_limit;
