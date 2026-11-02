-- Filtered CAGRA and IVF-PQ search over a built index plus CDC overflow rows. The base
-- index holds ids 1..1000 ([id,0,...]); ids 1001..1005 (the origin) reach the overflow
-- through CDC and are the nearest rows to the origin. One filter mask covers the index
-- and the overflow: a membership filter (note is not an INCLUDE column), a primary-key
-- filter and a subquery, in pre and iterative.
--
-- GPU REQUIRED. Lives under pessimistic_transaction/ because it depends on the ISCP CDC
-- consumer appending the rows into the index's overflow.

drop database if exists vector_filtered_search_overflow_gpu;
create database vector_filtered_search_overflow_gpu;
use vector_filtered_search_overflow_gpu;

set experimental_cagra_index = 1;
set experimental_ivfpq_index = 1;

create table t_cagra(id bigint primary key, note int, v vecf32(8));
insert into t_cagra select result, result % 2, cast(concat('[', result, ',0,0,0,0,0,0,0]') as vecf32(8)) from generate_series(1, 1000, 1) g;
create index i_cagra using cagra on t_cagra(v) op_type 'vector_l2_ops' intermediate_graph_degree=16 graph_degree=8;
insert into t_cagra select result, result % 2, '[0,0,0,0,0,0,0,0]' from generate_series(1001, 1005, 1) g;

create table t_ivfpq(id bigint primary key, note int, v vecf32(8));
insert into t_ivfpq select result, result % 2, cast(concat('[', result, ',0,0,0,0,0,0,0]') as vecf32(8)) from generate_series(1, 1000, 1) g;
create index i_ivfpq using ivfpq on t_ivfpq(v) op_type 'vector_l2_ops' lists=8 m=8 bits_per_code=8;
insert into t_ivfpq select result, result % 2, '[0,0,0,0,0,0,0,0]' from generate_series(1001, 1005, 1) g;

-- cagra: overflow rows first, then the nearest base rows
-- @wait_expect(1, 180)
select group_concat(id order by id) from (select id from t_cagra where note = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 5 by rank with option 'mode=pre') x;
select group_concat(id order by id) from (select id from t_cagra where note = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 5 by rank with option 'mode=iterative') x;
select group_concat(id order by id) from (select id from t_cagra where id > 2 and id < 1004 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 5 by rank with option 'mode=iterative') x;
select group_concat(id order by id) from (select id from t_cagra where id in (select id from t_cagra where id between 3 and 6 or id = 1005) order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 3 by rank with option 'mode=iterative') x;

-- ivfpq: the same filters; nprobe=1 makes iterative widen past the first list. PQ codes
-- approximate the base distances, so each query reports the overflow ids it returns, the
-- rows and the rows that fail the filter.
-- @wait_expect(1, 180)
select group_concat(if(id > 1000, id, null) order by id), count(*), sum(note != 1) from (select id, note from t_ivfpq where note = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 5 by rank with option 'mode=pre') x;
select group_concat(if(id > 1000, id, null) order by id), count(*), sum(note != 1) from (select id, note from t_ivfpq where note = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 5 by rank with option 'mode=iterative', 'nprobe=1') x;
select group_concat(if(id > 1000, id, null) order by id), count(*), sum(id <= 2 or id >= 1004) from (select id from t_ivfpq where id > 2 and id < 1004 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 5 by rank with option 'mode=iterative', 'nprobe=1') x;
select group_concat(if(id > 1000, id, null) order by id), count(*), sum(id not in (3, 4, 5, 6, 1005)) from (select id from t_ivfpq where id in (select id from t_ivfpq where id between 3 and 6 or id = 1005) order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 3 by rank with option 'mode=iterative', 'nprobe=1') x;

drop database vector_filtered_search_overflow_gpu;
