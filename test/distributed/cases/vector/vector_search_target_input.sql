-- Search targets of a vector Top-K query (issue #25964): the IVFFLAT and HNSW
-- index plans and the scan plan reject a malformed target with the same input
-- error, and a valid target after an error returns the Top-K.
drop database if exists vec_search_target;
create database vec_search_target;
use vec_search_target;
set experimental_ivf_index = 1;
set experimental_hnsw_index = 1;

create table t_ivf (id bigint primary key, v vecf32(3));
insert into t_ivf values (1, '[1,1,1]'), (2, '[2,2,2]'), (3, '[3,3,3]');
create index iv using ivfflat on t_ivf(v) lists = 1 op_type 'vector_l2_ops';
create table t_hnsw (id bigint primary key, v vecf32(3));
insert into t_hnsw select * from t_ivf;
create index hv using hnsw on t_hnsw(v) m = 8 ef_construction = 16 op_type 'vector_l2_ops';

-- @regex("Vector Index Scan on iv", true)
explain select id, l2_distance(v, '[1,1,1]') d from t_ivf order by d limit 1;
-- @regex("Vector Index Scan on hv", true)
explain select id, l2_distance(v, '[1,1,1]') d from t_hnsw order by d limit 1;
-- @regex("Vector Index Scan", false)
explain select id, l2_distance(v, '[1,1,1]') d from t_ivf order by d, id limit 1;

prepare p_ivf from 'select id, l2_distance(v, ?) d from t_ivf order by d limit 1';
prepare p_hnsw from 'select id, l2_distance(v, ?) d from t_hnsw order by d limit 1';
prepare p_scan from 'select id, l2_distance(v, ?) d from t_ivf order by d, id limit 1';

set @q = '[1,1,1]';
execute p_ivf using @q;
execute p_hnsw using @q;
execute p_scan using @q;

-- Malformed targets: one input error on every plan.
set @q = '';
execute p_ivf using @q;
execute p_hnsw using @q;
execute p_scan using @q;
set @q = 'abc';
execute p_ivf using @q;
execute p_hnsw using @q;
execute p_scan using @q;
set @q = '[1,1';
execute p_ivf using @q;
execute p_hnsw using @q;
execute p_scan using @q;
set @q = '[1,a,1]';
execute p_ivf using @q;
execute p_hnsw using @q;
execute p_scan using @q;
set @q = '[1,0]';
execute p_ivf using @q;
execute p_hnsw using @q;
execute p_scan using @q;

-- The same statements return the Top-K after an error.
set @q = '[3,3,3]';
execute p_ivf using @q;
execute p_hnsw using @q;
execute p_scan using @q;

-- A NULL target: the index plans return no row, the scan plan a NULL distance.
set @q = null;
execute p_ivf using @q;
execute p_hnsw using @q;
execute p_scan using @q;

deallocate prepare p_ivf;
deallocate prepare p_hnsw;
deallocate prepare p_scan;

-- Literal targets.
select id from t_ivf order by l2_distance(v, cast('abc' as varchar(10))) limit 1;
select id from t_hnsw order by l2_distance(v, cast('abc' as varchar(10))) limit 1;
select id from t_ivf order by l2_distance(v, cast('' as varchar(10))) limit 1;
select id from t_hnsw order by l2_distance(v, cast('' as varchar(10))) limit 1;

-- Other parse sites report the same input error.
select cast(cast('' as varchar(10)) as vecf32(3));
select cast('[1,2' as vecf64(3));
select cast('[1,a,3]' as vecbf16(3));
select cast('[1,2,nan]' as vecf32(3));
insert into t_ivf values (4, 'abc');
insert into t_ivf values (5, '[1,2');
select count(*) from t_ivf;

drop database vec_search_target;
