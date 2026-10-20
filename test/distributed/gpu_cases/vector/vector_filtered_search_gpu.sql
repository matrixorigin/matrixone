-- GPU REQUIRED. Filtered search modes on cagra and ivfpq (#28988). A filter on INCLUDE
-- columns or the primary key runs inside the GPU search in every mode but force; in pre
-- and iterative any other filter goes through membership: the primary keys of the rows
-- that pass it are ANDed into the GPU filter mask (the PRE shape). The default mode and
-- post run other filters after the search. mode=pre searches once; mode=iterative widens
-- the search (cagra itopk, ivfpq nprobe) while LIMIT rows have not qualified. mode=post
-- keeps INCLUDE filters after the search; mode=auto tries post, then pre, then force.
drop database if exists vector_filtered_search_gpu;
create database vector_filtered_search_gpu;
use vector_filtered_search_gpu;

set experimental_cagra_index = 1;
set experimental_ivfpq_index = 1;

-- ids 1..500 have category 0, 501..1000 category 1; the 500 nearest rows fail category = 1.
-- grp equals category but is not an INCLUDE column.
create table t_cagra(id bigint primary key, category int not null, note int, grp int, v vecf32(8));
insert into t_cagra select result, if(result <= 500, 0, 1), result % 3, if(result <= 500, 0, 1), cast(concat('[', result, ',0,0,0,0,0,0,0]') as vecf32(8)) from generate_series(1, 1000, 1) g;
create index i_cagra using cagra on t_cagra(v) op_type 'vector_l2_ops' intermediate_graph_degree=16 graph_degree=8 include(category);

create table t_ivfpq(id bigint primary key, category int not null, note int, grp int, v vecf32(8));
insert into t_ivfpq select result, if(result <= 500, 0, 1), result % 3, if(result <= 500, 0, 1), cast(concat('[', result / 100, ',0,0,0,0,0,0,0]') as vecf32(8)) from generate_series(1, 1000, 1) g;
create index i_ivfpq using ivfpq on t_ivfpq(v) op_type 'vector_l2_ops' lists=8 m=8 bits_per_code=8 include(category);

-- EXPLAIN shows the filter the GPU search applies; post keeps the INCLUDE filter after the search
-- @separator:table
-- @regex("Index Filter: [^\n]*category = 1", true)
explain select id from t_cagra where category = 1 and id > 0 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=pre';
-- @separator:table
-- @regex("Index Filter: id > 0", true)
-- @regex("Index Filter: category", false)
explain select id from t_ivfpq where category = 1 and id > 0 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=post';
-- the default mode runs the INCLUDE filter in the search, post does not
-- @separator:table
-- @regex("Index Filter: [^\n]*category = 1", true)
explain select id from t_cagra where category = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10;
-- @separator:table
-- @regex("Index Filter", false)
explain select id from t_cagra where category = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=post';

-- cagra: the INCLUDE filter runs inside the graph search in pre and iterative
select count(*) from (select id from t_cagra where category = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=pre') x;
select count(*) from (select id from t_cagra where category = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=iterative') x;
select count(*) from (select id from t_cagra where category = 1 and note = 2 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=iterative') x;

-- ivfpq: the nearest list holds no qualifying row; pre searches it once, iterative widens
select count(*) from (select id from t_ivfpq where category = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=pre', 'nprobe=1') x;
select group_concat(id order by id) from (select id from t_ivfpq where category = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=iterative', 'nprobe=1') x;
select group_concat(id order by id) from (select id from t_ivfpq where category = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=force') x;
-- max_nprobe below what the filter needs returns a short page; every list fills it
select count(*) from (select id from t_ivfpq where category = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=iterative', 'nprobe=1', 'max_nprobe=1') x;
select count(*) from (select id from t_ivfpq where category = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=iterative', 'nprobe=1', 'max_nprobe=8') x;
-- a filter outside the INCLUDE columns goes through membership: iterative widens to a full
-- page of qualifying rows (PQ ranking breaks distance ties between builds)
select count(*) from (select id from t_ivfpq where category = 1 and note = 2 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=iterative', 'nprobe=1') x;
select count(*) from (select id from t_ivfpq where category = 1 and note = 2 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=iterative', 'nprobe=1') x where x.id not in (select id from t_ivfpq where category = 1 and note = 2);
-- prepared LIMIT
prepare s from 'select group_concat(id order by id) from (select id from t_ivfpq where category = 1 order by l2_distance(v, ''[0,0,0,0,0,0,0,0]'') limit ? by rank with option ''mode=iterative'', ''nprobe=1'') x';
set @k = 3;
execute s using @k;
deallocate prepare s;

-- membership: a filter only on a column outside INCLUDE runs in the GPU search through
-- the PRE shape, whose producer scan also applies the INCLUDE filter
-- @separator:table
-- @regex("Join Type: SEMI", true)
-- @regex("Filter Cond: \(t_cagra.grp = 1\)", true)
explain select id from t_cagra where grp = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=pre';
-- @separator:table
-- @regex("Join Type: SEMI", true)
-- @regex("Index Filter: [^\n]*category = 1", true)
-- @regex("Filter Cond: \(t_ivfpq.grp = 1\), \(t_ivfpq.category = 1\)", true)
explain select id from t_ivfpq where category = 1 and grp = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=pre', 'nprobe=1';
-- the default mode keeps it after the search
-- @separator:table
-- @regex("Join Type: SEMI", false)
explain select id from t_cagra where grp = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10;
-- a distance range predicate runs on the search output, never through membership
-- @separator:table
-- @regex("Join Type: SEMI", false)
explain select id from t_cagra where l2_distance(v, '[0,0,0,0,0,0,0,0]') < 5.5 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=pre';
-- @separator:table
-- @regex("Join Type: SEMI", false)
explain select id from t_ivfpq where l2_distance(v, '[0,0,0,0,0,0,0,0]') < 5.5 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=iterative', 'nprobe=1';
select id from t_cagra where l2_distance(v, '[0,0,0,0,0,0,0,0]') < 5.5 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=pre';
select count(*) from (select id from t_cagra where grp = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=pre') x;
select count(*) from (select id from t_cagra where grp = 1 and note = 2 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=iterative') x;
select count(*) from (select id from t_ivfpq where grp = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=pre', 'nprobe=1') x;
select count(*) from (select id from t_ivfpq where grp = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=iterative', 'nprobe=1') x;
select count(*) from (select id from t_ivfpq where grp = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=iterative', 'nprobe=1') x where x.id not in (select id from t_ivfpq where grp = 1);
-- a membership join: 10 keys far from the query
create table t_ref(id bigint primary key);
insert into t_ref select result from generate_series(991, 1000, 1) g;
select group_concat(id order by id) from (select id from t_cagra where id in (select id from t_ref) order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10) x;
select group_concat(id order by id) from (select id from t_ivfpq where id in (select id from t_ref) order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'nprobe=1') x;

-- the default mode with a filter the nearest lists do not hold returns no error
select count(*) from (select id from t_ivfpq where category = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'nprobe=1') x;

-- auto: ADAPTIVE_TOP over post, pre and force; the flush gives the planner the table
-- statistics that keep auto from choosing force for a small table
select mo_ctl('dn', 'flush', 'vector_filtered_search_gpu.t_ivfpq');
-- @separator:table
-- @regex("Adaptive Top", true)
explain select id from t_ivfpq where category = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=auto', 'nprobe=1';
select id from t_ivfpq where category = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=auto', 'nprobe=1';
select mo_ctl('dn', 'flush', 'vector_filtered_search_gpu.t_cagra');
-- @separator:table
-- @regex("Adaptive Top", true)
explain select id from t_cagra where category = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=auto';
-- @ignore:0
select id from t_cagra where category = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=auto';

-- mode=include is an alias of mode=iterative
select count(*) from (select id from t_ivfpq where category = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=include') x;

-- A sequence filter in pre and iterative is evaluated once per row: the query runs the
-- exact sort, which returns the one row that draws 1 and leaves the sequence at the row
-- count. Each query uses its own sequence; force is the control.
create sequence s_cagra_pre start with 1;
select count(*) from (select id from t_cagra where nextval('s_cagra_pre') = '1' order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=pre') x;
select currval('s_cagra_pre');
create sequence s_cagra_iterative start with 1;
select count(*) from (select id from t_cagra where nextval('s_cagra_iterative') = '1' order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=iterative') x;
select currval('s_cagra_iterative');
create sequence s_cagra_force start with 1;
select count(*) from (select id from t_cagra where nextval('s_cagra_force') = '1' order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=force') x;
select currval('s_cagra_force');
create sequence s_ivfpq_pre start with 1;
select count(*) from (select id from t_ivfpq where nextval('s_ivfpq_pre') = '1' order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=pre') x;
select currval('s_ivfpq_pre');
create sequence s_ivfpq_iterative start with 1;
select count(*) from (select id from t_ivfpq where nextval('s_ivfpq_iterative') = '1' order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=iterative') x;
select currval('s_ivfpq_iterative');
create sequence s_ivfpq_force start with 1;
select count(*) from (select id from t_ivfpq where nextval('s_ivfpq_force') = '1' order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 10 by rank with option 'mode=force') x;
select currval('s_ivfpq_force');

-- the largest itopk: ivfpq searches at most the index rows; cagra rejects it above its limit
select count(*) from (select id from t_ivfpq order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 3 by rank with option 'itopk=18446744073709551615') x;
select count(*) from (select id from t_ivfpq where category = 1 order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 3 by rank with option 'mode=pre', 'itopk=18446744073709551615') x;
select id from t_cagra order by l2_distance(v, '[0,0,0,0,0,0,0,0]') limit 3 by rank with option 'itopk=18446744073709551615';

drop database vector_filtered_search_gpu;
