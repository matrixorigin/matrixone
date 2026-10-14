-- HNSW pre-filtering and auto (#28988). mode=pre searches only the keys of the filtered rows
-- (a SEMI join with a membership runtime filter), so a filter that rejects every nearest
-- row still returns the qualifying nearest rows. Each query is compared with mode=force,
-- the exact path on the same table. The l2 table has five sub-indexes.
drop database if exists vector_filtered_search_hnsw;
create database vector_filtered_search_hnsw;
use vector_filtered_search_hnsw;

set experimental_hnsw_index = 1;

-- ids 1..500 have category 0, 501..1000 category 1; tag is NULL on every 3rd row
create table t_l2(id bigint primary key, category int not null, tag int, v vecf32(2));
insert into t_l2 select result, if(result <= 500, 0, 1), if(result % 3 = 0, null, result % 10), cast(concat('[', result, ',0]') as vecf32(2)) from generate_series(1, 1000, 1) g;
create index i_l2 using hnsw on t_l2(v) op_type 'vector_l2_ops' max_index_capacity 200;

create table t_cos(id bigint primary key, category int not null, v vecf32(2));
insert into t_cos select result, if(result <= 500, 0, 1), normalize_l2(cast(concat('[1,', result / 1000, ']') as vecf32(2))) from generate_series(1, 1000, 1) g;
create index i_cos using hnsw on t_cos(v) op_type 'vector_cosine_ops';

-- the issue fixture: every nearest row fails the filter
-- @separator:table
-- @regex("Join Type: SEMI", true)
-- @regex("Vector Index Scan on", true)
explain select id from t_l2 where category = 1 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=pre';
select count(*) from (select id from t_l2 where category = 1 order by l2_distance(v, '[0,0]') limit 10) x;
select group_concat(id order by id) from (select id from t_l2 where category = 1 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=pre') x;
select group_concat(id order by id) from (select id from t_cos where category = 1 order by cosine_distance(v, '[1,0]') limit 10 by rank with option 'mode=pre') x;
select group_concat(id order by id) from (select id from t_cos where category = 1 order by cosine_distance(v, '[1,0]') limit 10 by rank with option 'mode=force') x;

-- qualifying row counts: none, fewer than k, exactly k, more than k
select count(*) from (select id from t_l2 where category = 9 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=pre') x;
select group_concat(id order by id) from (select id from t_l2 where id in (3, 700, 999) order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=pre') x;
select group_concat(id order by id) from (select id from t_l2 where id between 991 and 1000 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=pre') x;
select group_concat(id order by id) from (select id from t_l2 where id % 7 = 0 order by l2_distance(v, '[500,0]') limit 6 by rank with option 'mode=pre') x;
select group_concat(id order by id) from (select id from t_l2 where id % 7 = 0 order by l2_distance(v, '[500,0]') limit 6 by rank with option 'mode=force') x;

-- combined and NULL predicates
select group_concat(id order by id) from (select id from t_l2 where category = 1 and (tag = 4 or tag is null) order by l2_distance(v, '[0,0]') limit 5 by rank with option 'mode=pre') x;
select group_concat(id order by id) from (select id from t_l2 where category = 1 and (tag = 4 or tag is null) order by l2_distance(v, '[0,0]') limit 5 by rank with option 'mode=force') x;

-- LIMIT with OFFSET
select group_concat(id order by id) from (select id from t_l2 where category = 1 order by l2_distance(v, '[0,0]') limit 5 offset 5 by rank with option 'mode=pre') x;

-- prepared LIMIT
prepare s from 'select group_concat(id order by id) from (select id from t_l2 where category = 1 order by l2_distance(v, ''[0,0]'') limit ? by rank with option ''mode=pre'') x';
set @k = 3;
execute s using @k;
set @k = 7;
execute s using @k;
deallocate prepare s;

-- mode=iterative widens the search in rounds; mode=include is deprecated: an error
select group_concat(id order by id) from (select id from t_l2 where category = 1 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=iterative') x;
select group_concat(id order by id) from (select id from t_l2 where category = 1 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=iterative', 'max_itopk=10') x;
select group_concat(id order by id) from (select id from t_cos where category = 1 order by cosine_distance(v, '[1,0]') limit 10 by rank with option 'mode=iterative') x;
select group_concat(id order by id) from (select id from t_l2 where id % 7 = 0 order by l2_distance(v, '[500,0]') limit 6 by rank with option 'mode=iterative') x;
select id from t_l2 where category = 1 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=include';

-- mode=auto: ADAPTIVE_TOP over post, pre and force; the flush gives the planner the
-- table statistics that keep auto from choosing force for a small table
select mo_ctl('dn', 'flush', 'vector_filtered_search_hnsw.t_l2');
-- @separator:table
-- @regex("Adaptive Top", true)
-- @regex("Join Type: SEMI", true)
explain select id from t_l2 where category = 1 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=auto';
-- post returns no row, pre returns the qualifying nearest rows
select id from t_l2 where category = 1 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=auto';
-- post fills the page
select id from t_l2 where category = 0 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=auto';
select id from t_l2 where id % 7 = 0 order by l2_distance(v, '[500,0]') limit 6 by rank with option 'mode=auto';
-- without a filter the region has post and force only
-- @separator:table
-- @regex("Adaptive Top", true)
-- @regex("Join Type: SEMI", false)
explain select id from t_l2 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=auto';
select id from t_l2 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=auto';
-- inside an outer query the region runs force
-- @separator:table
-- @regex("Adaptive Top", false)
-- @regex("Vector Index Scan on", false)
explain select group_concat(id order by id) from (select id from t_l2 where category = 1 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=auto') x;
-- more than one output column: the region runs force
-- @separator:table
-- @regex("Adaptive Top", false)
-- @regex("Vector Index Scan on", false)
explain select id, category from t_l2 where category = 1 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=auto';
select id, category from t_l2 where category = 1 order by l2_distance(v, '[0,0]') limit 3 by rank with option 'mode=auto';
-- prepared LIMIT
prepare s from 'select id from t_l2 where category = 1 order by l2_distance(v, ''[0,0]'') limit ? by rank with option ''mode=auto''';
set @k = 4;
execute s using @k;
deallocate prepare s;

drop database vector_filtered_search_hnsw;
