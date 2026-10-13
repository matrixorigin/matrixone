-- IVFFLAT filtered search modes (#28988). mode=pre and mode=iterative search only the
-- rows that pass the filter: inside the index search when INCLUDE columns or the primary
-- key cover it (iterative), else through the keys of the filtered rows (a membership
-- join). mode=pre searches the nprobe nearest lists once. mode=iterative probes the
-- nearest lists in rounds until it holds the candidate budget, all lists are probed, or
-- max_nprobe is reached; a membership set of 100 keys or fewer is scanned in every list.
-- mode=include is mode=iterative. Each query is compared with mode=force.
drop database if exists vector_filtered_search_ivfflat;
create database vector_filtered_search_ivfflat;
use vector_filtered_search_ivfflat;

set experimental_ivf_index = 1;

-- ids 1..500 have category 0, 501..1000 category 1; tag is NULL on every 3rd row
create table t(id bigint primary key, category int not null, tag int, v vecf32(2));
insert into t select result, if(result <= 500, 0, 1), if(result % 3 = 0, null, result % 10), cast(concat('[', result, ',0]') as vecf32(2)) from generate_series(1, 1000, 1) g;
create index i using ivfflat on t(v) lists = 10 op_type 'vector_l2_ops';

-- the issue fixture: the nearest lists hold no qualifying row; the search widens
-- @separator:table
-- @regex("Join Type: SEMI", true)
-- @regex("round_count=[2-9]", true)
explain analyze select id from t where category = 1 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=iterative';
select count(*) from (select id from t where category = 1 order by l2_distance(v, '[0,0]') limit 10) x;
select group_concat(id order by id) from (select id from t where category = 1 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=iterative') x;
select group_concat(id order by id) from (select id from t where category = 1 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=force') x;

-- max_nprobe below what the filter needs returns a short page; raised, it fills
select count(*) from (select id from t where category = 1 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=iterative', 'max_nprobe=2') x;
select group_concat(id order by id) from (select id from t where category = 1 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=iterative', 'nprobe=1', 'max_nprobe=10') x;

-- qualifying row counts: none, fewer than k, exactly k, more than k
select count(*) from (select id from t where category = 9 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=iterative') x;
select group_concat(id order by id) from (select id from t where id in (3, 700, 999) order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=iterative') x;
select group_concat(id order by id) from (select id from t where id between 991 and 1000 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=iterative') x;
select group_concat(id order by id) from (select id from t where id % 7 = 0 order by l2_distance(v, '[500,0]') limit 6 by rank with option 'mode=iterative') x;
select group_concat(id order by id) from (select id from t where id % 7 = 0 order by l2_distance(v, '[500,0]') limit 6 by rank with option 'mode=force') x;

-- a filter on the primary key alone runs in the index search: iterative widens, post searches once
-- @separator:table
-- @regex("Index Filter", true)
-- @regex("round_count=[2-9]", true)
explain analyze select id from t where id between 991 and 1000 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=iterative';
select count(*) from (select id from t where id between 991 and 1000 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=post') x;

-- combined and NULL predicates
select group_concat(id order by id) from (select id from t where category = 1 and (tag = 4 or tag is null) order by l2_distance(v, '[0,0]') limit 5 by rank with option 'mode=iterative') x;
select group_concat(id order by id) from (select id from t where category = 1 and (tag = 4 or tag is null) order by l2_distance(v, '[0,0]') limit 5 by rank with option 'mode=force') x;

-- LIMIT with OFFSET
select group_concat(id order by id) from (select id from t where category = 1 order by l2_distance(v, '[0,0]') limit 5 offset 5 by rank with option 'mode=iterative') x;

-- prepared LIMIT
prepare s from 'select group_concat(id order by id) from (select id from t where category = 1 order by l2_distance(v, ''[0,0]'') limit ? by rank with option ''mode=iterative'') x';
set @k = 3;
execute s using @k;
set @k = 7;
execute s using @k;
deallocate prepare s;

-- mode=pre searches the nprobe nearest lists once: the nearest lists hold no qualifying row
-- @separator:table
-- @regex("round_count=1 ", true)
explain analyze select id from t where category = 1 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=pre';
select count(*) from (select id from t where category = 1 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=pre') x;
select group_concat(id order by id) from (select id from t where id in (3, 700, 999) order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=pre') x;

-- mode=include is mode=iterative
select group_concat(id order by id) from (select id from t where category = 1 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=include') x;
select group_concat(id order by id) from (select id from t where id between 991 and 1000 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=include') x;

-- a zero query vector ties every centroid under cosine: the rounds scan each list once
create table t_cos(id bigint primary key, v vecf32(4));
insert into t_cos select result, cast(concat('[', sin(result), ',', cos(result * 1.3), ',', sin(result * 0.7), ',', cos(result * 2.1), ']') as vecf32(4)) from generate_series(1, 3000, 1) g;
create index i_cos using ivfflat on t_cos(v) lists = 16 op_type 'vector_cosine_ops';
create table t_cos_ref(id bigint primary key, category int);
insert into t_cos_ref select result, result % 2 from generate_series(1, 3000, 1) g;
select count(*), count(distinct id) from (select id from t_cos where id < 300 order by cosine_distance(v, '[0,0,0,0]') limit 37 by rank with option 'mode=include') x;
select count(*), count(distinct id) from (select c.id from t_cos c where c.id in (select id from t_cos_ref where category = 1) order by cosine_distance(c.v, '[0,0,0,0]') limit 37 by rank with option 'mode=iterative') x;

-- INCLUDE columns: the default mode and iterative run the INCLUDE filter in the index
-- search; post and pre do not; post runs a primary-key filter in the index search
create table t_inc(id bigint primary key, category int not null, v vecf32(2));
insert into t_inc select id, category, v from t;
create index i_inc using ivfflat on t_inc(v) lists = 10 op_type 'vector_l2_ops' include(category);
-- @separator:table
-- @regex("Index Filter: [^\n]*category", true)
explain select id from t_inc where category = 1 order by l2_distance(v, '[0,0]') limit 10;
-- @separator:table
-- @regex("Index Filter: [^\n]*category", true)
-- @regex("Join Type: SEMI", false)
explain select id from t_inc where category = 1 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=iterative';
-- post with a filter on an INCLUDE column falls back to force when its page is short
-- @separator:table
-- @regex("Index Filter: [^\n]*category", false)
-- @regex("Adaptive Top", true)
explain select id from t_inc where category = 1 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=post';
select id from t_inc where category = 1 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=post';
-- @separator:table
-- @regex("Index Filter: [^\n]*category", false)
-- @regex("Join Type: SEMI", true)
explain select id from t_inc where category = 1 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=pre';
-- @separator:table
-- @regex("Index Filter: [^\n]*__mo_index_pri_col > 500", true)
-- @regex("Index Filter: [^\n]*category", false)
explain select id from t_inc where category = 1 and id > 500 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=post';
select group_concat(id order by id) from (select id from t_inc where category = 1 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=iterative') x;

drop database vector_filtered_search_ivfflat;
