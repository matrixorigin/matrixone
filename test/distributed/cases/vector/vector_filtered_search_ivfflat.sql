-- IVFFLAT pre-filtering (#28988). mode=pre searches only the keys of the filtered rows.
-- With more than 100 keys the search probes the nearest lists in rounds until it holds
-- the candidate budget, all lists are probed, or max_nprobe is reached; with 100 keys or
-- fewer it scans the keys in every list. A filter evaluated in the index search widens
-- the same way under mode=pre. Each query is compared with mode=force.
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
explain analyze select id from t where category = 1 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=pre';
select count(*) from (select id from t where category = 1 order by l2_distance(v, '[0,0]') limit 10) x;
select group_concat(id order by id) from (select id from t where category = 1 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=pre') x;
select group_concat(id order by id) from (select id from t where category = 1 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=force') x;

-- max_nprobe below what the filter needs returns a short page; raised, it fills
select count(*) from (select id from t where category = 1 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=pre', 'max_nprobe=2') x;
select group_concat(id order by id) from (select id from t where category = 1 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=pre', 'nprobe=1', 'max_nprobe=10') x;

-- qualifying row counts: none, fewer than k, exactly k, more than k
select count(*) from (select id from t where category = 9 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=pre') x;
select group_concat(id order by id) from (select id from t where id in (3, 700, 999) order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=pre') x;
select group_concat(id order by id) from (select id from t where id between 991 and 1000 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=pre') x;
select group_concat(id order by id) from (select id from t where id % 7 = 0 order by l2_distance(v, '[500,0]') limit 6 by rank with option 'mode=pre') x;
select group_concat(id order by id) from (select id from t where id % 7 = 0 order by l2_distance(v, '[500,0]') limit 6 by rank with option 'mode=force') x;

-- a filter on the primary key alone runs in the index search: pre widens, post searches once
-- @separator:table
-- @regex("Index Filter", true)
-- @regex("round_count=[2-9]", true)
explain analyze select id from t where id between 991 and 1000 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=pre';
select count(*) from (select id from t where id between 991 and 1000 order by l2_distance(v, '[0,0]') limit 10 by rank with option 'mode=post') x;

-- combined and NULL predicates
select group_concat(id order by id) from (select id from t where category = 1 and (tag = 4 or tag is null) order by l2_distance(v, '[0,0]') limit 5 by rank with option 'mode=pre') x;
select group_concat(id order by id) from (select id from t where category = 1 and (tag = 4 or tag is null) order by l2_distance(v, '[0,0]') limit 5 by rank with option 'mode=force') x;

-- LIMIT with OFFSET
select group_concat(id order by id) from (select id from t where category = 1 order by l2_distance(v, '[0,0]') limit 5 offset 5 by rank with option 'mode=pre') x;

-- prepared LIMIT
prepare s from 'select group_concat(id order by id) from (select id from t where category = 1 order by l2_distance(v, ''[0,0]'') limit ? by rank with option ''mode=pre'') x';
set @k = 3;
execute s using @k;
set @k = 7;
execute s using @k;
deallocate prepare s;

drop database vector_filtered_search_ivfflat;
