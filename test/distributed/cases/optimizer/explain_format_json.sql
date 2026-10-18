--- @metacmp(false)
drop database if exists issue_28301;
create database issue_28301;
use issue_28301;
create table t(id int primary key, v int);
insert into t values (1, 10), (2, 20);

-- The SQL result is one row containing a JSON document with both contracts.
-- @regex("(?s)query_block.*select_id.*matrixone.*schema_version.*table_name.*issue_28301[.]t",true)
explain format=json select * from t where id = 1;

-- Parenthesized and quoted forms share the same normalized option path.
-- @regex("(?s)query_block.*matrixone.*schema_version",true)
explain (format 'json') select * from t;

-- A join remains a typed MatrixOne graph node; no synthetic nested_loop is required.
-- @regex("(?s)matrixone.*operator.*Join.*join_type",true)
explain format = 'json' select a.id from t a join t b on a.id = b.id;

-- CTE and window plans are represented by their reachable MatrixOne nodes.
-- @regex("(?s)matrixone.*schema_version.*nodes",true)
explain format=json with c as (select id from t) select * from c;
-- @regex("(?s)matrixone.*operator.*Window",true)
explain format=json select id, row_number() over (order by id) as rn from t;

-- Recursive UNION mode is explicit for both values of the distinct flag.
-- @regex("(?s)recursive_union_mode=DISTINCT",true)
explain format=json with recursive r(n) as (select 1 union select n+1 from r where n<10) select * from r;
-- @regex("(?s)recursive_union_mode=ALL",true)
explain format=json with recursive r(n) as (select 1 union all select n+1 from r where n<10) select * from r;
prepare e28301_recursive from 'explain format=json with recursive r(n) as (select 1 union all select n+1 from r where n<10) select * from r';
-- @regex("(?s)recursive_union_mode=ALL",true)
execute e28301_recursive;
deallocate prepare e28301_recursive;

-- Aggregate layout must preserve DISTINCT and the non-DISTINCT control.
-- @regex("(?s)operator.*Agg.*aggregate.*DISTINCT.*sum",true)
explain format=json select id, sum(distinct v), sum(id) from t group by id;

-- Grouped SAMPLE keeps both its group key and computed sampled expression.
-- @regex("(?s)operator.*Sample.*group_by.*id.*aggregate.*[+]",true)
explain format=json select id, sample(v + 1, 1 rows) from t group by id;

-- Typed table-function identity must remain visible in the MatrixOne graph.
-- @regex("(?s)matrixone.*operator.*Function Scan.*table_name.*generate_series",true)
explain format=json select * from generate_series(1, 5) g;

-- Production optimizer vector scans retain query identity, probe count and bounds.
create table vectors(id bigint primary key, v vecf32(3));
insert into vectors values (1,'[0,0,0]'),(2,'[1,1,1]'),(3,'[2,2,2]');
create index ix using ivfflat on vectors(v) lists=1 op_type 'vector_l2_ops';
set probe_limit=1;
-- @regex("(?s)Vector Index Scan.*query_vector=0x0000803F0000803F0000803F",true)
explain format=json select id from vectors order by l2_distance(v,'[1,1,1]') limit 2;
-- @regex("(?s)Vector Index Scan.*query_vector=0x000000400000004000000040",true)
explain format=json select id from vectors order by l2_distance(v,'[2,2,2]') limit 2;
set probe_limit=16;
-- @regex("(?s)Vector Index Scan.*NProbe: 16",true)
explain format=json select id from vectors order by l2_distance(v,'[1,1,1]') limit 2;

-- Search concurrency is a distinct static option, including when NProbe is unchanged.
set ivf_threads_search=1;
-- @regex("(?s)Vector Index Scan.*ThreadsSearch: 1[^0-9]",true)
explain format=json select id from vectors order by l2_distance(v,'[1,1,1]') limit 2;
set ivf_threads_search=8;
-- @regex("(?s)Vector Index Scan.*ThreadsSearch: 8[^0-9]",true)
explain format=json select id from vectors order by l2_distance(v,'[1,1,1]') limit 2;
-- @regex("(?s)Vector Index: ix.*NProbe: 16.*BucketExpandStep: 0.*ThreadsSearch: 8",true)
explain verbose select id from vectors order by l2_distance(v,'[1,1,1]') limit 2;
set ivf_threads_search=default;

-- @regex("(?s)Vector Index Scan.*distance_upper_bound_type=EXCLUSIVE.*distance_upper_bound=5[^0-9]",true)
explain format=json select id from vectors where l2_distance(v,'[1,1,1]') < 5 order by l2_distance(v,'[1,1,1]') limit 2;
-- @regex("(?s)Vector Index Scan.*distance_upper_bound_type=EXCLUSIVE.*distance_upper_bound=50[^0-9]",true)
explain format=json select id from vectors where l2_distance(v,'[1,1,1]') < 50 order by l2_distance(v,'[1,1,1]') limit 2;

-- Planner-generated distance ranges must preserve inclusive and lower-bound semantics.
-- @regex("(?s)Vector Index Scan.*distance_upper_bound_type=INCLUSIVE.*distance_upper_bound=5[^0-9]",true)
explain format=json select id from vectors where l2_distance(v,'[1,1,1]') <= 5 order by l2_distance(v,'[1,1,1]') limit 2;
-- @regex("(?s)Vector Index Scan.*distance_lower_bound_type=INCLUSIVE.*distance_lower_bound=1[^0-9]",true)
explain format=json select id from vectors where l2_distance(v,'[1,1,1]') >= 1 order by l2_distance(v,'[1,1,1]') limit 2;
-- @regex("(?s)Vector Index Scan.*distance_lower_bound_type=EXCLUSIVE.*distance_lower_bound=1[^0-9]",true)
explain format=json select id from vectors where l2_distance(v,'[1,1,1]') > 1 order by l2_distance(v,'[1,1,1]') limit 2;

-- INCLUDE-mode plans are built by the production optimizer and must expose the
-- first-round budget derived from the semantic LIMIT, not only the final k.
create table vectors_include(id bigint primary key, v vecf32(3), tag varchar(10));
insert into vectors_include values (1,'[0,0,0]','zero'),(2,'[1,1,1]','one'),(3,'[2,2,2]','two');
create index ix_include using ivfflat on vectors_include(v) lists=1 op_type 'vector_l2_ops' include(tag);
-- @regex("(?s)Vector Index Scan.*BucketExpandStep: 16.*first_round_limit=2",true)
explain format=json select id, tag from vectors_include order by l2_distance(v,'[1,1,1]') limit 2 by rank with option 'mode=include';
-- @regex("(?s)Vector Index Scan.*BucketExpandStep: 16.*first_round_limit=3",true)
explain format=json select id, tag from vectors_include order by l2_distance(v,'[1,1,1]') limit 3 by rank with option 'mode=include';
drop table vectors_include;
set probe_limit=default;

-- HNSW also carries search concurrency in the shared algorithm options.
set experimental_hnsw_index=1;
create table json_hnsw(id bigint primary key, v vecf32(3) not null);
insert into json_hnsw values (1,'[0,0,0]'),(2,'[1,1,1]'),(3,'[2,2,2]');
create index json_hx using hnsw on json_hnsw(v) op_type 'vector_l2_ops' M 4 EF_CONSTRUCTION 16 EF_SEARCH 16;
set hnsw_threads_search=1;
-- @regex("(?s)Vector Index Scan.*index=json_hx.*ThreadsSearch: 1[^0-9]",true)
explain format=json select id from json_hnsw order by l2_distance(v,'[1,1,1]') limit 1;
set hnsw_threads_search=8;
-- @regex("(?s)Vector Index Scan.*index=json_hx.*ThreadsSearch: 8[^0-9]",true)
explain format=json select id from json_hnsw order by l2_distance(v,'[1,1,1]') limit 1;
-- @regex("(?s)Vector Index: json_hx.*ThreadsSearch: 8",true)
explain verbose select id from json_hnsw order by l2_distance(v,'[1,1,1]') limit 1;
select id from json_hnsw order by l2_distance(v,'[1,1,1]') limit 1;
set hnsw_threads_search=default;
set experimental_hnsw_index=default;
drop table json_hnsw;

-- Shared fulltext scans must expose the semantics carried in algorithm options.
set experimental_fulltext2_index=1;
create table json_docs(id bigint primary key, body text, status int);
insert into json_docs values (1,'ready token',1),(2,'ready steady',0),(3,'other word',1);
create fulltext2 index json_ft on json_docs(body) include(status);
-- Explicit VERBOSE text shares the detailed metadata, while plain text remains compact.
-- @regex("(?s)Fulltext Index: json_ft.*Match Mode: 0.*Include Column.*status.*Include Predicates:",true)
explain verbose select id,status from json_docs where match(body) against('ready') and status=1 order by id;
-- Covered predicates are removed from the base scan: both the column mapping and value must remain visible.
-- @regex("(?s)Fulltext Index Scan.*Include Column.*status.*Include Predicates:.*val[^0-9]*1",true)
explain format=json select id,status from json_docs where match(body) against('ready') and status=1 order by id;
select id,status from json_docs where match(body) against('ready') and status=1 order by id;
-- @regex("(?s)Fulltext Index Scan.*Include Predicates:.*val[^0-9]*0",true)
explain format=json select id,status from json_docs where match(body) against('ready') and status=0 order by id;
select id,status from json_docs where match(body) against('ready') and status=0 order by id;
-- Equal payloads with distinct search modes must produce distinct diagnostics.
-- @regex("(?s)Fulltext Index Scan.*Match Mode: 1.*query_payload=.*ready",true)
explain format=json select id from json_docs where match(body) against('ready -other' in natural language mode) order by id;
select id from json_docs where match(body) against('ready -other' in natural language mode) order by id;
-- @regex("(?s)Fulltext Index Scan.*Match Mode: 3.*query_payload=.*ready",true)
explain format=json select id from json_docs where match(body) against('ready -other' in boolean mode) order by id;
select id from json_docs where match(body) against('ready -other' in boolean mode) order by id;
-- @regex("(?s)Fulltext Index Scan.*Match Mode: 5.*Score Range:.*has_min",true)
explain format=json select id from json_docs where match(body) against('ready' in bm25 mode)>cast(0 as float) order by id;
prepare json_ft_prepared from 'explain format=json select id from json_docs where match(body) against(? in bm25 mode)>? order by id';
set @json_term='ready', @json_threshold=0;
-- @regex("(?s)Fulltext Index Scan.*Match Mode: 5.*zero_relevance_guard",true)
execute json_ft_prepared using @json_term,@json_threshold;
set @json_threshold=100;
-- @regex("(?s)Fulltext Index Scan.*Match Mode: 5.*zero_relevance_guard",true)
execute json_ft_prepared using @json_term,@json_threshold;
deallocate prepare json_ft_prepared;
drop table json_docs;
set experimental_fulltext2_index=default;
-- Classic fulltext uses the same shared plan contract and must retain its mode too.
create table json_classic_docs(id bigint primary key, body text);
insert into json_classic_docs values (1,'ready token'),(2,'other word');
create fulltext index json_classic_ft on json_classic_docs(body);
-- @regex("(?s)Fulltext Index Scan.*Match Mode: 1.*query_payload=.*ready",true)
explain format=json select id from json_classic_docs where match(body) against('ready' in natural language mode);
-- @regex("(?s)Fulltext Index Scan.*Match Mode: 3.*query_payload=.*ready",true)
explain format=json select id from json_classic_docs where match(body) against('ready' in boolean mode);
drop table json_classic_docs;

-- FILL preserves mode and the targets remaining after projection pruning.
create table fill_values(ts timestamp, v int);
insert into fill_values values ('2024-01-01 00:00:00',1),('2024-01-01 00:00:10',3);
-- @regex("(?s)fill_type=PREV.*fill_target.*min",true)
explain format=json select c from (select _wstart as a, max(v) as b, min(v) as c from fill_values interval(ts, 5, second) fill(prev)) x;
-- @regex("(?s)fill_type=NEXT.*fill_target.*min",true)
explain format=json select c from (select _wstart as a, max(v) as b, min(v) as c from fill_values interval(ts, 5, second) fill(next)) x;
-- @regex("(?s)fill_type=LINEAR.*fill_target.*min.*fill_value",true)
explain format=json select c from (select _wstart as a, max(v) as b, min(v) as c from fill_values interval(ts, 5, second) fill(linear)) x;
-- @regex("(?s)fill_type=VALUE.*fill_target.*min.*fill_value.*7",true)
explain format=json select min(v) from fill_values interval(ts, 5, second) fill(value, 7);
-- @regex("(?s)fill_type=VALUE.*fill_target.*min.*fill_value.*8",true)
explain format=json select min(v) from fill_values interval(ts, 5, second) fill(value, 8);
drop table fill_values;

-- Serialized IN predicates retain bytes even when runtime provenance keeps scalar lists.
create table serial_values(v varchar(64));
insert into serial_values values (serial(cast(99999 as decimal(38,0))));
-- @regex("(?s)serialized.*0x458000000000000000000000000001869F",true)
explain format=json select v from serial_values where v in (serial(cast(99999 as decimal(38,0))),serial(cast(100000 as decimal(38,0))));
-- @regex("(?s)serialized.*0x458000000000000000000000000001869E",true)
explain format=json select v from serial_values where v in (serial(cast(99998 as decimal(38,0))),serial(cast(100000 as decimal(38,0))));
prepare e28301_serial from 'explain format=json select v from serial_values where v in (serial(cast(99999 as decimal(38,0))),serial(cast(100000 as decimal(38,0))))';
-- @regex("(?s)serialized.*0x458000000000000000000000000001869F",true)
execute e28301_serial;
deallocate prepare e28301_serial;
drop table serial_values;

-- ANALYZE FALSE is normalized to ordinary JSON EXPLAIN and must not start a runner.
-- @regex("(?s)query_block.*matrixone.*schema_version",true)
explain (analyze false, format json) select * from t;

-- Explain-only DML must not run the write pipeline.
-- #30130: assignments resolve the row image through the lock projection.
-- @regex("(?s)statement_type.*UPDATE.*assignments.*target[^}]*issue_28301[.]t[.]id[^}]*value[^}]*t[.]id.*target[^}]*issue_28301[.]t[.]v[^}]*value[^}]*99",true)
explain format=json update t set v = 99 where id = 1;
-- @regex("(?s)assignments.*target[^}]*issue_28301[.]t[.]v[^}]*value[^}]*t[.]v[^}]*[+] 1",true)
explain format=json update t set v = v + 1 where id = 1;
-- @regex("(?s)assignments.*target[^}]*issue_28301[.]t[.]v[^}]*value[^}]*null",true)
explain format=json update t set v = null where id = 1;
prepare e30130_update from 'explain format=json update t set v = 7 where id = 1';
-- @regex("(?s)assignments.*target[^}]*issue_28301[.]t[.]v[^}]*value[^}]*7",true)
execute e30130_update;
deallocate prepare e30130_update;
-- Multi-target updates resolve materialized sources and dedup/window outputs.
create table update_peer(id int primary key, v int);
insert into update_peer values (1, 30);
-- @regex("(?s)assignments.*target[^}]*issue_28301[.]t[.]v[^}]*value[^}]*99.*target[^}]*issue_28301[.]update_peer[.]v[^}]*value[^}]*88",true)
explain format=json update t a join update_peer b on a.id = b.id set a.v = 99, b.v = 88;
-- @regex("(?s)assignments.*target[^}]*issue_28301[.]t[.]v[^}]*value(?=[^}]*101)(?=[^}]*102)[^}]*UNION_ALL",true)
explain format=json update t a join t b on a.id + 1 = b.id set a.v = 101, b.v = 102;
-- EXPLAIN has not written either target; execution is an independent control.
select id, v from t order by id;
select v from update_peer;
update t a join update_peer b on a.id = b.id set a.v = 99, b.v = 88;
select v from t where id = 1;
select v from update_peer;
update t set v = 10 where id = 1;
update t a join t b on a.id + 1 = b.id set a.v = 101, b.v = 102;
select id, v from t order by id;
update t set v = id * 10;
-- MARK outputs in subquery assignments retain TRUE/UNKNOWN/FALSE semantics.
update update_peer set v = 1 where id = 1;
insert into update_peer values (3, null);
-- @regex("(?s)assignments.*target[^}]*issue_28301[.]t[.]v[^}]*value[^}]*MARK[^}]*[ap][.]",true)
explain format=json update t a set v = case when exists (select 1 from update_peer p where p.v = a.id) then 1 else 0 end;
-- @regex("(?s)assignments.*target[^}]*issue_28301[.]t[.]v[^}]*value[^}]*MARK",true)
explain format=json update t a set v = a.id in (select v from update_peer);
-- @regex("(?s)assignments.*target[^}]*issue_28301[.]t[.]v[^}]*value[^}]*not[^}]*MARK",true)
explain format=json update t a set v = case when not exists (select 1 from update_peer p where p.v = a.id) then 1 else 0 end;
-- @regex("(?s)assignments.*target[^}]*issue_28301[.]t[.]v[^}]*value[^}]*not[^}]*MARK",true)
explain format=json update t a set v = a.id not in (select v from update_peer);
select id, v from t order by id;
update t a set v = case when exists (select 1 from update_peer p where p.v = a.id) then 1 else 0 end;
select id, v from t order by id;
update t a set v = a.id in (select v from update_peer);
select id, v from t order by id;
update t a set v = a.id not in (select v from update_peer);
select id, v from t order by id;
update t a set v = case when not exists (select 1 from update_peer p where p.v = a.id) then 1 else 0 end;
select id, v from t order by id;
delete from update_peer;
update t a set v = a.id in (select v from update_peer);
select id, v from t order by id;
update t a set v = a.id not in (select v from update_peer);
select id, v from t order by id;
update t set v = id * 10;
drop table update_peer;
-- @regex("(?s)matrixone.*statement_type.*DELETE",true)
explain format=json delete from t where id = 1;
-- @regex("(?s)matrixone.*statement_type.*INSERT",true)
explain format=json insert into t values (3, 30);
-- REPLACE is represented by the INSERT statement type in the typed plan.
-- @regex("(?s)matrixone.*statement_type.*INSERT",true)
explain format=json replace into t values (3, 30);
select count(*) as cnt from t;
select v from t where id = 1;

-- SQL prepared execution uses the same one-column JSON result contract.
prepare e28301 from 'explain format=json select * from t';
-- @regex("(?s)query_block.*matrixone.*schema_version",true)
execute e28301;
deallocate prepare e28301;

-- Execution forms that would require a data or physical-plan runner reject early.
-- @regex("EXPLAIN ANALYZE FORMAT=JSON is not supported",true)
explain analyze format=json select * from t;
-- @regex("EXPLAIN PHYPLAN FORMAT=JSON is not supported",true)
explain phyplan format=json select * from t;
-- @regex("EXPLAIN FORMAT=JSON does not support CHECK",true)
explain (format json, check '["Table Scan"]') select * from t;

drop database issue_28301;
