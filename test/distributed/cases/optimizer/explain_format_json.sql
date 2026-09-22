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
-- @regex("(?s)Vector Index Scan.*initial_probe_count=16",true)
explain format=json select id from vectors order by l2_distance(v,'[1,1,1]') limit 2;
-- @regex("(?s)Vector Index Scan.*distance_upper_bound_type=EXCLUSIVE.*distance_upper_bound=5[^0-9]",true)
explain format=json select id from vectors where l2_distance(v,'[1,1,1]') < 5 order by l2_distance(v,'[1,1,1]') limit 2;
-- @regex("(?s)Vector Index Scan.*distance_upper_bound_type=EXCLUSIVE.*distance_upper_bound=50[^0-9]",true)
explain format=json select id from vectors where l2_distance(v,'[1,1,1]') < 50 order by l2_distance(v,'[1,1,1]') limit 2;
set probe_limit=default;

-- ANALYZE FALSE is normalized to ordinary JSON EXPLAIN and must not start a runner.
-- @regex("(?s)query_block.*matrixone.*schema_version",true)
explain (analyze false, format json) select * from t;

-- Explain-only DML must not run the write pipeline.
-- @regex("(?s)matrixone.*statement_type.*UPDATE",true)
explain format=json update t set v = 99 where id = 1;
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
