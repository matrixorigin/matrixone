--- @metacmp(false)
drop database if exists issue_28301;
create database issue_28301;
use issue_28301;
create table t(id int primary key, v int);
insert into t values (1, 10), (2, 20);

-- The SQL result is one row containing a JSON document with both contracts.
-- @regex("(?s)query_block.*select_id.*matrixone.*schema_version.*table_name.*issue_28301\\.t",true)
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
cnt
2
select v from t where id = 1;
v
10

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
