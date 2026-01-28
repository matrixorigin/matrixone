-- Test table_stats table function

-- Non-existent table should return error
-- @regex("not found",true)
select * from table_stats('table_func_table_stats.no_exist_table') g;

-- Basic functionality test
use table_func_table_stats;

drop table if exists t1;
create table t1(a int, b varchar(100), c float);
insert into t1 values(1, 'hello', 1.1);
insert into t1 values(2, 'world', 2.2);
insert into t1 select * from t1;
insert into t1 select * from t1;
insert into t1 select * from t1;
insert into t1 select * from t1;
insert into t1 select * from t1;
insert into t1 select * from t1;
insert into t1 select * from t1;
insert into t1 select * from t1;
insert into t1 select * from t1;
insert into t1 select * from t1;
select count(*) from t1;

-- Flush to ensure stats are available
select mo_ctl('dn', 'flush', 'table_func_table_stats.t1');

select sleep(1);

-- Query table stats - basic columns (new column structure)
select * from table_stats('table_func_table_stats.t1') g;

-- Check sampling_ratio is present
select table_name, sampling_ratio >= 0 from table_stats('table_func_table_stats.t1') g;

-- Check ndv_map is valid JSON (verify it's not null)
select table_name, ndv_map is not null from table_stats('table_func_table_stats.t1') g;


-- Stats should reflect more rows (use refresh command with full mode)
select table_name, table_cnt, block_number from table_stats('table_func_table_stats.t1', 'refresh', 'full') g;

-- Test with empty table
drop table if exists t3;
create table t3(x int, y varchar(50));

-- Empty table should return stats with 0 rows
select table_name, table_cnt, block_number, accurate_object_number from table_stats('table_func_table_stats.t3') g;

-- Test get command (default)
select table_name, table_cnt from table_stats('table_func_table_stats.t1') g;
select table_name, table_cnt from table_stats('table_func_table_stats.t1', 'get') g;

-- Test refresh command - auto (default)
select table_name, table_cnt from table_stats('table_func_table_stats.t1', 'refresh') g;
select table_name, table_cnt from table_stats('table_func_table_stats.t1', 'refresh', 'auto') g;

-- Test refresh command - full (force refresh)
select table_name, table_cnt from table_stats('table_func_table_stats.t1', 'refresh', 'full') g;

-- Invalid refresh mode should return error
-- @regex("invalid refresh mode",true)
select * from table_stats('table_func_table_stats.t1', 'refresh', 'invalid') g;

-- Invalid command should return error
-- @regex("unknown command",true)
select * from table_stats('table_func_table_stats.t1', 'invalid_cmd') g;

-- Test patch command
select table_name, table_cnt, ndv_map from table_stats('table_func_table_stats.t1', 'patch', '{"table_cnt": 99999, "ndv_map": {"a": 1000}}') g;

-- Verify patch was applied
select table_name, table_cnt from table_stats('table_func_table_stats.t1') g;

-- Test patch with multiple fields
select table_name, table_cnt, block_number from table_stats('table_func_table_stats.t1', 'patch', '{"table_cnt": 50000, "block_number": 100}') g;

-- Patch command without args should return error
-- @regex("patch command requires args",true)
select * from table_stats('table_func_table_stats.t1', 'patch') g;

-- Patch command with invalid JSON should return error
-- @regex("invalid patch args",true)
select * from table_stats('table_func_table_stats.t1', 'patch', 'invalid json') g;

-- Test column projection (select specific columns)
select table_name, table_cnt, sampling_ratio from table_stats('table_func_table_stats.t1') g;
select table_name, ndv_map, min_val_map from table_stats('table_func_table_stats.t1') g;

-- Test with system tables (should work)
select table_name, table_cnt >= 0 from table_stats('mo_catalog.mo_tables') g;

-- Cleanup
drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
