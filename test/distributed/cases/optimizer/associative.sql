-- Test associative law transformations
-- This test verifies that associative law rule 1 correctly migrates OnList conditions
-- to avoid remapping errors during query execution.
--
-- Rule 1: A*(B*C) -> (A*B)*C when C.selectivity >= 0.9 and B.outcnt < C.outcnt

-- Create connector_job table (B in the transformation)
create table connector_job (
    id bigint primary key,
    file_id bigint,
    task_id bigint,
    status tinyint
);

-- Create task table (C in the transformation)
create table task (
    id bigint primary key,
    uid varchar(255)
);

-- Create file table (A in the transformation)
create table file (
    id bigint primary key,
    job_id bigint,
    task_id bigint,
    table_id bigint
);

-- Set stats to trigger associative law rule 1:
-- Rule 1: A*(B*C) -> (A*B)*C when C.selectivity >= 0.9 and B.outcnt < C.outcnt
-- - connector_job (B): small table, 9 rows
-- - task (C): default large table, selectivity = 1.0 (no filter on task)
-- - file (A): default large table
select table_cnt from table_stats("associative.connector_job", 'patch', '{"table_cnt": 9, "accurate_object_number": 1}') g;

-- Test query that triggers associative law transformation
-- This query should not cause remapping error after the fix
explain SELECT DISTINCT t.id 
FROM associative.connector_job job, associative.task t, associative.file f 
WHERE job.task_id = t.id 
  AND f.job_id = job.id  
  AND f.task_id = t.id 
  AND job.status != 4 
  AND job.status != 5 
  AND t.uid = '019ac448-45b7-7545-9762-2a73f9ab129' 
  AND f.table_id in (1);


-- ===========================================================================
-- TPC-H Q17 Shuffle Behavior Test
-- ===========================================================================
--
-- This section investigates how statistics affect shuffle decisions for TPC-H Q17.
-- By artificially constructing stats, we can observe the optimizer's behavior.
--
-- Key finding: When NDV (Number of Distinct Values) for l_partkey is reduced
-- (from ~19.7M to ~1.8M in the second test), the optimizer no longer chooses
-- shuffle for the join operation. This behavior change ultimately leads to
-- issue #23562 (https://github.com/matrixorigin/matrixone/issues/23562).
--
-- The shuffle_range_map's "result" array is normally [1024]float64, but here
-- we use a small array for simplicity since this test doesn't involve actual
-- execution - it only examines the EXPLAIN output.
-- ===========================================================================

CREATE TABLE `lineitem` (
  `L_ORDERKEY` bigint NOT NULL,
  `L_PARTKEY` int NOT NULL,
  `L_SUPPKEY` int NOT NULL,
  `L_LINENUMBER` int NOT NULL,
  `L_QUANTITY` decimal(15,2) NOT NULL,
  `L_EXTENDEDPRICE` decimal(15,2) NOT NULL,
  `L_DISCOUNT` decimal(15,2) NOT NULL,
  `L_TAX` decimal(15,2) NOT NULL,
  `L_RETURNFLAG` varchar(1) NOT NULL,
  `L_LINESTATUS` varchar(1) NOT NULL,
  `L_SHIPDATE` date NOT NULL,
  `L_COMMITDATE` date NOT NULL,
  `L_RECEIPTDATE` date NOT NULL,
  `L_SHIPINSTRUCT` char(25) NOT NULL,
  `L_SHIPMODE` char(10) NOT NULL,
  `L_COMMENT` varchar(44) NOT NULL,
  PRIMARY KEY (`L_ORDERKEY`,`L_LINENUMBER`)
);

CREATE TABLE `part` (
  `P_PARTKEY` int NOT NULL,
  `P_NAME` varchar(55) NOT NULL,
  `P_MFGR` char(25) NOT NULL,
  `P_BRAND` char(10) NOT NULL,
  `P_TYPE` varchar(25) NOT NULL,
  `P_SIZE` int NOT NULL,
  `P_CONTAINER` char(10) NOT NULL,
  `P_RETAILPRICE` decimal(15,2) NOT NULL,
  `P_COMMENT` varchar(23) NOT NULL,
  PRIMARY KEY (`P_PARTKEY`)
);

-- First test: High NDV for l_partkey (~19.7M)
-- With high NDV, the optimizer should choose shuffle for the join.
-- Note: The result array in shuffle_range_map is normally [1024]float64,
-- but we use a small array here since execution is not involved.
select table_cnt from table_stats("associative.lineitem", 'patch', '
{
  "accurate_object_number": 1063,
  "approx_object_number": 1063,
  "block_number": 73249,
  "ndv_map": {
    "l_partkey": 19732896.19418804,
    "l_quantity": 1769.2467200237772
  },
  "shuffle_range_map": {
    "l_partkey": {
      "overlap": 0.9898975142727277,
      "uniform": 0.999996954035638,
      "result": [1.0, 2.0, 3.0, 4.0, 5.0, 8.0]
    }
  },
  "table_cnt": 600037902
}'
) g;

select table_cnt from table_stats("associative.part", 'patch', '
{
    "accurate_object_number": 31,
    "approx_object_number": 31,
    "block_number": 2442,
    "ndv_map": {
      "p_brand": 25,
      "p_container": 40,
      "p_partkey": 20000000
    },
    "table_cnt": 20000000
  }
') g;

-- EXPLAIN should show shuffle being used for the join (due to high NDV)
explain select
    sum(l_extendedprice) / 7.0 as avg_yearly
from
    lineitem,
    part
where
    p_partkey = l_partkey
    and p_brand = 'Brand#54'
    and p_container = 'LG BAG'
    and l_quantity < (
        select
            0.2 * avg(l_quantity)
        from
            lineitem
        where
            l_partkey = p_partkey
    );

-- Second test: Reduce NDV for l_partkey (~1.8M, about 10x smaller)
-- With lower NDV, the optimizer will NOT choose shuffle for the join.
-- This behavior change is related to issue #23562.
select table_cnt, json_extract(ndv_map, '$.l_partkey') partk from table_stats("associative.lineitem", 'patch', '{"ndv_map": {"l_partkey": 1840290, "l_quantity": 50}}') g;

-- EXPLAIN should show NO shuffle being used (due to reduced NDV)
-- Compare this output with the previous EXPLAIN to see the difference.
explain select
    sum(l_extendedprice) / 7.0 as avg_yearly
from
    lineitem,
    part
where
    p_partkey = l_partkey
    and p_brand = 'Brand#54'
    and p_container = 'LG BAG'
    and l_quantity < (
        select
            0.2 * avg(l_quantity)
        from
            lineitem
        where
            l_partkey = p_partkey
    );

-- Cleanup
drop table if exists associative.lineitem;
drop table if exists associative.part;
drop table if exists associative.file;
drop table if exists associative.task;
drop table if exists associative.connector_job;


