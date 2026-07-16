-- issue #25782: bounded data set used by both join plans.
-- Each table has exactly 1,024 baseline rows plus four persisted 32,768-row
-- high-tail objects (132,096 rows total).  Planner statistics are patched in
-- the plan phases only; they never change the rows read by the query.
set @@join_spill_mem=1000;
select @@join_spill_mem;

CREATE DATABASE IF NOT EXISTS issue25782;
USE issue25782;
DROP TABLE IF EXISTS issue25782_probe;
DROP TABLE IF EXISTS issue25782_build;

CREATE TABLE issue25782_build (
    k BIGINT NOT NULL,
    payload VARCHAR(64) NOT NULL
) CLUSTER BY k;
CREATE TABLE issue25782_probe (
    k BIGINT NOT NULL,
    payload VARCHAR(64) NOT NULL
) CLUSTER BY k;

-- Baseline object: keys [1, 1024].
INSERT INTO issue25782_build
SELECT result, concat('build-small-', result) FROM generate_series(1, 1024, 1) g;
INSERT INTO issue25782_probe
SELECT result, concat('probe-small-', result) FROM generate_series(1, 1024, 1) g;
SELECT 'BASELINE_COUNT' AS marker, (SELECT count(*) FROM issue25782_build) AS build_rows,
       (SELECT count(*) FROM issue25782_probe) AS probe_rows;
SELECT mo_ctl('dn', 'flush', 'issue25782.issue25782_build');
SELECT mo_ctl('dn', 'flush', 'issue25782.issue25782_probe');
SELECT 'BASELINE_STATS' AS marker;
SELECT table_name, table_cnt, block_number, accurate_object_number
FROM table_stats('issue25782.issue25782_build', 'refresh', 'full') g;
SELECT table_name, table_cnt, block_number, accurate_object_number
FROM table_stats('issue25782.issue25782_probe', 'refresh', 'full') g;

-- High-tail object 1: keys [1000001, 1032768].
INSERT INTO issue25782_build
SELECT result, concat('build-high-1-', result)
FROM generate_series(1000001, 1032768, 1) g;
INSERT INTO issue25782_probe
SELECT result, concat('probe-high-1-', result)
FROM generate_series(1000001, 1032768, 1) g;
SELECT mo_ctl('dn', 'flush', 'issue25782.issue25782_build');
SELECT mo_ctl('dn', 'flush', 'issue25782.issue25782_probe');
SELECT 'CHUNK_1_STATS' AS marker;
SELECT table_name, table_cnt, block_number, accurate_object_number
FROM table_stats('issue25782.issue25782_build', 'refresh', 'full') g;
SELECT table_name, table_cnt, block_number, accurate_object_number
FROM table_stats('issue25782.issue25782_probe', 'refresh', 'full') g;

-- High-tail object 2: keys [1032769, 1065536], contiguous and non-overlapping.
INSERT INTO issue25782_build
SELECT result, concat('build-high-2-', result)
FROM generate_series(1032769, 1065536, 1) g;
INSERT INTO issue25782_probe
SELECT result, concat('probe-high-2-', result)
FROM generate_series(1032769, 1065536, 1) g;
SELECT mo_ctl('dn', 'flush', 'issue25782.issue25782_build');
SELECT mo_ctl('dn', 'flush', 'issue25782.issue25782_probe');
SELECT 'CHUNK_2_STATS' AS marker;
SELECT table_name, table_cnt, block_number, accurate_object_number
FROM table_stats('issue25782.issue25782_build', 'refresh', 'full') g;
SELECT table_name, table_cnt, block_number, accurate_object_number
FROM table_stats('issue25782.issue25782_probe', 'refresh', 'full') g;

-- High-tail object 3: keys [1065537, 1098304], contiguous and non-overlapping.
INSERT INTO issue25782_build
SELECT result, concat('build-high-3-', result)
FROM generate_series(1065537, 1098304, 1) g;
INSERT INTO issue25782_probe
SELECT result, concat('probe-high-3-', result)
FROM generate_series(1065537, 1098304, 1) g;
SELECT mo_ctl('dn', 'flush', 'issue25782.issue25782_build');
SELECT mo_ctl('dn', 'flush', 'issue25782.issue25782_probe');
SELECT 'CHUNK_3_STATS' AS marker;
SELECT table_name, table_cnt, block_number, accurate_object_number
FROM table_stats('issue25782.issue25782_build', 'refresh', 'full') g;
SELECT table_name, table_cnt, block_number, accurate_object_number
FROM table_stats('issue25782.issue25782_probe', 'refresh', 'full') g;

-- High-tail object 4: keys [1098305, 1131072], contiguous and non-overlapping.
INSERT INTO issue25782_build
SELECT result, concat('build-high-4-', result)
FROM generate_series(1098305, 1131072, 1) g;
INSERT INTO issue25782_probe
SELECT result, concat('probe-high-4-', result)
FROM generate_series(1098305, 1131072, 1) g;
SELECT mo_ctl('dn', 'flush', 'issue25782.issue25782_build');
SELECT mo_ctl('dn', 'flush', 'issue25782.issue25782_probe');
SELECT sleep(1);

SELECT 'FINAL_COUNT' AS marker, (SELECT count(*) FROM issue25782_build) AS build_rows,
       (SELECT count(*) FROM issue25782_probe) AS probe_rows;
SELECT 'FINAL_STATS' AS marker;
SELECT table_name, table_cnt, block_number, accurate_object_number
FROM table_stats('issue25782.issue25782_build', 'refresh', 'full') g;
SELECT table_name, table_cnt, block_number, accurate_object_number
FROM table_stats('issue25782.issue25782_probe', 'refresh', 'full') g;
