-- @suit
-- @case
-- @desc: Comprehensive TIMESTAMP precision tests - supplement existing tests
-- @label:bvt

-- Set timezone to UTC to ensure consistent results across different environments
SET time_zone = '+00:00';

DROP DATABASE IF EXISTS test_timestamp_precision;
CREATE DATABASE test_timestamp_precision;
USE test_timestamp_precision;

-- ============================================================================
-- Test 1: UPDATE scenario with TIMESTAMP precision
-- ============================================================================
DROP TABLE IF EXISTS t_timestamp_update;
CREATE TABLE t_timestamp_update (
    id INT PRIMARY KEY,
    ts0 TIMESTAMP(0),
    ts3 TIMESTAMP(3),
    ts6 TIMESTAMP(6)
);

-- Insert initial values
INSERT INTO t_timestamp_update VALUES (1, '2024-01-15 12:00:00', '2024-01-15 12:00:00.000', '2024-01-15 12:00:00.000000');

-- UPDATE with microsecond values (should be rounded according to column scale)
UPDATE t_timestamp_update SET ts0 = '2024-01-15 12:34:56.789012' WHERE id = 1;
UPDATE t_timestamp_update SET ts3 = '2024-01-15 12:34:56.789012' WHERE id = 1;
UPDATE t_timestamp_update SET ts6 = '2024-01-15 12:34:56.789012' WHERE id = 1;

-- Expected: ts0='2024-01-15 12:34:57', ts3='2024-01-15 12:34:56.789', ts6='2024-01-15 12:34:56.789012'
SELECT id, ts0, ts3, ts6,
       EXTRACT(MICROSECOND FROM ts0) AS ts0_micro,
       EXTRACT(MICROSECOND FROM ts3) AS ts3_micro,
       EXTRACT(MICROSECOND FROM ts6) AS ts6_micro
FROM t_timestamp_update;

DROP TABLE t_timestamp_update;

-- ============================================================================
-- Test 2: TIMESTAMPDIFF with different precision values
-- ============================================================================

-- TIMESTAMPDIFF should calculate based on actual stored precision
-- Compare two TIMESTAMP(0) values
SELECT TIMESTAMPDIFF(MICROSECOND, 
       CAST('2024-01-15 10:20:30.123456' AS TIMESTAMP(0)),
       CAST('2024-01-15 10:20:31.789012' AS TIMESTAMP(0))) AS diff_micro_scale0;

-- Compare two TIMESTAMP(6) values  
SELECT TIMESTAMPDIFF(MICROSECOND,
       CAST('2024-01-15 10:20:30.123456' AS TIMESTAMP(6)),
       CAST('2024-01-15 10:20:31.789012' AS TIMESTAMP(6))) AS diff_micro_scale6;

-- TIMESTAMPDIFF in seconds should work regardless of precision
SELECT TIMESTAMPDIFF(SECOND,
       CAST('2024-01-15 10:20:30.999999' AS TIMESTAMP(0)),
       CAST('2024-01-15 10:20:32.100000' AS TIMESTAMP(0))) AS diff_sec;

-- ============================================================================
-- Test 3: FROM_UNIXTIME with precision
-- ============================================================================

-- FROM_UNIXTIME with integer (should give TIMESTAMP(0))
SELECT FROM_UNIXTIME(1705319696) AS ts_from_int,
       EXTRACT(MICROSECOND FROM FROM_UNIXTIME(1705319696)) AS microseconds;

-- FROM_UNIXTIME with decimal (should preserve precision)
SELECT FROM_UNIXTIME(1705319696.123456) AS ts_from_decimal,
       EXTRACT(MICROSECOND FROM FROM_UNIXTIME(1705319696.123456)) AS microseconds;

-- ============================================================================
-- Test 4: UNIX_TIMESTAMP preserves precision
-- ============================================================================

-- UNIX_TIMESTAMP of TIMESTAMP with microseconds
SELECT UNIX_TIMESTAMP('2024-01-15 12:34:56.123456') AS unix_ts;

-- Compare with and without microseconds
SELECT UNIX_TIMESTAMP('2024-01-15 12:34:56.000000') AS unix_ts_0,
       UNIX_TIMESTAMP('2024-01-15 12:34:56.500000') AS unix_ts_500000;

-- ============================================================================
-- Test 5: CAST between TIMESTAMP and other types with precision
-- ============================================================================

-- TIMESTAMP(6) -> DATETIME(0)
SELECT CAST(CAST('2024-01-15 12:34:56.999999' AS TIMESTAMP(6)) AS DATETIME(0)) AS ts6_to_dt0;

-- TIMESTAMP(0) -> DATETIME(6)
SELECT CAST(CAST('2024-01-15 12:34:56' AS TIMESTAMP(0)) AS DATETIME(6)) AS ts0_to_dt6;

-- DATETIME(6) -> TIMESTAMP(0)
SELECT CAST(CAST('2024-01-15 12:34:56.999999' AS DATETIME(6)) AS TIMESTAMP(0)) AS dt6_to_ts0;

-- DATETIME(3) -> TIMESTAMP(3)
SELECT CAST(CAST('2024-01-15 12:34:56.789456' AS DATETIME(3)) AS TIMESTAMP(3)) AS dt3_to_ts3,
       EXTRACT(MICROSECOND FROM CAST(CAST('2024-01-15 12:34:56.789456' AS DATETIME(3)) AS TIMESTAMP(3))) AS microseconds;

-- ============================================================================
-- Test 6: CURRENT_TIMESTAMP with different scales in columns
-- ============================================================================
DROP TABLE IF EXISTS t_current_ts;
CREATE TABLE t_current_ts (
    id INT,
    ts0 TIMESTAMP(0),
    ts3 TIMESTAMP(3)
);

-- Insert using CURRENT_TIMESTAMP - should truncate to column scale
INSERT INTO t_current_ts VALUES (1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- Verify precision (only check that microseconds are properly truncated)
-- Expected: ts0_micro=0, ts3_sub_milli=0
SELECT id,
       EXTRACT(MICROSECOND FROM ts0) AS ts0_micro,
       EXTRACT(MICROSECOND FROM ts3) % 1000 AS ts3_sub_milli
FROM t_current_ts;

DROP TABLE t_current_ts;

-- ============================================================================
-- Test 7: WHERE clause precision matching for TIMESTAMP
-- ============================================================================
DROP TABLE IF EXISTS t_timestamp_where;
CREATE TABLE t_timestamp_where (
    id INT,
    ts3 TIMESTAMP(3)
);

-- Insert value that will be rounded
INSERT INTO t_timestamp_where VALUES (1, '2024-01-15 12:34:56.789456');

-- Expected: 1 (should match with rounded value)
SELECT COUNT(*) AS match_with_rounded
FROM t_timestamp_where
WHERE ts3 = '2024-01-15 12:34:56.789';

-- Expected: 1 (will be converted to TIMESTAMP(3) for comparison)
SELECT COUNT(*) AS match_with_original
FROM t_timestamp_where
WHERE ts3 = '2024-01-15 12:34:56.789456';

DROP TABLE t_timestamp_where;

-- ============================================================================
-- Test 8: Multiple rows with same second but different microseconds
-- ============================================================================
DROP TABLE IF EXISTS t_timestamp_distinct;
CREATE TABLE t_timestamp_distinct (
    id INT,
    ts0 TIMESTAMP(0)
);

-- Insert different microsecond values in same second (all should round to same value)
INSERT INTO t_timestamp_distinct VALUES (1, '2024-01-15 12:34:56.100000');
INSERT INTO t_timestamp_distinct VALUES (2, '2024-01-15 12:34:56.200000');
INSERT INTO t_timestamp_distinct VALUES (3, '2024-01-15 12:34:56.300000');
INSERT INTO t_timestamp_distinct VALUES (4, '2024-01-15 12:34:56.400000');

-- Expected: total=4, distinct=1
SELECT COUNT(*) AS total_count,
       COUNT(DISTINCT ts0) AS distinct_count
FROM t_timestamp_distinct;

-- Expected: 4 (all match)
SELECT COUNT(*) AS match_count
FROM t_timestamp_distinct
WHERE ts0 = '2024-01-15 12:34:56';

DROP TABLE t_timestamp_distinct;

-- ============================================================================
-- Test 9: Comparison between different TIMESTAMP precisions
-- ============================================================================

-- Expected: true (same base time)
SELECT CAST('2024-01-15 12:34:56' AS TIMESTAMP(0)) = CAST('2024-01-15 12:34:56.000000' AS TIMESTAMP(6)) AS ts_0_eq_6_same;

-- Expected: false (different microseconds)
SELECT CAST('2024-01-15 12:34:56' AS TIMESTAMP(0)) = CAST('2024-01-15 12:34:56.123456' AS TIMESTAMP(6)) AS ts_0_eq_6_diff;

-- ============================================================================
-- Test 10: Edge case - TIMESTAMP exactly at .500000 (round half up)
-- ============================================================================
DROP TABLE IF EXISTS t_timestamp_half;
CREATE TABLE t_timestamp_half (
    id INT,
    description VARCHAR(50),
    ts0 TIMESTAMP(0),
    ts1 TIMESTAMP(1),
    ts2 TIMESTAMP(2)
);

-- Exactly .500000 at scale 0 (should round up)
INSERT INTO t_timestamp_half VALUES (1, 'scale 0 half', CAST('2024-01-15 12:34:56.500000' AS TIMESTAMP(0)), NULL, NULL);

-- Exactly .X50000 at scale 1 (should round up)
INSERT INTO t_timestamp_half VALUES (2, 'scale 1 half', NULL, CAST('2024-01-15 12:34:56.150000' AS TIMESTAMP(1)), NULL);

-- Exactly .XX50000 at scale 2 (should round up)
INSERT INTO t_timestamp_half VALUES (3, 'scale 2 half', NULL, NULL, CAST('2024-01-15 12:34:56.125000' AS TIMESTAMP(2)));

-- Expected: ts0='2024-01-15 12:34:57', ts1='2024-01-15 12:34:56.2', ts2='2024-01-15 12:34:56.13'
SELECT id, description, ts0, ts1, ts2
FROM t_timestamp_half
ORDER BY id;

DROP TABLE t_timestamp_half;

-- ============================================================================
-- Test 11: DATE_ADD/DATE_SUB with TIMESTAMP and different INTERVAL types
-- ============================================================================
DROP TABLE IF EXISTS t_timestamp_dateadd;
CREATE TABLE t_timestamp_dateadd (
    id INT PRIMARY KEY,
    ts0 TIMESTAMP(0),
    ts3 TIMESTAMP(3),
    ts6 TIMESTAMP(6)
);

INSERT INTO t_timestamp_dateadd VALUES 
    (1, '2024-01-15 12:34:56', '2024-01-15 12:34:56.789', '2024-01-15 12:34:56.123456');

-- Test: INTERVAL with non-fractional units should preserve source scale
SELECT 
    id,
    DATE_ADD(ts0, INTERVAL 1 MINUTE) AS ts0_add_min,
    EXTRACT(MICROSECOND FROM DATE_ADD(ts0, INTERVAL 1 MINUTE)) AS ts0_micro,
    DATE_ADD(ts3, INTERVAL 1 HOUR) AS ts3_add_hour,
    EXTRACT(MICROSECOND FROM DATE_ADD(ts3, INTERVAL 1 HOUR)) AS ts3_micro,
    DATE_ADD(ts6, INTERVAL 1 DAY) AS ts6_add_day,
    EXTRACT(MICROSECOND FROM DATE_ADD(ts6, INTERVAL 1 DAY)) AS ts6_micro
FROM t_timestamp_dateadd;

-- Test: INTERVAL MICROSECOND should force scale=6
-- Expected: Results should have microsecond precision even for TIMESTAMP(0) source
SELECT 
    id,
    DATE_ADD(ts0, INTERVAL 1 MICROSECOND) AS ts0_add_micro,
    EXTRACT(MICROSECOND FROM DATE_ADD(ts0, INTERVAL 1 MICROSECOND)) AS ts0_result_micro,
    DATE_SUB(ts0, INTERVAL 1 MICROSECOND) AS ts0_sub_micro,
    EXTRACT(MICROSECOND FROM DATE_SUB(ts0, INTERVAL 1 MICROSECOND)) AS ts0_sub_result_micro
FROM t_timestamp_dateadd;

-- Test: Multiple microseconds with TIMESTAMP(6)
-- Expected: Correct arithmetic on microseconds
SELECT 
    id,
    DATE_ADD(ts6, INTERVAL 999 MICROSECOND) AS ts6_add_micro,
    EXTRACT(MICROSECOND FROM DATE_ADD(ts6, INTERVAL 999 MICROSECOND)) AS result_micro,
    DATE_SUB(ts6, INTERVAL 456 MICROSECOND) AS ts6_sub_micro,
    EXTRACT(MICROSECOND FROM DATE_SUB(ts6, INTERVAL 456 MICROSECOND)) AS sub_result_micro
FROM t_timestamp_dateadd;

-- Test: INTERVAL with integer SECOND should preserve source scale
SELECT 
    id,
    DATE_ADD(ts0, INTERVAL 1 SECOND) AS ts0_add_sec,
    EXTRACT(MICROSECOND FROM DATE_ADD(ts0, INTERVAL 1 SECOND)) AS ts0_sec_micro,
    DATE_ADD(ts3, INTERVAL 2 SECOND) AS ts3_add_sec,
    EXTRACT(MICROSECOND FROM DATE_ADD(ts3, INTERVAL 2 SECOND)) AS ts3_sec_micro
FROM t_timestamp_dateadd;

DROP TABLE t_timestamp_dateadd;

DROP DATABASE test_timestamp_precision;

