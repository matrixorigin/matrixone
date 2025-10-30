-- @suit
-- @case
-- @desc: Comprehensive DATETIME precision tests (scale 0-6) - Align with MySQL 8.0
-- @label:bvt

DROP DATABASE IF EXISTS test_datetime_precision;
CREATE DATABASE test_datetime_precision;
USE test_datetime_precision;

-- ============================================================================
-- Test 1: Basic DATETIME(0), DATETIME(3), DATETIME(6) insertion and retrieval
-- ============================================================================
DROP TABLE IF EXISTS t_datetime_basic;
CREATE TABLE t_datetime_basic (
    id INT,
    dt0 DATETIME(0),
    dt3 DATETIME(3),
    dt6 DATETIME(6)
);

-- Insert with full microsecond precision
INSERT INTO t_datetime_basic VALUES (1, '2024-01-15 12:34:56.123456', '2024-01-15 12:34:56.123456', '2024-01-15 12:34:56.123456');

-- Verify precision truncation/rounding
SELECT id, dt0, dt3, dt6,
       EXTRACT(MICROSECOND FROM dt0) AS dt0_micro,
       EXTRACT(MICROSECOND FROM dt3) AS dt3_micro,
       EXTRACT(MICROSECOND FROM dt6) AS dt6_micro
FROM t_datetime_basic;

DROP TABLE t_datetime_basic;

-- ============================================================================
-- Test 2: Rounding behavior for all scales (0-6)
-- ============================================================================
DROP TABLE IF EXISTS t_datetime_rounding;
CREATE TABLE t_datetime_rounding (
    scale_val INT,
    input_str VARCHAR(30),
    dt DATETIME(6)
);

-- Test scale 0: seconds (round .123456 -> .000000)
INSERT INTO t_datetime_rounding VALUES (0, '2024-01-15 12:34:56.123456', CAST('2024-01-15 12:34:56.123456' AS DATETIME(0)));

-- Test scale 1: deciseconds (round .123456 -> .100000)
INSERT INTO t_datetime_rounding VALUES (1, '2024-01-15 12:34:56.123456', CAST('2024-01-15 12:34:56.123456' AS DATETIME(1)));

-- Test scale 2: centiseconds (round .123456 -> .120000)
INSERT INTO t_datetime_rounding VALUES (2, '2024-01-15 12:34:56.123456', CAST('2024-01-15 12:34:56.123456' AS DATETIME(2)));

-- Test scale 3: milliseconds (round .123456 -> .123000)
INSERT INTO t_datetime_rounding VALUES (3, '2024-01-15 12:34:56.123456', CAST('2024-01-15 12:34:56.123456' AS DATETIME(3)));

-- Test scale 4: (round .123456 -> .123500)
INSERT INTO t_datetime_rounding VALUES (4, '2024-01-15 12:34:56.123456', CAST('2024-01-15 12:34:56.123456' AS DATETIME(4)));

-- Test scale 5: (round .123456 -> .123460)
INSERT INTO t_datetime_rounding VALUES (5, '2024-01-15 12:34:56.123456', CAST('2024-01-15 12:34:56.123456' AS DATETIME(5)));

-- Test scale 6: microseconds (no rounding)
INSERT INTO t_datetime_rounding VALUES (6, '2024-01-15 12:34:56.123456', CAST('2024-01-15 12:34:56.123456' AS DATETIME(6)));

SELECT scale_val, input_str, dt, EXTRACT(MICROSECOND FROM dt) AS microseconds
FROM t_datetime_rounding
ORDER BY scale_val;

DROP TABLE t_datetime_rounding;

-- ============================================================================
-- Test 3: CAST between different DATETIME scales
-- ============================================================================

-- Expected: 2024-01-15 12:34:57 (round .999999 up)
SELECT CAST(CAST('2024-01-15 12:34:56.999999' AS DATETIME(6)) AS DATETIME(0)) AS datetime_6_to_0;

-- Expected: 2024-01-15 12:34:56.000000
SELECT CAST(CAST('2024-01-15 12:34:56' AS DATETIME(0)) AS DATETIME(6)) AS datetime_0_to_6;

-- Expected: 2024-01-15 12:34:57 (round .789 up)
SELECT CAST(CAST('2024-01-15 12:34:56.789' AS DATETIME(3)) AS DATETIME(0)) AS datetime_3_to_0;

-- ============================================================================
-- Test 4: UPDATE scenario with precision
-- ============================================================================
DROP TABLE IF EXISTS t_datetime_update;
CREATE TABLE t_datetime_update (
    id INT PRIMARY KEY,
    dt0 DATETIME(0),
    dt3 DATETIME(3)
);

-- Insert initial values
INSERT INTO t_datetime_update VALUES (1, '2024-01-15 12:00:00', '2024-01-15 12:00:00.000');

-- UPDATE with microsecond values (should be rounded)
UPDATE t_datetime_update SET dt0 = '2024-01-15 12:34:56.789012' WHERE id = 1;
UPDATE t_datetime_update SET dt3 = '2024-01-15 12:34:56.789012' WHERE id = 1;

-- Expected: dt0='2024-01-15 12:34:57', dt3='2024-01-15 12:34:56.789'
SELECT id, dt0, dt3
FROM t_datetime_update;

DROP TABLE t_datetime_update;

-- ============================================================================
-- Test 5: WHERE clause with precision matching
-- ============================================================================
DROP TABLE IF EXISTS t_datetime_where;
CREATE TABLE t_datetime_where (
    id INT,
    dt3 DATETIME(3)
);

-- Insert value that will be rounded
INSERT INTO t_datetime_where VALUES (1, '2024-01-15 12:34:56.789456');

-- Expected: 1 (should match with rounded value)
SELECT COUNT(*) AS match_with_rounded
FROM t_datetime_where
WHERE dt3 = '2024-01-15 12:34:56.789';

-- Expected: 1 (will be converted to DATETIME(3) for comparison)
SELECT COUNT(*) AS match_with_original
FROM t_datetime_where
WHERE dt3 = '2024-01-15 12:34:56.789456';

DROP TABLE t_datetime_where;

-- ============================================================================
-- Test 6: Comparison between different DATETIME precisions
-- ============================================================================

-- Expected: true (same base time)
SELECT CAST('2024-01-15 12:34:56' AS DATETIME(0)) = CAST('2024-01-15 12:34:56.000000' AS DATETIME(6)) AS datetime_0_eq_6_same;

-- Expected: false (different microseconds)
SELECT CAST('2024-01-15 12:34:56' AS DATETIME(0)) = CAST('2024-01-15 12:34:56.123456' AS DATETIME(6)) AS datetime_0_eq_6_diff;

-- ============================================================================
-- Test 7: Edge case - DATETIME value exactly at .500000 (round half up)
-- ============================================================================
DROP TABLE IF EXISTS t_datetime_half;
CREATE TABLE t_datetime_half (
    id INT,
    description VARCHAR(50),
    dt0 DATETIME(0),
    dt1 DATETIME(1),
    dt2 DATETIME(2)
);

-- Exactly .500000 at scale 0 (should round up)
INSERT INTO t_datetime_half VALUES (1, 'scale 0 half', CAST('2024-01-15 12:34:56.500000' AS DATETIME(0)), NULL, NULL);

-- Exactly .X50000 at scale 1 (should round up)
INSERT INTO t_datetime_half VALUES (2, 'scale 1 half', NULL, CAST('2024-01-15 12:34:56.150000' AS DATETIME(1)), NULL);

-- Exactly .XX50000 at scale 2 (should round up)
INSERT INTO t_datetime_half VALUES (3, 'scale 2 half', NULL, NULL, CAST('2024-01-15 12:34:56.125000' AS DATETIME(2)));

-- Expected: dt0='2024-01-15 12:34:57', dt1='2024-01-15 12:34:56.2', dt2='2024-01-15 12:34:56.13'
SELECT id, description, dt0, dt1, dt2
FROM t_datetime_half
ORDER BY id;

DROP TABLE t_datetime_half;

-- ============================================================================
-- Test 8: DATE_ADD and DATE_SUB with precision preservation
-- ============================================================================

-- DATE_ADD should preserve precision
SELECT DATE_ADD('2024-01-15 12:34:56.123456', INTERVAL 1 HOUR) AS result,
       EXTRACT(MICROSECOND FROM DATE_ADD('2024-01-15 12:34:56.123456', INTERVAL 1 HOUR)) AS microseconds;

-- DATE_SUB should preserve precision
SELECT DATE_SUB('2024-01-15 12:34:56.789012', INTERVAL 30 MINUTE) AS result,
       EXTRACT(MICROSECOND FROM DATE_SUB('2024-01-15 12:34:56.789012', INTERVAL 30 MINUTE)) AS microseconds;

-- Adding fractional seconds
SELECT DATE_ADD('2024-01-15 12:34:56', INTERVAL 500000 MICROSECOND) AS result,
       EXTRACT(MICROSECOND FROM DATE_ADD('2024-01-15 12:34:56', INTERVAL 500000 MICROSECOND)) AS microseconds;

-- ============================================================================
-- Test 9: DATE to DATETIME conversion with different scales
-- ============================================================================
DROP TABLE IF EXISTS t_date_to_datetime;
CREATE TABLE t_date_to_datetime (
    id INT,
    d DATE,
    dt0 DATETIME(0),
    dt3 DATETIME(3),
    dt6 DATETIME(6)
);

-- DATE should convert to 00:00:00.000000 (all zeros for time part)
INSERT INTO t_date_to_datetime VALUES (1, '2024-01-15', CAST('2024-01-15' AS DATETIME(0)), CAST('2024-01-15' AS DATETIME(3)), CAST('2024-01-15' AS DATETIME(6)));

-- Expected: all microseconds should be 0
SELECT id, dt0, dt3, dt6,
       EXTRACT(MICROSECOND FROM dt0) AS dt0_micro,
       EXTRACT(MICROSECOND FROM dt3) AS dt3_micro,
       EXTRACT(MICROSECOND FROM dt6) AS dt6_micro
FROM t_date_to_datetime;

DROP TABLE t_date_to_datetime;

-- ============================================================================
-- Test 10: Range queries with precision
-- ============================================================================
DROP TABLE IF EXISTS t_datetime_range;
CREATE TABLE t_datetime_range (
    id INT,
    dt0 DATETIME(0)
);

-- Insert multiple values with different fractional parts (all round to same second)
INSERT INTO t_datetime_range VALUES (1, '2024-01-15 12:34:56.100000');
INSERT INTO t_datetime_range VALUES (2, '2024-01-15 12:34:56.200000');
INSERT INTO t_datetime_range VALUES (3, '2024-01-15 12:34:56.300000');
INSERT INTO t_datetime_range VALUES (4, '2024-01-15 12:34:56.400000');

-- Expected: total_count=4, distinct_count=1 (all stored as same second)
SELECT COUNT(*) AS total_count,
       COUNT(DISTINCT dt0) AS distinct_count
FROM t_datetime_range;

-- Expected: 4 (all should match)
SELECT COUNT(*) AS match_count
FROM t_datetime_range
WHERE dt0 = '2024-01-15 12:34:56';

DROP TABLE t_datetime_range;

-- ============================================================================
-- Test 11: STR_TO_DATE with precision
-- ============================================================================

-- STR_TO_DATE should preserve microseconds from format
SELECT STR_TO_DATE('2024-01-15 12:34:56.123456', '%Y-%m-%d %H:%i:%s.%f') AS result,
       EXTRACT(MICROSECOND FROM STR_TO_DATE('2024-01-15 12:34:56.123456', '%Y-%m-%d %H:%i:%s.%f')) AS microseconds;

-- ============================================================================
-- Test 12: DATETIME arithmetic preserves precision
-- ============================================================================

-- DATETIME + INTERVAL should preserve precision
SELECT '2024-01-15 12:34:56.123456' + INTERVAL 1 DAY AS result,
       EXTRACT(MICROSECOND FROM ('2024-01-15 12:34:56.123456' + INTERVAL 1 DAY)) AS microseconds;

-- DATETIME - INTERVAL should preserve precision  
SELECT '2024-01-15 12:34:56.789012' - INTERVAL 2 HOUR AS result,
       EXTRACT(MICROSECOND FROM ('2024-01-15 12:34:56.789012' - INTERVAL 2 HOUR)) AS microseconds;

-- ============================================================================
-- Test 13: NOW() with different scales
-- ============================================================================
DROP TABLE IF EXISTS t_now_scales;
CREATE TABLE t_now_scales (
    id INT,
    dt0 DATETIME(0),
    dt3 DATETIME(3),
    dt6 DATETIME(6)
);

-- Insert using NOW() - should truncate to column scale
INSERT INTO t_now_scales VALUES (1, NOW(), NOW(), NOW());

-- Verify that each column has appropriate precision (only check precision, not timestamp)
-- Expected: dt0_micro=0 (scale 0), dt3_sub_milli=0 (scale 3)
SELECT id,
       EXTRACT(MICROSECOND FROM dt0) % 1000000 AS dt0_micro,
       EXTRACT(MICROSECOND FROM dt3) % 1000 AS dt3_sub_milli
FROM t_now_scales;

DROP TABLE t_now_scales;

-- ============================================================================
-- Test 14: Comparison with string literals of different precision
-- ============================================================================
DROP TABLE IF EXISTS t_datetime_compare;
CREATE TABLE t_datetime_compare (
    id INT,
    dt3 DATETIME(3)
);

-- Insert with 6-digit precision (will round to 3)
INSERT INTO t_datetime_compare VALUES (1, '2024-01-15 12:34:56.123456');

-- Expected: 1 (should match)
SELECT COUNT(*) AS match_with_123
FROM t_datetime_compare
WHERE dt3 = '2024-01-15 12:34:56.123';

-- Expected: 0 (should not match)
SELECT COUNT(*) AS match_with_124
FROM t_datetime_compare
WHERE dt3 = '2024-01-15 12:34:56.124';

DROP TABLE t_datetime_compare;

-- ============================================================================
-- Test 15: DATETIME functions with DATETIME(0) columns
-- ============================================================================
DROP TABLE IF EXISTS t_datetime_funcs;
CREATE TABLE t_datetime_funcs (
    id INT,
    dt0 DATETIME(0)
);

INSERT INTO t_datetime_funcs VALUES (1, '2024-01-15 12:34:56.999999');

-- Expected: dt0='2024-01-15 12:34:57' (rounded to next second)
SELECT id, dt0
FROM t_datetime_funcs;

-- Expected: microseconds=0 (DATETIME(0) column should maintain scale 0)
SELECT DATE_ADD(dt0, INTERVAL 1 DAY) AS result,
       EXTRACT(MICROSECOND FROM DATE_ADD(dt0, INTERVAL 1 DAY)) AS microseconds
FROM t_datetime_funcs;

DROP TABLE t_datetime_funcs;

DROP DATABASE test_datetime_precision;

