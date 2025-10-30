-- @suit
-- @case
-- @desc: Comprehensive TIME precision tests (scale 0-6) - Align with MySQL 8.0
-- @label:bvt

DROP DATABASE IF EXISTS test_time_precision;
CREATE DATABASE test_time_precision;
USE test_time_precision;

-- ============================================================================
-- Test 1: Basic TIME(0), TIME(3), TIME(6) insertion and retrieval
-- ============================================================================
DROP TABLE IF EXISTS t_time_basic;
CREATE TABLE t_time_basic (
    id INT,
    t0 TIME(0),
    t3 TIME(3),
    t6 TIME(6)
);

-- Insert with full microsecond precision
INSERT INTO t_time_basic VALUES (1, '12:34:56.123456', '12:34:56.123456', '12:34:56.123456');

-- Verify precision truncation/rounding
SELECT id, t0, t3, t6,
       EXTRACT(MICROSECOND FROM t0) AS t0_micro,
       EXTRACT(MICROSECOND FROM t3) AS t3_micro,
       EXTRACT(MICROSECOND FROM t6) AS t6_micro
FROM t_time_basic;

DROP TABLE t_time_basic;

-- ============================================================================
-- Test 2: Rounding behavior for all scales (0-6)
-- ============================================================================
DROP TABLE IF EXISTS t_time_rounding;
CREATE TABLE t_time_rounding (
    scale_val INT,
    input_str VARCHAR(20),
    t TIME(6)
);

-- Test scale 0: seconds (round .123456 -> .000000)
INSERT INTO t_time_rounding VALUES (0, '12:34:56.123456', CAST('12:34:56.123456' AS TIME(0)));

-- Test scale 1: deciseconds (round .123456 -> .100000)
INSERT INTO t_time_rounding VALUES (1, '12:34:56.123456', CAST('12:34:56.123456' AS TIME(1)));

-- Test scale 2: centiseconds (round .123456 -> .120000)
INSERT INTO t_time_rounding VALUES (2, '12:34:56.123456', CAST('12:34:56.123456' AS TIME(2)));

-- Test scale 3: milliseconds (round .123456 -> .123000)
INSERT INTO t_time_rounding VALUES (3, '12:34:56.123456', CAST('12:34:56.123456' AS TIME(3)));

-- Test scale 4: (round .123456 -> .123500)
INSERT INTO t_time_rounding VALUES (4, '12:34:56.123456', CAST('12:34:56.123456' AS TIME(4)));

-- Test scale 5: (round .123456 -> .123460)
INSERT INTO t_time_rounding VALUES (5, '12:34:56.123456', CAST('12:34:56.123456' AS TIME(5)));

-- Test scale 6: microseconds (no rounding)
INSERT INTO t_time_rounding VALUES (6, '12:34:56.123456', CAST('12:34:56.123456' AS TIME(6)));

SELECT scale_val, input_str, t, EXTRACT(MICROSECOND FROM t) AS microseconds
FROM t_time_rounding
ORDER BY scale_val;

DROP TABLE t_time_rounding;

-- ============================================================================
-- Test 3: Boundary rounding - values that round up
-- ============================================================================
DROP TABLE IF EXISTS t_time_boundary;
CREATE TABLE t_time_boundary (
    id INT,
    description VARCHAR(50),
    t0 TIME(0)
);

-- Round to next second: .999999 -> +1 second
INSERT INTO t_time_boundary VALUES (1, 'next second', CAST('12:34:56.999999' AS TIME(0)));

-- Round to next minute: 59.999999 -> next minute
INSERT INTO t_time_boundary VALUES (2, 'next minute', CAST('12:34:59.999999' AS TIME(0)));

-- Round to next hour: 59:59.999999 -> next hour
INSERT INTO t_time_boundary VALUES (3, 'next hour', CAST('12:59:59.999999' AS TIME(0)));

-- Maximum TIME boundary: 838:59:59.999999
INSERT INTO t_time_boundary VALUES (4, 'max time', CAST('838:59:59.500000' AS TIME(0)));

SELECT id, description, t0,
       CASE id
           WHEN 1 THEN CASE WHEN t0 = '12:34:57' THEN 'PASS' ELSE 'FAIL' END
           WHEN 2 THEN CASE WHEN t0 = '12:35:00' THEN 'PASS' ELSE 'FAIL' END
           WHEN 3 THEN CASE WHEN t0 = '13:00:00' THEN 'PASS' ELSE 'FAIL' END
           WHEN 4 THEN CASE WHEN t0 = '839:00:00' THEN 'PASS' ELSE 'FAIL' END
       END AS result
FROM t_time_boundary
ORDER BY id;

DROP TABLE t_time_boundary;

-- ============================================================================
-- Test 4: Negative TIME values
-- ============================================================================
DROP TABLE IF EXISTS t_time_negative;
CREATE TABLE t_time_negative (
    id INT,
    t0 TIME(0),
    t3 TIME(3),
    t6 TIME(6)
);

-- Negative TIME with microseconds
INSERT INTO t_time_negative VALUES (1, '-12:34:56.123456', '-12:34:56.123456', '-12:34:56.123456');
INSERT INTO t_time_negative VALUES (2, '-12:34:56.999999', '-12:34:56.999999', '-12:34:56.999999');

SELECT id, t0, t3, t6,
       EXTRACT(MICROSECOND FROM t0) AS t0_micro,
       EXTRACT(MICROSECOND FROM t3) AS t3_micro,
       EXTRACT(MICROSECOND FROM t6) AS t6_micro
FROM t_time_negative
ORDER BY id;

DROP TABLE t_time_negative;

-- ============================================================================
-- Test 5: CAST between different TIME scales
-- ============================================================================

-- CAST TIME(6) -> TIME(0) using nested CAST
SELECT CAST(CAST('12:34:56.999999' AS TIME(6)) AS TIME(0)) AS result,
       CASE WHEN CAST(CAST('12:34:56.999999' AS TIME(6)) AS TIME(0)) = '12:34:57' 
            THEN 'PASS' ELSE 'FAIL' END AS check_result;

-- CAST TIME(0) -> TIME(6) (should add .000000)
SELECT CAST(CAST('12:34:56' AS TIME(0)) AS TIME(6)) AS result,
       CASE WHEN CAST(CAST('12:34:56' AS TIME(0)) AS TIME(6)) = '12:34:56.000000' 
            THEN 'PASS' ELSE 'FAIL' END AS check_result;

-- CAST TIME(3) -> TIME(0)
SELECT CAST(CAST('12:34:56.789' AS TIME(3)) AS TIME(0)) AS result,
       CASE WHEN CAST(CAST('12:34:56.789' AS TIME(3)) AS TIME(0)) = '12:34:57' 
            THEN 'PASS' ELSE 'FAIL' END AS check_result;

-- ============================================================================
-- Test 6: TIME arithmetic functions with precision
-- ============================================================================

-- TIMEDIFF preserves maximum precision
SELECT TIMEDIFF('12:00:00', '10:30:00.123456') AS result,
       EXTRACT(MICROSECOND FROM TIMEDIFF('12:00:00', '10:30:00.123456')) AS microseconds;

-- TIMEDIFF with both parameters having microseconds
SELECT TIMEDIFF('12:34:56.789012', '10:20:30.123456') AS result,
       EXTRACT(MICROSECOND FROM TIMEDIFF('12:34:56.789012', '10:20:30.123456')) AS microseconds;

-- Note: ADDTIME and SUBTIME are not yet supported in MatrixOne

-- ============================================================================
-- Test 7: UPDATE scenario with precision
-- ============================================================================
DROP TABLE IF EXISTS t_time_update;
CREATE TABLE t_time_update (
    id INT PRIMARY KEY,
    t0 TIME(0),
    t3 TIME(3)
);

-- Insert initial values
INSERT INTO t_time_update VALUES (1, '12:00:00', '12:00:00.000');

-- UPDATE with microsecond values (should be rounded)
UPDATE t_time_update SET t0 = '12:34:56.789012' WHERE id = 1;
UPDATE t_time_update SET t3 = '12:34:56.789012' WHERE id = 1;

-- Verify rounding
SELECT id, t0, t3,
       CASE WHEN t0 = '12:34:57' THEN 'PASS' ELSE 'FAIL' END AS t0_check,
       CASE WHEN t3 = '12:34:56.789' THEN 'PASS' ELSE 'FAIL' END AS t3_check
FROM t_time_update;

DROP TABLE t_time_update;

-- ============================================================================
-- Test 8: WHERE clause with precision matching
-- ============================================================================
DROP TABLE IF EXISTS t_time_where;
CREATE TABLE t_time_where (
    id INT,
    t3 TIME(3)
);

-- Insert value that will be rounded
INSERT INTO t_time_where VALUES (1, '12:34:56.789456');

-- Should match with rounded value
SELECT COUNT(*) AS match_count,
       CASE WHEN COUNT(*) = 1 THEN 'PASS' ELSE 'FAIL' END AS result
FROM t_time_where
WHERE t3 = '12:34:56.789';

-- Query with original microsecond value (will be converted to TIME(3) for comparison)
-- Note: This may match due to implicit conversion, which is expected behavior
SELECT COUNT(*) AS match_count
FROM t_time_where
WHERE t3 = '12:34:56.789456';

DROP TABLE t_time_where;

-- ============================================================================
-- Test 9: Comparison between different TIME precisions
-- ============================================================================

-- Compare TIME(0) with TIME(6) - same base time
SELECT CAST('12:34:56' AS TIME(0)) = CAST('12:34:56.000000' AS TIME(6)) AS equal_result,
       CASE WHEN CAST('12:34:56' AS TIME(0)) = CAST('12:34:56.000000' AS TIME(6))
            THEN 'PASS' ELSE 'FAIL' END AS check_result;

-- Compare TIME(0) with TIME(6) - different precision
SELECT CAST('12:34:56' AS TIME(0)) = CAST('12:34:56.123456' AS TIME(6)) AS equal_result,
       CASE WHEN CAST('12:34:56' AS TIME(0)) != CAST('12:34:56.123456' AS TIME(6))
            THEN 'PASS' ELSE 'FAIL' END AS check_result;

-- ============================================================================
-- Test 10: Edge case - TIME value exactly at .500000 (round half up)
-- ============================================================================
DROP TABLE IF EXISTS t_time_half;
CREATE TABLE t_time_half (
    id INT,
    description VARCHAR(50),
    t0 TIME(0),
    t1 TIME(1),
    t2 TIME(2)
);

-- Exactly .500000 at scale 0 (should round up)
INSERT INTO t_time_half VALUES (1, 'scale 0 half', CAST('12:34:56.500000' AS TIME(0)), NULL, NULL);

-- Exactly .X50000 at scale 1 (should round up)
INSERT INTO t_time_half VALUES (2, 'scale 1 half', NULL, CAST('12:34:56.150000' AS TIME(1)), NULL);

-- Exactly .XX50000 at scale 2 (should round up)
INSERT INTO t_time_half VALUES (3, 'scale 2 half', NULL, NULL, CAST('12:34:56.125000' AS TIME(2)));

SELECT id, description, t0, t1, t2,
       CASE id
           WHEN 1 THEN CASE WHEN t0 = '12:34:57' THEN 'PASS' ELSE 'FAIL' END
           WHEN 2 THEN CASE WHEN t1 = '12:34:56.2' THEN 'PASS' ELSE 'FAIL' END
           WHEN 3 THEN CASE WHEN t2 = '12:34:56.13' THEN 'PASS' ELSE 'FAIL' END
       END AS result
FROM t_time_half
ORDER BY id;

DROP TABLE t_time_half;

DROP DATABASE test_time_precision;

