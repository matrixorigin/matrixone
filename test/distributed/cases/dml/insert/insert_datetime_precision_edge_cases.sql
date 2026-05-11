-- @suit
-- @case
-- @desc: Test edge cases for DATETIME and TIMESTAMP precision truncation
-- @label:bvt

DROP DATABASE IF EXISTS test_precision_edge_cases;
CREATE DATABASE test_precision_edge_cases;
USE test_precision_edge_cases;

-- Test 1: Verify DATE to TIMESTAMP does not need truncation (microseconds are always 0)
DROP TABLE IF EXISTS t_date_to_timestamp;
CREATE TABLE t_date_to_timestamp (
    id INT,
    d DATE,
    ts0 TIMESTAMP(0),
    ts3 TIMESTAMP(3),
    ts6 TIMESTAMP(6)
);

INSERT INTO t_date_to_timestamp VALUES (1, '2024-01-15', '2024-01-15', '2024-01-15', '2024-01-15');
INSERT INTO t_date_to_timestamp VALUES (2, CURDATE(), CAST(CURDATE() AS TIMESTAMP(0)), CAST(CURDATE() AS TIMESTAMP(3)), CAST(CURDATE() AS TIMESTAMP(6)));

SELECT id, 
       EXTRACT(MICROSECOND FROM ts0) AS ts0_micro,
       EXTRACT(MICROSECOND FROM ts3) AS ts3_micro,
       EXTRACT(MICROSECOND FROM ts6) AS ts6_micro,
       CASE WHEN EXTRACT(MICROSECOND FROM ts0) = 0 AND 
                 EXTRACT(MICROSECOND FROM ts3) = 0 AND 
                 EXTRACT(MICROSECOND FROM ts6) = 0 
            THEN 'PASS' ELSE 'FAIL' END AS result
FROM t_date_to_timestamp;

DROP TABLE t_date_to_timestamp;

-- Test 2: DATETIME to TIMESTAMP conversion with precision
DROP TABLE IF EXISTS t_datetime_to_timestamp;
CREATE TABLE t_datetime_to_timestamp (
    id INT,
    dt DATETIME(6),
    ts0 TIMESTAMP(0),
    ts3 TIMESTAMP(3)
);

INSERT INTO t_datetime_to_timestamp VALUES (
    1, 
    '2024-01-15 10:20:30.739999',
    CAST('2024-01-15 10:20:30.739999' AS TIMESTAMP(0)),
    CAST('2024-01-15 10:20:30.739999' AS TIMESTAMP(3))
);

-- ts0 should round .739999 to next second (31)
-- ts3 should round .739999 to .740000
SELECT id,
       ts0,
       ts3,
       EXTRACT(SECOND FROM ts0) AS ts0_sec,
       EXTRACT(MICROSECOND FROM ts0) AS ts0_micro,
       EXTRACT(MICROSECOND FROM ts3) AS ts3_micro,
       CASE WHEN EXTRACT(SECOND FROM ts0) = 31 AND EXTRACT(MICROSECOND FROM ts0) = 0 THEN 'PASS' ELSE 'FAIL' END AS ts0_check,
       CASE WHEN EXTRACT(MICROSECOND FROM ts3) = 740000 THEN 'PASS' ELSE 'FAIL' END AS ts3_check
FROM t_datetime_to_timestamp;

DROP TABLE t_datetime_to_timestamp;

-- Test 3: Rounding at time boundaries (minute, hour, day)
DROP TABLE IF EXISTS t_boundary_rounding;
CREATE TABLE t_boundary_rounding (
    id INT,
    description VARCHAR(50),
    dt0 DATETIME(0)
);

-- Rounding to next minute
INSERT INTO t_boundary_rounding VALUES (1, 'next minute', CAST('2024-01-15 10:20:59.999999' AS DATETIME(0)));

-- Rounding to next hour
INSERT INTO t_boundary_rounding VALUES (2, 'next hour', CAST('2024-01-15 10:59:59.999999' AS DATETIME(0)));

-- Rounding to next day
INSERT INTO t_boundary_rounding VALUES (3, 'next day', CAST('2024-01-15 23:59:59.999999' AS DATETIME(0)));

-- Rounding to next month
INSERT INTO t_boundary_rounding VALUES (4, 'next month', CAST('2024-01-31 23:59:59.999999' AS DATETIME(0)));

-- Rounding to next year
INSERT INTO t_boundary_rounding VALUES (5, 'next year', CAST('2024-12-31 23:59:59.999999' AS DATETIME(0)));

SELECT id, 
       description, 
       dt0,
       CASE id
           WHEN 1 THEN CASE WHEN dt0 = '2024-01-15 10:21:00' THEN 'PASS' ELSE 'FAIL' END
           WHEN 2 THEN CASE WHEN dt0 = '2024-01-15 11:00:00' THEN 'PASS' ELSE 'FAIL' END
           WHEN 3 THEN CASE WHEN dt0 = '2024-01-16 00:00:00' THEN 'PASS' ELSE 'FAIL' END
           WHEN 4 THEN CASE WHEN dt0 = '2024-02-01 00:00:00' THEN 'PASS' ELSE 'FAIL' END
           WHEN 5 THEN CASE WHEN dt0 = '2025-01-01 00:00:00' THEN 'PASS' ELSE 'FAIL' END
       END AS result
FROM t_boundary_rounding
ORDER BY id;

DROP TABLE t_boundary_rounding;

-- Test 4: All scales (0-6) with same input value
DROP TABLE IF EXISTS t_all_scales;
CREATE TABLE t_all_scales (
    scale_val INT,
    ts TIMESTAMP(6)
);

-- Input: .123456 microseconds
INSERT INTO t_all_scales VALUES (0, CAST('2024-01-15 10:20:30.123456' AS TIMESTAMP(0)));
INSERT INTO t_all_scales VALUES (1, CAST('2024-01-15 10:20:30.123456' AS TIMESTAMP(1)));
INSERT INTO t_all_scales VALUES (2, CAST('2024-01-15 10:20:30.123456' AS TIMESTAMP(2)));
INSERT INTO t_all_scales VALUES (3, CAST('2024-01-15 10:20:30.123456' AS TIMESTAMP(3)));
INSERT INTO t_all_scales VALUES (4, CAST('2024-01-15 10:20:30.123456' AS TIMESTAMP(4)));
INSERT INTO t_all_scales VALUES (5, CAST('2024-01-15 10:20:30.123456' AS TIMESTAMP(5)));
INSERT INTO t_all_scales VALUES (6, CAST('2024-01-15 10:20:30.123456' AS TIMESTAMP(6)));

SELECT scale_val,
       ts,
       EXTRACT(MICROSECOND FROM ts) AS microseconds,
       CASE scale_val
           WHEN 0 THEN CASE WHEN EXTRACT(MICROSECOND FROM ts) = 0 THEN 'PASS' ELSE 'FAIL' END
           WHEN 1 THEN CASE WHEN EXTRACT(MICROSECOND FROM ts) = 100000 THEN 'PASS' ELSE 'FAIL' END
           WHEN 2 THEN CASE WHEN EXTRACT(MICROSECOND FROM ts) = 120000 THEN 'PASS' ELSE 'FAIL' END
           WHEN 3 THEN CASE WHEN EXTRACT(MICROSECOND FROM ts) = 123000 THEN 'PASS' ELSE 'FAIL' END
           WHEN 4 THEN CASE WHEN EXTRACT(MICROSECOND FROM ts) = 123500 THEN 'PASS' ELSE 'FAIL' END
           WHEN 5 THEN CASE WHEN EXTRACT(MICROSECOND FROM ts) = 123460 THEN 'PASS' ELSE 'FAIL' END
           WHEN 6 THEN CASE WHEN EXTRACT(MICROSECOND FROM ts) = 123456 THEN 'PASS' ELSE 'FAIL' END
       END AS result
FROM t_all_scales
ORDER BY scale_val;

DROP TABLE t_all_scales;

-- Test 5: Verify display vs stored value consistency
DROP TABLE IF EXISTS t_display_vs_stored;
CREATE TABLE t_display_vs_stored (
    id INT,
    ts0 TIMESTAMP(0)
);

-- Insert value with microseconds
INSERT INTO t_display_vs_stored VALUES (1, CAST('2024-01-15 10:20:30.500000' AS TIMESTAMP(0)));

-- The WHERE clause should work with the displayed value
SELECT COUNT(*) AS match_count,
       CASE WHEN COUNT(*) = 1 THEN 'PASS' ELSE 'FAIL' END AS result
FROM t_display_vs_stored
WHERE ts0 = '2024-01-15 10:20:31';  -- Should match because .5 rounds up

DROP TABLE t_display_vs_stored;

DROP DATABASE test_precision_edge_cases;

