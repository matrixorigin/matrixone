-- @suite
-- @case
-- @desc: Test for NULL-safe equal operator <=>
-- @label: bvt

CREATE DATABASE IF NOT EXISTS test_null_safe;
USE test_null_safe;

-- 1. Basic Scalar Tests
SELECT 1 <=> 1;
SELECT 1 <=> 0;
SELECT 1 <=> NULL;
SELECT NULL <=> 1;
SELECT NULL <=> NULL;
SELECT 'a' <=> 'a';
SELECT 'a' <=> 'b';
SELECT 'a' <=> NULL;
SELECT NULL <=> 'a';

-- 2. Table Tests
DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (
    id INT,
    val INT
);

INSERT INTO t1 VALUES (1, 1), (2, 0), (3, NULL);

SELECT id, val, val <=> 1 AS eq_1, val <=> 0 AS eq_0, val <=> NULL AS eq_null FROM t1 ORDER BY id;

-- 3. String Table Tests
DROP TABLE IF EXISTS t2;
CREATE TABLE t2 (
    id INT,
    val VARCHAR(20)
);

INSERT INTO t2 VALUES (1, 'a'), (2, 'b'), (3, NULL);

SELECT id, val, val <=> 'a' AS eq_a, val <=> 'b' AS eq_b, val <=> NULL AS eq_null FROM t2 ORDER BY id;

-- 4. Join Tests
DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_right;

CREATE TABLE t_left (id INT, val INT);
CREATE TABLE t_right (id INT, val INT);

INSERT INTO t_left VALUES (1, NULL), (2, 0), (3, 10);
INSERT INTO t_right VALUES (1, NULL), (2, 0), (3, 10);

-- Inner Join with <=>
SELECT l.id as lid, l.val as lval, r.id as rid, r.val as rval
FROM t_left l JOIN t_right r ON l.val <=> r.val
ORDER BY l.id, r.id;

-- Explain to verify it does NOT use Hash Join on <=> condition (it might use CP/BNL)
-- or if it uses Hash Join, ensure it's not treating <=> as the equi-join key directly
-- (Current implementation disables equi-join recognition for <=>)
-- EXPLAIN SELECT l.id, r.id FROM t_left l JOIN t_right r ON l.val <=> r.val;

-- 5. JSON Join Tests
DROP TABLE IF EXISTS t_json_left;
DROP TABLE IF EXISTS t_json_right;

CREATE TABLE t_json_left (id INT, j1 JSON, j2 JSON);
CREATE TABLE t_json_right (id INT, j1 JSON, j2 JSON);

INSERT INTO t_json_left VALUES
    (1, CAST('{"a":1,"b":{"c":2}}' AS JSON), CAST('[1,2,3]' AS JSON)),
    (2, CAST('{"a":1}' AS JSON), CAST('[1,2,3]' AS JSON)),
    (3, NULL, CAST('[1,2,3]' AS JSON)),
    (4, CAST('{"a":1,"b":{"c":2}}' AS JSON), NULL);

INSERT INTO t_json_right VALUES
    (10, CAST('{"a":1,"b":{"c":2}}' AS JSON), CAST('[1,2,3]' AS JSON)),
    (20, CAST('{"a":1}' AS JSON), CAST('[1,2,3]' AS JSON)),
    (30, NULL, CAST('[1,2,3]' AS JSON)),
    (40, CAST('{"a":1,"b":{"c":2}}' AS JSON), NULL);

-- Multi-column JSON join with <=>
SELECT l.id AS lid, r.id AS rid
FROM t_json_left l JOIN t_json_right r
ON l.j1 <=> r.j1 AND l.j2 <=> r.j2
ORDER BY l.id, r.id;

-- Tuple JSON join with <=>
SELECT l.id AS lid, r.id AS rid
FROM t_json_left l JOIN t_json_right r
ON (l.j1, l.j2) <=> (r.j1, r.j2)
ORDER BY l.id, r.id;

-- 6. Type mixing
SELECT 1.0 <=> 1;
SELECT 1.0 <=> 1.1;
SELECT 1.0 <=> NULL;
SELECT 1 <=> '1';
SELECT '1' <=> 1;

-- 7. Decimal
SELECT CAST(1.10 AS DECIMAL(10,2)) <=> CAST(1.1 AS DECIMAL(10,1)) AS dec_eq_1;
SELECT CAST(1.10 AS DECIMAL(10,2)) <=> CAST(1.11 AS DECIMAL(10,2)) AS dec_eq_2;
SELECT CAST(NULL AS DECIMAL(10,2)) <=> CAST(NULL AS DECIMAL(10,2)) AS dec_eq_3;
SELECT CAST(1.00 AS DECIMAL(10,2)) <=> CAST(NULL AS DECIMAL(10,2)) AS dec_eq_4;

-- 8. Date/Time
SELECT CAST('2024-01-01' AS DATE) <=> CAST('2024-01-01' AS DATE) AS date_eq_1;
SELECT CAST('2024-01-01' AS DATE) <=> CAST('2024-01-02' AS DATE) AS date_eq_2;
SELECT CAST('2024-01-01' AS DATE) <=> NULL AS date_eq_3;
SELECT CAST('12:34:56' AS TIME) <=> CAST('12:34:56' AS TIME) AS time_eq_1;
SELECT CAST('12:34:56' AS TIME) <=> CAST('12:34:57' AS TIME) AS time_eq_2;
SELECT CAST('2024-01-01 00:00:00' AS TIMESTAMP) <=> CAST('2024-01-01 00:00:00' AS TIMESTAMP) AS ts_eq_1;
SELECT CAST('2024-01-01 00:00:00' AS TIMESTAMP) <=> NULL AS ts_eq_2;

-- 9. Boolean
SELECT TRUE <=> TRUE;
SELECT TRUE <=> FALSE;
SELECT TRUE <=> NULL;
SELECT FALSE <=> NULL;

-- 10. Tuple Tests
SELECT (1, NULL) <=> (1, NULL);
SELECT (1, 1) <=> (1, NULL);
SELECT (1, 2) <=> (1, 3);

-- 11. NotNullable Optimization Test
-- Since <=> is marked as PRODUCE_NO_NULL, IS NULL should always be false (result 0)
SELECT (val <=> NULL) IS NULL FROM t1 ORDER BY id;

-- 12. WHERE clause and Constant Folding
-- Should return 1 row
SELECT 1 as result WHERE NULL <=> NULL;
-- Should return 0 rows
SELECT 1 as result WHERE 1 <=> NULL;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_right;
DROP TABLE IF EXISTS t_json_left;
DROP TABLE IF EXISTS t_json_right;
DROP DATABASE test_null_safe;
