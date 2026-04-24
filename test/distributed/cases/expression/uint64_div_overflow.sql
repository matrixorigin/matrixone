-- @suit
-- @case
-- @desc:test for uint64 DIV overflow behavior (MySQL 8.0 compatibility)
-- @label:bvt

DROP DATABASE IF EXISTS test_uint_div;
CREATE DATABASE test_uint_div;
USE test_uint_div;

CREATE TABLE t1 (a BIGINT UNSIGNED, b BIGINT UNSIGNED);

-- Test 1: Normal uint64 DIV
INSERT INTO t1 VALUES (100, 2), (1000, 10);
SELECT a, b, a DIV b AS result FROM t1;

-- Test 2: MAX_INT64 boundary (should succeed)
TRUNCATE t1;
INSERT INTO t1 VALUES (9223372036854775807, 1);
SELECT a, b, a DIV b AS result FROM t1;

-- Test 3: Large value that fits in INT64
TRUNCATE t1;
INSERT INTO t1 VALUES (9223372036854775806, 2);
SELECT a, b, a DIV b AS result FROM t1;

-- Test 4: MAX_INT64 + 1 (should error)
TRUNCATE t1;
INSERT INTO t1 VALUES (9223372036854775808, 1);
-- @bvt:issue
SELECT a DIV b FROM t1;
-- @bvt:issue

-- Test 5: MAX_UINT64 (should error)
TRUNCATE t1;
INSERT INTO t1 VALUES (18446744073709551615, 1);
-- @bvt:issue
SELECT a DIV b FROM t1;
-- @bvt:issue

DROP TABLE t1;
DROP DATABASE test_uint_div;
