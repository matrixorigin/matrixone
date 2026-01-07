-- Test index usage with type casting optimization
-- Verifies that columns are not cast when comparing with constants to preserve index usage

DROP TABLE IF EXISTS t_int32;
DROP TABLE IF EXISTS t_float32;
DROP TABLE IF EXISTS t_float64;
DROP TABLE IF EXISTS t_decimal;

-- Test 1: INT32 column with index
CREATE TABLE t_int32 (
    id INT PRIMARY KEY,
    val INT,
    KEY idx_val (val)
);

INSERT INTO t_int32 VALUES (1, 9), (2, 10), (3, 100);

-- Should use index: INT32 = INT64 constant
-- @separator:table
EXPLAIN SELECT * FROM t_int32 WHERE val = 9;

-- Should use index: INT32 = DECIMAL with zero fractional part (9.0)
-- Note: Currently may not optimize due to complexity, but should not break
-- @separator:table
EXPLAIN SELECT * FROM t_int32 WHERE val = 9.0;

-- Should NOT use index: INT32 = DECIMAL with non-zero fractional part
-- This is correct behavior to preserve semantics
-- @separator:table
EXPLAIN SELECT * FROM t_int32 WHERE val = 9.5;

-- Verify results
SELECT * FROM t_int32 WHERE val = 9;
SELECT * FROM t_int32 WHERE val = 9.0;
SELECT * FROM t_int32 WHERE val = 9.5;

-- Test 2: FLOAT32 column with index
CREATE TABLE t_float32 (
    id INT PRIMARY KEY,
    val FLOAT,
    KEY idx_val (val)
);

INSERT INTO t_float32 VALUES (1, 9.0), (2, 9.5), (3, 10.0);

-- Should NOT have cast on column: FLOAT32 = DECIMAL
-- @separator:table
EXPLAIN SELECT * FROM t_float32 WHERE val = 9.0;

-- Should NOT have cast on column: FLOAT32 = INT
-- @separator:table
EXPLAIN SELECT * FROM t_float32 WHERE val = 9;

-- Should NOT have cast on column: FLOAT32 >= INT
-- @separator:table
EXPLAIN SELECT * FROM t_float32 WHERE val >= 9;

-- Verify results
SELECT * FROM t_float32 WHERE val = 9.0;
SELECT * FROM t_float32 WHERE val = 9;
SELECT * FROM t_float32 WHERE val >= 9 ORDER BY id;

-- Test 3: FLOAT64 (DOUBLE) column with index
CREATE TABLE t_float64 (
    id INT PRIMARY KEY,
    val DOUBLE,
    KEY idx_val (val)
);

INSERT INTO t_float64 VALUES (1, 9.0), (2, 9.5), (3, 10.0);

-- Should NOT have cast on column: FLOAT64 = DECIMAL
-- @separator:table
EXPLAIN SELECT * FROM t_float64 WHERE val = 9.0;

-- Should NOT have cast on column: FLOAT64 = INT
-- @separator:table
EXPLAIN SELECT * FROM t_float64 WHERE val = 9;

-- Verify results
SELECT * FROM t_float64 WHERE val = 9.0;
SELECT * FROM t_float64 WHERE val = 9;

-- Test 4: DECIMAL column with index
CREATE TABLE t_decimal (
    id INT PRIMARY KEY,
    val DECIMAL(10,2),
    KEY idx_val (val)
);

INSERT INTO t_decimal VALUES (1, 9.00), (2, 9.50), (3, 10.00);

-- Should NOT have cast on column: DECIMAL = DECIMAL
-- @separator:table
EXPLAIN SELECT * FROM t_decimal WHERE val = 9.0;

-- Verify results
SELECT * FROM t_decimal WHERE val = 9.0;
SELECT * FROM t_decimal WHERE val = 9.5;

-- Test 5: Mixed comparisons
-- Test that optimization works for all comparison operators
-- @separator:table
EXPLAIN SELECT * FROM t_float32 WHERE val < 10;
-- @separator:table
EXPLAIN SELECT * FROM t_float32 WHERE val <= 10;
-- @separator:table
EXPLAIN SELECT * FROM t_float32 WHERE val > 9;
-- @separator:table
EXPLAIN SELECT * FROM t_float32 WHERE val >= 9;
-- @separator:table
EXPLAIN SELECT * FROM t_float32 WHERE val <> 9;

-- Verify results
SELECT COUNT(*) FROM t_float32 WHERE val < 10;
SELECT COUNT(*) FROM t_float32 WHERE val <= 10;
SELECT COUNT(*) FROM t_float32 WHERE val > 9;
SELECT COUNT(*) FROM t_float32 WHERE val >= 9;
SELECT COUNT(*) FROM t_float32 WHERE val <> 9;

-- Test 6: Column compared with expressions/functions
-- Verify optimization works for non-constant expressions too

-- FLOAT = CAST expression
-- @separator:table
EXPLAIN SELECT * FROM t_float32 WHERE val = CAST(9.0 AS DECIMAL);

-- FLOAT = arithmetic expression
-- @separator:table
EXPLAIN SELECT * FROM t_float32 WHERE val = 8 + 1;

-- FLOAT = function result
-- @separator:table
EXPLAIN SELECT * FROM t_float32 WHERE val = ABS(-9);

-- Verify results
SELECT * FROM t_float32 WHERE val = CAST(9.0 AS DECIMAL);
SELECT * FROM t_float32 WHERE val = 8 + 1;
SELECT * FROM t_float32 WHERE val = ABS(-9);

-- Cleanup
DROP TABLE t_int32;
DROP TABLE t_float32;
DROP TABLE t_float64;
DROP TABLE t_decimal;
