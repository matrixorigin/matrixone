-- Test overflow with actual table data (not constants)
-- This ensures type checking happens at the correct level

DROP TABLE IF EXISTS t_overflow_test;
CREATE TABLE t_overflow_test (
    id INT PRIMARY KEY,
    bigint_val BIGINT,
    int_val INT
);

-- Insert boundary values
INSERT INTO t_overflow_test VALUES (1, 9223372036854775807, 2147483647);
INSERT INTO t_overflow_test VALUES (2, -9223372036854775808, -2147483648);

-- Test 1: BIGINT + 1 (should error)
SELECT id, bigint_val, bigint_val + 1 AS result FROM t_overflow_test WHERE id = 1;

-- Test 2: BIGINT - 1 (should error)
SELECT id, bigint_val, bigint_val - 1 AS result FROM t_overflow_test WHERE id = 2;

-- Test 3: INT + 1 (should error)
SELECT id, int_val, int_val + 1 AS result FROM t_overflow_test WHERE id = 1;

-- Test 4: INT - 1 (should error)
SELECT id, int_val, int_val - 1 AS result FROM t_overflow_test WHERE id = 2;

-- Test 5: Normal operations (should work)
SELECT id, bigint_val, bigint_val + 0 AS result FROM t_overflow_test WHERE id = 1;
SELECT id, int_val, int_val + 10 AS result FROM t_overflow_test WHERE id = 1;

-- Test 6: Operations between two columns at boundary
DROP TABLE IF EXISTS t_overflow_test2;
CREATE TABLE t_overflow_test2 (
    id INT PRIMARY KEY,
    val1 BIGINT,
    val2 BIGINT
);

INSERT INTO t_overflow_test2 VALUES (1, 9223372036854775807, 1);
INSERT INTO t_overflow_test2 VALUES (2, -9223372036854775808, -1);

-- Test addition of two BIGINT columns (should error)
SELECT id, val1, val2, val1 + val2 AS result FROM t_overflow_test2 WHERE id = 1;

-- Test subtraction of two BIGINT columns (should error)
SELECT id, val1, val2, val1 - val2 AS result FROM t_overflow_test2 WHERE id = 2;

-- Cleanup
DROP TABLE t_overflow_test;
DROP TABLE t_overflow_test2;

