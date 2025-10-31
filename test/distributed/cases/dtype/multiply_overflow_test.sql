-- Test multiplication overflow

DROP TABLE IF EXISTS t_mul_test;
CREATE TABLE t_mul_test (
    id INT PRIMARY KEY,
    bigint_val BIGINT,
    int_val INT
);

-- Insert boundary values
INSERT INTO t_mul_test VALUES (1, 9223372036854775807, 2147483647);
INSERT INTO t_mul_test VALUES (2, 4611686018427387904, 1000000);
INSERT INTO t_mul_test VALUES (3, -9223372036854775808, -1);

-- Test 1: BIGINT * 2 (should error)
SELECT id, bigint_val, bigint_val * 2 AS result FROM t_mul_test WHERE id = 1;

-- Test 2: Large BIGINT multiplication (should error)
SELECT id, bigint_val, bigint_val * 2 AS result FROM t_mul_test WHERE id = 2;

-- Test 3: MIN_BIGINT * -1 (should error - special case)
SELECT id, bigint_val, bigint_val * -1 AS result FROM t_mul_test WHERE id = 3;

-- Test 4: Normal multiplication (should work)
SELECT id, bigint_val, bigint_val * 0 AS result FROM t_mul_test WHERE id = 1;
SELECT id, bigint_val, bigint_val * 1 AS result FROM t_mul_test WHERE id = 1;

-- Test 5: Two columns multiplication
DROP TABLE IF EXISTS t_mul_test2;
CREATE TABLE t_mul_test2 (
    id INT PRIMARY KEY,
    val1 BIGINT,
    val2 BIGINT
);

INSERT INTO t_mul_test2 VALUES (1, 9223372036854775807, 2);
INSERT INTO t_mul_test2 VALUES (2, 1000000, 1000000);

-- Should error
SELECT id, val1, val2, val1 * val2 AS result FROM t_mul_test2 WHERE id = 1;

-- Should error: 1000000 * 1000000 = 1000000000000 (overflow for INT but not BIGINT)
-- This actually won't overflow for BIGINT
SELECT id, val1, val2, val1 * val2 AS result FROM t_mul_test2 WHERE id = 2;

-- Cleanup
DROP TABLE t_mul_test;
DROP TABLE t_mul_test2;

