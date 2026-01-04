-- MySQL 8.0 Division by Zero Compatibility Test
-- Test division by zero behavior in different contexts and sql_mode settings

-- Setup
DROP TABLE IF EXISTS t_div_test;
CREATE TABLE t_div_test (
    id INT PRIMARY KEY,
    a INT,
    b INT,
    c DECIMAL(10,2),
    d DECIMAL(10,2),
    e FLOAT,
    f FLOAT
);

-- ============================================================================
-- Test 1: SELECT always returns NULL (regardless of sql_mode)
-- ============================================================================

-- Test with default sql_mode (includes STRICT_TRANS_TABLES and ERROR_FOR_DIVISION_BY_ZERO)
SELECT @@sql_mode;

-- Integer division by zero in SELECT
SELECT 10 / 0;
SELECT 10 DIV 0;
SELECT 10 % 0;

-- Integer DIV operator (now supported for all types)
SELECT 10 DIV 3;
SELECT CAST(10 AS DECIMAL(10,2)) DIV CAST(3 AS DECIMAL(10,2));

-- Decimal division by zero in SELECT
SELECT CAST(10.5 AS DECIMAL(10,2)) / CAST(0 AS DECIMAL(10,2));
SELECT CAST(10.5 AS DECIMAL(10,2)) % CAST(0 AS DECIMAL(10,2));
SELECT CAST(10.5 AS DECIMAL(10,2)) DIV CAST(0 AS DECIMAL(10,2));

-- Float division by zero in SELECT
SELECT 10.5 / 0.0;
SELECT 10.5 % 0.0;

-- Test with non-strict mode (no ERROR_FOR_DIVISION_BY_ZERO)
SET sql_mode = '';
SELECT @@sql_mode;

-- Should still return NULL in SELECT
SELECT 10 / 0;
SELECT 10 DIV 0;
SELECT 10 % 0;

-- Restore default mode
SET sql_mode = 'ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION,NO_ZERO_DATE,NO_ZERO_IN_DATE,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES';

-- ============================================================================
-- Test 2: INSERT with strict mode + ERROR_FOR_DIVISION_BY_ZERO (should error)
-- ============================================================================

-- Default mode includes both STRICT_TRANS_TABLES and ERROR_FOR_DIVISION_BY_ZERO
SELECT @@sql_mode;

-- These should fail with division by zero error
INSERT INTO t_div_test (id, a, b) VALUES (1, 10, 0), (2, 10/0, 5);

INSERT INTO t_div_test (id, a, b) VALUES (3, 10 % 0, 5);

-- Verify no rows were inserted
SELECT COUNT(*) FROM t_div_test;

-- ============================================================================
-- Test 3: INSERT with only ERROR_FOR_DIVISION_BY_ZERO (no strict mode)
-- ============================================================================

-- Remove strict mode, keep ERROR_FOR_DIVISION_BY_ZERO
SET sql_mode = 'ERROR_FOR_DIVISION_BY_ZERO';
SELECT @@sql_mode;

-- Should insert NULL (no error without strict mode)
INSERT INTO t_div_test (id, a, b) VALUES (1, 10/0, 5);
INSERT INTO t_div_test (id, a, b) VALUES (2, 10 % 0, 5);

-- Verify NULL was inserted
SELECT id, a, b FROM t_div_test ORDER BY id;

-- Cleanup
DELETE FROM t_div_test;

-- ============================================================================
-- Test 4: INSERT with no ERROR_FOR_DIVISION_BY_ZERO (should insert NULL)
-- ============================================================================

SET sql_mode = '';
SELECT @@sql_mode;

-- Should insert NULL without error
INSERT INTO t_div_test (id, a, b) VALUES (1, 10/0, 5);
INSERT INTO t_div_test (id, a, b) VALUES (2, 10 % 0, 5);

-- Verify NULL was inserted
SELECT id, a, b FROM t_div_test ORDER BY id;

-- Cleanup
DELETE FROM t_div_test;

-- ============================================================================
-- Test 5: INSERT IGNORE (should always insert NULL, never error)
-- ============================================================================

-- Restore strict mode
SET sql_mode = 'ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION,NO_ZERO_DATE,NO_ZERO_IN_DATE,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES';
SELECT @@sql_mode;

-- INSERT IGNORE should insert NULL even in strict mode
INSERT IGNORE INTO t_div_test (id, a, b) VALUES (1, 10/0, 5);
INSERT IGNORE INTO t_div_test (id, a, b) VALUES (2, 10 % 0, 5);

-- Verify NULL was inserted
SELECT id, a, b FROM t_div_test ORDER BY id;

-- Cleanup
DELETE FROM t_div_test;

-- ============================================================================
-- Test 6: UPDATE with strict mode + ERROR_FOR_DIVISION_BY_ZERO
-- ============================================================================

-- Insert test data
INSERT INTO t_div_test (id, a, b) VALUES (1, 10, 5), (2, 20, 10);

-- Should fail with division by zero error
UPDATE t_div_test SET a = 10/0 WHERE id = 1;

UPDATE t_div_test SET a = 10 % 0 WHERE id = 1;

-- Verify no updates occurred
SELECT id, a, b FROM t_div_test ORDER BY id;

-- ============================================================================
-- Test 7: UPDATE without strict mode (should update to NULL)
-- ============================================================================

SET sql_mode = '';
SELECT @@sql_mode;

-- Should update to NULL
UPDATE t_div_test SET a = 10/0 WHERE id = 1;
SELECT id, a, b FROM t_div_test WHERE id = 1;

UPDATE t_div_test SET a = 10 % 0 WHERE id = 2;
SELECT id, a, b FROM t_div_test WHERE id = 2;

-- Cleanup
DELETE FROM t_div_test;

-- ============================================================================
-- Test 8: Decimal types
-- ============================================================================

-- Restore strict mode
SET sql_mode = 'ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION,NO_ZERO_DATE,NO_ZERO_IN_DATE,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES';

-- SELECT should return NULL
SELECT CAST(10.5 AS DECIMAL(10,2)) / CAST(0 AS DECIMAL(10,2));
SELECT CAST(10.5 AS DECIMAL(10,2)) % CAST(0 AS DECIMAL(10,2));

-- INSERT should fail in strict mode
INSERT INTO t_div_test (id, c, d) VALUES (1, CAST(10.5 AS DECIMAL(10,2)) / CAST(0 AS DECIMAL(10,2)), 5.0);

-- INSERT should succeed without strict mode
SET sql_mode = '';
INSERT INTO t_div_test (id, c, d) VALUES (1, CAST(10.5 AS DECIMAL(10,2)) / CAST(0 AS DECIMAL(10,2)), 5.0);
SELECT id, c, d FROM t_div_test WHERE id = 1;

DELETE FROM t_div_test;

-- ============================================================================
-- Test 9: Float types
-- ============================================================================

-- Restore strict mode
SET sql_mode = 'ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION,NO_ZERO_DATE,NO_ZERO_IN_DATE,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES';

-- SELECT should return NULL
SELECT 10.5 / 0.0;
SELECT 10.5 % 0.0;

-- INSERT should fail in strict mode
INSERT INTO t_div_test (id, e, f) VALUES (1, 10.5 / 0.0, 5.0);

-- INSERT should succeed without strict mode
SET sql_mode = '';
INSERT INTO t_div_test (id, e, f) VALUES (1, 10.5 / 0.0, 5.0);
SELECT id, e, f FROM t_div_test WHERE id = 1;

DELETE FROM t_div_test;

-- ============================================================================
-- Test 10: Complex expressions
-- ============================================================================

-- Restore strict mode
SET sql_mode = 'ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION,NO_ZERO_DATE,NO_ZERO_IN_DATE,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES';

-- SELECT with complex expression (should return NULL)
SELECT (10 + 20) / (5 - 5);
SELECT CASE WHEN 1=1 THEN 10/0 ELSE 5 END;

-- INSERT test data for WHERE clause test
INSERT INTO t_div_test (id, a, b) VALUES (1, 10, 0), (2, 20, 5);

-- SELECT with division by zero in WHERE clause (should filter out rows)
SELECT id, a, b FROM t_div_test WHERE a / b > 0;

-- Cleanup
DROP TABLE t_div_test;

-- ============================================================================
-- Test 11: All integer types with DIV operator
-- ============================================================================

-- TINYINT
SELECT CAST(10 AS TINYINT) DIV CAST(3 AS TINYINT);
SELECT CAST(127 AS TINYINT) DIV CAST(1 AS TINYINT);
SELECT CAST(-128 AS TINYINT) DIV CAST(1 AS TINYINT);
SELECT CAST(10 AS TINYINT) DIV CAST(0 AS TINYINT);

-- SMALLINT
SELECT CAST(1000 AS SMALLINT) DIV CAST(3 AS SMALLINT);
SELECT CAST(32767 AS SMALLINT) DIV CAST(1 AS SMALLINT);
SELECT CAST(-32768 AS SMALLINT) DIV CAST(1 AS SMALLINT);
SELECT CAST(100 AS SMALLINT) DIV CAST(0 AS SMALLINT);

-- INT
SELECT CAST(100000 AS INT) DIV CAST(3 AS INT);
SELECT CAST(2147483647 AS INT) DIV CAST(1 AS INT);
SELECT CAST(-2147483648 AS INT) DIV CAST(1 AS INT);
SELECT CAST(100 AS INT) DIV CAST(0 AS INT);

-- BIGINT
SELECT CAST(10000000000 AS BIGINT) DIV CAST(3 AS BIGINT);
SELECT CAST(9223372036854775807 AS BIGINT) DIV CAST(1 AS BIGINT);
SELECT CAST(-9223372036854775808 AS BIGINT) DIV CAST(1 AS BIGINT);
SELECT CAST(100 AS BIGINT) DIV CAST(0 AS BIGINT);

-- ============================================================================
-- Test 12: All unsigned integer types with DIV operator
-- ============================================================================

-- TINYINT UNSIGNED
SELECT CAST(10 AS TINYINT UNSIGNED) DIV CAST(3 AS TINYINT UNSIGNED);
SELECT CAST(255 AS TINYINT UNSIGNED) DIV CAST(1 AS TINYINT UNSIGNED);
SELECT CAST(10 AS TINYINT UNSIGNED) DIV CAST(0 AS TINYINT UNSIGNED);

-- SMALLINT UNSIGNED
SELECT CAST(1000 AS SMALLINT UNSIGNED) DIV CAST(3 AS SMALLINT UNSIGNED);
SELECT CAST(65535 AS SMALLINT UNSIGNED) DIV CAST(1 AS SMALLINT UNSIGNED);
SELECT CAST(100 AS SMALLINT UNSIGNED) DIV CAST(0 AS SMALLINT UNSIGNED);

-- INT UNSIGNED
SELECT CAST(100000 AS INT UNSIGNED) DIV CAST(3 AS INT UNSIGNED);
SELECT CAST(4294967295 AS INT UNSIGNED) DIV CAST(1 AS INT UNSIGNED);
SELECT CAST(100 AS INT UNSIGNED) DIV CAST(0 AS INT UNSIGNED);

-- BIGINT UNSIGNED
SELECT CAST(10000000000 AS BIGINT UNSIGNED) DIV CAST(3 AS BIGINT UNSIGNED);
SELECT CAST(18446744073709551615 AS BIGINT UNSIGNED) DIV CAST(1 AS BIGINT UNSIGNED);
SELECT CAST(100 AS BIGINT UNSIGNED) DIV CAST(0 AS BIGINT UNSIGNED);

-- ============================================================================
-- Test 13: Float types with DIV operator
-- ============================================================================

-- FLOAT
SELECT CAST(10.5 AS FLOAT) DIV CAST(3.2 AS FLOAT);
SELECT CAST(-10.5 AS FLOAT) DIV CAST(3.2 AS FLOAT);
SELECT CAST(10.5 AS FLOAT) DIV CAST(-3.2 AS FLOAT);
SELECT CAST(-10.5 AS FLOAT) DIV CAST(-3.2 AS FLOAT);
SELECT CAST(10.5 AS FLOAT) DIV CAST(0.0 AS FLOAT);

-- DOUBLE
SELECT CAST(10.5 AS DOUBLE) DIV CAST(3.2 AS DOUBLE);
SELECT CAST(-10.5 AS DOUBLE) DIV CAST(3.2 AS DOUBLE);
SELECT CAST(10.5 AS DOUBLE) DIV CAST(-3.2 AS DOUBLE);
SELECT CAST(-10.5 AS DOUBLE) DIV CAST(-3.2 AS DOUBLE);
SELECT CAST(10.5 AS DOUBLE) DIV CAST(0.0 AS DOUBLE);

-- ============================================================================
-- Test 14: Decimal types with various scales
-- ============================================================================

-- DECIMAL(5,0)
SELECT CAST(10 AS DECIMAL(5,0)) DIV CAST(3 AS DECIMAL(5,0));
SELECT CAST(99999 AS DECIMAL(5,0)) DIV CAST(1 AS DECIMAL(5,0));
SELECT CAST(10 AS DECIMAL(5,0)) DIV CAST(0 AS DECIMAL(5,0));

-- DECIMAL(10,2)
SELECT CAST(10.50 AS DECIMAL(10,2)) DIV CAST(3.00 AS DECIMAL(10,2));
SELECT CAST(-10.50 AS DECIMAL(10,2)) DIV CAST(3.00 AS DECIMAL(10,2));
SELECT CAST(10.50 AS DECIMAL(10,2)) DIV CAST(-3.00 AS DECIMAL(10,2));
SELECT CAST(10.50 AS DECIMAL(10,2)) DIV CAST(0.00 AS DECIMAL(10,2));

-- DECIMAL(20,5)
SELECT CAST(12345.67890 AS DECIMAL(20,5)) DIV CAST(3.00000 AS DECIMAL(20,5));
SELECT CAST(12345.67890 AS DECIMAL(20,5)) DIV CAST(0.00000 AS DECIMAL(20,5));

-- DECIMAL(38,10) - maximum precision
SELECT CAST(123456789.123456789 AS DECIMAL(38,10)) DIV CAST(3.0 AS DECIMAL(38,10));
SELECT CAST(123456789.123456789 AS DECIMAL(38,10)) DIV CAST(0.0 AS DECIMAL(38,10));

-- ============================================================================
-- Test 15: NULL handling
-- ============================================================================

SELECT NULL DIV 3;
SELECT 10 DIV NULL;
SELECT NULL DIV NULL;
SELECT NULL / 3;
SELECT 10 / NULL;
SELECT NULL % 3;
SELECT 10 % NULL;

-- ============================================================================
-- Test 16: Negative numbers
-- ============================================================================

SELECT -10 DIV 3;
SELECT 10 DIV -3;
SELECT -10 DIV -3;
SELECT -10 / 3;
SELECT 10 / -3;
SELECT -10 / -3;
SELECT -10 % 3;
SELECT 10 % -3;
SELECT -10 % -3;

-- ============================================================================
-- Test 17: Edge cases with 1 and -1
-- ============================================================================

SELECT 100 DIV 1;
SELECT 100 DIV -1;
SELECT -100 DIV 1;
SELECT -100 DIV -1;
SELECT 1 DIV 1;
SELECT -1 DIV -1;

-- Restore default sql_mode
SET sql_mode = 'ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION,NO_ZERO_DATE,NO_ZERO_IN_DATE,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES';
