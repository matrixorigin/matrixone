-- Test division by zero behavior in strict mode (MySQL 8.0)
-- According to MySQL 8.0:
-- 1. SELECT: always returns NULL (never errors)
-- 2. INSERT/UPDATE with strict mode + ERROR_FOR_DIVISION_BY_ZERO: should error
-- 3. INSERT IGNORE: always returns NULL (never errors, even in strict mode)
--
-- NOTE: Tests 3, 5, 7, 8 are marked with @bvt:issue#23438 because mo-tester uses
-- prepared statements (useServerPrepStmts=true), and prepared statements
-- don't correctly track the actual statement type (INSERT/UPDATE) for
-- division by zero error checking. See issue #23438 for details.

-- Setup
DROP TABLE IF EXISTS t_strict_div;
CREATE TABLE t_strict_div (id INT, val INT);

-- Test 1: Non-strict mode - INSERT should succeed with NULL
SET sql_mode = '';
INSERT INTO t_strict_div VALUES (1, 10/0);
INSERT INTO t_strict_div VALUES (2, 10 DIV 0);
INSERT INTO t_strict_div VALUES (3, 10 % 0);
SELECT * FROM t_strict_div ORDER BY id;

-- Test 2: Strict mode without ERROR_FOR_DIVISION_BY_ZERO - should succeed with NULL
TRUNCATE TABLE t_strict_div;
SET sql_mode = 'STRICT_TRANS_TABLES';
INSERT INTO t_strict_div VALUES (1, 10/0);
INSERT INTO t_strict_div VALUES (2, 10 DIV 0);
INSERT INTO t_strict_div VALUES (3, 10 % 0);
SELECT * FROM t_strict_div ORDER BY id;

-- Test 3: Strict mode + ERROR_FOR_DIVISION_BY_ZERO - INSERT should ERROR
-- @bvt:issue#23438
TRUNCATE TABLE t_strict_div;
SET sql_mode = 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO';
-- These should fail with division by zero error
INSERT INTO t_strict_div VALUES (1, 10/0);
INSERT INTO t_strict_div VALUES (2, 10 DIV 0);
INSERT INTO t_strict_div VALUES (3, 10 % 0);
-- Table should be empty (all inserts failed)
SELECT COUNT(*) FROM t_strict_div;
-- @bvt:issue

-- Test 4: INSERT IGNORE in strict mode - should succeed with NULL (no error)
SET sql_mode = 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO';
INSERT IGNORE INTO t_strict_div VALUES (1, 10/0);
INSERT IGNORE INTO t_strict_div VALUES (2, 10 DIV 0);
INSERT IGNORE INTO t_strict_div VALUES (3, 10 % 0);
SELECT * FROM t_strict_div ORDER BY id;

-- Test 5: UPDATE in strict mode - should ERROR
-- @bvt:issue#23438
TRUNCATE TABLE t_strict_div;
INSERT INTO t_strict_div VALUES (1, 100), (2, 200), (3, 300);
SET sql_mode = 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO';
-- This should fail
UPDATE t_strict_div SET val = 10/0 WHERE id = 1;
-- Original values should remain (update failed)
SELECT * FROM t_strict_div ORDER BY id;
-- @bvt:issue

-- Test 6: SELECT always returns NULL (even in strict mode)
SET sql_mode = 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO';
SELECT 10/0 AS div_result;
SELECT 10 DIV 0 AS intdiv_result;
SELECT 10 % 0 AS mod_result;

-- Test 7: Multiple statements - cache should reset per statement
-- @bvt:issue#23438
SET sql_mode = 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO';
-- First: INSERT should error
INSERT INTO t_strict_div VALUES (10, 10/0);
-- Second: SELECT should return NULL (not error, even though previous was INSERT)
SELECT 10/0 AS should_be_null;
-- Third: INSERT should error again (cache should not leak from SELECT)
INSERT INTO t_strict_div VALUES (11, 10/0);
-- @bvt:issue

-- Test 8: Different sql_mode values in same session
-- @bvt:issue#23438
SET sql_mode = 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO';
INSERT INTO t_strict_div VALUES (20, 10/0);  -- Should error
SET sql_mode = '';
INSERT INTO t_strict_div VALUES (20, 10/0);  -- Should succeed with NULL
SELECT * FROM t_strict_div WHERE id = 20;
-- @bvt:issue

-- Cleanup
DROP TABLE t_strict_div;
SET sql_mode = DEFAULT;
