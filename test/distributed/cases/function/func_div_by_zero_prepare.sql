-- Test division by zero behavior with PREPARE/EXECUTE statements (MySQL 8.0)
-- According to MySQL 8.0:
-- 1. SELECT: always returns NULL (never errors)
-- 2. INSERT/UPDATE with strict mode + ERROR_FOR_DIVISION_BY_ZERO: should error
-- 3. INSERT IGNORE: always returns NULL (never errors, even in strict mode)
--
-- This file specifically tests PREPARE INSERT, PREPARE INSERT IGNORE, and EXECUTE.
-- See issue #23438 for known issues with prepared statements and division by zero.

-- Setup
DROP TABLE IF EXISTS t_prepare_div;
CREATE TABLE t_prepare_div (id INT PRIMARY KEY, a INT, b INT, c DECIMAL(10,2), e FLOAT);

-- ============================================================================
-- Test 1: PREPARE INSERT with non-strict mode - should succeed with NULL
-- ============================================================================
SET sql_mode = '';
SELECT @@sql_mode;

PREPARE p_ins1 FROM 'INSERT INTO t_prepare_div (id, a) VALUES (?, 10/0)';
SET @id = 1;
EXECUTE p_ins1 USING @id;

PREPARE p_ins2 FROM 'INSERT INTO t_prepare_div (id, a) VALUES (?, 10 DIV 0)';
SET @id = 2;
EXECUTE p_ins2 USING @id;

PREPARE p_ins3 FROM 'INSERT INTO t_prepare_div (id, a) VALUES (?, 10 % 0)';
SET @id = 3;
EXECUTE p_ins3 USING @id;

SELECT id, a FROM t_prepare_div ORDER BY id;
TRUNCATE TABLE t_prepare_div;
DEALLOCATE PREPARE p_ins1;
DEALLOCATE PREPARE p_ins2;
DEALLOCATE PREPARE p_ins3;

-- ============================================================================
-- Test 2: PREPARE INSERT with STRICT_TRANS_TABLES (no ERROR_FOR_DIVISION_BY_ZERO)
-- Should succeed with NULL
-- ============================================================================
SET sql_mode = 'STRICT_TRANS_TABLES';
SELECT @@sql_mode;

PREPARE p_ins1 FROM 'INSERT INTO t_prepare_div (id, a) VALUES (?, 10/0)';
SET @id = 1;
EXECUTE p_ins1 USING @id;

PREPARE p_ins2 FROM 'INSERT INTO t_prepare_div (id, a) VALUES (?, 10 DIV 0)';
SET @id = 2;
EXECUTE p_ins2 USING @id;

PREPARE p_ins3 FROM 'INSERT INTO t_prepare_div (id, a) VALUES (?, 10 % 0)';
SET @id = 3;
EXECUTE p_ins3 USING @id;

SELECT id, a FROM t_prepare_div ORDER BY id;
TRUNCATE TABLE t_prepare_div;
DEALLOCATE PREPARE p_ins1;
DEALLOCATE PREPARE p_ins2;
DEALLOCATE PREPARE p_ins3;

-- ============================================================================
-- Test 3: PREPARE INSERT with STRICT_TRANS_TABLES + ERROR_FOR_DIVISION_BY_ZERO
-- Should ERROR
-- ============================================================================
SET sql_mode = 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO';
SELECT @@sql_mode;

PREPARE p_ins1 FROM 'INSERT INTO t_prepare_div (id, a) VALUES (?, 10/0)';
SET @id = 1;
EXECUTE p_ins1 USING @id;

PREPARE p_ins2 FROM 'INSERT INTO t_prepare_div (id, a) VALUES (?, 10 DIV 0)';
SET @id = 2;
EXECUTE p_ins2 USING @id;

PREPARE p_ins3 FROM 'INSERT INTO t_prepare_div (id, a) VALUES (?, 10 % 0)';
SET @id = 3;
EXECUTE p_ins3 USING @id;

-- Table should be empty (all inserts failed)
SELECT COUNT(*) FROM t_prepare_div;
DEALLOCATE PREPARE p_ins1;
DEALLOCATE PREPARE p_ins2;
DEALLOCATE PREPARE p_ins3;

-- ============================================================================
-- Test 4: PREPARE INSERT IGNORE in strict mode
-- Should succeed with NULL (no error, even in strict mode)
-- ============================================================================
SET sql_mode = 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO';
SELECT @@sql_mode;

PREPARE p_ign1 FROM 'INSERT IGNORE INTO t_prepare_div (id, a) VALUES (?, 10/0)';
SET @id = 1;
EXECUTE p_ign1 USING @id;

PREPARE p_ign2 FROM 'INSERT IGNORE INTO t_prepare_div (id, a) VALUES (?, 10 DIV 0)';
SET @id = 2;
EXECUTE p_ign2 USING @id;

PREPARE p_ign3 FROM 'INSERT IGNORE INTO t_prepare_div (id, a) VALUES (?, 10 % 0)';
SET @id = 3;
EXECUTE p_ign3 USING @id;

SELECT id, a FROM t_prepare_div ORDER BY id;
TRUNCATE TABLE t_prepare_div;
DEALLOCATE PREPARE p_ign1;
DEALLOCATE PREPARE p_ign2;
DEALLOCATE PREPARE p_ign3;

-- ============================================================================
-- Test 5: PREPARE INSERT with parameterized divisor (zero passed via parameter)
-- ============================================================================
SET sql_mode = '';

PREPARE p_param FROM 'INSERT INTO t_prepare_div (id, a) VALUES (?, 10/?)';
SET @id = 1, @divisor = 0;
EXECUTE p_param USING @id, @divisor;
SET @id = 2, @divisor = 2;
EXECUTE p_param USING @id, @divisor;

SELECT id, a FROM t_prepare_div ORDER BY id;
TRUNCATE TABLE t_prepare_div;
DEALLOCATE PREPARE p_param;

-- ============================================================================
-- Test 6: PREPARE INSERT with parameterized divisor in strict mode
-- Should ERROR when divisor is 0
-- ============================================================================
SET sql_mode = 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO';

PREPARE p_param FROM 'INSERT INTO t_prepare_div (id, a) VALUES (?, 10/?)';
SET @id = 1, @divisor = 0;
EXECUTE p_param USING @id, @divisor;
-- Non-zero divisor should succeed
SET @id = 2, @divisor = 2;
EXECUTE p_param USING @id, @divisor;

SELECT id, a FROM t_prepare_div ORDER BY id;
TRUNCATE TABLE t_prepare_div;
DEALLOCATE PREPARE p_param;

-- ============================================================================
-- Test 7: PREPARE INSERT IGNORE with parameterized divisor in strict mode
-- Should succeed with NULL even when divisor is 0
-- ============================================================================
SET sql_mode = 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO';

PREPARE p_ign_param FROM 'INSERT IGNORE INTO t_prepare_div (id, a) VALUES (?, 10/?)';
SET @id = 1, @divisor = 0;
EXECUTE p_ign_param USING @id, @divisor;
SET @id = 2, @divisor = 0;
EXECUTE p_ign_param USING @id, @divisor;
SET @id = 3, @divisor = 5;
EXECUTE p_ign_param USING @id, @divisor;

SELECT id, a FROM t_prepare_div ORDER BY id;
TRUNCATE TABLE t_prepare_div;
DEALLOCATE PREPARE p_ign_param;

-- ============================================================================
-- Test 8: PREPARE with DECIMAL division by zero
-- ============================================================================
SET sql_mode = '';

PREPARE p_dec FROM 'INSERT INTO t_prepare_div (id, c) VALUES (?, CAST(10.5 AS DECIMAL(10,2)) / CAST(0 AS DECIMAL(10,2)))';
SET @id = 1;
EXECUTE p_dec USING @id;
SELECT id, c FROM t_prepare_div ORDER BY id;
TRUNCATE TABLE t_prepare_div;
DEALLOCATE PREPARE p_dec;

-- DECIMAL division in strict mode - should error
SET sql_mode = 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO';
PREPARE p_dec FROM 'INSERT INTO t_prepare_div (id, c) VALUES (?, CAST(10.5 AS DECIMAL(10,2)) / CAST(0 AS DECIMAL(10,2)))';
SET @id = 1;
EXECUTE p_dec USING @id;
SELECT COUNT(*) FROM t_prepare_div;
DEALLOCATE PREPARE p_dec;

-- INSERT IGNORE with DECIMAL in strict mode - should succeed with NULL
SET sql_mode = 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO';
PREPARE p_dec_ign FROM 'INSERT IGNORE INTO t_prepare_div (id, c) VALUES (?, CAST(10.5 AS DECIMAL(10,2)) / CAST(0 AS DECIMAL(10,2)))';
SET @id = 1;
EXECUTE p_dec_ign USING @id;
SELECT id, c FROM t_prepare_div ORDER BY id;
TRUNCATE TABLE t_prepare_div;
DEALLOCATE PREPARE p_dec_ign;

-- ============================================================================
-- Test 9: PREPARE with FLOAT division by zero
-- ============================================================================
SET sql_mode = '';

PREPARE p_flt FROM 'INSERT INTO t_prepare_div (id, e) VALUES (?, 10.5 / 0.0)';
SET @id = 1;
EXECUTE p_flt USING @id;
SELECT id, e FROM t_prepare_div ORDER BY id;
TRUNCATE TABLE t_prepare_div;
DEALLOCATE PREPARE p_flt;

-- FLOAT division in strict mode - should error
SET sql_mode = 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO';
PREPARE p_flt FROM 'INSERT INTO t_prepare_div (id, e) VALUES (?, 10.5 / 0.0)';
SET @id = 1;
EXECUTE p_flt USING @id;
SELECT COUNT(*) FROM t_prepare_div;
DEALLOCATE PREPARE p_flt;


-- INSERT IGNORE with FLOAT in strict mode - should succeed with NULL
SET sql_mode = 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO';
PREPARE p_flt_ign FROM 'INSERT IGNORE INTO t_prepare_div (id, e) VALUES (?, 10.5 / 0.0)';
SET @id = 1;
EXECUTE p_flt_ign USING @id;
SELECT id, e FROM t_prepare_div ORDER BY id;
TRUNCATE TABLE t_prepare_div;
DEALLOCATE PREPARE p_flt_ign;

-- ============================================================================
-- Test 10: PREPARE INSERT with multiple rows, mixed valid and div-by-zero
-- ============================================================================
SET sql_mode = '';

PREPARE p_multi FROM 'INSERT INTO t_prepare_div (id, a, b) VALUES (?, 10/0, 5), (?, 20, 10/0), (?, 30, 6)';
SET @id1 = 1, @id2 = 2, @id3 = 3;
EXECUTE p_multi USING @id1, @id2, @id3;
SELECT id, a, b FROM t_prepare_div ORDER BY id;
TRUNCATE TABLE t_prepare_div;
DEALLOCATE PREPARE p_multi;

-- Same in strict mode - should error and insert nothing
SET sql_mode = 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO';
PREPARE p_multi FROM 'INSERT INTO t_prepare_div (id, a, b) VALUES (?, 10/0, 5), (?, 20, 10/0), (?, 30, 6)';
SET @id1 = 1, @id2 = 2, @id3 = 3;
EXECUTE p_multi USING @id1, @id2, @id3;
SELECT COUNT(*) FROM t_prepare_div;
DEALLOCATE PREPARE p_multi;

-- INSERT IGNORE with multiple rows in strict mode - should succeed with NULL
SET sql_mode = 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO';
PREPARE p_multi_ign FROM 'INSERT IGNORE INTO t_prepare_div (id, a, b) VALUES (?, 10/0, 5), (?, 20, 10/0), (?, 30, 6)';
SET @id1 = 1, @id2 = 2, @id3 = 3;
EXECUTE p_multi_ign USING @id1, @id2, @id3;
SELECT id, a, b FROM t_prepare_div ORDER BY id;
TRUNCATE TABLE t_prepare_div;
DEALLOCATE PREPARE p_multi_ign;

-- ============================================================================
-- Test 11: Re-EXECUTE same prepared statement multiple times
-- ============================================================================
SET sql_mode = '';

PREPARE p_reuse FROM 'INSERT INTO t_prepare_div (id, a) VALUES (?, 10/?)';
SET @id = 1, @divisor = 0;
EXECUTE p_reuse USING @id, @divisor;
SET @id = 2, @divisor = 2;
EXECUTE p_reuse USING @id, @divisor;
SET @id = 3, @divisor = 0;
EXECUTE p_reuse USING @id, @divisor;
SET @id = 4, @divisor = 5;
EXECUTE p_reuse USING @id, @divisor;
SELECT id, a FROM t_prepare_div ORDER BY id;
TRUNCATE TABLE t_prepare_div;
DEALLOCATE PREPARE p_reuse;

-- INSERT IGNORE re-execute with mixed divisors in strict mode
SET sql_mode = 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO';
PREPARE p_reuse_ign FROM 'INSERT IGNORE INTO t_prepare_div (id, a) VALUES (?, 10/?)';
SET @id = 1, @divisor = 0;
EXECUTE p_reuse_ign USING @id, @divisor;
SET @id = 2, @divisor = 2;
EXECUTE p_reuse_ign USING @id, @divisor;
SET @id = 3, @divisor = 0;
EXECUTE p_reuse_ign USING @id, @divisor;
SET @id = 4, @divisor = 5;
EXECUTE p_reuse_ign USING @id, @divisor;
SELECT id, a FROM t_prepare_div ORDER BY id;
TRUNCATE TABLE t_prepare_div;
DEALLOCATE PREPARE p_reuse_ign;

-- ============================================================================
-- Test 12: PREPARE SELECT with division by zero (always returns NULL)
-- ============================================================================
SET sql_mode = 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO';

PREPARE p_sel1 FROM 'SELECT 10/0 AS div_result';
EXECUTE p_sel1;
DEALLOCATE PREPARE p_sel1;

PREPARE p_sel2 FROM 'SELECT 10 DIV 0 AS intdiv_result';
EXECUTE p_sel2;
DEALLOCATE PREPARE p_sel2;

PREPARE p_sel3 FROM 'SELECT 10 % 0 AS mod_result';
EXECUTE p_sel3;
DEALLOCATE PREPARE p_sel3;

PREPARE p_sel_param FROM 'SELECT 10/? AS div_result';
SET @divisor = 0;
EXECUTE p_sel_param USING @divisor;
SET @divisor = 2;
EXECUTE p_sel_param USING @divisor;
DEALLOCATE PREPARE p_sel_param;

-- Cleanup
DROP TABLE t_prepare_div;
SET sql_mode = DEFAULT;
