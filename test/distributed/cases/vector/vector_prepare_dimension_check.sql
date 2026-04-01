-- Test: Prepared statement should enforce vecf32/vecf64 dimension constraint
-- Issue: https://github.com/matrixorigin/matrixone/issues/23872
--
-- When inserting a vector via PREPARE/EXECUTE, MatrixOne should validate
-- the vector dimension against the column definition vecf32(N).
-- Previously, a mismatched dimension was silently accepted.

CREATE DATABASE IF NOT EXISTS test_vec_prepare_db;
USE test_vec_prepare_db;

-- ============================================================
-- 1. vecf32: direct insert with wrong dimension — should reject
-- ============================================================
DROP TABLE IF EXISTS t_vecf32;
CREATE TABLE t_vecf32 (id INT, v vecf32(3));

-- correct dimension: should succeed
INSERT INTO t_vecf32 VALUES (1, '[1.0, 2.0, 3.0]');
SELECT id, v, vector_dims(v) AS dim FROM t_vecf32 WHERE id = 1;

-- wrong dimension via direct SQL: should fail
INSERT INTO t_vecf32 VALUES (2, '[1.0, 2.0, 3.0, 4.0, 5.0]');

-- ============================================================
-- 2. vecf32: prepared statement with wrong dimension — should also reject
-- ============================================================
PREPARE stmt1 FROM 'INSERT INTO t_vecf32 VALUES (?, ?)';
SET @id = 3;
SET @v_bad = '[1.0, 2.0, 3.0, 4.0, 5.0]';
EXECUTE stmt1 USING @id, @v_bad;

-- verify no bad row was inserted
SELECT count(*) AS cnt FROM t_vecf32 WHERE id = 3;

-- correct dimension via prepared statement: should succeed
SET @id = 4;
SET @v_ok = '[4.0, 5.0, 6.0]';
EXECUTE stmt1 USING @id, @v_ok;
SELECT id, v, vector_dims(v) AS dim FROM t_vecf32 WHERE id = 4;

DEALLOCATE PREPARE stmt1;

-- ============================================================
-- 3. vecf64: same check for vecf64 type
-- ============================================================
DROP TABLE IF EXISTS t_vecf64;
CREATE TABLE t_vecf64 (id INT, v vecf64(4));

-- correct dimension
INSERT INTO t_vecf64 VALUES (1, '[1.0, 2.0, 3.0, 4.0]');
SELECT id, vector_dims(v) AS dim FROM t_vecf64 WHERE id = 1;

-- wrong dimension via direct SQL: should fail
INSERT INTO t_vecf64 VALUES (2, '[1.0, 2.0]');

-- wrong dimension via prepared statement: should also fail
PREPARE stmt2 FROM 'INSERT INTO t_vecf64 VALUES (?, ?)';
SET @id = 3;
SET @v_bad64 = '[1.0, 2.0]';
EXECUTE stmt2 USING @id, @v_bad64;

-- verify no bad row was inserted
SELECT count(*) AS cnt FROM t_vecf64 WHERE id = 3;

DEALLOCATE PREPARE stmt2;

-- ============================================================
-- 4. Verify data integrity: only correct-dimension rows exist
-- ============================================================
SELECT id, v, vector_dims(v) AS dim FROM t_vecf32 ORDER BY id;
SELECT id, v, vector_dims(v) AS dim FROM t_vecf64 ORDER BY id;

-- cleanup
DROP TABLE IF EXISTS t_vecf32;
DROP TABLE IF EXISTS t_vecf64;
DROP DATABASE test_vec_prepare_db;
