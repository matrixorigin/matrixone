-- @case
-- @desc: Prepared DECIMAL comparisons derive exact parameter types
-- @label:bvt
-- Regression for #26847 and #26845.

DROP DATABASE IF EXISTS prepare_decimal_comparison;
CREATE DATABASE prepare_decimal_comparison;
USE prepare_decimal_comparison;

CREATE TABLE t (
  id INT PRIMARY KEY,
  d64 DECIMAL(18,2),
  d128 DECIMAL(20,4)
);
INSERT INTO t VALUES
  (1, NULL, NULL),
  (2, 9007199254740991.99, 9007199254740991.9999),
  (3, 9007199254740992.00, 9007199254740992.0000),
  (4, 9007199254740992.01, 9007199254740992.0001),
  (5, 9007199254740993.00, 9007199254740993.0000),
  (6, 9007199254740993.01, 9007199254740993.0001);

-- DECIMAL128 non-NULL, NULL, and subsequent non-NULL executions reuse one plan.
PREPARE p128_nullsafe FROM 'SELECT id FROM t WHERE d128 <=> ? ORDER BY id';
SET @p = '9007199254740992.0001';
EXECUTE p128_nullsafe USING @p;
SET @p = NULL;
EXECUTE p128_nullsafe USING @p;
SET @p = '9007199254740993.0001';
EXECUTE p128_nullsafe USING @p;
DEALLOCATE PREPARE p128_nullsafe;

-- The comparison contract is symmetric in operand placement.
PREPARE p128_left FROM 'SELECT id FROM t WHERE ? <=> d128 ORDER BY id';
SET @p = '9007199254740992.0001';
EXECUTE p128_left USING @p;
DEALLOCATE PREPARE p128_left;

-- DECIMAL64 follows the same exact prepared-parameter path.
PREPARE p64_nullsafe FROM 'SELECT id FROM t WHERE d64 <=> ? ORDER BY id';
SET @p = '9007199254740992.01';
EXECUTE p64_nullsafe USING @p;
SET @p = NULL;
EXECUTE p64_nullsafe USING @p;
DEALLOCATE PREPARE p64_nullsafe;

-- Ordinary comparisons are controls for the shared parameter-typing rule.
PREPARE p_eq FROM 'SELECT id FROM t WHERE d128 = ? ORDER BY id';
PREPARE p_ne FROM 'SELECT id FROM t WHERE d128 <> ? ORDER BY id';
PREPARE p_lt FROM 'SELECT id FROM t WHERE d128 < ? ORDER BY id';
PREPARE p_le FROM 'SELECT id FROM t WHERE d128 <= ? ORDER BY id';
PREPARE p_gt FROM 'SELECT id FROM t WHERE d128 > ? ORDER BY id';
PREPARE p_ge FROM 'SELECT id FROM t WHERE d128 >= ? ORDER BY id';
SET @p = '9007199254740992.0001';
EXECUTE p_eq USING @p;
EXECUTE p_ne USING @p;
EXECUTE p_lt USING @p;
EXECUTE p_le USING @p;
EXECUTE p_gt USING @p;
EXECUTE p_ge USING @p;
DEALLOCATE PREPARE p_eq;
DEALLOCATE PREPARE p_ne;
DEALLOCATE PREPARE p_lt;
DEALLOCATE PREPARE p_le;
DEALLOCATE PREPARE p_gt;
DEALLOCATE PREPARE p_ge;

-- A prepared statement must normalize the reversed IN-list direction too.
PREPARE p_reverse_in FROM
  'SELECT id FROM t WHERE ''9007199254740992.0001'' IN (d128) ORDER BY id';
EXECUTE p_reverse_in;
DEALLOCATE PREPARE p_reverse_in;
PREPARE p_reverse_not_in FROM
  'SELECT id FROM t WHERE ''9007199254740992.0001'' NOT IN (d128) ORDER BY id';
EXECUTE p_reverse_not_in;
DEALLOCATE PREPARE p_reverse_not_in;

DROP DATABASE prepare_decimal_comparison;
