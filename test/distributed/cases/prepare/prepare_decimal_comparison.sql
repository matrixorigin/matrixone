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

CREATE TABLE prefix_values (id INT PRIMARY KEY, d DECIMAL(10,0));
INSERT INTO prefix_values VALUES (1, 0), (2, 1), (3, 16), (4, 100);
PREPARE p_prefix FROM 'SELECT id FROM prefix_values WHERE d = ? ORDER BY id';
SET @p = '0x10';
EXECUTE p_prefix USING @p;
SET @p = '1+2';
EXECUTE p_prefix USING @p;
SET @p = '1 2';
EXECUTE p_prefix USING @p;
DEALLOCATE PREPARE p_prefix;

PREPARE p_extra_scale FROM 'SELECT id FROM t WHERE d128 = ? ORDER BY id';
SET @p = '9007199254740992.00014';
EXECUTE p_extra_scale USING @p;
DEALLOCATE PREPARE p_extra_scale;

PREPARE p_dynamic_reverse_in FROM 'SELECT id FROM t WHERE ? IN (d128) ORDER BY id';
SET @p = '9007199254740992.0001';
EXECUTE p_dynamic_reverse_in USING @p;
DEALLOCATE PREPARE p_dynamic_reverse_in;
PREPARE p_dynamic_reverse_not_in FROM 'SELECT id FROM t WHERE ? NOT IN (d128) ORDER BY id';
SET @p = '9007199254740992.0001';
EXECUTE p_dynamic_reverse_not_in USING @p;
DEALLOCATE PREPARE p_dynamic_reverse_not_in;

PREPARE p_nested_or FROM
  'SELECT GROUP_CONCAT(id ORDER BY id) FROM t WHERE d128 = ? OR id = -1';
SET @p = '9007199254740992.00014';
EXECUTE p_nested_or USING @p;
SET @p = '9007199254740992.0001';
EXECUTE p_nested_or USING @p;
SET @p = '9007199254740992.00014';
EXECUTE p_nested_or USING @p;
DEALLOCATE PREPARE p_nested_or;

PREPARE p_nested_not FROM
  'SELECT GROUP_CONCAT(id ORDER BY id) FROM t WHERE NOT(d128 <> ?)';
SET @p = '9007199254740992.00014';
EXECUTE p_nested_not USING @p;
DEALLOCATE PREPARE p_nested_not;

PREPARE p_projection_case FROM
  'SELECT GROUP_CONCAT(CASE WHEN d128 = ? THEN id END ORDER BY id) FROM t';
SET @p = '9007199254740992.00014';
EXECUTE p_projection_case USING @p;
DEALLOCATE PREPARE p_projection_case;

PREPARE p_join_or FROM
  'SELECT GROUP_CONCAT(a.id ORDER BY a.id) FROM t a JOIN t b ON a.id=b.id AND (a.d128=? OR a.id=-1)';
SET @p = '9007199254740992.00014';
EXECUTE p_join_or USING @p;
DEALLOCATE PREPARE p_join_or;

PREPARE p_having FROM
  'SELECT GROUP_CONCAT(id ORDER BY id) FROM t GROUP BY id,d128 HAVING d128=? ORDER BY id';
SET @p = '9007199254740992.00014';
EXECUTE p_having USING @p;
DEALLOCATE PREPARE p_having;

DROP TABLE IF EXISTS ctas_decimal_param;
PREPARE p_ctas FROM
  'CREATE TABLE ctas_decimal_param AS SELECT id,d128 FROM t WHERE d128=?';
SET @p = '9007199254740992.00014';
EXECUTE p_ctas USING @p;
SELECT COUNT(*) FROM ctas_decimal_param;
DEALLOCATE PREPARE p_ctas;

DROP DATABASE prepare_decimal_comparison;
