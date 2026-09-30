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

-- Unresolved nested results inherit the fixed DECIMAL peer.
CREATE TABLE common_value (id INT PRIMARY KEY, d DECIMAL(38,10));
INSERT INTO common_value VALUES
  (1,9007199254740992.0000000001),
  (2,9007199254740992.0000000002),
  (3,9007199254740992.0000000003);
PREPARE p_nested FROM 'SELECT id FROM common_value WHERE GREATEST(d,COALESCE(?,?))=d ORDER BY id';
PREPARE p_ifnull FROM 'SELECT id FROM common_value WHERE GREATEST(?,IFNULL(?,d))=d ORDER BY id';
PREPARE p_marker_abs FROM 'SELECT id FROM common_value WHERE GREATEST(ABS(?),COALESCE(?,?))=d ORDER BY id';
PREPARE p_null_first FROM 'SELECT id FROM common_value WHERE COALESCE(?,NULL,d)=d ORDER BY id';
PREPARE p_null_last FROM 'SELECT id FROM common_value WHERE COALESCE(?,d,NULL)=d ORDER BY id';
PREPARE p_typed_null FROM 'SELECT id FROM common_value WHERE COALESCE(?,CAST(NULL AS DECIMAL(38,10)),d)=d ORDER BY id';
SET @p=NULL;
EXECUTE p_nested USING @p,@p;
EXECUTE p_ifnull USING @p,@p;
EXECUTE p_marker_abs USING @p,@p,@p;
EXECUTE p_null_first USING @p;
SET @p='9007199254740992.0000000002';
EXECUTE p_nested USING @p,@p;
EXECUTE p_ifnull USING @p,@p;
EXECUTE p_marker_abs USING @p,@p,@p;
EXECUTE p_null_first USING @p;
EXECUTE p_null_last USING @p;
EXECUTE p_typed_null USING @p;
SET @p='9007199254740992.0000000003';
EXECUTE p_nested USING @p,@p;
EXECUTE p_ifnull USING @p,@p;
EXECUTE p_marker_abs USING @p,@p,@p;
SET @p=NULL;
EXECUTE p_nested USING @p,@p;
EXECUTE p_ifnull USING @p,@p;
EXECUTE p_null_first USING @p;
SET @p='not-a-number';
EXECUTE p_nested USING @p,@p;
EXECUTE p_ifnull USING @p,@p;
EXECUTE p_marker_abs USING @p,@p,@p;
SET @p='9007199254740992.0000000002';
EXECUTE p_nested USING @p,@p;
EXECUTE p_ifnull USING @p,@p;
EXECUTE p_marker_abs USING @p,@p,@p;
DEALLOCATE PREPARE p_nested;
DEALLOCATE PREPARE p_ifnull;
DEALLOCATE PREPARE p_marker_abs;
DEALLOCATE PREPARE p_null_first;
DEALLOCATE PREPARE p_null_last;
DEALLOCATE PREPARE p_typed_null;

DROP DATABASE prepare_decimal_comparison;
