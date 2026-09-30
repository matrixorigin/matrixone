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

-- Every fixed peer contributes its scale, independent of operand order or nesting.
PREPARE p_scale_first FROM 'SELECT GREATEST(CAST(1 AS DECIMAL(65,30)),COALESCE(?,?),CAST(1 AS DECIMAL(38,0))) AS v';
PREPARE p_scale_last FROM 'SELECT GREATEST(CAST(1 AS DECIMAL(38,0)),COALESCE(?,?),CAST(1 AS DECIMAL(65,30))) AS v';
PREPARE p_scale_direct_first FROM 'SELECT GREATEST(CAST(1 AS DECIMAL(65,30)),?,?,CAST(1 AS DECIMAL(38,0))) AS v';
PREPARE p_scale_direct_last FROM 'SELECT GREATEST(CAST(1 AS DECIMAL(38,0)),?,?,CAST(1 AS DECIMAL(65,30))) AS v';
SET @p=REPEAT('9',65);
EXECUTE p_scale_first USING @p,@p;
EXECUTE p_scale_last USING @p,@p;
EXECUTE p_scale_direct_first USING @p,@p;
EXECUTE p_scale_direct_last USING @p,@p;
-- Decimal256 has 76 digits: scale 30 leaves at most 46 integral digits.
SET @p=REPEAT('9',47);
EXECUTE p_scale_first USING @p,@p;
EXECUTE p_scale_last USING @p,@p;
SET @p=REPEAT('9',46);
EXECUTE p_scale_first USING @p,@p;
EXECUTE p_scale_last USING @p,@p;
SET @p='1';
EXECUTE p_scale_first USING @p,@p;
EXECUTE p_scale_last USING @p,@p;
EXECUTE p_scale_direct_first USING @p,@p;
EXECUTE p_scale_direct_last USING @p,@p;
SET @p=REPEAT('9',65);
EXECUTE p_scale_first USING @p,@p;
EXECUTE p_scale_last USING @p,@p;
SET @p='1';
EXECUTE p_scale_first USING @p,@p;
EXECUTE p_scale_last USING @p,@p;
DEALLOCATE PREPARE p_scale_first;
DEALLOCATE PREPARE p_scale_last;
DEALLOCATE PREPARE p_scale_direct_first;
DEALLOCATE PREPARE p_scale_direct_last;

-- Runtime marker shapes are combined before any nested result fixes its scale.
PREPARE p_joint_direct FROM 'SELECT GREATEST(CAST(1 AS DECIMAL(38,0)),?,?) AS v';
PREPARE p_joint_coalesce FROM 'SELECT GREATEST(CAST(1 AS DECIMAL(38,0)),COALESCE(?,?)) AS v';
PREPARE p_joint_ifnull FROM 'SELECT GREATEST(CAST(1 AS DECIMAL(38,0)),IFNULL(?,?)) AS v';
PREPARE p_joint_deep FROM 'SELECT GREATEST(CAST(1 AS DECIMAL(38,0)),COALESCE(?,COALESCE(?,?))) AS v';
PREPARE p_joint_siblings FROM 'SELECT GREATEST(CAST(1 AS DECIMAL(38,0)),COALESCE(?,?),COALESCE(?,?)) AS v';
SET @big=REPEAT('9',65),@tiny='1e-30';
EXECUTE p_joint_direct USING @big,@tiny;
EXECUTE p_joint_coalesce USING @big,@tiny;
EXECUTE p_joint_ifnull USING @big,@tiny;
EXECUTE p_joint_deep USING @big,@tiny,@tiny;
EXECUTE p_joint_siblings USING @big,@big,@tiny,@tiny;
EXECUTE p_joint_direct USING @tiny,@big;
EXECUTE p_joint_coalesce USING @tiny,@big;
EXECUTE p_joint_ifnull USING @tiny,@big;
EXECUTE p_joint_deep USING @tiny,@big,@big;
EXECUTE p_joint_siblings USING @tiny,@tiny,@big,@big;
SET @big='1';
EXECUTE p_joint_direct USING @big,@tiny;
EXECUTE p_joint_coalesce USING @big,@tiny;
EXECUTE p_joint_ifnull USING @big,@tiny;
EXECUTE p_joint_deep USING @big,@tiny,@tiny;
EXECUTE p_joint_siblings USING @big,@big,@tiny,@tiny;
SET @big=REPEAT('9',77);
EXECUTE p_joint_direct USING @big,@tiny;
EXECUTE p_joint_coalesce USING @big,@tiny;
EXECUTE p_joint_ifnull USING @big,@tiny;
EXECUTE p_joint_deep USING @big,@tiny,@tiny;
EXECUTE p_joint_siblings USING @big,@big,@tiny,@tiny;
SET @big=REPEAT('9',65);
EXECUTE p_joint_direct USING @big,@tiny;
EXECUTE p_joint_coalesce USING @big,@tiny;
EXECUTE p_joint_ifnull USING @big,@tiny;
EXECUTE p_joint_deep USING @big,@tiny,@tiny;
EXECUTE p_joint_siblings USING @big,@big,@tiny,@tiny;
DEALLOCATE PREPARE p_joint_direct;
DEALLOCATE PREPARE p_joint_coalesce;
DEALLOCATE PREPARE p_joint_ifnull;
DEALLOCATE PREPARE p_joint_deep;
DEALLOCATE PREPARE p_joint_siblings;

DROP DATABASE prepare_decimal_comparison;
