DROP DATABASE IF EXISTS prepared_time_arithmetic_test;
CREATE DATABASE prepared_time_arithmetic_test;
USE prepared_time_arithmetic_test;

-- Issue #28963: cover real SQL PREPARE/EXECUTE rebinding and result metadata.
PREPARE p_time_numeric_0 FROM
    'SELECT CAST(''00:00:01'' AS TIME(0)) * ? AS result';
SET @p_time_numeric_v = CAST(10 AS SIGNED);
-- @metacmp(true)
EXECUTE p_time_numeric_0 USING @p_time_numeric_v;
SET @p_time_numeric_v = CAST(1.2345678901234 AS DECIMAL(14,13));
-- @metacmp(true)
EXECUTE p_time_numeric_0 USING @p_time_numeric_v;
SET @p_time_numeric_v = CAST(1.25 AS DECIMAL(3,2));
-- @metacmp(true)
EXECUTE p_time_numeric_0 USING @p_time_numeric_v;
SET @p_time_numeric_v = NULL;
-- @metacmp(true)
EXECUTE p_time_numeric_0 USING @p_time_numeric_v;
SET @p_time_numeric_v = CAST(10 AS SIGNED);
-- @metacmp(true)
EXECUTE p_time_numeric_0 USING @p_time_numeric_v;
DEALLOCATE PREPARE p_time_numeric_0;

-- Integer rebinding must retain the DECIMAL64 temporal arithmetic domain for
-- addition, subtraction, and modulo instead of widening to DECIMAL128.
PREPARE p_time_integer_add FROM
    'SELECT CAST(''00:00:01'' AS TIME(0)) + ? AS result';
SET @p_time_numeric_v = CAST(10 AS SIGNED);
-- @metacmp(true)
EXECUTE p_time_integer_add USING @p_time_numeric_v;
DEALLOCATE PREPARE p_time_integer_add;

PREPARE p_time_integer_sub FROM
    'SELECT CAST(''00:00:01'' AS TIME(0)) - ? AS result';
SET @p_time_numeric_v = CAST(10 AS SIGNED);
-- @metacmp(true)
EXECUTE p_time_integer_sub USING @p_time_numeric_v;
DEALLOCATE PREPARE p_time_integer_sub;

PREPARE p_time_integer_mod FROM
    'SELECT CAST(''00:00:01'' AS TIME(0)) % ? AS result';
SET @p_time_numeric_v = CAST(10 AS SIGNED);
-- @metacmp(true)
EXECUTE p_time_integer_mod USING @p_time_numeric_v;
DEALLOCATE PREPARE p_time_integer_mod;

-- The temporal DECIMAL64 provenance must also reach nested signed-integer
-- arithmetic, not only a direct parameter occurrence.
PREPARE p_time_nested_add FROM
    'SELECT CAST(''00:00:01'' AS TIME(0)) + (? + ?) AS result';
SET @p_time_nested_left = CAST(1 AS SIGNED);
SET @p_time_nested_right = CAST(2 AS SIGNED);
-- @metacmp(true)
EXECUTE p_time_nested_add USING @p_time_nested_left, @p_time_nested_right;
DEALLOCATE PREPARE p_time_nested_add;

-- The nested integer operation must finish in its ordinary integer domain
-- before the completed value is coerced to the TIME(6) decimal boundary.
PREPARE p_time_nested_scale FROM
    'SELECT CAST(''00:00:01.000000'' AS TIME(6)) + (? - ?) AS result';
SET @p_time_nested_left = CAST(10000000000000 AS SIGNED);
SET @p_time_nested_right = CAST(9999999999999 AS SIGNED);
-- @metacmp(true)
EXECUTE p_time_nested_scale USING @p_time_nested_left, @p_time_nested_right;
DEALLOCATE PREPARE p_time_nested_scale;

-- TIME plus an unsigned integer follows the ordinary DECIMAL64 domain too.
PREPARE p_time_unsigned FROM
    'SELECT CAST(''00:00:01'' AS TIME(0)) + ? AS result';
SET @p_time_numeric_v = CAST(10 AS UNSIGNED);
-- @metacmp(true)
EXECUTE p_time_unsigned USING @p_time_numeric_v;
DEALLOCATE PREPARE p_time_unsigned;

-- A folded typed NULL must not promote the independently signed integer
-- subtree to DECIMAL256 before it meets the TIME operand.
PREPARE p_time_folded_signed FROM
    'SELECT CAST(''00:00:01'' AS TIME(0)) + (CAST(NULL AS SIGNED) + ?) AS result';
SET @p_time_folded_v = CAST(2 AS SIGNED);
-- @metacmp(true)
EXECUTE p_time_folded_signed USING @p_time_folded_v;
DEALLOCATE PREPARE p_time_folded_signed;

PREPARE p_time_folded_signed_value FROM
    'SELECT CAST(''00:00:01'' AS TIME(0)) + (CAST(1 AS SIGNED) + ?) AS result';
SET @p_time_folded_v = CAST(2 AS SIGNED);
-- @metacmp(true)
EXECUTE p_time_folded_signed_value USING @p_time_folded_v;
DEALLOCATE PREPARE p_time_folded_signed_value;

-- An explicit DECIMAL cast remains a DECIMAL128 boundary for a large integer.
PREPARE p_time_explicit_decimal FROM
    'SELECT CAST(CAST(''00:00:01'' AS TIME(0)) AS DECIMAL(10,2)) + ? AS result';
SET @p_time_numeric_v = CAST(9223372036854775807 AS SIGNED);
-- @metacmp(true)
EXECUTE p_time_explicit_decimal USING @p_time_numeric_v;
DEALLOCATE PREPARE p_time_explicit_decimal;

PREPARE p_time_numeric_6 FROM
    'SELECT CAST(''03:04:05.123456'' AS TIME(6)) * ? AS result';
SET @p_time_numeric_v = CAST(10 AS SIGNED);
-- @metacmp(true)
EXECUTE p_time_numeric_6 USING @p_time_numeric_v;
SET @p_time_numeric_v = CAST(1.25 AS DECIMAL(3,2));
-- @metacmp(true)
EXECUTE p_time_numeric_6 USING @p_time_numeric_v;
DEALLOCATE PREPARE p_time_numeric_6;

-- Ordinary expressions are controls for both prepared result domains.
-- @metacmp(true)
SELECT CAST('00:00:01' AS TIME(0)) * CAST(1.2345678901234 AS DECIMAL(14,13)) AS result;
-- @metacmp(true)
SELECT CAST('03:04:05.123456' AS TIME(6)) * CAST(1.25 AS DECIMAL(3,2)) AS result;

DROP DATABASE prepared_time_arithmetic_test;
