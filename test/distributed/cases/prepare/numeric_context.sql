DROP DATABASE IF EXISTS numeric_context_test;
CREATE DATABASE numeric_context_test;
USE numeric_context_test;

SET @a = 9007199254740993;
SET @b = 0;

PREPARE p_cast FROM 'SELECT CAST((? + ?) + 1 AS DECIMAL(30, 0))';
EXECUTE p_cast USING @a, @b;
EXECUTE p_cast USING @b, @a;
DEALLOCATE PREPARE p_cast;

PREPARE p_reuse FROM 'SELECT CAST((? + ?) + 1 AS DECIMAL(30, 0))';
SET @dynamic = 9007199254740993;
EXECUTE p_reuse USING @dynamic, @b;
SET @dynamic = '9007199254740993';
EXECUTE p_reuse USING @dynamic, @b;
SET @dynamic = CAST(9007199254740993 AS DECIMAL(30, 0));
EXECUTE p_reuse USING @dynamic, @b;
SET @dynamic = CAST(9007199254740993 AS UNSIGNED);
EXECUTE p_reuse USING @dynamic, @b;
SET @dynamic = -9007199254740993;
EXECUTE p_reuse USING @dynamic, @b;
SET @dynamic = NULL;
EXECUTE p_reuse USING @dynamic, @b;
SET @dynamic = 'not-a-number';
EXECUTE p_reuse USING @dynamic, @b;
SET @dynamic = 9007199254740993;
EXECUTE p_reuse USING @dynamic, @b;
DEALLOCATE PREPARE p_reuse;

PREPARE p_mod FROM 'SELECT CAST(MOD(?, 2) AS SIGNED)';
EXECUTE p_mod USING @a;
DEALLOCATE PREPARE p_mod;

-- Issue #28484: Boolean provenance must survive SQL EXECUTE's text transport.
-- Reuse one prepared plan across Boolean, NULL and ordinary string values.
PREPARE p_bool FROM 'SELECT CAST(? AS DOUBLE) AS d, ROUND(SIN(?),12) AS s, ROUND(ACOS(?),12) AS a';
SET @bool_math = TRUE;
EXECUTE p_bool USING @bool_math, @bool_math, @bool_math;
SET @bool_math = FALSE;
EXECUTE p_bool USING @bool_math, @bool_math, @bool_math;
SET @bool_math = NULL;
EXECUTE p_bool USING @bool_math, @bool_math, @bool_math;
SET @bool_math = 'true';
EXECUTE p_bool USING @bool_math, @bool_math, @bool_math;
SET @bool_math = TRUE;
EXECUTE p_bool USING @bool_math, @bool_math, @bool_math;
DEALLOCATE PREPARE p_bool;

PREPARE p_bool_int FROM 'SELECT ABS(?) AS a, ROUND(?) AS r, SIGN(?) AS s';
EXECUTE p_bool_int USING @bool_math, @bool_math, @bool_math;
SET @bool_math = FALSE;
EXECUTE p_bool_int USING @bool_math, @bool_math, @bool_math;
SET @bool_math = NULL;
EXECUTE p_bool_int USING @bool_math, @bool_math, @bool_math;
SET @bool_math = TRUE;
EXECUTE p_bool_int USING @bool_math, @bool_math, @bool_math;
DEALLOCATE PREPARE p_bool_int;

DROP DATABASE numeric_context_test;
