-- @case
-- @desc: SQL EXECUTE common-value functions retain fixed DECIMAL precision (#29389)
-- @label:bvt

DROP DATABASE IF EXISTS prepared_decimal_extrema;
CREATE DATABASE prepared_decimal_extrema;
USE prepared_decimal_extrema;
CREATE TABLE t(id INT PRIMARY KEY, d DECIMAL(38,10));
INSERT INTO t VALUES
  (1, 9007199254740992.0000000001),
  (2, 9007199254740992.0000000002),
  (3, 9007199254740992.0000000003),
  (4, 9007199254740993.0000000001);

PREPARE pg FROM 'SELECT id FROM t WHERE GREATEST(d,?)=d ORDER BY id';
PREPARE pl FROM 'SELECT id FROM t WHERE LEAST(d,?)=d ORDER BY id';
PREPARE pg_ne FROM 'SELECT id FROM t WHERE GREATEST(d,?)<>d ORDER BY id';
PREPARE pl_ne FROM 'SELECT id FROM t WHERE LEAST(d,?)<>d ORDER BY id';
PREPARE p_reverse FROM 'SELECT id FROM t WHERE d=GREATEST(d,?) ORDER BY id';
PREPARE p_derived FROM 'SELECT id FROM (SELECT id,d,GREATEST(d,?) AS g FROM t) s WHERE g=d ORDER BY id';
PREPARE p_values FROM 'SELECT id,GREATEST(d,?) AS g,LEAST(d,?) AS l FROM t ORDER BY id';
PREPARE pc FROM 'SELECT id FROM t WHERE COALESCE(?,d)=d ORDER BY id';
PREPARE pc_values FROM 'SELECT id,COALESCE(?,d) AS c FROM t ORDER BY id';
SET @p='9007199254740992.0000000002';
EXECUTE pg USING @p;
EXECUTE pl USING @p;
EXECUTE pg_ne USING @p;
EXECUTE pl_ne USING @p;
EXECUTE p_reverse USING @p;
EXECUTE p_derived USING @p;
EXECUTE p_values USING @p,@p;
EXECUTE pc USING @p;
EXECUTE pc_values USING @p;

-- Reuse the cached statements across NULL and a second exact value.
SET @p=NULL;
EXECUTE pg USING @p;
EXECUTE pl USING @p;
EXECUTE pc USING @p;
SET @p='9007199254740992.0000000003';
EXECUTE pg USING @p;
EXECUTE pl USING @p;
EXECUTE pc USING @p;
SET @p='9007199254740992.0000000002';
EXECUTE pg USING @p;
EXECUTE pl USING @p;
EXECUTE pc USING @p;
SET @p='9007199254740992.0000000002tail';
EXECUTE pg USING @p;
EXECUTE pl USING @p;
EXECUTE pc USING @p;
SET @p='9007199254740992.0000000002';

-- A second, explicitly typed marker must stay bound during peer restoration.
PREPARE pg_multi FROM 'SELECT id FROM t WHERE GREATEST(d,?,CAST(? AS DECIMAL(38,10)))=d ORDER BY id';
PREPARE pl_multi FROM 'SELECT id FROM t WHERE LEAST(d,?,CAST(? AS DECIMAL(38,10)))=d ORDER BY id';
SET @q='9007199254740992.0000000004';
EXECUTE pg_multi USING @p,@q;
EXECUTE pl_multi USING @p,@q;
SET @q=NULL;
EXECUTE pg_multi USING @p,@q;
EXECUTE pl_multi USING @p,@q;
SET @q='9007199254740992.0000000004';
EXECUTE pg_multi USING @p,@q;
EXECUTE pl_multi USING @p,@q;

PREPARE pg_cast FROM 'SELECT id FROM t WHERE GREATEST(d,CAST(? AS DECIMAL(38,10)))=d ORDER BY id';
PREPARE pl_cast FROM 'SELECT id FROM t WHERE LEAST(d,CAST(? AS DECIMAL(38,10)))=d ORDER BY id';
EXECUTE pg_cast USING @p;
EXECUTE pl_cast USING @p;

-- A typed marker and a nested common-value result retain their DECIMAL peer.
PREPARE pc_typed FROM 'SELECT id FROM t WHERE COALESCE(?,CAST(? AS DECIMAL(38,10)))=d ORDER BY id';
PREPARE pg_typed FROM 'SELECT id FROM t WHERE GREATEST(?,CAST(? AS DECIMAL(38,10)))=d ORDER BY id';
PREPARE pg_nested FROM 'SELECT id FROM t WHERE GREATEST(?,COALESCE(?,d))=d ORDER BY id';
PREPARE pg_abs_nested FROM 'SELECT id FROM t WHERE GREATEST(?,ABS(COALESCE(?,d)))=d ORDER BY id';
PREPARE pc_abs_nested FROM 'SELECT id FROM t WHERE COALESCE(?,ABS(COALESCE(?,d)))=d ORDER BY id';
PREPARE pg_arith_nested FROM 'SELECT id FROM t WHERE GREATEST(?,COALESCE(?,d)+0)=d ORDER BY id';
EXECUTE pc_typed USING @p,@p;
EXECUTE pg_typed USING @p,@p;
EXECUTE pg_nested USING @p,@p;
EXECUTE pg_abs_nested USING @p,@p;
EXECUTE pc_abs_nested USING @p,@p;
EXECUTE pg_arith_nested USING @p,@p;

-- A separate concrete SQL string still owns the mixed-result domain.
SET @decimal_null=CAST(NULL AS DECIMAL(20,0)), @text_peer='abc';
PREPARE pc_string_peer FROM 'SELECT COALESCE(?, ?, d) AS c FROM t WHERE id=2';
PREPARE pg_string_peer FROM 'SELECT GREATEST(d, ?, CAST(9007199254740992 AS DECIMAL(20,0))) AS g FROM t WHERE id=2';
EXECUTE pc_string_peer USING @decimal_null,@text_peer;
EXECUTE pg_string_peer USING @text_peer;
SET @char_null=CAST(NULL AS CHAR);
PREPARE pc_char_null FROM 'SELECT COALESCE(?,d) AS c FROM t WHERE id=2';
EXECUTE pc_char_null USING @char_null;

-- A tiny text marker must not turn the DECIMAL column into FLOAT when the
-- combined declared precision exceeds Decimal256's width.
PREPARE pg_width FROM 'SELECT id FROM t WHERE GREATEST(d,?)=CAST(9007199254740992.0000000002 AS DECIMAL(38,10)) ORDER BY id';
PREPARE pc_width FROM 'SELECT id FROM t WHERE COALESCE(d,?)=CAST(9007199254740992.0000000002 AS DECIMAL(38,10)) ORDER BY id';
SET @tiny='1e-48';
EXECUTE pg_width USING @tiny;
EXECUTE pc_width USING @tiny;
SET @tiny='1e-49';
EXECUTE pg_width USING @tiny;
EXECUTE pc_width USING @tiny;
SET @tiny='1e-1000';
EXECUTE pg_width USING @tiny;
EXECUTE pc_width USING @tiny;
-- The next value has the same inferred FLOAT64 category. A cached zero from
-- the previous execution must not suppress this overflow error.
SET @tiny='1e1000';
EXECUTE pg_width USING @tiny;
SET @tiny='1e-48';
EXECUTE pg_width USING @tiny;

-- A text prefix whose fractional tail exceeds the DECIMAL envelope must
-- retain enough integral digits before rounding that tail.
SET @wide=CONCAT('123456789.',REPEAT('0',67),'1');
PREPARE pl_wide FROM 'SELECT LEAST(?,CAST(1.25 AS DECIMAL(10,2))) AS v';
EXECUTE pl_wide USING @wide;

-- A concrete string, explicit CHAR marker, or FLOAT peer does not establish
-- the fixed DECIMAL-only context for the text parameter.
SET @small='2';
PREPARE p_char_peer FROM 'SELECT GREATEST(CAST(1 AS DECIMAL(10,0)),?,CAST(10 AS CHAR)) AS g';
PREPARE p_char_marker FROM 'SELECT GREATEST(CAST(1 AS DECIMAL(10,0)),CAST(? AS CHAR),CAST(10 AS DECIMAL(10,0))) AS g';
PREPARE p_float_peer FROM 'SELECT GREATEST(CAST(1 AS DECIMAL(10,0)),?,CAST(10 AS DOUBLE)) AS g';
EXECUTE p_char_peer USING @small;
EXECUTE p_char_marker USING @small;
EXECUTE p_float_peer USING @small;

DEALLOCATE PREPARE pg;
DEALLOCATE PREPARE pl;
DEALLOCATE PREPARE pg_ne;
DEALLOCATE PREPARE pl_ne;
DEALLOCATE PREPARE p_reverse;
DEALLOCATE PREPARE p_derived;
DEALLOCATE PREPARE p_values;
DEALLOCATE PREPARE pc;
DEALLOCATE PREPARE pc_values;
DEALLOCATE PREPARE pg_multi;
DEALLOCATE PREPARE pl_multi;
DEALLOCATE PREPARE pg_cast;
DEALLOCATE PREPARE pl_cast;
DEALLOCATE PREPARE pc_typed;
DEALLOCATE PREPARE pg_typed;
DEALLOCATE PREPARE pg_nested;
DEALLOCATE PREPARE pg_abs_nested;
DEALLOCATE PREPARE pc_abs_nested;
DEALLOCATE PREPARE pg_arith_nested;
DEALLOCATE PREPARE pc_string_peer;
DEALLOCATE PREPARE pg_string_peer;
DEALLOCATE PREPARE pc_char_null;
DEALLOCATE PREPARE pg_width;
DEALLOCATE PREPARE pl_wide;
DEALLOCATE PREPARE pc_width;
DEALLOCATE PREPARE p_char_peer;
DEALLOCATE PREPARE p_char_marker;
DEALLOCATE PREPARE p_float_peer;
DROP DATABASE prepared_decimal_extrema;
