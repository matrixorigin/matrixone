-- @case
-- @desc: Prepared parameters use exact DECIMAL common types in conditional functions
-- @label:bvt

DROP DATABASE IF EXISTS prepare_decimal_common_type;
CREATE DATABASE prepare_decimal_common_type;
USE prepare_decimal_common_type;

CREATE TABLE t (
  id INT PRIMARY KEY,
  d64 DECIMAL(18,2),
  d128 DECIMAL(38,10)
);
CREATE TABLE common_peers (
  d DECIMAL(10,2),
  f DOUBLE,
  b BOOL,
  y YEAR,
  bitv BIT(8),
  e ENUM('x','z')
);
CREATE TABLE decimal_limits (
  d65i DECIMAL(65,0),
  d65f DECIMAL(65,65)
);
CREATE TABLE decimal_order_controls (
  d65i DECIMAL(65,0),
  d65f DECIMAL(65,65)
);
INSERT INTO t VALUES
  (1,9007199254740992.01,9007199254740992.0000000001),
  (2,9007199254740992.02,9007199254740992.0000000002),
  (3,9007199254740992.03,9007199254740992.0000000003),
  (4,9007199254740993.01,9007199254740993.0000000001);
INSERT INTO common_peers VALUES (2.00,1.5,TRUE,2024,b'00000011','z');
INSERT INTO decimal_limits VALUES (
  99999999999999999999999999999999999999999999999999999999999999999,
  0.00000000000000000000000000000000000000000000000000000000000000001
);
INSERT INTO decimal_order_controls VALUES (2,0.1);

-- DECIMAL128 precision and value/NULL reuse.
PREPARE pc FROM 'SELECT id FROM t WHERE COALESCE(?,d128)=d128 ORDER BY id';
SET @p='9007199254740992.0000000002';
EXECUTE pc USING @p;
SET @p=NULL;
EXECUTE pc USING @p;
SET @p='9007199254740992.0000000002';
EXECUTE pc USING @p;
SET @p=NULL;
EXECUTE pc USING @p;
DEALLOCATE PREPARE pc;

-- A fresh NULL-first statement follows the same common-type path.
PREPARE pc_null_first FROM 'SELECT id FROM t WHERE COALESCE(?,d128)=d128 ORDER BY id';
SET @p=NULL;
EXECUTE pc_null_first USING @p;
SET @p='9007199254740992.0000000002';
EXECUTE pc_null_first USING @p;
DEALLOCATE PREPARE pc_null_first;

-- GREATEST and LEAST keep exact ordering above 2^53.
PREPARE pg FROM 'SELECT id FROM t WHERE GREATEST(d128,?)=d128 ORDER BY id';
SET @p='9007199254740992.0000000002';
EXECUTE pg USING @p;
DEALLOCATE PREPARE pg;

PREPARE pl FROM 'SELECT id FROM t WHERE LEAST(d128,?)=d128 ORDER BY id';
EXECUTE pl USING @p;
DEALLOCATE PREPARE pl;

-- DECIMAL64 uses the same contextual parameter typing.
PREPARE pc64 FROM 'SELECT id FROM t WHERE COALESCE(?,d64)=d64 ORDER BY id';
SET @p='9007199254740992.02';
EXECUTE pc64 USING @p;
SET @p=NULL;
EXECUTE pc64 USING @p;
DEALLOCATE PREPARE pc64;

-- A direct parameter contributes DECIMAL(65,30), preserving its own scale and
-- integral width instead of being narrowed to the DECIMAL column definition.
PREPARE p_param_precision FROM
  'SELECT COALESCE(?,d),GREATEST(?,d),LEAST(?,d) FROM common_peers';
SET @p='1.234567';
EXECUTE p_param_precision USING @p,@p,@p;
SET @p='12345678901.23';
EXECUTE p_param_precision USING @p,@p,@p;
DEALLOCATE PREPARE p_param_precision;

-- Direct parameters in a DECIMAL common-type function use MySQL numeric-prefix
-- conversion without changing the binding category based on their bytes.
PREPARE p_mysql_numeric_conversion FROM
  'SELECT COALESCE(?,d),GREATEST(?,d),LEAST(?,d) FROM common_peers';
SET @p='abc';
EXECUTE p_mysql_numeric_conversion USING @p,@p,@p;
SET @p='2026-08-10 12:34:56';
EXECUTE p_mysql_numeric_conversion USING @p,@p,@p;
SET @p='1.234567';
EXECUTE p_mysql_numeric_conversion USING @p,@p,@p;
DEALLOCATE PREPARE p_mysql_numeric_conversion;

-- A legal extreme DECIMAL peer must not shrink the parameter's fractional or
-- integral domain merely to fit a fixed DECIMAL256 common representation.
PREPARE p_limit_integer FROM
  'SELECT COALESCE(?,d65i),GREATEST(?,d65i),LEAST(?,d65i) FROM decimal_limits';
SET @p='0.123456789011';
EXECUTE p_limit_integer USING @p,@p,@p;
SET @p='0.123456789012';
EXECUTE p_limit_integer USING @p,@p,@p;
SET @p='0.123456789012345678901234567890';
EXECUTE p_limit_integer USING @p,@p,@p;
DEALLOCATE PREPARE p_limit_integer;

PREPARE p_limit_fraction FROM
  'SELECT COALESCE(?,d65f),GREATEST(?,d65f),LEAST(?,d65f) FROM decimal_limits';
SET @p='123456789012.1';
EXECUTE p_limit_fraction USING @p,@p,@p;
DEALLOCATE PREPARE p_limit_fraction;

-- The exact text fallback for domains wider than DECIMAL256 must retain
-- numeric ordering rather than compare the preserved values lexically.
PREPARE p_limit_integer_order FROM
  'SELECT GREATEST(?,d65i),LEAST(?,d65i) FROM decimal_order_controls';
SET @p='10';
EXECUTE p_limit_integer_order USING @p,@p;
DEALLOCATE PREPARE p_limit_integer_order;

PREPARE p_limit_fraction_order FROM
  'SELECT GREATEST(?,d65f),LEAST(?,d65f) FROM decimal_order_controls';
SET @p='0.01';
EXECUTE p_limit_fraction_order USING @p,@p;
DEALLOCATE PREPARE p_limit_fraction_order;

-- FLOAT participates in numeric aggregation and promotes the result to DOUBLE.
PREPARE p_float FROM
  'SELECT COALESCE(?,d,f),GREATEST(?,d,f),LEAST(?,d,f) FROM common_peers';
SET @p='10';
EXECUTE p_float USING @p,@p,@p;
DEALLOCATE PREPARE p_float;

-- BOOL is TINYINT(1) for aggregation and converges with DECIMAL.
PREPARE p_bool FROM
  'SELECT COALESCE(?,d,b),GREATEST(?,d,b),LEAST(?,d,b) FROM common_peers';
EXECUTE p_bool USING @p,@p,@p;
DEALLOCATE PREPARE p_bool;

-- YEAR contributes its four-digit numeric value to the DECIMAL common type.
PREPARE p_year FROM
  'SELECT COALESCE(?,d,y),GREATEST(?,d,y),LEAST(?,d,y) FROM common_peers';
SET @p='300';
EXECUTE p_year USING @p,@p,@p;
DEALLOCATE PREPARE p_year;

-- BIT is an exact numeric peer and converges with DECIMAL without coercing a
-- fractional parameter to UNSIGNED BIGINT.
PREPARE p_bit FROM
  'SELECT COALESCE(?,d,bitv),GREATEST(?,d,bitv),LEAST(?,d,bitv) FROM common_peers';
SET @p='1.234567';
EXECUTE p_bit USING @p,@p,@p;
DEALLOCATE PREPARE p_bit;

-- ENUM remains a string aggregation boundary.
PREPARE p_enum FROM
  'SELECT COALESCE(?,d,e),GREATEST(?,d,e),LEAST(?,d,e) FROM common_peers';
SET @p='10';
EXECUTE p_enum USING @p,@p,@p;
DEALLOCATE PREPARE p_enum;

-- Explicitly typed forms are independent semantic controls.
PREPARE pc_typed FROM
  'SELECT id FROM t WHERE COALESCE(CAST(? AS DECIMAL(38,10)),d128)=d128 ORDER BY id';
SET @p='9007199254740992.0000000002';
EXECUTE pc_typed USING @p;
DEALLOCATE PREPARE pc_typed;

PREPARE pg_typed FROM
  'SELECT id FROM t WHERE GREATEST(d128,CAST(? AS DECIMAL(38,10)))=d128 ORDER BY id';
EXECUTE pg_typed USING @p;
DEALLOCATE PREPARE pg_typed;

PREPARE pl_typed FROM
  'SELECT id FROM t WHERE LEAST(d128,CAST(? AS DECIMAL(38,10)))=d128 ORDER BY id';
EXECUTE pl_typed USING @p;
DEALLOCATE PREPARE pl_typed;

-- One-row input is a cardinality boundary control.
PREPARE pc_one FROM 'SELECT id FROM t WHERE id=2 AND COALESCE(?,d128)=d128';
SET @p=NULL;
EXECUTE pc_one USING @p;
SET @p='9007199254740992.0000000002';
EXECUTE pc_one USING @p;
DEALLOCATE PREPARE pc_one;

DROP DATABASE prepare_decimal_common_type;
