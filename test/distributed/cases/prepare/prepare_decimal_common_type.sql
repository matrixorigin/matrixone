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
INSERT INTO t VALUES
  (1,9007199254740992.01,9007199254740992.0000000001),
  (2,9007199254740992.02,9007199254740992.0000000002),
  (3,9007199254740992.03,9007199254740992.0000000003),
  (4,9007199254740993.01,9007199254740993.0000000001);
INSERT INTO common_peers VALUES (2.00,1.5,TRUE,2024,b'00000011','z');

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

-- A direct parameter contributes its actual runtime decimal domain, preserving
-- scale and integral width without forcing every value through DECIMAL(65,30).
PREPARE p_param_precision FROM
  'SELECT COALESCE(?,d),GREATEST(?,d),LEAST(?,d) FROM common_peers';
SET @p='1.234567';
EXECUTE p_param_precision USING @p,@p,@p;
SET @p='12345678901.23';
EXECUTE p_param_precision USING @p,@p,@p;
DEALLOCATE PREPARE p_param_precision;

-- Direct parameters in a DECIMAL common-type function use MySQL numeric-prefix
-- conversion without changing ordinary arithmetic or aggregate semantics.
PREPARE p_mysql_numeric_conversion FROM
  'SELECT COALESCE(?,d),GREATEST(?,d),LEAST(?,d) FROM common_peers';
SET @p='abc';
EXECUTE p_mysql_numeric_conversion USING @p,@p,@p;
SET @p='2026-08-10 12:34:56';
EXECUTE p_mysql_numeric_conversion USING @p,@p,@p;
SET @p='1.234567';
EXECUTE p_mysql_numeric_conversion USING @p,@p,@p;
SET @p='1e100';
EXECUTE p_mysql_numeric_conversion USING @p,@p,@p;
SET @p='1e-40';
EXECUTE p_mysql_numeric_conversion USING @p,@p,@p;
PREPARE p_exact_dynamic_domain FROM
  'SELECT COALESCE(?,CAST(2.0000000000 AS DECIMAL(46,10))),GREATEST(?,CAST(2.0000000000 AS DECIMAL(46,10))),LEAST(?,CAST(2.0000000000 AS DECIMAL(46,10)))';
SET @p='999999999999999999999999999999999999.1234567890';
EXECUTE p_exact_dynamic_domain USING @p,@p,@p;
DEALLOCATE PREPARE p_exact_dynamic_domain;

-- Integer-valued SQL PREPARE parameters use MySQL's stable DECIMAL result
-- domain, including when CREATE TABLE AS SELECT persists that metadata.
SET @p=42;
PREPARE p_integer_decimal_domain FROM
  'SELECT COALESCE(?,CAST(2 AS DECIMAL(10,2)))';
EXECUTE p_integer_decimal_domain USING @p;
DEALLOCATE PREPARE p_integer_decimal_domain;
PREPARE p_integer_decimal_ctas FROM
  'CREATE TABLE integer_decimal_ctas AS SELECT COALESCE(?,CAST(2 AS DECIMAL(10,2))) AS v';
EXECUTE p_integer_decimal_ctas USING @p;
DEALLOCATE PREPARE p_integer_decimal_ctas;
SHOW CREATE TABLE integer_decimal_ctas;
DROP TABLE integer_decimal_ctas;

SET @p='1e100tail';
EXECUTE p_mysql_numeric_conversion USING @p,@p,@p;
DEALLOCATE PREPARE p_mysql_numeric_conversion;

-- Equivalent exact numeric-prefix spellings must keep the same DECIMAL domain.
PREPARE p_wide_spelling FROM
  'SELECT COALESCE(?,CAST(0.0000000002 AS DECIMAL(10,10)))+1-COALESCE(?,CAST(0.0000000002 AS DECIMAL(10,10)))';
SET @p='999999999999999999999999999999999999';
EXECUTE p_wide_spelling USING @p,@p;
SET @p='1e35';
EXECUTE p_wide_spelling USING @p,@p;
SET @p='999999999999999999999999999999999999tail';
EXECUTE p_wide_spelling USING @p,@p;
DEALLOCATE PREPARE p_wide_spelling;

-- Oversized full and prefix spellings share the approximate numeric domain;
-- a suffix must never turn overflow into a saturated DECIMAL maximum.
PREPARE p_oversized_prefix FROM
  'SELECT COALESCE(?,CAST(0 AS DECIMAL(1,0)))';
SET @p='1e76';
EXECUTE p_oversized_prefix USING @p;
SET @p='10000000000000000000000000000000000000000000000000000000000000000000000000000tail';
EXECUTE p_oversized_prefix USING @p;
DEALLOCATE PREPARE p_oversized_prefix;

-- Native BETWEEN uses the same runtime numeric-prefix normalization as its
-- equivalent pair of scalar DECIMAL comparisons.
PREPARE p_runtime_between FROM
  'SELECT ? BETWEEN CAST(? AS DECIMAL(46,10)) AND CAST(? AS DECIMAL(46,10)),? NOT BETWEEN CAST(? AS DECIMAL(46,10)) AND CAST(? AS DECIMAL(46,10))';
SET @left='100000000000000000000000000000000000tail';
SET @bound='100000000000000000000000000000000000';
EXECUTE p_runtime_between USING @left,@bound,@bound,@left,@bound,@bound;
EXECUTE p_runtime_between USING @left,@bound,@bound,@left,@bound,@bound;
DEALLOCATE PREPARE p_runtime_between;

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

-- A multi-element IN list uses one comparison domain. Explicit OR remains the
-- control in which each equality resolves independently.
PREPARE p_mixed_in FROM 'SELECT id FROM t WHERE d128 IN (?,?) ORDER BY id';
PREPARE p_mixed_not_in FROM 'SELECT id FROM t WHERE d128 NOT IN (?,?) ORDER BY id';
PREPARE p_mixed_or FROM 'SELECT id FROM t WHERE d128=? OR d128=? ORDER BY id';
SET @exact='9007199254740992.0000000002';
SET @float_zero=CAST(0 AS DOUBLE);
EXECUTE p_mixed_in USING @exact,@float_zero;
EXECUTE p_mixed_in USING @float_zero,@exact;
EXECUTE p_mixed_not_in USING @exact,@float_zero;
EXECUTE p_mixed_or USING @exact,@float_zero;
DEALLOCATE PREPARE p_mixed_in;
DEALLOCATE PREPARE p_mixed_not_in;
DEALLOCATE PREPARE p_mixed_or;

-- A Decimal256-overflowing numeric prefix remains numeric when it has a suffix.
PREPARE p_fractional_overflow FROM
  'SELECT COALESCE(?,CAST(0 AS DECIMAL(1,0)))';
SET @overflow='999999999999999999999999999999999999.11111111111111111111111111111111111111111';
EXECUTE p_fractional_overflow USING @overflow;
SET @overflow='999999999999999999999999999999999999.11111111111111111111111111111111111111111tail';
EXECUTE p_fractional_overflow USING @overflow;
DEALLOCATE PREPARE p_fractional_overflow;

PREPARE p_fractional_overflow_ctas FROM
  'CREATE TABLE fractional_overflow_ctas AS SELECT COALESCE(?,CAST(0 AS DECIMAL(1,0))) AS v';
EXECUTE p_fractional_overflow_ctas USING @overflow;
SELECT COUNT(*) AS exact_decimal256_columns
FROM information_schema.columns
WHERE table_schema=DATABASE() AND table_name='fractional_overflow_ctas' AND column_name='v'
  AND data_type='decimal' AND numeric_precision=66 AND numeric_scale=30;
SELECT v FROM fractional_overflow_ctas;
DEALLOCATE PREPARE p_fractional_overflow_ctas;
DROP TABLE fractional_overflow_ctas;

CREATE TABLE runtime_range_bound(id INT, d DECIMAL(10,0));
INSERT INTO runtime_range_bound VALUES (1,1),(2,2),(3,3);
PREPARE p_row_bound_left FROM
  'SELECT id FROM runtime_range_bound WHERE ? BETWEEN d AND ? ORDER BY id';
PREPARE p_row_bound_high FROM
  'SELECT id FROM runtime_range_bound WHERE ? BETWEEN ? AND d ORDER BY id';
PREPARE p_row_bound_update FROM
  'UPDATE runtime_range_bound SET id=id+10 WHERE ? BETWEEN d AND ?';
SET @two='2';
EXECUTE p_row_bound_left USING @two,@two;
EXECUTE p_row_bound_high USING @two,@two;
EXECUTE p_row_bound_update USING @two,@two;
SELECT id,d FROM runtime_range_bound ORDER BY d;
DEALLOCATE PREPARE p_row_bound_left;
DEALLOCATE PREPARE p_row_bound_high;
DEALLOCATE PREPARE p_row_bound_update;
DROP TABLE runtime_range_bound;

CREATE TABLE runtime_in_real(id INT, d DECIMAL(38,10));
INSERT INTO runtime_in_real VALUES
  (1,9007199254740992.0000000001),(2,9007199254740992.0000000002),
  (3,9007199254740992.0000000003),(4,9007199254740994.0000000001),
  (5,9007199254740995.0000000001),(6,9007199254740996.0000000001);
PREPARE p_in_real FROM 'SELECT id FROM runtime_in_real WHERE d IN (?,?) ORDER BY id';
PREPARE p_not_in_real FROM 'SELECT id FROM runtime_in_real WHERE d NOT IN (?,?) ORDER BY id';
PREPARE p_or_real FROM 'SELECT id FROM runtime_in_real WHERE d=? OR d=? ORDER BY id';
PREPARE p_in_real_ctas FROM
  'CREATE TABLE runtime_in_real_ctas AS SELECT id FROM runtime_in_real WHERE d IN (?,?)';
PREPARE p_in_real_update FROM 'UPDATE runtime_in_real SET id=id+10 WHERE d IN (?,?)';
SET @real=CAST(9007199254740992 AS DOUBLE);
SET @zero='0';
SELECT id FROM runtime_in_real WHERE d IN (9007199254740992e0,'0') ORDER BY id;
SELECT id FROM runtime_in_real WHERE d NOT IN (9007199254740992e0,'0') ORDER BY id;
SELECT id FROM runtime_in_real
  WHERE d IN ((9007199254740992e0+0e0),'0') ORDER BY id;
CREATE TABLE runtime_in_static_ctas AS
  SELECT id FROM runtime_in_real WHERE d IN (9007199254740992e0,'0');
SELECT id FROM runtime_in_static_ctas ORDER BY id;
EXECUTE p_in_real USING @real,@zero;
EXECUTE p_not_in_real USING @real,@zero;
EXECUTE p_or_real USING @real,@zero;
EXECUTE p_in_real_ctas USING @real,@zero;
SELECT id FROM runtime_in_real_ctas ORDER BY id;
EXECUTE p_in_real_update USING @real,@zero;
SELECT id FROM runtime_in_real ORDER BY id;
UPDATE runtime_in_real SET id=id+100 WHERE d IN (9007199254740992e0,'0');
SELECT id FROM runtime_in_real ORDER BY id;
DEALLOCATE PREPARE p_in_real;
DEALLOCATE PREPARE p_not_in_real;
DEALLOCATE PREPARE p_or_real;
DEALLOCATE PREPARE p_in_real_ctas;
DEALLOCATE PREPARE p_in_real_update;
DROP TABLE runtime_in_real_ctas;
DROP TABLE runtime_in_static_ctas;
DROP TABLE runtime_in_real;

PREPARE p_leading_zero FROM 'SELECT COALESCE(?,CAST(2 AS DECIMAL(1,0)))';
SET @leading=REPEAT('0',77);
EXECUTE p_leading_zero USING @leading;
SET @leading=CONCAT(REPEAT('0',76),'1');
EXECUTE p_leading_zero USING @leading;
DEALLOCATE PREPARE p_leading_zero;

PREPARE p_tiny FROM
  'SELECT COALESCE(?,CAST(2 AS DECIMAL(1,0))),LEAST(?,CAST(2 AS DECIMAL(1,0)))';
SET @tiny='1e-100';
EXECUTE p_tiny USING @tiny,@tiny;
SET @tiny=CONCAT('0.',REPEAT('0',99),'1');
EXECUTE p_tiny USING @tiny,@tiny;
PREPARE p_tiny_ctas FROM
  'CREATE TABLE runtime_tiny_ctas AS SELECT COALESCE(?,CAST(2 AS DECIMAL(1,0))) AS v';
EXECUTE p_tiny_ctas USING @tiny;
SHOW CREATE TABLE runtime_tiny_ctas;
SELECT v FROM runtime_tiny_ctas;
DEALLOCATE PREPARE p_tiny_ctas;
DROP TABLE runtime_tiny_ctas;
DEALLOCATE PREPARE p_tiny;

PREPARE p_extreme FROM
  'SELECT GREATEST(?,?,CAST(0 AS DECIMAL(38,10))),LEAST(?,?,CAST(0 AS DECIMAL(38,10))),COALESCE(?,?,CAST(0 AS DECIMAL(38,10)))';
SET @extreme='9007199254740992.0000000002';
SET @float=CAST(0 AS DOUBLE);
EXECUTE p_extreme USING @extreme,@float,@extreme,@float,@extreme,@float;
PREPARE p_extreme_ctas FROM
  'CREATE TABLE runtime_extreme_ctas AS SELECT GREATEST(?,?,CAST(0 AS DECIMAL(38,10))) AS v';
EXECUTE p_extreme_ctas USING @extreme,@float;
SHOW CREATE TABLE runtime_extreme_ctas;
SELECT v FROM runtime_extreme_ctas;
DEALLOCATE PREPARE p_extreme_ctas;
DROP TABLE runtime_extreme_ctas;
DEALLOCATE PREPARE p_extreme;

DROP DATABASE prepare_decimal_common_type;
