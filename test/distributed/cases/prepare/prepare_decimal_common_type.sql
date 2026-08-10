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
  e ENUM('x','z')
);
INSERT INTO t VALUES
  (1,9007199254740992.01,9007199254740992.0000000001),
  (2,9007199254740992.02,9007199254740992.0000000002),
  (3,9007199254740992.03,9007199254740992.0000000003),
  (4,9007199254740993.01,9007199254740993.0000000001);
INSERT INTO common_peers VALUES (2.00,1.5,TRUE,2024,'z');

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
