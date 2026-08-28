-- @suite
-- @case
-- @desc: Exact DECIMAL comparisons with numeric string constants
-- @label:bvt

DROP DATABASE IF EXISTS decimal_string_comparison;
CREATE DATABASE decimal_string_comparison;
USE decimal_string_comparison;

CREATE TABLE boundary_values (
  id INT PRIMARY KEY,
  d DECIMAL(20,4),
  s VARCHAR(32),
  KEY idx_d (d)
);
INSERT INTO boundary_values VALUES
  (1, 9007199254740992.0000, '9007199254740992.0001'),
  (2, 9007199254740992.0001, '9007199254740992.0001'),
  (3, 9007199254740992.9999, '9007199254740992.0001');

SELECT id FROM boundary_values
WHERE d = '9007199254740992.0001' ORDER BY id;
SELECT id FROM boundary_values
WHERE d <=> '9007199254740992.0001' ORDER BY id;
SELECT id FROM boundary_values
WHERE d <> '9007199254740992.0001' ORDER BY id;
SELECT id FROM boundary_values
WHERE d < '9007199254740992.0001' ORDER BY id;
SELECT id FROM boundary_values
WHERE d <= '9007199254740992.0001' ORDER BY id;
SELECT id FROM boundary_values
WHERE d > '9007199254740992.0001' ORDER BY id;
SELECT id FROM boundary_values
WHERE d >= '9007199254740992.0001' ORDER BY id;

SELECT id FROM boundary_values
WHERE '9007199254740992.0001' = d ORDER BY id;
SELECT id FROM boundary_values
WHERE d = CAST('9007199254740992.0001' AS VARCHAR) ORDER BY id;
SELECT id FROM boundary_values
WHERE d = CAST('9007199254740992.0001' AS DECIMAL(20,4)) ORDER BY id;
SELECT id FROM boundary_values FORCE INDEX (idx_d)
WHERE d = '9007199254740992.0001' ORDER BY id;

SELECT id FROM boundary_values
WHERE d IN ('9007199254740992.0001') ORDER BY id;
SELECT id FROM boundary_values
WHERE d NOT IN ('9007199254740992.0001') ORDER BY id;
SELECT id FROM boundary_values
WHERE '9007199254740992.0001' IN (d) ORDER BY id;

-- Multiple string candidates retain their common approximate domain.
SELECT id FROM boundary_values
WHERE d IN ('9007199254740992.0001', '9007199254740992.9999') ORDER BY id;
SELECT id FROM boundary_values
WHERE d NOT IN ('9007199254740992.0001', '9007199254740992.9999') ORDER BY id;

-- A runtime VARCHAR expression remains in the generic approximate domain.
SELECT id FROM boundary_values
WHERE d = s ORDER BY id;

CREATE TABLE scale_values (id INT PRIMARY KEY, d DECIMAL(10,4));
INSERT INTO scale_values VALUES (1, 1.2345), (2, 1.2346);
SELECT id FROM scale_values WHERE d < '1.23456' ORDER BY id;
SELECT id FROM scale_values
WHERE d < CAST('1.23456' AS DECIMAL(10,5)) ORDER BY id;

CREATE TABLE wide_values (
  id INT PRIMARY KEY,
  d DECIMAL(38,30),
  KEY idx_d (d)
);
INSERT INTO wide_values VALUES
  (1, 12345678.000000000000000000000000000000);
SELECT id FROM wide_values
WHERE d < '12345678.0000000000000000000000000000001' ORDER BY id;
SELECT id FROM wide_values FORCE INDEX (idx_d)
WHERE d < '12345678.0000000000000000000000000000001' ORDER BY id;

DROP DATABASE decimal_string_comparison;
