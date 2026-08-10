-- @suite
-- @case
-- @desc: Exact DECIMAL comparisons with numeric string constants
-- @label:bvt

DROP DATABASE IF EXISTS decimal_string_comparison;
CREATE DATABASE decimal_string_comparison;
USE decimal_string_comparison;

CREATE TABLE boundary_values (
  id INT PRIMARY KEY,
  d64 DECIMAL(18,2),
  d128 DECIMAL(20,4)
);
INSERT INTO boundary_values VALUES
  (1, NULL, NULL),
  (2, 9007199254740991.99, 9007199254740991.9999),
  (3, 9007199254740992.00, 9007199254740992.0000),
  (4, 9007199254740992.01, 9007199254740992.0001),
  (5, 9007199254740993.00, 9007199254740992.9999),
  (6, 9007199254740993.01, 9007199254740993.0000);

SELECT GROUP_CONCAT(id ORDER BY id) AS eq_ids
FROM boundary_values WHERE d128 = '9007199254740992.0001';
SELECT GROUP_CONCAT(id ORDER BY id) AS null_safe_eq_ids
FROM boundary_values WHERE d128 <=> '9007199254740992.0001';
SELECT GROUP_CONCAT(id ORDER BY id) AS ne_ids
FROM boundary_values WHERE d128 <> '9007199254740992.0001';
SELECT GROUP_CONCAT(id ORDER BY id) AS lt_ids
FROM boundary_values WHERE d128 < '9007199254740992.0001';
SELECT GROUP_CONCAT(id ORDER BY id) AS le_ids
FROM boundary_values WHERE d128 <= '9007199254740992.0001';
SELECT GROUP_CONCAT(id ORDER BY id) AS gt_ids
FROM boundary_values WHERE d128 > '9007199254740992.0001';
SELECT GROUP_CONCAT(id ORDER BY id) AS ge_ids
FROM boundary_values WHERE d128 >= '9007199254740992.0001';

SELECT GROUP_CONCAT(id ORDER BY id) AS reversed_eq_ids
FROM boundary_values WHERE '9007199254740992.0001' = d128;
SELECT GROUP_CONCAT(id ORDER BY id) AS varchar_cast_eq_ids
FROM boundary_values WHERE d128 <=> CAST('9007199254740992.0001' AS VARCHAR);
SELECT GROUP_CONCAT(id ORDER BY id) AS decimal_cast_eq_ids
FROM boundary_values WHERE d128 <=> CAST('9007199254740992.0001' AS DECIMAL(20,4));

SELECT GROUP_CONCAT(id ORDER BY id) AS decimal64_eq_ids
FROM boundary_values WHERE d64 = '9007199254740992.01';
SELECT GROUP_CONCAT(id ORDER BY id) AS in_ids
FROM boundary_values WHERE d128 IN ('9007199254740992.0001');
SELECT GROUP_CONCAT(id ORDER BY id) AS not_in_ids
FROM boundary_values WHERE d128 NOT IN ('9007199254740992.0001');
SELECT GROUP_CONCAT(id ORDER BY id) AS reversed_in_ids
FROM boundary_values WHERE '9007199254740992.0001' IN (d128);
SELECT GROUP_CONCAT(id ORDER BY id) AS reversed_not_in_ids
FROM boundary_values WHERE '9007199254740992.0001' NOT IN (d128);

CREATE TABLE scale_values (
  id INT PRIMARY KEY,
  d DECIMAL(10,4)
);
INSERT INTO scale_values VALUES (1, 1.2345), (2, 1.2346);
SELECT GROUP_CONCAT(id ORDER BY id) AS higher_scale_lt_ids
FROM scale_values WHERE d < '1.23456';
SELECT GROUP_CONCAT(id ORDER BY id) AS higher_scale_decimal_cast_lt_ids
FROM scale_values WHERE d < CAST('1.23456' AS DECIMAL(10,5));

CREATE TABLE numeric_prefix_values (
  id INT PRIMARY KEY,
  d DECIMAL(10,0)
);
INSERT INTO numeric_prefix_values VALUES (1, 0), (2, 1), (3, 16), (4, 100);
SELECT GROUP_CONCAT(id ORDER BY id) AS hex_looking_prefix_ids
FROM numeric_prefix_values WHERE d = '0x10';
SELECT GROUP_CONCAT(id ORDER BY id) AS embedded_plus_prefix_ids
FROM numeric_prefix_values WHERE d = '1+2';
SELECT GROUP_CONCAT(id ORDER BY id) AS embedded_space_prefix_ids
FROM numeric_prefix_values WHERE d = '1 2';
SELECT GROUP_CONCAT(id ORDER BY id) AS scientific_notation_ids
FROM numeric_prefix_values WHERE d = '1e2';

CREATE TABLE decimal256_boundary (
  id INT PRIMARY KEY,
  d DECIMAL(38,30),
  KEY idx_d (d)
);
INSERT INTO decimal256_boundary VALUES
  (1, 12345678.000000000000000000000000000000);
SELECT COUNT(*) AS decimal256_lt_count
FROM decimal256_boundary
WHERE d < '12345678.0000000000000000000000000000001';
SELECT COUNT(*) AS decimal256_eq_count
FROM decimal256_boundary
WHERE d = '12345678.0000000000000000000000000000001';
SELECT COUNT(*) AS decimal256_index_lt_count
FROM decimal256_boundary FORCE INDEX (idx_d)
WHERE d < '12345678.0000000000000000000000000000001';

DROP DATABASE decimal_string_comparison;
