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

SELECT GROUP_CONCAT(id ORDER BY id) AS zero_large_exponent_ids
FROM numeric_prefix_values WHERE d = '0e10000';
SELECT GROUP_CONCAT(id ORDER BY id) AS zero_small_exponent_ids
FROM numeric_prefix_values WHERE d = '0e-10000';
SELECT GROUP_CONCAT(id ORDER BY id) AS large_exponent_lt_ids
FROM numeric_prefix_values WHERE d < '1e10000';
SELECT GROUP_CONCAT(id ORDER BY id) AS tiny_exponent_eq_ids
FROM numeric_prefix_values WHERE d = '1e-10000';
SELECT GROUP_CONCAT(id ORDER BY id) AS redundant_leading_zero_ids
FROM numeric_prefix_values
WHERE d = '000000000000000000000000000000000000000000000000000000000000000000000000000000001';
SELECT GROUP_CONCAT(id ORDER BY id) AS trailing_point_ids
FROM numeric_prefix_values WHERE d = '1.';
SELECT GROUP_CONCAT(id ORDER BY id) AS redundant_fraction_zero_ids
FROM numeric_prefix_values
WHERE d = '0.00000000000000000000000000000000000000000000000000000000000000000000000000000000';
SELECT GROUP_CONCAT(id ORDER BY id) AS out_of_domain_scale_ids
FROM numeric_prefix_values
WHERE d = '0.00000000000000000000000000000000000000000000000000000000000000000000000000000001';

SELECT GROUP_CONCAT(id ORDER BY id) AS lower_constant_ids
FROM boundary_values WHERE d128 = LOWER('9007199254740992.0001');
SELECT GROUP_CONCAT(id ORDER BY id) AS concat_constant_ids
FROM boundary_values WHERE d128 = CONCAT('9007199254740992.000', '1');
SELECT GROUP_CONCAT(id ORDER BY id) AS case_constant_ids
FROM boundary_values
WHERE d128 = CASE WHEN 1 = 1 THEN '9007199254740992.0001' ELSE '0' END;

SET @runtime_decimal_string = '9007199254740992.0001';
SELECT GROUP_CONCAT(id ORDER BY id) AS variable_real_fallback_ids
FROM boundary_values WHERE d128 = @runtime_decimal_string;
CREATE TABLE runtime_strings (s VARCHAR(64));
INSERT INTO runtime_strings VALUES ('9007199254740992.0001');
SELECT GROUP_CONCAT(b.id ORDER BY b.id) AS concat_runtime_real_fallback_ids
FROM boundary_values b JOIN runtime_strings r ON b.d128 = CONCAT(r.s, '');

SELECT GROUP_CONCAT(id ORDER BY id) AS multi_in_real_fallback_ids
FROM boundary_values
WHERE d128 IN ('9007199254740992.0001', '9007199254740992.9999');
SELECT GROUP_CONCAT(id ORDER BY id) AS multi_not_in_real_fallback_ids
FROM boundary_values
WHERE d128 NOT IN ('9007199254740992.0001', '9007199254740992.9999');

CREATE TABLE multi_in_update LIKE boundary_values;
ALTER TABLE multi_in_update ADD COLUMN matched INT DEFAULT 0;
INSERT INTO multi_in_update (id, d64, d128) SELECT id, d64, d128 FROM boundary_values;
UPDATE multi_in_update SET matched = 1
WHERE d128 IN ('9007199254740992.0001', '9007199254740992.9999');
SELECT GROUP_CONCAT(id ORDER BY id) AS multi_in_update_ids
FROM multi_in_update WHERE matched = 1;
DELETE FROM multi_in_update
WHERE d128 NOT IN ('9007199254740992.0001', '9007199254740992.9999');
SELECT GROUP_CONCAT(id ORDER BY id) AS multi_not_in_delete_remaining_ids
FROM multi_in_update;

CREATE TABLE negative_values (
  id INT PRIMARY KEY,
  d DECIMAL(10,2),
  KEY idx_d (d)
);
INSERT INTO negative_values VALUES (1, -1.20), (2, 0.00), (3, 1.20);
SELECT GROUP_CONCAT(id ORDER BY id) AS negative_eq_ids
FROM negative_values WHERE d = '-1.200';
SELECT GROUP_CONCAT(id ORDER BY id) AS negative_null_safe_eq_ids
FROM negative_values WHERE d <=> '-1.200';
SELECT GROUP_CONCAT(id ORDER BY id) AS negative_ne_ids
FROM negative_values WHERE d <> '-1.200';
SELECT GROUP_CONCAT(id ORDER BY id) AS negative_index_eq_ids
FROM negative_values FORCE INDEX (idx_d) WHERE d = '-1.200';

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
