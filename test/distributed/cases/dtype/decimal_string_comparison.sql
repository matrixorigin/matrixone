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
WHERE d = CAST('9007199254740992.0001x' AS CHAR(21)) ORDER BY id;
SELECT id FROM boundary_values
WHERE d = CAST('9007199254740992.0001x' AS VARCHAR(21)) ORDER BY id;
SELECT id FROM boundary_values
WHERE d = CONCAT('9007199254740992.', '0001') ORDER BY id;

-- Binary string domains retain their generic approximate comparison domain.
SELECT id FROM boundary_values
WHERE d = _binary '9007199254740992.0001' ORDER BY id;
SELECT id FROM boundary_values
WHERE d = CAST('9007199254740992.0001' AS BINARY(21)) ORDER BY id;
SELECT id FROM boundary_values
WHERE d = CAST('9007199254740992.0001' AS VARBINARY(21)) ORDER BY id;
SELECT id FROM boundary_values
WHERE d = CAST(CONCAT('9007199254740992.', '0001') AS VARBINARY(21)) ORDER BY id;

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
SELECT id FROM boundary_values
WHERE d = CONCAT(s, '') ORDER BY id;

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

-- Redundant coefficient zeroes must be removed before the DECIMAL256 bound is checked.
CREATE TABLE normalized_values (id INT PRIMARY KEY, d DECIMAL(20,0));
INSERT INTO normalized_values VALUES
  (1, 90071992547409920001),
  (2, 90071992547409920002);
SELECT id FROM normalized_values
WHERE d = '90071992547409920001000000000000000000000000000000000000000000000000000000000e-57'
ORDER BY id;

-- Prefixes and extension tokens stay in the runtime DOUBLE coercion path.
CREATE TABLE token_values (id INT PRIMARY KEY, d DECIMAL(20,4));
INSERT INTO token_values VALUES (1, 16), (2, 100);
SELECT id FROM token_values WHERE d = X'10' ORDER BY id;
SELECT id FROM token_values WHERE d = b'10000' ORDER BY id;
SELECT id FROM token_values WHERE d = '0x10' ORDER BY id;
SELECT id FROM token_values WHERE d = CONCAT('0x', '10') ORDER BY id;
SELECT id FROM token_values WHERE d = CONCAT('1e2', 'suffix') ORDER BY id;
SHOW WARNINGS;
SELECT id FROM token_values WHERE d = CONCAT('1e2', 'suffix') OR d = CONCAT('1e3', 'suffix') ORDER BY id;
SHOW COUNT(*) WARNINGS;
SELECT id FROM token_values WHERE d = CONCAT('1e3', 'suffix') ORDER BY id;
SHOW COUNT(*) WARNINGS;
-- One implicit scalar conversion keeps one owner through JOIN and UNION.
SELECT COUNT(*) FROM token_values a JOIN token_values b ON a.d = b.d
WHERE a.d = CONCAT('1e2', 'suffix');
SHOW COUNT(*) WARNINGS;
SELECT COUNT(*) FROM (SELECT d FROM token_values UNION ALL SELECT d FROM token_values) u
WHERE d = CONCAT('1e2', 'suffix');
SHOW COUNT(*) WARNINGS;
SELECT COUNT(*) FROM token_values WHERE d = CASE WHEN TRUE THEN '1e2suffix' ELSE '2suffix' END;
SHOW COUNT(*) WARNINGS;
SELECT COUNT(*) FROM token_values a JOIN token_values b
ON a.d = b.d AND a.d = CASE WHEN TRUE THEN '1e2suffix' ELSE '2suffix' END;
SHOW COUNT(*) WARNINGS;
-- HashBuild may calculate a constant ON key before the probe has a row. The
-- diagnostic belongs to the join's logical evaluation, including no-match and
-- null-key cases, not to speculative key preparation or a runtime filter.
CREATE TABLE empty_token_values (id INT PRIMARY KEY, d DECIMAL(20,4));
CREATE TABLE distant_token_values (id INT PRIMARY KEY, d DECIMAL(20,4));
INSERT INTO distant_token_values VALUES (1, 200);
SELECT COUNT(*) FROM empty_token_values a JOIN token_values b
ON a.d = b.d + CASE WHEN TRUE THEN '0suffix' ELSE '1suffix' END;
SHOW COUNT(*) WARNINGS;
SELECT COUNT(*) FROM token_values a JOIN empty_token_values b
ON a.d = b.d + CASE WHEN TRUE THEN '0suffix' ELSE '1suffix' END;
SHOW COUNT(*) WARNINGS;
SELECT COUNT(*) FROM token_values a JOIN token_values b
ON a.d = b.d + CASE WHEN TRUE THEN '0suffix' ELSE '1suffix' END;
SHOW COUNT(*) WARNINGS;
SELECT COUNT(*) FROM distant_token_values a JOIN token_values b
ON a.d = b.d + CASE WHEN TRUE THEN '0suffix' ELSE '1suffix' END;
SHOW COUNT(*) WARNINGS;
SELECT COUNT(*) FROM token_values a JOIN token_values b
ON a.d = b.d + CASE WHEN FALSE THEN '0suffix' ELSE '0' END;
SHOW COUNT(*) WARNINGS;
SELECT COUNT(*) FROM token_values a JOIN token_values b
ON a.id = b.id AND a.d = b.d + CASE WHEN TRUE THEN '0suffix' ELSE '1suffix' END;
SHOW COUNT(*) WARNINGS;
CREATE TABLE join_time_values (id INT PRIMARY KEY, v TIME);
INSERT INTO join_time_values VALUES (1, '00:00:00');
SELECT COUNT(*) FROM join_time_values a JOIN join_time_values b
ON a.v = ADDTIME(b.v, ADDTIME('838:59:59', '00:00:01'));
SHOW COUNT(*) WARNINGS;
DROP TABLE join_time_values;
PREPARE join_diagnostic_stmt FROM
'SELECT COUNT(*) FROM empty_token_values a JOIN token_values b ON a.d = b.d + CASE WHEN TRUE THEN ''0suffix'' ELSE ''1suffix'' END';
EXECUTE join_diagnostic_stmt;
SHOW COUNT(*) WARNINGS;
INSERT INTO empty_token_values VALUES (1, 16), (2, 100);
EXECUTE join_diagnostic_stmt;
SHOW COUNT(*) WARNINGS;
DELETE FROM empty_token_values;
EXECUTE join_diagnostic_stmt;
SHOW COUNT(*) WARNINGS;
DEALLOCATE PREPARE join_diagnostic_stmt;
SET @join_diagnostic_old_mode = @@session.sql_mode;
SET SESSION sql_mode = 'MATRIXONE_NATIVE';
SELECT COUNT(*) FROM empty_token_values a JOIN token_values b
ON a.d = b.d + CASE WHEN TRUE THEN '1e2suffix' ELSE '0' END;
SHOW COUNT(*) WARNINGS;
SELECT COUNT(*) FROM token_values a JOIN token_values b
ON a.d = b.d + CASE WHEN TRUE THEN '1e2suffix' ELSE '0' END;
SET SESSION sql_mode = @join_diagnostic_old_mode;
SET @join_diagnostic_old_mode = NULL;
DROP TABLE empty_token_values, distant_token_values;
-- User-written casts in a projector retain row-level conversion diagnostics.
SELECT CAST('12suffix' AS DOUBLE) FROM token_values;
SHOW COUNT(*) WARNINGS;
SELECT id FROM token_values WHERE d = CONCAT('0x', '10foo') ORDER BY id;

CREATE TABLE adjusted_values (d DOUBLE);
INSERT IGNORE INTO adjusted_values SELECT '12suffix' FROM token_values;
SHOW COUNT(*) WARNINGS;
SELECT COUNT(*), MIN(d), MAX(d) FROM adjusted_values;
DROP TABLE adjusted_values;

SET @decimal_string_old_sql_mode = @@session.sql_mode;
SET SESSION sql_mode = 'MATRIXONE_NATIVE';
SELECT id FROM token_values WHERE d = CONCAT('0x', '10') ORDER BY id;
SELECT id FROM token_values WHERE d = CONCAT('1e2', 'suffix') ORDER BY id;
SET SESSION sql_mode = @decimal_string_old_sql_mode;
SELECT @@session.sql_mode = @decimal_string_old_sql_mode AS sql_mode_restored;
SET @decimal_string_old_sql_mode = NULL;

DELETE FROM token_values;
SELECT id FROM token_values WHERE d = CONCAT('1e2', 'suffix') ORDER BY id;
SHOW COUNT(*) WARNINGS;

DROP DATABASE decimal_string_comparison;
