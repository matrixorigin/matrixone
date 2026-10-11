-- @suite
-- @case
-- @desc: Explicit collation domains, conflict errors and unchanged source values
-- @label:bvt
DROP DATABASE IF EXISTS collation_expression_test;
CREATE DATABASE collation_expression_test;
USE collation_expression_test;

-- Explicit native comparison overrides weaker text operands in either order.
SELECT 'Á' COLLATE utf8mb4_unicode_ci = 'a',
       'a' = 'Á' COLLATE utf8mb4_unicode_ci,
       'a ' COLLATE utf8mb4_unicode_ci = 'a',
       _binary'a ' = _binary'a';
SELECT 'Á' COLLATE utf8mb4_unicode_ci IN ('a', 'z'),
       'Á' COLLATE utf8mb4_unicode_ci BETWEEN 'a' AND 'b';
SELECT LEAST('a' COLLATE utf8mb4_unicode_ci, 'B'),
       GREATEST('a' COLLATE utf8mb4_unicode_ci, 'B'),
       CONCAT('a' COLLATE utf8mb4_unicode_ci, 'B');
SELECT COALESCE(NULL, 'Á' COLLATE utf8mb4_unicode_ci) = 'a';
SELECT 'a' COLLATE utf8mb4_bin = 'a' COLLATE utf8mb4_bin;
SELECT CASE WHEN TRUE THEN 'Á' COLLATE utf8mb4_unicode_ci
            ELSE CAST('bad' AS SIGNED) END = 'a';

-- Conflict checking also applies to derived expressions and set columns.
SELECT 'a' COLLATE utf8mb4_bin = 'a' COLLATE utf8mb4_general_ci;
SELECT CONCAT('a' COLLATE utf8mb4_bin, 'b' COLLATE utf8mb4_general_ci);
SELECT CASE WHEN TRUE THEN 'a' COLLATE utf8mb4_bin
            ELSE 'b' COLLATE utf8mb4_general_ci END;
SELECT COALESCE('a' COLLATE utf8mb4_bin, 'b' COLLATE utf8mb4_general_ci);
SELECT LEAST('a' COLLATE utf8mb4_bin, 'b' COLLATE utf8mb4_general_ci);
SELECT 'a' COLLATE utf8mb4_bin UNION ALL SELECT 'b' COLLATE utf8mb4_general_ci;
SELECT 'a' COLLATE nonexistent_ci;
SELECT _binary'a' COLLATE utf8mb4_unicode_ci;
SELECT 1 COLLATE utf8mb4_unicode_ci;
SELECT NULL COLLATE utf8mb4_unicode_ci;

CREATE TABLE source_values (id INT PRIMARY KEY, v VARCHAR(20), KEY k_v(v));
INSERT INTO source_values VALUES (1, 'Á'), (2, 'a '), (3, 'z');
-- Legacy source identities are preserved. Explicit expression conversion must
-- compare at the resolved domain without probing a raw-byte key as a UCA key.
SELECT id, v FROM source_values WHERE v COLLATE utf8mb4_unicode_ci = 'a' ORDER BY id;
SELECT id, v FROM source_values WHERE 'a' = v COLLATE utf8mb4_unicode_ci ORDER BY id;
SELECT id, v FROM source_values WHERE v COLLATE utf8mb4_unicode_ci IN ('a', 'z') ORDER BY id;
SELECT id, v COLLATE utf8mb4_unicode_ci AS original_v FROM source_values ORDER BY id;
SELECT id, v FROM source_values WHERE v = 'a' ORDER BY id;
CREATE VIEW expression_view AS SELECT id, v COLLATE utf8mb4_unicode_ci AS v FROM source_values;
SELECT id, v FROM expression_view WHERE v = 'a' ORDER BY id;
CREATE TABLE expression_copy AS SELECT id, v COLLATE utf8mb4_unicode_ci AS v FROM source_values;
SELECT id, v FROM expression_copy WHERE v = 'a' ORDER BY id;
SELECT id, v FROM expression_copy WHERE v = 'Á' COLLATE utf8mb4_bin ORDER BY id;
SELECT v FROM (SELECT 'Á' COLLATE utf8mb4_unicode_ci AS v UNION ALL SELECT 'a ') AS u WHERE v = 'a' ORDER BY HEX(v);
SELECT v FROM (VALUES ROW('Á' COLLATE utf8mb4_unicode_ci), ROW('a ')) AS d(v) WHERE v = 'a' ORDER BY HEX(v);
DROP INDEX k_v ON source_values;
SELECT id, v FROM source_values WHERE v COLLATE utf8mb4_unicode_ci = 'a' ORDER BY id;

PREPARE expression_stmt FROM 'SELECT ? COLLATE utf8mb4_unicode_ci = ?';
SET @left = 'Á', @right = 'a';
EXECUTE expression_stmt USING @left, @right;
SET @left = NULL;
EXECUTE expression_stmt USING @left, @right;
SET @left = 'z';
EXECUTE expression_stmt USING @left, @right;
SET @left = 'a ';
EXECUTE expression_stmt USING @left, @right;
SET @left = _binary'Á';
EXECUTE expression_stmt USING @left, @right;
DEALLOCATE PREPARE expression_stmt;
PREPARE expression_union FROM 'SELECT v FROM (SELECT ? COLLATE utf8mb4_unicode_ci AS v UNION ALL SELECT ?) u WHERE v=''a'' ORDER BY HEX(v)';
SET @left = 'Á', @right = 'a ';
EXECUTE expression_union USING @left, @right;
SET @right = _binary'a ';
EXECUTE expression_union USING @left, @right;
DEALLOCATE PREPARE expression_union;
PREPARE expression_values FROM 'SELECT v FROM (VALUES ROW(? COLLATE utf8mb4_unicode_ci), ROW(?)) d(v) WHERE v=''a'' ORDER BY HEX(v)';
SET @right = 'a ';
EXECUTE expression_values USING @left, @right;
SET @right = _binary'a ';
EXECUTE expression_values USING @left, @right;
DEALLOCATE PREPARE expression_values;
DROP DATABASE collation_expression_test;
