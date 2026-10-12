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

-- A scalar subquery retains the projected expression's comparison domain.
CREATE TABLE scalar_values (id INT PRIMARY KEY, s VARCHAR(20) COLLATE utf8mb4_unicode_ci);
INSERT INTO scalar_values VALUES (1, 'Á');
SELECT s = 'a', (SELECT s FROM scalar_values WHERE id = 1) = 'a',
       'a' = (SELECT s FROM scalar_values WHERE id = 1) FROM scalar_values;
SELECT (SELECT n.s FROM scalar_values n WHERE n.id = t.id) = 'a' FROM scalar_values t;
SELECT (SELECT s FROM scalar_values WHERE id = 2) = 'a',
       (SELECT s FROM scalar_values WHERE id = 2) <=> NULL;

-- Width promotion is transparent; a SQL CAST has its own implicit priority.
SELECT LEAST('a' COLLATE utf8mb4_bin, 'bb') = 'a' COLLATE utf8mb4_general_ci;
SELECT GREATEST('a' COLLATE utf8mb4_bin, 'bb') = 'bb' COLLATE utf8mb4_general_ci;
SELECT LEAST('a' COLLATE utf8mb4_bin, 'b') = 'a' COLLATE utf8mb4_general_ci;
SELECT LEAST('Á' COLLATE utf8mb4_unicode_ci, 'zz') = 'a';
SELECT CAST(LEAST('a' COLLATE utf8mb4_bin, 'bb') AS CHAR(20)) = 'a' COLLATE utf8mb4_general_ci;

-- COLLATE qualifies a literal without admitting arbitrary folded expressions.
SELECT NAME_CONST('v', 'Á' COLLATE utf8mb4_unicode_ci) = 'a';
SELECT sub.v = 'a' FROM (SELECT NAME_CONST('v', 'Á' COLLATE utf8mb4_unicode_ci)) sub;
SELECT NAME_CONST('v', 1 COLLATE utf8mb4_unicode_ci) = '1';
SELECT NAME_CONST('v', 'a' COLLATE utf8mb4_unicode_ci) = 'a' COLLATE utf8mb4_bin;
SELECT NAME_CONST('v', LOWER('A') COLLATE utf8mb4_unicode_ci);
SELECT NAME_CONST('v', CAST('a' AS CHAR) COLLATE utf8mb4_unicode_ci);

-- COLLATE admission uses the source domain before column assignment.
CREATE TABLE assignment_values (id INT PRIMARY KEY, t VARCHAR(4), b VARBINARY(4));
INSERT INTO assignment_values VALUES
       (1, _binary'a' COLLATE 'binary', _binary'b' COLLATE 'binary'),
       (2, 'c', _binary'd');
REPLACE INTO assignment_values VALUES (1, _binary'e' COLLATE 'binary', 'f' COLLATE utf8mb4_unicode_ci);
SELECT id, HEX(t), HEX(b) FROM assignment_values ORDER BY id;
INSERT INTO assignment_values (id, t) VALUES (3, _binary'a' COLLATE utf8mb4_unicode_ci);
REPLACE INTO assignment_values (id, t) VALUES (1, _binary'a' COLLATE utf8mb4_unicode_ci);
INSERT INTO assignment_values (id, b) VALUES (4, 'a' COLLATE 'binary');
REPLACE INTO assignment_values (id, b) VALUES (1, 'a' COLLATE 'binary');
SELECT id, HEX(t), HEX(b) FROM assignment_values ORDER BY id;

-- PAD SPACE adaptation also keeps explicit provenance across derived CHAR.
CREATE TABLE pad_values (c CHAR(3));
INSERT INTO pad_values VALUES ('a');
SELECT LEAST(COALESCE(c COLLATE utf8mb4_bin, 'a'), COALESCE(c, 'bb'))
       = 'a' COLLATE utf8mb4_general_ci FROM pad_values;
SELECT LEAST(COALESCE(CAST('a' AS CHAR(3)) COLLATE utf8mb4_bin, 'a'),
             COALESCE(CAST('bb' AS CHAR(3)), 'bb')) = 'a' COLLATE utf8mb4_general_ci;
SELECT GREATEST(COALESCE(CAST('a' AS CHAR(3)) COLLATE utf8mb4_bin, 'a'),
                COALESCE(CAST('bb' AS CHAR(3)), 'bb')) = 'bb' COLLATE utf8mb4_general_ci;
SELECT LEAST(IFNULL(CAST('a' AS CHAR(3)) COLLATE utf8mb4_bin, 'a'),
             IFNULL(CAST('b' AS CHAR(4)), 'b')) = 'a' COLLATE utf8mb4_general_ci;
SELECT LEAST(COALESCE(CAST('a' AS CHAR(3)) COLLATE utf8mb4_bin, 'a'),
             COALESCE(CAST('bb' AS CHAR(3)), 'bb')) = 'a';
SELECT LEAST(COALESCE(CAST(NULL AS CHAR(3)) COLLATE utf8mb4_unicode_ci, 'Á'), 'zz') = 'a';
SELECT CAST(LEAST(COALESCE(c COLLATE utf8mb4_bin, 'a'), COALESCE(c, 'bb')) AS CHAR(20))
       = 'a' COLLATE utf8mb4_general_ci FROM pad_values;
DROP DATABASE collation_expression_test;
