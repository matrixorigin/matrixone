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
-- Numeric comparisons do not resolve the text operands' conflicting collations.
SELECT 5 BETWEEN '0' COLLATE utf8mb4_bin AND '9' COLLATE utf8mb4_general_ci,
       5 IN ('5' COLLATE utf8mb4_bin, '7' COLLATE utf8mb4_general_ci),
       5 NOT IN ('5' COLLATE utf8mb4_bin, '7' COLLATE utf8mb4_general_ci);
SELECT '5' COLLATE utf8mb4_bin BETWEEN '0' COLLATE utf8mb4_general_ci AND 9;
SELECT CAST(NULL AS SIGNED) BETWEEN '0' COLLATE utf8mb4_bin AND '9' COLLATE utf8mb4_general_ci;
SELECT LEAST('a' COLLATE utf8mb4_unicode_ci, 'B'),
       GREATEST('a' COLLATE utf8mb4_unicode_ci, 'B'),
       CONCAT('a' COLLATE utf8mb4_unicode_ci, 'B');
-- A NULL operand never determines the string comparison domain.
SELECT LEAST(NULL, CAST('a' AS CHAR(1))) AS least_char_null,
       GREATEST(NULL, CAST('a' AS CHAR(1))) AS greatest_char_null,
       LEAST(CAST('a' AS CHAR(1)), NULL) AS least_char_last_null,
       GREATEST(CAST('a' AS CHAR(1)), NULL) AS greatest_char_last_null;
SELECT LEAST(NULL, 'a' COLLATE utf8mb4_unicode_ci) AS least_native_null,
       GREATEST(NULL, 'a' COLLATE utf8mb4_unicode_ci) AS greatest_native_null,
       LEAST('a' COLLATE utf8mb4_unicode_ci, NULL, 'B') AS least_middle_null,
       GREATEST('a' COLLATE utf8mb4_unicode_ci, NULL, 'B') AS greatest_middle_null;
SELECT LEAST(NULL, NULL) AS least_all_null,
       GREATEST(CAST(NULL AS CHAR(1)), CAST(NULL AS CHAR(1))) AS greatest_typed_null;
-- A folded view still persists its pre-fold collation ordering requirement.
CREATE VIEW least_protocol_view AS
       SELECT LEAST('a' COLLATE utf8mb4_unicode_ci, 'B') AS lo,
              GREATEST('a' COLLATE utf8mb4_unicode_ci, 'B') AS hi;
SELECT lo, hi FROM least_protocol_view;
SELECT CAST(JSON_UNQUOTE(JSON_EXTRACT(viewdef, '$.required_protocol_version')) AS SIGNED) AS required_protocol
       FROM mo_catalog.mo_tables
       WHERE reldatabase = 'collation_expression_test' AND relname = 'least_protocol_view';
DROP VIEW least_protocol_view;
SELECT COALESCE(NULL, 'Á' COLLATE utf8mb4_unicode_ci) = 'a';
SELECT 'a' COLLATE utf8mb4_bin = 'a' COLLATE utf8mb4_bin;
SELECT CASE WHEN TRUE THEN 'Á' COLLATE utf8mb4_unicode_ci
            ELSE CAST('bad' AS SIGNED) END = 'a';

-- Conflict checking also applies to derived expressions and set columns.
SELECT 'a' COLLATE utf8mb4_bin = 'a' COLLATE utf8mb4_general_ci;
SELECT '5' COLLATE utf8mb4_bin IN ('7' COLLATE utf8mb4_general_ci, 5);
SELECT '5' BETWEEN '0' COLLATE utf8mb4_bin AND '9' COLLATE utf8mb4_general_ci;
SELECT CAST(NULL AS CHAR) BETWEEN '0' COLLATE utf8mb4_bin AND '9' COLLATE utf8mb4_general_ci;
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
SELECT id, id BETWEEN '0' COLLATE utf8mb4_bin AND '9' COLLATE utf8mb4_general_ci,
       id IN ('1' COLLATE utf8mb4_bin, '2' COLLATE utf8mb4_general_ci)
       FROM source_values ORDER BY id;
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

-- A numeric parameter retains the numeric comparison domain on each EXECUTE.
PREPARE numeric_collation FROM 'SELECT CAST(? AS SIGNED) BETWEEN ''0'' COLLATE utf8mb4_bin AND ''9'' COLLATE utf8mb4_general_ci';
SET @numeric_value = 5;
EXECUTE numeric_collation USING @numeric_value;
SET @numeric_value = 10;
EXECUTE numeric_collation USING @numeric_value;
SET @numeric_value = NULL;
EXECUTE numeric_collation USING @numeric_value;
SET @numeric_value = 5;
EXECUTE numeric_collation USING @numeric_value;
DEALLOCATE PREPARE numeric_collation;
SET @numeric_value = NULL;

-- COLLATE changes the rule within the source charset, not its encoding.
CREATE TABLE charset_values (s3 VARCHAR(4) COLLATE utf8_unicode_ci, s4 VARCHAR(4) COLLATE utf8mb4_unicode_ci);
INSERT INTO charset_values VALUES ('a', 'a');
SELECT s3 COLLATE utf8_unicode_ci, s3 COLLATE utf8mb3_unicode_ci,
       s4 COLLATE utf8mb4_unicode_ci FROM charset_values;
SELECT s3 COLLATE utf8mb4_unicode_ci FROM charset_values;
SELECT s4 COLLATE utf8_unicode_ci FROM charset_values;
SELECT _utf8mb4'a' COLLATE utf8_unicode_ci;
-- The utf8mb4 introducer name is matched case-insensitively.
SELECT _utf8mb4'test', _UTF8MB4'test', _UtF8mB4'test';
SELECT DATE_FORMAT('2024-01-15', _UTF8MB4'%Y-%m');

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

-- Frontend AST consumers traverse the operand of COLLATE.
-- Distinct source data detects a missed remap even while the source exists.
DROP DATABASE IF EXISTS collation_expression_source;
CREATE DATABASE collation_expression_source;
CREATE TABLE collation_expression_source.source_values (id INT PRIMARY KEY, v VARCHAR(20));
INSERT INTO collation_expression_source.source_values VALUES (1, 'source');
SELECT HEX((SELECT v FROM collation_expression_source.source_values WHERE id = 1) COLLATE utf8mb4_bin);
SET enable_remap_hint = 1;
SET remap_rewrites = '{"remapdb":{"collation_expression_source":"collation_expression_test"}}';
SELECT HEX((SELECT v FROM collation_expression_source.source_values WHERE id = 1) COLLATE utf8mb4_bin);
SELECT HEX(collation_expression_source.source_values.v COLLATE utf8mb4_bin)
       FROM collation_expression_source.source_values WHERE id = 1;
SET remap_rewrites = '';
DROP DATABASE collation_expression_source;
SET remap_rewrites = '{"remapdb":{"collation_expression_source":"collation_expression_test"}}';
SELECT HEX((SELECT v FROM collation_expression_source.source_values WHERE id = 1) COLLATE utf8mb4_bin);
SET remap_rewrites = '';
SET enable_remap_hint = 0;

-- Materialize collated scalar subqueries in prepared SET, in argument order.
PREPARE collated_set FROM 'SET @collated_value = (SELECT ''a'') COLLATE utf8mb4_bin';
EXECUTE collated_set;
SELECT HEX(@collated_value);
DEALLOCATE PREPARE collated_set;
PREPARE collated_set FROM 'SET @collated_value = CONCAT((SELECT ''left'') COLLATE utf8mb4_bin, (SELECT ''right'') COLLATE utf8mb4_bin)';
EXECUTE collated_set;
SELECT HEX(@collated_value);
DEALLOCATE PREPARE collated_set;

-- Reuse the same cached plan across changing scalar results, including NULL.
PREPARE collated_set FROM 'SET @collated_value = (SELECT v FROM source_values WHERE id = 1) COLLATE utf8mb4_bin';
EXECUTE collated_set;
SELECT HEX(@collated_value);
UPDATE source_values SET v = NULL WHERE id = 1;
EXECUTE collated_set;
SELECT @collated_value IS NULL;
UPDATE source_values SET v = 'Z' WHERE id = 1;
EXECUTE collated_set;
SELECT HEX(@collated_value);
DEALLOCATE PREPARE collated_set;
SET @collated_value = NULL;

-- 二进制位聚合保留输入的隐式/显式 collation provenance，且不改变字节结果。
SELECT LENGTH(CAST(REPEAT('a',65536) AS BINARY) COLLATE binary) AS binary_length;
CREATE TABLE bitwise_collation (b BLOB);
INSERT INTO bitwise_collation VALUES (X'0F'), (X'03');
SELECT HEX(BIT_AND(SUBSTRING(b,1,511))) AS and_hex,
       HEX(BIT_OR(SUBSTRING(b,1,511))) AS or_hex,
       HEX(BIT_XOR(SUBSTRING(b,1,511))) AS xor_hex
FROM bitwise_collation;
SELECT BIT_AND(SUBSTRING(b,1,511) COLLATE binary) = (CAST(X'03' AS BINARY) COLLATE binary) AS and_equal,
       BIT_OR(SUBSTRING(b,1,511) COLLATE binary) = (CAST(X'0F' AS BINARY) COLLATE binary) AS or_equal,
       BIT_XOR(SUBSTRING(b,1,511) COLLATE binary) = (CAST(X'0C' AS BINARY) COLLATE binary) AS xor_equal
FROM bitwise_collation;
SELECT BIT_AND(SUBSTRING(b,1,511) COLLATE binary) = (_utf8mb4'a' COLLATE utf8mb4_bin) FROM bitwise_collation;
SELECT BIT_OR(SUBSTRING(b,1,511) COLLATE binary) = (_utf8mb4'a' COLLATE utf8mb4_bin) FROM bitwise_collation;
SELECT BIT_XOR(SUBSTRING(b,1,511) COLLATE binary) = (_utf8mb4'a' COLLATE utf8mb4_bin) FROM bitwise_collation;
DROP TABLE bitwise_collation;
DROP DATABASE collation_expression_test;
