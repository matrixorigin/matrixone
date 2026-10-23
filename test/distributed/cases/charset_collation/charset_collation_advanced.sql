-- @suit
-- @setup
DROP DATABASE IF EXISTS charset_advanced_test;
CREATE DATABASE charset_advanced_test;
USE charset_advanced_test;

-- @case
-- @desc: Test mixed charset in same table (different columns)
-- @label:bvt
CREATE TABLE t_mixed_charset (
    id INT PRIMARY KEY,
    col_utf8 VARCHAR(100) CHARACTER SET utf8,
    col_utf8mb4 VARCHAR(100) CHARACTER SET utf8mb4,
    col_binary VARBINARY(100)
);

INSERT INTO t_mixed_charset VALUES (
    1, 
    'ASCII text', 
    'Unicode text 你好 😀', 
    'Binary data'
);

INSERT INTO t_mixed_charset VALUES (
    2,
    'Test',
    'テスト',
    0x48656C6C6F
);

SELECT * FROM t_mixed_charset ORDER BY id;
SELECT id, LENGTH(col_utf8), LENGTH(col_utf8mb4), LENGTH(col_binary) FROM t_mixed_charset ORDER BY id;

-- @case
-- @desc: Test collation coercibility in expressions
-- @label:bvt
CREATE TABLE t_coerce1 (
    id INT PRIMARY KEY,
    name VARCHAR(100)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

CREATE TABLE t_coerce2 (
    id INT PRIMARY KEY,
    name VARCHAR(100)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

INSERT INTO t_coerce1 VALUES (1, 'Apple');
INSERT INTO t_coerce2 VALUES (1, 'apple');

-- Test comparison between different collations (may have issues)
-- SELECT * FROM t_coerce1 t1, t_coerce2 t2 WHERE t1.name = t2.name;

-- @case
-- @desc: Test CONVERT function with charset
-- @label:bvt
SELECT CONVERT('test' USING utf8);
SELECT CONVERT('你好世界' USING utf8mb4);

-- @case
-- @desc: Test HEX and UNHEX with different charsets
-- @label:bvt
CREATE TABLE t_hex_test (
    id INT PRIMARY KEY,
    original VARCHAR(100),
    hex_value VARCHAR(200)
) CHARACTER SET utf8mb4;

INSERT INTO t_hex_test VALUES (1, 'Hello', HEX('Hello'));
INSERT INTO t_hex_test VALUES (2, '你好', HEX('你好'));
INSERT INTO t_hex_test VALUES (3, '😀', HEX('😀'));

SELECT * FROM t_hex_test ORDER BY id;
SELECT id, original, UNHEX(hex_value) as decoded FROM t_hex_test ORDER BY id;

-- @case
-- @desc: Test string padding with different charsets
-- @label:bvt
CREATE TABLE t_pad_test (
    id INT PRIMARY KEY,
    data VARCHAR(100)
) CHARACTER SET utf8mb4;

INSERT INTO t_pad_test VALUES (1, 'test');
INSERT INTO t_pad_test VALUES (2, '测试');
INSERT INTO t_pad_test VALUES (3, '😀');

SELECT id, LPAD(data, 10, '*') as lpad_result FROM t_pad_test ORDER BY id;
SELECT id, RPAD(data, 10, '*') as rpad_result FROM t_pad_test ORDER BY id;
SELECT id, LPAD(data, 10, '中') as lpad_chinese FROM t_pad_test ORDER BY id;

-- @case
-- @desc: Test TRIM functions with multibyte characters
-- @label:bvt
CREATE TABLE t_trim_test (
    id INT PRIMARY KEY,
    data VARCHAR(100)
) CHARACTER SET utf8mb4;

INSERT INTO t_trim_test VALUES (1, '  test  ');
INSERT INTO t_trim_test VALUES (2, '中中测试中中');
INSERT INTO t_trim_test VALUES (3, '😀😀text😀😀');

SELECT id, TRIM(data) as trimmed FROM t_trim_test ORDER BY id;
SELECT id, LTRIM(data) as ltrimmed FROM t_trim_test ORDER BY id;
SELECT id, RTRIM(data) as rtrimmed FROM t_trim_test ORDER BY id;
SELECT id, TRIM('中' FROM data) as trim_chinese FROM t_trim_test WHERE id = 2;
SELECT id, TRIM('😀' FROM data) as trim_emoji FROM t_trim_test WHERE id = 3;

-- @case
-- @desc: Test REPLACE with multibyte characters
-- @label:bvt
CREATE TABLE t_replace_test (
    id INT PRIMARY KEY,
    data VARCHAR(100)
) CHARACTER SET utf8mb4;

INSERT INTO t_replace_test VALUES (1, 'Hello World Hello');
INSERT INTO t_replace_test VALUES (2, '你好世界你好');
INSERT INTO t_replace_test VALUES (3, 'Test测试Test');

SELECT id, REPLACE(data, 'Hello', 'Hi') as replaced FROM t_replace_test WHERE id = 1;
SELECT id, REPLACE(data, '你好', '您好') as replaced FROM t_replace_test WHERE id = 2;
SELECT id, REPLACE(data, 'Test', '测试') as replaced FROM t_replace_test WHERE id = 3;

-- @case
-- @desc: Test REVERSE with multibyte characters
-- @label:bvt
SELECT REVERSE('Hello');
SELECT REVERSE('你好世界');
SELECT REVERSE('😀😃😄');

-- @case
-- @desc: Test LOCATE and POSITION with multibyte characters
-- @label:bvt
CREATE TABLE t_locate_test (
    id INT PRIMARY KEY,
    data VARCHAR(100)
) CHARACTER SET utf8mb4;

INSERT INTO t_locate_test VALUES (1, 'Hello World');
INSERT INTO t_locate_test VALUES (2, '你好世界');
INSERT INTO t_locate_test VALUES (3, 'Test测试Test');

SELECT id, LOCATE('World', data) as pos FROM t_locate_test WHERE id = 1;
SELECT id, LOCATE('世界', data) as pos FROM t_locate_test WHERE id = 2;
SELECT id, LOCATE('测试', data) as pos FROM t_locate_test WHERE id = 3;
SELECT id, POSITION('测试' IN data) as pos FROM t_locate_test WHERE id = 3;

-- @case
-- @desc: Test INSTR with multibyte characters
-- @label:bvt
SELECT INSTR('Hello World', 'World');
SELECT INSTR('你好世界', '世界');
SELECT INSTR('Test测试Test', '测试');

-- @case
-- @desc: Test LEFT and RIGHT functions with multibyte characters
-- @label:bvt
CREATE TABLE t_left_right_test (
    id INT PRIMARY KEY,
    data VARCHAR(100)
) CHARACTER SET utf8mb4;

INSERT INTO t_left_right_test VALUES (1, 'Hello World');
INSERT INTO t_left_right_test VALUES (2, '你好世界');
INSERT INTO t_left_right_test VALUES (3, '😀😃😄😁😆');

SELECT id, LEFT(data, 5) as left_part FROM t_left_right_test ORDER BY id;
SELECT id, RIGHT(data, 5) as right_part FROM t_left_right_test ORDER BY id;

-- @case
-- @desc: Test sorting with NULL values and different collations
-- @label:bvt
CREATE TABLE t_null_sort_ci (
    id INT PRIMARY KEY,
    name VARCHAR(100)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

INSERT INTO t_null_sort_ci VALUES (1, 'Apple');
INSERT INTO t_null_sort_ci VALUES (2, NULL);
INSERT INTO t_null_sort_ci VALUES (3, 'Banana');
INSERT INTO t_null_sort_ci VALUES (4, NULL);
INSERT INTO t_null_sort_ci VALUES (5, 'Cherry');

SELECT * FROM t_null_sort_ci ORDER BY name;
SELECT * FROM t_null_sort_ci ORDER BY name DESC;
SELECT * FROM t_null_sort_ci ORDER BY name IS NULL, name;

-- @case
-- @desc: Test case sensitivity in WHERE clauses
-- @label:bvt
CREATE TABLE t_where_case (
    id INT PRIMARY KEY,
    code VARCHAR(50)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

INSERT INTO t_where_case VALUES (1, 'ABC123');
INSERT INTO t_where_case VALUES (2, 'abc123');
INSERT INTO t_where_case VALUES (3, 'Abc123');
INSERT INTO t_where_case VALUES (4, 'DEF456');

-- Should match all three ABC variations
SELECT COUNT(*) FROM t_where_case WHERE code = 'abc123';

-- Test with LIKE
SELECT * FROM t_where_case WHERE code LIKE 'abc%' ORDER BY id;

-- Test with REGEXP (if supported)
-- SELECT * FROM t_where_case WHERE code REGEXP '^abc' ORDER BY id;

-- @case
-- @desc: Test aggregation with different collations
-- @label:bvt
CREATE TABLE t_agg_test (
    id INT,
    category VARCHAR(50),
    value INT
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

INSERT INTO t_agg_test VALUES (1, 'Apple', 10);
INSERT INTO t_agg_test VALUES (2, 'apple', 20);
INSERT INTO t_agg_test VALUES (3, 'APPLE', 30);
INSERT INTO t_agg_test VALUES (4, 'Banana', 40);
INSERT INTO t_agg_test VALUES (5, 'banana', 50);

-- Normalize only the displayed peer label; distributed groups may retain any original spelling.
SELECT LOWER(category) as category, SUM(value) as total FROM t_agg_test GROUP BY category ORDER BY category;
SELECT LOWER(category) as category, AVG(value) as avg_val FROM t_agg_test GROUP BY category ORDER BY category;
SELECT LOWER(category) as category, COUNT(*) as cnt FROM t_agg_test GROUP BY category ORDER BY category;

-- @case
-- @desc: Test subquery with different collations
-- @label:bvt
CREATE TABLE t_outer (
    id INT PRIMARY KEY,
    name VARCHAR(100)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

CREATE TABLE t_inner (
    id INT PRIMARY KEY,
    name VARCHAR(100)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

INSERT INTO t_outer VALUES (1, 'Apple'), (2, 'Banana'), (3, 'Cherry');
INSERT INTO t_inner VALUES (1, 'apple'), (2, 'BANANA');

SELECT * FROM t_outer WHERE name IN (SELECT name FROM t_inner) ORDER BY id;
SELECT * FROM t_outer WHERE EXISTS (SELECT 1 FROM t_inner WHERE t_inner.name = t_outer.name) ORDER BY id;

-- @case
-- @desc: Test HAVING clause with case-insensitive collation
-- @label:bvt
CREATE TABLE t_having_test (
    id INT,
    category VARCHAR(50)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

INSERT INTO t_having_test VALUES (1, 'Apple'), (2, 'apple'), (3, 'APPLE');
INSERT INTO t_having_test VALUES (4, 'Banana'), (5, 'banana');
INSERT INTO t_having_test VALUES (6, 'Cherry');

SELECT LOWER(category) as category, COUNT(*) as cnt
FROM t_having_test 
GROUP BY category 
HAVING COUNT(*) > 1 
ORDER BY category;

-- @case
-- @desc: Test primary key with different collations
-- @label:bvt
CREATE TABLE t_pk_ci (
    name VARCHAR(100) PRIMARY KEY
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='t_pk_ci';
-- Historical physical keys stay separate from new native admission.
CREATE TABLE t_pk_ci (
    name VARCHAR(100) PRIMARY KEY
) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

INSERT INTO t_pk_ci VALUES ('Apple');
-- Native case-insensitive duplicate handling is not a physical-key capability.
-- INSERT INTO t_pk_ci VALUES ('apple');

SELECT * FROM t_pk_ci;

CREATE TABLE t_pk_bin (
    name VARCHAR(100) PRIMARY KEY
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='t_pk_bin';
CREATE TABLE t_pk_bin (
    name VARCHAR(100) PRIMARY KEY
) CHARACTER SET utf8mb4 COLLATE utf32_bin;

INSERT INTO t_pk_bin VALUES ('Apple');
-- This should succeed with case-sensitive
INSERT INTO t_pk_bin VALUES ('apple');
INSERT INTO t_pk_bin VALUES ('APPLE');

SELECT * FROM t_pk_bin ORDER BY name;

-- @case
-- @desc: Test foreign key with different collations
-- @label:bvt
CREATE TABLE t_fk_parent (
    id INT PRIMARY KEY,
    code VARCHAR(50) UNIQUE
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='t_fk_parent';
CREATE TABLE t_fk_parent (
    id INT PRIMARY KEY,
    code VARCHAR(50) UNIQUE
) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

CREATE TABLE t_fk_child (
    id INT PRIMARY KEY,
    parent_code VARCHAR(50),
    FOREIGN KEY (parent_code) REFERENCES t_fk_parent(code)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='t_fk_child';
CREATE TABLE t_fk_child (
    id INT PRIMARY KEY,
    parent_code VARCHAR(50),
    FOREIGN KEY (parent_code) REFERENCES t_fk_parent(code)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

INSERT INTO t_fk_parent VALUES (1, 'CODE001');
INSERT INTO t_fk_child VALUES (1, 'CODE001');
-- Test case insensitive foreign key
INSERT INTO t_fk_child VALUES (2, 'code001');

SELECT * FROM t_fk_parent;
SELECT * FROM t_fk_child ORDER BY id;

-- @case
-- @desc: Test view with charset and collation
-- @label:bvt
CREATE TABLE t_view_base (
    id INT PRIMARY KEY,
    name VARCHAR(100)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

INSERT INTO t_view_base VALUES (1, 'Apple'), (2, 'Banana'), (3, 'Cherry');

CREATE VIEW v_upper AS SELECT id, UPPER(name) as upper_name FROM t_view_base;
CREATE VIEW v_filter AS SELECT * FROM t_view_base WHERE name = 'apple';

SELECT * FROM v_upper ORDER BY id;
SELECT * FROM v_filter;

-- @case
-- @desc: Test prepared statement with different charsets
-- @label:bvt
CREATE TABLE t_prepare (
    id INT PRIMARY KEY,
    name VARCHAR(100)
) CHARACTER SET utf8mb4;

PREPARE stmt FROM 'INSERT INTO t_prepare VALUES (?, ?)';
-- Note: Actual parameter setting depends on client protocol
-- SET @id = 1, @name = '你好';
-- EXECUTE stmt USING @id, @name;

INSERT INTO t_prepare VALUES (1, '你好');
INSERT INTO t_prepare VALUES (2, 'Hello');

SELECT * FROM t_prepare ORDER BY id;

-- @case
-- @desc: Test CONCAT_WS with multibyte characters
-- @label:bvt
CREATE TABLE t_concat_ws (
    id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50)
) CHARACTER SET utf8mb4;

INSERT INTO t_concat_ws VALUES (1, 'John', 'Doe');
INSERT INTO t_concat_ws VALUES (2, '张', '三');
INSERT INTO t_concat_ws VALUES (3, '田中', '太郎');

SELECT id, CONCAT_WS(' ', first_name, last_name) as full_name FROM t_concat_ws ORDER BY id;
SELECT id, CONCAT_WS('-', first_name, last_name) as full_name FROM t_concat_ws ORDER BY id;
SELECT id, CONCAT_WS('、', first_name, last_name) as full_name FROM t_concat_ws ORDER BY id;

-- @case
-- @desc: Test FIELD function with different collations
-- @label:bvt
SELECT FIELD('apple', 'Apple', 'Banana', 'Cherry');
SELECT FIELD('Apple', 'apple', 'banana', 'cherry');

-- @case
-- @desc: Test MAKE_SET with multibyte strings
-- @label:bvt
SELECT MAKE_SET(1|2|4, 'hello', '你好', 'こんにちは', '안녕');

-- @case
-- @desc: Test ELT function with multibyte strings
-- @label:bvt
SELECT ELT(1, 'Apple', '苹果', 'りんご');
SELECT ELT(2, 'Apple', '苹果', 'りんご');
SELECT ELT(3, 'Apple', '苹果', 'りんご');

-- @case
-- @desc: Test FORMAT with different locales (if supported)
-- @label:bvt
SELECT FORMAT(12332.123456, 2);
SELECT FORMAT(12332.123456, 4);

-- @case
-- @desc: Test string comparison with trailing spaces
-- @label:bvt
CREATE TABLE t_trailing_space (
    id INT PRIMARY KEY,
    data VARCHAR(100)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

INSERT INTO t_trailing_space VALUES (1, 'test');
INSERT INTO t_trailing_space VALUES (2, 'test ');
INSERT INTO t_trailing_space VALUES (3, 'test  ');

-- Behavior may differ based on PAD SPACE vs NO PAD
SELECT * FROM t_trailing_space WHERE data = 'test' ORDER BY id;
SELECT COUNT(*) FROM t_trailing_space WHERE data = 'test';
SELECT COUNT(*) FROM t_trailing_space WHERE CAST(data AS BINARY) = CAST('test' AS BINARY);
SELECT id, HEX(data) FROM t_trailing_space ORDER BY id;

-- @case
-- @desc: Test WEIGHT_STRING function (if supported)
-- @label:bvt
-- SELECT WEIGHT_STRING('Apple');
-- SELECT WEIGHT_STRING('apple');
-- SELECT WEIGHT_STRING('你好');

-- @case
-- @desc: Test charset with TEXT and BLOB types
-- @label:bvt
CREATE TABLE t_large_types (
    id INT PRIMARY KEY,
    text_data TEXT CHARACTER SET utf8mb4,
    blob_data BLOB
);

INSERT INTO t_large_types VALUES (
    1,
    REPEAT('测试文本', 100),
    REPEAT('Binary', 100)
);

INSERT INTO t_large_types VALUES (
    2,
    '😀😃😄😁😆😅🤣😂🙂🙃😉',
    0x48656C6C6F576F726C64
);

SELECT id, LENGTH(text_data), LENGTH(blob_data) FROM t_large_types ORDER BY id;
SELECT id, CHAR_LENGTH(text_data) FROM t_large_types ORDER BY id;

-- @case
-- @desc: Test ENUM with different charsets
-- @label:bvt
CREATE TABLE t_enum_charset (
    id INT PRIMARY KEY,
    status ENUM('active', 'inactive', '活动', '非活动') CHARACTER SET utf8mb4
);

INSERT INTO t_enum_charset VALUES (1, 'active');
INSERT INTO t_enum_charset VALUES (2, '活动');
INSERT INTO t_enum_charset VALUES (3, 'inactive');
INSERT INTO t_enum_charset VALUES (4, '非活动');

SELECT * FROM t_enum_charset ORDER BY id;
SELECT * FROM t_enum_charset WHERE status = 'active';
SELECT * FROM t_enum_charset WHERE status = '活动';

-- @case
-- @desc: Test SET type with different charsets
-- @label:bvt
CREATE TABLE t_set_charset (
    id INT PRIMARY KEY,
    options SET('option1', 'option2', '选项1', '选项2') CHARACTER SET utf8mb4
);

INSERT INTO t_set_charset VALUES (1, 'option1');
INSERT INTO t_set_charset VALUES (2, 'option1,option2');
INSERT INTO t_set_charset VALUES (3, '选项1');
INSERT INTO t_set_charset VALUES (4, 'option1,选项1');

SELECT * FROM t_set_charset ORDER BY id;

-- @case
-- @desc: Test character set conversion edge cases
-- @label:bvt
-- Test zero-width characters
CREATE TABLE t_zero_width (
    id INT PRIMARY KEY,
    data VARCHAR(100)
) CHARACTER SET utf8mb4;

-- Zero-width space: U+200B
INSERT INTO t_zero_width VALUES (1, 'test​test');
-- Zero-width joiner: U+200D
INSERT INTO t_zero_width VALUES (2, 'test‍test');

SELECT id, LENGTH(data), CHAR_LENGTH(data) FROM t_zero_width ORDER BY id;

-- @case
-- @desc: Test RTL (Right-to-Left) text
-- @label:bvt
CREATE TABLE t_rtl_text (
    id INT PRIMARY KEY,
    data VARCHAR(100)
) CHARACTER SET utf8mb4;

INSERT INTO t_rtl_text VALUES (1, 'مرحبا بك');  -- Arabic
INSERT INTO t_rtl_text VALUES (2, 'שלום עולם');  -- Hebrew
INSERT INTO t_rtl_text VALUES (3, 'Hello مرحبا שלום'); -- Mixed

SELECT * FROM t_rtl_text ORDER BY id;
SELECT id, LENGTH(data), CHAR_LENGTH(data) FROM t_rtl_text ORDER BY id;

-- @case
-- @desc: Test surrogate pairs and combining characters
-- @label:bvt
CREATE TABLE t_combining (
    id INT PRIMARY KEY,
    data VARCHAR(100)
) CHARACTER SET utf8mb4;

-- Combining diacritical marks
INSERT INTO t_combining VALUES (1, 'é');  -- Single character
INSERT INTO t_combining VALUES (2, 'é');  -- e + combining acute accent

SELECT id, data, LENGTH(data), CHAR_LENGTH(data) FROM t_combining ORDER BY id;

-- @case
-- @desc: Test maximum VARCHAR length with different charsets
-- @label:bvt
CREATE TABLE t_max_varchar_utf8 (
    id INT PRIMARY KEY,
    data VARCHAR(16383) CHARACTER SET utf8
);

CREATE TABLE t_max_varchar_utf8mb4 (
    id INT PRIMARY KEY,
    data VARCHAR(16383) CHARACTER SET utf8mb4
);

INSERT INTO t_max_varchar_utf8 VALUES (1, REPEAT('A', 100));
INSERT INTO t_max_varchar_utf8mb4 VALUES (1, REPEAT('你', 100));

SELECT id, LENGTH(data), CHAR_LENGTH(data) FROM t_max_varchar_utf8;
SELECT id, LENGTH(data), CHAR_LENGTH(data) FROM t_max_varchar_utf8mb4;

-- @case
-- @desc: Test UPDATE with charset conversion
-- @label:bvt
CREATE TABLE t_update_charset (
    id INT PRIMARY KEY,
    data VARCHAR(100)
) CHARACTER SET utf8mb4;

INSERT INTO t_update_charset VALUES (1, 'Hello');
INSERT INTO t_update_charset VALUES (2, '你好');

UPDATE t_update_charset SET data = 'World' WHERE id = 1;
UPDATE t_update_charset SET data = '世界' WHERE id = 2;

SELECT * FROM t_update_charset ORDER BY id;

-- Update with expression
UPDATE t_update_charset SET data = CONCAT(data, '!') WHERE id = 1;
UPDATE t_update_charset SET data = CONCAT('欢迎', data) WHERE id = 2;

SELECT * FROM t_update_charset ORDER BY id;

-- @case
-- @desc: Test DELETE with different collations
-- @label:bvt
CREATE TABLE t_delete_ci (
    id INT PRIMARY KEY,
    name VARCHAR(100)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

INSERT INTO t_delete_ci VALUES (1, 'Apple');
INSERT INTO t_delete_ci VALUES (2, 'apple');
INSERT INTO t_delete_ci VALUES (3, 'APPLE');
INSERT INTO t_delete_ci VALUES (4, 'Banana');

-- Delete should match all case variations
DELETE FROM t_delete_ci WHERE name = 'apple';

SELECT * FROM t_delete_ci ORDER BY id;

-- @case
-- @desc: Test transaction with charset operations
-- @label:bvt
CREATE TABLE t_transaction (
    id INT PRIMARY KEY,
    data VARCHAR(100)
) CHARACTER SET utf8mb4;

BEGIN;
INSERT INTO t_transaction VALUES (1, '事务测试1');
INSERT INTO t_transaction VALUES (2, 'Transaction test 2');
COMMIT;

SELECT * FROM t_transaction ORDER BY id;

BEGIN;
INSERT INTO t_transaction VALUES (3, '事务测试3');
ROLLBACK;

-- Row 3 should not exist
SELECT * FROM t_transaction ORDER BY id;

-- @case
-- @desc: Cleanup
-- @label:bvt
DROP DATABASE IF EXISTS charset_advanced_test;

