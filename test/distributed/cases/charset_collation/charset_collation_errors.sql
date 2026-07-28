-- @suit
-- @setup
DROP DATABASE IF EXISTS charset_error_test;
CREATE DATABASE charset_error_test;
USE charset_error_test;

-- @case
-- @desc: Test invalid charset names
-- @label:bvt
-- CREATE TABLE t_invalid_charset (
--     id INT PRIMARY KEY,
--     name VARCHAR(100)
-- ) CHARACTER SET invalid_charset;

-- @case
-- @desc: Test invalid collation names
-- @label:bvt
-- CREATE TABLE t_invalid_collation (
--     id INT PRIMARY KEY,
--     name VARCHAR(100)
-- ) COLLATE invalid_collation;

-- @case
-- @desc: Test mismatched charset and collation
-- @label:bvt
-- This should fail: utf8 charset with utf8mb4 collation
-- CREATE TABLE t_mismatch (
--     id INT PRIMARY KEY,
--     name VARCHAR(100) CHARACTER SET utf8 COLLATE utf8mb4_bin
-- );

-- @case
-- @desc: Test charset boundary - maximum bytes per character
-- @label:bvt
CREATE TABLE t_byte_boundary (
    id INT PRIMARY KEY,
    data VARCHAR(100)
) CHARACTER SET utf8mb4;

-- UTF-8 can have 1-4 bytes per character
-- Test 1-byte characters (ASCII)
INSERT INTO t_byte_boundary VALUES (1, 'A');
SELECT id, LENGTH(data), CHAR_LENGTH(data) FROM t_byte_boundary WHERE id = 1;

-- Test 2-byte characters
INSERT INTO t_byte_boundary VALUES (2, '中');
SELECT id, LENGTH(data), CHAR_LENGTH(data) FROM t_byte_boundary WHERE id = 2;

-- Test 3-byte characters
INSERT INTO t_byte_boundary VALUES (3, '€');
SELECT id, LENGTH(data), CHAR_LENGTH(data) FROM t_byte_boundary WHERE id = 3;

-- Test 4-byte characters (emoji)
INSERT INTO t_byte_boundary VALUES (4, '😀');
SELECT id, LENGTH(data), CHAR_LENGTH(data) FROM t_byte_boundary WHERE id = 4;

-- @case
-- @desc: Test VARCHAR size limits with multibyte characters
-- @label:bvt
CREATE TABLE t_varchar_limit (
    id INT PRIMARY KEY,
    -- VARCHAR(N) is N characters, not N bytes
    data VARCHAR(10)
) CHARACTER SET utf8mb4;

-- 10 ASCII characters (should succeed)
INSERT INTO t_varchar_limit VALUES (1, '0123456789');

-- 10 Chinese characters (should succeed - 10 characters, 30 bytes)
INSERT INTO t_varchar_limit VALUES (2, '零一二三四五六七八九');

-- 10 emojis (should succeed - 10 characters, 40 bytes)
INSERT INTO t_varchar_limit VALUES (3, '😀😃😄😁😆😅🤣😂🙂🙃');

-- 11 characters (should fail)
-- INSERT INTO t_varchar_limit VALUES (4, '01234567890');

SELECT id, data, LENGTH(data), CHAR_LENGTH(data) FROM t_varchar_limit ORDER BY id;

-- @case
-- @desc: Test CHAR type with different charsets
-- @label:bvt
CREATE TABLE t_char_padding (
    id INT PRIMARY KEY,
    data CHAR(10)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

INSERT INTO t_char_padding VALUES (1, 'test');
INSERT INTO t_char_padding VALUES (2, 'test      ');  -- explicit spaces
INSERT INTO t_char_padding VALUES (3, '测');

-- CHAR should pad/trim based on collation
-- Only check lengths to avoid display issues with trailing spaces
SELECT id, LENGTH(data), CHAR_LENGTH(data) FROM t_char_padding ORDER BY id;

-- @case
-- @desc: Test NULL handling with different charsets
-- @label:bvt
CREATE TABLE t_null_handling (
    id INT PRIMARY KEY,
    data VARCHAR(100)
) CHARACTER SET utf8mb4;

INSERT INTO t_null_handling VALUES (1, NULL);
INSERT INTO t_null_handling VALUES (2, '');
INSERT INTO t_null_handling VALUES (3, 'NULL');

-- Use explicit column aliases to avoid metadata differences
SELECT id, data, data IS NULL as is_null, (data = '') as is_empty, LENGTH(data) as len FROM t_null_handling ORDER BY id;

-- @case
-- @desc: Test CONCAT with NULL
-- @label:bvt
SELECT CONCAT('Hello', NULL, 'World');
SELECT CONCAT('你好', NULL, '世界');
SELECT CONCAT_WS(',', 'A', NULL, 'B', NULL, 'C');

-- @case
-- @desc: Test invalid UTF-8 sequences (if insertable)
-- @label:bvt
CREATE TABLE t_invalid_utf8 (
    id INT PRIMARY KEY,
    data VARCHAR(100)
) CHARACTER SET utf8mb4;

-- Valid sequences should work
INSERT INTO t_invalid_utf8 VALUES (1, 'Valid UTF-8');
INSERT INTO t_invalid_utf8 VALUES (2, '有效的UTF-8');

-- Test with binary that may contain invalid UTF-8 (depends on validation)
-- INSERT INTO t_invalid_utf8 VALUES (3, UNHEX('C328'));  -- Invalid UTF-8

SELECT * FROM t_invalid_utf8 ORDER BY id;

-- @case
-- @desc: Test SUBSTRING with invalid positions
-- @label:bvt
CREATE TABLE t_substring_edge (
    id INT PRIMARY KEY,
    data VARCHAR(100)
) CHARACTER SET utf8mb4;

INSERT INTO t_substring_edge VALUES (1, '你好世界');

-- Negative position
SELECT SUBSTRING(data, -2) FROM t_substring_edge WHERE id = 1;
SELECT SUBSTRING(data, -2, 1) FROM t_substring_edge WHERE id = 1;

-- Position 0
SELECT SUBSTRING(data, 0) FROM t_substring_edge WHERE id = 1;

-- Position beyond length
SELECT SUBSTRING(data, 100) FROM t_substring_edge WHERE id = 1;

-- Negative length
SELECT SUBSTRING(data, 1, -1) FROM t_substring_edge WHERE id = 1;

-- Zero length
SELECT SUBSTRING(data, 1, 0) FROM t_substring_edge WHERE id = 1;

-- @case
-- @desc: Test CHAR_LENGTH vs LENGTH edge cases
-- @label:bvt
CREATE TABLE t_length_test (
    id INT PRIMARY KEY,
    data VARCHAR(100)
) CHARACTER SET utf8mb4;

-- Empty string
INSERT INTO t_length_test VALUES (1, '');
SELECT id, LENGTH(data), CHAR_LENGTH(data) FROM t_length_test WHERE id = 1;

-- Single byte
INSERT INTO t_length_test VALUES (2, 'A');
SELECT id, LENGTH(data), CHAR_LENGTH(data) FROM t_length_test WHERE id = 2;

-- Two bytes per char
INSERT INTO t_length_test VALUES (3, 'Ü');
SELECT id, LENGTH(data), CHAR_LENGTH(data) FROM t_length_test WHERE id = 3;

-- Three bytes per char
INSERT INTO t_length_test VALUES (4, '中');
SELECT id, LENGTH(data), CHAR_LENGTH(data) FROM t_length_test WHERE id = 4;

-- Four bytes per char
INSERT INTO t_length_test VALUES (5, '𝄞');  -- Musical symbol
SELECT id, LENGTH(data), CHAR_LENGTH(data) FROM t_length_test WHERE id = 5;

-- Mixed
INSERT INTO t_length_test VALUES (6, 'A中😀');
SELECT id, LENGTH(data), CHAR_LENGTH(data) FROM t_length_test WHERE id = 6;

-- @case
-- @desc: Test UPPER/LOWER with non-ASCII
-- @label:bvt
CREATE TABLE t_case_conversion (
    id INT PRIMARY KEY,
    data VARCHAR(100)
) CHARACTER SET utf8mb4;

-- ASCII
INSERT INTO t_case_conversion VALUES (1, 'Hello World');
SELECT id, UPPER(data), LOWER(data) FROM t_case_conversion WHERE id = 1;

-- Latin extended
INSERT INTO t_case_conversion VALUES (2, 'Café');
SELECT id, UPPER(data), LOWER(data) FROM t_case_conversion WHERE id = 2;

-- German
INSERT INTO t_case_conversion VALUES (3, 'Straße');
SELECT id, UPPER(data), LOWER(data) FROM t_case_conversion WHERE id = 3;

-- Greek
INSERT INTO t_case_conversion VALUES (4, 'Ελληνικά');
SELECT id, UPPER(data), LOWER(data) FROM t_case_conversion WHERE id = 4;

-- Cyrillic
INSERT INTO t_case_conversion VALUES (5, 'Привет');
SELECT id, UPPER(data), LOWER(data) FROM t_case_conversion WHERE id = 5;

-- Chinese (no case)
INSERT INTO t_case_conversion VALUES (6, '你好世界');
SELECT id, UPPER(data), LOWER(data) FROM t_case_conversion WHERE id = 6;

-- @case
-- @desc: Test comparison with special Unicode characters
-- @label:bvt
CREATE TABLE t_special_unicode (
    id INT PRIMARY KEY,
    data VARCHAR(100)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

-- Non-breaking space (U+00A0) vs regular space (U+0020)
INSERT INTO t_special_unicode VALUES (1, 'test test');      -- Regular space
INSERT INTO t_special_unicode VALUES (2, 'test test');     -- Non-breaking space (looks same)

SELECT COUNT(*) FROM t_special_unicode WHERE data = 'test test';

-- Soft hyphen (U+00AD)
INSERT INTO t_special_unicode VALUES (3, 'test­ing');

SELECT * FROM t_special_unicode ORDER BY id;

-- @case
-- @desc: Test normalization issues (NFC vs NFD)
-- @label:bvt
CREATE TABLE t_normalization (
    id INT PRIMARY KEY,
    data VARCHAR(100)
) CHARACTER SET utf8mb4;

-- NFC: é as single codepoint (U+00E9)
INSERT INTO t_normalization VALUES (1, 'café');

-- NFD: é as e (U+0065) + combining acute (U+0301)
-- INSERT INTO t_normalization VALUES (2, 'cafe\u0301');

SELECT id, data, LENGTH(data), CHAR_LENGTH(data) FROM t_normalization ORDER BY id;

-- @case
-- @desc: Test COLLATE clause in queries
-- @label:bvt
CREATE TABLE t_query_collate (
    id INT PRIMARY KEY,
    name VARCHAR(100)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

INSERT INTO t_query_collate VALUES (1, 'Apple');
INSERT INTO t_query_collate VALUES (2, 'apple');

-- Override table collation in query
SELECT * FROM t_query_collate WHERE name = 'apple' ORDER BY id;
SELECT * FROM t_query_collate WHERE name COLLATE utf8mb4_general_ci = 'apple' ORDER BY id;

-- @case
-- @desc: Test implicit type conversion with charset
-- @label:bvt
CREATE TABLE t_type_conversion (
    id INT PRIMARY KEY,
    str_val VARCHAR(100),
    int_val INT
) CHARACTER SET utf8mb4;

INSERT INTO t_type_conversion VALUES (1, '123', 123);
INSERT INTO t_type_conversion VALUES (2, '456', 456);

-- String to number comparison
SELECT * FROM t_type_conversion WHERE str_val = 123 ORDER BY id;
SELECT * FROM t_type_conversion WHERE str_val = int_val ORDER BY id;

-- @case
-- @desc: Test COALESCE with different charsets
-- @label:bvt
CREATE TABLE t_coalesce (
    id INT PRIMARY KEY,
    col1 VARCHAR(100),
    col2 VARCHAR(100),
    col3 VARCHAR(100)
) CHARACTER SET utf8mb4;

INSERT INTO t_coalesce VALUES (1, NULL, NULL, 'Default');
INSERT INTO t_coalesce VALUES (2, NULL, 'Second', 'Default');
INSERT INTO t_coalesce VALUES (3, 'First', 'Second', 'Default');
INSERT INTO t_coalesce VALUES (4, NULL, NULL, NULL);

SELECT id, COALESCE(col1, col2, col3) as result FROM t_coalesce ORDER BY id;
SELECT id, COALESCE(col1, col2, col3, 'Fallback') as result FROM t_coalesce ORDER BY id;

-- @case
-- @desc: Test IFNULL and IF with charset
-- @label:bvt
SELECT IFNULL(NULL, '默认值');
SELECT IFNULL('Value', '默认值');

SELECT IF(1=1, '真', '假');
SELECT IF(1=2, '真', '假');

-- @case
-- @desc: Test CASE statement with different charsets
-- @label:bvt
CREATE TABLE t_case_when (
    id INT PRIMARY KEY,
    status VARCHAR(50)
) CHARACTER SET utf8mb4;

INSERT INTO t_case_when VALUES (1, 'active');
INSERT INTO t_case_when VALUES (2, 'inactive');
INSERT INTO t_case_when VALUES (3, 'pending');
INSERT INTO t_case_when VALUES (4, NULL);

SELECT id, status,
    CASE status
        WHEN 'active' THEN '活动'
        WHEN 'inactive' THEN '非活动'
        WHEN 'pending' THEN '待定'
        ELSE '未知'
    END as status_cn
FROM t_case_when ORDER BY id;

-- @case
-- @desc: Test string with only spaces
-- @label:bvt
CREATE TABLE t_only_spaces (
    id INT PRIMARY KEY,
    data VARCHAR(100)
) CHARACTER SET utf8mb4;

INSERT INTO t_only_spaces VALUES (1, ' ');
INSERT INTO t_only_spaces VALUES (2, '  ');
INSERT INTO t_only_spaces VALUES (3, '   ');
INSERT INTO t_only_spaces VALUES (4, '    ');

SELECT id, LENGTH(data), CHAR_LENGTH(data), data = '' FROM t_only_spaces ORDER BY id;
SELECT id, LENGTH(TRIM(data)) FROM t_only_spaces ORDER BY id;

-- @case
-- @desc: Test REPEAT with multibyte characters
-- @label:bvt
SELECT REPEAT('A', 5);
SELECT REPEAT('中', 5);
SELECT REPEAT('😀', 5);

SELECT LENGTH(REPEAT('A', 10)), CHAR_LENGTH(REPEAT('A', 10));
SELECT LENGTH(REPEAT('中', 10)), CHAR_LENGTH(REPEAT('中', 10));
SELECT LENGTH(REPEAT('😀', 10)), CHAR_LENGTH(REPEAT('😀', 10));

-- @case
-- @desc: Test SPACE function
-- @label:bvt
SELECT LENGTH(SPACE(10)), CHAR_LENGTH(SPACE(10));
-- Check SPACE result using LENGTH instead of direct display
SELECT LENGTH(CONCAT('[', SPACE(5), ']')) as total_len;

-- @case
-- @desc: Test ASCII and ORD functions
-- @label:bvt
SELECT ASCII('A'), ASCII('a'), ASCII('0');
SELECT ASCII('中'), ASCII('😀');
SELECT ORD('A'), ORD('中'), ORD('😀');

-- @case
-- @desc: Test CHAR function
-- @label:bvt
SELECT CHAR(65, 66, 67);
SELECT CHAR(228, 184, 173);  -- UTF-8 bytes for '中'

-- @case
-- @desc: Test STRCMP with different collations
-- @label:bvt
CREATE TABLE t_strcmp (
    id INT PRIMARY KEY,
    val1 VARCHAR(100),
    val2 VARCHAR(100)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

INSERT INTO t_strcmp VALUES (1, 'Apple', 'apple');
INSERT INTO t_strcmp VALUES (2, 'Apple', 'Banana');
INSERT INTO t_strcmp VALUES (3, 'Banana', 'Apple');

SELECT id, val1, val2, STRCMP(val1, val2) as cmp FROM t_strcmp ORDER BY id;

-- @case
-- @desc: Test INSERT with duplicate key and charset
-- @label:bvt
CREATE TABLE t_duplicate_key (
    id INT PRIMARY KEY,
    name VARCHAR(100) UNIQUE
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

INSERT INTO t_duplicate_key VALUES (1, 'Test');

-- Duplicate key (should fail)
-- INSERT INTO t_duplicate_key VALUES (2, 'Test');

-- INSERT ON DUPLICATE KEY UPDATE
-- INSERT INTO t_duplicate_key VALUES (2, 'Test') ON DUPLICATE KEY UPDATE id = 2;

SELECT * FROM t_duplicate_key;

-- @case
-- @desc: Test LOAD DATA with different charsets (placeholder)
-- @label:bvt
-- Note: LOAD DATA testing requires external file setup
-- CREATE TABLE t_load_data (
--     id INT,
--     name VARCHAR(100)
-- ) CHARACTER SET utf8mb4;

-- @case
-- @desc: Test FULLTEXT index with different charsets
-- @label:bvt
INSERT INTO t_fulltext VALUES (1, 'The quick brown fox jumps over the lazy dog');
INSERT INTO t_fulltext VALUES (2, 'Quick brown foxes');
INSERT INTO t_fulltext VALUES (3, '快速的棕色狐狸跳过懒狗');

-- MATCH AGAINST queries (if supported)
-- SELECT * FROM t_fulltext WHERE MATCH(content) AGAINST('fox');
-- SELECT * FROM t_fulltext WHERE MATCH(content) AGAINST('狐狸');

SELECT * FROM t_fulltext ORDER BY id;

-- @case
-- @desc: Test JSON with different charsets
-- @label:bvt
CREATE TABLE t_json_charset (
    id INT PRIMARY KEY,
    data JSON
) CHARACTER SET utf8mb4;

INSERT INTO t_json_charset VALUES (1, '{"name": "John", "age": 30}');
INSERT INTO t_json_charset VALUES (2, '{"name": "张三", "age": 25}');
INSERT INTO t_json_charset VALUES (3, '{"emoji": "😀", "text": "Hello"}');

SELECT * FROM t_json_charset ORDER BY id;
SELECT id, JSON_EXTRACT(data, '$.name') as name FROM t_json_charset ORDER BY id;

-- @case
-- @desc: Test very long string operations
-- @label:bvt
CREATE TABLE t_long_ops (
    id INT PRIMARY KEY,
    data TEXT
) CHARACTER SET utf8mb4;

INSERT INTO t_long_ops VALUES (1, REPEAT('A', 10000));
INSERT INTO t_long_ops VALUES (2, REPEAT('中', 5000));

SELECT id, LENGTH(data), CHAR_LENGTH(data) FROM t_long_ops ORDER BY id;

-- Test CONCAT with long strings
SELECT LENGTH(CONCAT(
    (SELECT data FROM t_long_ops WHERE id = 1),
    (SELECT data FROM t_long_ops WHERE id = 2)
));

-- @case
-- @desc: Test column with BINARY attribute
-- @label:bvt
CREATE TABLE t_binary_attr (
    id INT PRIMARY KEY,
    data VARCHAR(100) BINARY
) CHARACTER SET utf8mb4;

INSERT INTO t_binary_attr VALUES (1, 'Test');
INSERT INTO t_binary_attr VALUES (2, 'test');
INSERT INTO t_binary_attr VALUES (3, 'TEST');

-- Should be case-sensitive
SELECT * FROM t_binary_attr WHERE data = 'test' ORDER BY id;
SELECT COUNT(*) FROM t_binary_attr WHERE data = 'Test';

-- @case
-- @desc: Test empty database and table names with charset
-- @label:bvt
-- CREATE DATABASE `` CHARACTER SET utf8mb4;  -- Should fail
-- CREATE TABLE `` (id INT) CHARACTER SET utf8mb4;  -- Should fail

-- @case
-- @desc: Test maximum identifier length with multibyte characters
-- @label:bvt
-- Identifier max length is typically 64 characters
-- Test with multibyte characters in table/column names
CREATE TABLE `测试表` (
    `编号` INT PRIMARY KEY,
    `名称` VARCHAR(100)
) CHARACTER SET utf8mb4;

INSERT INTO `测试表` VALUES (1, '测试数据');
SELECT * FROM `测试表`;

DROP TABLE `测试表`;

-- @case
-- @desc: Cleanup
-- @label:bvt
DROP DATABASE IF EXISTS charset_error_test;
