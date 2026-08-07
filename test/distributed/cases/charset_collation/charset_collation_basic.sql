-- @suit
-- @setup
DROP DATABASE IF EXISTS charset_test;

-- @case
-- @desc: Test SHOW CHARSET command
-- @label:bvt
SHOW CHARSET;
SHOW CHARACTER SET;
SHOW CHARSET LIKE 'utf8%';
SHOW CHARACTER SET WHERE Charset = 'utf8mb4';
SELECT CHARACTER_SET_NAME, DEFAULT_COLLATE_NAME, MAXLEN
FROM information_schema.CHARACTER_SETS
ORDER BY CHARACTER_SET_NAME;

-- @case
-- @desc: Test SHOW COLLATION command
-- @label:bvt
SHOW COLLATION;
SHOW COLLATION LIKE 'utf8mb4%';
SHOW COLLATION WHERE Charset = 'utf8mb4';
SHOW COLLATION WHERE Collation = 'utf8mb4_bin';
SHOW COLLATION WHERE Charset = 'utf8mb4' AND Collation LIKE '%bin%';

-- @case
-- @desc: Test database level charset and collation
-- @label:bvt
CREATE DATABASE charset_test;
USE charset_test;

-- information_schema metadata used by ODBC SQLColumns
CREATE TABLE charset_metadata_repro (
    c_char CHAR(8),
    c_varchar VARCHAR(128),
    c_text TEXT
);
SELECT c.data_type, c.character_set_name,
       c.character_maximum_length, c.character_octet_length, cs.maxlen
FROM information_schema.columns c
LEFT JOIN information_schema.character_sets cs
  ON c.character_set_name = cs.character_set_name
WHERE c.table_schema = 'charset_test'
  AND c.table_name = 'charset_metadata_repro'
  AND c.column_name IN ('c_char', 'c_varchar', 'c_text')
ORDER BY c.ordinal_position;
DROP TABLE charset_metadata_repro;

-- Create database with explicit charset
DROP DATABASE IF EXISTS charset_test_utf8;
CREATE DATABASE charset_test_utf8 CHARACTER SET utf8;
SHOW CREATE DATABASE charset_test_utf8;

DROP DATABASE IF EXISTS charset_test_utf8mb4;
CREATE DATABASE charset_test_utf8mb4 CHARACTER SET utf8mb4;
SHOW CREATE DATABASE charset_test_utf8mb4;

-- Create database with charset and collation
DROP DATABASE IF EXISTS charset_test_utf8mb4_bin;
CREATE DATABASE charset_test_utf8mb4_bin CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
SHOW CREATE DATABASE charset_test_utf8mb4_bin;

DROP DATABASE IF EXISTS charset_test_utf8mb4_general;
CREATE DATABASE charset_test_utf8mb4_general CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
SHOW CREATE DATABASE charset_test_utf8mb4_general;

-- @case
-- @desc: Test table level charset and collation
-- @label:bvt
USE charset_test;

-- Default charset table
CREATE TABLE t_default (
    id INT PRIMARY KEY,
    name VARCHAR(100)
);
SHOW CREATE TABLE t_default;

-- UTF8 charset table
CREATE TABLE t_utf8 (
    id INT PRIMARY KEY,
    name VARCHAR(100)
) CHARACTER SET utf8;
SHOW CREATE TABLE t_utf8;

-- UTF8MB4 with different collations
CREATE TABLE t_utf8mb4_bin (
    id INT PRIMARY KEY,
    name VARCHAR(100)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
SHOW CREATE TABLE t_utf8mb4_bin;

CREATE TABLE t_utf8mb4_general_ci (
    id INT PRIMARY KEY,
    name VARCHAR(100)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
SHOW CREATE TABLE t_utf8mb4_general_ci;

CREATE TABLE t_utf8mb4_unicode_ci (
    id INT PRIMARY KEY,
    name VARCHAR(100)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- @case
-- @desc: Test column level charset and collation
-- @label:bvt
CREATE TABLE t_column_charset (
    id INT PRIMARY KEY,
    name_default VARCHAR(100),
    name_utf8 VARCHAR(100) CHARACTER SET utf8,
    name_utf8mb4 VARCHAR(100) CHARACTER SET utf8mb4,
    name_utf8mb4_bin VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
    name_utf8mb4_general VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci
);
SHOW CREATE TABLE t_column_charset;
DESC t_column_charset;

-- @case
-- @desc: Test data insertion and retrieval with different charsets
-- @label:bvt
CREATE TABLE t_insert_test (
    id INT PRIMARY KEY,
    data VARCHAR(200)
) CHARACTER SET utf8mb4;

-- Insert ASCII characters
INSERT INTO t_insert_test VALUES (1, 'Hello World');
INSERT INTO t_insert_test VALUES (2, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ');
INSERT INTO t_insert_test VALUES (3, 'abcdefghijklmnopqrstuvwxyz');
INSERT INTO t_insert_test VALUES (4, '0123456789');

-- Insert special characters
INSERT INTO t_insert_test VALUES (5, '!@#$%^&*()_+-=[]{}|;:,.<>?');
INSERT INTO t_insert_test VALUES (6, '`~"\''\\');

-- Insert Unicode characters (Chinese)
INSERT INTO t_insert_test VALUES (10, '你好世界');
INSERT INTO t_insert_test VALUES (11, '中文测试');
INSERT INTO t_insert_test VALUES (12, '汉字');

-- Insert Unicode characters (Japanese)
INSERT INTO t_insert_test VALUES (20, 'こんにちは');
INSERT INTO t_insert_test VALUES (21, 'ひらがな');
INSERT INTO t_insert_test VALUES (22, 'カタカナ');

-- Insert Unicode characters (Korean)
INSERT INTO t_insert_test VALUES (30, '안녕하세요');
INSERT INTO t_insert_test VALUES (31, '한글');

-- Insert Unicode characters (Emoji)
INSERT INTO t_insert_test VALUES (40, '😀😃😄😁');
INSERT INTO t_insert_test VALUES (41, '🎉🎊🎈');
INSERT INTO t_insert_test VALUES (42, '❤️💕💖');

-- Insert Unicode characters (Various languages)
INSERT INTO t_insert_test VALUES (50, 'Привет');  -- Russian
INSERT INTO t_insert_test VALUES (51, 'مرحبا');   -- Arabic
INSERT INTO t_insert_test VALUES (52, 'שלום');    -- Hebrew
INSERT INTO t_insert_test VALUES (53, 'Γειά σου'); -- Greek
INSERT INTO t_insert_test VALUES (54, 'สวัสดี');  -- Thai

-- Insert mixed content
INSERT INTO t_insert_test VALUES (60, 'Hello 你好 こんにちは 안녕');
INSERT INTO t_insert_test VALUES (61, 'Test测试テスト테스트');

SELECT * FROM t_insert_test ORDER BY id;

-- @case
-- @desc: Test string comparison with case-insensitive collation
-- @label:bvt
CREATE TABLE t_compare_ci (
    id INT PRIMARY KEY,
    name VARCHAR(100)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

INSERT INTO t_compare_ci VALUES (1, 'Apple');
INSERT INTO t_compare_ci VALUES (2, 'apple');
INSERT INTO t_compare_ci VALUES (3, 'APPLE');
INSERT INTO t_compare_ci VALUES (4, 'aPpLe');

-- Case-insensitive comparison
SELECT * FROM t_compare_ci WHERE name = 'apple';
SELECT * FROM t_compare_ci WHERE name = 'APPLE';
SELECT * FROM t_compare_ci WHERE name = 'Apple';

SELECT COUNT(*) FROM t_compare_ci WHERE name = 'apple';

-- @case
-- @desc: Test string comparison with binary collation
-- @label:bvt
CREATE TABLE t_compare_bin (
    id INT PRIMARY KEY,
    name VARCHAR(100)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

INSERT INTO t_compare_bin VALUES (1, 'Apple');
INSERT INTO t_compare_bin VALUES (2, 'apple');
INSERT INTO t_compare_bin VALUES (3, 'APPLE');
INSERT INTO t_compare_bin VALUES (4, 'aPpLe');

-- Case-sensitive comparison
SELECT * FROM t_compare_bin WHERE name = 'apple';
SELECT * FROM t_compare_bin WHERE name = 'APPLE';
SELECT * FROM t_compare_bin WHERE name = 'Apple';

SELECT COUNT(*) FROM t_compare_bin WHERE name = 'apple';
SELECT COUNT(*) FROM t_compare_bin WHERE name = 'APPLE';

-- @case
-- @desc: Test ORDER BY with different collations
-- @label:bvt
CREATE TABLE t_sort_ci (
    id INT PRIMARY KEY,
    name VARCHAR(100)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

INSERT INTO t_sort_ci VALUES (1, 'zebra');
INSERT INTO t_sort_ci VALUES (2, 'Zebra');
INSERT INTO t_sort_ci VALUES (3, 'ZEBRA');
INSERT INTO t_sort_ci VALUES (4, 'apple');
INSERT INTO t_sort_ci VALUES (5, 'Apple');
INSERT INTO t_sort_ci VALUES (6, 'APPLE');
INSERT INTO t_sort_ci VALUES (7, 'banana');
INSERT INTO t_sort_ci VALUES (8, 'Banana');
INSERT INTO t_sort_ci VALUES (9, 'BANANA');

SELECT * FROM t_sort_ci ORDER BY name;
SELECT * FROM t_sort_ci ORDER BY name DESC;

CREATE TABLE t_sort_bin (
    id INT PRIMARY KEY,
    name VARCHAR(100)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

INSERT INTO t_sort_bin VALUES (1, 'zebra');
INSERT INTO t_sort_bin VALUES (2, 'Zebra');
INSERT INTO t_sort_bin VALUES (3, 'ZEBRA');
INSERT INTO t_sort_bin VALUES (4, 'apple');
INSERT INTO t_sort_bin VALUES (5, 'Apple');
INSERT INTO t_sort_bin VALUES (6, 'APPLE');
INSERT INTO t_sort_bin VALUES (7, 'banana');
INSERT INTO t_sort_bin VALUES (8, 'Banana');
INSERT INTO t_sort_bin VALUES (9, 'BANANA');

SELECT * FROM t_sort_bin ORDER BY name;
SELECT * FROM t_sort_bin ORDER BY name DESC;

-- @case
-- @desc: Test DISTINCT with different collations
-- @label:bvt
CREATE TABLE t_distinct_ci (
    name VARCHAR(100)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

INSERT INTO t_distinct_ci VALUES ('Apple'), ('apple'), ('APPLE');
INSERT INTO t_distinct_ci VALUES ('Banana'), ('banana'), ('BANANA');

SELECT DISTINCT name FROM t_distinct_ci ORDER BY name;
SELECT COUNT(DISTINCT name) FROM t_distinct_ci;

CREATE TABLE t_distinct_bin (
    name VARCHAR(100)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

INSERT INTO t_distinct_bin VALUES ('Apple'), ('apple'), ('APPLE');
INSERT INTO t_distinct_bin VALUES ('Banana'), ('banana'), ('BANANA');

SELECT DISTINCT name FROM t_distinct_bin ORDER BY name;
SELECT COUNT(DISTINCT name) FROM t_distinct_bin;

-- @case
-- @desc: Test GROUP BY with different collations
-- @label:bvt
CREATE TABLE t_group_ci (
    id INT,
    name VARCHAR(100)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

INSERT INTO t_group_ci VALUES (1, 'Apple'), (2, 'apple'), (3, 'APPLE');
INSERT INTO t_group_ci VALUES (4, 'Banana'), (5, 'banana'), (6, 'BANANA');

SELECT name, COUNT(*) as cnt FROM t_group_ci GROUP BY name ORDER BY name;

CREATE TABLE t_group_bin (
    id INT,
    name VARCHAR(100)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

INSERT INTO t_group_bin VALUES (1, 'Apple'), (2, 'apple'), (3, 'APPLE');
INSERT INTO t_group_bin VALUES (4, 'Banana'), (5, 'banana'), (6, 'BANANA');

SELECT name, COUNT(*) as cnt FROM t_group_bin GROUP BY name ORDER BY name;

-- @case
-- @desc: Test LIKE operator with different collations
-- @label:bvt
CREATE TABLE t_like_ci (
    id INT PRIMARY KEY,
    name VARCHAR(100)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

INSERT INTO t_like_ci VALUES (1, 'Apple');
INSERT INTO t_like_ci VALUES (2, 'apple');
INSERT INTO t_like_ci VALUES (3, 'Application');
INSERT INTO t_like_ci VALUES (4, 'Banana');

SELECT * FROM t_like_ci WHERE name LIKE 'app%' ORDER BY id;
SELECT * FROM t_like_ci WHERE name LIKE 'APP%' ORDER BY id;
SELECT * FROM t_like_ci WHERE name LIKE '%le' ORDER BY id;

CREATE TABLE t_like_bin (
    id INT PRIMARY KEY,
    name VARCHAR(100)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

INSERT INTO t_like_bin VALUES (1, 'Apple');
INSERT INTO t_like_bin VALUES (2, 'apple');
INSERT INTO t_like_bin VALUES (3, 'Application');
INSERT INTO t_like_bin VALUES (4, 'Banana');

SELECT * FROM t_like_bin WHERE name LIKE 'app%' ORDER BY id;
SELECT * FROM t_like_bin WHERE name LIKE 'APP%' ORDER BY id;
SELECT * FROM t_like_bin WHERE name LIKE '%le' ORDER BY id;

-- @case
-- @desc: Test JOIN with different collations
-- @label:bvt
CREATE TABLE t_join1_ci (
    id INT PRIMARY KEY,
    name VARCHAR(100)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

CREATE TABLE t_join2_ci (
    id INT PRIMARY KEY,
    name VARCHAR(100)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

INSERT INTO t_join1_ci VALUES (1, 'Apple'), (2, 'Banana'), (3, 'Cherry');
INSERT INTO t_join2_ci VALUES (1, 'apple'), (2, 'BANANA'), (3, 'cherry');

SELECT t1.id, t1.name, t2.name 
FROM t_join1_ci t1 
JOIN t_join2_ci t2 ON t1.name = t2.name 
ORDER BY t1.id;

CREATE TABLE t_join1_bin (
    id INT PRIMARY KEY,
    name VARCHAR(100)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE TABLE t_join2_bin (
    id INT PRIMARY KEY,
    name VARCHAR(100)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

INSERT INTO t_join1_bin VALUES (1, 'Apple'), (2, 'Banana'), (3, 'Cherry');
INSERT INTO t_join2_bin VALUES (1, 'apple'), (2, 'BANANA'), (3, 'cherry');

SELECT t1.id, t1.name, t2.name 
FROM t_join1_bin t1 
JOIN t_join2_bin t2 ON t1.name = t2.name 
ORDER BY t1.id;

-- @case
-- @desc: Test UNION with different collations
-- @label:bvt
CREATE TABLE t_union1_ci (
    name VARCHAR(100)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

CREATE TABLE t_union2_ci (
    name VARCHAR(100)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

INSERT INTO t_union1_ci VALUES ('Apple'), ('Banana');
INSERT INTO t_union2_ci VALUES ('apple'), ('Cherry');

SELECT name FROM t_union1_ci
UNION
SELECT name FROM t_union2_ci
ORDER BY name;

SELECT name FROM t_union1_ci
UNION ALL
SELECT name FROM t_union2_ci
ORDER BY name;

-- @case
-- @desc: Test string functions with different charsets
-- @label:bvt
CREATE TABLE t_functions (
    id INT PRIMARY KEY,
    data VARCHAR(200)
) CHARACTER SET utf8mb4;

INSERT INTO t_functions VALUES (1, 'Hello World');
INSERT INTO t_functions VALUES (2, '你好世界');
INSERT INTO t_functions VALUES (3, 'こんにちは');
INSERT INTO t_functions VALUES (4, '안녕하세요');

-- LENGTH vs CHAR_LENGTH
SELECT id, data, LENGTH(data) as byte_len, CHAR_LENGTH(data) as char_len 
FROM t_functions ORDER BY id;

-- UPPER and LOWER
SELECT id, data, UPPER(data) as upper_data, LOWER(data) as lower_data 
FROM t_functions WHERE id = 1;

-- SUBSTRING
SELECT id, SUBSTRING(data, 1, 5) as substr FROM t_functions ORDER BY id;

-- CONCAT
SELECT CONCAT('prefix_', data, '_suffix') as result FROM t_functions WHERE id = 1;
SELECT CONCAT('前缀_', data, '_后缀') as result FROM t_functions WHERE id = 2;

-- @case
-- @desc: Test ALTER TABLE with charset and collation
-- @label:bvt
CREATE TABLE t_alter (
    id INT PRIMARY KEY,
    name VARCHAR(100)
);

-- Alter table charset
ALTER TABLE t_alter CHARACTER SET utf8mb4;
SHOW CREATE TABLE t_alter;

-- Alter table collation
ALTER TABLE t_alter COLLATE utf8mb4_bin;
SHOW CREATE TABLE t_alter;

-- Alter column charset
ALTER TABLE t_alter MODIFY COLUMN name VARCHAR(100) CHARACTER SET utf8mb4;
SHOW CREATE TABLE t_alter;

-- Alter column collation
ALTER TABLE t_alter MODIFY COLUMN name VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
SHOW CREATE TABLE t_alter;

-- @case
-- @desc: Test index with different collations
-- @label:bvt
CREATE TABLE t_index_ci (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    KEY idx_name (name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

INSERT INTO t_index_ci VALUES (1, 'Apple'), (2, 'apple'), (3, 'APPLE');
INSERT INTO t_index_ci VALUES (4, 'Banana'), (5, 'banana');

SELECT * FROM t_index_ci WHERE name = 'apple' ORDER BY id;

CREATE TABLE t_index_bin (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    KEY idx_name (name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

INSERT INTO t_index_bin VALUES (1, 'Apple'), (2, 'apple'), (3, 'APPLE');
INSERT INTO t_index_bin VALUES (4, 'Banana'), (5, 'banana');

SELECT * FROM t_index_bin WHERE name = 'apple' ORDER BY id;

-- @case
-- @desc: Test unique constraint with different collations
-- @label:bvt
CREATE TABLE t_unique_ci (
    id INT PRIMARY KEY,
    name VARCHAR(100) UNIQUE
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

INSERT INTO t_unique_ci VALUES (1, 'Apple');
-- This should fail due to case-insensitive unique constraint
-- INSERT INTO t_unique_ci VALUES (2, 'apple');

SELECT * FROM t_unique_ci;

CREATE TABLE t_unique_bin (
    id INT PRIMARY KEY,
    name VARCHAR(100) UNIQUE
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

INSERT INTO t_unique_bin VALUES (1, 'Apple');
-- This should succeed with case-sensitive unique constraint
INSERT INTO t_unique_bin VALUES (2, 'apple');
INSERT INTO t_unique_bin VALUES (3, 'APPLE');

SELECT * FROM t_unique_bin ORDER BY id;

-- @case
-- @desc: Test special characters and escape sequences
-- @label:bvt
CREATE TABLE t_special_chars (
    id INT PRIMARY KEY,
    data VARCHAR(200)
) CHARACTER SET utf8mb4;

INSERT INTO t_special_chars VALUES (1, 'Tab:\ttest');
INSERT INTO t_special_chars VALUES (2, 'Newline:\ntest');
INSERT INTO t_special_chars VALUES (3, 'Quote:\'test\'');
INSERT INTO t_special_chars VALUES (4, 'Double quote:\"test\"');
INSERT INTO t_special_chars VALUES (5, 'Backslash:\\test');
INSERT INTO t_special_chars VALUES (6, 'NULL:\0test');

-- Use LENGTH to verify special characters are stored correctly
SELECT id, LENGTH(data), CHAR_LENGTH(data) FROM t_special_chars ORDER BY id;
-- Use HEX to verify the exact bytes
SELECT id, HEX(data) FROM t_special_chars WHERE id IN (1, 6) ORDER BY id;

-- @case
-- @desc: Test long strings with different charsets
-- @label:bvt
CREATE TABLE t_long_strings (
    id INT PRIMARY KEY,
    data TEXT
) CHARACTER SET utf8mb4;

INSERT INTO t_long_strings VALUES (1, REPEAT('A', 1000));
INSERT INTO t_long_strings VALUES (2, REPEAT('你', 500));
INSERT INTO t_long_strings VALUES (3, REPEAT('😀', 250));

SELECT id, LENGTH(data) as byte_len, CHAR_LENGTH(data) as char_len 
FROM t_long_strings ORDER BY id;

-- @case
-- @desc: Test CHARSET() and COLLATION() functions
-- @label:bvt
SELECT CHARSET('test');
SELECT CHARSET('你好');
SELECT COLLATION('test');
SELECT COLLATION('你好');

-- @case
-- @desc: Test comparison operators with different collations
-- @label:bvt
CREATE TABLE t_comparison (
    id INT PRIMARY KEY,
    name VARCHAR(100)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

INSERT INTO t_comparison VALUES (1, 'Apple');
INSERT INTO t_comparison VALUES (2, 'Banana');
INSERT INTO t_comparison VALUES (3, 'Cherry');

SELECT * FROM t_comparison WHERE name > 'apple' ORDER BY id;
SELECT * FROM t_comparison WHERE name >= 'apple' ORDER BY id;
SELECT * FROM t_comparison WHERE name < 'cherry' ORDER BY id;
SELECT * FROM t_comparison WHERE name <= 'cherry' ORDER BY id;
SELECT * FROM t_comparison WHERE name BETWEEN 'Apple' AND 'Cherry' ORDER BY id;

-- @case
-- @desc: Test IN operator with different collations
-- @label:bvt
SELECT * FROM t_comparison WHERE name IN ('apple', 'BANANA', 'cherry') ORDER BY id;

-- @case
-- @desc: Test MIN/MAX with different collations
-- @label:bvt
SELECT MIN(name) as min_name, MAX(name) as max_name FROM t_comparison;

CREATE TABLE t_minmax_bin (
    id INT,
    name VARCHAR(100)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

INSERT INTO t_minmax_bin VALUES (1, 'Apple'), (2, 'apple'), (3, 'APPLE');

SELECT MIN(name) as min_name, MAX(name) as max_name FROM t_minmax_bin;

CREATE TABLE t_minmax_bin_pad (
    name VARCHAR(100)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

INSERT INTO t_minmax_bin_pad VALUES ('a '), (CONCAT('a', CHAR(0)));

SELECT HEX(MIN(name)) AS min_name, HEX(MAX(name)) AS max_name
FROM t_minmax_bin_pad;

CREATE TABLE t_minmax_cast_ci (
    name VARCHAR(100)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

INSERT INTO t_minmax_cast_ci VALUES ('a'), ('B');

SELECT MIN(CAST(name AS CHAR)) as min_name, MAX(CAST(name AS CHAR)) as max_name
FROM t_minmax_cast_ci;

CREATE TABLE t_minmax_derived_bin (
    name VARCHAR(100),
    wide_name VARCHAR(200)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

INSERT INTO t_minmax_derived_bin VALUES ('a', 'a'), ('B', 'B');

SELECT MIN(CONVERT(name USING binary)) AS min_binary,
       MAX(CONVERT(name USING binary)) AS max_binary
FROM t_minmax_derived_bin;
SELECT MIN(CONVERT(name USING utf8mb4)) AS min_utf8,
       MAX(CONVERT(name USING utf8mb4)) AS max_utf8
FROM t_minmax_derived_bin;
SELECT MIN(SUBSTRING(name, 1)) AS min_substring,
       MAX(SUBSTRING(name, 1)) AS max_substring
FROM t_minmax_derived_bin;
SELECT MIN(CASE WHEN name IS NOT NULL THEN name ELSE wide_name END) AS min_case,
       MAX(CASE WHEN name IS NOT NULL THEN name ELSE wide_name END) AS max_case
FROM t_minmax_derived_bin;
SELECT MIN(COALESCE(name, wide_name)) AS min_coalesce,
       MAX(COALESCE(name, wide_name)) AS max_coalesce
FROM t_minmax_derived_bin;
SELECT MIN(IF(name IS NOT NULL, name, wide_name)) AS min_if,
       MAX(IF(name IS NOT NULL, name, wide_name)) AS max_if
FROM t_minmax_derived_bin;

CREATE TABLE t_minmax_least_greatest_bin (
    varchar_name VARCHAR(100),
    text_name TEXT
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
INSERT INTO t_minmax_least_greatest_bin VALUES ('a', 'a'), ('B', 'B');
SELECT MIN(LEAST(varchar_name, text_name)) AS min_least,
       MAX(GREATEST(text_name, varchar_name)) AS max_greatest
FROM t_minmax_least_greatest_bin;

CREATE TABLE t_minmax_json_least_greatest_bin (
    varchar_name VARCHAR(100),
    json_name JSON
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
INSERT INTO t_minmax_json_least_greatest_bin VALUES
    ('a', JSON_EXTRACT('"a"', '$')), ('B', JSON_EXTRACT('"B"', '$'));
SELECT MIN(LEAST(varchar_name, json_name)) AS min_json_least,
       MAX(GREATEST(json_name, varchar_name)) AS max_json_greatest
FROM t_minmax_json_least_greatest_bin;

SELECT MIN(x) AS min_union, MAX(x) AS max_union
FROM (
    SELECT name AS x FROM t_minmax_cast_ci
    UNION ALL
    SELECT NULL AS x
) AS u;

SELECT MIN(x) AS min_derived_null_union, MAX(x) AS max_derived_null_union
FROM (
    SELECT name AS x FROM t_minmax_cast_ci
    UNION ALL
    SELECT x FROM (SELECT NULL AS x) AS n
) AS u;

WITH n AS (SELECT NULL AS x)
SELECT MIN(x) AS min_cte_null_union, MAX(x) AS max_cte_null_union
FROM (
    SELECT name AS x FROM t_minmax_cast_ci
    UNION ALL
    SELECT x FROM n
) AS u;

SELECT MIN(binary_gc) AS min_binary_gc, MAX(binary_gc) AS max_binary_gc,
       MIN(bin_gc) AS min_bin_gc, MAX(bin_gc) AS max_bin_gc
FROM (
    SELECT GROUP_CONCAT(CONVERT(name USING binary)) AS binary_gc,
           GROUP_CONCAT(name) AS bin_gc
    FROM t_minmax_derived_bin
    GROUP BY name
) AS g;

CREATE TABLE t_binary_default_general_columns (
    id INT,
    create_col VARCHAR(100) CHARACTER SET utf8mb4,
    modify_col VARCHAR(100) COLLATE utf8mb4_bin
) CHARACTER SET binary;
ALTER TABLE t_binary_default_general_columns
    ADD COLUMN add_col VARCHAR(100) COLLATE utf8mb4_general_ci;
ALTER TABLE t_binary_default_general_columns
    MODIFY COLUMN modify_col VARCHAR(100) COLLATE utf8mb4_general_ci;
INSERT INTO t_binary_default_general_columns VALUES
    (1, 'a', 'a', 'a'), (2, 'B', 'B', 'B');
SELECT MIN(create_col) AS min_create, MAX(create_col) AS max_create,
       MIN(add_col) AS min_add, MAX(add_col) AS max_add,
       MIN(modify_col) AS min_modify, MAX(modify_col) AS max_modify
FROM t_binary_default_general_columns;

-- @case
-- @desc: Test CAST with charset
-- @label:bvt
SELECT CAST('test' AS CHAR CHARACTER SET utf8mb4);
SELECT CAST('你好' AS CHAR(10) CHARACTER SET utf8mb4);

-- @case
-- @desc: Test edge cases and boundary conditions
-- @label:bvt
CREATE TABLE t_edge_cases (
    id INT PRIMARY KEY,
    data VARCHAR(200)
) CHARACTER SET utf8mb4;

-- Empty string
INSERT INTO t_edge_cases VALUES (1, '');

-- Single character
INSERT INTO t_edge_cases VALUES (2, 'A');
INSERT INTO t_edge_cases VALUES (3, '中');
INSERT INTO t_edge_cases VALUES (4, '😀');

-- Space characters
INSERT INTO t_edge_cases VALUES (5, ' ');
INSERT INTO t_edge_cases VALUES (6, '  ');
INSERT INTO t_edge_cases VALUES (7, '   ');

-- Mixed spaces and characters
INSERT INTO t_edge_cases VALUES (8, ' test ');
INSERT INTO t_edge_cases VALUES (9, '  test  ');

SELECT * FROM t_edge_cases ORDER BY id;
SELECT id, LENGTH(data), CHAR_LENGTH(data) FROM t_edge_cases ORDER BY id;

-- @case
-- @desc: Cleanup
-- @label:bvt
DROP DATABASE IF EXISTS charset_test;
DROP DATABASE IF EXISTS charset_test_utf8;
DROP DATABASE IF EXISTS charset_test_utf8mb4;
DROP DATABASE IF EXISTS charset_test_utf8mb4_bin;
DROP DATABASE IF EXISTS charset_test_utf8mb4_general;
