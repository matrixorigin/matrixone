create database testx;
use testx;

SELECT LOCATE(null, "begtut.com", null) AS MatchPosition;
--预期结果：NULL

SELECT LOCATE("com", "begtut.com", null) AS MatchPosition;
--预期结果：NULL

SELECT LOCATE("com", null, 3) AS MatchPosition;
--预期结果：NULL

SELECT LOCATE(NULL, null, 3) AS MatchPosition;
--预期结果：NULL

SELECT LOCATE("com", "", 3) AS MatchPosition;
-- 预期结果：0
SELECT LOCATE("", "cm", 3) AS MatchPosition;
-- 预期结果：3

SELECT LOCATE("com", "begtut.com", 3) AS MatchPosition;
-- 预期结果：8

-- 测试子字符串存在的情况
SELECT LOCATE('lo', 'Hello, World!', 4) AS position;
-- 预期结果：4

-- 测试子字符串不存在的情况
SELECT LOCATE('lo', 'Hello, World!', 7) AS position;
-- 预期结果：0

-- 测试不同的起始位置
SELECT LOCATE('is', 'This is a test string.', 6) AS position1,
       LOCATE('is', 'This is a test string.', 10) AS position2;
-- 预期结果：6, 0

-- 创建一个测试表
DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table (
      id INT AUTO_INCREMENT PRIMARY KEY,
      my_string VARCHAR(200)
);

-- 插入测试数据
INSERT INTO test_table (my_string) VALUES
('Hello, World!'),
('This is a test string.'),
('Another string to test.'),
('The quick brown fox jumps over the lazy dog.'),
('Testing the LOCATE function.'),
('12345');

-- 测试子字符串存在的情况
SELECT LOCATE('lo', my_string) AS position FROM test_table;
-- 预期结果：4, 0, 0, 0, 13, 0

-- 测试子字符串不存在的情况
SELECT LOCATE('xyz', my_string) AS position FROM test_table;
-- 预期结果：0, 0, 0, 0, 0, 0

-- 测试不同子字符串的情况
SELECT LOCATE('is', my_string) AS position FROM test_table;
-- 预期结果：0, 3, 0, 0, 0, 0

-- 测试不同字符串的情况
SELECT LOCATE('test', my_string) AS position FROM test_table;
-- 预期结果： 0 ,11 ,19 , 0 , 1 , 0

-- 清空测试数据
DROP TABLE test_table;


-------------------------------------------------------------------------------------------------
--测试包含字符串中文
DROP TABLE IF EXISTS mixed_language_table;
CREATE TABLE mixed_language_table (
      id INT AUTO_INCREMENT PRIMARY KEY,
      text_data VARCHAR(255)
);

-- 插入包含中文和英文数据的记录
INSERT INTO mixed_language_table (text_data) VALUES
('applebanana苹果orange香蕉'),
('grapepeach桃子mango'),
('西瓜猕猴桃watermelonkiwistrawberry草莓');

-- 测试用例
SELECT
    id,
    text_data,
    LOCATE('orange', text_data) AS orange_position,
    LOCATE('桃子', text_data) AS peach_position,
    LOCATE('strawberry', text_data) AS strawberry_position
FROM
    mixed_language_table;

SELECT
    id,
    text_data,
    LOCATE('orange', text_data, 14) AS orange_position,
    LOCATE('桃子', text_data, 14) AS peach_position,
    LOCATE('strawberry', text_data, 14) AS strawberry_position
FROM
    mixed_language_table;

SELECT
    id,
    text_data,
    LOCATE('orange', text_data, 5) AS orange_position,
    LOCATE('桃子', text_data, 5) AS peach_position,
    LOCATE('strawberry', text_data, 5) AS strawberry_position
FROM
    mixed_language_table;

DROP TABLE IF EXISTS mixed_language_table;
drop database testx;