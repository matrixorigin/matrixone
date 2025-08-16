select strcmp('a', 'b');
select strcmp('abc', 'acd');
select strcmp('a', null);
select strcmp('a ', 'a');
select strcmp('a', 'a');
select strcmp(BINARY 'a', BINARY 'A');
select strcmp(65, 97);

-- basic string equality test
SELECT STRCMP('apple', 'apple');
SELECT STRCMP('banana', 'apple');
SELECT STRCMP('apple', 'banana');

SELECT STRCMP('Apple', 'apple');
SELECT STRCMP('apple', 'Apple');
SELECT STRCMP('apple', 'Apple');
SELECT STRCMP('这样就实现了按与指定字符串的字典顺序升序排列的效果。如果想降序排列，只需要把 ORDER BY子句改成 ORDER BY STRCMP(name, "banana") DESC即可，原理是一样的：比较结果大的（正数大的）会排在前面，小的排在后面', '这样就实现了按与指定字符串的字典顺序升序排列的效果。如果想降序排列，只需要把 ORDER BY子句改成 ORDER BY STRCMP(name, "banana") DESC即可，原理是一样的：比较结果大的（正数大的）会排在前面，小的排在后面');


-- Basic string size comparison test
SELECT STRCMP('banana', 'apple') AS banana_vs_apple, STRCMP('apple', 'banana') AS apple_vs_banana;


-- string comparison test containing numbers
SELECT STRCMP('10', '10') AS equal_numbers, STRCMP('11', '10') AS eleven_vs_ten, STRCMP('10', '11') AS ten_vs_eleven;


-- Case sensitivity test
SELECT STRCMP(LOWER('Apple'), LOWER('apple'));
SELECT STRCMP('Apple', 'apple') AS uppercase_vs_lowercase, STRCMP('apple', 'Apple') AS lowercase_vs_uppercase;


-- NULL value handling test
SELECT STRCMP('', '');
SELECT STRCMP(' ', ' ');
SELECT
    STRCMP('apple', NULL) AS apple_vs_null,
    STRCMP(NULL, 'apple') AS null_vs_apple,
    STRCMP(NULL, NULL) AS null_vs_null;


-- Special character testing
SELECT
    STRCMP('😊', '😢') AS emoji_comparison,
    STRCMP('你', '好') AS chinese_character_comparison;


-- Multibyte character (Chinese) test
SELECT
    STRCMP('苹果', '香蕉') AS apple_vs_banana,
    STRCMP('香蕉', '苹果') AS banana_vs_apple;


-- Table field comparison test
drop database if exists test;
create database test;
use test;
drop table if exists fruits;
CREATE TABLE fruits (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(50) NOT NULL
);
INSERT INTO fruits (name) VALUES ('apple'), ('banana'), ('cherry'), ('date');

SELECT * FROM fruits WHERE STRCMP(name, 'banana') != 0;
SELECT * FROM fruits WHERE STRCMP(name, 'banana') = 0;
SELECT name FROM fruits ORDER BY STRCMP(name, 'banana');
SELECT
    name,
    STRCMP(name, 'banana') AS comparison_result
FROM
    fruits
ORDER BY
    comparison_result;
drop table fruits;

drop table if exists t_strcmp_test;
CREATE TEMPORARY TABLE t_strcmp_test(
    id          INT AUTO_INCREMENT PRIMARY KEY,
    s1          VARCHAR(100),
    s2          VARCHAR(100),
    expect      INT,
    actual      INT
);

INSERT INTO t_strcmp_test(s1, s2, expect) VALUES
('abc', 'abc', 0),
('MySQL', 'mysql', 0),
('abc', ' abc', -1),
('abc ', 'abc', 1),
('', '', 0),
(NULL, 'abc', NULL),
('abc', NULL, NULL),
('123', '0123', -1),
('2', '10', 1),
('数据库', '数据', 1),
('数据库', '数锯', -1);

UPDATE t_strcmp_test
SET actual = STRCMP(s1, s2);
SELECT  s1, s2, expect, actual
FROM t_strcmp_test
ORDER BY id;
drop table t_strcmp_test;


drop table if exists varchar_test;
create table varchar_test (a varchar(30), b varchar(30));
insert into varchar_test values('abcdef', 'abcdef');
insert into varchar_test values('_bcdef', '_bcdef');
insert into varchar_test values('mo是云原生数据库', 'Mo是云原生数据库');
insert into varchar_test values('STRCMP函数的作用是比较两个字符串的字典序列','STRCMP函数的作用是比较两个字符串的字典序列');
SELECT * FROM varchar_test WHERE STRCMP(a, b) != 0;
SELECT * FROM varchar_test WHERE STRCMP(a, b) = 0;
drop table varchar_test;

drop database test;