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

select locate('he','hello',-2);
select locate('lo','hello',-4294967295);
select locate('lo','hello',4294967295);
select locate('lo','hello',-4294967296);
select locate('lo','hello',4294967296);
select locate('lo','hello',-4294967297);
select locate('lo','hello',4294967297);
select locate('DB', 'ewhDBfjiejwew');
select locate('db', 'HHUHDNDBDBDBfewoiujfewqf vew');
select locate("3q", 'fwfwefw3rqfw3fqff3qwfw');
select locate("&&**", " ekwefw&&&**^^&3928u3f ") as result;
select locate(1,2);
select locate(100, 2300100100);
select locate(-100, 3243211433100);

select locate('foo', null);
select locate(null, 'o');
select locate(null, null);
select locate('foo', null) is null;
select locate(null, 'o') is null;
select locate(null, null) is null;
select isnull(locate('foo', null));
select isnull(locate(null, 'o'));
select isnull(locate(null, null));

select locate('lo','hello',3) as result;
select locate('he','hello',null),locate('he',null,2),locate(null,'hello',2);
select locate(null,'hello',null),locate('he',null,null);

-- locate function nested with string function
select locate('abc',ltrim('   ueenjfwabc123'));
select locate('123',rtrim('  3782dfw23123123123   '));
select locate('%^',trim('  32431 %^ 3829  3huicn2432g23   '));
select locate('12',substring('21214avewwe12 ',3,20));
select locate('kjs', reverse('sjkdakjevenjwvev')) as result;

-- select locate in prepare statement
drop table if exists locate01;
create table locate01(col1 char(5), col2 varchar(20));
insert into locate01 values ('da','database数据库DB数据库管理系统');
insert into locate01 values (null,null);
insert into locate01 values ('ABCDF','GHIJKLMNabcdfeowejfwve');
insert into locate01 values ('圣诞节快乐','圣诞节快乐Merry Charismas!');
insert into locate01 values ('@#$', '^&**(&^$@@#$');
select * from locate01;
prepare s1 from 'select locate(col1, col2) from locate01';
execute s1;
prepare s2 from 'select isnull(locate(col1, col2)) from locate01';
execute s2;
drop table locate01;

drop table if exists locate02;
create table locate02 (col1 tinytext, col2 text);
insert into locate02 values ('融合异构分布式','MatrixOne 是一款超融合异构分布式数据库，通过云原生化和存储、计算、事务分离的架构构建 HSTAP 超融合数据引擎，实现单一数据库系统支持 OLTP、OLAP、流计算等多种业务负载，并且支持公有云、私有云、边缘云部署和使用，实现异构基础设施的兼容。');
insert into locate02 values (null, 'MatrixOne 具备实时 HTAP，多租户，流式计算，极致扩展性，高性价比，企业级高可用及 MySQL 高度兼容等重要特性，通过为用户提供一站式超融合数据解决方案，可以将过去由多个数据库完成的工作合并到一个数据库里，从而简化开发运维，消减数据碎片，提高开发敏捷度。');
insert into locate02 values ('分布式', null);
select *  from locate02;
select locate(col1, col2) from locate02;
select locate(col1, col2, 10) from locate02;
select locate(col1, col2, -10) form locate02;
drop table locate02;

-- select locate in temporary table
drop table if exists locate03;
create temporary table locate03(col1 char(5), col2 mediumtext);
insert into locate03 values ('DB','database数据库DB数据库管理系统');
insert into locate03 values (null,null);
insert into locate03 values ('ABCDF','GHIJKLMNabcdfeowejfwve');
insert into locate03 values ('圣诞节快乐','圣诞节快乐Merry Charismas!');
insert into locate03 values ('@#$', '^&**(&^$@@#$');
select * from locate03;
prepare s1 from 'select locate(col1, col2) from locate03';
execute s1;
prepare s2 from 'select isnull(locate(col1, col2)) from locate03';
execute s2;
drop table locate03;
drop database testx;



