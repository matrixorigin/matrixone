-- @suit

-- @case
-- @desc:test for LIKE operator
-- @label:bvt

SELECT 'a' LIKE 'ae';
SELECT 'ae' LIKE 'a';
SELECT 'MYSQL' LIKE 'mysql';
SELECT 'David!' LIKE 'David_';
SELECT 'David!' LIKE '%D%v%';
SELECT 'David!' LIKE 'David\_';
SELECT 'David_' LIKE 'David\_';

DROP TABLE IF EXISTS like_test;
CREATE TABLE like_test(
    str1 VARCHAR(50)
);

INSERT INTO like_test VALUES('D:');
INSERT INTO like_test VALUES('D:\\');
INSERT INTO like_test VALUES('D:\\System_files');
INSERT INTO like_test VALUES('D:\\System_files\\');
INSERT INTO like_test VALUES(NULL);
INSERT INTO like_test VALUES('NULL');
INSERT INTO like_test VALUES('  A');
INSERT INTO like_test VALUES('s _');

SELECT * FROM like_test WHERE str1 LIKE '%:';
SELECT * FROM like_test WHERE str1 LIKE '%\_%';
SELECT * FROM like_test WHERE str1 LIKE '___';
SELECT * FROM like_test WHERE str1 LIKE ' %';
SELECT * FROM like_test WHERE str1 LIKE NULL;
SELECT * FROM like_test WHERE str1 LIKE '_:%';
SELECT * FROM like_test WHERE str1 LIKE 's__';
SELECT * FROM like_test WHERE str1 LIKE '%s';
SELECT * FROM like_test WHERE str1 LIKE 'd%';
SELECT * FROM like_test WHERE str1 LIKE '%%';
SELECT * FROM like_test WHERE str1 IS NULL;
SELECT * FROM like_test WHERE str1 LIKE '%U%';
SELECT * FROM like_test WHERE str1 LIKE '%:%' AND str1 LIKE '%s%';
SELECT * FROM like_test WHERE str1 LIKE '' AND str1 LIKE 's%';
SELECT * FROM like_test WHERE str1 LIKE '% %' AND str1 LIKE '%N%';
SELECT * FROM like_test WHERE str1 LIKE '%L' OR str1 LIKE '%s%';
SELECT * FROM like_test WHERE str1 LIKE '% %' OR str1 LIKE '%N%';
SELECT * FROM (SELECT * FROM like_test WHERE str1 LIKE 'D%') AS a WHERE LENGTH(str1) > 4;
SELECT str1 FROM (SELECT * FROM like_test WHERE str1 LIKE '_:%') AS T WHERE str1 LIKE '%S%';
SELECT * FROM like_test WHERE str1 NOT LIKE '___';
SELECT * FROM like_test WHERE str1 NOT LIKE 'D%';
SELECT * FROM like_test WHERE str1 NOT LIKE '%\_%';
SELECT * FROM like_test WHERE str1 NOT LIKE 'NULL';
SELECT * FROM like_test WHERE str1 NOT LIKE NULL;
SELECT * FROM like_test WHERE str1 NOT LIKE '';
SELECT COUNT(*) FROM like_test WHERE str1 LIKE '%baz%';
SELECT COUNT(*) FROM like_test WHERE str1 NOT LIKE '%baz%';
SELECT COUNT(*) FROM like_test WHERE str1 NOT LIKE '%baz%' OR str1 IS NULL;
SELECT str1, str1 LIKE '%\\' FROM like_test;

-- @bvt:issue#5078
SELECT str1, str1 LIKE '%\\\\' FROM like_test;
-- @bvt:issue

DELETE FROM like_test;
INSERT INTO like_test VALUES('99.9');
INSERT INTO like_test VALUES('88');
INSERT INTO like_test VALUES('89.88');
INSERT INTO like_test VALUES(19.88);
INSERT INTO like_test VALUES(2887.08);

SELECT * FROM like_test WHERE str1 LIKE '9%';
SELECT * FROM like_test WHERE str1 LIKE '%9';
SELECT * FROM like_test WHERE str1 LIKE '%88';
SELECT * FROM like_test WHERE str1 NOT LIKE '%88';

DELETE FROM like_test;
INSERT INTO like_test VALUES('ABC1_23D');
INSERT INTO like_test VALUES('123ABCD\\');
INSERT INTO like_test VALUES('ABCD\\123');
INSERT INTO like_test VALUES(' ');

-- Same Problem with bvt:issue#5078
SELECT * FROM like_test WHERE str1 LIKE '%D\\%';
SELECT * FROM like_test WHERE str1 LIKE '% ';
SELECT * FROM like_test WHERE str1 LIKE '%3_';

DROP TABLE IF EXISTS like_test;
CREATE TABLE like_test(
    str1 VARCHAR(50),
    str2 CHAR(50)
);

INSERT INTO like_test(str1, str2) VALUES('CHINA IS OUR HOMELAND', 'BEIJING IS THE CAPITAL');
INSERT INTO like_test(str1, str2) VALUES('DHINA IS OUR HOMELAN ', 'THIS MAN PUSH HIM');
INSERT INTO like_test(str1, str2) VALUES(' HINA IS OUR HOMELAND%', 'HE LIKE WHORE');
INSERT INTO like_test(str1, str2) VALUES('THE UNITED OF AMERICAN_', 'NORTH AMERICA');
INSERT INTO like_test(str1, str2) VALUES('AB%CDE', 'PDFXLXDOC');
INSERT INTO like_test(str1, str2) VALUES('THE UNITED OF ENGLAND\\', "KINGDOM'");
INSERT INTO like_test(str1, str2) VALUES(' ', '');
INSERT INTO like_test(str1, str2) VALUES('', NULL);
INSERT INTO like_test(str1, str2) VALUES('法的萨菲厄张三三三三三', '考虑理论可看见年年年年年年');

SELECT NULL LIKE 'ABC', 'ABC' LIKE NULL;
SELECT * FROM like_test WHERE str1 LIKE 'C%';
SELECT * FROM like_test WHERE str1 LIKE '% ';
SELECT * FROM like_test WHERE str1 LIKE '%s%';
SELECT * FROM like_test WHERE str1 LIKE '%C%' AND str1 LIKE '%D%';
SELECT * FROM like_test WHERE str2 LIKE '__I%';
SELECT * FROM like_test WHERE str2 LIKE '__ %';
SELECT * FROM like_test WHERE str2 LIKE '';
SELECT * FROM like_test WHERE str2 LIKE NULL;
SELECT * FROM like_test WHERE str1 LIKE '%\\';
SELECT * FROM like_test WHERE str2 LIKE '%\'';
SELECT * FROM like_test WHERE str1 LIKE '%三__';

-- @bvt:issue#5056
SELECT * FROM like_test WHERE str1 LIKE '%\%';
-- @bvt:issue

-- NOT LIKE
SELECT * FROM like_test WHERE str1 NOT LIKE 'D%';
SELECT * FROM like_test WHERE str1 NOT LIKE '%E';
DELETE FROM like_test;
DROP TABLE like_test;

DROP TABLE IF EXISTS chinese_test;
CREATE TABLE chinese_test(
    name VARCHAR(50),
    home VARCHAR(100),
    job VARCHAR(20)
);
INSERT INTO chinese_test VALUES('张三', '河南省信阳市桥东区广平小区', '软件工程师');
INSERT INTO chinese_test VALUES('张飞', '北京市朝阳区天宫苑桥东街道', '教师');
INSERT INTO chinese_test VALUES('里斯', '广东省广州村', '学生教师');
INSERT INTO chinese_test VALUES('刘五', '%%%江苏省南京市金区陵新村', '%%工人');
SELECT name FROM chinese_test WHERE home LIKE '%桥东%';
SELECT * FROM chinese_test WHERE name LIKE '张%';
SELECT * FROM (SELECT * FROM chinese_test WHERE name LIKE '张%') AS a WHERE home LIKE '%广%';
SELECT * FROm (SELECT * FROM chinese_test WHERE job LIKE '%师') AS a WHERE home LIKE '%市%';
SELECT name FROM chinese_test WHERE home LIKE '\%%' AND job LIKE '\%%';
SELECT * FROM chinese_test WHERE home LIKE '%省%区';
DROP TABLE chinese_test;

CREATE TABLE stu(
    id INT PRIMARY KEY,
    name VARCHAR(20),
    class INT,
    sex VARCHAR(2),
    address VARCHAR(50)
);

CREATE TABLE score(
    id INT PRIMARY KEY,
    name VARCHAR(20),
    chinese FLOAT,
    math FLOAT,
    english FLOAT
);

INSERT INTO stu VALUES('1001', 'JACK', 7, 'M', 'US');
INSERT INTO stu VALUES('1002', 'TOM', 7, '', 'UK');
INSERT INTO stu VALUES('1005', 'ROMMY', 8, 'F', 'CHINA');
INSERT INTO stu VALUES('1006', 'RACK', 8, 'M', 'ITALY');
INSERT INTO score VALUES('1001', 'JACK', 89.5, 45.2, 67.5);
INSERT INTO score VALUES('1002', 'TOM', 58.5, 76, 78);
INSERT INTO score VALUES('1005', 'ROMMY', 68, 55, 69);

SELECT * FROM stu INNER JOIN score ON stu.id = score.id WHERE stu.name LIKE '_O%';
SELECT * FROM stu INNER JOIN score ON stu.id = score.id WHERE score.math > 60 AND stu.address LIKE 'U%_';
SELECT * FROM stu RIGHT JOIN score ON stu.id = score.id WHERE score.english > 65 AND stu.sex LIKE '%';
SELECT * FROM stu WHERE address LIKE '%A%' AND name LIKE '%R%';
SELECT * FROM stu INNER JOIN score ON stu.id = score.id WHERE stu.address NOT LIKE 'CH%';
SELECT * FROM stu WHERE name NOT LIKE '%A%';

-- LIKE with SUBQUERY
SELECT
    stu.id, stu.name, stu.address, a.chinese, a.english
FROM
    (SELECT * FROM score WHERE math < 60 AND name LIKE '%M__') AS a,stu
WHERE
    a.id = stu.id AND stu.address LIKE '%H%';

-- LIKE with SUBSTRING and SUBQUERY
SELECT
    stu.name, stu.address
FROM
    (SELECT * FROm score WHERE chinese BETWEEN 60 AND 70) AS a, stu
WHERE
    a.id = stu.id AND address LIKE SUBSTRING('THE CHINA',5);

-- LIKE with JOIN
SELECT
    stu.id, stu.name, stu.address, score.english
FROM
    stu
INNER JOIN
    score
ON
    stu.id = score.id
WHERE stu.sex NOT LIKE '';

DROP TABLE IF EXISTS stu;
DROP TABLE IF EXISTS score;

create table t1(a tinyint, b smallint, c int, d bigint);
insert into t1 values(121, 121, 121, 121);
select * from t1 where (a like '%2%' and b like '%2%' and c like '%2%' and d like '%2%');

drop table t1;
create table t1(a tinyint unsigned, b smallint unsigned, c int unsigned, d bigint unsigned);
insert into t1 values(121, 121, 121, 121);
select * from t1 where (a like '%2%' and b like '%2%' and c like '%2%' and d like '%2%');
