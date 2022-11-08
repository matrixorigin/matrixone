-- @suit

-- @case
-- @desc:test for insert data with function
-- @label:bvt
CREATE TABLE char_test(
	str1 CHAR(5),
	str2 VARCHAR(5)
);
INSERT INTO char_test(str1, str2) VALUES('ABCED', 'MESSI');

-- CONCAT_WS()
INSERT INTO char_test(str1) VALUES(CONCAT_WS(' ', ' I have ', '  a dream'));
INSERT INTO char_test(str1) VALUES(CONCAT_WS(',', ',I have ', ',a dream'));
INSERT INTO char_test(str1) VALUES(CONCAT_WS('x', ' I have x', 'x a dream'));
INSERT INTO char_test(str1) SELECT CONCAT_WS('.', 'MESSI is a ', '. aloha bumda.');
DELETE FROM char_test;

--FIND_IN_SET()
INSERT INTO char_test(str2) VALUES(FIND_IN_SET('a', 'b,d,c,a'));
INSERT INTO char_test(str2) VALUES(FIND_IN_SET('a', 'if i were a boy'));
INSERT INTO char_test(str2) VALUES(FIND_IN_SET('A', CONCAT_WS(',', 'The english Union',', have a king.')));
SELECT * FROM char_test;
DELETE FROM char_test;

-- OCT()
INSERT INTO char_test(str2) VALUES(OCT(FIND_IN_SET('a', 'b,c,d,e,f,g,h,a')));
INSERT INTO char_test(str2) VALUES(OCT(NULL));
INSERT INTO char_test(str2) VALUES(OCT(LENGTH('JFKLD;AJKFLD;AJFKDL;ASJFKDLSA;FJDKSAL;FJDKSAL;FJDKA;')));
SELECT * FROM char_test;
DELETE FROM char_test;

-- EMPTY()
INSERT INTO char_test(str1) VALUES(SPACE(100));
INSERT INTO char_test(str1) VALUES('RONALDOSHOOTGOAL');
INSERT INTO char_test(str2) VALUES(EMPTY(""));
INSERT INTO char_test(str2) VALUES(EMPTY(null));
INSERT INTO char_test(str2) VALUES(EMPTY(CONCAT_WS(' ', 'ABCDE','JKFL;JDK','FDAFD')));
INSERT INTO char_test(str2) VALUES(EMPTY(SPACE(100)));
INSERT INTO char_test(str2) VALUES(EMPTY(OCT(4564123156)));

-- @bvt:issue#4963
--SELECT str1, STARTSWITH(str1,'') FROM char_test;
-- @bvt:issue

-- @bvt:issue#4966
--SELECT str1, ENDSWITH(str1,'') FROM char_test;
-- @bvt:issue

-- 日期时间类型
DROP TABLE IF EXISTS date_test;
CREATE TABLE date_test(
	d2 DATE,
	d3 DATETIME,
	d4 TIMESTAMP,
	d5 BIGINT
);
INSERT INTO date_test(d2,d3) VALUES('2022-08-07', '2018-09-13 13:45:13');
INSERT INTO date_test(d2,d3) VALUES('2015-11-07', '2013-09-14 13:45:13');
INSERT INTO date_test(d2,d3) VALUES('2013-08-07', '2006-05-23 13:23:13');
INSERT INTO date_test(d2,d3) VALUES('2011-08-07', '2018-07-08 23:59:59');
INSERT INTO date_test(d5) SELECT UNIX_TIMESTAMP("2021-02-29");
INSERT INTO date_test(d3) VALUES(DATE_ADD('2008-13-26 23:59:59', NULL));
SELECT * FROM date_test;
DELETE FROM date_test;





-- 数字类型
CREATE TABLE math_test(
	tiny TINYINT,
	small SMALLINT,
	int_test INT,
	big BIGINT,
	tiny_un TINYINT UNSIGNED,
	small_un SMALLINT UNSIGNED,
	int_un INT UNSIGNED,
	big_un BIGINT UNSIGNED,
	float_32 FLOAT,
	float_64 DOUBLE
);

INSERT INTO math_test(tiny,small,int_test,big) VALUES(32, 2432, 54354, 543324324);
INSERT INTO math_test(tiny_un, small_un, int_un) VALUES(127, 32768, 2147483648);
SELECT * FROM math_test;

-- @bvt:issue#4952
--INSERT INTO math_test(tiny_un, small_un, int_un) VALUES(-128, -32768, -2147483648);
-- @bvt:issue

-- 小数位数
DROP TABLE IF EXISTS test1;
CREATE TABLE test1(
	num1 FLOAT(6,2),
	num2 DOUBLE(6,2),
	num3 DECIMAL(6,2)
);
INSERT INTO test1(num1, num2, num3) VALUES(12.21, 43.43, 999.899);
INSERT INTO test1 VALUES(3.1415, 3.1415, 3.1415);
SELECT * FROM test1;


DROP TABLE char_test;
DROP TABLE date_test;
DROP TABLE math_test;
DROP TABLE test1;