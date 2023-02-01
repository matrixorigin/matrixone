-- @suit
-- @case
-- @test function mid(str,pos,len)
-- @label:bvt

-- constant test
SELECT mid('zhanziziefw',1,2);
SELECT mid('jdkajfkw83q9iwqjfeqw_7832',5,22);
SELECT mid('我是一只自由的鸟',1,3);
SELECT mid('好的fjewka鸭dfj00-=392*&*^$',-8,4);
SELECT mid('好的fjewka鸭dfj00-=392',0,2);
SELECT mid('abchfjwabcABCvshfjdrvoiewjvkmdns mndsabcAbcueiuiwqjemkvwme000',6,40);
SELECT mid('http://www.google.com.cn/',-2,6);
SELECT mid('',1,3);
SELECT mid(NULL,1,2);
SELECT mid('hduw',3,8);
SELECT mid('',-1,3);
SELECT mid('wfroiewjvre',NULL,3);
SELECT mid('vjdiejevfe',1,NULL);
SELECT mid('efjkw',NULL,NULL);
SELECT mid('',2,9);


-- @suite
-- @setup
DROP TABLE IF EXISTS mid_01;
CREATE TABLE mid_01(id int,
					str1 CHAR,
					d1 int,
					d2 tinyint unsigned,
					PRIMARY KEY(id));

INSERT INTO mid_01 VALUES(1, 'h', 3, 12);
INSERT INTO mid_01 VALUES(2, '', -2, 32);
INSERT INTO mid_01 VALUES(3, '2',-2, 2);
INSERT into mid_01 VALUES(4, '*', NULL, 3);
INSERT INTO mid_01 VALUES(5, 'd', 2, NULL);


-- Abnormal insertion
INSERT INTO mid_01 VALUES(6, 'ehiuwjqnelfkw', NULL, 3);
INSERT INTO mid_01 VALUES(7, '2',-2147483649, 0);
INSERT INTO mid_01 VALUES(8, '1', 328, 258);


SELECT mid(str1, 1, 2) FROM mid_01;
SELECT mid(str1, d1, 3) FROM mid_01;
SELECT mid(str1, d1, d2) FROM mid_01;
SELECT * FROM mid_01 WHERE mid(str1,1,1) = 'h';
SELECT * FROM mid_01 WHERE mid(str1,-1,2) IS NULL AND d1 IS NOT NULL;
SELECT d1, d2 FROM mid_01 WHERE str1 = (SELECT str1 FROM mid_01 WHERE mid(str1,-1,1) = '*');
SELECT(SELECT str1 FROM mid_01 WHERE mid(str1,2,3) = NULL),d1,d2 FROM mid_01 WHERE id = 4;


-- string function
SELECT LENGTH(mid(str1, ABS(d1),d2)) FROM mid_01 WHERE str1 = '2';
SELECT empty(mid(str1, d1, d2)) FROM mid_01 WHERE ABS(d2) = 3;
SELECT mid(str1, d1, d2) FROM mid_01;
SELECT mid(str1, d1, 2) FROM mid_01 WHERE ABS(d1) % 2 = 0;
SELECT mid(str1, d1, d2) FROM mid_01 WHERE d1 > 0;


-- @suite
-- @setup
DROP TABLE IF EXISTS mid_02;
CREATE TABLE mid_02(id int, 
					s VARCHAR(50),
					d1 smallint,
					d2 bigint unsigned NOT NULL,
					PRIMARY KEY(id));
					
					
INSERT INTO mid_02 VALUES(1, 'woshishei3829', 3, 12);
INSERT INTO mid_02 VALUES(2, '', -2, 2132);
INSERT INTO mid_02 VALUES(3, ' 356284o 329&***((^%$%^&',-2, 2);
INSERT into mid_02 VALUES(4, NULL, NULL, 3);
INSERT INTO mid_02 VALUES(5, NULL, 2, 4);
INSERT INTO mid_02 VALUES(6, 'ehwqkjf8392__+ ',NULL,6);
INSERT INTO mid_02 values(7, '123', 0, 2);


-- Abnormal insertion
INSERT INTO mid_02 VALUES(8, 'ehiuwjey73y8213092kjfm3e#$%^WHJfne32edwfdewvvcqeveqnelfkw', NULL, 3);
INSERT INTO mid_02 VALUES(9, '2',32769, 0);
INSERT INTO mid_02 VALUES(10, '1', 328, 18446744073709551618);


SELECT mid(s, NULL, NULL) FROM mid_02;
SELECT mid(s, NULL, 2) FROM mid_02;
SELECT mid(s, 1, NULL) FROM mid_02;
SELECT mid(s, 1, 9) FROM mid_02 WHERE mid(s, 1, 2) = 'eh';
SELECT mid(s, d1, d2) FROM mid_02;
SELECT mid(s, d1, -3) FROM mid_02 WHERE d2 = 2;


-- string function
SELECT concat_ws('-',mid(s,2,3),mid(s,1,2)) FROM mid_02 WHERE id BETWEEN 2 AND 3;
SELECT find_in_set(mid(s,1 + 2,9),'woshishei') FROM mid_02 WHERE id = 1;
SELECT empty(mid(s,1,2)) FROM mid_02;
SELECT LENGTH(mid(s, -1, 7281979 % 2)) FROM mid_02;
SELECT lengthutf8(mid(s, -1, 3)) FROM mid_02;
SELECT LTRIM(mid(s, 1, 16 - 11)) FROM mid_02;
SELECT RTRIM(mid(s, -3, 2)) FROM mid_02 WHERE id = 6;
SELECT LPAD(mid(s, 1, 2),20,'*') FROM mid_02;
SELECT RPAD(mid(s, -8, 4), 5, '-') FROM mid_02 WHERE ABS(d1) = 0;
SELECT startswith(mid(s, 1, 6), 'ehwq') FROM mid_02 WHERE d2 = NULL;
SELECT endswith(mid(s,-1,1),' ') FROM mid_02 WHERE id = 6;
SELECT substring(mid(s, 3, 19),3, 10) FROM mid_02 WHERE id + 1 = 4;
SELECT REVERSE(mid(s, -1, 2)) FROM mid_02;
SELECT hex(mid(s, 1, 2)) FROM mid_02 WHERE id = 7;


-- subqueries
SELECT * FROM mid_02 WHERE s = (SELECT s FROM mid_02 WHERE mid(s,1,2) = 'wo');
SELECT(SELECT s FROM mid_02 WHERE mid(s,1,3) = 'ehw'),d1,d2 FROM mid_02 WHERE id = 6;
SELECT * FROM mid_02 WHERE s = (SELECT s FROM mid_02 WHERE mid(s,1,10) = NULL);


-- @suite
-- @setup
DROP TABLE IF EXISTS mid_03;
DROP TABLE IF EXISTS mid_04;
CREATE TABLE mid_03(
	id int,
    d1 tinyint unsigned,
    str1 VARCHAR(50),
    primary key (id));
	
CREATE TABLE mid_04(
    id int,
    d2 bigint,
	str1 mediumtext NOT NULL,
    primary key (id));


INSERT INTO mid_03 VALUES(1, 255, 'zheshimeihaodeyitian,这是美好的一天');
INSERT INTO mid_03 VALUES(2, 10, '明天更美好ehwqknjcw*^*qk67329&&*');
INSERT INTO mid_03 VALUES(3, NULL, 'ewgu278wd-+ABNJDSK');
INSERT INTO mid_03 VALUES(4, 1, NULL);

INSERT INTO mid_04 VALUES(1, 0, '盼望着,盼望着,东风来了,春天的脚步近了。 一切都像刚睡醒的样子,欣欣然张开了眼。山朗润起来了,水涨 起来了,太阳的脸红起来了。 小草偷偷地从土里钻出来，Choose to Be Alone on Purpose Here we are, all by ourselves, all 22 million of us by recent count, alone in our rooms');
INSERT INTO mid_04 VALUES(2, -34, 'zheshimeihaodeyitian,这是美好的一天');
INSERT INTO mid_04 VALUES(3, 35267192, 'ewgu278wd-+ABNJDSK');
INSERT INTO mid_04 VALUES(4, NULL, 'hey32983..........,,');


-- join 
SELECT mid_03.id AS id_3,mid_04.id AS id_4 FROM mid_03,mid_04 WHERE mid(mid_03.str1,1,4) = mid(mid_04.str1,1,4);
SELECT mid_03.str1 AS str1_3,mid_04.str1 FROM mid_03,mid_04 WHERE mid(mid_03.str1,2,1) = mid(mid_04.str1,-1,1);
SELECT mid(mid_03.str1, -10, 5) FROM mid_03,mid_04 WHERE mid_03.str1 = mid_04.str1;
SELECT * FROM mid_03 WHERE str1 = (SELECT str1 FROM mid_04 WHERE mid(str1, 1, 19) = 'ewgu278wd-+ABNJDSK');
SELECT mid(mid_03.str1, -10, 5)AS tmp, mid_04.str1 AS temp FROM mid_03 join mid_04 ON mid_03.str1 = mid_04.str1;
SELECT mid_03.id AS id_3,mid_04.id AS id_4 FROM mid_03 left join mid_04 ON mid(mid_03.str1,1,4) = mid(mid_04.str1,1,4);
SELECT mid_03.d1 AS d1_3,mid_04.d2 AS d2_4 FROM mid_03 right join mid_04 ON mid(mid_03.str1,1,4) = mid(mid_04.str1,1,4);
