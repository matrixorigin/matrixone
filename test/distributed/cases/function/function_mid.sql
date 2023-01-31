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
