-- @suit
-- @case
-- @desc:test for regular expression function
-- @label:bvt

-- REGEXP
SELECT 'Michael!' REGEXP '.*';
SELECT 'new*\n*line' REGEXP 'new\\*.\\*line';
SELECT 'a' REGEXP '^[a-d]';

-- REGEXP with table's column
DROP TABLE IF EXISTS t;
CREATE TABLE t(
	id INT,
	str1 VARCHAR(20) NOT NULL,
	PRIMARY KEY (id)
);
INSERT INTO t VALUES (1,'jesason'), (2,'jackikky'), (3,'roesaonay'),(4,'hiserber');
SELECT str1 REGEXP '.*' FROM t;
SELECT str1 REGEXP 'j.*' FROM t;
SELECT str1 REGEXP 'NULL' FROM t;
SELECT str1 REGEXP NULL FROM t;
-- This SQL stmt result is different from MySQL, Cause:Upper and Lower case
SELECT str1 REGEXP '^[A-Z]' FROM t;
SELECT str1 REGEXP '^[a-z]' FROM t;

DROP TABLE IF EXISTS t;
CREATE TABLE t(
	id INT,
	str1 VARCHAR(20),
	PRIMARY KEY (id)
);
INSERT INTO t VALUES (1,'chinaiscaptia'), (2,'spanishisasee'), (3,'unitedstates'),(4,'kindomofeu'),(5,NULL),(6,'NULL');
SELECT str1 REGEXP 'LL$' FROM t;
SELECT str1 REGEXP 'n.a' FROM t;
SELECT str1 REGEXP 'c*a' FROM t;
SELECT str1 REGEXP 'sp+' FROM t;

DROP TABLE IF EXISTS t;
CREATE TABLE t(
	id INT,
	str1 VARCHAR(20),
	PRIMARY KEY (id)
);
INSERT INTO t VALUES (1,'+aacbd'), (2,'aacbd+'), (3,'aac*'),(4,'^aac');
SELECT str1 REGEXP '^\\^' FROM t;
SELECT str1 REGEXP '\\+$' FROM t;
SELECT str1 REGEXP '\\*$' FROM t;
SELECT str1 REGEXP '^\\+' FROM t;

-- REGEXP_INSTR()
SELECT REGEXP_INSTR('dog cat dog', 'dog');
SELECT REGEXP_INSTR('dog cat dog', 'dog', 2);
SELECT REGEXP_INSTR('aa aaa aaaa', 'a{2}');
SELECT REGEXP_INSTR('aa aaa aaaa', 'a{4}');
DROP TABLE IF EXISTS t;
CREATE TABLE t(
	id INT,
	str1 VARCHAR(20),
	str2 CHAR(20),
	PRIMARY KEY (id)
);
INSERT INTO t VALUES (1,'aa bb cc aa','a b c a'), (2,'x y z x','a e i o u'), (3,'cat do cat Le','les go le'), (4,NULL,'ee f ee');
SELECT REGEXP_INSTR(str1,'a{2}') FROM t;
SELECT REGEXP_INSTR(str1,'a{2}',2) FROM t;
SELECT REGEXP_INSTR(str1,'le'), REGEXP_INSTR(str2,'le',2) FROM t;
SELECT REGEXP_INSTR(str1,'NULL'), REGEXP_INSTR(str1,NULL) FROM t;
SELECT REGEXP_INSTR(str1,'NULL'), REGEXP_INSTR(NULL,NULL) FROM t;
SELECT REGEXP_INSTR(str1,'Le'), REGEXP_INSTR(str1,'le') FROM t;

DROP TABLE IF EXISTS t;
CREATE TABLE t(
	d1 DATE,
	d2 DATETIME,
	d3 TIMESTAMP
);
INSERT INTO t VALUES ('2020-10-01', '2020-10-01 10:00:01', '2020-10-01 11:11:11.127653');
INSERT INTO t VALUES ('2120-12-12', '2220-12-12 12:12:12', '2120-12-12 12:12:12.765321');
INSERT INTO t VALUES ('2218-10-01', '2118-10-01 10:00:01', '2218-10-01 09:09:09.653828');
SELECT REGEXP_INSTR(d1,'20'), REGEXP_INSTR(d2,'19'), REGEXP_INSTR(d3,'18') FROM t;
SELECT REGEXP_INSTR(d1,'2'+'0'), REGEXP_INSTR(d2,'2'+FIND_IN_SET('a','a,b,c')), REGEXP_INSTR(d3,'22') FROM t;
SELECT REGEXP_INSTR(d1,20), REGEXP_INSTR(d2,21), REGEXP_INSTR(d3,22) FROM t;
SELECT REGEXP_INSTR(d1,19), REGEXP_INSTR(d2,ABS(-21)), REGEXP_INSTR(d3,SIN(10)) FROM t;

-- REGEXP_LIKE(), Its optional parameters are: c, i, m, n, u
SELECT REGEXP_LIKE('CamelCase', 'CAMELCASE');
SELECT REGEXP_LIKE('Michael!', '.*');
SELECT REGEXP_LIKE('new*\n*line', 'new\\*.\\*line');
SELECT REGEXP_LIKE('a', '^[a-d]');
SELECT REGEXP_LIKE('abc', 'ABC');
-- string's beginning
SELECT REGEXP_LIKE('fo\nfo', '^fo$');
SELECT REGEXP_LIKE('fofo', '^fo');
-- string's tail
SELECT REGEXP_LIKE('fo\no', '^fo\no$');
SELECT REGEXP_LIKE('fo\no', '^fo$');
-- any char sequence
SELECT REGEXP_LIKE('fofo', '^f.*$');
SELECT REGEXP_LIKE('fo\r\nfo', '^f.*$');
SELECT REGEXP_LIKE('fo\r\nfo', '(?m)^f.*$');
-- 0 or any char sequence, a* a{0,}
SELECT REGEXP_LIKE('Ban', '^Ba*n');
SELECT REGEXP_LIKE('Baaan', '^Ba*n');
SELECT REGEXP_LIKE('Bn', '^Ba*n');
-- 1 or any char sequence, a+ a{1,}
SELECT REGEXP_LIKE('Ban', '^Ba+n');
SELECT REGEXP_LIKE('Bn', '^Ba+n');
-- 0 or any character, a? {0,1}
SELECT REGEXP_LIKE('Bn', '^Ba?n');
SELECT REGEXP_LIKE('Ban', '^Ba?n');
SELECT REGEXP_LIKE('Baan', '^Ba?n');
-- exchange or matching, such as ab|cde
SELECT REGEXP_LIKE('pi', 'pi|apa');
SELECT REGEXP_LIKE('axe', 'pi|apa');
SELECT REGEXP_LIKE('apa', 'pi|apa');
SELECT REGEXP_LIKE('apa', '^(pi|apa)$');
SELECT REGEXP_LIKE('pi', '^(pi|apa)$');
SELECT REGEXP_LIKE('pix', '^(pi|apa)$');
-- match sequence 0 or any instance
SELECT REGEXP_LIKE('pi', '^(pi)*$');
SELECT REGEXP_LIKE('pip', '^(pi)*$');
SELECT REGEXP_LIKE('pipi', '^(pi)*$');
-- with {m,n}
SELECT REGEXP_LIKE('abcde', 'a[bcd]{2}e');
SELECT REGEXP_LIKE('abcde', 'a[bcd]{3}e');
SELECT REGEXP_LIKE('abcde', 'a[bcd]{1,10}e');
-- with range matching
SELECT REGEXP_LIKE('aXbc', '[a-dXYZ]');
SELECT REGEXP_LIKE('aXbc', '^[a-dXYZ]$');
SELECT REGEXP_LIKE('aXbc', '^[a-dXYZ]+$');
SELECT REGEXP_LIKE('aXbc', '^[^a-dXYZ]+$');
SELECT REGEXP_LIKE('gheis', '^[^a-dXZ]+$');
SELECT REGEXP_LIKE('gheisa', '^[^a-dXYZ]+$');
SELECT REGEXP_LIKE('justalnums', '[[:alnum:]]+');
SELECT REGEXP_LIKE('!!', '[[:alnum:]]+');
SELECT REGEXP_LIKE('1+2', '1+2');
SELECT REGEXP_LIKE('1+2', '1\+2');
SELECT REGEXP_LIKE('1+2', '1\\+2');
-- SELECT REGEXP_LIKE('(', '(');
-- SELECT REGEXP_LIKE('[', '[');
SELECT REGEXP_LIKE('(', '\\(');
SELECT REGEXP_LIKE(']', ']');
SELECT REGEXP_LIKE('[', '\\[');

DROP TABLE IF EXISTS t;
CREATE TABLE t(
	str1 VARCHAR(20),
	str2 CHAR(20)
);
INSERT INTO t VALUES ('aBcM**X', '!!~&*^YYsD'), ('2*3', '3/3/0'), ('Xadec', '--8Kiys');
SELECT REGEXP_LIKE(str1,'[a-dXY]') FROM t;
SELECT REGEXP_LIKE(str1,'[[sXd]]+'), REGEXP_LIKE(str2,'[[a-z*A-Z]]+') FROM t;

-- @bvt:issue#5537
SELECT REGEXP_LIKE('abc', 'ABC', 'c');
SELECT REGEXP_LIKE('fo\r\nfo', '^f.*$', 'm');
-- @bvt:issue

-- REGEXP_REPLACE()
SELECT REGEXP_REPLACE('a b c', 'b', 'X');
SELECT REGEXP_REPLACE('abc def ghi', '[a-z]+', 'X', 1, 3);

DROP TABLE IF EXISTS t;
CREATE TABLE t(
	str1 VARCHAR(20),
	str2 CHAR(20)
);
INSERT INTO t VALUES ('W * P', 'W + Z - O'), ('have has having', 'do does doing');
INSERT INTO t VALUES ('XV*XZ', 'PP-ZZ-DXA'), ('aa bbb cc ddd', 'k ii lll oooo');
SELECT * FROM t;
SELECT REGEXP_REPLACE(str1, 'W', 'a') FROM t;
SELECT REGEXP_REPLACE(str2, 'do', '+') FROM t;
SELECT REGEXP_REPLACE(str1, 'd'+'0', NULL) FROM t;
SELECT REGEXP_REPLACE(str1, NULL, NULL) FROM t;

-- @bvt:issue#5547
SELECT REGEXP_REPLACE(str1, '*', 'i'), REGEXP_REPLACE(str2,'hav','hiv') FROM t;
-- @bvt:issue

-- REGEXP_SUBSTR()
SELECT REGEXP_SUBSTR('abc def ghi', '[a-z]+');
SELECT REGEXP_SUBSTR('abc def ghi', '[a-z]+', 1, 3);
SELECT REGEXP_SUBSTR('jfidaj***fkd', '.*', 2);

DROP TABLE IF EXISTS t;
CREATE TABLE t(
	str1 VARCHAR(20),
	str2 CHAR(20)
);
INSERT INTO t VALUES ('fjidajljil', 'iopuicuoiz'), ('^&*^*%*&', '=-=00-JDjfds');
INSERT INTO t VALUES ('I hanva fkldjia', '  fds  jkl  '), (NULL, 'NULL');
SELECT REGEXP_SUBSTR(str1, '[A-Z+]'), REGEXP_SUBSTR(str2, '[a-z]-') FROM t;
SELECT REGEXP_SUBSTR(str1, '[A-Z*]'), REGEXP_SUBSTR(str2, '.*') FROM t;
SELECT REGEXP_SUBSTR(str1, '[A-Z]'), REGEXP_SUBSTR(str2, '^[0-9a-z]') FROM t;
SELECT REGEXP_SUBSTR(str1, ' ') FROM t;
SELECT REGEXP_SUBSTR(str1, ' *') FROM t;
SELECT REGEXP_SUBSTR(NULL, ' *') FROM t;
SELECT REGEXP_SUBSTR(NULL, NULL) FROM t;
