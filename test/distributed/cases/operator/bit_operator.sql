-- @suit

-- @case
-- @desc:test bit operators such as &, |, ^, ~, <<, >>
-- @label:bvt

SELECT 1 ^ 1, 9 &4& 2, 1 ^ 0;
SELECT 29 & 15;
SELECT ~0, 64 << 2, '40' << 2;
SELECT 1 << 2;
SELECT 4 >> 2;
SELECT 100 << ABS(-3);
SELECT BIN(~1);
SELECT 3 & ~8;

DROP TABLE IF EXISTS t1;
CREATE TABLE t1(
    tiny TINYINT NOT NULL,
    small SMALLINT NOT NULL,
    int_test INT NOT NULL,
    big BIGINT NOT NULL
);
INSERT INTO t1(tiny, small, int_test, big) VALUES(0, SIN(1), 1, -0);
INSERT INTO t1() VALUES('2', 32767, '1', 9223372036854775807);
SELECT tiny & small FROM t1;
SELECT tiny << small FROM t1;
SELECT tiny >> big FROM t1;

DROP TABLE IF EXISTS t1;
CREATE TABLE t1(
    tiny TINYINT UNSIGNED NOT NULL,
    small SMALLINT UNSIGNED NOT NULL,
    int_test INT UNSIGNED NOT NULL,
    big BIGINT UNSIGNED NOT NULL
);
INSERT INTO t1(tiny, small, int_test, big) VALUES(0, SIN(1)-COS(0), 1, 9223372036854775807);
INSERT INTO t1() VALUES(1, 927, LENGTH('abcd'), 90);
SELECT big >> COS(0) FROM t1;
SELECT tiny & int_test | small ^ big FROM t1;
SELECT tiny << int_test | small >> big FROM t1;
SELECT tiny << LENGTH('abcdefghijklmnopqrst') FROM t1 ORDER BY tiny;

DROP TABLE IF EXISTS t1;
CREATE TABLE t1(
    int_test INT UNSIGNED NOT NULL,
    float_32 FLOAT NOT NULL,
    float_64 DOUBLE NOT NULL
);
INSERT INTO t1() VALUES(1, 0.0, 123.146484666486456);
INSERT INTO t1() VALUES('99', 0.000001, 1.0);
SELECT int_test & float_32 FROM t1;
SELECT float_32 | float_64 FROM t1;
SELECT float_32 ^ float_64 + 11 FROM t1;
SELECT float_32 >> int_test FROM t1;
--SELECT float_32 >> int_test / 0 FROM t1;
--SELECT float_32 / 0 >> int_test FROM t1;
--SELECT float_32 / (COS(0) - 1.0) >> int_test FROM t1;

DROP TABLE IF EXISTS t1;
CREATE TABLE t1(
    int_test INT UNSIGNED NOT NULL,
    float_32 FLOAT NOT NULL,
    d1 DECIMAL NOT NULL
);
INSERT INTO t1() VALUES(1, 0.000001, 1.00000000000000000000000001);
INSERT INTO t1() VALUES(YEAR('2022-02-02'), SIN(1), LENGTH('abcdefghijk') - MONTH('2022-09-09'));
SELECT ~float_32 & float_32 FROM t1;

DROP TABLE IF EXISTS t1;
CREATE TABLE t1(
    str1 CHAR(10),
    str2 VARCHAR(10)
);
INSERT INTO t1() VALUES('abc', '123');
INSERT INTO t1() VALUES(NULL, 'dc');
SELECT LENGTH(str1) | LENGTH(str2) FROM t1;
SELECT STARTSWITH(str1, 'a') | LENGTH(str2) FROM t1;
DELETE FROM t1;
INSERT INTO t1() VALUES('123', 23), (LENGTH('abc'), 0), ('0', NULL);
SELECT str1 << '1' FROM t1;
SELECT str1 & '0' FROM t1;
--SELECT str1 & '0.0' FROM t1;
SELECT str1 & '11111' | '000101' & BIN(12) FROM t1;
SELECT str1 & '5555' | SPACE(100)+'1' & BIN(16) FROM t1;
SELECT str1 ^ '000000000000000'+'1'+'000000000000000' & '000000000000000'+'1'+'000000000000000' FROM t1;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1(
    str1 VARCHAR(10),
    PRIMARY KEY (str1)
);
CREATE TABLE t2(
    n1 INT,
    PRIMARY KEY (n1)
);
INSERT INTO t1() VALUES('101'),('-1'),(TRUE),(FALSE);
INSERT INTO t2() VALUES(101),(-1),(FALSE),(TRUE);
SELECT str1 & n1 FROM t1,t2 LIMIT 4;
SELECT str1 & ABS(-SIN(7)) FROM t1;
SELECT n1 & str1 & n1 & '111' & n1 & '1001' FROM t1,t2;

SELECT 1 << n1 FROM t2;
SELECT n1 << n1 >> n1 FROM t2;
SELECT n1 ^ str1 | n1 & str1 >> n1 << str1 FROM t1,t2;
SELECT n1 ^ 1 | n1 & '111' >> n1 << '1001' FROM t1,t2;

SELECT '0150' | str1 | n1 | '000111' | n1 | '101010' FROM t1,t2;

-- multi table insert or update with bit operator
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1(
    id INT,
    str1 VARCHAR(10),
    PRIMARY KEY (id)
);
CREATE TABLE t2(
    id INT,
    n1 INT,
    PRIMARY KEY (id)
);
INSERT INTO t1() VALUES(1, '1'), (2, 'red'), (3, 'United'), (4, FALSE);
INSERT INTO t2() VALUES(1, 101), (2, 01010), (4, -1);
SELECT str1 | n1 FROM t1, t2 WHERE t1.id = t2.id AND t1.id = 1;
UPDATE t1 JOIN t2 ON t1.id = t2.id SET str1 = n1 << 2;
UPDATE t1,t2 SET str1 = 2 >> str1, n1 = 3 >> 3 WHERE t1.id = 1;
INSERT INTO t1() VALUES(2 << 4, 'shift'), (3 & 0, 'bit');
INSERT INTO t1 SELECT 2 << 8, 'UK';
SELECT * FROM t1;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1(
    id INT,
    class VARCHAR(10),
    name VARCHAR(10),
    PRIMARY KEY (id)
);
CREATE TABLE t2(
    id INT,
    grade VARCHAR(10),
    score FLOAT,
    PRIMARY KEY (id)
);
INSERT INTO t1() VALUES(1,'c1','nion'), (2,'c2','unitd'), (3,'c1','jake'), (4,'c2','hadd'), (5,'c3','laik');
INSERT INTO t2() VALUES(1,'A',70.1), (2,'B',59.3), (3,'C',81.2), (4,'B',48.3), (5,'C',99.4);
SELECT id,MAX(score),grade FROM t2 GROUP BY id,grade HAVING id > (2 << 2);
SELECT t1.id, t1.name, t2.grade FROM t1, t2 WHERE t1.id = t2.id AND t1.id < (2 << 2);
SELECT score FROM t2 WHERE t2.score BETWEEN 2 << 5 AND 2 << 8;

DROP TABLE IF EXISTS t1;
CREATE TABLE t1(
    id INT,
    str1 VARCHAR(10),
    PRIMARY KEY (id)
);
INSERT INTO t1() VALUES(1,'c1'), (2,'11'), (3,'cd'), (4,'df');
SELECT id | BIN(100), id & HEX(100) FROM t1;
INSERT INTO t1(id) VALUES(0 & BIN(4));
INSERT INTO t1(id) VALUES(-1 & HEX(8));
SELECT id << HEX(88), id >> BIN(88) FROM t1;
SELECT HEX(88) & BIN(88), BIN(id) | BIN(88) FROM t1;

DROP TABLE IF EXISTS t1;
CREATE TABLE t1(
    str1 VARCHAR(10),
    PRIMARY KEY (str1)
);
INSERT INTO t1() VALUES ('111'), ('222'), ('0'), ('333');
SELECT HEX(str1) & BIN(88), BIN(str1) | HEX(88) FROM t1;
SELECT HEX(str1) & BIN(88) ^ BIN(str1) | HEX(100) FROM t1;
SELECT HEX(str1) >> BIN(88) << BIN(str1) >> HEX(100) FROM t1;
DROP TABLE t1;
select binary(3) & binary(4);
select binary(3) | binary(4);
