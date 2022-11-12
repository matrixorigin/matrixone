-- @suit
-- @case
-- @desc:test for Built-in Function LEFT()
-- @label:bvt

SELECT LEFT('ABCDEFG', 2);
SELECT LEFT('ABCDEFG', 0);
SELECT LEFT('ABCDEFG', -1);
SELECT LEFT('ABCDEFG', 2+1);
SELECT LEFT('ABCDEFG', ABS(-2));
SELECT LEFT('ABCDEFG', COS(0));
SELECT LEFT('ABCDEFG', LENGTH('KING'));
SELECT LEFT('ABCDEFG', NULL);
SELECT LEFT(NULL, 1);
SELECT LEFT(NULL, 0);
SELECT LEFT(NULL, -1);

-- String column
DROP TABLE IF EXISTS t;
CREATE TABLE t(
    id INT,
    str1 VARCHAR(20),
    str2 CHAR(20),
    PRIMARY KEY (id)
);
INSERT INTO t VALUES (123456, 'anike1001@gmail.com', 'googood'), (123457, 'nitin5438@yahoo.com','hainghing');
SELECT LEFT(id, 3), LEFT(str1, 5), LEFT(str2, 5) FROM t;
SELECT LEFT(id, '1'), LEFT(str1, '1'), LEFT(str2, '1') FROM t;
SELECT LEFT(id, LENGTH(str1)/1-1), LEFT(str1, 1) FROM t;
SELECT LEFT(id, FIND_IN_SET('b','a,b,c,d')), LEFT(str2, EMPTY(str1)) FROM t;
SELECT LEFT(id, '-1'), LEFT(str1, '0') FROM t;
-- @bvt:issue#5510
SELECT LEFT(id, '1'+2), LEFT(str1, '1'+'1'), LEFT(str2, '1'+'0') FROM t;
-- @bvt:issue

-- DATE column
DROP TABLE IF EXISTS t;
CREATE TABLE t(
    id INT,
    d1 DATE,
    d2 DATETIME,
    d3 TIMESTAMP,
    PRIMARY KEY (id)
);
INSERT INTO t VALUES (1, '2020-01-01', '2020-01-01 12:12:12', '2020-02-02 06:06:06.163');
INSERT INTO t VALUES (2, '2021-11-11', '2021-01-11 23:23:23', '2021-12-12 16:16:16.843');
SELECT LEFT(d1, MONTH(d1)), LEFT(d2, DAY(d2)) FROM t;
SELECT LEFT(d2, LENGTH(d2)) FROM t;
SELECT LEFT(d1, SIN(0)+MONTH(d1)) FROM t;
SELECT LEFT(d1, COS(10)+MONTH(d1)) FROM t;

-- Decimal column
-- @bvt:issue#5511
CREATE TABLE t(
    d INT,
    d1 BIGINT,
    d2 FLOAT,
    d3 DOUBLE,
    PRIMARY KEY (id)
);
-- @bvt:issue

DROP TABLE IF EXISTS t;
CREATE TABLE t(
    d INT,
    d1 BIGINT,
    d2 FLOAT,
    d3 DOUBLE,
    PRIMARY KEY (d)
);
INSERT INTO t VALUES (1,101210131014,50565056.5566,80898089.8899);
INSERT INTO t VALUES (2,46863515648464,9876453.3156153,6486454631564.156153489);
-- @bvt:issue#5513
SELECT LEFT(d1,3), LEFT(d2,4), LEFT(d3,5) FROM t;
SELECT LEFT(d1,LENGTH(d1)), LEFT(d2,LENGTH(d2)) FROM t;
-- @bvt:issue

-- JOIN
DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t1;
CREATE TABLE t(
    d INT,
    d1 VARCHAR(20),
    d2 BIGINT,
    PRIMARY KEY (d)
);
CREATE TABLE t1(
    d INT,
    d1 CHAR(20),
    d2 DATE,
    PRIMARY KEY (d)
);
INSERT INTO t VALUES (1,'lijklnfdsalj',19290988), (2,'xlziblkfdi',1949100132);
INSERT INTO t VALUES (3,'ixioklakmaria',69456486), (4,'brzilaiusd',6448781575);
INSERT INTO t1 VALUES (1,'usaisagoodnat','1970-01-02'),(2,'chanialfakbjap','1971-11-12');
INSERT INTO t1 VALUES (3,'indiaisashit','1972-09-09'),(4,'xingoporelka','1973-12-07');
SELECT t.d, LEFT(t.d1,FIND_IN_SET('d','a,b,c,d')) FROM t;
SELECT t.d, LEFT(t.d2, FIND_IN_SET('d','a,b,c,d')), LEFT(t1.d1, ABS(-3)+1) FROM t,t1 WHERE t.d = t1.d;
SELECT t.d, LEFT(t1.d2, NULL) FROM t JOIN t1 ON t.d = t1.d;
--SELECT t.d, LEFT(t1.d2, 'NULL') FROM t RIGHT JOIN t1 ON t.d = t1.d;
--SELECT t.d, LEFT(t1.d2, 2>1) FROM t JOIN t1 ON t.d = t1.d;
SELECT t.d, LEFT(t1.d2, BIN(1)) FROM t RIGHT JOIN t1 ON t.d = t1.d;
SELECT t.d, LEFT(t1.d2, BIN(2)) FROM t RIGHT JOIN t1 ON t.d = t1.d;
DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t1;
