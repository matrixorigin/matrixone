-- @suit

-- @case
-- @desc:test for IN_RANGE operator
-- @label:bvt

-- Test basic IN_RANGE with inclusive bounds (flag=0)
SELECT IN_RANGE(2, 1, 3, 0), IN_RANGE(2, 3, 1, 0);
SELECT IN_RANGE(1, 2, 3, 0);
SELECT IN_RANGE('b', 'a', 'c', 0);
SELECT IN_RANGE(2, 2, '3', 0);

DROP TABLE IF EXISTS in_range_test;
CREATE TABLE in_range_test(
    int_test_max INT,
    int_test_min INT,
    str1 VARCHAR(20),
    str2 CHAR(20),
    float_32 FLOAT,
    float_64 DOUBLE
);

INSERT INTO in_range_test() VALUES(2147483647, -2147483648, 'A', 'Z', 0.0000000000000000000000001, -1.1);
INSERT INTO in_range_test() VALUES(2147483647, -2147483648, 'a', 'z', 0.002, 0.00003);
INSERT INTO in_range_test(float_32, float_64) VALUES(2147483648, 16542147483647);

-- MO is strictly case sensitive, Ex : 'A' is not seem to 'a';
SELECT float_32, float_64 FROM in_range_test WHERE IN_RANGE(float_64, -1.1, 0.00003, 0);
SELECT float_32, float_64 FROM in_range_test WHERE IN_RANGE(-0.0, -1.1, 0.00003, 0);

-- Test exclusive left bound (flag=1)
SELECT * FROM in_range_test WHERE IN_RANGE(NULL, 'A', 'z', 1);
SELECT * FROM in_range_test WHERE IN_RANGE('#', 'A', 'z', 1);
SELECT * FROM in_range_test WHERE IN_RANGE(float_32, -2147483648, 2147483647, 1);
SELECT * FROM in_range_test WHERE IN_RANGE(float_64, -2147483648, 2147483647, 1);

-- Test exclusive right bound (flag=2)
SELECT * FROM in_range_test WHERE IN_RANGE(NULL, 'A', 'z', 2);
SELECT * FROM in_range_test WHERE IN_RANGE('#', 'A', 'z', 2);
SELECT * FROM in_range_test WHERE IN_RANGE(float_32, -2147483648, 2147483647, 2);
SELECT * FROM in_range_test WHERE IN_RANGE(float_64, -2147483648, 2147483647, 2);

-- Test exclusive both bounds (flag=3)
SELECT * FROM in_range_test WHERE IN_RANGE(NULL, 'A', 'z', 3);
SELECT * FROM in_range_test WHERE IN_RANGE('#', 'A', 'z', 3);
SELECT * FROM in_range_test WHERE IN_RANGE(float_32, -2147483648, 2147483647, 3);
SELECT * FROM in_range_test WHERE IN_RANGE(float_64, -2147483648, 2147483647, 3);

DELETE FROM in_range_test;
DROP TABLE IF EXISTS in_range_test;

CREATE TABLE in_range_test(
    d1 DATE,
    d2 DATETIME,
    d3 TIMESTAMP
);

INSERT INTO in_range_test() VALUES('2022-06-06','2022-06-09 13:13:16', '2022-08-11 15:56:16');
INSERT INTO in_range_test() VALUES('2022-06-30','2022-07-31 00:00:00', '2022-08-11 15:56:16');
INSERT INTO in_range_test() VALUES('2021-06-30','2022-07-31 00:00:01', '2022-09-01 13:03:13.65456');
SELECT * FROM in_range_test WHERE IN_RANGE(d1, '2022-06-01', '2022-06-30', 0);
SELECT d2 FROM in_range_test WHERE IN_RANGE(d2, '2022-06-01', '2022-07-30 23:59:59', 0);
SELECT * FROM in_range_test WHERE IN_RANGE(d1, '2022-06-01', '2022-06-15', 3);
SELECT * FROM in_range_test WHERE IN_RANGE(d1, '2022-06-15', '2022-06-01', 3);
SELECT d2 FROM in_range_test WHERE IN_RANGE(d2, '2022-06-09 00:13:16', '2022-06-9 14:00:00', 3);
SELECT d2 FROM in_range_test WHERE IN_RANGE(d2, '2022-07-31', '2022-07-31', 0);
SELECT d2 FROM in_range_test WHERE IN_RANGE(d2, '2022-07-31', '2022-07-31', 3);

DELETE FROM in_range_test;
DROP TABLE IF EXISTS in_range_test;

CREATE TABLE t1(
    id INT,
    str VARCHAR(20)
);

CREATE TABLE t2(
    id INT,
    order_time DATE
);

INSERT INTO t1 VALUES(0, 'honda'), (-1, 'toyota'), (1, ' '), (-2, 'NULL');
INSERT INTO t2 VALUES(0, '2022-09-21'), (-1, '2022-09-11'), (1, '2021-09-21'), (-2, '2021-09-11');
INSERT INTO t1 VALUES(2, '#$%@RETE');

SELECT * FROM t1 WHERE IN_RANGE(id, '0', '2', 0);
SELECT id,str FROM t1 WHERE IN_RANGE(id, 0, 0, 0) AND IN_RANGE(str, 'A', 'Z', 0);
SELECT * FROM (SELECT * FROM t1 WHERE IN_RANGE(str, 'a', 'x', 0)) AS t WHERE IN_RANGE(id, 0, 1, 0);
SELECT * FROM (SELECT * FROM t1 WHERE IN_RANGE(str, '', 'z', 0)) AS t WHERE IN_RANGE(id, -1, 1, 0);

SELECT t1.id, str, order_time
FROM t1 INNER JOIN t2 ON t1.id = t2.id
WHERE IN_RANGE(t1.id, -2, 2, 0) AND IN_RANGE(t2.order_time, '2022-09-11', '2022-09-21', 0);

SELECT t1.id, str, order_time
FROM t1 INNER JOIN t2 ON t1.id = t2.id
WHERE IN_RANGE(YEAR(order_time), '2021', '2022', 0);

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1(
    id VARCHAR(20),
    name VARCHAR(20)
);

CREATE TABLE t2(
    id VARCHAR(20),
    n1 INT,
    n2 FLOAT
);

INSERT INTO t1 VALUES('0001', 'Smi'), ('1111', 'Fri'), ('2222', '22TOM');
INSERT INTO t2 VALUES('0001', 0001, 0.01), ('1111', 1111, -0.01), ('2222', 2222, NULL);

SELECT * FROM t1 WHERE IN_RANGE(id, '0', '2', 0);
SELECT * FROM t1 WHERE IN_RANGE(name, '0', '2', 0);
SELECT * FROM t1 WHERE IN_RANGE(id, '0', '2', 3);



SELECT t1.id, name, n1, n2
FROM t1 RIGHT JOIN t2 ON t1.id = t2.id
WHERE IN_RANGE(t1.id, 'a', 'Z', 3) AND IN_RANGE(n2, -0.01, 0.01, 0);

SELECT t1.id, name, n1, n2
FROM t1 INNER JOIN t2 ON t1.id = t2.id
WHERE IN_RANGE(t1.id, '0', '1', 0) AND IN_RANGE(n1, 0, 1111, 0);

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

-- @case
-- @desc:test for IN_RANGE operator in function
-- @label:bvt
CREATE TABLE t1(
    str1 VARCHAR(50),
    b BOOL
);

INSERT INTO t1(str1) VALUES('This product is merged some high tech.');
INSERT INTO t1(str1) VALUES('&^%$#@ has some error');
INSERT INTO t1() VALUES('many years age, joe smith always', TRUE);
INSERT INTO t1(b) VALUES(TRUE), (FALSE), (NULL);

SELECT * FROM t1 WHERE IN_RANGE(SUBSTRING(str1,LENGTH(str1)), 'A', 'z', 0);
SELECT * FROM t1 WHERE IN_RANGE(SUBSTRING(str1,LENGTH(str1) - (LENGTH(str1)-1)), 'A', 'z', 0);
SELECT * FROM t1 WHERE IN_RANGE(SUBSTRING(str1,LENGTH(str1) - (LENGTH(str1)-1)), 'A', 'z', 3);
SELECT * FROM t1 WHERE IN_RANGE(str1, 'm', 'T', 3);
SELECT * FROM t1 WHERE IN_RANGE(b, TRUE, FALSE, 0);
SELECT * FROM t1 WHERE IN_RANGE(b, TRUE, FALSE, 3);
SELECT * FROM t1 WHERE IN_RANGE(LENGTH(str1), 25, 50, 0);
SELECT * FROM t1 WHERE IN_RANGE(LENGTH(str1), 25, 50, 3);

DROP TABLE IF EXISTS t1;

CREATE TABLE t1(
    d1 DATE,
    d2 DATETIME
);

INSERT INTO t1 VALUES('2022-09-11', '2029-12-16 01:21:34');
INSERT INTO t1 VALUES('2021-11-11', '2028-11-06 00:21:34');
INSERT INTO t1 VALUES('2222-12-11', '1999-05-16 21:21:34');

SELECT * FROM t1 WHERE IN_RANGE(d1, '2022-01-01', '2022-12-31', 0);
SELECT * FROM t1 WHERE IN_RANGE(d1, '2021-01-01', '2022-12-31', 0);
SELECT * FROM t1 WHERE IN_RANGE(d1, '2022-09-01', '2222-12-11', 0);
SELECT * FROM t1 WHERE IN_RANGE(d1, '2022-09-01', '2222-12-11', 3);
SELECT * FROM t1 WHERE IN_RANGE(MONTH(d1) - 2, 9, 12, 0);
SELECT * FROM t1 WHERE IN_RANGE(YEAR(d2), 1999, 2020, 0);
SELECT * FROM t1 WHERE IN_RANGE(YEAR(d2), 1999, 2020, 3);
SELECT * FROM t1 WHERE IN_RANGE(d2, '2028-11-06', '2029-12-16', 0);
SELECT * FROM t1 WHERE IN_RANGE(d2, '2028-11-06', '2029-12-16', 3);
