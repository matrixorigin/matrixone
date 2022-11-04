-- @suit

-- @case
-- @desc:test for IS and IS NOT operator
-- @label:bvt

SELECT 1 IS TRUE, 0 IS FALSE;
SELECT NULL IS TRUE;
SELECT 1 IS NULL, 0 IS NULL, NULL IS NULL;
SELECT '' IS NULL, ' ' IS NULL;

DROP TABLE IF EXISTS is_test;
CREATE TABLE is_test(
    str1 CHAR(20),
    str2 VARCHAR(20),
    d1 DATE,
    d2 DATETIME,
    d3 TIMESTAMP
);

INSERT INTO is_test VALUES('', ' ', NULL, NULL, NULL);
INSERT INTO is_test VALUES('NULL', NULL, '0001-01-01', '2022-02-01 13:46:02', '1994-03-02');
INSERT INTO is_test(str1, str2) VALUES('0', 1);
INSERT INTO is_test(str1, str2) VALUES('1', 0);
INSERT INTO is_test(str1, str2) VALUES(1, '1');
INSERT INTO is_test(str1, str2) VALUES(0, '0');

SELECT * FROM is_test WHERE str1 IS FALSE;
SELECT d1, d2 FROM is_test WHERE str2 IS NOT FALSE;
SELECT * FROM is_test WHERE str2 IS TRUE;
SELECT * FROM is_test WHERE str1 IS FALSE;
SELECT * FROM is_test WHERE str2 IS TRUE AND str1 IS TRUE;
SELECT * FROM is_test WHERE LENGTH(str2) IS TRUE;
SELECT * FROM is_test WHERE str1 IS TRUE OR str2 IS TRUE;

DELETE FROM is_test;
DROP TABLE IF EXISTS is_test;
CREATE TABLE is_test(
    tiny TINYINT,
    small SMALLINT,
    int_test INTEGER,
    big BIGINT,
    tiny_un TINYINT UNSIGNED,
    small_un SMALLINT UNSIGNED,
    int_test_un INTEGER UNSIGNED,
    big_un BIGINT UNSIGNED,
    float_32 FLOAT,
    float_64 DOUBLE
);

INSERT INTO is_test(tiny, small, int_test, big) VALUES(0, 1, 0, -0);
INSERT INTO is_test(tiny_un, small_un) VALUES(0, 1);
INSERT INTO is_test(tiny_un, small_un) VALUES(0, 2);
INSERT INTO is_test(float_32, float_64) VALUES(0.0, 0.00000000000000000000001);
INSERT INTO is_test(float_32, float_64) VALUES(1.0, 0.4999999999999);
INSERT INTO is_test(float_32, float_64) VALUES(0.51, -0.00000000000000000000001);

SELECT -1 IS TRUE;
SELECT 0 IS TRUE;
SELECT -1 IS TRUE;
SELECT 1 IS TRUE;
-- @bvt:issue#5113
SELECT 2 IS TRUE;
SELECT -2 IS TRUE;
-- @bvt:issue
SELECT * FROM is_test WHERE big IS TRUE;
SELECT small_un FROM is_test WHERE small_un IS TRUE;
SELECT float_32, float_64 FROM is_test WHERE float_32 > 0 IS FALSE;
SELECT float_32, float_64 FROM is_test WHERE float_64 > 0 IS FALSE;
SELECT float_32, float_64 FROM is_test WHERE float_64 = 0 IS TRUE;

DELETE FROM is_test;
DROP TABLE IF EXISTS is_test;

CREATE TABLE is_test(
    b1 BOOL,
    b2 BOOL
);

INSERT INTO is_test VALUES(TRUE, FALSE);
INSERT INTO is_test VALUES(1, FALSE);
INSERT INTO is_test VALUES(0, TRUE);
INSERT INTO is_test VALUES(FALSE, 1);
INSERT INTO is_test VALUES(1, NULL);
INSERT INTO is_test VALUES(NULL, 0);
SELECT * FROM is_test WHERE b1 IS TRUE;
SELECT * FROM is_test WHERE b1 IS NOT FALSE;
SELECT * FROM is_test WHERE b2 IS FALSE;
SELECT * FROM is_test WHERE b2 IS NOT FALSE;

DELETE FROM is_test;
DROP TABLE IF EXISTS is_test;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1(
    id INT PRIMARY KEY,
    name VARCHAR(20),
    class VARCHAR(4)
);
CREATE TABLE t2(
    id INT,
    nation VARCHAR(20),
    major VARCHAR(20)
);

INSERT INTO t1 VALUES('1001', 'JACK', '');
INSERT INTO t1 VALUES('1002', 'TOM', '0');
INSERT INTO t1 VALUES('1003', '1', '5');
INSERT INTO t1 VALUES('1004', ' ', '9');
INSERT INTO t2 VALUES('1001', 'BRAZIL', 'AI');
INSERT INTO t2 VALUES('1002', 'IRELAND', '1');
INSERT INTO t2 VALUES('1003', 'TRUE', 'AI');
INSERT INTO t2 VALUES('1004', 'IRELAND', '0');
SELECT id FROM t1 WHERE name IS TRUE;
SELECT name, class FROM t1 WHERE name IS NOT TRUE;
SELECT id, name, class FROM t1 WHERE CAST(SUBSTRING(id, 4) AS TINYINT) IS TRUE;
SELECT t1.id, t1.name, t2.nation FROM t1 INNER JOIN t2 ON t1.id = t2.id WHERE t1.class IS NOT FALSE;
SELECT t1.id, t2.major FROM t1 INNER JOIN t2 ON t1.id = t2.id WHERE t1.name IS TRUE;
SELECT t1.id, t2.major FROM t1 INNER JOIN t2 ON t1.id = t2.id WHERE t2.nation IS TRUE;

-- @case
-- @desc:test for IS and IS NOT operator in function of string type
-- @label:bvt
SELECT * FROM t1 WHERE id = 1004 AND LENGTH(name) IS TRUE;
SELECT * FROM t1 WHERE LENGTH(name) IS FALSE;
SELECT id FROM t1 WHERE LENGTH(name) IS TRUE;
SELECT * FROM t2 WHERE STARTSWITH(id, '1') IS TRUE;
SELECT * FROM t2 WHERE ENDSWITH(id, '1') IS FALSE;
SELECT * FROM t1 WHERE FIND_IN_SET('J', name) IS TRUE;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1(
    user_id VARCHAR(50) PRIMARY KEY,
    user_name VARCHAR(20),
    order_time DATETIME,
    pay_status BOOL
);
CREATE TABLE t2(
    order_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50),
    product_id VARCHAR(50),
    number INT,
    price FLOAT
);

INSERT INTO t1 VALUES('7321', '小明', '2022-09-18 13:33:56', TRUE);
INSERT INTO t1 VALUES('7322', '小红', '2021-07-11 22:39:18', NULL);
INSERT INTO t1 VALUES('7323', '小刘', '2022-11-11 23:49:02', 1);
INSERT INTO t1 VALUES('7324', ' ', '2022-12-06 09:22:19', 0);
INSERT INTO t2 VALUES('9991', '7321', '', 1, 0.01);
INSERT INTO t2 VALUES('9992', '7322', '1023', 0, 9.99);
INSERT INTO t2 VALUES('9993', '7323', '1024', 18, 0.0);

SELECT user_id, user_name FROM t1 WHERE pay_status IS NOT TRUE;
SELECT user_id, user_name FROM t1 WHERE YEAR(order_time) > 1 IS TRUE;
SELECT user_id, user_name FROM t1 WHERE LENGTH(MONTH(order_time))-1 IS TRUE;
SELECT * FROM (SELECT * FROM t1 WHERE pay_status IS FALSE) AS t WHERE LENGTH(user_name) IS TRUE;
-- Check The BOOL and INT whether works.
SELECT
    t1.user_id, user_name, pay_status
FROM t1
INNER JOIN t2 ON t1.user_id = t2.user_id
WHERE pay_status IS TRUE AND number IS NOT FALSE;

-- Check The String and INT whether works, The INT is not just be 1 or 0.
SELECT
    t1.user_name, order_time, product_id
FROM
    t1
LEFT JOIN t2 ON t1.user_id = t2.user_id
WHERE STARTSWITH(order_id, '9') IS TRUE AND number IS TRUE;

-- Check The INT and BOOL whether works, The INT is not just be 1 or 0.
SELECT
    t1.user_name, order_time, number, price
FROM
    t1
RIGHT JOIN t2 ON t1.user_id = t2.user_id
WHERE number IS TRUE AND pay_status IS NOT FALSE;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE temp(
    tiny TINYINT,
    str CHAR(20),
    b BOOL,
    int_test INT
);

INSERT INTO temp VALUES(NULL, '1', FALSE, 1);
INSERT INTO temp VALUES(-1, '0', TRUE, 0);
INSERT INTO temp VALUES(-1, '1', 0, 0);
SELECT * FROM temp WHERE int_test IS FALSE;
SELECT * FROM temp WHERE str IS FALSE AND int_test IS FALSE;
SELECT * FROM (SELECT * FROM temp WHERE str IS TRUE) AS t WHERE int_test IS TRUE;
SELECT * FROM (SELECT * FROM temp WHERE tiny IS FALSE) AS t WHERE b IS NOT TRUE;
-- This SQL statement's result is different from MySQL and MO, MO's result is empty.
SELECT * FROM (SELECT * FROM temp WHERE tiny IS TRUE) AS t WHERE str IS TRUE;