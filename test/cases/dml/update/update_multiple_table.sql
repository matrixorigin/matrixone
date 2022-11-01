-- @suit

-- @case
-- @desc:test for UPDATE for multiple table
-- @label:bvt

CREATE TABLE product(
    id VARCHAR(20),
    product_id VARCHAR(20),
    product_name VARCHAR(50),
    price FLOAT
);

CREATE TABLE product_price(
    id VARCHAR(20),
    product_id VARCHAR(20),
    price FLOAT
);

INSERT INTO product() VALUES ('1', '1001', 'Java教程', '100');
INSERT INTO product() VALUES ('2', '1002', 'MySQL教程', '80');
INSERT INTO product() VALUES ('3', '1003', 'Python教程', '120');
INSERT INTO product() VALUES ('4', '1004', 'C语言教程', '150');
INSERT INTO product_price() VALUES ('1', '1001', NULL);
INSERT INTO product_price() VALUES ('2', '1002', NULL);
INSERT INTO product_price() VALUES ('3', '1003', NULL);

UPDATE product_price, product SET product.id = product.id + 1, product_price.id = product_price.id + 1;
SELECT product.id, product_price.id FROM product, product_price;
UPDATE product, product_price SET product_name = 'PHP', product_price.price = 100 WHERE product.product_id = '1001';
SELECT * FROM product;
SELECT * FROM product_price;
-- USE '=' to connect two tables for UPDATE
UPDATE product p, product_price pp SET pp.price = p.price * 0.8 WHERE p.product_id = pp.product_id;
SELECT product.id, product.product_name, product_price.price FROM product, product_price;
-- USE 'INNER JOIN' to connect two tables for UPDATE
UPDATE product p INNER JOIN product_price pp ON p.product_id = pp.product_id SET pp.price = p.price * 0.8;
SELECT product.product_name, product_price.price FROM product, product_price;
DROP TABLE IF EXISTS product;
DROP TABLE IF EXISTS product_price;

CREATE TABLE stu(
    id VARCHAR(10),
    name VARCHAR(20),
    class_id VARCHAR(5),
    class_name VARCHAR(10)
);
CREATE TABLE class(
    id VARCHAR(10),
    name VARCHAR(20),
    stu_name VARCHAR(20)
);

INSERT INTO stu VALUES('1', '张三', '1', NULL), ('2', '李四', '1', NULL), ('3', '王五', '2', NULL);
INSERT INTO stu VALUES('4', 'Rose', '2', NULL), ('5', 'Bob', '4', NULL), ('6', 'Ruby', '5', NULL);
INSERT INTO class VALUES('1', '一班', NULL), ('2', '二班', NULL), ('3', '三班', NULL);

UPDATE stu s , class c SET s.class_name = 'test00', c.stu_name = 'test00' WHERE s.class_id = c.id;
SELECT stu.name, stu.class_name, class.name, class.stu_name FROM stu, class;
UPDATE stu s INNER JOIN class c ON s.class_id = c.id SET s.class_name = 'test11', c.stu_name = 'test11';
SELECT stu.id, stu.name, stu.class_name, class.stu_name FROM stu, class;


UPDATE stu s LEFT JOIN class c ON s.class_id = c.id SET s.class_name = 'test22', c.stu_name = 'test22';
UPDATE stu s RIGHT JOIN class c ON s.class_id = c.id SET s.class_name = 'test33',c.stu_name = 'test33';
SELECT stu.name, stu.class_name, class.stu_name FROM stu, class;
UPDATE stu s JOIN class c ON s.class_id = c.id SET s.class_name = c.name , c.stu_name = s.name;
SELECT stu.name, stu.class_name, class.stu_name FROM stu, class;

DROP TABLE IF EXISTS stu;
DROP TABLE IF EXISTS class;

CREATE TABLE t1(
    id INT,
    name VARCHAR(20),
    paytime DATETIME
);
CREATE TABLE t2(
    id INT,
    processtime DATETIME
);

INSERT INTO t1() VALUES(1, 'has', '2022-08-17 19:16:18'), (2, 'norm', '2022-04-12 04:32:46');
INSERT INTO t2() VALUES(1, '2022-08-17 20:23:06'), (2, '2022-04-18 09:19:15');
SELECT * FROM t1;
SELECT * FROM t2;
UPDATE t1,t2 SET name = 'tom' WHERE t1.id = '1' AND t2.id = '2';
SELECT t1.id, t1.name FROM t1;
UPDATE t1,t2 SET paytime = '2022-08-16 19:16:18', processtime = '2022-08-16 20:23:06' WHERE t1.id = t2.id AND paytime > '2022-08-15';
-- USE DATE_ADD function.
UPDATE t1,t2
SET
    paytime = DATE_ADD(paytime, INTERVAL 4 DAY), processtime = DATE_SUB(processtime, INTERVAL 6 DAY)
WHERE
    paytime > '2022-08-25';
SELECT t1.paytime, t2.processtime FROM t1,t2;

-- USE TO_DATE() function.
UPDATE t1,t2
SET
    paytime = TO_DATE('2099-01-01 00:00:01', '%Y-%m-%d %H-%i-%s'), processtime = TO_DATE('2088-01-01 :00:00:02', '%Y-%m-%d %H:%i:%s')
WHERE
    t1.id = t2.id;

-- USE DATE() function
UPDATE t1,t2
SET
    paytime = DATE('2088-01-01'), processtime = DATE('9999-01-01')
WHERE
    t1.id = t2.id;
SELECT t1.id, t1.paytime, t2.processtime FROM t1, t2;

-- SubQuery
UPDATE t1,t2 SET paytime = (SELECT DATE('3333-06-06')), processtime = (SELECT DATE('8888-09-09'));
SELECT paytime,processtime FROM t1,t2;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1(
    str1 CHAR(20) NOT NULL PRIMARY KEY,
    str2 VARCHAR(20) NOT NULL
);
CREATE TABLE t2(
    str1 CHAR(20),
    str2 VARCHAR(20) NOT NULL
);

INSERT INTO t1 VALUES('this is a test', 'that is actual'), ('1001', 'game of throne');
INSERT INTO t2 VALUES('1001', 'little Iamp'), ('targeran', 'snow and rain');

UPDATE t1,t2 SET t1.str1 = '', t2.str2 = 'NULL' WHERE t1.str1 = t2.str1;
SELECT t1.str1, t2.str2 FROM t1,t2;
UPDATE t1,t2 SET t1.str2 = SUBSTRING('jkfldisaojfd',5), t2.str1 = LENGTH('123');
SELECT t1.str2, t2.str1 FROM t1,t2;
UPDATE t1,t2 SET t1.str2 = STARTSWITH(t1.str2,'t'), t2.str2 = STARTSWITH(t2.str2, 's');
SELECT t1.str2, t2.str2 FROM t1,t2;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1(
    n1 INT,
    n2 INT UNSIGNED,
    n3 FLOAT
);
CREATE TABLE t2(
    n1 INT,
    n2 DOUBLE
);

INSERT INTO t1 VALUES(1, 2, 3), (5, 6, 7), (10, 11, 12), (12, -0, 19);
INSERT INTO t2 VALUES(1, 0), (5, -1), (19, 13);

UPDATE t1,t2 SET t1.n2 = EXP(1), t2.n2 = SIN(3) WHERE t1.n1 = t2.n1;
SELECT t1.n2, t2.n2 FROM t1, t2;
UPDATE t1,t2 SET t1.n2 = POWER(5, -1), t2.n2 = POWER(0,0) WHERE t1.n3 BETWEEN 0 AND 3;
SELECT t1.n2, t2.n2 FROM t1, t2;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;




