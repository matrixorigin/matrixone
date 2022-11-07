-- @suit

-- @case
-- @desc:test filter key ORDER BY
-- @label:bvt

-- No INDEX explicitly
-- different data type
DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (
	id INT AUTO_INCREMENT,
	name VARCHAR(255),
	category_id INT,
	d TIMESTAMP,
	PRIMARY KEY (id)
);
INSERT INTO t1 (id, name, category_id, d) VALUES
(1, 'aaa', 1, '2010-06-10 19:14:37'),
(2, 'bbb', 2, '2010-06-10 19:14:55'),
(3, 'ccc', 1, '2010-06-10 19:16:02'),
(4, 'ddd', 1, '2010-06-10 19:16:15'),
(5, 'eee', 2, '2010-06-10 19:16:35');
-- MO default ASC
SELECT * FROM t1 ORDER BY d;
SELECT * FROM (SELECT * FROM t1 ORDER BY d DESC) temp ORDER BY d DESC;
SELECT * FROM t1 WHERE category_id = 1 ORDER BY id;
SELECT * FROM t1 WHERE category_id = 1 ORDER BY d;
SELECT * FROM t1 WHERE category_id = 1 ORDER BY 1;
SELECT * FROM t1 WHERE category_id = 1 ORDER BY 1+1;
SELECT * FROM t1 WHERE category_id = 1 ORDER BY SIN(1);
-- ???
SELECT * FROM t1 WHERE category_id = 1 ORDER BY TRUE;

-- CHAR and VARCHAR
DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (
	name VARCHAR(200),
	area CHAR(200),
	PRIMARY KEY (name)
);
INSERT INTO t1() VALUES
('a','b'),
('tewr','lojj'),
('The index may also be used even if the ORDER BY d', ' all unused portions of the index and all ex'),
('ssed by the query, the index is used', 'ex is more efficient than a table scan if c'),
('more expensive than scanning the table and', 'e optimizer probably does not use the index. If SELECT'),
('imilar to the same queries without DESC', 'homogeneity, but need not have the same actual direction.');

-- Use Table Scan
SELECT name FROM t1 ORDER BY name;
SELECT name FROM t1 ORDER BY name,area;
SELECT name FROM t1 ORDER BY area,name;
SELECT * FROM t1 WHERE name = 'a' ORDER BY area;
SELECT * FROM t1 WHERE LENGTH(name) > 10 ORDER BY name ASC;
SELECT * FROM t1 WHERE LENGTH(name) < 10 ORDER BY name DESC;
SELECT * FROM t1 WHERE LENGTH(name) > 10 ORDER BY LENGTH(area);
SELECT name FROM t1 ORDER BY name ASC, area DESC;
SELECT name FROM t1 ORDER BY name DESC, area ASC;
DELETE FROM t1;
INSERT INTO t1(name) VALUES('abkl'),('bfdjskl'),('cdjkl'),('djiofj'),('efjkl;'),('fjkldsa'),('gljfdka');
SELECT * FROM t1 ORDER BY name DESC, name ASC;

-- DATE, DATETIME, TIMESTAMP
DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (
	d1 DATE,
	d2 DATETIME,
	d3 TIMESTAMP
);
INSERT INTO t1() VALUES ('2020-08-08','2020-08-07 00:01:02','2020-08-07 00:01:02.136487');
INSERT INTO t1() VALUES ('2021-09-09','2020-09-09 10:11:02','2020-09-09 10:11:02.136558');
INSERT INTO t1() VALUES ('2021-07-07','2020-07-07 17:17:12','2020-07-07 07:07:05.135582');
INSERT INTO t1() VALUES ('2021-06-06','2020-06-06 21:21:22','2020-06-06 02:21:22.135418');
SELECT * FROM t1 ORDER BY d1 DESC;
SELECT * FROM t1 ORDER BY d1 DESC, d2 ASC, d3 DESC;
SELECT * FROM t1 ORDER BY d2 ASC, d1 ASC, d3 DESC;
SELECT * FROM t1 WHERE d1 BETWEEN '2021-06-06' AND '2021-08-08' ORDER BY d1;

-- TINYINT, INT, SMALLINT, BIGINT
DROP TABLE IF EXISTS t1;
CREATE TABLE t1(
    tiny TINYINT NOT NULL,
    small SMALLINT NOT NULL,
    int_test INT NOT NULL,
    big BIGINT NOT NULL
);
INSERT INTO t1() VALUES(1, 2, 3, 4),(100, 101, 102, 103),(NULL, NULL, NULL, 204),(64,1,4564,46843);
SELECT * FROM t1 ORDER BY small DESC;
SELECT * FROM t1 WHERE tiny < 100 ORDER BY big ASC;
SELECT * FROM t1 ORDER BY tiny ASC, small DESC, int_test ASC, big DESC;

-- FLOAT, DOUBLE, DECIMAL
DROP TABLE IF EXISTS t1;
CREATE TABLE t1(
    float_32 FLOAT,
    float_64 DOUBLE,
    d DECIMAL
);
INSERT INTO t1() VALUES(0.01, 0.02, 0.03), (0.000001,0.000002,0),(-1,-1.1,-1.2),(0.000003,0.000001,3);
SELECT * FROM t1 ORDER BY float_32 ASC, float_64 DESC;
SELECT * FROM t1 ORDER BY float_64 DESC;
SELECT * FROM t1 ORDER BY d ASC;

-- JOIN
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
CREATE TABLE t1(
    id INT NOT NULL,
    name CHAR(20) NOT NULL,
    PRIMARY KEY (id)
);
CREATE TABLE t2(
    id VARCHAR(10) NOT NULL,
    nation VARCHAR(20) NOT NULL,
    PRIMARY KEY(id)
);
CREATE TABLE t3(
    nation VARCHAR(20) NOT NULL,
    city CHAR(20) NOT NULL,
    GDP FLOAT NOT NULL,
    PRIMARY KEY(nation)
);
INSERT INTO t1() VALUES(1,'ronaldo'), (2,'kante'), (3,'noyer'),(4,'modrici');
INSERT INTO t2() VALUES(1,'Poutanga'), (2,'NA'), (4,'Fenland');
INSERT INTO t3() VALUES('Poutanga','liseber',520135), ('NA','bolando',62102), ('Fenland','yisdilne', 612094);
SELECT * FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.id BETWEEN 1 AND 4+1 ORDER BY name;
SELECT * FROM t1 JOIN t2 ON t1.id = t2.id ORDER BY t1.id;
SELECT * FROM t1 JOIN t2 ON t1.id = t2.id JOIN t3 ON t2.nation = t3.nation ORDER BY t2.nation ASC, t3.GDP DESC;
SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id RIGHT JOIN t3 ON t2.nation = t3.nation ORDER BY t2.nation ASC, t3.GDP DESC;
SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id RIGHT JOIN t3 ON t2.nation = t3.nation ORDER BY t2.nation DESC, t3.GDP DESC;

-- UNION and UNION ALL
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1(
    id INT NOT NULL,
    name CHAR(20) NOT NULL,
    sex CHAR(4) NOT NULL,
    PRIMARY KEY (id)
);
CREATE TABLE t2(
    id VARCHAR(10) NOT NULL,
    name VARCHAR(20) NOT NULL,
    nation VARCHAR(20) NOT NULL,
    PRIMARY KEY(id)
);
INSERT INTO t1() VALUES(1,'ronaldo','F'), (2,'kante','M'), (3,'noyer','F'),(4,'modrici','M');
INSERT INTO t2() VALUES(1,'ronaldo','American'), (2,'kante','Franch'), (3,'noyer','Germany'),(4,'modrici','UK');
SELECT * FROM t1 UNION ALL SELECT * FROM t2 ORDER BY name;
(SELECT * FROM t1 UNION ALL SELECT * FROM t2) ORDER BY name DESC, id ASC;
(SELECT * FROM t1) UNION (SELECT * FROM t2 ORDER BY nation) ORDER BY id DESC;
(SELECT * FROM t1 ORDER BY sex) UNION (SELECT * FROM t2);
(SELECT * FROM t1 WHERE sex = 'M' ORDER BY sex DESC)
UNION
(SELECT * FROM t2 WHERE id >= 3 ORDER BY nation ASC) ORDER BY id ASC;

(SELECT * FROM t1 WHERE id BETWEEN 1 AND 2 ORDER BY name)
UNION ALL
(SELECT * FROM t2 WHERE nation BETWEEN 'A' AND 'F' ORDER BY id DESC);

-- UNION and UNION ALL, Multi table great than 2
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
CREATE TABLE t1(
    id INT NOT NULL,
    name CHAR(20) NOT NULL,
    PRIMARY KEY (id)
);
CREATE TABLE t2(
    id VARCHAR(10) NOT NULL,
    nation VARCHAR(20) NOT NULL,
    PRIMARY KEY(id)
);
CREATE TABLE t3(
    id VARCHAR(10) NOT NULL,
    area VARCHAR(20) NOT NULL,
    PRIMARY KEY(id)
);
INSERT INTO t1() VALUES(1,'ronaldo'), (2,'kante'), (3,'noyer'),(4,'modrici');
INSERT INTO t2() VALUES(1,'UK'), (2,'USA'), (3,'RA'),(4,'CN');
INSERT INTO t3() VALUES(1,'EU'), (2,'NA'), (3,'AU'),(4,'AS');
(SELECT * FROM t1) UNION (SELECT * FROM t2 ORDER BY id DESC) UNION ALL (SELECT * FROM t3 ORDER BY area);
((SELECT * FROM t1 ORDER BY id DESC) UNION (SELECT * FROM t2) UNION ALL (SELECT * FROM t3 ORDER BY area)) ORDER BY id;
(SELECT * FROM t1 ORDER BY name) UNION (SELECT * FROM t2 ORDER BY id) UNION ALL (SELECT * FROM t3 ORDER BY area);
-- Multi table JOIN(INNER, LEFT, RIGHT)
(SELECT * FROM t1 JOIN t2 ON t1.id = t2.id ORDER BY t2.id)
UNION
(SELECT * FROM t2 RIGHT JOIN t3 ON t2.id = t3.id ORDER BY t3.id DESC);
-- Joins between different tables
(SELECT * FROM t1 LEFT JOIN t3 ON t1.id = t3.id ORDER BY t1.id DESC, t3.area ASC)
UNION
(SELECT * FROM t2 RIGHT JOIN t3 ON t2.id = t3.id ORDER BY t2.nation ASC, t3.id DESC);

-- ORDER BY with GROUP BY
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1(
    id INT NOT NULL,
    d1 CHAR(50) NOT NULL,
    salary FLOAT NOT NULL,
    PRIMARY KEY (id)
);
CREATE TABLE t2(
    id INT NOT NULL,
    name CHAR(50) NOT NULL,
    sex CHAR(4) NOT NULL,
    PRIMARY KEY (id)
);
INSERT INTO t1() VALUES(1,'2020-01-01',23.6), (2,'2020-01-01',89.6), (3,'2020-01-02',45.6);
INSERT INTO t1() VALUES(4,'2020-01-01',66.6), (5,'2020-01-03',17.6), (6,'2020-01-03',123.6);
INSERT INTO t2() VALUES(1,'jaca','F'), (2,'mecan','M'),(3,'right','F'),(4,'rodia','M');
INSERT INTO t2() VALUES(5,'hila','F'), (6,'pika','M');
SELECT DATE(d1), MAX(salary) FROM t1 GROUP BY d1 ORDER BY MAX(salary) DESC;
SELECT DATE(d1), MAX(salary) FROM t1 GROUP BY d1 ORDER BY MAX(salary) ASC;
SELECT DATE(d1), MAX(salary) FROM t1 JOIN t2 ON t1.id = t2.id GROUP BY d1 ORDER BY MAX(salary) DESC;

-- NULL LAST/FIRST
-- Single table
DROP TABLE IF EXISTS t1;
CREATE TABLE t1(
    id INT,
    name CHAR(20),
    PRIMARY KEY(id)
);
INSERT INTO t1() VALUES(1, 'jacak'), (2, 'tommy'), (3, 'rorgdbs'), (4, NULL);
SELECT * FROM t1 ORDER BY name ASC;
SELECT * FROM t1 ORDER BY name DESC, id ASC;
SELECT * FROM t1 ORDER BY name ASC NULLS LAST;
SELECT * FROM t1 ORDER BY name ASC NULLS FIRST;
SELECT * FROM t1 ORDER BY name ASC NULLS FIRST LAST;

DROP TABLE IF EXISTS t1;
CREATE TABLE t1(
    id INT,
    name CHAR(20),
    salary FLOAT,
    dept INT,
    PRIMARY KEY(id)
);
INSERT INTO t1() VALUES(1,'bdkia',133.1,11), (2, 'dodro',983.6,NULL), (3, 'fafeaz',301.5,10), (4, NULL,NULL,13);
SELECT * FROM t1 WHERE id > 1 ORDER BY id DESC, salary NULLS FIRST;
SELECT * FROM t1 WHERE id BETWEEN 1 AND 4 ORDER BY id DESC, dept NULLS LAST, salary DESC NULLS FIRST;
SELECT id,name FROM t1 ORDER BY salary DESC NULLS FIRST, dept DESC NULLS LAST, name ASC NULLS FIRST;

-- JOIN -> ORDER BY...NULLS LAST/FIRST
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1(
    id INT,
    name CHAR(20),
    PRIMARY KEY(id)
);
CREATE TABLE t2(
    id INT,
    nation CHAR(20),
    PRIMARY KEY(id)
);
INSERT INTO t1() VALUES(1, 'jacak'), (2, 'tommy'), (3, 'roses'), (4, NULL);
INSERT INTO t2() VALUES(1, 'US'), (2, 'UK'), (3, NULL), (4, NULL), (5, NULL);
SELECT * FROM t1 JOIN t2 ON t1.id = t2.id ORDER BY name DESC NULLS FIRST;
SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id ORDER BY name ASC NULLS LAST;
SELECT * FROM t1 RIGHT JOIN t2 ON t1.id = t2.id ORDER BY name ASC, nation DESC NULLS LAST;
SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id ORDER BY name DESC, nation ASC NULLS FIRST;

-- NO primary key
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1(
    id INT,
    sex CHAR(20)
);
CREATE TABLE t2(
    id INT,
    home CHAR(20)
);
INSERT INTO t1() VALUES(1, 'F'), (2, 'M'), (NULL, 'M'), (4, NULL), (NULL, NULL);
INSERT INTO t2() VALUES(1, 'EU'), (2, 'UK'), (3, NULL), (4, NULL), (5, NULL), (NULL, NULL);
SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id ORDER BY sex ASC NULLS LAST;
SELECT * FROM t1 RIGHT JOIN t2 ON t1.id = t2.id ORDER BY home ASC NULLS FIRST;
SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id ORDER BY home ASC, sex DESC NULLS FIRST;
