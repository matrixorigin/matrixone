drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
CREATE TABLE t1 (S1 INT, S2 varchar(10));
CREATE TABLE t2 (S1 INT, S2 varchar(10));
CREATE TABLE t3 (S1 INT, S2 varchar(10));
INSERT INTO t1 VALUES (1,'aaa'),(2,'bbb'),(3,'ccc'),(4,NULL),(5,'eee'),(6,'fff'),(NULL,'aaa'),(9,'bbb'),(10,'ccc'),(11,'ddd'),(12,'abc'),(NULL,NULL);
INSERT INTO t2 VALUES  (11,'aaa'),(12,'bbb'),(13,'ccc'),(14,NULL),(1,'aaa'),(2,'bbb'),(3,'ccc'),(4,'ddd'),(NULL,'abc'),(NULL,NULL);
SELECT * FROM t1 RIGHT JOIN t2 on t1.S1=t2.S1;
SELECT * FROM t1 RIGHT JOIN t2 on t1.S2=t2.S2;
SELECT * FROM t2 RIGHT JOIN t1 on t1.S1=t2.S1;
SELECT * FROM t2 RIGHT JOIN t1 on t1.S2=t2.S2;
SELECT * FROM t3 RIGHT JOIN t1 on t1.S1=t3.S1;
SELECT * FROM t3 RIGHT JOIN t1 on t1.S2=t3.S2;
SELECT * FROM t1 RIGHT JOIN t3 on t1.S1=t3.S1;
SELECT * FROM t1 RIGHT JOIN t3 on t1.S2=t3.S2;

