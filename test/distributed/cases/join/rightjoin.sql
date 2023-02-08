drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
CREATE TABLE t1 (S1 INT);
CREATE TABLE t2 (S1 INT);
CREATE TABLE t3 (S1 INT);

INSERT INTO t1 VALUES (1),(2),(3),(4),(5),(6),(7),(9),(10),(11),(12),(13);
INSERT INTO t2 VALUES (5),(6),(7),(8);

SELECT * FROM t1 RIGHT JOIN t2 on t1.S1=t2.S1;
SELECT * FROM t2 LEFT JOIN t1 on t1.S1=t2.S1;
SELECT * FROM t3 LEFT JOIN t1 on t1.S1=t3.S1;


