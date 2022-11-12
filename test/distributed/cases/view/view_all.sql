
DROP TABLE IF EXISTS t00;
DROP TABLE IF EXISTS t01;
DROP VIEW IF EXISTS v0;
CREATE TABLE t00(a INTEGER);
INSERT INTO t00 VALUES (1),(2);
CREATE TABLE t01(a INTEGER);
INSERT INTO t01 VALUES (1);

CREATE VIEW v0 AS SELECT t00.a, t01.a AS b FROM t00 LEFT JOIN t01 USING(a);
SELECT t00.a, t01.a AS b FROM t00 LEFT JOIN t01 USING(a);
SELECT * FROM v0 WHERE b >= 0;
SHOW CREATE VIEW v0;
DROP TABLE IF EXISTS t00;
DROP TABLE IF EXISTS t01;
DROP TABLE IF EXISTS v0;

-----

CREATE VIEW v1 AS SELECT 1;
create view v2 as select 'foo' from dual;
SELECT * from v1;
SELECT * from v2;
DROP TABLE IF EXISTS v1;
DROP TABLE IF EXISTS v2;

CREATE VIEW v1 AS SELECT CAST(1/3 AS DOUBLE), CAST(1/3 AS FLOAT(2)), CAST(1/3 AS FLOAT(50));
SHOW CREATE VIEW v1;
SELECT * FROM v1;
DROP VIEW v1;

-- test insert to view duplicated
DROP TABLE IF EXISTS t1;
CREATE TABLE t1(c1 INT PRIMARY KEY, c2 INT);
CREATE VIEW v1 AS SELECT * FROM t1;
INSERT INTO v1 VALUES(1,20);
INSERT INTO v1 VALUES(1,5);
SELECT * FROM t1;
SELECT * FROM v1;
DROP VIEW v1;
DROP TABLE t1;

drop table if exists t1;
create table t1 (i int);
insert into t1 values (0), (1);
create view v1 as select * from t1;
select count(distinct i) from v1;
drop table t1;
drop view v1;

drop table if exists t;
CREATE TABLE t (x char(3));
INSERT INTO t VALUES ('foo'), ('bar'); 
CREATE VIEW v AS SELECT 'x' AS x FROM t;
SELECT DISTINCT x FROM v;
DROP TABLE t;
DROP VIEW v;

-- test view duplicate with table
drop table if exists t1;
drop table if exists t2;
CREATE TABLE t1 (a INT);
CREATE TABLE t2 (a INT);
INSERT INTO t1 VALUES (1),(2),(3);
INSERT INTO t2 VALUES (1),(2),(3);

CREATE VIEW v1 AS SELECT t1.a FROM t1, t2;
CREATE TABLE v1 (a INT);
select * from v1;

DROP VIEW v1;
drop table if exists t1;
drop table if exists t2;

create table t2 (a int);
create view t1 as select a from t2;
insert into t1 (a) values (1);
select * from t1;
create table t1 (a int);
create table if not exists t1 (a int,b int);
show create table t1;
select * from t1;
drop table t2;
drop view t1;

-- test insert to view when view is not a table 
drop view if exists t1;
drop table if exists t2;
create table t2 (a int);
create view t1 as select a + 5 as a from t2;
-- error ER_NONUPDATEABLE_COLUMN
insert into t1 (a) values (1);
-- error ER_NONUPDATEABLE_COLUMN
update t1 set a=3 where a=2;
drop view if exists t1;
drop table if exists t2;

create view t1 as select 1 as a;
-- error ER_NON_INSERTABLE_TABLE
insert into t1 (a) values (1);
-- error ER_NON_UPDATABLE_TABLE
update t1 set a=3 where a=2;
drop view if exists t1;


-- test for delete from join view
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP VIEW IF EXISTS v1;
DROP VIEW IF EXISTS v2;

CREATE TABLE t1(a INT);
CREATE TABLE t2(b INT);
insert into t1 values(1),(2);
insert into t2 values(1),(2);
CREATE VIEW v1 AS SELECT a, b FROM t1, t2;
CREATE VIEW v2 AS SELECT a FROM t1;
select * from v1;
select * from v2;
DELETE FROM v1;
DELETE v2 FROM v2;
select * from v1;
select * from v2;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP VIEW IF EXISTS v1;
DROP VIEW IF EXISTS v2;

-- test for view join table
drop table if exists t;
drop VIEW if exists v;
CREATE TABLE t(f1 INTEGER);
insert into t values(1),(2),(3),(6);
CREATE VIEW v AS SELECT f1 FROM t;
SELECT f1 FROM (SELECT f1 FROM v) AS dt1 NATURAL JOIN v dt2 WHERE f1 > 5;
SELECT f1 FROM v NATURAL JOIN v dt2 WHERE f1 > 5;
drop table if exists t;
drop VIEW if exists v;

-- test for view as DERIVED CONDITION
drop table if exists t1;
drop table if exists t2;
CREATE TABLE t1(f1 INTEGER PRIMARY KEY);
CREATE TABLE t2(f1 INTEGER);
INSERT INTO t1 VALUES (1),(2),(3),(4),(5);
CREATE VIEW v AS SELECT f1 FROM t1;
INSERT INTO t2 SELECT * FROM v WHERE f1=2;
select * from t2;
UPDATE t2 SET f1=3 WHERE f1 IN (SELECT f1 FROM v WHERE f1=2);
select * from t2;
DELETE FROM t2 WHERE f1 IN (SELECT f1 FROM v WHERE f1=3);
select * from t2;

DROP TABLE t1;
DROP TABLE t2;
DROp VIEW v;

-- 
CREATE TABLE C (
  col_varchar_10_key varchar(10) DEFAULT NULL,
  col_int_key int DEFAULT NULL,
  pk int NOT NULL AUTO_INCREMENT,
  col_date_key date DEFAULT NULL,
  PRIMARY KEY (`pk`),
  KEY `col_varchar_10_key` (`col_varchar_10_key`),
  KEY `col_int_key` (`col_int_key`),
  KEY `col_date_key` (`col_date_key`)
);
INSERT INTO C VALUES ('ok',3,1,'2003-04-02');
INSERT INTO C VALUES ('ok',3,2,'2003-04-02');

CREATE VIEW viewC AS SELECT * FROM C;

SELECT  table1.col_date_key AS field1 
FROM
  C AS table1
WHERE 
  (table1.col_int_key <=ANY 
    ( SELECT SUBQUERY1_t1.col_int_key 
      FROM viewC AS SUBQUERY1_t1 
      WHERE SUBQUERY1_t1.col_varchar_10_key <= table1.col_varchar_10_key 
    ) 
  )
;
DROP TABLE C;
DROP VIEW viewC;

-- 
DROP TABLE if exists t2;
DROP TABLE if exists t1;
DROP VIEW if exists v1;
DROP VIEW if exists v2;

CREATE TABLE t1(f1 int, f11 int);
CREATE TABLE t2(f2 int, f22 int);
INSERT INTO t1 VALUES(1,1),(2,2),(3,3),(5,5),(9,9),(7,7);
INSERT INTO t2 VALUES(1,1),(3,3),(2,2),(4,4),(8,8),(6,6);
CREATE VIEW v1 AS SELECT * FROM t1;
CREATE VIEW v2 AS SELECT * FROM t1 JOIN t2 ON f1=f2;
CREATE VIEW v3 AS SELECT * FROM t1 WHERE f1 IN (2,3);
CREATE VIEW v4 AS SELECT * FROM t2 WHERE f2 IN (2,3);

SELECT * FROM v1;
SELECT * FROM v2;
SELECT * FROM v3 WHERE f11 IN (1,3);
SELECT * FROM v3 JOIN v4 ON f1=f2;
SELECT * FROM v4 WHERE f2 IN (1,3);
SELECT * FROM (SELECT * FROM t1 group by f1 HAVING f1=f1) tt;
SELECT * FROM t1 JOIN (SELECT * FROM t2 GROUP BY f2) tt ON f1=f2;
DROP TABLE if exists t2;
DROP TABLE if exists t1;
DROP VIEW v1;
DROP VIEW v2;
DROP VIEW v3;
DROP VIEW v4;

-- test for view with order by 
CREATE TABLE t1 (f1 VARCHAR(1), key(f1));
INSERT INTO t1 VALUES ('a');
CREATE VIEW v1 AS SELECT f1 FROM t1 ORDER BY 1 LIMIT 0;
CREATE VIEW v2 AS SELECT f1 FROM t1 ORDER BY 1 LIMIT 1;
SELECT * FROM v1;
SELECT * FROM v2;
DROP VIEW v1;
DROP VIEW v2;
DROP TABLE t1;








