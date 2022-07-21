-- @suit

-- @case
-- @desc:test for [any] subquery with operand-is-column
-- @label:bvt
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t4;
create table t1 (a int);
create table t2 (a int, b int);
create table t3 (a int);
create table t4 (a int not null, b int not null);
insert into t1 values (2);
insert into t2 values (1,7),(2,7);
insert into t4 values (4,8),(3,8),(5,9);
insert into t3 values (6),(7),(3);
select * from t3 where a <> any (select b from t2);
select * from t3 where a <> some (select b from t2);
select * from t3 where a = some (select b from t2);
select * from t3 where a = any (select b from t2);

insert into t2 values (100, 5);
select * from t3 where a < any (select b from t2);
select * from t3 where a >= any (select b from t2);
select * from t3 where a < some (select b from t2);
select * from t3 where a >= some (select b from t2);
select * from t3 where a >= some (select b from t2);

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t4;
create table t1 (s1 char(5));
create table t2 (s1 char(5));
insert into t1 values ('a1'),('a2'),('a3');
insert into t2 values ('a1'),('a2');
-- @bvt:issue#3312
select s1, s1 = ANY (SELECT s1 FROM t2) from t1;
select s1, s1 < ANY (SELECT s1 FROM t2) from t1;
select s1, s1 = ANY (SELECT s1 FROM t2) from t1;
-- @bvt:issue

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
create table t2 (a int, b int);
create table t3 (a int);
insert into t3 values (6),(7),(3);
select * from t3 where a >= some (select b from t2);
select * from t3 where a >= some (select b from t2 group by 1);
select * from t3 where NULL >= any (select b from t2);
select * from t3 where NULL >= any (select b from t2 group by 1);
select * from t3 where NULL >= some (select b from t2);
select * from t3 where NULL >= some (select b from t2 group by 1);
insert into t2 values (2,2), (2,1), (3,3), (3,1);

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
CREATE TABLE t1 ( a int, b int );
INSERT INTO t1 VALUES (1,1),(2,2),(3,3);
SELECT a FROM t1 WHERE a > ANY ( SELECT a FROM t1 WHERE b = 2 );
SELECT a FROM t1 WHERE a < ANY ( SELECT a FROM t1 WHERE b = 2 );
SELECT a FROM t1 WHERE a = ANY ( SELECT a FROM t1 WHERE b = 2 );
SELECT a FROM t1 WHERE a >= ANY ( SELECT a FROM t1 WHERE b = 2 );
SELECT a FROM t1 WHERE a <= ANY ( SELECT a FROM t1 WHERE b = 2 );
SELECT a FROM t1 WHERE a <> ANY ( SELECT a FROM t1 WHERE b = 2 );
SELECT a FROM t1 WHERE (1,2) > ANY (SELECT a FROM t1 WHERE b = 2);
SELECT a FROM t1 WHERE a > ANY (SELECT a,2 FROM t1 WHERE b = 2);
SELECT a FROM t1 WHERE (1,2) > ANY (SELECT a,2 FROM t1 WHERE b = 2);
-- error
SELECT a FROM t1 WHERE (1,2) <> ANY (SELECT a,2 FROM t1 WHERE b = 2);
SELECT a FROM t1 WHERE (1,2) = ANY (SELECT a FROM t1 WHERE b = 2);
SELECT a FROM t1 WHERE a = ANY (SELECT a,2 FROM t1 WHERE b = 2);
SELECT a FROM t1 WHERE (1,2) = ANY (SELECT a,2 FROM t1 WHERE b = 2);
SELECT a FROM t1 WHERE (1,2) <> ALL (SELECT a FROM t1 WHERE b = 2);

SELECT a FROM t1 WHERE (a,1) = ANY (SELECT a,1 FROM t1 WHERE b = 2);
SELECT a FROM t1 WHERE (a,1) = ANY (SELECT a,1 FROM t1 HAVING a = 2);
SELECT a FROM t1 WHERE (a,1) = ANY (SELECT a,1 FROM t1 WHERE b = 2 UNION SELECT a,1 FROM t1 WHERE b = 2);
SELECT a FROM t1 WHERE (a,1) = ANY (SELECT a,1 FROM t1 HAVING a = 2 UNION SELECT a,1 FROM t1 HAVING a = 2);

DROP TABLE IF EXISTS t1;
CREATE TABLE t1 ( a double, b double );
INSERT INTO t1 VALUES (1,1),(2,2),(3,3);
SELECT a FROM t1 WHERE a > ANY (SELECT a FROM t1 WHERE b = 2e0);
SELECT a FROM t1 WHERE a < ANY (SELECT a FROM t1 WHERE b = 2e0);
SELECT a FROM t1 WHERE a = ANY (SELECT a FROM t1 WHERE b = 2e0);
SELECT a FROM t1 WHERE a >= ANY (SELECT a FROM t1 WHERE b = 2e0);
SELECT a FROM t1 WHERE a <= ANY (SELECT a FROM t1 WHERE b = 2e0);
SELECT a FROM t1 WHERE a <> ANY (SELECT a FROM t1 WHERE b = 2e0);

DROP TABLE IF EXISTS t1;
CREATE TABLE t1 ( a char(1), b char(1));
INSERT INTO t1 VALUES ('1','1'),('2','2'),('3','3');
SELECT a FROM t1 WHERE a > ANY (SELECT a FROM t1 WHERE b = '2');
SELECT a FROM t1 WHERE a < ANY (SELECT a FROM t1 WHERE b = '2');
SELECT a FROM t1 WHERE a = ANY (SELECT a FROM t1 WHERE b = '2');
SELECT a FROM t1 WHERE a >= ANY (SELECT a FROM t1 WHERE b = '2');
SELECT a FROM t1 WHERE a <= ANY (SELECT a FROM t1 WHERE b = '2');
SELECT a FROM t1 WHERE a <> ANY (SELECT a FROM t1 WHERE b = '2');

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
create table t1 (a1 int);
create table t2 (b1 int);
--error
select * from t1 where a2 > any(select b1 from t2);
select * from t1 where a1 > any(select b1 from t2);

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
create table t1 (s1 char);
insert into t1 values ('1'),('2');
select * from t1 where (s1 < any (select s1 from t1));
-- @bvt:issue#3312
select * from t1 where not (s1 < any (select s1 from t1));
-- @bvt:issue
select * from t1 where (s1+1 = ANY (select s1 from t1));
select * from t1 where NOT(s1+1 = ANY (select s1 from t1));

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1 (s1 CHAR(1));
INSERT INTO t1 VALUES ('a');
SELECT * FROM t1 WHERE 'a' = ANY (SELECT s1 FROM t1);

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

-- @case
-- @desc:test for [any] subquery with with * and mutil tuple
-- @label:bvt
create table t1 (a integer, b integer);
-- @bvt:issue#3312
select (select * from t1) = (select 1,2);
select (select 1,2) = (select * from t1);
select  (1,2) = ANY (select * from t1);
select  (1,2) != ALL (select * from t1);
-- @bvt:issue
DROP TABLE IF EXISTS t1;

-- @case
-- @desc:test for [any] subquery with with without any tables gives wrong results
-- @label:bvt
select 1 from dual where 1 < any (select 2);
select 1 from dual where 2 > any (select 1);

-- @case
-- @desc:test for [any] subquery with group by and having
-- @label:bvt
CREATE TABLE `t1` (
  `numeropost` int(8) unsigned NOT NULL,
  `maxnumrep` int(10) unsigned NOT NULL default 0,
  PRIMARY KEY  (`numeropost`)
);
INSERT INTO t1 (numeropost,maxnumrep) VALUES (40143,1),(43506,2);
CREATE TABLE `t2` (
      `mot` varchar(30) NOT NULL default '',
      `topic` int(8) unsigned NOT NULL default 0,
      `dt` date,
      `pseudo` varchar(35) NOT NULL default ''
    );
INSERT INTO t2 (mot,topic,dt,pseudo) VALUES ('joce','40143','2002-10-22','joce'), ('joce','43506','2002-10-22','joce');
SELECT * from t2 where topic = any (SELECT topic FROM t2 GROUP BY topic);
SELECT * from t2 where topic = any (SELECT topic FROM t2 GROUP BY topic HAVING topic < 4100);
-- @bvt:issue#3307
SELECT * from t2 where topic = any (SELECT SUM(topic) FROM t1);
-- @bvt:issue
SELECT * from t2 where topic <> any (SELECT SUM(topic) FROM t2);
SELECT * from t2 where topic = any (SELECT topic FROM t2 GROUP BY topic HAVING topic < 41000);

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1 ( a int, b int );
INSERT INTO t1 VALUES (1,1),(2,2),(3,3);
SELECT a FROM t1 WHERE a > ANY (SELECT a FROM t1 HAVING a = 2);
SELECT a FROM t1 WHERE a < ANY (SELECT a FROM t1 HAVING a = 2);
SELECT a FROM t1 WHERE a = ANY (SELECT a FROM t1 HAVING a = 2);
SELECT a FROM t1 WHERE a >= ANY (SELECT a FROM t1 HAVING a = 2);
SELECT a FROM t1 WHERE a <= ANY (SELECT a FROM t1 HAVING a = 2);
SELECT a FROM t1 WHERE a <> ANY (SELECT a FROM t1 HAVING a = 2);

SELECT a FROM t1 WHERE a > ANY (SELECT a FROM t1 WHERE b = 2 group by a);
SELECT a FROM t1 WHERE a < ANY (SELECT a FROM t1 WHERE b = 2 group by a);
SELECT a FROM t1 WHERE a = ANY (SELECT a FROM t1 WHERE b = 2 group by a);
SELECT a FROM t1 WHERE a >= ANY (SELECT a FROM t1 WHERE b = 2 group by a);
SELECT a FROM t1 WHERE a <= ANY (SELECT a FROM t1 WHERE b = 2 group by a);
SELECT a FROM t1 WHERE a <> ANY (SELECT a FROM t1 WHERE b = 2 group by a);

SELECT a FROM t1 WHERE a > ANY (SELECT a FROM t1 group by a HAVING a = 2);
SELECT a FROM t1 WHERE a < ANY (SELECT a FROM t1 group by a HAVING a = 2);
SELECT a FROM t1 WHERE a = ANY (SELECT a FROM t1 group by a HAVING a = 2);
SELECT a FROM t1 WHERE a >= ANY (SELECT a FROM t1 group by a HAVING a = 2);
SELECT a FROM t1 WHERE a <= ANY (SELECT a FROM t1 group by a HAVING a = 2);
SELECT a FROM t1 WHERE a <> ANY (SELECT a FROM t1 group by a HAVING a = 2);

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE `t1` ( `a` int(11) default NULL);
insert into t1 values (1);
CREATE TABLE `t2` ( `b` int(11) default NULL, `a` int(11) default NULL);
insert into t2 values (1,2);
select t000.a, count(*) `C` FROM t1 t000 GROUP BY t000.a HAVING count(*) > ALL (SELECT count(*) FROM t2 t001 WHERE t001.a=1);

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1 (
  field1 int NOT NULL,
  field2 int NOT NULL,
  field3 int NOT NULL
);
CREATE TABLE t2 (
  fieldA int NOT NULL,
  fieldB int NOT NULL
);
INSERT INTO t1 VALUES (1,1,1), (1,1,2), (1,2,1), (1,2,2), (1,2,3), (1,3,1);
INSERT INTO t2 VALUES (1,1), (1,2), (1,3);
SELECT field1, field2
  FROM  t1
    GROUP BY field1, field2
      HAVING COUNT(*) < ANY (SELECT fieldB
                               FROM t2 WHERE fieldA = field1);

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1 (
 pk INT NOT NULL PRIMARY KEY,
 number INT
);
INSERT INTO t1 VALUES (8,8);

CREATE TABLE t2 (
 pk INT NOT NULL PRIMARY KEY,
 number INT
);
INSERT INTO t2 VALUES (1,2);
INSERT INTO t2 VALUES (2,8);
INSERT INTO t2 VALUES (3,NULL);
INSERT INTO t2 VALUES (4,166);

SELECT * FROM t1 WHERE t1.number < ANY(SELECT number FROM t2 GROUP BY number);
SELECT * FROM t1 WHERE t1.number < ANY(SELECT number FROM t2);

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1 (a varchar(5), b varchar(10));
INSERT INTO t1 VALUES ('AAA', '5'), ('BBB', '4'), ('BBB', '1'), ('CCC', '2'), ('CCC', '7'), ('AAA', '2'), ('AAA', '4'), ('BBB', '3'), ('AAA', '8');
SELECT * FROM t1 WHERE (a,b) = ANY (SELECT a, max(b) FROM t1 GROUP BY a);
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

-- @case
-- @desc:test for [any] subquery with uion
-- @label:bvt
create table t1 (s1 char);
insert into t1 values ('e');
select * from t1 where 'f' > any (select s1 from t1);

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1 ( a int, b int );
INSERT INTO t1 VALUES (1,1),(2,2),(3,3);
SELECT a FROM t1 WHERE a > ANY (SELECT a FROM t1 WHERE b = 2);
SELECT a FROM t1 WHERE a < ANY (SELECT a FROM t1 WHERE b = 2);
SELECT a FROM t1 WHERE a = ANY (SELECT a FROM t1 WHERE b = 2);
SELECT a FROM t1 WHERE a >= ANY (SELECT a FROM t1 WHERE b = 2);
SELECT a FROM t1 WHERE a <= ANY (SELECT a FROM t1 WHERE b = 2);
SELECT a FROM t1 WHERE a <> ANY (SELECT a FROM t1 WHERE b = 2);
SELECT a FROM t1 WHERE a > ANY (SELECT a FROM t1 HAVING a = 2);
SELECT a FROM t1 WHERE a < ANY (SELECT a FROM t1 HAVING a = 2);
SELECT a FROM t1 WHERE a = ANY (SELECT a FROM t1 HAVING a = 2);
SELECT a FROM t1 WHERE a >= ANY (SELECT a FROM t1 HAVING a = 2);
SELECT a FROM t1 WHERE a <= ANY (SELECT a FROM t1 HAVING a = 2);
SELECT a FROM t1 WHERE a <> ANY (SELECT a FROM t1 HAVING a = 2);

-- @case
-- @desc:test for [any] subquery with NULL
-- @label:bvt
DROP TABLE IF EXISTS t1;
create table t1 (a int);
insert into t1 values (1),(2),(3);
-- @ignore{
update t1 set a=NULL where a=2;
-- @bvt:issue#3312
select 1 > ANY (SELECT * from t1);
select 10 > ANY (SELECT * from t1);
-- @bvt:issue
-- @ignore}

DROP TABLE IF EXISTS t1;
create table t1 (a varchar(20));
insert into t1 values ('A'),('BC'),('DEF');
update t1 set a=NULL where a='BC';
-- @bvt:issue#3312
select 'A' > ANY (SELECT * from t1);
select 'XYZS' > ANY (SELECT * from t1);
-- @bvt:issue

DROP TABLE IF EXISTS t1;
create table t1 (a float);
insert into t1 values (1.5),(2.5),(3.5);
update t1 set a=NULL where a=2.5;
-- @bvt:issue#3312
select 1.5 > ANY (SELECT * from t1);
select 10.5 > ANY (SELECT * from t1);
-- @bvt:issue

DROP TABLE IF EXISTS t1;
create table t1 (s1 int);
insert into t1 values (1),(null);
select * from t1 where s1 < all (select s1 from t1);
-- @bvt:issue#3312
select s1, s1 < all (select s1 from t1) from t1;
-- @bvt:issue

DROP TABLE IF EXISTS t1;
CREATE TABLE t1( a INT );
INSERT INTO t1 VALUES (1),(2);
CREATE TABLE t2( a INT, b INT );
SELECT * FROM t1 WHERE a = ANY ( SELECT 1 UNION ( SELECT 1 UNION SELECT 1 ) );
SELECT * FROM t1 WHERE a = ANY ( ( SELECT 1 UNION SELECT 1 )  UNION SELECT 1 );
SELECT * FROM t1 WHERE a = ANY ( SELECT 1 UNION SELECT 1 UNION SELECT 1 );

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

-- @case
-- @desc:test for [any] subquery with join
-- @label:bvt
CREATE TABLE t1(i INT);
INSERT INTO t1 VALUES (1), (2), (3);
CREATE TABLE t1s(i INT);
INSERT INTO t1s VALUES (10), (20), (30);
CREATE TABLE t2s(i INT);
INSERT INTO t2s VALUES (100), (200), (300);
SELECT * FROM t1
WHERE NOT t1.I = ANY
(
  SELECT t2s.i
  FROM
  t1s LEFT OUTER JOIN t2s ON t2s.i = t1s.i
  HAVING t2s.i = 999
);
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t1s;
DROP TABLE IF EXISTS t2s;









