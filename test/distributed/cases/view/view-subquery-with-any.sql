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
create view v1 as select * from t3 where a <> any (select b from t2);
create view v2 as select * from t3 where a <> some (select b from t2);
create view v3 as select * from t3 where a = some (select b from t2);
create view v4 as select * from t3 where a = any (select b from t2);
select * from v1;
select * from v2;
select * from v3;
select * from v4;

drop view v1;
drop view v2;
drop view v3;
drop view v4;


insert into t2 values (100, 5);
select * from t3 where a < any (select b from t2);
select * from t3 where a >= any (select b from t2);
select * from t3 where a < some (select b from t2);
select * from t3 where a >= some (select b from t2);
select * from t3 where a >= some (select b from t2);
create view v1 as select * from t3 where a < any (select b from t2);
create view v2 as select * from t3 where a >= any (select b from t2);
create view v3 as select * from t3 where a < some (select b from t2);
create view v4 as select * from t3 where a >= some (select b from t2);
create view v5 as select * from t3 where a >= some (select b from t2);
select * from v1;
select * from v2;
select * from v3;
select * from v4;
select * from v5;

drop view v1;
drop view v2;
drop view v3;
drop view v4;
drop view v5;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t4;
create table t1 (s1 char(5));
create table t2 (s1 char(5));
insert into t1 values ('a1'),('a2'),('a3');
insert into t2 values ('a1'),('a2');
select s1, s1 = ANY (SELECT s1 FROM t2) from t1;
select s1, s1 < ANY (SELECT s1 FROM t2) from t1;
select s1, s1 = ANY (SELECT s1 FROM t2) from t1;
create view v1 as select s1, s1 = ANY (SELECT s1 FROM t2) from t1;
create view v2 as select s1, s1 < ANY (SELECT s1 FROM t2) from t1;
create view v3 as select s1, s1 = ANY (SELECT s1 FROM t2) from t1;
select * from v1;
select * from v2;
select * from v3;
drop view v1;
drop view v2;
drop view v3;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

create table t2 (a int, b int);
create table t3 (a int);
insert into t3 values (6),(7),(3);
insert into t2 values (2,2), (2,1), (3,3), (3,1);
select * from t3 where a >= some (select b from t2);
select * from t3 where a >= some (select b from t2 group by 1);
select * from t3 where NULL >= any (select b from t2);
select * from t3 where NULL >= any (select b from t2 group by 1);
select * from t3 where NULL >= some (select b from t2);
select * from t3 where NULL >= some (select b from t2 group by 1);
create view v1 as select * from t3 where a >= some (select b from t2);
create view v2 as select * from t3 where a >= some (select b from t2 group by 1);
create view v3 as select * from t3 where NULL >= any (select b from t2);
create view v4 as select * from t3 where NULL >= any (select b from t2 group by 1);
create view v5 as select * from t3 where NULL >= some (select b from t2);
create view v6 as select * from t3 where NULL >= some (select b from t2 group by 1);
select * from v1;
select * from v2;
select * from v3;
select * from v4;
select * from v5;
select * from v6;

drop view v1;
drop view v2;
drop view v3;
drop view v4;
drop view v5;
drop view v6;


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

-- 
create view v1 as SELECT a FROM t1 WHERE a > ANY ( SELECT a FROM t1 WHERE b = 2 );
create view v2 as SELECT a FROM t1 WHERE a < ANY ( SELECT a FROM t1 WHERE b = 2 );
create view v3 as SELECT a FROM t1 WHERE a = ANY ( SELECT a FROM t1 WHERE b = 2 );
create view v4 as SELECT a FROM t1 WHERE a >= ANY ( SELECT a FROM t1 WHERE b = 2 );
create view v5 as SELECT a FROM t1 WHERE a <= ANY ( SELECT a FROM t1 WHERE b = 2 );
create view v6 as SELECT a FROM t1 WHERE a <> ANY ( SELECT a FROM t1 WHERE b = 2 );
create view v7 as SELECT a FROM t1 WHERE (1,2) > ANY (SELECT a FROM t1 WHERE b = 2);
create view v8 as SELECT a FROM t1 WHERE a > ANY (SELECT a,2 FROM t1 WHERE b = 2);
create view v9 as SELECT a FROM t1 WHERE (1,2) > ANY (SELECT a,2 FROM t1 WHERE b = 2);
-- error
create view v10 as SELECT a FROM t1 WHERE (1,2) <> ANY (SELECT a,2 FROM t1 WHERE b = 2);
create view v11 as SELECT a FROM t1 WHERE (1,2) = ANY (SELECT a FROM t1 WHERE b = 2);
create view v12 as SELECT a FROM t1 WHERE a = ANY (SELECT a,2 FROM t1 WHERE b = 2);
create view v13 as SELECT a FROM t1 WHERE (1,2) = ANY (SELECT a,2 FROM t1 WHERE b = 2);
create view v14 as SELECT a FROM t1 WHERE (1,2) <> ALL (SELECT a FROM t1 WHERE b = 2);

create view v15 as SELECT a FROM t1 WHERE (a,1) = ANY (SELECT a,1 FROM t1 WHERE b = 2);
create view v16 as SELECT a FROM t1 WHERE (a,1) = ANY (SELECT a,1 FROM t1 HAVING a = 2);
create view v17 as SELECT a FROM t1 WHERE (a,1) = ANY (SELECT a,1 FROM t1 WHERE b = 2 UNION SELECT a,1 FROM t1 WHERE b = 2);
create view v18 as SELECT a FROM t1 WHERE (a,1) = ANY (SELECT a,1 FROM t1 HAVING a = 2 UNION SELECT a,1 FROM t1 HAVING a = 2);

select * from v1;
select * from v2;
select * from v3;
select * from v4;
select * from v5;
select * from v6;
select * from v7;
select * from v8;
select * from v9;
select * from v10;
select * from v11;
select * from v12;
select * from v13;
select * from v14;
select * from v15;
select * from v16;
select * from v17;
select * from v18;

drop view v1;
drop view v2;
drop view v3;
drop view v4;
drop view v5;
drop view v6;
drop view v7;
drop view v8;
drop view v9;
drop view v10;
drop view v11;
drop view v12;
drop view v13;
drop view v14;
drop view v15;
drop view v16;
drop view v17;
drop view v18;

-- ------

DROP TABLE IF EXISTS t1;
CREATE TABLE t1 ( a double, b double );
INSERT INTO t1 VALUES (1,1),(2,2),(3,3);
SELECT a FROM t1 WHERE a > ANY (SELECT a FROM t1 WHERE b = 2e0);
SELECT a FROM t1 WHERE a < ANY (SELECT a FROM t1 WHERE b = 2e0);
SELECT a FROM t1 WHERE a = ANY (SELECT a FROM t1 WHERE b = 2e0);
SELECT a FROM t1 WHERE a >= ANY (SELECT a FROM t1 WHERE b = 2e0);
SELECT a FROM t1 WHERE a <= ANY (SELECT a FROM t1 WHERE b = 2e0);
SELECT a FROM t1 WHERE a <> ANY (SELECT a FROM t1 WHERE b = 2e0);

create view v1 as SELECT a FROM t1 WHERE a > ANY (SELECT a FROM t1 WHERE b = 2e0);
create view v2 as SELECT a FROM t1 WHERE a < ANY (SELECT a FROM t1 WHERE b = 2e0);
create view v3 as SELECT a FROM t1 WHERE a = ANY (SELECT a FROM t1 WHERE b = 2e0);
create view v4 as SELECT a FROM t1 WHERE a >= ANY (SELECT a FROM t1 WHERE b = 2e0);
create view v5 as SELECT a FROM t1 WHERE a <= ANY (SELECT a FROM t1 WHERE b = 2e0);
create view v6 as SELECT a FROM t1 WHERE a <> ANY (SELECT a FROM t1 WHERE b = 2e0);
select * from v1;
select * from v2;
select * from v3;
select * from v4;
select * from v5;
select * from v6;

drop view v1;
drop view v2;
drop view v3;
drop view v4;
drop view v5;
drop view v6;



DROP TABLE IF EXISTS t1;
CREATE TABLE t1 ( a char(1), b char(1));
INSERT INTO t1 VALUES ('1','1'),('2','2'),('3','3');
SELECT a FROM t1 WHERE a > ANY (SELECT a FROM t1 WHERE b = '2');
SELECT a FROM t1 WHERE a < ANY (SELECT a FROM t1 WHERE b = '2');
SELECT a FROM t1 WHERE a = ANY (SELECT a FROM t1 WHERE b = '2');
SELECT a FROM t1 WHERE a >= ANY (SELECT a FROM t1 WHERE b = '2');
SELECT a FROM t1 WHERE a <= ANY (SELECT a FROM t1 WHERE b = '2');
SELECT a FROM t1 WHERE a <> ANY (SELECT a FROM t1 WHERE b = '2');

create view v1 as SELECT a FROM t1 WHERE a > ANY (SELECT a FROM t1 WHERE b = '2');
create view v2 as SELECT a FROM t1 WHERE a < ANY (SELECT a FROM t1 WHERE b = '2');
create view v3 as SELECT a FROM t1 WHERE a = ANY (SELECT a FROM t1 WHERE b = '2');
create view v4 as SELECT a FROM t1 WHERE a >= ANY (SELECT a FROM t1 WHERE b = '2');
create view v5 as SELECT a FROM t1 WHERE a <= ANY (SELECT a FROM t1 WHERE b = '2');
create view v6 as SELECT a FROM t1 WHERE a <> ANY (SELECT a FROM t1 WHERE b = '2');
select * from v1;
select * from v2;
select * from v3;
select * from v4;
select * from v5;
select * from v6;

drop view v1;
drop view v2;
drop view v3;
drop view v4;
drop view v5;
drop view v6;


DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
create table t1 (a1 int);
create table t2 (b1 int);
--  error
select * from t1 where a2 > any(select b1 from t2);
select * from t1 where a1 > any(select b1 from t2);
create view v1 as select * from t1 where a2 > any(select b1 from t2);
create view v2 as select * from t1 where a1 > any(select b1 from t2);
select * from v1;
select * from v2;
drop view v1;
drop view v2;


DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
create table t1 (s1 char);
insert into t1 values ('1'),('2');
select * from t1 where (s1 < any (select s1 from t1));
create view v1 as select * from t1 where (s1 < any (select s1 from t1));
select * from t1 where not (s1 < any (select s1 from t1));
create view v2 as select * from t1 where not (s1 < any (select s1 from t1));
select * from t1 where (s1+1 = ANY (select s1 from t1));
select * from t1 where NOT(s1+1 = ANY (select s1 from t1));

create view v3 as select * from t1 where (s1+1 = ANY (select s1 from t1));
create view v4 as select * from t1 where NOT(s1+1 = ANY (select s1 from t1));
select * from v1;
select * from v2;
select * from v3;
select * from v4;

drop view v1;
drop view v2;
drop view v3;
drop view v4;


DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1 (s1 CHAR(1));
INSERT INTO t1 VALUES ('a');
SELECT * FROM t1 WHERE 'a' = ANY (SELECT s1 FROM t1);
create view v1 as SELECT * FROM t1 WHERE 'a' = ANY (SELECT s1 FROM t1);
select * from v1;
drop view v1;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

-- @case
-- @desc:test for [any] subquery with with * and mutil tuple
-- @label:bvt
create table t1 (a integer, b integer);
-- @bvt:issue#7691
select (select * from t1) = (select 1,2);
select (select 1,2) = (select * from t1);
-- @bvt:issue
select  (1,2) = ANY (select * from t1);
select  (1,2) != ALL (select * from t1);
-- @bvt:issue#7691
create view v1 as select (select * from t1) = (select 1,2);
create view v2 as select (select 1,2) = (select * from t1);
-- @bvt:issue
create view v3 as select  (1,2) = ANY (select * from t1);
create view v4 as select  (1,2) != ALL (select * from t1);

-- @bvt:issue#7691
select * from v1;
select * from v2;
-- @bvt:issue
select * from v3;
select * from v4;

-- @bvt:issue#7691
drop view v1;
drop view v2;
-- @bvt:issue
drop view v3;
drop view v4;

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

create view v1 as SELECT * from t2 where topic = any (SELECT topic FROM t2 GROUP BY topic);
create view v2 as SELECT * from t2 where topic = any (SELECT topic FROM t2 GROUP BY topic HAVING topic < 4100);
create view v3 as SELECT * from t2 where topic <> any (SELECT SUM(topic) FROM t2);
create view v4 as SELECT * from t2 where topic = any (SELECT topic FROM t2 GROUP BY topic HAVING topic < 41000);

select * from v1;
select * from v2;
select * from v3;
select * from v4;

drop view v1;
drop view v2;
drop view v3;
drop view v4;


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

create view v1 as SELECT a FROM t1 WHERE a > ANY (SELECT a FROM t1 HAVING a = 2);
create view v2 as SELECT a FROM t1 WHERE a < ANY (SELECT a FROM t1 HAVING a = 2);
create view v3 as SELECT a FROM t1 WHERE a = ANY (SELECT a FROM t1 HAVING a = 2);
create view v4 as SELECT a FROM t1 WHERE a >= ANY (SELECT a FROM t1 HAVING a = 2);
create view v5 as SELECT a FROM t1 WHERE a <= ANY (SELECT a FROM t1 HAVING a = 2);
create view v6 as SELECT a FROM t1 WHERE a <> ANY (SELECT a FROM t1 HAVING a = 2);

create view v7 as SELECT a FROM t1 WHERE a > ANY (SELECT a FROM t1 WHERE b = 2 group by a);
create view v8 as SELECT a FROM t1 WHERE a < ANY (SELECT a FROM t1 WHERE b = 2 group by a);
create view v9 as SELECT a FROM t1 WHERE a = ANY (SELECT a FROM t1 WHERE b = 2 group by a);
create view v10 as SELECT a FROM t1 WHERE a >= ANY (SELECT a FROM t1 WHERE b = 2 group by a);
create view v11 as SELECT a FROM t1 WHERE a <= ANY (SELECT a FROM t1 WHERE b = 2 group by a);
create view v12 as SELECT a FROM t1 WHERE a <> ANY (SELECT a FROM t1 WHERE b = 2 group by a);

create view v13 as SELECT a FROM t1 WHERE a > ANY (SELECT a FROM t1 group by a HAVING a = 2);
create view v14 as SELECT a FROM t1 WHERE a < ANY (SELECT a FROM t1 group by a HAVING a = 2);
create view v15 as SELECT a FROM t1 WHERE a = ANY (SELECT a FROM t1 group by a HAVING a = 2);
create view v16 as SELECT a FROM t1 WHERE a >= ANY (SELECT a FROM t1 group by a HAVING a = 2);
create view v17 as SELECT a FROM t1 WHERE a <= ANY (SELECT a FROM t1 group by a HAVING a = 2);
create view v18 as SELECT a FROM t1 WHERE a <> ANY (SELECT a FROM t1 group by a HAVING a = 2);

select * from v1;
select * from v2;
select * from v3;
select * from v4;
select * from v5;
select * from v6;
select * from v7;
select * from v8;
select * from v9;
select * from v10;
select * from v11;
select * from v12;
select * from v13;
select * from v14;
select * from v15;
select * from v16;
select * from v17;
select * from v18;

drop view v1;
drop view v2;
drop view v3;
drop view v4;
drop view v5;
drop view v6;
drop view v7;
drop view v8;
drop view v9;
drop view v10;
drop view v11;
drop view v12;
drop view v13;
drop view v14;
drop view v15;
drop view v16;
drop view v17;
drop view v18;

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
create view v1 as SELECT * FROM t1 WHERE t1.number < ANY(SELECT number FROM t2 GROUP BY number);
create view v2 as SELECT * FROM t1 WHERE t1.number < ANY(SELECT number FROM t2);
select * from v1;
select * from v2;
drop view v1;
drop view v2;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1 (a varchar(5), b varchar(10));
INSERT INTO t1 VALUES ('AAA', '5'), ('BBB', '4'), ('BBB', '1'), ('CCC', '2'), ('CCC', '7'), ('AAA', '2'), ('AAA', '4'), ('BBB', '3'), ('AAA', '8');
SELECT * FROM t1 WHERE (a,b) = ANY (SELECT a, max(b) FROM t1 GROUP BY a);
create view v1 as SELECT * FROM t1 WHERE (a,b) = ANY (SELECT a, max(b) FROM t1 GROUP BY a);
select * from v1;
drop view v1;
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

create view v1 as SELECT a FROM t1 WHERE a > ANY (SELECT a FROM t1 WHERE b = 2);
create view v2 as SELECT a FROM t1 WHERE a < ANY (SELECT a FROM t1 WHERE b = 2);
create view v3 as SELECT a FROM t1 WHERE a = ANY (SELECT a FROM t1 WHERE b = 2);
create view v4 as SELECT a FROM t1 WHERE a >= ANY (SELECT a FROM t1 WHERE b = 2);
create view v5 as SELECT a FROM t1 WHERE a <= ANY (SELECT a FROM t1 WHERE b = 2);
create view v6 as SELECT a FROM t1 WHERE a <> ANY (SELECT a FROM t1 WHERE b = 2);
create view v7 as SELECT a FROM t1 WHERE a > ANY (SELECT a FROM t1 HAVING a = 2);
create view v8 as SELECT a FROM t1 WHERE a < ANY (SELECT a FROM t1 HAVING a = 2);
create view v9 as SELECT a FROM t1 WHERE a = ANY (SELECT a FROM t1 HAVING a = 2);
create view v10 as SELECT a FROM t1 WHERE a >= ANY (SELECT a FROM t1 HAVING a = 2);
create view v11 as SELECT a FROM t1 WHERE a <= ANY (SELECT a FROM t1 HAVING a = 2);
create view v12 as SELECT a FROM t1 WHERE a <> ANY (SELECT a FROM t1 HAVING a = 2);

select * from v1;
select * from v2;
select * from v3;
select * from v4;
select * from v5;
select * from v6;
select * from v7;
select * from v8;
select * from v9;
select * from v10;
select * from v11;
select * from v12;

drop view v1;
drop view v2;
drop view v3;
drop view v4;
drop view v5;
drop view v6;
drop view v7;
drop view v8;
drop view v9;
drop view v10;
drop view v11;
drop view v12;


-- @case
-- @desc:test for [any] subquery with NULL
-- @label:bvt
DROP TABLE IF EXISTS t1;
create table t1 (a int);
insert into t1 values (1),(2),(3);
update t1 set a=NULL where a=2;
select 1 > ANY (SELECT * from t1);
select 10 > ANY (SELECT * from t1);
create view v1 as select 1 > ANY (SELECT * from t1);
create view v2 as select 10 > ANY (SELECT * from t1);
select * from v1;
select * from v2;
drop view v1;
drop view v2;

DROP TABLE IF EXISTS t1;
create table t1 (a varchar(20));
insert into t1 values ('A'),('BC'),('DEF');
update t1 set a=NULL where a='BC';
select 'A' > ANY (SELECT * from t1);
select 'XYZS' > ANY (SELECT * from t1);

create view v1 as select 'A' > ANY (SELECT * from t1);
create view v2 as select 'XYZS' > ANY (SELECT * from t1);
select * from v1;
select * from v2;
drop view v1;
drop view v2;

DROP TABLE IF EXISTS t1;
create table t1 (a float);
insert into t1 values (1.5),(2.5),(3.5);
update t1 set a=NULL where a=2.5;
select 1.5 > ANY (SELECT * from t1);
select 10.5 > ANY (SELECT * from t1);
create view v1 as select 1.5 > ANY (SELECT * from t1);
create view v2 as select 10.5 > ANY (SELECT * from t1);
select * from v1;
select * from v2;
drop view v1;
drop view v2;

DROP TABLE IF EXISTS t1;
create table t1 (s1 int);
insert into t1 values (1),(null);
select * from t1 where s1 < all (select s1 from t1);
create view v1 as select * from t1 where s1 < all (select s1 from t1);
select * from t1 where s1 < all (select s1 from t1);
create view v2 as select * from t1 where s1 < all (select s1 from t1);
select * from v1;
select * from v2;
drop view v1;
drop view v2;

DROP TABLE IF EXISTS t1;
CREATE TABLE t1( a INT );
INSERT INTO t1 VALUES (1),(2);
CREATE TABLE t2( a INT, b INT );
SELECT * FROM t1 WHERE a = ANY ( SELECT 1 UNION ( SELECT 1 UNION SELECT 1 ) );
SELECT * FROM t1 WHERE a = ANY ( ( SELECT 1 UNION SELECT 1 )  UNION SELECT 1 );
SELECT * FROM t1 WHERE a = ANY ( SELECT 1 UNION SELECT 1 UNION SELECT 1 );
create view v1 as SELECT * FROM t1 WHERE a = ANY ( SELECT 1 UNION ( SELECT 1 UNION SELECT 1 ) );
create view v2 as SELECT * FROM t1 WHERE a = ANY ( ( SELECT 1 UNION SELECT 1 )  UNION SELECT 1 );
create view v3 as SELECT * FROM t1 WHERE a = ANY ( SELECT 1 UNION SELECT 1 UNION SELECT 1 );
select * from v1;
select * from v2;
select * from v3;
drop view v1;
drop view v2;
drop view v3;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;








