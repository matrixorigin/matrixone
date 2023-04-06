-- @suit

-- @case
-- @desc:test for [in] subquery with constant operand
-- @label:bvt
SELECT 1 IN (SELECT 1);

create view v1 as SELECT 1 IN (SELECT 1);
select * from v1;
drop view v1;
SELECT 1 FROM (SELECT 1 as a) b WHERE 1 IN (SELECT (SELECT a));
create view v1 as SELECT 1 FROM (SELECT 1 as a) b WHERE 1 IN (SELECT (SELECT a));
select * from v1;
drop view v1;
SELECT 1 FROM (SELECT 1 as a) b WHERE 1 not IN (SELECT (SELECT a));
create view v1 as SELECT 1 FROM (SELECT 1 as a) b WHERE 1 not IN (SELECT (SELECT a));
select * from v1;
drop view v1;
SELECT * FROM (SELECT 1 as id) b WHERE id IN (SELECT * FROM (SELECT 1 as id) c ORDER BY id);
SELECT * FROM (SELECT 1) a  WHERE 1 IN (SELECT 1,1);
SELECT * FROM (SELECT 1) b WHERE 1 IN (SELECT *);
create view v1 as SELECT * FROM (SELECT 1 as id) b WHERE id IN (SELECT * FROM (SELECT 1 as id) c ORDER BY id);
create view v2 as SELECT * FROM (SELECT 1) a  WHERE 1 IN (SELECT 1,1);
create view v3 as SELECT * FROM (SELECT 1) b WHERE 1 IN (SELECT *);
select * from v1;
select * from v2;
select * from v3;
drop view v1;
drop view v2;
drop view v3;
SELECT ((0,1) NOT IN (SELECT NULL,1)) IS NULL;
create view v1 as SELECT ((0,1) NOT IN (SELECT NULL,1)) IS NULL;
select * from v1;
drop view v1;

-- @case
-- @desc:test for [in] subquery with operand-is-column
-- @label:bvt

drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
create table t1 (a int);
create table t2 (a int, b int);
create table t3 (a int);
create table t4 (a int not null, b int not null);
insert into t1 values (2);
insert into t2 values (1,7),(2,7);
insert into t4 values (4,8),(3,8),(5,9);
insert into t2 values (100, 5);
select * from t3 where a in (select b from t2);
select * from t3 where a in (select b from t2 where b > 7);
select * from t3 where a not in (select b from t2);
create view v1 as select * from t3 where a in (select b from t2);
create view v2 as select * from t3 where a in (select b from t2 where b > 7);
create view v3 as select * from t3 where a not in (select b from t2);
select * from v1;
select * from v2;
select * from v3;
drop view v1;
drop view v2;
drop view v3;
SELECT 0 IN (SELECT 1 FROM t1 a);
create view v1 as SELECT 0 IN (SELECT 1 FROM t1 a);
select * from v1;
drop view v1;
select * from t3 where a in (select a,b from t2);
select * from t3 where a in (select * from t2);
create view v1 as select * from t3 where a in (select a,b from t2);
create view v2 as select * from t3 where a in (select * from t2);
select * from v1;
select * from v2;
drop view v1;
drop view v2;

drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
create table t1 (s1 char(5), index s1(s1));
create table t2 (s1 char(5), index s1(s1));
insert into t1 values ('a1'),('a2'),('a3');
insert into t2 values ('a1'),('a2');
select s1, s1 NOT IN (SELECT s1 FROM t2) from t1;
select s1, s1 NOT IN (SELECT s1 FROM t2 WHERE s1 < 'a2') from t1;
create view v1 as select s1, s1 NOT IN (SELECT s1 FROM t2) from t1;
create view v2 as select s1, s1 NOT IN (SELECT s1 FROM t2 WHERE s1 < 'a2') from t1;
select * from v1;
select * from v2;
drop view v1;
drop view v2;

drop table if exists t1;
drop table if exists t2;
create table t1(val varchar(10));
insert into t1 values ('aaa'), ('bbb'),('eee'),('mmm'),('ppp');
select count(*) from t1 as w1 where w1.val in (select w2.val from t1 as w2 where w2.val like 'm%') and w1.val in (select w3.val from t1 as w3 where w3.val like 'e%');

create view v1 as select count(*) from t1 as w1 where w1.val in (select w2.val from t1 as w2 where w2.val like 'm%') and w1.val in (select w3.val from t1 as w3 where w3.val like 'e%');
select * from v1;
drop view v1;

DROP TABLE IF EXISTS t1;
create table t1 (id int not null, text varchar(20) not null default '', primary key (id));
insert into t1 (id, text) values (1, 'text1'), (2, 'text2'), (3, 'text3'), (4, 'text4'), (5, 'text5'), (6, 'text6'), (7, 'text7'), (8, 'text8'), (9, 'text9'), (10, 'text10'), (11, 'text11'), (12, 'text12');
select * from t1 where id not in (select id from t1 where id < 8);
create view v1 as select * from t1 where id not in (select id from t1 where id < 8);
select * from v1;
drop view v1;

drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
CREATE TABLE t1 (a int);
CREATE TABLE t2 (a int, b int);
CREATE TABLE t3 (b int NOT NULL);
INSERT INTO t1 VALUES (1), (2), (3), (4);
INSERT INTO t2 VALUES (1,10), (3,30);
select * from t1 where t1.a in (SELECT t1.a FROM t1 LEFT JOIN t2 ON t2.a=t1.a);
SELECT * FROM t2 LEFT JOIN t3 ON t2.b=t3.b WHERE t3.b IS NOT NULL OR t2.a > 10;
SELECT * FROM t1 WHERE t1.a NOT IN (SELECT a FROM t2 LEFT JOIN t3 ON t2.b=t3.b WHERE t3.b IS NOT NULL OR t2.a > 10);
create view v1 as select * from t1 where t1.a in (SELECT t1.a FROM t1 LEFT JOIN t2 ON t2.a=t1.a);
create view v2 as SELECT * FROM t2 LEFT JOIN t3 ON t2.b=t3.b WHERE t3.b IS NOT NULL OR t2.a > 10;
create view v3 as SELECT * FROM t1 WHERE t1.a NOT IN (SELECT a FROM t2 LEFT JOIN t3 ON t2.b=t3.b WHERE t3.b IS NOT NULL OR t2.a > 10);
select * from v1;
select * from v2;
select * from v3;
drop view v1;
drop view v2;
drop view v3;

drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
CREATE TABLE t1(a int, INDEX (a));
INSERT INTO t1 VALUES (1), (3), (5), (7);
INSERT INTO t1 VALUES (NULL);
CREATE TABLE t2(a int);
INSERT INTO t2 VALUES (1),(2),(3);
SELECT a, a IN (SELECT a FROM t1) FROM t2;
create view v1 as SELECT a, a IN (SELECT a FROM t1) FROM t2;
select * from v1;
drop view v1;

drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
CREATE table t1 ( c1 int );
INSERT INTO t1 VALUES ( 1 );
INSERT INTO t1 VALUES ( 2 );
INSERT INTO t1 VALUES ( 3 );
CREATE TABLE t2 ( c2 int );
INSERT INTO t2 VALUES ( 1 );
INSERT INTO t2 VALUES ( 4 );
INSERT INTO t2 VALUES ( 5 );
SELECT * FROM t1 LEFT JOIN t2 ON c1 = c2 WHERE c2 IN (1);
SELECT * FROM t1 LEFT JOIN t2 ON c1 = c2 WHERE c2 IN ( SELECT c2 FROM t2 WHERE c2 IN ( 1 ) );
create view v1 as SELECT * FROM t1 LEFT JOIN t2 ON c1 = c2 WHERE c2 IN (1);
create view v2 as SELECT * FROM t1 LEFT JOIN t2 ON c1 = c2 WHERE c2 IN ( SELECT c2 FROM t2 WHERE c2 IN ( 1 ) );
select * from v1;
select * from v2;
drop view v1;
drop view v2;

drop table if exists t1;
drop table if exists t2;
DROP TABLE IF EXISTS c;
CREATE TABLE `c` (
  `int_nokey` int(11) NOT NULL,
  `int_key` int(11) NOT NULL
);
INSERT INTO `c` VALUES (9,9), (0,0), (8,6), (3,6), (7,6), (0,4),
(1,7), (9,4), (0,8), (9,4), (0,7), (5,5), (0,0), (8,5), (8,7),
(5,2), (1,8), (7,0), (0,9), (9,5);
SELECT * FROM c WHERE `int_key` IN (SELECT `int_nokey`);
create view v1 as SELECT * FROM c WHERE `int_key` IN (SELECT `int_nokey`);
select * from v1;
drop view v1;
DROP TABLE IF EXISTS c;

drop table if exists t1;
drop table if exists t2;
CREATE TABLE t1(c INT);
CREATE TABLE t2(a INT, b INT);
INSERT INTO t2 VALUES (1, 10), (2, NULL);
INSERT INTO t1 VALUES (1), (3);
SELECT * FROM t2 WHERE b NOT IN (SELECT max(t.c) FROM t1, t1 t WHERE t.c>10);
create view v1 as SELECT * FROM t2 WHERE b NOT IN (SELECT max(t.c) FROM t1, t1 t WHERE t.c>10);
select * from v1;
drop view v1;
drop table if exists t1;
drop table if exists t2;

CREATE TABLE t1 (
  a1 char(8) DEFAULT NULL,
  a2 char(8) DEFAULT NULL
);
CREATE TABLE t2 (
  b1 char(8) DEFAULT NULL,
  b2 char(8) DEFAULT NULL
);
INSERT INTO t1 VALUES
 ('1 - 00', '2 - 00'),('1 - 01', '2 - 01'),('1 - 02', '2 - 02');
INSERT INTO t2 VALUES
 ('1 - 01', '2 - 01'),('1 - 01', '2 - 01'),
 ('1 - 02', '2 - 02'),('1 - 02', '2 - 02'),('1 - 03', '2 - 03');
SELECT * FROM t2 WHERE b1 NOT IN ('1 - 00', '1 - 01', '1 - 02');
create view v1 as SELECT * FROM t2 WHERE b1 NOT IN ('1 - 00', '1 - 01', '1 - 02');
select * from v1;
drop view v1;
drop table if exists t1;
drop table if exists t2;

-- @case
-- @desc:test for [in] subquery with groupby,having,
-- @label:bvt
drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
drop table if exists t4;
CREATE TABLE `t1` (
  `numeropost` int(8) unsigned NOT NULL,
  `maxnumrep` int(10) unsigned NOT NULL default 0,
  PRIMARY KEY  (`numeropost`)
) ;

INSERT INTO t1 (numeropost,maxnumrep) VALUES (40143,1),(43506,2);

CREATE TABLE `t2` (
      `mot` varchar(30) NOT NULL default '',
      `topic` int(8) unsigned NOT NULL default 0,
      `dt` date,
      `pseudo` varchar(35) NOT NULL default '',
       PRIMARY KEY  (`topic`)
    ) ;

INSERT INTO t2 (mot,topic,dt,pseudo) VALUES ('joce','40143','2002-10-22','joce'), ('joce','43506','2002-10-22','joce');
SELECT * from t2 where topic IN (SELECT topic FROM t2 GROUP BY topic);
SELECT * from t2 where topic IN (SELECT topic FROM t2 GROUP BY topic HAVING topic < 4100);
-- @bvt:issue#3307
SELECT * from t2 where topic IN (SELECT SUM(topic) FROM t1);
create view v3 as SELECT * from t2 where topic IN (SELECT SUM(topic) FROM t1);
select * from v3;
-- @bvt:issue
SELECT * from t2 where topic IN (SELECT topic FROM t2 GROUP BY topic HAVING topic < 41000);
SELECT * from t2 where topic NOT IN (SELECT topic FROM t2 GROUP BY topic HAVING topic < 41000);
SELECT * FROM t2 WHERE mot IN (SELECT 'joce');
create view v1 as SELECT * from t2 where topic IN (SELECT topic FROM t2 GROUP BY topic);
create view v2 as SELECT * from t2 where topic IN (SELECT topic FROM t2 GROUP BY topic HAVING topic < 4100);
create view v4 as SELECT * from t2 where topic IN (SELECT topic FROM t2 GROUP BY topic HAVING topic < 41000);
create view v5 as SELECT * from t2 where topic NOT IN (SELECT topic FROM t2 GROUP BY topic HAVING topic < 41000);
create view v6 as SELECT * FROM t2 WHERE mot IN (SELECT 'joce');
select * from v1;
select * from v2;
select * from v4;
select * from v5;
select * from v6;
drop view v1;
drop view v2;
drop view v3;
drop view v4;
drop view v5;
drop view v6;

drop table if exists t1;
drop table if exists t2;
create table t1 (a int);
create table t2 (a int);
insert into t1 values (1),(2);
insert into t2 values (0),(1),(2),(3);
select a from t2 where a in (select a from t1);
select a from t2 having a in (select a from t1);
create view v1 as select a from t2 where a in (select a from t1);
create view v2 as select a from t2 having a in (select a from t1);
select * from v1;
select * from v2;
drop view v1;
drop view v2;

drop table if exists t1;
drop table if exists t2;
create table t1 (oref int, grp int, ie int) ;
insert into t1 (oref, grp, ie) values(1, 1, 1),(1, 1, 1), (1, 2, NULL),(2, 1, 3),(3, 1, 4),(3, 2, NULL);
create table t2 (oref int, a int);
insert into t2 values(1, 1),(2, 2),(3, 3), (4, NULL),(2, NULL);
create table t3 (a int);
insert into t3 values (NULL), (NULL);
select a, oref, a in (select max(ie) from t1 where oref=t2.oref group by grp) Z from t2;
select a, oref from t2 where a in (select max(ie) from t1 where oref=t2.oref group by grp);
select a, oref, a in (
  select max(ie) from t1 where oref=t2.oref group by grp union
  select max(ie) from t1 where oref=t2.oref group by grp
  ) Z from t2;
select a in (select max(ie) from t1 where oref=4 group by grp) from t3;

create view v1 as select a, oref, a in (select max(ie) from t1 where oref=t2.oref group by grp) Z from t2;
create view v2 as select a, oref from t2 where a in (select max(ie) from t1 where oref=t2.oref group by grp);
create view v3 as select a, oref, a in (
  select max(ie) from t1 where oref=t2.oref group by grp union
  select max(ie) from t1 where oref=t2.oref group by grp
  ) Z from t2;
create view v4 as select a in (select max(ie) from t1 where oref=4 group by grp) from t3;
select * from v1;
select * from v2;
select * from v3;
select * from v4;
drop view v1;
drop view v2;
drop view v3;
drop view v4;


-- @case
-- @desc:test for [in] subquery with UNION
-- @label:bvt
drop table if exists t1;
drop table if exists t2;
CREATE TABLE t2 (id int(11) default NULL);
INSERT INTO t2 VALUES (1),(2);
SELECT * FROM t2 WHERE id IN (SELECT 1);
create view v1 as SELECT * FROM t2 WHERE id IN (SELECT 1);
select * from v1;
select * from v1;
-- @bvt:issue#4354
SELECT * FROM t2 WHERE id IN (SELECT 1 UNION SELECT 3);
SELECT * FROM t2 WHERE id IN (SELECT 1+(select 1));
SELECT * FROM t2 WHERE id IN (SELECT 5 UNION SELECT 3);
SELECT * FROM t2 WHERE id IN (SELECT 5 UNION SELECT 2);
SELECT * FROM t2 WHERE id NOT IN (SELECT 5 UNION SELECT 2);
create view v2 as SELECT * FROM t2 WHERE id IN (SELECT 1 UNION SELECT 3);
create view v3 as SELECT * FROM t2 WHERE id IN (SELECT 1+(select 1));
create view v4 as SELECT * FROM t2 WHERE id IN (SELECT 5 UNION SELECT 3);
create view v5 as SELECT * FROM t2 WHERE id IN (SELECT 5 UNION SELECT 2);
create view v6 as SELECT * FROM t2 WHERE id NOT IN (SELECT 5 UNION SELECT 2);
select * from v2;
select * from v3;
select * from v4;
select * from v5;
select * from v6;
-- @bvt:issue
drop view v1;
-- @bvt:issue#4354
drop view v2;
drop view v3;
drop view v4;
drop view v5;
drop view v6;
-- @bvt:issue

-- @case
-- @desc:test for [in] subquery with null
-- @label:bvt
drop table if exists t1;
drop table if exists t2;
create table t1 (a int);
insert into t1 values (1),(2),(3);
select 1 IN (SELECT * from t1);
select 10 IN (SELECT * from t1);
select NULL IN (SELECT * from t1);
update t1 set a=NULL where a=2;
select 1 IN (SELECT * from t1);
select 3 IN (SELECT * from t1);
select 10 IN (SELECT * from t1);
create view v1 as select 1 IN (SELECT * from t1);
create view v2 as select 10 IN (SELECT * from t1);
create view v3 as select NULL IN (SELECT * from t1);
create view v5 as select 1 IN (SELECT * from t1);
create view v6 as select 3 IN (SELECT * from t1);
create view v7 as select 10 IN (SELECT * from t1);

select * from v1;
select * from v2;
select * from v3;
select * from v5;
select * from v6;
select * from v7;

drop view v1;	
drop view v2;	
drop view v3;	
drop view v5;	
drop view v6;	
drop view v7;	

DROP TABLE IF EXISTS t1;
create table t1 (a varchar(20));
insert into t1 values ('A'),('BC'),('DEF');
select 'A' IN (SELECT * from t1);
select 'XYZS' IN (SELECT * from t1);
select NULL IN (SELECT * from t1);
create view v1 as select 'A' IN (SELECT * from t1);
create view v2 as select 'XYZS' IN (SELECT * from t1);
create view v3 as select NULL IN (SELECT * from t1);
select * from v1;
select * from v2;
select * from v3;
drop view v1;	
drop view v2;	
drop view v3;

update t1 set a=NULL where a='BC';
select 'A' IN (SELECT * from t1);
select 'DEF' IN (SELECT * from t1);
select 'XYZS' IN (SELECT * from t1);
create view v1 as select 'A' IN (SELECT * from t1);
create view v2 as select 'DEF' IN (SELECT * from t1);
create view v3 as select 'XYZS' IN (SELECT * from t1);
select * from v1;
select * from v2;
select * from v3;
drop view v1;	
drop view v2;	
drop view v3;

DROP TABLE IF EXISTS t1;
create table t1 (a float);
insert into t1 values (1.5),(2.5),(3.5);
select 1.5 IN (SELECT * from t1);
select 10.5 IN (SELECT * from t1);
select NULL IN (SELECT * from t1);
create view v1 as select 1.5 IN (SELECT * from t1);
create view v2 as select 10.5 IN (SELECT * from t1);
create view v3 as select NULL IN (SELECT * from t1);
select * from v1;
select * from v2;
select * from v3;
drop view v1;	
drop view v2;	
drop view v3;
update t1 set a=NULL where a=2.5;
select 1.5 IN (SELECT * from t1);
select 3.5 IN (SELECT * from t1);
select 10.5 IN (SELECT * from t1);
create view v1 as select 1.5 IN (SELECT * from t1);
create view v2 as select 3.5 IN (SELECT * from t1);
create view v3 as select 10.5 IN (SELECT * from t1);
select * from v1;
select * from v2;
select * from v3;
drop view v1;	
drop view v2;	
drop view v3;

drop table if exists t1;
drop table if exists t2;
CREATE TABLE t1 (a int(11) NOT NULL default 0, PRIMARY KEY  (a));
CREATE TABLE t2 (a int(11) default 0, INDEX (a));
CREATE TABLE t3 (a int(11) default 0);
INSERT INTO t3 VALUES (1),(2),(3);
INSERT INTO t1 VALUES (1),(2),(3),(4);
INSERT INTO t2 VALUES (1),(2),(3);
SELECT t1.a, t1.a in (select t2.a from t2) FROM t1;
SELECT t1.a, t1.a in (select t2.a from t2,t3 where t3.a=t2.a) FROM t1;
create view v1 as SELECT t1.a, t1.a in (select t2.a from t2) FROM t1;
create view v2 as SELECT t1.a, t1.a in (select t2.a from t2,t3 where t3.a=t2.a) FROM t1;
select * from v1;
select * from v2;
drop view v1;	
drop view v2;
drop table if exists t1;
drop table if exists t2;
drop table if exists t3;

create table t1 (a int);
insert into t1 values (-1), (-4), (-2), (NULL);
select -10 IN (select a from t1);
create view v1 as select -10 IN (select a from t1);
select * from v1;
drop view v1;
DROP TABLE IF EXISTS t1;

-- @case
-- @desc:test for [in] subquery with limit
-- @label:bvt
create table t1 (a float);
select 10.5 IN (SELECT * from t1 LIMIT 1);
-- @bvt:issue#4354
select 10.5 IN (SELECT * from t1 LIMIT 1 UNION SELECT 1.5);
-- @bvt:issue
select 10.5 IN (SELECT * from t1 UNION SELECT 1.5 LIMIT 1);
create view v1 as select 10.5 IN (SELECT * from t1 LIMIT 1);
-- @bvt:issue#4354
create view v2 as select 10.5 IN (SELECT * from t1 LIMIT 1 UNION SELECT 1.5);
-- @bvt:issue
create view v3 as select 10.5 IN (SELECT * from t1 UNION SELECT 1.5 LIMIT 1);
select * from v1;
-- @bvt:issue#4354
select * from v2;
-- @bvt:issue
select * from v3;
drop view v1;	
-- @bvt:issue#4354
drop view v2;	
-- @bvt:issue
drop view v3;

-- @case
-- @desc:test for [in] subquery with Multi tuple
-- @label:bvt
DROP TABLE IF EXISTS t1;
create table t1 (a int, b real, c varchar(10));
insert into t1 values (1, 1, 'a'), (2,2,'b'), (NULL, 2, 'b');
select (1, 1, 'a') IN (select a,b,c from t1);
select (1, 2, 'a') IN (select a,b,c from t1);
select (1, 1, 'a') IN (select b,a,c from t1);
select (1, 1, 'a') IN (select a,b,c from t1 where a is not null);
select (1, 2, 'a') IN (select a,b,c from t1 where a is not null);
select (1, 1, 'a') IN (select b,a,c from t1 where a is not null);
select (1, 1, 'a') IN (select a,b,c from t1 where c='b' or c='a');
select (1, 2, 'a') IN (select a,b,c from t1 where c='b' or c='a');
select (1, 1, 'a') IN (select b,a,c from t1 where c='b' or c='a');
select (1, 1, 'a') IN (select b,a,c from t1 limit 2);
create view v1 as select (1, 1, 'a') IN (select a,b,c from t1);
create view v2 as select (1, 2, 'a') IN (select a,b,c from t1);
create view v3 as select (1, 1, 'a') IN (select b,a,c from t1);
create view v4 as select (1, 1, 'a') IN (select a,b,c from t1 where a is not null);
create view v5 as select (1, 2, 'a') IN (select a,b,c from t1 where a is not null);
create view v6 as select (1, 1, 'a') IN (select b,a,c from t1 where a is not null);
create view v7 as select (1, 1, 'a') IN (select a,b,c from t1 where c='b' or c='a');
create view v8 as select (1, 2, 'a') IN (select a,b,c from t1 where c='b' or c='a');
create view v9 as select (1, 1, 'a') IN (select b,a,c from t1 where c='b' or c='a');
create view v10 as select (1, 1, 'a') IN (select b,a,c from t1 limit 2);

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

DROP TABLE IF EXISTS t1;

create table t1 (a integer, b integer);
-- @bvt:issue#7691
select (1,(2,2)) in (select * from t1 );
select (1,(2,2)) = (select * from t1 );
select (select * from t1) = (1,(2,2));
create view v1 as select (1,(2,2)) in (select * from t1 );
create view v2 as select (1,(2,2)) = (select * from t1 );
create view v3 as select (select * from t1) = (1,(2,2));
select * from v1;
select * from v2;
select * from v3;
drop view v1;	
drop view v2;	
drop view v3;
-- @bvt:issue

DROP TABLE IF EXISTS t1;
drop table if exists t2;
CREATE TABLE t1 (a1 INT, a2 INT);
CREATE TABLE t2 (b1 INT, b2 INT);
INSERT INTO t1 VALUES (100, 200);
INSERT INTO t1 VALUES (101, 201);
INSERT INTO t2 VALUES (101, 201);
INSERT INTO t2 VALUES (103, 203);
SELECT ((a1,a2) IN (SELECT * FROM t2 WHERE b2 > 0)) IS NULL FROM t1;
create view v1 as SELECT ((a1,a2) IN (SELECT * FROM t2 WHERE b2 > 0)) IS NULL FROM t1;
select * from v1;
drop view v1;


DROP TABLE IF EXISTS t1;
drop table if exists t2;

create table t1 (a int, b int);
insert into t1 values (0,0), (2,2), (3,3);
create table t2 (a int, b int);
insert into t2 values (1,1), (3,3);
select a, b, (a,b) in (select a, min(b) from t2 group by a) Z from t1;
create view v1 as select a, b, (a,b) in (select a, min(b) from t2 group by a) Z from t1;
select * from v1;
drop view v1;	
insert into t2 values (NULL,4);
select a, b, (a,b) in (select a, min(b) from t2 group by a) Z from t1;
create view v1 as select a, b, (a,b) in (select a, min(b) from t2 group by a) Z from t1;
select * from v1;
drop view v1;	
DROP TABLE IF EXISTS t1;
drop table if exists t2;

DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (a INT);
INSERT INTO t1 VALUES (1), (2), (11);
-- @bvt:issue#7691
SELECT a, (11, 12) = (SELECT a, 22), (11, 12) IN (SELECT a, 22) FROM t1 GROUP BY t1.a;
SELECT a, (11, 12) = (SELECT a, 12), (11, 12) IN (SELECT a, 12) FROM t1 GROUP BY t1.a;
SELECT a, (11, 12) = (SELECT a, 22), (11, 12) IN (SELECT a, 22) FROM t1;
SELECT a, (11, 12) = (SELECT a, 12), (11, 12) IN (SELECT a, 12) FROM t1;
SELECT a AS x, (11, 12) = (SELECT MAX(x), 22), (11, 12) IN (SELECT MAX(x), 22) FROM t1;
SELECT a AS x, (11, 12) = (SELECT MAX(x), 12), (11, 12) IN (SELECT MAX(x), 12) FROM t1;
create view v1 as SELECT a, (11, 12) = (SELECT a, 22), (11, 12) IN (SELECT a, 22) FROM t1 GROUP BY t1.a;
create view v2 as SELECT a, (11, 12) = (SELECT a, 12), (11, 12) IN (SELECT a, 12) FROM t1 GROUP BY t1.a;
create view v3 as SELECT a, (11, 12) = (SELECT a, 22), (11, 12) IN (SELECT a, 22) FROM t1;
create view v4 as SELECT a, (11, 12) = (SELECT a, 12), (11, 12) IN (SELECT a, 12) FROM t1;
create view v5 as SELECT a AS x, (11, 12) = (SELECT MAX(x), 22), (11, 12) IN (SELECT MAX(x), 22) FROM t1;
create view v6 as SELECT a AS x, (11, 12) = (SELECT MAX(x), 12), (11, 12) IN (SELECT MAX(x), 12) FROM t1;

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
-- @bvt:issue
DROP TABLE IF EXISTS t1;
-- @bvt:issue#7691
SELECT (1,2) = (SELECT NULL, NULL), (1,2) IN (SELECT NULL, NULL);
SELECT (1,2) = (SELECT   1,  NULL), (1,2) IN (SELECT    1, NULL);
SELECT (1,2) = (SELECT NULL,    2), (1,2) IN (SELECT NULL,    2);
SELECT (1,2) = (SELECT NULL,    1), (1,2) IN (SELECT NULL,    1);
SELECT (1,2) = (SELECT    1,    1), (1,2) IN (SELECT    1,    1);
SELECT (1,2) = (SELECT    1,    2), (1,2) IN (SELECT    1,    2);
create view v1 as SELECT (1,2) = (SELECT NULL, NULL), (1,2) IN (SELECT NULL, NULL);
create view v2 as SELECT (1,2) = (SELECT   1,  NULL), (1,2) IN (SELECT    1, NULL);
create view v3 as SELECT (1,2) = (SELECT NULL,    2), (1,2) IN (SELECT NULL,    2);
create view v4 as SELECT (1,2) = (SELECT NULL,    1), (1,2) IN (SELECT NULL,    1);
create view v5 as SELECT (1,2) = (SELECT    1,    1), (1,2) IN (SELECT    1,    1);
create view v6 as SELECT (1,2) = (SELECT    1,    2), (1,2) IN (SELECT    1,    2);
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
-- @bvt:issue


DROP TABLE IF EXISTS t_out;
DROP TABLE IF EXISTS t_in;

-- @case
-- @desc:test for [in] subquery with netsed subquery
-- @label:bvt
DROP TABLE IF EXISTS t1;
drop table if exists t2;
create table t1 (a int);
create table t2 (b int);
insert into t1 values (1),(2);
insert into t2 values (1);
select a from t1 where a in (select a from t1 where a in (select b from t2));
create view v1 as select a from t1 where a in (select a from t1 where a in (select b from t2));
select * from v1;
drop view v1;	
DROP TABLE IF EXISTS t1;
drop table if exists t2;

create table t1 (a int, b int);
create table t2 (a int, b int);
select * from t1 where (a,b) in (select a,b from t2);
create view v1 as select * from t1 where (a,b) in (select a,b from t2);
select * from v1;
drop view v1;	
DROP TABLE IF EXISTS t1;
drop table if exists t2;


DROP TABLE IF EXISTS t1;
drop table if exists t2;

CREATE TABLE t1( a INT );
INSERT INTO t1 VALUES (1),(2);
CREATE TABLE t2( a INT, b INT );
SELECT * FROM t2 WHERE (a, b) IN (SELECT a, b FROM t2);
SELECT * FROM t1 WHERE a IN    ( SELECT 1 UNION ( SELECT 1 UNION SELECT 1 ) );
SELECT * FROM t1 WHERE a IN    ( ( SELECT 1 UNION SELECT 1 )  UNION SELECT 1 );
SELECT * FROM t1 WHERE a IN    ( SELECT 1 UNION SELECT 1 UNION SELECT 1 );
create view v1 as SELECT * FROM t2 WHERE (a, b) IN (SELECT a, b FROM t2);
create view v2 as SELECT * FROM t1 WHERE a IN    ( SELECT 1 UNION ( SELECT 1 UNION SELECT 1 ) );
create view v3 as SELECT * FROM t1 WHERE a IN    ( ( SELECT 1 UNION SELECT 1 )  UNION SELECT 1 );
create view v4 as SELECT * FROM t1 WHERE a IN    ( SELECT 1 UNION SELECT 1 UNION SELECT 1 );
select * from v1;
select * from v2;
select * from v3;
select * from v4;
drop view v1;
drop view v2;
drop view v3;
drop view v4;


CREATE TABLE t1 (i1 int DEFAULT NULL,i2 int DEFAULT NULL) ;
INSERT INTO t1 VALUES (1,    NULL);
INSERT INTO t1 VALUES (2,    3);
INSERT INTO t1 VALUES (4,    NULL);
INSERT INTO t1 VALUES (4,    0);
INSERT INTO t1 VALUES (NULL, NULL);
CREATE TABLE t2 (i1 int DEFAULT NULL,i2 int DEFAULT NULL) ;
INSERT INTO t2 VALUES (4, NULL);
INSERT INTO t2 VALUES (5, 0);
SELECT i1, i2
FROM t1
WHERE (i1, i2)
      NOT IN (SELECT i1, i2 FROM t2);

create view v1 as SELECT i1, i2
FROM t1
WHERE (i1, i2)
      NOT IN (SELECT i1, i2 FROM t2);
      
select * from v1;
drop view v1;

INSERT INTO t1 VALUES (NULL, NULL);
SELECT i1, i2
FROM t1
WHERE (i1, i2)
      NOT IN (SELECT i1, i2 FROM t2);
      

create view v1 as SELECT i1, i2
FROM t1
WHERE (i1, i2)
      NOT IN (SELECT i1, i2 FROM t2);
      
select * from v1;
drop view v1;

-- @case
-- @desc:test for [in] subquery with order by
-- @label:bvt
DROP TABLE IF EXISTS t1;
drop table if exists t2;
DROP TABLE IF EXISTS t3;
CREATE TABLE t1 (a int(11) NOT NULL default 0, PRIMARY KEY  (a));
CREATE TABLE t2 (a int(11) default 0);
CREATE TABLE t3 (a int(11) default 0);
INSERT INTO t3 VALUES (1),(2),(3);
INSERT INTO t1 VALUES (1),(2),(3),(4);
INSERT INTO t2 VALUES (1),(2),(3);
SELECT t1.a FROM t1 where t1.a in (select t2.a from t2 order by t2.a desc) ;
create view v1 as SELECT t1.a FROM t1 where t1.a in (select t2.a from t2 order by t2.a desc) ;
select * from v1;
drop view v1;

-- @case
-- @desc:test for [in] subquery with compound index
-- @label:bvt
DROP TABLE IF EXISTS t1;
drop table if exists t2;
CREATE TABLE `t1` ( `aid` int(11) NOT NULL default 0, `bid` int(11) NOT NULL default 0);
CREATE TABLE `t2` ( `aid` int(11) NOT NULL default 0, `bid` int(11) NOT NULL default 0);
insert into t1 values (1,1),(1,2),(2,1),(2,2);
insert into t2 values (1,2),(2,2);
select * from t1 where t1.aid not in (select aid from t2 where bid=t1.bid);
select * from t1 where t1.aid in (select aid from t2 where bid=t1.bid);
SELECT t1.a FROM t1 where t1.a in (select t2.a from t2 order by t2.a desc) ;
create view v1 as select * from t1 where t1.aid not in (select aid from t2 where bid=t1.bid);
create view v2 as SELECT t1.a FROM t1 where t1.a in (select t2.a from t2 order by t2.a desc) ;
select * from v1;
select * from v2;
drop view v1;
drop view v2;



drop table if exists t2;
CREATE TABLE t1(select_id BIGINT, values_id BIGINT);
INSERT INTO t1 VALUES (1, 1);
CREATE TABLE t2 (select_id BIGINT, values_id BIGINT);
INSERT INTO t2 VALUES (0, 1), (0, 2), (0, 3), (1, 5);
SELECT values_id FROM t1
WHERE values_id IN (SELECT values_id FROM t2  WHERE select_id IN (1, 0));
SELECT values_id FROM t1 WHERE values_id IN (SELECT values_id FROM t2 WHERE select_id BETWEEN 0 AND 1);
SELECT values_id FROM t1 WHERE values_id IN (SELECT values_id FROM t2 WHERE select_id = 0 OR select_id = 1);
create view v1 as SELECT values_id FROM t1 WHERE values_id IN (SELECT values_id FROM t2 WHERE select_id BETWEEN 0 AND 1);
create view v2 as SELECT values_id FROM t1 WHERE values_id IN (SELECT values_id FROM t2 WHERE select_id = 0 OR select_id = 1);
select * from v1;
select * from v2;
drop view v1;
drop view v2;


DROP TABLE IF EXISTS t1;
drop table if exists t2;
CREATE TABLE t1 (a INT NOT NULL);
INSERT INTO t1 VALUES (1),(-1), (65),(66);
CREATE TABLE t2 (a INT UNSIGNED NOT NULL PRIMARY KEY);
INSERT INTO t2 VALUES (65),(66);
SELECT a FROM t1 WHERE a NOT IN (65,66);
SELECT a FROM t1 WHERE a NOT IN (SELECT a FROM t2);
create view v1 as SELECT a FROM t1 WHERE a NOT IN (65,66);
create view v2 as SELECT a FROM t1 WHERE a NOT IN (SELECT a FROM t2);
select * from v1;
select * from v2;
drop view v1;
drop view v2;



DROP TABLE IF EXISTS t1;
drop table if exists t2;
CREATE TABLE t1 (a INT);
INSERT INTO t1 VALUES (1),(2),(3);
CREATE TABLE t2 (a INT);
INSERT INTO t1 VALUES (1),(2),(3);
SELECT 1 FROM t1 WHERE t1.a NOT IN (SELECT 1 FROM t1, t2 WHERE false);
create view v1 as SELECT 1 FROM t1 WHERE t1.a NOT IN (SELECT 1 FROM t1, t2 WHERE false);
select * from v1;
drop view v1;
DROP TABLE IF EXISTS t1;
drop table if exists t2;


-- @case
-- @desc:test for [in] subquery with an aggregate function in HAVING
-- @label:bvt
DROP TABLE IF EXISTS t1;
drop table if exists t2;
drop table if exists t3;
CREATE TABLE t1 (a int, b int);
CREATE TABLE t2 (c int, d int);
CREATE TABLE t3 (e int);
INSERT INTO t1 VALUES
  (1,10), (2,10), (1,20), (2,20), (3,20), (2,30), (4,40);
INSERT INTO t2 VALUES
  (2,10), (2,20), (4,10), (5,10), (3,20), (2,40);
INSERT INTO t3 VALUES (10), (30), (10), (20) ;

SELECT a FROM t1 GROUP BY a HAVING a IN (SELECT c FROM t2 WHERE MAX(b)>20);
SELECT a FROM t1 GROUP BY a  HAVING a IN (SELECT c FROM t2 WHERE MAX(b)<d);
SELECT a FROM t1 GROUP BY a  HAVING a IN (SELECT c FROM t2 WHERE MAX(b)>d);
SELECT a FROM t1 GROUP BY a  HAVING a IN (SELECT c FROM t2  WHERE d >= SOME(SELECT e FROM t3 WHERE MAX(b)=e));
SELECT a FROM t1 GROUP BY a  HAVING a IN (SELECT c FROM t2  WHERE  EXISTS(SELECT e FROM t3 WHERE MAX(b)=e AND e <= d));
SELECT a FROM t1 GROUP BY a  HAVING a IN (SELECT c FROM t2  WHERE d > SOME(SELECT e FROM t3 WHERE MAX(b)=e));
SELECT a FROM t1 GROUP BY a  HAVING a IN (SELECT c FROM t2  WHERE  EXISTS(SELECT e FROM t3 WHERE MAX(b)=e AND e < d));
SELECT a FROM t1 GROUP BY a  HAVING a IN (SELECT c FROM t2  WHERE MIN(b) < d AND EXISTS(SELECT e FROM t3 WHERE MAX(b)=e AND e <= d));

create view v1 as SELECT a FROM t1 GROUP BY a HAVING a IN (SELECT c FROM t2 WHERE MAX(b)>20);
create view v2 as SELECT a FROM t1 GROUP BY a  HAVING a IN (SELECT c FROM t2 WHERE MAX(b)<d);
create view v3 as SELECT a FROM t1 GROUP BY a  HAVING a IN (SELECT c FROM t2 WHERE MAX(b)>d);
create view v4 as SELECT a FROM t1 GROUP BY a  HAVING a IN (SELECT c FROM t2  WHERE d >= SOME(SELECT e FROM t3 WHERE MAX(b)=e));
create view v5 as SELECT a FROM t1 GROUP BY a  HAVING a IN (SELECT c FROM t2  WHERE  EXISTS(SELECT e FROM t3 WHERE MAX(b)=e AND e <= d));
create view v6 as SELECT a FROM t1 GROUP BY a  HAVING a IN (SELECT c FROM t2  WHERE d > SOME(SELECT e FROM t3 WHERE MAX(b)=e));
create view v7 as SELECT a FROM t1 GROUP BY a  HAVING a IN (SELECT c FROM t2  WHERE  EXISTS(SELECT e FROM t3 WHERE MAX(b)=e AND e < d));
create view v8 as SELECT a FROM t1 GROUP BY a  HAVING a IN (SELECT c FROM t2  WHERE MIN(b) < d AND EXISTS(SELECT e FROM t3 WHERE MAX(b)=e AND e <= d));

select * from v1;
select * from v2;
select * from v3;
select * from v4;
select * from v5;
select * from v6;
select * from v7;
select * from v8;

drop view v1;
drop view v2;
drop view v3;
drop view v4;
drop view v5;
drop view v6;
drop view v7;
drop view v8;



-- @case
-- @desc:test for [in] subquery IN with a double "(())"
-- @label:bvt
DROP TABLE IF EXISTS  t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t1xt2;
CREATE TABLE t1 (
  id_1 int(5) NOT NULL,
  t varchar(4) DEFAULT NULL
);
CREATE TABLE t2 (
  id_2 int(5) NOT NULL,
  t varchar(4) DEFAULT NULL
);
CREATE TABLE t1xt2 (
  id_1 int(5) NOT NULL,
  id_2 int(5) NOT NULL
);
INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');
INSERT INTO t2 VALUES (2, 'bb'), (3, 'cc'), (4, 'dd'), (12, 'aa');
INSERT INTO t1xt2 VALUES (2, 2), (3, 3), (4, 4);
SELECT DISTINCT t1.id_1 FROM t1 WHERE(12 IN (SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1));
SELECT DISTINCT t1.id_1 FROM t1 WHERE(12 IN ((SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1)));
SELECT DISTINCT t1.id_1 FROM t1 WHERE(12 IN (((SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1))));
SELECT DISTINCT t1.id_1 FROM t1 WHERE(12 NOT IN (SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1));
SELECT DISTINCT t1.id_1 FROM t1 WHERE(12 NOT IN ((SELECT t1xt2.id_2 FROM t1xt2 where t1.id_1 = t1xt2.id_1)));
SELECT DISTINCT t1.id_1 FROM t1 WHERE(12 NOT IN (((SELECT t1xt2.id_2 FROM t1xt2 where t1.id_1 = t1xt2.id_1))));
insert INTO t1xt2 VALUES (1, 12);
SELECT DISTINCT t1.id_1 FROM t1 WHERE(12 IN (SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1));
SELECT DISTINCT t1.id_1 FROM t1 WHERE(12 IN ((SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1)));
SELECT DISTINCT t1.id_1 FROM t1 WHERE(12 IN (((SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1))));
SELECT DISTINCT t1.id_1 FROM t1 WHERE(12 NOT IN (SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1));
SELECT DISTINCT t1.id_1 FROM t1 WHERE(12 NOT IN ((SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1)));
SELECT DISTINCT t1.id_1 FROM t1 WHERE(12 NOT IN (((SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1))));
insert INTO t1xt2 VALUES (2, 12);
SELECT DISTINCT t1.id_1 FROM t1 WHERE(12 IN (SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1));
SELECT DISTINCT t1.id_1 FROM t1 WHERE(12 IN ((SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1)));
SELECT DISTINCT t1.id_1 FROM t1 WHERE(12 IN (((SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1))));
SELECT DISTINCT t1.id_1 FROM t1 WHERE(12 NOT IN (SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1));
SELECT DISTINCT t1.id_1 FROM t1 WHERE(12 NOT IN ((SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1)));
SELECT DISTINCT t1.id_1 FROM t1 WHERE(12 NOT IN (((SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1))));

create view v1 as SELECT DISTINCT t1.id_1 FROM t1 WHERE(12 IN (SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1));
create view v2 as SELECT DISTINCT t1.id_1 FROM t1 WHERE(12 IN ((SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1)));
create view v3 as SELECT DISTINCT t1.id_1 FROM t1 WHERE(12 IN (((SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1))));
create view v4 as SELECT DISTINCT t1.id_1 FROM t1 WHERE(12 NOT IN (SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1));
create view v5 as SELECT DISTINCT t1.id_1 FROM t1 WHERE(12 NOT IN ((SELECT t1xt2.id_2 FROM t1xt2 where t1.id_1 = t1xt2.id_1)));
create view v6 as SELECT DISTINCT t1.id_1 FROM t1 WHERE(12 NOT IN (((SELECT t1xt2.id_2 FROM t1xt2 where t1.id_1 = t1xt2.id_1))));
create view v7 as insert INTO t1xt2 VALUES (1, 12);
create view v8 as SELECT DISTINCT t1.id_1 FROM t1 WHERE(12 IN (SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1));
create view v9 as SELECT DISTINCT t1.id_1 FROM t1 WHERE(12 IN ((SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1)));
create view v10 as SELECT DISTINCT t1.id_1 FROM t1 WHERE(12 IN (((SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1))));
create view v11 as SELECT DISTINCT t1.id_1 FROM t1 WHERE(12 NOT IN (SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1));
create view v12 as SELECT DISTINCT t1.id_1 FROM t1 WHERE(12 NOT IN ((SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1)));
create view v13 as SELECT DISTINCT t1.id_1 FROM t1 WHERE(12 NOT IN (((SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1))));
create view v14 as insert INTO t1xt2 VALUES (2, 12);
create view v15 as SELECT DISTINCT t1.id_1 FROM t1 WHERE(12 IN (SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1));
create view v16 as SELECT DISTINCT t1.id_1 FROM t1 WHERE(12 IN ((SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1)));
create view v17 as SELECT DISTINCT t1.id_1 FROM t1 WHERE(12 IN (((SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1))));
create view v18 as SELECT DISTINCT t1.id_1 FROM t1 WHERE(12 NOT IN (SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1));
create view v19 as SELECT DISTINCT t1.id_1 FROM t1 WHERE(12 NOT IN ((SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1)));
create view v20 as SELECT DISTINCT t1.id_1 FROM t1 WHERE(12 NOT IN (((SELECT t1xt2.id_2 FROM t1xt2 WHERE t1.id_1 = t1xt2.id_1))));

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
select * from v19;
select * from v20;

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
drop view v19;
drop view v20;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t1xt2;





