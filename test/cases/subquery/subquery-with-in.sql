-- @suit

-- @case
-- @desc:test for [in] subquery with constant operand
-- @label:bvt
-- @bvt:issue#3312
SELECT 1 IN (SELECT 1);
-- @bvt:issue
-- @bvt:issue#3307
SELECT 1 FROM (SELECT 1 as a) b WHERE 1 IN (SELECT (SELECT a));
-- @bvt:issue
-- @bvt:issue#3556
SELECT 1 FROM (SELECT 1 as a) b WHERE 1 not IN (SELECT (SELECT a));
-- @bvt:issue
SELECT * FROM (SELECT 1 as id) b WHERE id IN (SELECT * FROM (SELECT 1 as id) c ORDER BY id);
SELECT * FROM (SELECT 1) a  WHERE 1 IN (SELECT 1,1);
SELECT * FROM (SELECT 1) b WHERE 1 IN (SELECT *);
-- @bvt:issue#3312
SELECT ((0,1) NOT IN (SELECT NULL,1)) IS NULL;
-- @bvt:issue

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
-- @bvt:issue#3312
SELECT 0 IN (SELECT 1 FROM t1 a);
-- @bvt:issue
select * from t3 where a in (select a,b from t2);
select * from t3 where a in (select * from t2);

drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
create table t1 (s1 char(5), index s1(s1));
create table t2 (s1 char(5), index s1(s1));
insert into t1 values ('a1'),('a2'),('a3');
insert into t2 values ('a1'),('a2');
-- @bvt:issue#3312
select s1, s1 NOT IN (SELECT s1 FROM t2) from t1;
select s1, s1 NOT IN (SELECT s1 FROM t2 WHERE s1 < 'a2') from t1;
-- @bvt:issue

drop table if exists t1;
drop table if exists t2;
create table t1(val varchar(10));
insert into t1 values ('aaa'), ('bbb'),('eee'),('mmm'),('ppp');
select count(*) from t1 as w1 where w1.val in (select w2.val from t1 as w2 where w2.val like 'm%') and w1.val in (select w3.val from t1 as w3 where w3.val like 'e%');

DROP TABLE IF EXISTS t1;
create table t1 (id int not null, text varchar(20) not null default '', primary key (id));
insert into t1 (id, text) values (1, 'text1'), (2, 'text2'), (3, 'text3'), (4, 'text4'), (5, 'text5'), (6, 'text6'), (7, 'text7'), (8, 'text8'), (9, 'text9'), (10, 'text10'), (11, 'text11'), (12, 'text12');
select * from t1 where id not in (select id from t1 where id < 8);

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
drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
CREATE TABLE t1(a int, INDEX (a));
INSERT INTO t1 VALUES (1), (3), (5), (7);
INSERT INTO t1 VALUES (NULL);
CREATE TABLE t2(a int);
INSERT INTO t2 VALUES (1),(2),(3);
-- @bvt:issue#3312
SELECT a, a IN (SELECT a FROM t1) FROM t2;
-- @bvt:issue

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
-- @bvt:issue#3307
SELECT * FROM c WHERE `int_key` IN (SELECT `int_nokey`);
-- @bvt:issue
DROP TABLE IF EXISTS c;

drop table if exists t1;
drop table if exists t2;
CREATE TABLE t1(c INT);
CREATE TABLE t2(a INT, b INT);
INSERT INTO t2 VALUES (1, 10), (2, NULL);
INSERT INTO t1 VALUES (1), (3);
SELECT * FROM t2 WHERE b NOT IN (SELECT max(t.c) FROM t1, t1 t WHERE t.c>10);
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
-- @bvt:issue
SELECT * from t2 where topic IN (SELECT topic FROM t2 GROUP BY topic HAVING topic < 41000);
SELECT * from t2 where topic NOT IN (SELECT topic FROM t2 GROUP BY topic HAVING topic < 41000);
SELECT * FROM t2 WHERE mot IN (SELECT 'joce');

drop table if exists t1;
drop table if exists t2;
create table t1 (a int);
create table t2 (a int);
insert into t1 values (1),(2);
insert into t2 values (0),(1),(2),(3);
select a from t2 where a in (select a from t1);
select a from t2 having a in (select a from t1);

drop table if exists t1;
drop table if exists t2;
create table t1 (oref int, grp int, ie int) ;
insert into t1 (oref, grp, ie) values(1, 1, 1),(1, 1, 1), (1, 2, NULL),(2, 1, 3),(3, 1, 4),(3, 2, NULL);
create table t2 (oref int, a int);
insert into t2 values(1, 1),(2, 2),(3, 3), (4, NULL),(2, NULL);
create table t3 (a int);
insert into t3 values (NULL), (NULL);
-- @bvt:issue#3312
select a, oref, a in (select max(ie) from t1 where oref=t2.oref group by grp) Z from t2;
-- @bvt:issue
select a, oref from t2 where a in (select max(ie) from t1 where oref=t2.oref group by grp);
-- @bvt:issue#3312
select a, oref, a in (
  select max(ie) from t1 where oref=t2.oref group by grp union
  select max(ie) from t1 where oref=t2.oref group by grp
  ) Z from t2;
select a in (select max(ie) from t1 where oref=4 group by grp) from t3;
-- @bvt:issue

drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
create table t1 (a int, oref int);
insert into t1 values(1, 1),(1, NULL),(2, 3),(2, NULL),(3, NULL);
create table t2 (a int, oref int);
insert into t2 values (1, 1), (2,2), (NULL, 3), (NULL, 4);
-- @bvt:issue#3312
select oref, a, a in (select a from t1 where oref=t2.oref) Z from t2;
-- @bvt:issue
select oref, a from t2 where a in (select a from t1 where oref=t2.oref);
-- @ignore{
delete from t2;
insert into t2 values (NULL, 0),(NULL, 0), (NULL, 0), (NULL, 0);
-- @bvt:issue#3312
select oref, a, a in (select a from t1 where oref=t2.oref) Z from t2;
-- @bvt:issue
drop table if exists t1;
drop table if exists t2;

-- @case
-- @desc:test for [in] subquery with UNION
-- @label:bvt
drop table if exists t1;
drop table if exists t2;
CREATE TABLE t2 (id int(11) default NULL);
INSERT INTO t2 VALUES (1),(2);
SELECT * FROM t2 WHERE id IN (SELECT 1);
-- @bvt:issue#4354
SELECT * FROM t2 WHERE id IN (SELECT 1 UNION SELECT 3);
-- @bvt:issue
SELECT * FROM t2 WHERE id IN (SELECT 1+(select 1));
-- @bvt:issue#4354
SELECT * FROM t2 WHERE id IN (SELECT 5 UNION SELECT 3);
SELECT * FROM t2 WHERE id IN (SELECT 5 UNION SELECT 2);
SELECT * FROM t2 WHERE id NOT IN (SELECT 5 UNION SELECT 2);
-- @bvt:issue

-- @case
-- @desc:test for [in] subquery with null
-- @label:bvt
drop table if exists t1;
drop table if exists t2;
create table t1 (a int);
insert into t1 values (1),(2),(3);
-- @bvt:issue#3312
select 1 IN (SELECT * from t1);
select 10 IN (SELECT * from t1);
select NULL IN (SELECT * from t1);
-- @bvt:issue
update t1 set a=NULL where a=2;
-- @bvt:issue#3312
select 1 IN (SELECT * from t1);
select 3 IN (SELECT * from t1);
select 10 IN (SELECT * from t1);
-- @bvt:issue

DROP TABLE IF EXISTS t1;
create table t1 (a varchar(20));
insert into t1 values ('A'),('BC'),('DEF');
-- @bvt:issue#3312
select 'A' IN (SELECT * from t1);
select 'XYZS' IN (SELECT * from t1);
select NULL IN (SELECT * from t1);
-- @bvt:issue
update t1 set a=NULL where a='BC';
-- @bvt:issue#3312
select 'A' IN (SELECT * from t1);
select 'DEF' IN (SELECT * from t1);
select 'XYZS' IN (SELECT * from t1);
-- @bvt:issue

DROP TABLE IF EXISTS t1;
create table t1 (a float);
insert into t1 values (1.5),(2.5),(3.5);
-- @bvt:issue#3312
select 1.5 IN (SELECT * from t1);
select 10.5 IN (SELECT * from t1);
select NULL IN (SELECT * from t1);
-- @bvt:issue
update t1 set a=NULL where a=2.5;
-- @bvt:issue#3312
select 1.5 IN (SELECT * from t1);
select 3.5 IN (SELECT * from t1);
select 10.5 IN (SELECT * from t1);
-- @bvt:issue

drop table if exists t1;
drop table if exists t2;
CREATE TABLE t1 (a int(11) NOT NULL default 0, PRIMARY KEY  (a));
CREATE TABLE t2 (a int(11) default 0, INDEX (a));
CREATE TABLE t3 (a int(11) default 0);
INSERT INTO t3 VALUES (1),(2),(3);
INSERT INTO t1 VALUES (1),(2),(3),(4);
INSERT INTO t2 VALUES (1),(2),(3);
-- @bvt:issue#3312
SELECT t1.a, t1.a in (select t2.a from t2) FROM t1;
SELECT t1.a, t1.a in (select t2.a from t2,t3 where t3.a=t2.a) FROM t1;
-- @bvt:issue
drop table if exists t1;
drop table if exists t2;
drop table if exists t3;

create table t1 (a int);
insert into t1 values (-1), (-4), (-2), (NULL);
-- @bvt:issue#3312
select -10 IN (select a from t1);
-- @bvt:issue
DROP TABLE IF EXISTS t1;

-- @case
-- @desc:test for [in] subquery with limit
-- @label:bvt
create table t1 (a float);
-- @bvt:issue#3312
select 10.5 IN (SELECT * from t1 LIMIT 1);
-- error
select 10.5 IN (SELECT * from t1 LIMIT 1 UNION SELECT 1.5);
-- error
select 10.5 IN (SELECT * from t1 UNION SELECT 1.5 LIMIT 1);
-- @bvt:issue

-- @case
-- @desc:test for [in] subquery with Multi tuple
-- @label:bvt
DROP TABLE IF EXISTS t1;
create table t1 (a int, b real, c varchar(10));
insert into t1 values (1, 1, 'a'), (2,2,'b'), (NULL, 2, 'b');
-- @bvt:issue#3312
select (1, 1, 'a') IN (select a,b,c from t1);
select (1, 2, 'a') IN (select a,b,c from t1);
select (1, 1, 'a') IN (select b,a,c from t1);
select (1, 1, 'a') IN (select a,b,c from t1 where a is not null);
select (1, 2, 'a') IN (select a,b,c from t1 where a is not null);
select (1, 1, 'a') IN (select b,a,c from t1 where a is not null);
select (1, 1, 'a') IN (select a,b,c from t1 where c='b' or c='a');
select (1, 2, 'a') IN (select a,b,c from t1 where c='b' or c='a');
select (1, 1, 'a') IN (select b,a,c from t1 where c='b' or c='a');
-- error
select (1, 1, 'a') IN (select b,a,c from t1 limit 2);
-- @bvt:issue
DROP TABLE IF EXISTS t1;

create table t1 (a integer, b integer);
-- @bvt:issue#3312
select (1,(2,2)) in (select * from t1 );
-- error
select (1,(2,2)) = (select * from t1 );
-- error
select (select * from t1) = (1,(2,2));
-- @bvt:issue

DROP TABLE IF EXISTS t1;
drop table if exists t2;
CREATE TABLE t1 (a1 INT, a2 INT);
CREATE TABLE t2 (b1 INT, b2 INT);
INSERT INTO t1 VALUES (100, 200);
INSERT INTO t1 VALUES (101, 201);
INSERT INTO t2 VALUES (101, 201);
INSERT INTO t2 VALUES (103, 203);
-- @bvt:issue#3312
SELECT ((a1,a2) IN (SELECT * FROM t2 WHERE b2 > 0)) IS NULL FROM t1;
-- @bvt:issue

DROP TABLE IF EXISTS t1;
drop table if exists t2;
CREATE TABLE t1 (
pk INTEGER,
col_int_nokey INTEGER,
col_int_key INTEGER,
col_datetime_key DATETIME,
PRIMARY KEY (pk)
);
INSERT INTO t1 VALUES (1, 1, 7, '2001-11-04 19:07:55.051133');
CREATE TABLE t2(field1 INT, field2 INT);
-- @bvt:issue#3307
SELECT * FROM t2 WHERE (field1, field2) IN (
  SELECT MAX(col_datetime_key), col_int_key
  FROM t1
  WHERE col_int_key > col_int_nokey
  GROUP BY col_int_key);
-- @bvt:issue

DROP TABLE IF EXISTS t1;
drop table if exists t2;

create table t1 (a int, b int);
insert into t1 values (0,0), (2,2), (3,3);
create table t2 (a int, b int);
insert into t2 values (1,1), (3,3);
-- @bvt:issue#3312
select a, b, (a,b) in (select a, min(b) from t2 group by a) Z from t1;
-- @bvt:issue
insert into t2 values (NULL,4);
-- @bvt:issue#3312
select a, b, (a,b) in (select a, min(b) from t2 group by a) Z from t1;
-- @bvt:issue
DROP TABLE IF EXISTS t1;
drop table if exists t2;

DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (a INT);
INSERT INTO t1 VALUES (1), (2), (11);
-- @bvt:issue#3312
SELECT a, (11, 12) = (SELECT a, 22), (11, 12) IN (SELECT a, 22) FROM t1 GROUP BY t1.a;
SELECT a, (11, 12) = (SELECT a, 12), (11, 12) IN (SELECT a, 12) FROM t1 GROUP BY t1.a;
SELECT a, (11, 12) = (SELECT a, 22), (11, 12) IN (SELECT a, 22) FROM t1;
SELECT a, (11, 12) = (SELECT a, 12), (11, 12) IN (SELECT a, 12) FROM t1;
SELECT a AS x, (11, 12) = (SELECT MAX(x), 22), (11, 12) IN (SELECT MAX(x), 22) FROM t1;
SELECT a AS x, (11, 12) = (SELECT MAX(x), 12), (11, 12) IN (SELECT MAX(x), 12) FROM t1;
-- @bvt:issue
DROP TABLE IF EXISTS t1;
-- @bvt:issue#3312
SELECT (1,2) = (SELECT NULL, NULL), (1,2) IN (SELECT NULL, NULL);
SELECT (1,2) = (SELECT   1,  NULL), (1,2) IN (SELECT    1, NULL);
SELECT (1,2) = (SELECT NULL,    2), (1,2) IN (SELECT NULL,    2);
SELECT (1,2) = (SELECT NULL,    1), (1,2) IN (SELECT NULL,    1);
SELECT (1,2) = (SELECT    1,    1), (1,2) IN (SELECT    1,    1);
SELECT (1,2) = (SELECT    1,    2), (1,2) IN (SELECT    1,    2);
-- @bvt:issue

create table t_out (subcase char(3),a1 char(2), b1 char(2), c1 char(2));
create table t_in  (a2 char(2), b2 char(2), c2 char(2));
insert into t_out values ('A.1','2a', NULL, '2a');
insert into t_out values ('A.3', '2a', NULL, '2a');
insert into t_out values ('A.4', '2a', NULL, 'xx');
insert into t_out values ('B.1', '2a', '2a', '2a');
insert into t_out values ('B.2', '2a', '2a', '2a');
insert into t_out values ('B.3', '3a', 'xx', '3a');
insert into t_out values ('B.4', 'xx', '3a', '3a');
insert into t_in values ('1a', '1a', '1a');
insert into t_in values ('2a', '2a', '2a');
insert into t_in values (NULL, '2a', '2a');
insert into t_in values ('3a', NULL, '3a');
-- @bvt:issue#3312
select subcase,
       (a1, b1, c1)     IN (select * from t_in where a2 = 'no_match') pred_in,
       (a1, b1, c1) NOT IN (select * from t_in where a2 = 'no_match') pred_not_in
from t_out where subcase = 'A.1';

select subcase,
       (a1, b1, c1)     IN (select * from t_in) pred_in,
       (a1, b1, c1) NOT IN (select * from t_in) pred_not_in
from t_out where subcase = 'A.3';

select subcase,
       (a1, b1, c1)     IN (select * from t_in) pred_in,
       (a1, b1, c1) NOT IN (select * from t_in) pred_not_in
from t_out where subcase = 'A.4';

select subcase,
       (a1, b1, c1)     IN (select * from t_in where a2 = 'no_match') pred_in,
       (a1, b1, c1) NOT IN (select * from t_in where a2 = 'no_match') pred_not_in
from t_out where subcase = 'B.1';

select subcase,
       (a1, b1, c1)     IN (select * from t_in) pred_in,
       (a1, b1, c1) NOT IN (select * from t_in) pred_not_in
from t_out where subcase = 'B.2';

select subcase,
       (a1, b1, c1)     IN (select * from t_in) pred_in,
       (a1, b1, c1) NOT IN (select * from t_in) pred_not_in
from t_out where subcase = 'B.3';

select subcase,
       (a1, b1, c1)     IN (select * from t_in) pred_in,
       (a1, b1, c1) NOT IN (select * from t_in) pred_not_in
from t_out where subcase = 'B.4';

select case when count(*) > 0 then 'T' else 'F' end as pred_in from t_out
where subcase = 'A.1' and
      (a1, b1, c1) IN (select * from t_in where a1 = 'no_match');

select case when count(*) > 0 then 'T' else 'F' end as pred_not_in from t_out
where subcase = 'A.1' and
      (a1, b1, c1) NOT IN (select * from t_in where a1 = 'no_match');

select case when count(*) > 0 then 'T' else 'F' end as not_pred_in from t_out
where subcase = 'A.1' and
      NOT((a1, b1, c1) IN (select * from t_in where a1 = 'no_match'));

select case when count(*) > 0 then 'T' else 'F' end as pred_in from t_out
where subcase = 'A.3' and
      (a1, b1, c1) IN (select * from t_in);

select case when count(*) > 0 then 'T' else 'F' end as pred_not_in from t_out
where subcase = 'A.3' and
      (a1, b1, c1) NOT IN (select * from t_in);

select case when count(*) > 0 then 'T' else 'F' end as not_pred_in from t_out
where subcase = 'A.3' and
      NOT((a1, b1, c1) IN (select * from t_in));

select case when count(*) > 0 then 'N' else 'wrong result' end as pred_in from t_out
where subcase = 'A.3' and
      ((a1, b1, c1) IN (select * from t_in)) is NULL and
      ((a1, b1, c1) NOT IN (select * from t_in)) is NULL;

select case when count(*) > 0 then 'T' else 'F' end as pred_in from t_out
where subcase = 'A.4' and
      (a1, b1, c1) IN (select * from t_in);

select case when count(*) > 0 then 'T' else 'F' end as pred_not_in from t_out
where subcase = 'A.4' and
      (a1, b1, c1) NOT IN (select * from t_in);

select case when count(*) > 0 then 'T' else 'F' end as not_pred_in from t_out
where subcase = 'A.4' and
      NOT((a1, b1, c1) IN (select * from t_in));

select case when count(*) > 0 then 'T' else 'F' end as pred_in from t_out
where subcase = 'B.1' and
      (a1, b1, c1) IN (select * from t_in where a1 = 'no_match');

select case when count(*) > 0 then 'T' else 'F' end as pred_not_in from t_out
where subcase = 'B.1' and
      (a1, b1, c1) NOT IN (select * from t_in where a1 = 'no_match');

select case when count(*) > 0 then 'T' else 'F' end as not_pred_in from t_out
where subcase = 'B.1' and
      NOT((a1, b1, c1) IN (select * from t_in where a1 = 'no_match'));

select case when count(*) > 0 then 'T' else 'F' end as pred_in from t_out
where subcase = 'B.2' and
      (a1, b1, c1) IN (select * from t_in);

select case when count(*) > 0 then 'T' else 'F' end as pred_not_in from t_out
where subcase = 'B.2' and
      (a1, b1, c1) NOT IN (select * from t_in);

select case when count(*) > 0 then 'T' else 'F' end as not_pred_in from t_out
where subcase = 'B.2' and
      NOT((a1, b1, c1) IN (select * from t_in));

select case when count(*) > 0 then 'T' else 'F' end as pred_in from t_out
where subcase = 'B.3' and
      (a1, b1, c1) IN (select * from t_in);

select case when count(*) > 0 then 'T' else 'F' end as pred_not_in from t_out
where subcase = 'B.3' and
      (a1, b1, c1) NOT IN (select * from t_in);

select case when count(*) > 0 then 'T' else 'F' end as not_pred_in from t_out
where subcase = 'B.3' and
      NOT((a1, b1, c1) IN (select * from t_in));

select case when count(*) > 0 then 'N' else 'wrong result' end as pred_in from t_out
where subcase = 'B.3' and
      ((a1, b1, c1) IN (select * from t_in)) is NULL and
      ((a1, b1, c1) NOT IN (select * from t_in)) is NULL;

select case when count(*) > 0 then 'T' else 'F' end as pred_in from t_out
where subcase = 'B.4' and
      (a1, b1, c1) IN (select * from t_in);

select case when count(*) > 0 then 'T' else 'F' end as pred_not_in from t_out
where subcase = 'B.4' and
      (a1, b1, c1) NOT IN (select * from t_in);

select case when count(*) > 0 then 'T' else 'F' end as not_pred_in from t_out
where subcase = 'B.4' and
      NOT((a1, b1, c1) IN (select * from t_in));

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
DROP TABLE IF EXISTS t1;
drop table if exists t2;

create table t1 (a int, b int);
create table t2 (a int, b int);
select * from t1 where (a,b) in (select a,b from t2);
DROP TABLE IF EXISTS t1;
drop table if exists t2;

create table t1 (a int);
insert into t1 values (1), (2), (3);
-- @bvt:issue#3312
SELECT 1 FROM t1 WHERE (SELECT 1) in (SELECT 1);
-- @bvt:issue
DROP TABLE IF EXISTS t1;

DROP TABLE IF EXISTS t1;
drop table if exists t2;

CREATE TABLE t1( a INT );
INSERT INTO t1 VALUES (1),(2);
CREATE TABLE t2( a INT, b INT );
SELECT * FROM t2 WHERE (a, b) IN (SELECT a, b FROM t2);
-- error
SELECT * FROM t1 WHERE a IN    ( SELECT 1 UNION ( SELECT 1 UNION SELECT 1 ) );
SELECT * FROM t1 WHERE a IN    ( ( SELECT 1 UNION SELECT 1 )  UNION SELECT 1 );
SELECT * FROM t1 WHERE a IN    ( SELECT 1 UNION SELECT 1 UNION SELECT 1 );

DROP TABLE IF EXISTS t1;
drop table if exists t2;
drop table if exists t3;
drop table if exists t4;
drop table if exists t5;
create table t0 (a int);
insert into t0 values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);
create table t1 (
  a int(11) default null,
  b int(11) default null
);
insert into t1 select A.a+10*(B.a+10*C.a),A.a+10*(B.a+10*C.a) from t0 A, t0 B, t0 C;
create table t2 (a int(11) default null);
insert into t2 values (0),(1);
create table t3 (a int(11) default null);
insert into t3 values (0),(1);
create table t4 (a int(11) default null);
insert into t4 values (0),(1);
create table t5 (a int(11) default null);
insert into t5 values (0),(1),(0),(1);
select * from t2, t3
where
    t2.a < 10 and
    t3.a+1 = 2 and
    t3.a in (select t1.b from t1
                 where t1.a+1=t1.a+1 and
                       t1.a < (select t4.a+10
                                  from t4, t5 limit 2));
DROP TABLE IF EXISTS t1;
drop table if exists t2;
drop table if exists t3;
drop table if exists t4;
drop table if exists t5;

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
INSERT INTO t1 VALUES (NULL, NULL);
SELECT i1, i2
FROM t1
WHERE (i1, i2)
      NOT IN (SELECT i1, i2 FROM t2);

drop table if exists t1;
drop table if exists t2;
CREATE TABLE t1 (a INT);
INSERT INTO t1 VALUES(1);
CREATE TABLE t2(a INT);
INSERT INTO t2 VALUES(1),(1),(1),(1),(1);
-- @bvt:issue#3312
SELECT
(
  (SELECT 1 IN (SELECT 1 FROM t1 AS x1))
  IN
  (
    SELECT 1 FROM t2
    WHERE a IN (SELECT 4 FROM t1 AS x2)
  )
) AS result
FROM t1 AS x3;
SELECT
(
  (36, (SELECT 1 IN (SELECT 1 FROM t1 AS x1)))
  IN
  (
    SELECT 36, 1 FROM t2
    WHERE a IN (SELECT 4 FROM t1 AS x2)
  )
) AS result
FROM t1 AS x3;
-- @bvt:issue
DROP TABLE IF EXISTS t1;
drop table if exists t2;

-- @case
-- @desc:test for [in] subquery with delete,update
-- @label:bvt
CREATE TABLE t1 (
  id int(11) default NULL
) ;
INSERT INTO t1 VALUES (1),(1),(2),(2),(1),(3);
CREATE TABLE t2 (
  id int(11) default NULL,
  name varchar(15) default NULL
) ;
INSERT INTO t2 VALUES (4,'vita'), (1,'vita'), (2,'vita'), (1,'vita');
-- @ignore{
update t2 set t2.name='lenka' where t2.id in (select id from t1);
select * from t2;
delete from t1 where t1.id in  (select id from t2);
select * from t1;
-- @ignore}
DROP TABLE IF EXISTS t1;
drop table if exists t2;

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


-- @case
-- @desc:test for [in] subquery with range access in a subquery
-- @label:bvt
DROP TABLE IF EXISTS t1;
drop table if exists t2;
CREATE TABLE t1(select_id BIGINT, values_id BIGINT);
INSERT INTO t1 VALUES (1, 1);
CREATE TABLE t2 (select_id BIGINT, values_id BIGINT);
INSERT INTO t2 VALUES (0, 1), (0, 2), (0, 3), (1, 5);
SELECT values_id FROM t1
WHERE values_id IN (SELECT values_id FROM t2  WHERE select_id IN (1, 0));
SELECT values_id FROM t1 WHERE values_id IN (SELECT values_id FROM t2 WHERE select_id BETWEEN 0 AND 1);
SELECT values_id FROM t1 WHERE values_id IN (SELECT values_id FROM t2 WHERE select_id = 0 OR select_id = 1);

DROP TABLE IF EXISTS t1;
drop table if exists t2;
CREATE TABLE t1 (a INT NOT NULL);
INSERT INTO t1 VALUES (1),(-1), (65),(66);
CREATE TABLE t2 (a INT UNSIGNED NOT NULL PRIMARY KEY);
INSERT INTO t2 VALUES (65),(66);
SELECT a FROM t1 WHERE a NOT IN (65,66);
SELECT a FROM t1 WHERE a NOT IN (SELECT a FROM t2);

DROP TABLE IF EXISTS t1;
drop table if exists t2;
CREATE TABLE t1 (a INT);
INSERT INTO t1 VALUES(1);
CREATE TABLE t2 (placeholder CHAR(11));
INSERT INTO t2 VALUES("placeholder");
-- @bvt:issue#3312
SELECT (1, 2) IN (SELECT t1.a, 2)         FROM t1 GROUP BY t1.a;
SELECT (1, 2) IN (SELECT t1.a, 2 FROM t2) FROM t1 GROUP BY t1.a;
-- @bvt:issue

DROP TABLE IF EXISTS t1;
drop table if exists t2;
CREATE TABLE t1 (a INT);
INSERT INTO t1 VALUES (1),(2),(3);
CREATE TABLE t2 (a INT);
INSERT INTO t1 VALUES (1),(2),(3);
SELECT 1 FROM t1 WHERE t1.a NOT IN (SELECT 1 FROM t1, t2 WHERE false);
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


-- @case
-- @desc:test for [in] subquery that is join
-- @label:bvt
DROP TABLE IF EXISTS t1;
drop table if exists t2;
drop table if exists t3;
create table t1 (a int, b int);
insert into t1 values (0,0),(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9);
create table t2 (a int, b int);
insert into t2 values (0,0),(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9);
-- @ignore{
update t2 set b=1;
create table t3 (a int, oref int);
insert into t3 values (1, 1), (NULL,1), (NULL,0);
-- @bvt:issue#3312
select a, oref,t3.a in (select t1.a from t1, t2 where t1.b=t2.a and t2.b=t3.oref) Z from t3;
-- @bvt:issue

DROP TABLE IF EXISTS t1;
drop table if exists t2;
drop table if exists t3;
create table t1 (a int NOT NULL, b int NOT NULL);
insert into t1 values (0,0),(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9);
create table t2 (a int, b int);
insert into t2 values (0,0),(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9);
update t2 set b=1;
create table t3 (a int, oref int);
insert into t3 values (1, 1), (NULL,1), (NULL,0);
-- @bvt:issue#3312
select a, oref,t3.a in (select t1.a from t1, t2 where t1.b=t2.a and t2.b=t3.oref) Z from t3;
-- @bvt:issue

DROP TABLE IF EXISTS t1;
drop table if exists t2;
drop table if exists t3;
create table t1 (oref int, grp int);
insert into t1 (oref, grp) values(1, 1),(1, 1);
create table t2 (oref int, a int);
insert into t2 values(1, NULL),(2, NULL);
select a, oref, a in (select count(*) from t1 group by grp having grp=t2.oref) Z from t2;

DROP TABLE IF EXISTS t1;
drop table if exists t2;
create table t1 (a int, b int, primary key (a));
insert into t1 values (1,1), (3,1),(100,1);
create table t2 (a int, b int);
insert into t2 values (1,1),(2,1),(NULL,1),(NULL,0);
-- @bvt:issue#3312
select a,b, a in (select a from t1 where t1.b = t2.b union select a from t1 where t1.b = t2.b) Z from t2 ;
select a,b, a in (select a from t1 where t1.b = t2.b) Z from t2 ;
-- @bvt:issue


DROP TABLE IF EXISTS t1;
drop table if exists t2;
drop table if exists t3;
drop table if exists t4;
create table t3 (a int);
insert into t3 values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);
create table t2 (a int, b int, oref int);
insert into t2 values (NULL,1, 100), (NULL,2, 100);
create table t1 (a int, b int, c int);
insert into t1 select 2*A, 2*A, 100 from t3;
create table t4 (x int);
insert into t4 select A.a + 10*B.a from t1 A, t1 B;
-- @bvt:issue#3312
select a,b, oref, (a,b) in (select a,b from t1 where c=t2.oref) z from t2;
-- @bvt:issue

DROP TABLE IF EXISTS t1;
drop table if exists t2;
drop table if exists t3;
drop table if exists t4;
create table t1 (oref char(4), grp int, ie1 int, ie2 int);
insert into t1 (oref, grp, ie1, ie2) values('aa', 10, 2, 1),('aa', 10, 1, 1),('aa', 20, 2, 1),('bb', 10, 3, 1),('cc', 10, 4, 2),('cc', 20, 3, 2),('ee', 10, 2, 1),('ee', 10, 1, 2),('ff', 20, 2, 2),('ff', 20, 1, 2);
create table t2 (oref char(4), a int, b int);
insert into t2 values('ee', NULL, 1),('bb', 2, 1),('ff', 2, 2),('cc', 3, NULL),('bb', NULL, NULL),('aa', 1, 1),('dd', 1, NULL);
-- @bvt:issue#3312
select oref, a, b, (a,b) in (select ie1,ie2 from t1 where oref=t2.oref) Z from t2 where a=3 and b is null ;
-- @bvt:issue
insert into t2 values ('new1', 10,10);
insert into t1 values ('new1', 1234, 10, NULL);
-- @bvt:issue#3312
select oref, a, b, (a,b) in (select ie1,ie2 from t1 where oref=t2.oref) Z from t2 where a=10 and b=10;
-- @bvt:issue

DROP TABLE IF EXISTS t1;
drop table if exists t2;
create table t1 (oref char(4), grp int, ie int);
insert into t1 (oref, grp, ie) values ('aa', 10, 2),('aa', 10, 1),('aa', 20, NULL),('bb', 10, 3),('cc', 10, 4),('cc', 20, NULL),('ee', 10, NULL),('ee', 10, NULL),('ff', 20, 2),('ff', 20, 1);
create table t2 (oref char(4), a int);
insert into t2 values('ee', NULL),('bb', 2),('ff', 2),('cc', 3),('aa', 1),('dd', NULL),('bb', NULL);
-- @bvt:issue#3312
select oref, a, a in (select ie from t1 where oref=t2.oref) Z from t2;
-- @bvt:issue
select oref, a from t2 where a in (select ie from t1 where oref=t2.oref);
select oref, a from t2 where a not in (select ie from t1 where oref=t2.oref);
-- @bvt:issue#3312
select oref, a, a in (select min(ie) from t1 where oref=t2.oref group by grp) Z from t2;
-- @bvt:issue
select oref, a from t2 where a in (select min(ie) from t1 where oref=t2.oref group by grp);
select oref, a from t2 where a not in (select min(ie) from t1 where oref=t2.oref group by grp);
update t1 set ie=3 where oref='ff' and ie=1;
-- @bvt:issue#3312
select oref, a, a in (select min(ie) from t1 where oref=t2.oref group by grp) Z from t2;
-- @bvt:issue
select oref, a from t2 where a in (select min(ie) from t1 where oref=t2.oref group by grp);
select oref, a from t2 where a not in (select min(ie) from t1 where oref=t2.oref group by grp);
-- @bvt:issue#3312
select oref, a, a in (select min(ie) from t1 where oref=t2.oref group by grp having min(ie) > 1) Z from t2;
-- @bvt:issue
select oref, a from t2 where a in (select min(ie) from t1 where oref=t2.oref group by grp having min(ie) > 1);
select oref, a from t2 where a not in (select min(ie) from t1 where oref=t2.oref group by grp having min(ie) > 1);

DROP TABLE IF EXISTS t1;
drop table if exists t2;
create table t1 (oref char(4), grp int, ie1 int, ie2 int);
insert into t1 (oref, grp, ie1, ie2) values ('aa', 10, 2, 1),('aa', 10, 1, 1),('aa', 20, 2, 1),('bb', 10, 3, 1),('cc', 10, 4, 2),('cc', 20, 3, 2),('ee', 10, 2, 1),('ee', 10, 1, 2),('ff', 20, 2, 2),('ff', 20, 1, 2);
create table t2 (oref char(4), a int, b int);
insert into t2 values('ee', NULL, 1),('bb', 2, 1), ('ff', 2, 2),('cc', 3, NULL),('bb', NULL, NULL),('aa', 1, 1),('dd', 1, NULL);
-- @bvt:issue#3312
select oref, a, b, (a,b) in (select ie1,ie2 from t1 where oref=t2.oref) Z from t2;
-- @bvt:issue
select oref, a, b from t2 where (a,b) in (select ie1,ie2 from t1 where oref=t2.oref);
select oref, a, b from t2 where (a,b) not in (select ie1,ie2 from t1 where oref=t2.oref);
-- @bvt:issue#3312
select oref, a, b,(a,b) in (select min(ie1),max(ie2) from t1 where oref=t2.oref group by grp) Z from t2;
-- @bvt:issue
select oref, a, b from t2 where (a,b) in (select min(ie1), max(ie2) from t1 where oref=t2.oref group by grp);
select oref, a, b from t2 where (a,b) not in (select min(ie1), max(ie2) from t1 where oref=t2.oref group by grp);

DROP TABLE IF EXISTS t1;
drop table if exists t2;
create table t1 (oref char(4), grp int, ie int primary key);
insert into t1 (oref, grp, ie) values('aa', 10, 2),('aa', 10, 1),('bb', 10, 3),('cc', 10, 4),('cc', 20, 5),('cc', 10, 6);
create table t2 (oref char(4), a int);
insert into t2 values  ('ee', NULL),('bb', 2),('cc', 5),('cc', 2),('cc', NULL),('aa', 1),('bb', NULL);
-- @bvt:issue#3312
select oref, a, a in (select ie from t1 where oref=t2.oref) z from t2;
-- @bvt:issue
select oref, a from t2 where a in (select ie from t1 where oref=t2.oref);
select oref, a from t2 where a not in (select ie from t1 where oref=t2.oref);
-- @bvt:issue#3312
select oref, a, a in (select min(ie) from t1 where oref=t2.oref group by grp) z from t2;
-- @bvt:issue

DROP TABLE IF EXISTS t1;
drop table if exists t2;
CREATE TABLE t1 (a int);
CREATE TABLE t2 (b int, PRIMARY KEY(b));
INSERT INTO t1 VALUES (1), (NULL), (4);
INSERT INTO t2 VALUES (3), (1),(2), (5), (4), (7), (6);
SELECT a FROM t1, t2 WHERE a=b AND (b NOT IN (SELECT a FROM t1));
SELECT a FROM t1, t2 WHERE a=b AND (b NOT IN (SELECT a FROM t1 WHERE a > 4));

DROP TABLE IF EXISTS t1;
drop table if exists t2;
CREATE TABLE t1 (id int);
CREATE TABLE t2 (id int PRIMARY KEY);
CREATE TABLE t3 (id int PRIMARY KEY, name varchar(10));
INSERT INTO t1 VALUES (2), (NULL), (3), (1);
INSERT INTO t2 VALUES (234), (345), (457);
INSERT INTO t3 VALUES (222,'bbb'), (333,'ccc'), (111,'aaa');
SELECT * FROM t1 WHERE t1.id NOT IN (SELECT t2.id FROM t2,t3  WHERE t3.name='xxx' AND t2.id=t3.id);
-- @bvt:issue#3312
SELECT (t1.id IN (SELECT t2.id FROM t2,t3  WHERE t3.name='xxx' AND t2.id=t3.id)) AS x FROM t1;
-- @bvt:issue
DROP TABLE IF EXISTS t1;
drop table if exists t2;
drop table if exists t3;

CREATE TABLE t1 (
  pk INT PRIMARY KEY,
  int_key INT,
  varchar_key VARCHAR(5) UNIQUE,
  varchar_nokey VARCHAR(5)
);
INSERT INTO t1 VALUES (9, 7,NULL,NULL), (10,8,'p' ,'p');
SELECT varchar_nokey FROM t1
WHERE NULL NOT IN (
 SELECT INNR.pk FROM t1 AS INNR2
   LEFT JOIN t1 AS INNR ON ( INNR2.int_key = INNR.int_key )
   WHERE INNR.varchar_key > 'n{'
);
DROP TABLE IF EXISTS t1;

drop table if exists t1;
drop table if exists t2;
CREATE TABLE t1(i INT);
INSERT INTO t1 VALUES (1), (2), (3);
CREATE TABLE t1s(i INT);
INSERT INTO t1s VALUES (10), (20), (30);
CREATE TABLE t2s(i INT);
INSERT INTO t2s VALUES (100), (200), (300);
SELECT * FROM t1
WHERE t1.i NOT IN
(
  SELECT t2s.i
  FROM
  t1s LEFT OUTER JOIN t2s ON t2s.i = t1s.i
  HAVING t2s.i = 999
);

SELECT * FROM t1
WHERE t1.I IN
(
  SELECT t2s.i
  FROM
  t1s LEFT OUTER JOIN t2s ON t2s.i = t1s.i
  HAVING t2s.i = 999
);

SELECT * FROM t1
WHERE NOT t1.I = ANY
(
  SELECT t2s.i
  FROM
  t1s LEFT OUTER JOIN t2s ON t2s.i = t1s.i
  HAVING t2s.i = 999
);

SELECT * FROM t1
 WHERE t1.i = ANY (
  SELECT t2s.i
  FROM
  t1s LEFT OUTER JOIN t2s ON t2s.i = t1s.i
  HAVING t2s.i = 999
 );

DROP TABLE IF EXISTS t1;
drop table if exists t2;
drop table if exists t3;
CREATE TABLE parent (id int);
INSERT INTO parent VALUES (1), (2);
CREATE TABLE child (parent_id int, other int);
INSERT INTO child VALUES (1,NULL);
SELECT    p.id, c.parent_id
FROM      parent p
LEFT JOIN child  c
ON        p.id = c.parent_id
WHERE     c.parent_id NOT IN (
              SELECT parent_id
              FROM   child
              WHERE  parent_id = 3
          );

SELECT    p.id, c.parent_id
FROM      parent p
LEFT JOIN child  c
ON        p.id = c.parent_id
WHERE     c.parent_id IN (
              SELECT parent_id
              FROM   child
              WHERE  parent_id = 3
          );

SELECT    p.id, c.parent_id
FROM      parent p
LEFT JOIN child  c
ON        p.id = c.parent_id
WHERE     c.parent_id IN (
              SELECT parent_id
              FROM   child
              WHERE  parent_id = 3
          );

DROP TABLE IF EXISTS parent;
DROP TABLE IF EXISTS child;

DROP TABLE IF EXISTS cc;
DROP TABLE IF EXISTS bb;
DROP TABLE IF EXISTS c;
DROP TABLE IF EXISTS b;
CREATE TABLE cc (
  pk INT,
  col_int_key INT,
  col_varchar_key VARCHAR(1),
  PRIMARY KEY (pk)
);
INSERT INTO cc VALUES (10,7,'v');
INSERT INTO cc VALUES (11,1,'r');

CREATE TABLE bb (
  pk INT,
  col_date_key DATE,
  PRIMARY KEY (pk)
);
INSERT INTO bb VALUES (10,'2002-02-21');

CREATE TABLE c (
  pk INT,
  col_int_key INT,
  col_varchar_key VARCHAR(1),
  PRIMARY KEY (pk)
);
INSERT INTO c VALUES (1,NULL,'w');
INSERT INTO c VALUES (19,NULL,'f');

CREATE TABLE b (
  pk INT,
  col_int_key INT,
  col_varchar_key VARCHAR(1),
  PRIMARY KEY (pk)
);
INSERT INTO b VALUES (1,7,'f');

-- @bvt:issue#4139
SELECT col_int_key
FROM b granparent1
WHERE (col_int_key, col_int_key) IN (
    SELECT parent1.pk, parent1.pk
    FROM bb parent1 JOIN cc parent2
                    ON parent2.col_varchar_key = parent2.col_varchar_key
    WHERE granparent1.col_varchar_key IN (
        SELECT col_varchar_key
        FROM c)
      AND parent1.pk = granparent1.col_int_key
    ORDER BY parent1.col_date_key
);
-- @bvt:issue

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
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t1xt2;

-- @case
-- @desc:test for wrong result for select NULL in (<SUBQUERY>)
-- @label:bvt
DROP TABLE IF EXISTS t1;
CREATE TABLE t1(a int );
INSERT INTO t1 VALUES(0);
-- @bvt:issue#3312
SELECT NULL IN (SELECT 1 FROM t1);
-- @bvt:issue
-- @bvt:issue#3312
SELECT (NULL AND 1) IN (SELECT 1 FROM t1);
-- @bvt:issue
-- @bvt:issue#3312
SELECT (NULL, 1) IN (SELECT 1,1 FROM t1);
SELECT (NULL, NULL) IN (SELECT 1,1 FROM t1);
-- @bvt:issue
-- @bvt:issue#3312
SELECT (NULL OR 1) IN (SELECT 1 FROM t1);
-- @bvt:issue
SELECT (NULL IS NULL) IN  (SELECT 1 FROM t1);
DELETE FROM t1;
-- @bvt:issue#3312
SELECT NULL IN (SELECT 1 FROM t1);
-- @bvt:issue
SELECT (NULL AND 1) IN (SELECT 1 FROM t1);
-- @bvt:issue#3312
SELECT (NULL, 1) IN (SELECT 1,1 FROM t1);
SELECT (NULL, NULL) IN (SELECT 1,1 FROM t1);
-- @bvt:issue
SELECT (NULL OR 1) IN (SELECT 1 FROM t1);
SELECT (NULL IS NULL) IN  (SELECT 1 FROM t1);

DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (a INTEGER);
INSERT INTO t1 VALUES (1), (2), (3);
-- @bvt:issue#4354
SELECT 2 IN ( SELECT 5 UNION SELECT NULL ) FROM t1;
-- @bvt:issue
DROP TABLE IF EXISTS t1;




