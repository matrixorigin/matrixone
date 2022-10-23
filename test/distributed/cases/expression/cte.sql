-- @suit

-- @case
-- @desc:test for with clause
-- @label:bvt
drop table if exists t1;
create table t1(a int, b int, c int);
insert into t1 values(null,null,null),(2,3,4);

WITH qn AS (SELECT a FROM t1) SELECT * FROM qn;
WITH qn AS (SELECT a FROM t1), qn2 as (select b from t1)
SELECT * FROM qn;
WITH qn AS (SELECT a FROM t1), qn2 as (select b from t1)
SELECT * FROM qn2;
-- error
WITH qn AS (SELECT a FROM t1), qn as (select b from t1)
SELECT 1 FROM qn;
-- error parser
with test.qn as (select "with") select * from test.qn;
with qn as (select "with" as a)
with qn2 as (select "with" as a)
select a from qn;
with qne as (select a from t1),
     qnm as (select a from t1),
     qnea as (select a from t1),
     qnma as (select a from t1)
select qne.a,qnm.a,alias1.a,alias2.a
from qne, qnm, qnea as alias1, qnma as alias2 limit 2;

-- @case
-- @desc:test for with multiple refs
-- @label:bvt
drop table if exists t1;
create table t1(a int, b int, c int);
insert into t1 values(null,null,null),(2,3,4);

WITH qn AS (SELECT b as a FROM t1)
SELECT qn.a, qn2.a  FROM qn, qn as qn2;
WITH qn AS (SELECT b as a FROM t1),
qn2 AS (SELECT c FROM t1 WHERE a IS NULL or a>0)
SELECT qn.a, qn2.c  FROM qn, qn2;

-- @case
-- @desc:test for with multiple refs intersection
-- @label:bvt
drop table if exists t1;
create table t1(a int, b int, c int);
insert into t1 values(null,null,null),(2,3,4);
WITH qn AS (SELECT 10*a as a FROM t1),qn2 AS (SELECT 3*a FROM qn)
SELECT * from qn2;

WITH qn AS (SELECT a FROM t1), qn2 AS (SELECT a FROM qn)
SELECT * from qn2;

WITH qn AS (SELECT b as a FROM t1),
qn2 AS (SELECT a FROM qn WHERE a IS NULL or a>0)
SELECT qn.a, qn2.a  FROM qn, qn2;

with qn0 as (select 1), qn1 as (select * from qn0), qn2 as (select 1), qn3 as (select 1 from qn1, qn2) select 1 from qn3;

-- error
WITH qn2 AS (SELECT a FROM qn WHERE a IS NULL or a>0),
qn AS (SELECT b as a FROM t1)
SELECT qn2.a  FROM qn2;

-- error
with qn1 as (with qn3 as (select * from qn2) select * from qn3),
     qn2 as (select 1)
select * from qn1;

-- error
WITH qn2 AS (SELECT a FROM qn WHERE a IS NULL or a>0),
qn AS (SELECT b as a FROM qn2)
SELECT qn.a  FROM qn;

-- @case
-- @desc:test for with no refs
-- @label:bvt
drop table if exists t1;
create table t1(a int, b int, c int);
insert into t1 values(null,null,null),(2,3,4);
with qn as (select 1) select 2;

-- @case
-- @desc:test for with subquery
-- @label:bvt
drop table if exists t1;
create table t1(a int, b int, c int);
insert into t1 values(null,null,null),(2,3,4),(4,5,6);
with qn as (select * from t1) select (select max(a) from qn);
-- ref defined in subquery
SELECT (WITH qn AS (SELECT 10*a as a FROM t1),
        qn2 AS (SELECT 3*a AS b FROM qn)
        SELECT * from qn2 LIMIT 1)
FROM t1;

SELECT *
FROM (WITH qn AS (SELECT 10*a as a FROM t1),
      qn2 AS (SELECT 3*a AS b FROM qn)
      SELECT * from qn2)
AS dt;

with qn as (select * from t1 limit 10)
select (select max(a) from qn where a=0),
       (select min(b) from qn where b=3);

drop table if exists sales_days;
create table sales_days(day_of_sale DATE, amount INT);
insert into sales_days values('2015-01-02', 100), ('2015-01-05', 200),('2015-02-02', 10),  ('2015-02-10', 100),('2015-03-02', 10),  ('2015-03-18', 1);

with sales_by_month(month,total) as
 (select month(day_of_sale), sum(amount) from sales_days
  where year(day_of_sale)=2015
  group by month(day_of_sale)),
 best_month(month, total, award) as
 (select month, total, "best" from sales_by_month
  where total=(select max(total) from sales_by_month)),
 worst_month(month, total, award) as
 (select month, total, "worst" from sales_by_month
  where total=(select min(total) from sales_by_month))
 select * from best_month union all select * from worst_month;

drop table if exists sales_days;

drop table if exists t1;
create table t1(a int);
insert into t1 values(1),(2);

with qn(a) as (select 1 from t1 limit 2)
select * from qn where qn.a=(select * from qn qn1 limit 1) union select 2;

-- @case
-- @desc:test for with  with-nested
-- @label:bvt
drop table if exists t1;
create table t1(a int, b int, c int);
insert into t1 values(null,null,null),(2,3,4),(4,5,6);
with qn as
  (with qn2 as (select "qn2" as a from t1) select "qn", a from qn2)
select * from qn;
-- @bvt:issue#3304
SELECT (WITH qn AS (SELECT t2.a*a as a FROM t1),
        qn2 AS (SELECT 3*a AS b FROM qn)
        SELECT * from qn2 LIMIT 1)
FROM t1 as t2;
-- @bvt:issue

WITH qn AS (SELECT b as a FROM t1)
SELECT (WITH qn2 AS (SELECT a FROM qn WHERE a IS NULL or a>0)
        SELECT qn2.a FROM qn2) FROM qn;

WITH qn AS (select "outer" as a)
SELECT (WITH qn AS (SELECT "inner" as a) SELECT a from qn),
       qn.a
FROM qn;

-- @case
-- @desc:test for with insert select
-- @label:bvt
drop table if exists t1;
drop table if exists t2;
create table t1(a int, b int, c int);
create table t2(a int);
insert into t1 values(null,null,null),(2,3,4),(4,5,6);
INSERT INTO t2
WITH qn AS (SELECT 10*a as a FROM t1),
      qn2 AS (SELECT 3*a AS b FROM qn)
      SELECT * from qn2;
SELECT * FROM t2;
drop table if exists t1;
drop table if exists t2;

-- @case
-- @desc:test for with order by ,limit .etc
-- @label:bvt
drop table if exists t1;
drop table if exists t2;
create table t1(a int, b int, c int);
insert into t1 values(null,null,null),(2,3,4),(4,5,6);
with qn as (select a from t1 order by 1)
select a from qn;

with qn as (select a from t1 order by 1)
select qn.a from qn, t1 as t2;

with qn as (select a from t1 order by 1 limit 10)
select qn.a from qn, t1 as t2;

-- @case
-- @desc:test for with group by
-- @label:bvt
drop table if exists t1;
drop table if exists t2;
create table t1(a int, b int, c int);
insert into t1 values(null,null,null),(2,3,4),(4,5,6);
with qn as (select a, b from t1)
select b from qn group by a;

with qn as (select a, b from t1 where a=b)
select b from qn group by a;

with qn as (select a, sum(b) as s from t1 group by a)
select s from qn group by a;

-- @case
-- @desc:test for with using column in name
-- @label:bvt
drop table if exists t1;
drop table if exists t2;
create table t1(a int, b int, c int);
insert into t1 values(null,null,null),(2,3,4),(4,5,6),(4,5,6),(8,9,10);
-- error
with qn () as (select 1) select * from qn, qn qn1;
with qn (foo, bar) as (select 1) select * from qn, qn qn1;
with qn as (select 1,1) select * from qn;
with qn as (select 1,1 from t1) select * from qn;
with qn (foo, foo) as (select 1,2) select * from qn;

with qn (foo, bar) as (select 1,1 from t1) select * from qn;
with qn (foo, bar) as (select 1,1) select * from qn;
with qn (foo, bar) as (select 1, 2 from t1 limit 2) select * from qn, qn qn1;
with qn (foo, bar) as (select 1 as col, 2 as coll from t1 limit 2) select * from qn, qn qn1;
with qn (foo, bar) as (select 1 as col, 2 as coll union
                       select a,b from t1 order by col) select qn1.bar from qn qn1;
with qn (foo, bar) as (select a, b from t1 limit 2) select qn.bar,foo from qn;

-- @case
-- @desc:test for with-as with where filtler and in ,some ,any
-- @label:bvt
drop table if exists t1;
drop table if exists t2;
DROP TABLE IF EXISTS t3;
create table t1 (s1 char(5), index s1(s1));
create table t2 (s1 char(5), index s1(s1));
insert into t1 values ('a1'),('a2'),('a3');
insert into t2 values ('a1'),('a2');
-- @bvt:issue#3312
with qn as (SELECT s1 FROM t2)
select s1, s1 = ANY (select * from qn) from t1;
with qn as (SELECT s1 FROM t2)
select s1, s1 < ANY (select * from qn) from t1;
with qn as (SELECT s1 FROM t2)
select s1, s1 = ANY (select * from qn) from t1;
-- @bvt:issue

drop table if exists t1;
drop table if exists t2;
DROP TABLE IF EXISTS t3;
create table t1 (a int);
create table t2 (a int, b int);
create table t3 (a int);
create table t4 (a int not null, b int not null);
insert into t1 values (2);
insert into t2 values (1,7),(2,7),(2,9);
insert into t4 values (4,8),(3,8),(5,9);
insert into t3 values(1),(0),(2),(9);
insert into t2 values (100, 5);
with qn as (select b from t2)
select * from t3 where a in (select * from qn);

with qn as (select b from t2 where b > 7)
select * from t3 where a in (select * from qn);

with qn as (select b from t2 where b > 7)
select * from t3 where a not in (select * from qn);

drop table if exists t1;
drop table if exists t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t4;
DROP TABLE IF EXISTS t5;
DROP TABLE IF EXISTS t6;
DROP TABLE IF EXISTS t7;
create table t1 (a int);
create table t2 (a int, b int);
create table t3 (a int);
create table t4 (a int not null, b int not null);
insert into t1 values (2);
insert into t2 values (1,7),(2,7);
insert into t4 values (4,8),(3,8),(5,9);
insert into t3 values (6),(7),(3);
-- @bvt:issue#3304
with qn as (select * from t2 where t2.b=t3.a)
select * from t3 where exists (select * from qn);

with qn as (select * from t2 where t2.b=t3.a)
select * from t3 where not exists (select * from qn);
-- @bvt:issue
drop table if exists t1;
drop table if exists t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t4;
DROP TABLE IF EXISTS t5;
DROP TABLE IF EXISTS t6;
DROP TABLE IF EXISTS t7;

-- @case
-- @desc:test for with-as with lots of expression
-- @label:bvt
drop table if exists `t`;
CREATE TABLE `t` (
  `c1` int(11) DEFAULT NULL,
  `c2` int(11) DEFAULT NULL,
  `c3` int(11) DEFAULT NULL,
  `c4` int(11) DEFAULT NULL,
  `c5` int(11) DEFAULT NULL,
  `c6` int(11) DEFAULT NULL,
  `c7` int(11) DEFAULT NULL,
  `c8` int(11) DEFAULT NULL,
  `c9` int(11) DEFAULT NULL,
  `c10` int(11) DEFAULT NULL,
  `c11` int(11) DEFAULT NULL,
  `c12` int(11) DEFAULT NULL,
  `c13` int(11) DEFAULT NULL,
  `c14` int(11) DEFAULT NULL,
  `c15` int(11) DEFAULT NULL,
  `c16` int(11) DEFAULT NULL,
  `c17` int(11) DEFAULT NULL,
  `c18` int(11) DEFAULT NULL,
  `c19` int(11) DEFAULT NULL,
  `c20` int(11) DEFAULT NULL,
  `c21` int(11) DEFAULT NULL,
  `c22` int(11) DEFAULT NULL,
  `c23` int(11) DEFAULT NULL,
  `c24` int(11) DEFAULT NULL,
  `c25` int(11) DEFAULT NULL,
  `c26` int(11) DEFAULT NULL,
  `c27` int(11) DEFAULT NULL,
  `c28` int(11) DEFAULT NULL,
  `c29` int(11) DEFAULT NULL,
  `c30` int(11) DEFAULT NULL,
  `c31` int(11) DEFAULT NULL
);
with qn as (select * from t limit 2)
select
(select max(c1) from qn where qn.c1=1),
(select max(c2) from qn where qn.c2=1),
(select max(c3) from qn where qn.c3=1),
(select max(c4) from qn where qn.c4=1),
(select max(c5) from qn where qn.c5=1),
(select max(c6) from qn where qn.c6=1),
(select max(c7) from qn where qn.c7=1),
(select max(c8) from qn where qn.c8=1),
(select max(c9) from qn where qn.c9=1),
(select max(c10) from qn where qn.c10=1),
(select max(c11) from qn where qn.c11=1),
(select max(c12) from qn where qn.c12=1),
(select max(c13) from qn where qn.c13=1),
(select max(c14) from qn where qn.c14=1),
(select max(c15) from qn where qn.c15=1),
(select max(c16) from qn where qn.c16=1),
(select max(c17) from qn where qn.c17=1),
(select max(c18) from qn where qn.c18=1),
(select max(c19) from qn where qn.c19=1),
(select max(c20) from qn where qn.c20=1),
(select max(c21) from qn where qn.c21=1),
(select max(c22) from qn where qn.c22=1),
(select max(c23) from qn where qn.c23=1),
(select max(c24) from qn where qn.c24=1),
(select max(c25) from qn where qn.c25=1),
(select max(c26) from qn where qn.c26=1),
(select max(c27) from qn where qn.c27=1),
(select max(c28) from qn where qn.c28=1),
(select max(c29) from qn where qn.c29=1),
(select max(c30) from qn where qn.c30=1),
(select max(c31) from qn where qn.c31=1) from qn;
drop table if exists `t`;




