-- @suit

-- @case
-- @desc:test for case_when expression with constant operand
-- @label:bvt
select CASE "b" when "a" then 1 when "b" then 2 END;
select CASE "c" when "a" then 1 when "b" then 2 END;
select CASE "c" when "a" then 1 when "b" then 2 ELSE 3 END;
select CASE when 1=0 then "true" else "false" END;
select CASE 1 when 1 then "one" WHEN 2 then "two" ELSE "more" END;
select CASE 2.0 when 1 then "one" WHEN 2.0 then "two" ELSE "more" END;
-- @bvt:issue#3294
select (CASE "two" when "one" then "1" WHEN "two" then "2" END) | 0;
-- @bvt:issue
select (CASE "two" when "one" then 1.00 WHEN "two" then 2.00 END) +0.0;
select case 1/0 when "a" then "true" else "false" END;
select case 1/0 when "a" then "true" END;
-- @bvt:issue#3294
select (case 1/0 when "a" then "true" END) | 0;
-- @bvt:issue
select (case 1/0 when "a" then "true" END) + 0.0;
select case when 1>0 then "TRUE" else "FALSE" END;
select case when 1<0 then "TRUE" else "FALSE" END;
SELECT CAST(CASE WHEN 0 THEN '2001-01-01' END AS DATE);
SELECT CAST(CASE WHEN 0 THEN DATE'2001-01-01' END AS DATE);
select case 1.0 when 0.1 then "a" when 1.0 then "b" else "c" END;
select case 0.1 when 0.1 then "a" when 1.0 then "b" else "c" END;
select case 1 when 0.1 then "a" when 1.0 then "b" else "c" END;
select case 1.0 when 0.1 then "a" when 1 then "b" else "c" END;
select case 1.001 when 0.1 then "a" when 1 then "b" else "c" END;

-- @case
-- @desc:test for case_when expression with normal select
-- @label:bvt
drop table if exists t1;
drop table if exists t2;
CREATE TABLE t1 (a varchar(10), PRIMARY KEY (a));
CREATE TABLE t2 (a varchar(10), b date, PRIMARY KEY(a));
INSERT INTO t1 VALUES ('test1');
INSERT INTO t2 VALUES
('test1','2016-12-13'),('test2','2016-12-14'),('test3','2016-12-15');
-- @bvt:issue#3254
SELECT b, b = '20161213',
       CASE b WHEN '20161213' then 'found' ELSE 'not found' END FROM t2;
-- @bvt:issue


-- @case
-- @desc:test for case_when expression with group by
-- @label:bvt
drop table if exists t1;
create table t1 (a int);
insert into t1 values(1),(2),(3),(4);
select case a when 1 then 2 when 2 then 3 else 0 end as fcase, count(*) from t1 group by fcase;
select case a when 1 then 2 when 2 then 3 else 0 end as fcase, count(*) from t1 group by fcase;
select case a when 1 then "one" when 2 then "two" else "nothing" end as fcase, count(*) from t1 group by fcase;
drop table if exists t1;

-- @case
-- @desc:test for case_when expression with function
-- @label:bvt
create table t1 (`row` int not null, col int not null, val varchar(255) not null);
insert into t1 values (1,1,'orange'),(1,2,'large'),(2,1,'yellow'),(2,2,'medium'),(3,1,'green'),(3,2,'small');
select max(case col when 1 then val else null end) as color from t1 group by `row`;
drop table if exists t1;

create table t1(a float, b int default 3);
insert into t1 (a) values (2), (11), (8);
select min(a), min(case when 1=1 then a else NULL end),
  min(case when 1!=1 then NULL else a end)
from t1 where b=3 group by b;

drop table if exists  t1;
CREATE TABLE t1 (a INT, b INT);
INSERT INTO t1 VALUES (1,1),(2,1),(3,2),(4,2),(5,3),(6,3);
SELECT CASE WHEN AVG(a)>=0 THEN 'Positive' ELSE 'Negative' END FROM t1 GROUP BY b;

drop table if exists  t1;

-- @case
-- @desc:test for case_when expression with join
-- @label:bvt
drop table if exists  t1;
drop table if exists  t2;
create table t1 (a int, b bigint unsigned);
create table t2 (c int);
insert into t1 (a, b) values (1,4572794622775114594), (2,18196094287899841997),
  (3,11120436154190595086);
insert into t2 (c) values (1), (2), (3);
select t1.a, (case t1.a when 0 then 0 else t1.b end) d from t1
  join t2 on t1.a=t2.c order by d;
select t1.a, (case t1.a when 0 then 0 else t1.b end) d from t1
  join t2 on t1.a=t2.c where b=11120436154190595086 order by d;
drop table if exists small;
drop table if exists big;
CREATE TABLE small (id int not null,PRIMARY KEY (id));
CREATE TABLE big (id int not null,PRIMARY KEY (id));
INSERT INTO small VALUES (1), (2);
INSERT INTO big VALUES (1), (2), (3), (4);
SELECT big.*, dt.* FROM big LEFT JOIN (SELECT id as dt_id,
                           CASE id WHEN 0 THEN 0 ELSE 1 END AS simple,
                           CASE WHEN id=0 THEN NULL ELSE 1 END AS cond
                    FROM small) AS dt
     ON big.id=dt.dt_id;

drop table if exists small;
drop table if exists big;

-- @case
-- @desc:test for case_when expression with union
-- @label:bvt
SELECT 'case+union+test'
UNION
SELECT CASE '1' WHEN '2' THEN 'BUG' ELSE 'nobug' END;

-- @case
-- @desc:test for case_when expression in where filter
-- @label:bvt
drop table t1;
CREATE TABLE t1(a int);
insert into t1 values(1),(1),(2),(1),(3),(2),(1);
SELECT 1 FROM t1 WHERE a=1 AND CASE 1 WHEN a THEN 1 ELSE 1 END;
DROP TABLE if exists t1;

-- @case
-- @desc:test for case_when expression with count()
-- @label:bvt
DROP TABLE if exists t1;
create table t1 (USR_ID int not null, MAX_REQ int not null);
insert into t1 values (1, 3);
select count(*) + MAX_REQ - MAX_REQ + MAX_REQ - MAX_REQ + MAX_REQ - MAX_REQ + MAX_REQ - MAX_REQ + MAX_REQ - MAX_REQ from t1 group by MAX_REQ;
select Case When Count(*) < MAX_REQ Then 1 Else 0 End from t1 where t1.USR_ID = 1 group by MAX_REQ;
DROP TABLE if exists t1;