
#ORDER BY, WHERE
create table t1(a int, b int, c int);
insert into t1 values(200,1,1),(100,1,2),(400,2,2),(300,2,1);
SELECT distinct 1 FROM t1 group by a order by any_value(count(*)-count(b));
SELECT distinct 1 FROM t1 group by a order by any_value(count(*))-any_value(count(b));
SELECT DISTINCT GP1.a AS g1 FROM t1 AS GP1
WHERE GP1.a >= 0
ORDER BY 2+ANY_VALUE(GP1.b) LIMIT 8;
drop table t1;

#SELECT 算式运算
create table t1(
a int,
b int,
c int
);
create table t2(
a int,
b int,
c int
);
insert into t1 values(1,10,34),(2,20,14);
insert into t2 values(1,-10,-45);
select ANY_VALUE(t1.b) from t1 left join t2 on t1.c=t1.b and t1.a=t1.c group by t1.a;
select 3+(5*ANY_VALUE(t1.b)) from t1 left join t2 on t1.c=t1.b and t1.a=t1.c group by t1.a;
delete from t1;
insert into t1 (a,b) values(1,10),(1,20),(2,30),(2,40);
select any_value(a), sum(b) from t1;
select any_value(a), sum(b) from (select * from t1 order by a desc) as d;
select a,any_value(b),sum(c) from t1 group by a;
select a,any_value(b),sum(c) from (select * from t1 order by a desc, b desc) as d group by a;
drop table t1;
drop table t2;

#NULL
select any_value(null);
#嵌套
SELECT any_value(floor(0.5413));

#算式操作
SELECT any_value(floor(0.5413))-any_value(ceiling(0.553));

#DATATYPE
create table t1(a tinyint, b SMALLINT, c BIGINT, d INT, e BIGINT, f FLOAT, g DOUBLE, h decimal(38,19), i DATE, k datetime, l TIMESTAMP, m char(255), n varchar(255));
insert into t1 values(1, 1, 2, 43, 5, 35.5, 31.133, 14.314, "2012-03-10", "2012-03-12 10:03:12", "2022-03-12 13:03:12", "ab23c", "d5cf");
insert into t1 values(71, 1, 2, 34, 5, 5.5, 341.13, 15.314, "2012-03-22", "2013-03-12 10:03:12", "2032-03-12 13:04:12", "abr23c", "3dcf");
insert into t1 values(1, 1, 21, 4, 54, 53.5, 431.13, 14.394, "2011-03-12", "2015-03-12 10:03:12", "2002-03-12 13:03:12", "afbc", "dct5f");
insert into t1 values(1, 71, 2, 34, 5, 5.5, 31.313, 124.314, "2012-01-12", "2019-03-12 10:03:12", "2013-03-12 13:03:12", "3abd1c", "dcvf");
select any_value(a) from t1;
select any_value(b) from t1;
select any_value(c) from t1;
select any_value(d) from t1;
select any_value(e) from t1;
select any_value(f) from t1;
select any_value(g) from t1;
select any_value(h) from t1;
select any_value(i) from t1;
select any_value(k) from t1;
select any_value(l) from t1;
select any_value(m) from t1;
select any_value(n) from t1;
drop table t1;

#0.5暂不支持time类型
#create table t1(a time)
#insert into t1 values("10:03:12");
#insert into t1 values("10:03:12");
#insert into t1 values("10:03:12");
#insert into t1 values("10:03:12");
#select any_value(a) from t1;
#drop table t1;

#EXTREME VALUE
--- @bvt:issue#3579
select any_value(9999999999999999999999999999.9999999999);
--- @bvt:issue
select any_value("0000-00-00 00:00:00");
select any_value("你好");

#WHERE, INSERT, distinct
drop table if exists t1;
create table t1(a INT,  b float);
insert into t1 values(12124, -4213.413), (12124, -42413.409);
select distinct * from t1 where any_value(a)>12100;
drop table t1;


#ON CONDITION
drop table if exists t1;
drop table if exists t2;
create table t1(a INT,  b float);
create table t2(a INT,  b float);
insert into t1 values(12124, -4213.413), (1212, -42413.409);
insert into t2 values(14124, -4213.413), (8479, -980.409);
SELECT t1.a, t2.a FROM t1 JOIN t2 ON (any_value(t1.b) = any_value(t2.b));
drop table t1;
drop table t2;

#HAVING，比较操作
drop table if exists t1;
create table t1(a float);
insert into t1 values(14124.413), (-4213.413), (984798.123), (-980.409);
select a from t1 group by a having any_value(a)<0;
drop table t1;



