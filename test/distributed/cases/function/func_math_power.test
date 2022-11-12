#SELECT, 嵌套
select power(exp(10), log(100)),exp(power(2,2)),power(-1,1),power(NULL,0),power(1,1),power(3,9),power(-1,2),power(NULL,2);

#SELECT
SELECT power(2,13);
SELECT power(-2,-3);
SELECT power(2,100);
SELECT power(10,100);
SELECT power(1,100);

#EXTREME VALUE, 科学计数法
select power(2,-1);
select power(-2,1);
select power(0.00000000000000001,123413);
select power(10e100,-39413312);
select power(0.141241241241313, 124314124124.12412341);


#NULL
select power(null,2);
select power(2, null);
select power(null,null);

#INSERT
CREATE TABLE t1(a DOUBLE);
INSERT INTO t1 select (power(56,124));
INSERT INTO t1 select (power(10,100));
INSERT INTO t1 select (power(2,234));
SELECT * FROM t1 ORDER BY a;
drop table t1;

#DATATYPE
create table t1(a tinyint, b SMALLINT, c bigint, d INT, e BIGINT, f FLOAT, g DOUBLE, h decimal(38,19));
insert into t1 values(1, 1, 2, 4, 5, 5.5, 31.13, 14.314);
select power(a,b),power(b,c),power(c,d),power(d,e),power(e,f),power(f,g),power(g,h) from t1;
drop table t1;

#算术操作
select power(123.54-123.03, 12-34);
select power(123.54*0.34, 1203-1200);
select power(134,34)-power(194,44);


#WHERE,distinct
drop table if exists t1;
create table t1(a float,  b float);
insert into t1 values(10, 100), (2, 5);
select distinct * from t1 where power(a, b)>0;
drop table t1;

#ON CONDITION
create table t1(a INT, b int);
create table t2(a INT, b int);
insert into t1 values(2,4), (100,23);
insert into t2 values(10,100), (4,41);
SELECT t1.a, t2.a FROM t1 JOIN t2 ON (power(t1.a, t1.b) <> power(t2.a, t2.b));
drop table t1;
drop table t2;


#HAVING，比较操作
drop table if exists t1;
create table t1(a float,  b float);
insert into t1 values(14.3, 4.413), (9.123, 9.409);
select b from t1 group by b having power(1,b)>0;
drop table t1;

