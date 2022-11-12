#SELECT, 嵌套
select log(ln(10)),ln(log(2));

#SELECT
SELECT ln(2);
SELECT ln(-2);
SELECT ln(0);


#EXTREME VALUE, 科学计数

select ln(0.00000000000000001);
select ln(2e2);
select ln(0.141241241241313);
select ln(-124314124124.12412341);


#NULL
select ln(null);

#INSERT
CREATE TABLE t1(a DOUBLE);
INSERT INTO t1 select (ln(56));
INSERT INTO t1 select (ln(100));
SELECT * FROM t1 ORDER BY a;
drop table t1;

#DATATYPE
create table t1(a tinyint, b SMALLINT, c bigint, d INT, e BIGINT, f FLOAT, g DOUBLE, h decimal(38,19));
insert into t1 values(1, 1, 2, 4, 5, 5.5, 31.13, 14.314);
select ln(a),ln(b),ln(c),ln(d),ln(e),ln(f),ln(g),ln(h) from t1;
drop table t1;

#算术操作
select ln(123.54-123.03);
select ln(123.54*0.34);
select ln(134)-ln(194);


#WHERE,distinct
drop table if exists t1;
create table t1(a int);
insert into t1 values(10), (100);
select distinct * from t1 where ln(a)>0;
drop table t1;

#ON CONDITION
create table t1(a INT, b int);
create table t2(a INT, b int);
insert into t1 values(2,4), (100,23);
insert into t2 values(10,100), (4,41);
SELECT t1.a, t2.a FROM t1 JOIN t2 ON (ln(t1.a) <> ln(t2.a));
drop table t1;
drop table t2;



#HAVING，比较操作
drop table if exists t1;
create table t1(a float,  b float);
insert into t1 values(14.413, 43.413), (8.123, 0.409);
select b from t1 group by b having ln(b)>0;
drop table t1;

