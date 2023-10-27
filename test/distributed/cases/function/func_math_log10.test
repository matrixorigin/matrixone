#SELECT, 嵌套
select log10(log10(10000)),log10(log10(10));

#SELECT
SELECT log10(1000);
SELECT log10(-10);
SELECT log10(0);


#EXTREME VALUE, 科学计数

select log10(0.00000000000000001);
select log10(2e2);
select log10(0.141241241241313);
select log10(-124314124124.12412341);


#NULL
select log10(null);

#INSERT
CREATE TABLE t1(a DOUBLE);
INSERT INTO t1 select (log10(56));
INSERT INTO t1 select (log10(100));
SELECT * FROM t1 ORDER BY a;
drop table t1;

#DATATYPE
create table t1(a tinyint, b SMALLINT, c bigint, d INT, e BIGINT, f FLOAT, g DOUBLE, h decimal(38,19));
insert into t1 values(1, 1, 2, 4, 5, 5.5, 31.13, 14.314);
select log10(a),log10(b),log10(c),log10(d),log10(e),log10(f),log10(g),log10(h) from t1;
drop table t1;

#算术操作
select log10(123.54-123.03);
select log10(123.54*0.34);
select log10(134)-log10(194);


#WHERE,distinct
drop table if exists t1;
create table t1(a int);
insert into t1 values(10), (100);
select distinct * from t1 where log10(a)>0;
drop table t1;

#ON CONDITION
create table t1(a INT, b int);
create table t2(a INT, b int);
insert into t1 values(2,4), (100,23);
insert into t2 values(10,100), (4,41);
SELECT t1.a, t2.a FROM t1 JOIN t2 ON (log10(t1.a) <> log10(t2.a));
drop table t1;
drop table t2;



#HAVING，比较操作
drop table if exists t1;
create table t1(a float,  b float);
insert into t1 values(14.413, 43.413), (8.123, 0.409);
select b from t1 group by b having log10(b)>0;
drop table t1;