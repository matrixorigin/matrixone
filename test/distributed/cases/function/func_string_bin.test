#SELECT, 科学计数
select bin(0b11111111);
select bin(12);
SELECT bin(1314);
select bin(2e5);

#NULL
SELECT bin(null);

#嵌套
SELECT concat_ws(",", bin(1000), bin(2000));
#EXTREME VALUES
select bin(0);
select bin(-1);
select bin(10e50);
select bin(-10e50);
select bin(0.00000000000000000000000001);
select bin(-0.00000000000000000000000001);
select bin("你好");
create table t1(a int);
insert into t1 values();
select bin(a) from t1;
drop table t1;
#create table t1(a tinyint, b SMALLINT, c BIGINT, d INT, e BIGINT, f FLOAT, g DOUBLE, h decimal(38,19), i DATE, k datetime, l TIMESTAMP, m char(255), n varchar(255));
#insert into t1 values(1, 1, 2, 4, 5, 5.5, 31.13, 14.314, "2012-03-12", "2012-03-12 10:03:12", "2012-03-12 13:03:12", "abc", "dcf");
#insert into t1 values(1, 1, 2, 4, 5, 5.5, 31.13, 14.314, "2012-03-12", "2012-03-12 10:03:12", "2012-03-12 13:03:12", "abc", "dcf");
#insert into t1 values(1, 1, 2, 4, 5, 5.5, 31.13, 14.314, "2012-03-12", "2012-03-12 10:03:12", "2012-03-12 13:03:12", "abc", "dcf");
#insert into t1 values(1, 1, 2, 4, 5, 5.5, 31.13, 14.314, "2012-03-12", "2012-03-12 10:03:12", "2012-03-12 13:03:12", "abc", "dcf");
#select bin(a),bin(b),bin(c),bin(d),bin(e),bin(f),bin(g),bin(h),bin(i),bin(k),bin(l),bin(m),bin(n) from t1;
#drop table t1;
#create table t1(a time);
#insert into t1 values("10:03:12");
#select bin(a) from t1;
#drop table t1;
CREATE TABLE t1(a char(255), b varchar(255));
INSERT INTO t1 select bin(56), bin(234);
INSERT INTO t1 select bin(100), bin(234);
SELECT distinct bin(a), bin(b) FROM t1 ORDER BY bin(a);
drop table t1;
CREATE TABLE t1 (a int);
INSERT INTO t1 VALUES (100), (12);
SELECT a FROM t1
GROUP BY a
HAVING bin(a) <>0;
DROP TABLE t1;
drop table if exists t1;
CREATE TABLE t1 (a int);
CREATE TABLE t2 (a int);
INSERT INTO t1 VALUES (100), (200), (300), (10);
INSERT INTO t2 VALUES (100), (50), (20), (10), (300);
SELECT t1.a, t2.a FROM t1 JOIN t2 ON (bin(t1.a) = bin(t2.a));
drop table t1;
drop table t2;