
-- test keyword distinct
drop table if exists t1;

create table t1(
a int,
b varchar(10)
);

insert into t1 values (111, 'a'),(110, 'a'),(100, 'a'),(000, 'b'),(001, 'b'),(011,'b');

select distinct b from t1;
select distinct b, a from t1;

select count(distinct a) from t1;
select sum(distinct a) from t1;
select avg(distinct a) from t1;
select min(distinct a) from t1;
select max(distinct a) from t1;

drop table t1;

-- test values is NULL or empty
drop table if exists t2;
create table t2(a int, b varchar(10));

insert into t2 values (1, 'a');
insert into t2 values (2, NULL);
insert into t2 values (NULL, 'b');
insert into t2 values (NULL, '');
insert into t2 values (3, '');
insert into t2 values (NULL, NULL);

select distinct a from t2;
select distinct b from t2;
select distinct a, b from t2;

drop table t2;


drop table if exists t3;
create table t3 (i int, j int);
insert into t3 values (1,1), (1,2), (2,3), (2,4);
select i, count(distinct j) from t3 group by i;

-- @bvt:issue#4797
select i+0.0 as i2, count(distinct j) from t3 group by i2;
-- @bvt:issue

select i+0.0 as i2, count(distinct j) from t3 group by i;

drop table t3;


drop table if exists t4;
CREATE TABLE t4 (a INT, b INT);
INSERT INTO t4 VALUES (1,1),(1,2),(2,3);

-- echo error
SELECT (SELECT COUNT(DISTINCT t4.b)) FROM t4 GROUP BY t4.a;

SELECT (SELECT COUNT(DISTINCT 12)) FROM t4 GROUP BY t4.a;

drop table t4;


drop table if exists t5;
create table t5 (ff double);

insert into t5 values (2.2);
select cast(sum(distinct ff) as decimal(5,2)) from t5;
select cast(sum(distinct ff) as signed) from t5;
select cast(variance(ff) as decimal(10,3)) from t5;
select cast(min(ff) as decimal(5,2)) from t5;

drop table t5;


drop table if exists t6;
create table t6 (df decimal(5,1));

insert into t6 values(1.1);
insert into t6 values(2.2);
select cast(sum(distinct df) as signed) from t6;
select cast(min(df) as signed) from t6;
select 1e8 * sum(distinct df) from t6;
select 1e8 * min(df) from t6;

drop table t6;


-- test space key
drop table if exists t7;
CREATE TABLE t7 (a VARCHAR(400));

INSERT INTO t7 (a) VALUES ("A"), ("a"), ("a "), ("a   "),
                          ("B"), ("b"), ("b "), ("b   ");

select * from t7;
SELECT COUNT(DISTINCT a) FROM t7;

DROP TABLE t7;


drop table if exists t8;
CREATE TABLE t8 (a INT, b INT);

INSERT INTO t8 VALUES (1,1),(1,2),(1,3),(1,4),(1,5),(1,6),(1,7),(1,8);
INSERT INTO t8 SELECT a, b+8       FROM t8;
INSERT INTO t8 SELECT a, b+16      FROM t8;
INSERT INTO t8 SELECT a, b+32      FROM t8;
INSERT INTO t8 SELECT a, b+64      FROM t8;
INSERT INTO t8 SELECT a, b+128     FROM t8;
INSERT INTO t8 SELECT a, b+256     FROM t8;
INSERT INTO t8 SELECT a, b+512     FROM t8;
INSERT INTO t8 SELECT a, b+1024    FROM t8;
INSERT INTO t8 SELECT a, b+2048    FROM t8;
INSERT INTO t8 SELECT a, b+4096    FROM t8;
INSERT INTO t8 SELECT a, b+8192    FROM t8;
INSERT INTO t8 SELECT a, b+16384   FROM t8;
INSERT INTO t8 SELECT a, b+32768   FROM t8;

-- echo error mag
SELECT a,COUNT(DISTINCT b) AS cnt FROM t8 GROUP BY a HAVING cnt > 50;
SELECT a,SUM(DISTINCT b) AS sumation FROM t8 GROUP BY a HAVING sumation > 50;
SELECT a,AVG(DISTINCT b) AS average FROM t8 GROUP BY a HAVING average > 50;

DROP TABLE t8;

drop table if exists t9;
CREATE TABLE t9 (a INT);
INSERT INTO t9 values (),(),();

select distinct * from t9;

drop table t9;


drop table if exists t10;
CREATE TABLE t10 (col_int_nokey int(11));

INSERT INTO t10 VALUES (7),(8),(NULL);
SELECT AVG(DISTINCT col_int_nokey) FROM t10;
SELECT AVG(DISTINCT outr.col_int_nokey) FROM t10 AS outr LEFT JOIN t10 AS outr2 ON
outr.col_int_nokey = outr2.col_int_nokey;

DROP TABLE t10;


drop table if exists t11;
CREATE TABLE t11(c1 CHAR(30));
INSERT INTO t11 VALUES('111'),('222');

SELECT DISTINCT substr(c1, 1, 2147483647) FROM t11;
SELECT DISTINCT substr(c1, 1, 2147483648) FROM t11;
SELECT DISTINCT substr(c1, -1, 2147483648) FROM t11;
SELECT DISTINCT substr(c1, -2147483647, 2147483648) FROM t11;
SELECT DISTINCT substr(c1, 9223372036854775807, 23) FROM t11;

DROP TABLE t11;


drop table if exists t12;
drop view if exists v1;
create table t12(pk int primary key);
create view v1 as select pk from t12 where pk < 20;

insert into t12 values (1), (2), (3), (4);
select distinct pk from v1;

insert into t12 values (5), (6), (7);
select distinct pk from v1;

drop view v1;
drop table t12;


SELECT AVG(2), BIT_AND(2), BIT_OR(2), BIT_XOR(2);

select count(*);

select COUNT(12), COUNT(DISTINCT 12), MIN(2),MAX(2),STD(2), VARIANCE(2),SUM(2);

drop table if exists t13;
CREATE TABLE t13(product VARCHAR(32),country_id INTEGER NOT NULL,year INTEGER,profit INTEGER);
INSERT INTO t13 VALUES ( 'Computer', 2,2000, 1200),
( 'TV', 1, 1999, 150),
( 'Calculator', 1, 1999,50),
( 'Computer', 1, 1999,1500),
( 'Computer', 1, 2000,1500),
( 'TV', 1, 2000, 150),
( 'TV', 2, 2000, 100),
( 'TV', 2, 2000, 100),
( 'Calculator', 1, 2000,75),
( 'Calculator', 2, 2000,75),
( 'TV', 1, 1999, 100),
( 'Computer', 1, 1999,1200),
( 'Computer', 2, 2000,1500),
( 'Calculator', 2, 2000,75),
( 'Phone', 3, 2003,10);

SELECT product, country_id, COUNT(*), COUNT(distinct year) FROM t13 GROUP BY product, country_id order by product;

drop table t13;