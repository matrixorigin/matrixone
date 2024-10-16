-- @suite
-- @case
-- @desc:test for numeric datatype
-- @label:bvt

#Test cases of query without table
select 0.00,1.11,1234567890.1234567890123456789;
select 1123.2333+1233.3331;
select cast(9223372.036854775808 as numeric)+1;

select round(cast(2320310.66666612312 as numeric));
select floor(cast(2231231.501231 as numeric));


#Test cases of query with single table
drop table if exists t1;
create table t1 (a numeric(29,0) not null, primary key(a));
-- @bvt:issue#3364
insert into t1 values (18446744073709551615), (0xFFFFFFFFFFFFFE), (18446744073709551613.0000000), (18446744073709551612.0000000001);
select * from t1 order by 1 asc;
select * from t1 where a=18446744073709551615 order by a desc;
delete from t1 where a=18446744073709551615.000000000;
select * from t1;
-- @bvt:issue
drop table t1;
create table t1 ( a int not null default 1, big numeric(29,11) );
insert into t1 (big) values (-1),(12.34567891234567),(92.23372036854775807);
select * from t1 order by a desc, big asc;
select min(big),max(big),max(big)-1 from t1;
select min(big),avg(big),max(big)-1 from t1 group by a order by 1+2;
-- @bvt:issue#3364
drop table t1;
create table t1 ( a int not null default 1, big numeric(20,4) primary key);
insert into t1 (big) values (0),(18446744073), (0xFFFFFE), (184467.13), (184462);
select * from t1 order by 1,2 desc;
select * from t1 order by big limit 1,2;
select * from t1 order by big limit 2 offset 1;
select min(big),max(big),max(big)-1 from t1;
select min(big),count(big),max(big)-1 from t1 group by a;
-- @bvt:issue

#Test cases of query with multi tables
drop table if exists t1;
drop table if exists t2;
create table t1 (
numericd  numeric(6,5) not null,
value32  integer          not null,
primary key(value32)
);
create table t2 (
numericd  numeric(5,4)  not null,
value32  integer          not null,
primary key(value32)
);
insert into t1 values(0.1715600000, 1);
insert into t1 values(9.2234, 2);
insert into t2 values(1.7156e-1, 3);
insert into t2 values(9.2233720368547758070000000000, 4);
select * from t1;
select * from t2;
select * from t1 join t2 on t1.numericd=t2.numericd order by 1 asc, 2 desc;
select * from t1 join t2 on t1.numericd=t2.numericd where t1.numericd!=0;
select * from t1 join t2 on t1.numericd=t2.numericd order by 1,2 desc;
drop table if exists t1;
drop table if exists t2;
create table t1 (numeric20 numeric(20,18) not null);
insert into t1 values (1.4e-19),(1.4e-18);
select * from t1;
drop table t1;
create table t1 (numeric_col numeric(29,0));
insert into t1 values (-17666000000000000000);
-- @bvt:issue#3364
select * from t1 where numeric_col=-17666000000000000000 order by 1 asc;
-- @bvt:issue
select * from t1 where numeric_col='-17666000000000000000' order by numeric_col desc;
drop table t1;

#Test cases of cast
-- @bvt:issue#4241
select cast(10000002383263201056 as numeric) mod 50 as result;
-- @bvt:issue
select cast(cast(19999999999999999999 as numeric) as unsigned);
CREATE TABLE t1 (id INT PRIMARY KEY,
a numeric(20),
b VARCHAR(20));
INSERT INTO t1 (id,a) VALUES
(1,0),
(2,CAST(0x7FFFFFFFFFFFFFFF AS UNSIGNED)),
(3,CAST(0x8000000000000000 AS UNSIGNED)),
(4,CAST(0xFFFFFFFFFFFFFFFF AS UNSIGNED));
UPDATE t1 SET b = a;
-- @bvt:issue#4383
select distinct * from t1 where ((a = '2147483647') and (b = '2147483647'));
select a,count(a) from t1 group by a having count(a)>=2;
-- @bvt:issue

#Test cases of operators
CREATE TABLE t_numeric(id numeric(10,5));
INSERT INTO t_numeric VALUES (1), (2),(1.099999999),(2.20000000001);
select * from t_numeric;
SELECT id, id >= 1.1 FROM t_numeric;
SELECT id, 1.1 <= id FROM t_numeric;
SELECT id, id = 1.1 FROM t_numeric;
SELECT id, 1.1 = id FROM t_numeric;
SELECT * from t_numeric WHERE id = 1.1;
SELECT * from t_numeric WHERE id = 1.1e0;
SELECT * from t_numeric WHERE id = '1.1';
SELECT * from t_numeric WHERE id = '1.1e0';
SELECT * from t_numeric WHERE id IN (1.1, 2.2);
SELECT * from t_numeric WHERE id IN (1.1e0, 2.2e0);
SELECT * from t_numeric WHERE id IN ('1.1', '2.2');
SELECT * from t_numeric WHERE id IN ('1.1e0', '2.2e0');
SELECT * from t_numeric WHERE id BETWEEN 1.1 AND 1.9;
SELECT * from t_numeric WHERE id BETWEEN 1.1e0 AND 1.9e0;
SELECT * from t_numeric WHERE id BETWEEN '1.1' AND '1.9';
SELECT * from t_numeric WHERE id BETWEEN '1.1e0' AND '1.9e0';
drop table t1;
CREATE TABLE t1 (a numeric(2,1));
INSERT INTO t1 VALUES (1),(0.8999),(0.9);
-- @bvt:issue#3185
SELECT * FROM t1 WHERE coalesce(a) BETWEEN 0 and 0.9;
SELECT * FROM t1 WHERE coalesce(a)=0.9;
SELECT * FROM t1 WHERE coalesce(a) in (0.8,0.9);
-- @bvt:issue
-- @bvt:issue#3280
SELECT * FROM t1 WHERE a BETWEEN 0 AND 0.9;
SELECT * FROM t1 WHERE a=0.9;
SELECT * FROM t1 WHERE a IN (0.8,0.9);
drop table t1;
create table t (id numeric(23,3) unsigned, b int);
insert into t values(889475494977969.3574,1);
insert into t values(889475494977969.3579,2);
insert into t values(889475494977969.357,3);
select count(*) from t
where id>=88947549497796.3574 and id <=889475494977969.358;
select count(*) from t
where id between 88947549497796.3574 and 889475494977969.358;
drop table t;
-- @bvt:issue
SELECT CAST(1.00 AS numeric) BETWEEN 1 AND -1;
SELECT CAST(1.00 AS numeric) NOT BETWEEN 1 AND -1;
SELECT CAST(-0 AS numeric) BETWEEN 0 AND -1;
SELECT CAST(0 AS numeric) NOT BETWEEN 0 AND -1;

#Test cases of update with single table
drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
drop table if exists t11;
drop table if exists t12;
CREATE TABLE t1 (a numeric(3,2), b numeric(5,2) primary key);
INSERT INTO t1 VALUES (1.00,1.0000),(1.00,2.0000);
-- @bvt:issue#3280
update t1 set a=2.00 where a=1 limit 1;
select * from t1;
INSERT INTO t1 VALUES (1,3);
update t1 set a=2 where a=1.00;
select * from t1;
-- @bvt:issue
drop table t1;
create table t1 (
a numeric(10,5) not null,
b int not null default 12346,
c numeric(10,5) not null default 12345.67890,
d numeric(10,5) not null default 12345.67890,
e numeric(10,5) not null default 12345.67890,
f numeric(10,5) not null default 12345.67890,
g numeric(10,5) not null default 12345.67890,
h numeric(10,5) not null default 12345.67890,
i numeric(10,5) not null default 12345.67890,
j numeric(10,5) not null default 12345.67890,
primary key (a));
insert into t1 (a) values (2),(4),(6),(8),(10),(12),(14),(16),(18),(20),(22),(24),(26),(23);
update t1 set a=a+101;
select a,b from t1 order by 1;
update t1 set a=27 where a=125;
select a,b from t1 order by 1;
update t1 set a=a-1 where 1 > 2;
select a,b from t1 order by 1;
update t1 set a=a-1 where 3 > 2;
select a,b from t1 order by 1;
drop table t1;
create table t1 (a numeric(10,5) primary key, b char(32));
insert into t1 values (1.000000,'apple'), (2.00,'apple');
select * from t1;

#Test case of delete with single table
drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
drop table if exists t11;
drop table if exists t12;
CREATE TABLE t1 (a numeric(3,2), b numeric(5,4) primary key);
INSERT INTO t1 VALUES (1,1),(1,2);
delete from t1 where a=1 limit 1;
select * from t1;
INSERT INTO t1 VALUES (1,3);
delete from t1 where a=1;
select * from t1;
drop table t1;
create table t1 (
a numeric(10,5) not null,
b int not null default 12346,
c numeric(10,5) not null default 12345.67890,
d numeric(10,5) not null default 12345.67890,
e numeric(10,5) not null default 12345.67890,
f numeric(10,5) not null default 12345.67890,
g numeric(10,5) not null default 12345.67890,
h numeric(10,5) not null default 12345.67890,
i numeric(10,5) not null default 12345.67890,
j numeric(10,5) not null default 12345.67890,
primary key (a));
insert into t1 (a) values (2.1111),(4),(00006.12311),(8.41231),(24.0000);
-- @bvt:issue#3280
delete from t1 where a=2+2.0000;
select a,b from t1 order by 1;
delete from t1 where  a=24.0000;
select a,b from t1 order by 1;
delete from t1 where  3 < 2;
select a,b from t1 order by 1;
delete from t1 where  1 < 2;
select a,b from t1 order by 1;
-- @bvt:issue
drop table t1;
create table t1 (a numeric(10,5) primary key, b char(32));
insert into t1 values (1.000000,'apple'), (2.00,'apple');
select * from t1;

drop table t1;
create table t1(a numeric(5,2));
insert into t1 values(0), (2.1), (2.994), (2.995);
select * from t1;
insert into t1 values(999.99);
insert into t1 values(-999.99);
insert into t1 values(999.994);
insert into t1 values(-999.994);
select * from t1;
insert into t1 values(999.995);
insert into t1 values(-999.995);

drop table t1;
create table t1(a numeric(17,2));
insert into t1 values(0), (2.1), (2.994), (2.995);
select * from t1;
insert into t1 values(999999999999999.99);
insert into t1 values(-999999999999999.99);
insert into t1 values(999999999999999.994);
insert into t1 values(-999999999999999.994);
select * from t1;
insert into t1 values(999999999999999.995);
insert into t1 values(-999999999999999.995);

drop table t1;
create table t1(a numeric(5,5));
insert into t1 values(0), (0.9), (0.99), (0.999), (0.9999), (0.99999), (0.999994);
select * from t1;
insert into t1 values(-0.9), (-0.99), (-0.999), (-0.9999), (-0.99999), (-0.999994);
select * from t1;
insert into t1 values(0.999995);
insert into t1 values(-0.999995);

drop table t1;
create table t1(a numeric(17,17));
insert into t1 values(0), (0.99999999999999999), (0.999999999999999994), (-0.99999999999999999), (-0.999999999999999994);
select * from t1;
insert into t1 values(0.999999999999999995);
insert into t1 values(-0.999999999999999995);

drop table if exists t1;
create table t1 (a numeric(12,2), b numeric(12, 2));
insert into t1 values (301934.27, 301934.27);
select a + 4589.6 from t1;
select a + 4589.60 from t1;
select 4589.6 + a from t1;
select 4589.60 + a from t1;
drop table t1;

drop table if exists t1;
create table t1 (a numeric(12,2), b numeric(12, 2));
insert into t1 values (301934.27, 301934.27);
select a - 4589.6 from t1;
select a - 4589.60 from t1;
select 4589.6 - a from t1;
select 4589.60 - a from t1;
drop table t1;

drop table if exists t1;
create table t1 (a numeric(12,2));
insert into t1 values (301934.27);
select a * 4589.6 from t1;
select a * 4589.60 from t1;
select a * 54545.5 from t1;
select a * 54545.50 from t1;
select a * 54545.8 from t1;
select a * 54545.80 from t1;
drop table t1;

drop table if exists t1;
create table t1 (a numeric(12,2), b numeric(12, 2));
insert into t1 values (301934.27, 301934.27);
select a / 4589.6 from t1;
select a / 4589.60 from t1;
select 4589.6 / a from t1;
select 4589.60 / a from t1;
drop table t1;

-- numeric(M,D),Both M and D exist and M>=D
DROP TABLE IF EXISTS numeric01;
CREATE TABLE numeric01(a numeric(10,6));
INSERT INTO numeric01 VALUES(123.37284);
INSERT INTO numeric01 VALUES(3782.3);
INSERT INTO numeric01 VALUES(328.0);
INSERT INTO numeric01 VALUES(-373.909890);
INSERT INTO numeric01 VALUES(-1.1);
INSERT INTO numeric01 VALUES(0);
INSERT INTO numeric01 VALUES(3246.3674578902132322913);

-- The total number of inserted digits is greater than 10,
-- and the integer part is less than or equal to 4 digits, keep 6 numeric places
INSERT INTO numeric01 VALUES(0.37281738921302);
INSERT INTO numeric01 VALUES(12.4738244432449324);
INSERT INTO numeric01 VALUES(-3278.38928432434932);

-- Truncated when inserted
INSERT INTO numeric01 VALUES(-1.434324654846543221327891321);
INSERT INTO numeric01 VALUES(372.37287392839232943043);

-- Verify data correctness
SELECT * FROM numeric01;

-- update
UPDATE numeric01 set a = 0.999999999999 WHERE a = 12.473824;
UPDATE numeric01 set a = -0.00000000000 WHERE a = -1.1;

-- delete
DELETE FROM numeric01 WHERE a = -3278.389284;
DELETE FROM numeric01 WHERE a = 0.372817;

-- Mathematical operation(+ - * /)
SELECT a + 7382121 FROM numeric01;
SELECT a - 0.27832 FROM numeric01;
SELECT a * 0 FROM numeric01;
SELECT a / 6 FROM numeric01;
SELECT a * a FROM numeric01;

-- insert null
INSERT INTO numeric01 VALUES(NULL);

-- Abnormal insertion: the total number of digits exceeds the limit
INSERT INTO numeric01 VALUES(3271838219.12);
INSERT INTO numeric01 VALUEs(-278732.48392480932);
DROP TABLE numeric01;

-- Abnormal:Exception creation table:M < D
DROP TABLE IF EXISTS numeric02;
CREATE TABLE numeric02(a numeric(10,11));
DROP TABLE numeric02;

-- Abnormal:numerics appear when creating tables
DROP TABLE IF EXISTS numeric03;
CREATE TABLE numeric03(a numeric(1.1,11));
DROP TABLE numeric03;

-- Abnormal:A negative number appears when creating a table
DROP TABLE IF EXISTS numeric04;
CREATE TABLE numeric04(a numeric(11,-1));
DROP TABLE numeric04;

-- Abnormal:Exception creation table:D < 0 or M < 0
DROP TABLE IF EXISTS numeric05;
CREATE TABLE numeric05(a numeric(20,-1));
CREATE TABLE numeric05(b numeric(-2,23));
DROP TABLE numeric05;

-- Abnormal:Exception creation table:D or M is out of valid range
DROP TABLE IF EXISTS numeric06;
CREATE TABLE numeric06(a numeric(39,10));
CREATE TABLE numeric06(b numeric(40,39));
DROP TABLE numeric06;

-- D is not specified:Default D is 0
DROP TABLE IF EXISTS numeric07;
CREATE TABLE numeric07(a int PRIMARY KEY, b numeric(38));
INSERT INTO numeric07 VALUES(1, 3728193.3902);
INSERT INTO numeric07 VALUES(2, 0.327813092);
INSERT INTO numeric07 VALUES(3, -3728.4324);
INSERT INTO numeric07 VALUES(4, 12345678909876543212345678909876543243);

-- D is not specified. The default value is 0
SELECT * FROM numeric07;

-- Mathematical operation:*，+，-，/，DIV
SELECT a, a * b FROM numeric07;
SELECT a, a + b FROM numeric07;
SELECT a, a - b - b FROM numeric07;
SELECT a, b DIV a FROM numeric07;

-- Nesting with mathematical functions
SELECT a, ABS(b) FROM numeric07;
SELECT a, CEIL(b) FROM numeric07;
SELECT a, POWER(b,2) FROM numeric07;

SELECT a, b / a FROM numeric07;
SELECT b + pi() FROM numeric07;
SELECT LOG(10, b + 100000) FROM numeric07;
SELECT LN(b + 20000) FROM numeric07;
SELECT EXP(b) FROM numeric07;

DROP TABLE numeric07;

-- Critical value test
DROP TABLE IF EXISTS numeric08;
CREATE TABLE numeric08(a numeric(38,0));
INSERT INTO numeric08 VALUES(21737187383787273829839184932843922131);
INSERT INTO numeric08 VALUES(99999999999999999999999999999999999999);
INSERT INTO numeric08 VALUES(999999999999999.99999999999999999999999);
INSERT INTO numeric08 VALUES(1367281378213923.7382197382717382999911);
INSERT INTO numeric08 VALUES(-63723829382993920323820398294832849309);
INSERT INTO numeric08 VALUES(0.2777389100215365283243325321437821372);
INSERT INTO numeric08 VALUES(-99999999999999999999999999999999999999);
SELECT * FROM numeric08;

SELECT a + 1 FROM numeric08;

-- Abnormal:value overflow
SELECT a * 2 FROM numeric08;

-- @bvt:issue#8513
SELECT a / 3 FROM numeric08;
-- @bvt:issue
DROP TABLE numeric08;

-- default numeric,D and M is not specified, default M is 38, default D is 0
DROP TABLE IF EXISTS numeric09;
CREATE TABLE numeric09 (d numeric DEFAULT NULL);
INSERT INTO numeric09 VALUES (NULL);
INSERT INTO numeric09 VALUES(212839);
INSERT INTO numeric09 VALUES(44455788525777778895412365489563123654);
INSERT INTO numeric09 VALUES(0.1236547899874561233211236544569877898);
SELECT * FROM numeric09;
SELECT format(d, 2) FROM numeric09;
DROP TABLE numeric09;

-- numeric and int, int unsigned and other types of operations
-- The column constraint of numeric is null
DROP TABLE IF EXISTS numeric10;
CREATE TABLE numeric10(a numeric(20,8) NOT NULL, b smallint, c float, d DOUBLE);
INSERT INTO numeric10 VALUES(12323.3829, -32768, -483924.43, 32932.323232);
INSERT INTO numeric10 VALUES(3829.23, -38, 32943243.1, -3829.32);
INSERT INTO numeric10 VALUES(-3728832.3982, 0, 0.32893029, 329832013.32893);
INSERT INTO numeric10 VALUES(0.217832913924324,NULL,3728.39,NULL);

SELECT * FROM numeric10;

-- Abnormal insert
INSERT INTO numeric10 VALUES(NULL, 21,32,32.0);
INSERT INTO numeric10 VALUES(21732843219738283.21,NULL,NULL,0);

-- Mathematical operations and nesting with mathematical functions
SELECT a + b + c - d FROM numeric10;
SELECT a * b + c * d FROM numeric10;
SELECT a + c + a * c FROM numeric10;
SELECT a / d FROM numeric10;

SELECT ABS(a) * c FROM numeric10;
SELECT FLOOR(a) FROM numeric10;
SELECT CEIL(a) * CEIL(c) FROM numeric10;
SELECT POWER(ABS(a), 10) FROM numeric10;
SELECT pi() * a FROM numeric10;
SELECT LOG(ABS(a)) FROM numeric10;
SELECT LN(ABS(a)) FROM numeric10;
SELECT EXP(a div c) FROM numeric10;

-- Aggregate function test
SELECT SUM(a) FROM numeric10;
SELECT AVG(a + b) FROM numeric10;
SELECT COUNT(a) FROM numeric10;
SELECT MAX(a) FROM numeric10;
SELECT MIN(a) + MAX(b) FROM numeric10;

-- sort
SELECT * FROM numeric10 ORDER BY a DESC;
SELECT a * c AS SUM FROM numeric10 ORDER BY SUM ASC;
DROP TABLE numeric10;

-- CAST
SELECT CAST(a AS DOUBLE) FROM numeric10;
SELECT CAST(a AS FLOAT) FROM numeric10;
SELECT CAST(b AS numeric(10,1)) FROM numeric10;
SELECT CAST(b AS numeric(20,10)) FROM numeric10;
SELECT CAST(c AS numeric) FROM numeric10;
SELECT CAST((a * c) AS numeric) FROM numeric10;
SELECT CAST(POWER(c,2) AS numeric(28,6)) FROM numeric10;
SELECT CAST((d - c) * a AS numeric(30, 4)) FROM numeric10;

-- The column constraint of numeric is the primary key and default value is 0
DROP TABLE IF EXISTS numeric10;
CREATE TABLE numeric10 (a numeric(10,9) PRIMARY KEY DEFAULT 0, b tinyint unsigned, c bigint,id int);
INSERT INTO numeric10 VALUES(0, 155, -654346789,1);
INSERT INTO numeric10 VALUES(0.389932,78,38238293232,2);
INSERT INTO numeric10 VALUES(-2.22,0, 32783232,1);
INSERT INTO numeric10 VALUES(9, 111, NULL, 2);

SELECT * FROM numeric10;

-- Abnormal insert
INSERT INTO numeric10 VALUES(NULL,9,2819323242,2);

-- CAST
SELECT CAST(b AS numeric(10,9)) FROM numeric10;
SELECT CAST(b AS numeric(10)) FROM numeric10;
SELECT CAST(ABS(a) AS numeric) FROM numeric10;
SELECT CAST(a AS numeric) FROM numeric10;
SELECT CAST((a + b) AS numeric(20,3)) FROM numeric10;

-- Conditional query and subquery
SELECT * FROM numeric10 WHERE a  = 0;
SELECT a, b, c FROM numeric10 WHERE ABS(a) = 12323.38290000;
SELECT * FROM numeric10 WHERE a > 3829.23000000;
SELECT * FROM numeric10 WHERE CEIL(a) != 9;
SELECT * FROM numeric10 WHERE CEIL(a) < 10000;
SELECT * FROM numeric10 WHERE ABS(a) <= 1;
SELECT * FROM numeric10 WHERE a BETWEEN -1 AND 3;
SELECT * FROM numeric10 WHERE a NOT BETWEEN -1 AND 3;
SELECT * FROM numeric10 WHERE a IN(1.0,9.0);
SELECT a,b,c FROM numeric10 WHERE a NOT IN(1.00000,9.00000);

SELECT * FROM numeric10 WHERE a IN(1,9);
SELECT a,b,c FROM numeric10 WHERE a NOT IN(1,9);

SELECT a * b FROM numeric10 WHERE a NOT BETWEEN 2 AND 9;
SELECT any_value(a) FROM numeric10 WHERE ABS(a) >= 0 GROUP BY id;
SELECT * FROM numeric10 WHERE a > (SELECT COUNT(a) FROM numeric10);
DROP TABLE numeric10;

-- truncate result
DROP TABLE IF EXISTS numeric11;
CREATE TABLE numeric11 (a numeric(38,13), b numeric(25,12));
INSERT INTO numeric11 VALUES(1234567890981.111231146421, 1.232143214321);
INSERT INTO numeric11 VALUES(32838293.3387298323, -37827.382983283022);
SELECT * FROM numeric11;
SELECT CAST((a * b) AS numeric(20,10)) FROM numeric11;
SELECT a * b FROM numeric11;

-- char and varchar to numeric
DROP TABLE IF EXISTS numeric12;
CREATE TABLE numeric12(a char, b VARCHAR(38));
INSERT INTO numeric12 VALUES('a','3.14e+09');
INSERT INTO numeric12 VALUES('b', '-3.14e+09');
INSERT INTO numeric12 VALUES('c','545676678738');
INSERT INTO numeric12 VALUES('d',NULL);
INSERT INTO numeric12 VALUES('e','99999999009999999999999999999999999999');

SELECT * FROM numeric12;

-- cast
SELECT CAST(b AS numeric(38,0)) FROM numeric12;
SELECT CAST(b AS numeric) FROM numeric12 WHERE a = 'b';

-- joins
DROP TABLE IF EXISTS numeric13;
DROP TABLE IF EXISTS numeric14;
CREATE TABLE numeric13(a numeric(10,2) NOT NULL,b FLOAT,c smallint unsigned);
INSERT INTO numeric13 VALUES(1,12.9,61555);
INSERT INTO numeric13 VALUES(-38299323.8880,33.3283,0);
INSERT INTO numeric13 VALUES(0.894324,-327832.932,90);
SELECT * FROM numeric13;

CREATE TABLE numeric14(a numeric(11,9), b DOUBLE, c int unsigned);
INSERT INTO numeric14 VALUES(1,728.392032,2147483647);
INSERT INTO numeric14 VALUES(32.12,38293,23829321);
INSERT INTO numeric14 VALUES(0.3289302,382943243.438,0);
SELECT * FROM numeric14;

SELECT numeric13.a,numeric14.a FROM numeric13,numeric14 WHERE numeric13.a = numeric14.a;
SELECT numeric13.a,numeric14.a FROM numeric13 join numeric11 ON numeric13.a = numeric14.a;
SELECT numeric13.a,numeric14.a FROM numeric13 left join numeric11 ON numeric13.a = numeric14.a;
SELECT numeric13.a,numeric14.b,numeric13.c FROM numeric14 right join numeric13 ON numeric14.a < numeric11.a;
DROP TABLE numeric13;
DROP TABLE numeric14;

-- unique index
DROP TABLE IF EXISTS numeric15;
CREATE TABLE numeric15(a numeric(10,5),b FLOAT,c double);
CREATE UNIQUE INDEX a_index on numeric15(a);
INSERT INTO numeric15 VALUES(271.212121,387213.0,3289);
INSERT INTO numeric15 VALUES(-28.3232,387213.0,32132313);
INSERT INTO numeric15 VALUES(NULL,327.328932,-38922.2123);
SELECT * FROM numeric15;

-- unique index cannot have duplicate value insertion
INSERT INTO numeric15 VALUES(271.212121,387213.0,3289);
DROP TABLE numeric15;

-- secondary index
DROP TABLE IF EXISTS numeric16;
CREATE TABLE numeric16(a numeric, b numeric(38,10), c varchar(20),UNIQUE INDEX(a),INDEX(b));
INSERT INTO numeric16 VALUES(1234789456456456456567898552556, 3728321323.4321214,'小明');
SELECT * FROM numeric16;

INSERT INTO numeric16 VALUES(-64564567898552556, 3728321323.4321214,'小花');
INSERT INTO numeric16 VALUES(-64564568552556, 8321323.4321214,'小强');
DROP TABLE numeric16;

-- load data CSV
DROP TABLE IF EXISTS numeric17;
CREATE TABLE numeric17(a numeric,b numeric(38,0),c numeric(20,4),d numeric(10));
INSERT INTO numeric17 VALUES(3231.44112,0,-38232432541431.7890,3728739824.0909898765);
INSERT INTO numeric17 VALUES(-3892.020,NULL,3872932.3289323,3829);
INSERT INTO numeric17 VALUES(123.456,NULL,7281392.902,328392323);

DROP TABLE IF EXISTS numeric18;
CREATE TABLE numeric18(a numeric, b numeric(38,0));

INSERT INTO numeric18 (a,b) SELECT a,b FROM numeric17;
INSERT INTO numeric18 (a,b) SELECT c,d FROM numeric17;
SELECT * FROM numeric17;
DROP TABLE numeric17;

-- Precision 256
DROP TABLE IF EXISTS numeric18;
CREATE TABLE numeric18 (col1 numeric(38,37),col2 numeric(38,37),col3 float, col4 double);
INSERT INTO numeric18 VALUES(0.1221212134567890987654321333546543213,0.9999999999999999999999999999999999999,1278945.21588,78153178.49845612);
INSERT INTO numeric18 VALUES(0.9999999999999999999999999999999999999,0.1484484651187895121879845615156784548,1545.1548,879.89484);
INSERT INTO numeric18 VALUES(0.8932839724832437289437927438274832748,0,453201,78465121);
INSERT INTO numeric18 VALUES(0.372837842743762,9.9999999999999999999999999999999999999,0,-454.49845);
INSERT INTO numeric18 VALUES(-0.3728329487324893628746328746873236438,-9.3820342423,-329837842932.4932,0);
SELECT * FROM numeric18;

-- cast
SELECT CAST(col3 AS numeric(38)) FROM numeric18;
SELECT CAST(col4 AS numeric(20,3)) FROM numeric18;

-- operation
SELECT 0.32846287164921643232142372817438921749321 * col1 FROM numeric18;
SELECT -0.487794599999999999999999999999999945451154 * col2 FROM numeric18;

-- @bvt:issue#8516
SELECT col1 * col2 FROM numeric18;
SELECT col3 * col2 FROM numeric18;
SELECT col2 * col4 FROM numeric18;
-- @bvt:issue

-- @bvt:issue#8513
SELECT col1 / col2 FROM numeric18;
SELECT 12345678965412365478965444565896532145 / col1 FROM numeric18;
SELECT col2/522222222225456987.23212654569987523654 FROM numeric18;
-- @bvt:issue

DROP TABLE IF EXISTS numeric19;
CREATE TABLE numeric19 (col1 numeric(38,0),col2 numeric(19,0));
INSERT INTO numeric19 VALUES(12345645678978945612312345678885229999, 1235467899687894561);
INSERT INTO numeric19 VALUES(99999999999999999999999999999999999999, -3123456987456987456);
INSERT INTO numeric19 VALUES(-99999999997899999999999999999999999999, 5681569874569999999);
SELECT col1 / col2 FROM numeric19;
SELECT col1 * col2 FROM numeric19;
SELECT col1 * 0.2871438217498217489217843728134214212143 FROM numeric19;
SELECT col2 * -2733892455124775.7851878942123454 FROM numeric19;

-- abnormal test
SELECT col1 / 0;
SELECT col2 / 0;

-- cast
SELECT CAST('1.2' AS numeric(3,2));
SELECT CAST('1.327832' AS numeric(20));
SELECT CAST('-29012123143.432478274329432' AS numeric);
SELECT CAST('-99999999999999999999999999999999999999' AS numeric(38));
SELECT 1e18 * CAST('1.2' as numeric(3,2));
SELECT CAST(CAST('1.234221421' AS numeric(3,2)) AS signed);
SELECT CAST(@v1 as numeric(22, 2));
SELECT CAST(-1e18 as numeric(22,2));
SELECT CAST(NULL AS numeric(6));
SELECT 10.0 + CAST('aaa' AS numeric);
SELECT CAST('101010101001' AS numeric);
SELECT CAST('10101010010101010101010101' AS numeric(26));
SELECT CAST('0101010' AS numeric);
SELECT CAST('' AS numeric);

DROP TABLE IF EXISTS numeric20;
CREATE TABLE numeric20(v varchar(40), tt tinytext, t text, mt mediumtext, lt longtext);
INSERT INTO numeric20 VALUES('1.01415', '2.02115', '-3.03', '444.04', '4515.05');
INSERT INTO numeric20 VALUES('123654.78984115855111555555888859999999','1e18','229e15','2178291','873273271432714032713243214333');
SELECT * FROM numeric20;
SELECT CAST(v AS numeric) FROM numeric20;
SELECT CAST(tt AS numeric(24,2)) FROM numeric20;
SELECT CAST(t AS numeric(20)) FROM numeric20;
SELECT CAST(mt AS numeric) FROM numeric20;
SELECT CAST(lt AS numeric) FROM numeric20;
SELECT 12487561513.48465123 / CAST(t AS numeric(20)) FROM numeric20;
SELECT 789414561531231.485416 / CAST(t AS numeric(20)) FROM numeric20;

DROP TABLE IF EXISTS numeric21;
CREATE TABLE numeric21(col1 numeric(38,3));
INSERT INTO numeric21 VALUES(99999999999999999999999999999999999.83293323);
INSERT INTO numeric21 VALUES(99999999999999999999999999999999999.83293323);
SELECT SUM(col1) from numeric21;
DROP TABLE numeric21;

DROP TABLE IF EXISTS numeric22;
CREATE TABLE numeric22(col1 numeric(38,5),col2 numeric(38,25));
INSERT INTO numeric22 VALUES('0xffffffffffffffffffffffff ',0.00000000000000000001);
SELECT col1/(col1*col2) from numeric22;
SELECT col1%col2 from numeric22;
DROP TABLE numeric22;

DROP TABLE IF EXISTS numeric23;
CREATE TABLE numeric23(col1 numeric(18,5),col2 numeric(18,15));
INSERT INTO numeric23 VALUES('0xffffffffff',0.0000000000001);
SELECT col1%col2 from numeric23;
SELECT cast(col2 as double) from numeric23;
DROP TABLE numeric23;
