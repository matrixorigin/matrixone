-- @suit
-- @case
-- @test function LEFT
-- @label:bvt

-- constant test
select left('abcde', 3) from dual;
select left('abcde', 0) from dual;
select left('abcde', 10) from dual;
select left('abcde', -1) from dual;
select left('abcde', null) from dual;
select left(null, 3) from dual;
select left(null, null) from dual;
select left('foobarbar', 5) from dual;
select left('qwerty', 1.2) from dual;
select left('qwerty', 1.5) from dual;
select left('qwerty', 1.8) from dual;
select left("是都方式快递费",3) from dual;
select left("あいうえお",3) from dual;
select left("あいうえお ",3) from dual;
select left("あいうえお  ",3) from dual;
select left("あいうえお   ",3) from dual;
select left("龔龖龗龞龡",3) from dual;
select left("龔龖龗龞龡 ",3) from dual;
select left("龔龖龗龞龡  ",3) from dual;
select left("龔龖龗龞龡   ",3) from dual;

-- @suite
-- @setup
drop table if exists t1;

CREATE TABLE t1 (str VARCHAR(100) NOT NULL, len INT);
insert into t1 values('abcdefghijklmn',3);
insert into t1 values('  ABCDEFGH123456', 3);
insert into t1 values('ABCDEF  GHIJKLMN', 20);
insert into t1 values('ABCDEFGHijklmn   ', -1);
insert into t1 values('ABCDEFGH123456', -35627164);
insert into t1 values('', 3);

-- @case
-- String test
select left(str, len) from t1;
SELECT * from t1 where left(str, cos(0) + 3) = 'ABC' and len > 3;
select left(str, 3) from t1;
select left('sdfsdfsdfsdf', len) from t1;
select left(NULL, TAN(45)) FROM t1;
select left('str', COS(0) + TAN(45)) from t1 where len between 6 AND 21;
SELECT left(str, -2) from t1 where len%3 = 1;
drop table t1;

-- @suite
-- @setup
DROP TABLE IF EXISTS t;

CREATE table t(age INT, name CHAR(20), address VARCHAR(30));
INSERT INTO t VALUES(20,'ejifwvewv','shanghaishi1032long');
INSERT INTO t VALUES(30,'zhangzianjd','minhangqulongminglu');
INSERT INTO t VALUES(3627832,'hcdusanjfds','xuhuiqudadao');
INSERT INTO t VALUES(3782,'ehuwqhd3283&*^','ehiw3232$');
INSERT INTO t VALUES(42,'','nkej32');
INSERT INTO t VALUES(-2281928939,'wlll','');

-- 异常：数值超过所表示范围
INSERT INTO t VALUES(-2281928939,'wlll','');


-- @case
-- String test
SELECT left(age, NULL) from t;
SELECT left(age, 0) from t;
SELECT left(age, 2),left(name,5),left(address,10) from t;
SELECT left(age,'1'),left(name,'2'), left(address, '3') from t;
SELECT left(age, COS(0)),left(name, sin(90) + 1),left(address, TAN(45) + 3) from t;
SELECT left(age, length(name) / 2),left(name,1) from t;
SELECT left(name, 3) from t where age >= 20;
SELECT left(age, -4) from t where LENGTH(address) >= 20;
SELECT left(age, 2) from t where name LIKE 'hcdusanjfds';
SELECT left(age, 2), address from t where name LIKE '%vewv';
SELECT address from t where left(name, 3) = 'eji';

-- @suite
-- @setup
DROP TABLE IF EXISTS t;

CREATE TABLE t(id INT,dd1 DATE, dd2 DATETIME,  dd3 TIMESTAMP, PRIMARY KEY (id));
INSERT INTO t VALUES (1, '2020-01-01', '2020-01-01 12:12:12', '2020-02-02 06:06:06.163');
INSERT INTO t VALUES (2, '2021-11-11', '2021-01-11 23:23:23', '2021-12-12 16:16:16.843');
INSERT INTO t VALUES (3, '2002-11-11', '2002-01-11 23:23:23', '2002-12-12 16:16:16.843');


-- @case
-- data type test
SELECT left(dd1, length(dd2)) FROM t;
SELECT left(dd1, TAN(45) + 6) FROM t;
SELECT left(dd1,COS(10) + 1) FROM t;
SELECT left(dd1,NULL) FROM t;
SELECT left(NULL, -2) FROM t;
SELECT left(dd3, 4) FROM t WHERE dd1 LIKE '0001-01-01';
SELECT * from t WHERE left(dd1, 2) = 20;
SELECT * from t WHERE MONTH(dd1) = 11 AND left(dd1, 4) = 2021;


-- @suite
-- @setup
DROP TABLE IF EXISTS t;

CREATE TABLE t(id INT,d1 BIGINT,d2 FLOAT,d3 DOUBLE,PRIMARY KEY (id));
INSERT INTO t VALUES(1,12345678977,4679.45,-46576898.09877);
INSERT INTO t VALUES(2,4251382834,-456.785,32913023.3213);
INSERT INTO t VALUES(3,-46382749832,0,456215.454);
INSERT INTO t VALUES(4,0,8.121,0);
INSERT INTO t VALUES(5,-329323809293,0,0);
INSERT INTO t VALUES(6,47832745,4672493280324.37644342323242,-1.8976931348623157E+308);
INSERT INTO t VALUES(7,47832745,4.402823466351E+38,666.666);


-- 异常：数值超过所表示范围
INSERT INTO t VALUES(6,47832745,4672493280324.37644342323242,-1.8976931348623157E+308);
INSERT INTO t VALUES(7,47832745,4.402823466351E+38,666.666);


-- @case
-- @floating point types test
SELECT left(d1,abs(-5)) from t;
SELECT left(d2, LENGTH(d1) - 5) from t;
SELECT left(d1, 3),left(d2, 6) from t ORDER by d1;
SELECT * from t where ABS(d1) > 200000;


-- @suite
-- @setup
DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t1;

CREATE TABLE t(d INT,d1 VARCHAR(20), d2 BIGINT,PRIMARY KEY (d));
CREATE TABLE t1( d INT,d1 CHAR(20),d2 DATE,PRIMARY KEY (d));
INSERT INTO t VALUES (1,'lijklnfdsalj',19290988), (2,'xlziblkfdi',1949100132);
INSERT INTO t VALUES (3,'ixioklakmaria',69456486), (4,'brzilaiusd',6448781575);
INSERT INTO t1 VALUES (1,'usaisagoodnat','1970-01-02'),(2,'chanialfakbjap','1971-11-12');
INSERT INTO t1 VALUES (3,'indiaisashit','1972-09-09'),(4,'xingoporelka','1973-12-07');

-- @case
-- @join and function test
SELECT t.d, LEFT(t.d1, abs(-4)) FROM t;
SELECT t.d, LEFT(t.d2, FIND_IN_SET('d','a,b,c,d')), LEFT(t1.d1, ABS(-3)+1) FROM t,t1 WHERE t.d = t1.d;
SELECT t.d, LEFT(t1.d2, NULL) FROM t JOIN t1 ON t.d = t1.d;
SELECT t.d,left(t1.d2, abs(-1)+1),left(t.d2, cos(0)+3) from t join t1 on t.d=t1.d;
SELECT t.d,left(t.d2, find_in_set('e','a,b,c,d,e')),left(t1.d1, 20%3)from t right join t1 on t.d=t1.d;
SELECT t.d,left(t.d1, find_in_set('d','a,b,c,d,e')),left(t1.d2, 20%3)from t right join t1 on t.d=t1.d;

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t1;

