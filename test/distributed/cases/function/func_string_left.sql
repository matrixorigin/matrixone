-- @suit
-- @case
-- @test function LEFT
-- @label:bvt

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
CREATE TABLE t1 (str VARCHAR(100), len INT);
insert into t1 values('abcdefghijklmn',3);
insert into t1 values('ABCDEFGH123456', 3);
insert into t1 values('ABCDEFGHIJKLMN', 20);
insert into t1 values('ABCDEFGHijklmn', -1);
insert into t1 values('ABCDEFGH123456', 7);
insert into t1 values('', 3);

--  @case
select left(str, len) from t1;
select * from t1 where left(str, len) = 'ABC';
select left(str, 3) from t1;
select left('sdfsdfsdfsdf', len) from t1;
drop table t1;

-- @suite
-- @setup
DROP TABLE IF EXISTS t;
CREATE table t(age INT, name CHAR(20), address VARCHAR(30));
INSERT INTO t VALUES(20,'ejifwvewv','shanghaishi1032long'),(30,'zhangzianjd','minhangqulongminglu'),
                    (3627832,'hcdusanjfds','xuhuiqudadao');

-- @case
SELECT left(age, 2),left(name,5),left(address,10) from t;
SELECT left(age,'1'),left(name,'2'), left(address, '3') from t;
SELECT left(age, COS(0)),left(name, sin(90) + 1),left(address, TAN(45) + 3) from t;
SELECT left(age, length(name) / 2),left(name,1) from t;
SELECT left(name, 3) from t where age >= 20;

-- @suite
-- @setup
DROP TABLE IF EXISTS t;
CREATE TABLE t(id INT,dd1 DATE, dd2 DATETIME,  dd3 TIMESTAMP, PRIMARY KEY (id));
INSERT INTO t VALUES (1, '2020-01-01', '2020-01-01 12:12:12', '2020-02-02 06:06:06.163');
INSERT INTO t VALUES (2, '2021-11-11', '2021-01-11 23:23:23', '2021-12-12 16:16:16.843');

-- @case

SELECT left(dd1, length(dd2)) FROM t;
SELECT left(dd1, TAN(45) + 6) FROM t;
SELECT left(dd1,cos(10) + 1) FROM t;
SELECT left(dd1,null) FROM t;

-- @suite
-- @setup
DROP TABLE IF EXISTS t;
CREATE TABLE t(id INT,d1 BIGINT,d2 FLOAT,d3 DOUBLE,PRIMARY KEY (id)
);
INSERT INTO t VALUES(1,12345678977,467789.4565767,46576898.09877),(2,4251382834,456.785615,32913023.3213);

-- @case
SELECT left(d1,abs(-5)),left(d2,tan(45)) from t;
SELECT left(d2, LENGTH(d1) - 5) from t;

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

--case
SELECT t.d, LEFT(t.d1, abs(-4)) FROM t;
SELECT t.d, LEFT(t.d2, FIND_IN_SET('d','a,b,c,d')), LEFT(t1.d1, ABS(-3)+1) FROM t,t1 WHERE t.d = t1.d;
SELECT t.d, LEFT(t1.d2, NULL) FROM t JOIN t1 ON t.d = t1.d;
SELECT t.d,left(t1.d2, abs(-1)+1),left(t.d2, cos(0)+3) from t join t1 on t.d=t1.d;
SELECT t.d,left(t.d2, find_in_set('e','a,b,c,d,e')),left(t1.d1, 20%3)from t right join t1 on t.d=t1.d;
SELECT t.d,left(t.d1, find_in_set('d','a,b,c,d,e')),left(t1.d2, 20%3)from t right join t1 on t.d=t1.d;

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t1;

