select median(null);

drop table if exists t1;
create table t1 (a int,b int);
insert into t1 values (1,null);
select median(b) from t1;
insert into t1 values (1,1);
select median(b) from t1;
insert into t1 values (1,2);
select median(b) from t1;
select median(b) from t1 group by a order by a;
insert into t1 values (2,1),(2,2),(2,3),(2,4);
select median(b) from t1 group by a order by a;
insert into t1 values (2,null);
select median(b) from t1 group by a order by a;

drop table if exists t1;
create table t1 (a int,b float,c double);
insert into t1 values (1,null,null);
select median(b),median(c) from t1;
insert into t1 values (1,1.1,1.1);
select median(b),median(c) from t1;
insert into t1 values (1,2.2,2.2);
select median(b),median(c) from t1;
select median(b),median(c) from t1 group by a order by a;
insert into t1 values (2,1.1,1.1),(2,2.2,2.2),(2,3.3,3.3),(2,4.4,4.4);
select median(b),median(c) from t1 group by a order by a;
insert into t1 values (2,null,null);
select median(b),median(c) from t1 group by a order by a;

drop table if exists t1;
create table t1 (a int,b decimal(10,2),c decimal(34,10));
insert into t1 values (1,null,null);
select median(b),median(c) from t1;
insert into t1 values (1,'1.1','1.1');
select median(b),median(c) from t1;
insert into t1 values (1,'2.2','2.2');
select median(b),median(c) from t1;
select median(b),median(c) from t1 group by a order by a;
insert into t1 values (2,'1.1','1.1'),('2','2.2','2.2'),('2','3.3','3.3'),('2','4.4','4.4');
select median(b),median(c) from t1 group by a order by a;
insert into t1 values (2,null,null);
select median(b),median(c) from t1 group by a order by a;

select median(distinct a) from t1;
drop table if exists t1;
create table t1 (a int,b varchar(10));
select median(b) from t1;

-- @suit
-- @case
-- @test function LEFT
-- @label:bvt

-- median()函数是求中位数的函数。

SELECT median(1);
SELECT median(-6372.2);
SELECT median(NULL);
SELECT median(ABS(-99));
SELECT median(COS(0) + 2);


-- 异常输入
SELECT median(1,2,3);
SELECT median(fekwelwfew);
SELECT median(3hewh32ioj);
SELECT median("ejwjlvd23232r43f");
SELECT median("4");
SELECT median('');



-- @suite
-- @setup
DROP TABLE IF EXISTS median_01;
CREATE TABLE median_01(id int, d1 tinyint, d2 smallint unsigned, d3 bigint);
INSERT INTO median_01 VALUES(1, -128, 65534, 5554584122);
INSERT INTO median_01 VALUES(2, 0, 68, -7855122);
INSERT INTO median_01 VALUES(3, 45, 0, 67432648932);
INSERT INTO median_01 VALUES(4, 45, 5789, 0);
INSERT INTO median_01 VALUES(5, NULL, 3782, NULL);

-- 异常插入
-- tinyint超出插入范围
INSERT INTO median_01 VALUES(6, -129, 65534, 5554584122);

-- smallint unsigned超出插入范围
INSERT INTO median_01 VALUES(7, -123, 89555, 5554584122);

-- bigint超出插入范围
INSERT INTO median_01 VALUES(8, -62, 33, 9223372036854775808);

-- int超出插入范围
INSERT INTO median_01 VALUES(2147483648, -62, 33, 9223372036854775808);


-- median
SELECT median(d2) from median_01;
SELECT median(d1),median(d2),median(d3),median(id) from median_01;
SELECT median(d1) + median(d2) as he, median(d2) * median(d3) as pr from median_01 where id = 2;
SELECT median(id) / 4 from median_01;
SELECT median(d3) FROM median_01 WHERE id BETWEEN 1 AND 4;
SELECT median(d1),median(d2),median(d3) from median_01 GROUP by d1;
SELECT d1, d2 FROM median_01 group by median(d1);
SELECT median(d1) FROM median_01 WHERE id = ABS(-1) + TAN(45);

-- 数学函数
SELECT ABS(median(d2)), FLOOR(median(id) * 3) from median_01;
SELECT SIN(median(d1)), COS(median(d2)), TAN(median(d2)) FROM median_01;
SELECT TAN(median(d2)), cot(median(d2) * 2), ACOS(median(d1)) FROM median_01;
SELECT ATAN(median(d2)), SINH(median(id)) FROM median_01;
SELECT ROUND(median(id) / 2) from median_01;
SELECT CEIL(median(d1)) FROM median_01 WHERE id = 1;
SELECT power(median(id),3) FROM median_01;
SELECT LOG(median(id)) AS a,LN(median(id)) AS b FROM median_01;
SELECT EXP(median(id)) FROM median_01;



-- @suite
-- @setup
DROP TABLE IF EXISTS median_02;
CREATE TABLE median_02(id int PRIMARY KEY, d1 FLOAT, d2 DOUBLE NOT NULL);

INSERT INTO median_02 VALUES(1, 645545.11, 65534.5554584122);
INSERT INTO median_02 VALUES(2, NULL, 638239.1);
INSERT INTO median_02 VALUES(3, -32783, -56323298.8327382);
INSERT INTO median_02 VALUES(4, 0, 389283920.1);
INSERT INTO median_02 VALUES(5, 382, 0);


-- 异常插入
-- DOUBLE超出插入范围
INSERT INTO median_02 VALUES(6, 0, -1.8976931348623157E+308);

-- FLOAT超出插入范围
INSERT INTO median_02 VALUES(7, 4.402823466351E+38, 5554584122);

-- d2为空
INSERT INTO median_02 VALUES(8, -55845.0, NULL);

SELECT median(d1), median(d2) from median_02;
SELECT median(d2) from median_02 group by d2;
SELECT median(d2) from median_02 WHERE id BETWEEN 2 AND 4;
-- 嵌套查询
SELECT median(d1) from median_02 WHERE id = (SELECT id from median_02 where d2 = 65534.5554584122);

-- 数学函数
SELECT ACOS(median(d2)) from median_02;
SELECT CEIL(median(d2)), FLOOR(median(d2)) from median_02;
SELECT power(median(d2),2) from median_02;


-- @suite
-- @setup
DROP TABLE IF EXISTS median_03;
DROP TABLE IF EXISTS median_04;

CREATE TABLE median_03(id int, ch smallint NOT NULL, ma bigint unsigned NOT NULL, en FLOAT, ph double,
                       PRIMARY KEY(id));
INSERT INTO median_03 VALUES(1, 88, 99999, -99.98, 88.99);
INSERT INTO median_03 VALUES(2, 65, 744515, 0, 78.789);
INSERT INTO median_03 VALUES(3, 76, 21, 893293.1, NULL);
INSERT INTO median_03 VALUES(4, -367, 3298, NULL, 0);
INSERT INTO median_03 VALUES(5, 674, 432, 8767687.0, 0.1);

CREATE TABLE median_04(id int, name VARCHAR(10), ch smallint, ma bigint, en FLOAT not NULL,
                       PRIMARY KEY(id));
INSERT INTO median_04 VALUES(1, 'Alice', 327, 45451, 3232.1);
INSERT INTO median_04 VALUES(2, 'Bob', 3728, -8889, 899);
INSERT INTO median_04 VALUES(3, 'Grace', 0, NULL, 0.1);
INSERT INTO median_04 VALUES(4, 'Vicky', 88, 99, 88888.0);
INSERT INTO median_04 VALUES(5, 'John', 10, 23211, -78);


-- @case
-- @join and function test
SELECT median(median_04.ch) from median_03, median_04 where median_03.id = median_04.id;
SELECT median(median_03.ma) AS a, median(median_04.ma) AS b from median_03 join median_04 ON median_03.ph = median_04.en;
SELECT median(median_03.ch),median(median_03.en) from median_03 WHERE id % 2 =1;
SELECT median(median_03.ch) from median_04 WHERE left(name,2) = 'Al';
