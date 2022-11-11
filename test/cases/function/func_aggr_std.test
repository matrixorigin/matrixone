#NULL

SELECT STD(null);
SELECT STDDEV_POP(null);



#DATATYPE
create table t1(a tinyint, b SMALLINT, c BIGINT, d INT, e BIGINT, f FLOAT, g DOUBLE, h decimal(38,19), i DATE, k datetime, l TIMESTAMP, m char(255), n varchar(255));
insert into t1 values(1, 1, 2, 43, 5, 35.5, 31.133, 14.314, "2012-03-10", "2012-03-12 10:03:12", "2022-03-12 13:03:12", "ab23c", "d5cf");
insert into t1 values(71, 1, 2, 34, 5, 5.5, 341.13, 15.314, "2012-03-22", "2013-03-12 10:03:12", "2032-03-12 13:04:12", "abr23c", "3dcf");
insert into t1 values(1, 1, 21, 4, 54, 53.5, 431.13, 14.394, "2011-03-12", "2015-03-12 10:03:12", "2002-03-12 13:03:12", "afbc", "dct5f");
insert into t1 values(1, 71, 2, 34, 5, 5.5, 31.313, 124.314, "2012-01-12", "2019-03-12 10:03:12", "2013-03-12 13:03:12", "3abd1c", "dcvf");
select std(a) from t1;
select std(b) from t1;
select std(c) from t1;
select std(d) from t1;
select std(e) from t1;
select std(f) from t1;
select std(g) from t1;

select std(h) from t1;
select std(i) from t1;
select std(k) from t1;
select std(l) from t1;
select std(m) from t1;
select std(n) from t1;

select STDDEV_POP(a) from t1;
select STDDEV_POP(b) from t1;
select STDDEV_POP(c) from t1;
select STDDEV_POP(d) from t1;
select STDDEV_POP(e) from t1;
select STDDEV_POP(f) from t1;
select STDDEV_POP(g) from t1;

select STDDEV_POP(h) from t1;
select STDDEV_POP(i) from t1;
select STDDEV_POP(k) from t1;
select STDDEV_POP(l) from t1;
select STDDEV_POP(m) from t1;
select STDDEV_POP(n) from t1;
drop table t1;


#0.5暂不支持time类型
#create table t1(a time)
#insert into t1 values("10:03:12");
#insert into t1 values("10:03:12");
#insert into t1 values("10:03:12");
#insert into t1 values("10:03:12");
#select STDDEV_POP(a) from t1;
#drop table t1;

#EXTREME VALUE, DISTINCT
select STDDEV_POP(99999999999999999.99999);
select STDDEV_POP(999999999999999933193939.99999);
select STDDEV_POP(9999999999999999999999999999999999.9999999999999);
select STDDEV_POP(-99999999999999999.99999);
select STDDEV_POP(-999999999999999933193939.99999);
select STDDEV_POP(-9999999999999999999999999999999999.9999999999999);
create table t1(a bigint);
select STDDEV_POP(a) from t1;
insert into t1 values(null),(null),(null),(null);
select STDDEV_POP(a) from t1;
insert into t1 values(12417249128419),(124124125124151),(5124125151415),(124125152651515);
select STDDEV_POP(a) from t1;
drop table t1;
create table t1 ( a int not null default 1, big bigint );
insert into t1 (big) values (-1),(1234567890167),(92233720368547),(18446744073709515);
select * from t1;
select distinct STDDEV_POP(big),max(big),STDDEV_POP(big)-1 from t1;
select STDDEV_POP(big),max(big),STDDEV_POP(big)-1 from t1 group by a;
insert into t1 (big) values (184467440737615);
select * from t1;
select STDDEV_POP(big),max(big),STDDEV_POP(big)-1 from t1;
select STDDEV_POP(big),max(big),STDDEV_POP(big)-1 from t1 group by a;
drop table t1;

#HAVING, DISTINCT#HAVING,DISTINCT
CREATE TABLE t1 (Fld1 int(11) default NULL,Fld2 int(11) default NULL);
INSERT INTO t1 VALUES (1,10),(1,20),(2,NULL),(2,NULL),(3,50);
select Fld1, STDDEV_POP(Fld2) as q from t1 group by Fld1 having q is not null;
select Fld1, STDDEV_POP(Fld2) from t1 group by Fld1 having STDDEV_POP(Fld2) is not null;
select Fld1, STDDEV_POP(Fld2) from t1 group by Fld1 having avg(Fld2) is not null;
select Fld1, STDDEV_POP(Fld2) from t1 group by Fld1 having variance(Fld2) is not null;
drop table t1;

#比较操作
SELECT STDDEV_POP(1)<STDDEV_POP(2);

#DISTINCT, 算式操作
CREATE TABLE t1(i INT);
INSERT INTO t1 VALUES (NULL),(1);
SELECT STDDEV_POP(i)+0 as splus0, i+0 as plain FROM t1 GROUP BY i ;
DROP TABLE t1;
