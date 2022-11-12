SELECT DATEDIFF('2017-08-17','2017-08-17');
SELECT DATEDIFF('2017-08-17','2017-08-08');
SELECT DATEDIFF('2017-08-08','2017-08-17');

SELECT DATEDIFF(NULL,'2017-08-17');
SELECT DATEDIFF('2017-08-17',NULL);
SELECT DATEDIFF(NULL, NULL);

drop table if exists t1;
create table t1(a INT,  b date);
insert into t1 values(1, "2012-10-11");
insert into t1 values(2, "2004-04-24");
insert into t1 values(3, "2008-12-04");
insert into t1 values(4, "2012-03-23");
insert into t1 values(5, "2000-03-23");
insert into t1 values(6, "2030-03-23");
insert into t1 values(7, "2040-03-23");
SELECT a, DATEDIFF('2022-10-9', b) from t1;
drop table t1;

drop table if exists t1;
create table t1(a INT,  b date);
insert into t1 values(1, "2012-10-11");
insert into t1 values(2, "2004-04-24");
insert into t1 values(3, "2008-12-04");
insert into t1 values(4, "2012-03-23");
insert into t1 values(5, "2000-03-23");
insert into t1 values(6, "2030-03-23");
insert into t1 values(7, "2040-03-23");
SELECT a, DATEDIFF(b, '2022-10-9') from t1;
drop table t1;

drop table if exists t1;
create table t1(a date,  b date);
insert into t1 values('2022-10-9', "2012-10-11");
insert into t1 values('2022-10-9', "2004-04-24");
insert into t1 values('2022-10-9', "2008-12-04");
insert into t1 values('2022-10-9', "2012-03-23");
insert into t1 values('2022-10-9', "2000-03-23");
insert into t1 values('2022-10-9', "2030-03-23");
insert into t1 values('2022-10-9', "2040-03-23");
SELECT a, b, DATEDIFF(a, b) from t1;
drop table t1;


