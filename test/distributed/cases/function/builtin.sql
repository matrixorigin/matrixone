-- @suite
-- @setup
drop table if exists t1;
create table t1(a int,b int);
insert into t1 values(5,-2),(10,3),(100,0),(4,3),(6,-3);
-- @case
-- @desc:test for func power() select
-- @label:bvt
select power(a,b) from t1;

-- @case
-- @desc:test for func power() as where filter and order by power()
-- @label:level0
select power(a,2) as a1, power(b,2) as b1 from t1 where power(a,2) > power(b,2) order by a1 asc;

-- @suite
-- @setup
drop table if exists t1;
create table t1(a date,b datetime);
insert into t1 values("2022-06-01","2022-07-01 00:00:00");
insert into t1 values("2022-12-31","2011-01-31 12:00:00");

-- @case
-- @desc:test for func month() select
select month(a),month(b) from t1;
select * from t1 where month(a)>month(b);
select * from t1 where month(a) between 1 and 6;
-- @teardown
drop table if exists t1;

-- @suite
-- @setup
create table t1(a varchar(12),c char(30));

insert into t1 values('sdfad  ','2022-02-02 22:22:22');
insert into t1 values('  sdfad  ','2022-02-02 22:22:22');
insert into t1 values('adsf  sdfad','2022-02-02 22:22:22');
insert into t1 values('    sdfad','2022-02-02 22:22:22');
-- @case
-- @desc:test for func reverse() select
-- @separator:table
select reverse(a),reverse(c) from t1;
-- @separator:table
select a from t1 where reverse(a) like 'daf%';
-- @separator:table
select reverse(a) reversea,reverse(reverse(a)) normala from t1;


-- @suite
-- @setup
drop table if exists t1;
create table t1(a int,b float);
insert into t1 values(0,0),(-15,-20),(-22,-12.5);
insert into t1 values(0,360),(30,390),(90,450),(180,270),(180,180);
-- @case
-- @desc:test for func acos() select
select acos(a*pi()/180) as acosa,acos(b*pi()/180) acosb from t1;
select acos(a*pi()/180)*acos(b*pi()/180) as acosab,acos(acos(a*pi()/180)) as c from t1;
select b from t1 where acos(a*pi()/180)<=acos(b*pi()/180)  order by a;

-- @suite
-- @setup
drop table if exists t1;
create table t1(a int,b float);
insert into t1 values(0,0),(-15,-20),(-22,-12.5);
insert into t1 values(0,360),(30,390),(90,450),(180,270),(180,180);

-- @case
-- @desc:test for func atan() select
select atan(a*pi()/180) as atana,atan(b*pi()/180) atanb from t1;
select atan(a*pi()/180)*atan(b*pi()/180) as atanab,atan(atan(a*pi()/180)) as c from t1;
select b from t1 where atan(a*pi()/180)<=atan(b*pi()/180)  order by a;

-- @suite
-- @setup
drop table if exists t1;
CREATE TABLE t1(
Employee_Name VARCHAR(100) NOT NULL,
Working_At VARCHAR(20) NOT NULL,
Work_Location  VARCHAR(20) NOT NULL,
Joining_Date DATE NOT NULL,
Annual_Income INT  NOT NULL);
INSERT INTO t1
VALUES
('Amit Khan', 'XYZ Digital', 'Kolkata', '2019-10-06', 350000),
('Shreetama Pal', 'ABC Corp.', 'Kolkata', '2018-12-16', 500000),
('Aniket Sharma', 'PQR Soln.', 'Delhi', '2020-01-11', 300000),
('Maitree Jana', 'XYZ Digital', 'Kolkata', '2019-05-01', 400000),
('Priyanka Ojha', 'ABC Corp.', 'Delhi', '2019-02-13', 350000),
('Sayani Mitra', 'XYZ Digital', 'Kolkata', '2019-09-15', 320000),
('Nitin Dey', 'PQR Soln.', 'Delhi', '2019-10-06', 250000),
('Sujata Samanta', 'PQR Soln.', 'Kolkata', '2020-10-06', 350000),
('Sudip Majhi', 'ABC Corp.', 'Delhi', '2018-10-30', 600000),
('Sanjoy Kohli', 'XYZ Digital', 'Delhi', '2019-04-18', 450000);

-- @case
-- @desc:test for func BIT_AND() select
SELECT Working_At, BIT_AND(Annual_Income) AS BITORINCOME FROM t1 group by Working_At;	
SELECT Work_Location, BIT_AND(Annual_Income) AS BITORINCOME FROM t1 Group By Work_Location;

-- @suite
-- @setup
drop table if exists t1;
CREATE TABLE t1(
Employee_Name VARCHAR(100) NOT NULL,
Working_At VARCHAR(20) NOT NULL,
Work_Location  VARCHAR(20) NOT NULL,
Joining_Date DATE NOT NULL,
Annual_Income INT  NOT NULL);
INSERT INTO t1
VALUES
('Amit Khan', 'XYZ Digital', 'Kolkata', '2019-10-06', 350000),
('Shreetama Pal', 'ABC Corp.', 'Kolkata', '2018-12-16', 500000),
('Aniket Sharma', 'PQR Soln.', 'Delhi', '2020-01-11', 300000),
('Maitree Jana', 'XYZ Digital', 'Kolkata', '2019-05-01', 400000),
('Priyanka Ojha', 'ABC Corp.', 'Delhi', '2019-02-13', 350000),
('Sayani Mitra', 'XYZ Digital', 'Kolkata', '2019-09-15', 320000),
('Nitin Dey', 'PQR Soln.', 'Delhi', '2019-10-06', 250000),
('Sujata Samanta', 'PQR Soln.', 'Kolkata', '2020-10-06', 350000),
('Sudip Majhi', 'ABC Corp.', 'Delhi', '2018-10-30', 600000),
('Sanjoy Kohli', 'XYZ Digital', 'Delhi', '2019-04-18', 450000);

-- @case
-- @desc:test for func BIT_AND() select
SELECT Work_Location, BIT_AND(Annual_Income) AS BITORINCOME FROM t1 Group By Work_Location;
SELECT Working_At, BIT_AND(Annual_Income) AS BITORINCOME FROM t1 group by Working_At;

-- @suite
-- @setup
drop table if exists t1;
CREATE TABLE t1(
Employee_Name VARCHAR(100) NOT NULL,
Working_At VARCHAR(20) NOT NULL,
Work_Location  VARCHAR(20) NOT NULL,
Joining_Date DATE NOT NULL,
Annual_Income INT  NOT NULL);
INSERT INTO t1
VALUES
('Amit Khan', 'XYZ Digital', 'Kolkata', '2019-10-06', 350000),
('Shreetama Pal', 'ABC Corp.', 'Kolkata', '2018-12-16', 500000),
('Aniket Sharma', 'PQR Soln.', 'Delhi', '2020-01-11', 300000),
('Maitree Jana', 'XYZ Digital', 'Kolkata', '2019-05-01', 400000),
('Priyanka Ojha', 'ABC Corp.', 'Delhi', '2019-02-13', 350000),
('Sayani Mitra', 'XYZ Digital', 'Kolkata', '2019-09-15', 320000),
('Nitin Dey', 'PQR Soln.', 'Delhi', '2019-10-06', 250000),
('Sujata Samanta', 'PQR Soln.', 'Kolkata', '2020-10-06', 350000),
('Sudip Majhi', 'ABC Corp.', 'Delhi', '2018-10-30', 600000),
('Sanjoy Kohli', 'XYZ Digital', 'Delhi', '2019-04-18', 450000);

-- @case
-- @desc:test for func BIT_XOR() select
SELECT Work_Location, BIT_XOR(Annual_Income) AS BITORINCOME FROM t1 Group By Work_Location;
SELECT Working_At, BIT_XOR(Annual_Income) AS BITORINCOME FROM t1 group by Working_At;

-- @suite
-- @setup
drop table if exists t1;
create table t1(a int,b float);
insert into t1 values(0,0);
insert into t1 values(0,360),(30,390),(90,450),(180,270),(180,180);
-- @case
-- @desc:test for func cos() select
select cos(a),cos(b) from t1;
select cos(a)*cos(b),cos(cos(a)) as c from t1;
select distinct a from t1 where cos(a)<=cos(b) order by a desc;

-- @suite
-- @setup
drop table if exists t1;
create table t1(a int,b float);
insert into t1 values(0,0),(-15,-20),(-22,-12.5);
insert into t1 values(0,360),(30,390),(90,450),(180,270),(180,180);
-- @case
-- @desc:test for func cot() select
select cot(a*pi()/180) as cota,cot(b*pi()/180) cotb from t1;
select cot(a*pi()/180)*cot(b*pi()/180) as cotab,cot(cot(a*pi()/180)) as c from t1;
select b from t1 where cot(a*pi()/180)<=cot(b*pi()/180) order by a;

drop table if exists t1;
create table t1(a date, b datetime,c varchar(30));
insert into t1 values('20220101','2022-01-01 01:01:01','2022-13-13 01:01:01');
select * from t1;


-- @suite
-- @setup
drop table if exists t1;
create table t1(a date, b datetime,c varchar(30));
insert into t1 values('2022-01-01','2022-01-01 01:01:01','2022-01-01 01:01:01');
insert into t1 values('2022-01-01','2022-01-01 01:01:01','2022-01-01 01:01:01');
insert into t1 values('2022-01-02','2022-01-02 23:01:01','2022-01-01 23:01:01');
insert into t1 values('2021-12-31','2021-12-30 23:59:59','2021-12-30 23:59:59');
insert into t1 values('2022-06-30','2021-12-30 23:59:59','2021-12-30 23:59:59');

-- @case
-- @desc:test for func dayofyear() select
select distinct dayofyear(a) as dya from t1;
select * from t1 where dayofyear(a)>120;
select * from t1 where dayofyear(a) between 1 and 184;

-- @suite
-- @setup
drop table if exists t1;
CREATE TABLE t1(a INT,b VARCHAR(100),c CHAR(20));
INSERT INTO t1
VALUES
(1,'Ananya Majumdar', 'XI'),
(2,'Anushka Samanta', 'X'),
(3,'Aniket Sharma', 'XI'),
(4,'Anik Das', 'X'),
(5,'Riya Jain', 'IX'),
(6,'Tapan Samanta', 'XI');

-- @case
-- @desc:test for func endswith() select
select a,endswith(b,'a') from t1;
select a,b,c from t1 where endswith(b,'a')=1 and endswith(c,'I')=1;

-- @suite
-- @setup
drop table if exists t1;
CREATE TABLE t1(Student_id INT,Student_name VARCHAR(100),Student_Class CHAR(20));
INSERT INTO t1
VALUES
(1,'Ananya Majumdar', 'IX'),
(2,'Anushka Samanta', 'X'),
(3,'Aniket Sharma', 'XI'),
(4,'Anik Das', 'X'),
(5,'Riya Jain', 'IX'),
(6,'Tapan Samanta', 'X');

-- @case
-- @desc:test for func LPAD() select
SELECT Student_id, Student_name,LPAD(Student_Class, 10, ' _') AS LeftPaddedString FROM t1;
SELECT Student_id, lpad(Student_name,4,'new') AS LeftPaddedString FROM t1;
SELECT Student_id, lpad(Student_name,-4,'new') AS LeftPaddedString FROM t1;
SELECT Student_id, lpad(Student_name,0,'new') AS LeftPaddedString FROM t1;

-- @suite
-- @setup
drop table if exists t1;
CREATE TABLE t1(Student_id INT,Student_name VARCHAR(100),Student_Class CHAR(20));
INSERT INTO t1
VALUES
(1,'Ananya Majumdar', 'IX'),
(2,'Anushka Samanta', 'X'),
(3,'Aniket Sharma', 'XI'),
(4,'Anik Das', 'X'),
(5,'Riya Jain', 'IX'),
(6,'Tapan Samanta', 'X');

-- @case
-- @desc:test for func rpad() select
SELECT Student_id, Student_name,RPAD(Student_Class, 10, ' _') AS LeftPaddedString FROM t1;
SELECT Student_id, rpad(Student_name,4,'new') AS LeftPaddedString FROM t1;
SELECT Student_id, rpad(Student_name,-4,'new') AS LeftPaddedString FROM t1;
SELECT Student_id, rpad(Student_name,0,'new') AS LeftPaddedString FROM t1;

-- @suite
-- @setup
drop table if exists t1;
CREATE TABLE t1
(
Employee_name VARCHAR(100) NOT NULL,
Joining_Date DATE NOT NULL
);
INSERT INTO t1
(Employee_name, Joining_Date )
VALUES
('     Ananya Majumdar', '2000-01-11'),
('   Anushka Samanta', '2002-11-10' ),
('   Aniket Sharma ', '2005-06-11' ),
('   Anik Das', '2008-01-21'  ),
('  Riya Jain', '2008-02-01' ),
('    Tapan Samanta', '2010-01-11' ),
('   Deepak Sharma', '2014-12-01'  ),
('   Ankana Jana', '2018-08-17'),
('  Shreya Ghosh', '2020-09-10') ;

-- @case
-- @desc:test for func LTRIM() select
-- @sortkey:1
-- @separator:table
SELECT LTRIM( Employee_name) LTrimName,RTRIM(Employee_name) AS RTrimName FROM t1 order by  RTrimName desc;
SELECT LTRIM(RTRIM(Employee_name)) as TrimName from t1 where Employee_name like '%Ani%' order by TrimName asc;
drop table if exists t1;
create table t1(a int,b float);
insert into t1 values(0,0);
insert into t1 values(0,360),(30,390),(90,450),(180,270),(180,180);
select sin(a),sin(b) from t1;
select sin(a)*sin(b),sin(sin(a)) as c from t1;
select distinct a from t1 where sin(a)<=sin(b) order by a desc;

-- @suite
-- @setup
drop table if exists t1;
create table t1(a int,b float);
insert into t1 values(0,0),(-15,-20),(-22,-12.5);
insert into t1 values(0,360),(30,390),(90,450),(180,270),(180,180);

-- @case
-- @desc:test for func sinh() sinh() select
select sinh(a*pi()/180) as sinha,sinh(b*pi()/180) sinhb from t1;
select sinh(a*pi()/180)*sinh(b*pi()/180) as sinhab,sinh(sinh(a*pi()/180)) as c from t1;
select b from t1 where sinh(a*pi()/180)<=sinh(b*pi()/180)  order by a;

-- @suite
-- @setup
drop table if exists t1;
CREATE TABLE t1
(
Employee_name VARCHAR(100) NOT NULL,
Joining_Date DATE NOT NULL
);
INSERT INTO t1
(Employee_name, Joining_Date )
VALUES
('     Ananya Majumdar', '2000-01-11'),
('   Anushka Samanta', '2002-11-10' ),
('   Aniket Sharma ', '2005-06-11' ),
('   Anik Das', '2008-01-21'  ),
('  Riya Jain', '2008-02-01' ),
('    Tapan Samanta', '2010-01-11' ),
('   Deepak Sharma', '2014-12-01'  ),
('   Ankana Jana', '2018-08-17'),
('  Shreya Ghosh', '2020-09-10') ;
INSERT INTO t1
(Employee_name, Joining_Date ) values('     ','2014-12-01');
-- @separator:table
select * from t1 where Employee_name=space(5);
drop table if exists t1;
CREATE TABLE t1(a INT,b VARCHAR(100),c CHAR(20));
INSERT INTO t1
VALUES
(1,'Ananya Majumdar', 'IX'),
(2,'Anushka Samanta', 'X'),
(3,'Aniket Sharma', 'XI'),
(4,'Anik Das', 'X'),
(5,'Riya Jain', 'IX'),
(6,'Tapan Samanta', 'X');


select a,startswith(b,'An') from t1;
select a,b,c from t1 where startswith(b,'An')=1 and startswith(c,'I')=1;

-- @suite
-- @setup
drop table if exists t1;
CREATE TABLE t1(PlayerName VARCHAR(100) NOT NULL,RunScored INT NOT NULL,WicketsTaken INT NOT NULL);
INSERT INTO t1 VALUES('KL Rahul', 52, 0 ),('Hardik Pandya', 30, 1 ),('Ravindra Jadeja', 18, 2 ),('Washington Sundar', 10, 1),('D Chahar', 11, 2 ),  ('Mitchell Starc', 0, 3);
-- @case
-- @desc:test for func STDDEV_POP() select
SELECT STDDEV_POP(RunScored) as Pop_Standard_Deviation FROM t1;
SELECT  STDDEV_POP(WicketsTaken) as Pop_Std_Dev_Wickets FROM t1;

-- @suite
-- @setup
drop table if exists t1;
create table t1(a int,b float);
insert into t1 values(0,0),(-15,-20),(-22,-12.5);
insert into t1 values(0,360),(30,390),(90,450),(180,270),(180,180);
-- @case
-- @desc:test for func tan() select
select tan(a*pi()/180) as tana,tan(b*pi()/180) tanb from t1;

select tan(a*pi()/180)*tan(b*pi()/180) as tanab,tan(a*pi()/180)+tan(b*pi()/180) as c from t1;

select b from t1 where tan(a*pi()/180)<=tan(b*pi()/180)  order by a;

-- @suite
-- @setup
drop table if exists t1;
create table t1(a date,b datetime);
insert into t1 values("2022-06-01","2022-07-01 00:00:00");
insert into t1 values("2022-12-31","2011-01-31 12:00:00");
insert into t1 values("2022-06-12","2022-07-01 00:00:00");

-- @case
-- @desc:test for func weekday() select
select a,weekday(a),b,weekday(b) from t1;
select * from t1 where weekday(a)>weekday(b);
select * from t1 where weekday(a) between 0 and 4;

-- @suite
-- @setup
drop table if exists t1;
create table t1(a date,b datetime);
insert into t1 values("2022-06-01","2022-07-01 00:00:00");
insert into t1 values("2022-12-31","2011-01-31 12:00:00");
insert into t1 values("2022-06-12","2022-07-01 00:00:00");
-- @case
-- @desc:test for func weekday() select
select a,weekday(a),b,weekday(b) from t1;
select * from t1 where weekday(a)>weekday(b);
select * from t1 where weekday(a) between 0 and 4;

-- @suite
-- @setup
drop table if exists t1;
create table t1(a date, b datetime);
insert into t1 values('2022-01-01','2022-01-01 01:01:01');
insert into t1 values('2022-01-01','2022-01-01 01:01:01');
insert into t1 values('2022-01-02','2022-01-02 23:01:01');
insert into t1 values('2021-12-31','2021-12-30 23:59:59');
insert into t1 values('2022-06-30','2021-12-30 23:59:59');

-- @case
-- @desc:test for func date() select
select date(a),date(b) from t1;
select date(a),date(date(a)) as dda from t1;

drop table t1;

-- @suite
-- @setup
drop table if exists t1;
create table t1(a datetime, b timestamp);
insert into t1 values("2022-07-01", "2011-01-31 12:00:00");
insert into t1 values("2011-01-31 12:32:11", "1979-10-22");
insert into t1 values(NULL, "2022-08-01 23:10:11");
insert into t1 values("2011-01-31", NULL);
insert into t1 values("2022-06-01 14:11:09","2022-07-01 00:00:00");
insert into t1 values("2022-12-31","2011-01-31 12:00:00");
insert into t1 values("2022-06-12","2022-07-01 00:00:00");

-- @case
-- @desc:test for func hour() select
select hour(a),hour(b) from t1;
select * from t1 where hour(a)>hour(b);
select * from t1 where hour(a) between 10 and 16;

-- @case
-- @desc:test for func minute() select
select minute(a),minute(b) from t1;
select * from t1 where minute(a)<=minute(b);
select * from t1 where minute(a) between 10 and 36;

-- @case
-- @desc:test for func second() select
select second(a),second(b) from t1;
select * from t1 where second(a)>=second(b);
select * from t1 where second(a) between 10 and 36;

-- @teardown
drop table if exists t1;

-- @suite
-- @setup
drop table if exists t1;
create table t1(a int, b int);
select mo_table_rows(db_name,'t1'),mo_table_size(db_name,'t1') from (select database() as db_name);
insert into t1 values(1, 2);
insert into t1 values(3, 4);
select mo_table_rows(db_name,'t1'),mo_table_size(db_name,'t1') from (select database() as db_name);


-- @teardown
drop table if exists t1;

drop database if exists test01;
create database test01;
use test01;
create table t(a int, b varchar(10));
insert into t values(1, 'h'), (2, 'b'), (3, 'c'), (4, 'q'), (5, 'd'), (6, 'b'), (7, 's'), (8, 'a'), (9, 'z'), (10, 'm');
-- @separator:table
select mo_ctl('dn', 'flush', 'test01.t');
select mo_table_col_max('test01', 't', 'a'), mo_table_col_min('test01', 't', 'a');
drop table t;
drop database test01;