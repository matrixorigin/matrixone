drop database if exists db1;
create database db1;
use db1;

drop table if exists t1;
CREATE TABLE t1 (a INTEGER PRIMARY KEY, b CHAR(10));
desc t1;

insert into t1 values(1, 'ab');
insert into t1 values(2, 'ac');
insert into t1 values(3, 'ad');
select * from t1;

--modify单列主键不丢失
alter table t1 modify a VARCHAR(20);
desc t1;
select * from t1;
--------------------------------------------------------------------------------------
drop table if exists t1;
CREATE TABLE t1 (a INTEGER PRIMARY KEY, b CHAR(10), c date);
desc t1;

insert into t1 values(1, 'ab', '1980-12-17');
insert into t1 values(2, 'ac', '1981-02-20');
insert into t1 values(3, 'ad', '1981-02-22');
select * from t1;

alter table t1 modify a VARCHAR(20) after b;
desc t1;
select * from t1;
---------------------------------------------------------------------------------------
drop table if exists t1;
CREATE TABLE t1 (a INTEGER PRIMARY KEY, b CHAR(10), c date);
desc t1;

insert into t1 values(1, 'ab', '1980-12-17');
insert into t1 values(2, 'ac', '1981-02-20');
insert into t1 values(3, 'ad', '1981-02-22');
select * from t1;

alter table t1 modify a VARCHAR(20) after c;
desc t1;
select * from t1;
--------------------------------------------------------------------------------------
drop table if exists t1;
CREATE TABLE t1 (a INTEGER, b CHAR(10), c date, PRIMARY KEY(a));
desc t1;

insert into t1 values(1, 'ab', '1980-12-17');
insert into t1 values(2, 'ac', '1981-02-20');
insert into t1 values(3, 'ad', '1981-02-22');
select * from t1;

alter table t1 modify b VARCHAR(20) first;
desc t1;
select * from t1;
-----------------------------------------------------------------------------------
drop table if exists t1;
CREATE TABLE t1 (a INTEGER PRIMARY KEY, b CHAR(10), c date, d decimal(7,2));
desc t1;

insert into t1 values(1, 'ab', '1980-12-17', 800);
insert into t1 values(2, 'ac', '1981-02-20', 1600);
insert into t1 values(3, 'ad', '1981-02-22', 500);
select * from t1;

alter table t1 modify b VARCHAR(20), modify d int unsigned;
desc t1;
select * from t1;
----------------------------------------------------------------------------------------
drop table if exists t1;
CREATE TABLE t1 (a INTEGER PRIMARY KEY, b CHAR(10), c date, d decimal(7,2));
desc t1;

insert into t1 values(1, 'ab', '1980-12-17', 800);
insert into t1 values(2, 'ac', '1981-02-20', 1600);
insert into t1 values(3, 'ad', '1981-02-22', 500);

select * from t1;
--ERROR 1068 (42000): Multiple primary key defined
alter table t1 modify a VARCHAR(20) PRIMARY KEY;
--ERROR 1068 (42000): Multiple primary key defined
alter table t1 modify b VARCHAR(20) PRIMARY KEY;

alter table t1 modify b VARCHAR(20) first, modify d int unsigned after b;
desc t1;
select * from t1;
----------------------------------------------------------------------------------------
drop table if exists t1;
CREATE TABLE t1(a INTEGER, b CHAR(10), c datetime, d decimal(7,2), PRIMARY KEY(a, b));
desc t1;

insert into t1 values(1, 'ab', '1980-12-17 11:34:45', 800);
insert into t1 values(2, 'ac', '1981-02-20 10:34:45', 1600);
insert into t1 values(3, 'ad', '1981-02-22 09:34:45', 500);
select * from t1;

alter table t1 modify c datetime default '2023-06-21 12:34:45' on update CURRENT_TIMESTAMP;
desc t1;
select * from t1;

alter table t1 modify c date;
desc t1;
select * from t1;
----------------------------------------------------------------------------------------
drop table if exists t1;
CREATE TABLE t1 (a INTEGER, b CHAR(10), c datetime PRIMARY KEY default '2023-06-21' on update CURRENT_TIMESTAMP);
desc t1;

insert into t1 values(1, 'ab', '1980-12-17');
insert into t1 values(2, 'ac', '1981-02-20');
insert into t1 values(3, 'ad', '1981-02-22');
select * from t1;

alter table t1 modify c date first;
desc t1;
select * from t1;

alter table t1 modify c datetime default '2023-06-21';
desc t1;
select * from t1;
-----------------------------------------------------------------------------------------
drop table if exists t1;
CREATE TABLE t1(a INTEGER PRIMARY KEY, b CHAR(10), c date, d decimal(7,2), UNIQUE KEY(a, b));
desc t1;

insert into t1 values(1, 'ab', '1980-12-17', 800);
insert into t1 values(2, 'ac', '1981-02-20', 1600);
insert into t1 values(3, 'ad', '1981-02-22', 500);
select * from t1;

alter table t1 modify b VARCHAR(20);
show index from t1;

alter table t1 modify b VARCHAR(20) UNIQUE KEY;
show index from t1;

alter table t1 modify b VARCHAR(20) UNIQUE KEY;
show index from t1;

desc t1;
select * from t1;
-----------------------------------------------------------------------------------------
drop table if exists t1;
CREATE TABLE t1(a INTEGER PRIMARY KEY, b CHAR(10), c date, d decimal(7,2), INDEX(a, b), KEY(c));
desc t1;

insert into t1 values(1, 'ab', '1980-12-17', 800);
insert into t1 values(2, 'ac', '1981-02-20', 1600);
insert into t1 values(3, 'ad', '1981-02-22', 500);
select * from t1;

alter table t1 modify b VARCHAR(20);
show index from t1;

--ERROR 1068 (42000): Multiple primary key defined
alter table t1 modify b VARCHAR(20) KEY;


alter table t1 modify b VARCHAR(20) UNIQUE KEY;
show index from t1;

alter table t1 modify c VARCHAR(20) UNIQUE KEY;
show index from t1;

desc t1;
select * from t1;
-----------------------------------------------------------------------------------------
drop table if exists t1;
create table t1(
                   empno int unsigned auto_increment,
                   ename varchar(15) ,
                   job varchar(10),
                   mgr int unsigned,
                   hiredate date,
                   sal decimal(7,2),
                   comm decimal(7,2),
                   deptno int unsigned,
                   primary key(empno, ename)
);
desc t1;

INSERT INTO t1 VALUES (7369,'SMITH','CLERK',7902,'1980-12-17',800,NULL,20);
INSERT INTO t1 VALUES (7499,'ALLEN','SALESMAN',7698,'1981-02-20',1600,300,30);
INSERT INTO t1 VALUES (7521,'WARD','SALESMAN',7698,'1981-02-22',1250,500,30);
INSERT INTO t1 VALUES (7566,'JONES','MANAGER',7839,'1981-04-02',2975,NULL,20);
INSERT INTO t1 VALUES (7654,'MARTIN','SALESMAN',7698,'1981-09-28',1250,1400,30);
INSERT INTO t1 VALUES (7698,'BLAKE','MANAGER',7839,'1981-05-01',2850,NULL,30);
INSERT INTO t1 VALUES (7782,'CLARK','MANAGER',7839,'1981-06-09',2450,NULL,10);
INSERT INTO t1 VALUES (7788,'SCOTT','ANALYST',7566,'0087-07-13',3000,NULL,20);
INSERT INTO t1 VALUES (7839,'KING','PRESIDENT',NULL,'1981-11-17',5000,NULL,10);
INSERT INTO t1 VALUES (7844,'TURNER','SALESMAN',7698,'1981-09-08',1500,0,30);
INSERT INTO t1 VALUES (7876,'ADAMS','CLERK',7788,'0087-07-13',1100,NULL,20);
INSERT INTO t1 VALUES (7900,'JAMES','CLERK',7698,'1981-12-03',950,NULL,30);
INSERT INTO t1 VALUES (7902,'FORD','ANALYST',7566,'1981-12-03',3000,NULL,20);
INSERT INTO t1 VALUES (7934,'MILLER','CLERK',7782,'1982-01-23',1300,NULL,10);
select * from t1;

alter table t1 modify empno varchar(20) after sal;
desc t1;
select * from t1;
-------------------------------------------------------------------------------------------
drop table if exists t1;
create table t1(a int unsigned, b varchar(15) NOT NULL, c date, d decimal(7,2), primary key(a));
desc t1;

insert into t1 values (7369,'SMITH','1980-12-17',800);
insert into t1 values  (7499,'ALLEN','1981-02-20',1600);
insert into t1 values (7521,'WARD','1981-02-22',1250);
insert into t1 values  (7566,'JONES','1981-04-02',2975);
insert into t1 values  (7654,'MARTIN','1981-09-28',1250);
select * from t1;

alter table t1 modify a int auto_increment;
desc t1;
select * from t1;

alter table t1 modify d decimal(6,2);
desc t1;
select * from t1;
-------------------------------------------------------------------------------------------
drop table if exists dept;
create table dept(
                     deptno varchar(20),
                     dname varchar(15),
                     loc varchar(50),
                     primary key(deptno)
);

INSERT INTO dept VALUES (10,'ACCOUNTING','NEW YORK');
INSERT INTO dept VALUES (20,'RESEARCH','DALLAS');
INSERT INTO dept VALUES (30,'SALES','CHICAGO');
INSERT INTO dept VALUES (40,'OPERATIONS','BOSTON');

drop table if exists emp;
create table emp(
                    empno int unsigned auto_increment,
                    ename varchar(15),
                    job varchar(10),
                    mgr int unsigned,
                    hiredate date,
                    sal decimal(7,2),
                    comm decimal(7,2),
                    deptno varchar(20),
                    primary key(empno),
                    constraint `c1` FOREIGN KEY (deptno) REFERENCES dept(deptno)
);

INSERT INTO emp VALUES (7369,'SMITH','CLERK',7902,'1980-12-17',800,NULL,20);
INSERT INTO emp VALUES (7499,'ALLEN','SALESMAN',7698,'1981-02-20',1600,300,30);
INSERT INTO emp VALUES (7521,'WARD','SALESMAN',7698,'1981-02-22',1250,500,30);
INSERT INTO emp VALUES (7566,'JONES','MANAGER',7839,'1981-04-02',2975,NULL,20);
INSERT INTO emp VALUES (7654,'MARTIN','SALESMAN',7698,'1981-09-28',1250,1400,30);
INSERT INTO emp VALUES (7698,'BLAKE','MANAGER',7839,'1981-05-01',2850,NULL,30);
INSERT INTO emp VALUES (7782,'CLARK','MANAGER',7839,'1981-06-09',2450,NULL,10);
INSERT INTO emp VALUES (7788,'SCOTT','ANALYST',7566,'0087-07-13',3000,NULL,20);
INSERT INTO emp VALUES (7839,'KING','PRESIDENT',NULL,'1981-11-17',5000,NULL,10);
INSERT INTO emp VALUES (7844,'TURNER','SALESMAN',7698,'1981-09-08',1500,0,30);
INSERT INTO emp VALUES (7876,'ADAMS','CLERK',7788,'0087-07-13',1100,NULL,20);
INSERT INTO emp VALUES (7900,'JAMES','CLERK',7698,'1981-12-03',950,NULL,30);
INSERT INTO emp VALUES (7902,'FORD','ANALYST',7566,'1981-12-03',3000,NULL,20);
INSERT INTO emp VALUES (7934,'MILLER','CLERK',7782,'1982-01-23',1300,NULL,10);

--ERROR 1832 (HY000): Cannot change column 'deptno': used in a foreign key constraint 'emp_ibfk_1'
alter table emp modify deptno char(20);
--ERROR 1832 (HY000): Cannot change column 'deptno': used in a foreign key constraint 'emp_ibfk_1'
alter table emp modify deptno int;
--ERROR 1832 (HY000): Cannot change column 'deptno': used in a foreign key constraint 'emp_ibfk_1'
alter table emp modify deptno varchar(10);
alter table emp modify deptno varchar(25);
desc emp;
select * from emp;

drop table emp;
drop table dept;
----------------------------------------------------------------------------------------
drop table if exists t1;
CREATE TABLE t1(col1 int not null, col2 varchar(10));
insert into t1 values (1, '137iu2');
insert into t1 values (1, '73ujf34f');
select * from t1;

alter table t1 modify col1 int primary key;
--ERROR 1062 (23000): Duplicate entry '1' for key 't1.PRIMARY'
desc t1;

alter table t1 modify col2 varchar(10) primary key;
desc t1;

insert into t1 values (1, 'cdsdsa');
select * from t1;
drop table t1;
drop database if exists db2;

-- @suit
-- @case
-- @desc: alter table modify column
-- @label:bvt

drop database if exists test;
use test;

-- abnormal test: modify column to bool
drop table if exists bool01;
create table bool01 (col1 int, col2 char);
insert into bool01 values (1, 'q');
insert into bool01 values (2, 'a');
insert into bool01 values (10, '3');
select * from bool01;
alter table bool01 modify col2 bool after col1;
show create table bool01;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'bool01' and COLUMN_NAME not like '__mo%';
drop table bool01;


-- cast char to varchar after col
drop table if exists char01;
create table char01 (col1 int, col2 char);
insert into char01 values (1, 'q');
insert into char01 values (2, '*');
insert into char01 values (10, '3');
insert into char01 values (20, '数');
select * from char01;
alter table char01 modify col2 varchar(20) after col1;
show create table char01;
insert into char01 values (100, '**(*(&(*UJHI');
truncate table char01;
select * from char01;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'char01' and COLUMN_NAME not like '__mo%';
drop table char01;


-- cast varchar to char first
drop table if exists char02;
create table char02 (col1 int, col2 varchar(10));
insert into char02 values (1, 'w43234rfq');
insert into char02 values (2, 'a32f4');
insert into char02 values (10, '3432t43r4f');
select * from char02;
alter table char02 modify col2 char(20) first;
show create table char02;
insert into char02 values ('738fewhu&^YUH', 100);
select * from char02;
update char02 set col1 = 100 where col2 = 'w43234rfq';
select * from char02;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'char02' and COLUMN_NAME not like '__mo%';
drop table char02;


-- abnormal test: varchar length modify to char length, and varchar length > char length
drop table if exists char03;
create table char03 (col1 int, col2 char(10));
insert into char03 values (1, 'ahu323ew32');
insert into char03 values (2, '367283r343');
insert into char03 values (3, null);
select * from char03;
alter table char03 modify col2 varchar(5);
show create table char03;
insert into char03 values (4, 'eyuiwqewq3');
select * from char03;
delete from char03 where col2 = 'eyuiwqewq3';
select * from char03;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'char03' and COLUMN_NAME not like '__mo%';
drop table char03;


-- abnormal test: null modify to not null
drop table if exists char04;
create table char04 (col1 varchar(200), col2 char(10));
insert into char04 values (null, 'ahu323ew32');
insert into char04 values ('23789178942u1uj3ridjfh2d28u49u4ueji32jf2f32ef32894rjk32nv432f432f', '367283r343');
insert into char04 values ('32jhbfchjecmwd%^&^(*&)UJHFRE%^T&YUHIJKNM', null);
select * from char04;
alter table char04 modify col1 varchar(100) not null;
alter table char04 modify col2 char(10) not null;
show create table char04;
insert into char04 values (4, 'eyuiwqewq3');
select * from char04;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'char04' and COLUMN_NAME not like '__mo%';
drop table char04;

-- text, mediumtext, longtext
drop table if exists text01;
create table text01 (col1 text primary key , col2 mediumtext unique index, col3 longtext);
insert into text01 values ('37268434','32718hcuwh432fr234f34g4f34e4','&*&YHRE%^&*YUIHGT^&Y*UIJK');
insert into text01 values ('jefwyq3uih2r321fr3', '38eu4jfc3w4e3dcewcevewcve', null);
insert into text01 values (null, '2789378u2uifj4234r23', '32r43frecdrfedwq');
select * from text01;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'text01' and COLUMN_NAME not like '__mo%';
drop table text01;


-- continuously modify column
drop table if exists alter03;
create table alter03 (col1 int, col2 binary, col3 decimal);
insert into alter03 values (1, 'e', 324214.2134123);
insert into alter03 values (2, '4', -242134.3231432);
select * from alter03;
alter table alter03 modify col1 decimal after col3, modify col2 varbinary(20);
show create table alter03;
insert into alter03 values ('32143124', 42432321.000, 132432.214234);
select * from alter03;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'alter03' and COLUMN_NAME not like '__mo%';
drop table alter03;


-- modify column auto_increment
drop table if exists alter04;
create table alter04 (col1 int not null default 100 primary key );
insert into alter04 values ();
insert into alter04 values (101);
alter table alter04 modify col1 int auto_increment;
show create table alter04;
insert into alter04 values ();
insert into alter04 values ();
select * from alter04;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'alter04' and COLUMN_NAME not like '__mo%';
drop table alter04;


-- modify auto_increment column
drop table if exists alter05;
create table alter05 (col1 int primary key auto_increment);
insert into alter05 values ();
insert into alter05 values ();
select * from alter05;
alter table alter05 modify col1 int unique key;
show create table alter05;
insert into alter05 values ();
insert into alter05 values ();
select * from alter05;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'alter05' and COLUMN_NAME not like '__mo%';
drop table alter05;


-- modify column to primary key column
drop table if exists primary01;
create table primary01 (col1 int, col2 text);
insert into primary01 values (1, 'wq432r43rf32y2493821ijfk2env3ui4y33i24');
insert into primary01 values (2, '243ewfvefreverewfcwr');
alter table primary01 modify col1 float primary key;
show create table primary01;
insert into primary01 values (1, '432r2f234day89ujfw42342');
insert into primary01 values (2378.32423, '234242))())_');
select * from primary01;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'primary01' and COLUMN_NAME not like '__mo%';
drop table primary01;


-- modify column to primary key, duplicate values exist in the table
drop table if exists primary02;
create table primary02(col1 int, col2 binary(10));
insert into primary02 values (1, '32143');
insert into primary02 values (1, '3e');
select * from primary02;
alter table primary02 modify col1 int primary key;
show create table primary02;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'primary02' and COLUMN_NAME not like '__mo%';
drop table primary02;


-- modify column to primary key, no duplicate values in the table
drop table if exists primary03;
create table primary03(col1 int, col2 binary(10));
insert into primary03 values (1, '32143');
insert into primary03 values (2, '3e');
alter table primary03 modify col1 int primary key;
show create table primary03;
insert into primary03 values (3, '*');
insert into primary03 values (3, 'assad');
select * from primary03;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'primary03' and COLUMN_NAME not like '__mo%';
drop table primary03;


-- modify column with primary key
drop table if exists primary04;
create table primary04(col1 int primary key ,col2 varbinary(20));
insert into primary04 values (1, 'qfreqvreq');
insert into primary04 values (2, '324543##');
alter table primary04 modify col1 float;
show create table primary04;
insert into primary04 values (1, '324342__');
insert into primary04 values (3, 'qw');
select * from primary04;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'primary04' and COLUMN_NAME not like '__mo%';
drop table primary04;


-- primary key exist in the table, modify another column to primary key
drop table if exists primary05;
create table primary05(col1 int primary key ,col2 varbinary(20));
insert into primary05 values (1, 'qfreqvreq');
insert into primary05 values (2, '324543##');
alter table primary05 modify col2 binary(30) primary key;
show create table primary05;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'primary05' and COLUMN_NAME not like '__mo%';
drop table primary05;


-- multiple primary key defined
drop table if exists primary06;
create table primary06(col1 int primary key ,col2 varbinary(20));
insert into primary06 values (1, 'qfreqvreq');
insert into primary06 values (2, '324543##');
alter table primary06 modify col1 int unsigned primary key;
alter table primary06 modify col2 binary(30) primary key;
show create table primary06;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'primary06' and COLUMN_NAME not like '__mo%';
drop table primary06;


-- abnormal test :joint primary key, modify one of the primary column null
drop table if exists primary07;
create table primary07(col1 int ,col2 float, col3 decimal, primary key (col1, col2));
insert into primary07 values (1, 213412.32143, 3214312.34243214242);
insert into primary07 values (2, -324.2342432423, -1243.42334234242);
alter table primary07 modify col1 double default null;
alter table primary07 modify col2 int default null;
show create table primary07;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'primary07' and COLUMN_NAME not like '__mo%';
drop table primary07;


-- joint primary key
drop table if exists primary07;
create table primary07(col1 int ,col2 float, col3 decimal, primary key (col1, col2));
insert into primary07 values (1, 213412.32143, 3214312.34243214242);
insert into primary07 values (2, -324.2342432423, -1243.42334234242);
alter table primary07 modify col1 double not null;
alter table primary07 modify col2 decimal(28,10);
show create table primary07;
drop table primary07;


-- unique key
drop table if exists index01;
CREATE TABLE index01(a INTEGER not null , b CHAR(10), c date, d decimal(7,2), UNIQUE KEY(a, b));
show create table index01;
insert into index01 values(1, 'ab', '1980-12-17', 800);
insert into index01 values(2, 'ac', '1981-02-20', 1600);
insert into index01 values(3, 'ad', '1981-02-22', 500);
select * from index01;
alter table index01 modify b VARCHAR(20);
show create table index01;
show index from index01;
alter table index01 modify b VARCHAR(20) UNIQUE KEY;
show index from index01;
show create table index01;
select * from index01;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'index01' and COLUMN_NAME not like '__mo%';
drop table index01;


-- index
drop table if exists index02;
CREATE TABLE index02(a INTEGER PRIMARY KEY, b CHAR(10), c date, d decimal(7,2), INDEX(a, b), KEY(c));
insert into index02 values(1, 'ab', '1980-12-17', 800);
insert into index02 values(2, 'ac', '1981-02-20', 1600);
insert into index02 values(3, 'ad', '1981-02-22', 500);
select * from index02;
alter table index02 modify b VARCHAR(20);
show index from index02;
alter table index02 modify b VARCHAR(20) KEY;
show create table index02;
alter table index02 modify b VARCHAR(20) UNIQUE KEY;
show index from index02;
alter table index02 modify c VARCHAR(20) UNIQUE KEY;
show index from index02;
show create table index02;
desc index02;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'index02' and COLUMN_NAME not like '__mo%';
select * from index02;
drop table index02;


-- foreign key
drop table if exists foreign01;
create table foreign01(col1 int primary key,
                       col2 varchar(20),
                       col3 int,
                       col4 bigint);
drop table if exists foreign02;
create table foreign02(col1 int,
                       col2 int,
                       col3 int primary key,
                       constraint `c1` foreign key(col1) references foreign01(col1));
show create table foreign01;
show create table foreign02;
insert into foreign01 values(1,'sfhuwe',1,1);
insert into foreign01 values(2,'37829901k3d',2,2);
insert into foreign02 values(1,1,1);
insert into foreign02 values(2,2,2);
select * from foreign01;
select * from foreign02;
alter table foreign01 modify col1 decimal;
alter table foreign02 modify col1 float after col3;
show create table foreign01;
show create table foreign02;
alter table foreign01 modify col2 varchar(100);
alter table foreign02 modify col2 double after col3;
show create table foreign01;
show create table foreign02;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'foreign01' and COLUMN_NAME not like '__mo%';
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'foreign02' and COLUMN_NAME not like '__mo%';
drop table foreign02;
drop table foreign01;


-- decimal：Conversion between high precision and low precision
drop table if exists decimal01;
create table decimal01 (col1 decimal,col2 decimal(38,10));
insert into decimal01 values (23746723468723.242334243, 38208439024234.43242342423423423422342);
insert into decimal01 values (32487329847923424, -4324324324.3297932749028949373324242423424);
alter table decimal01 modify column col1 decimal(38,6);
alter table decimal01 modify col2 decimal(38,9);
show create table decimal01;
select * from decimal01;
truncate table decimal01;
alter table decimal01 add column col3 decimal(19,0) first;
show create table decimal01;
insert into decimal01 values (37298342.123, -37829342.3244234, -283794324.2342);
select * from decimal01;
drop table decimal01;


-- abnormal test: modify column from null to not null, null exist in the table
drop table if exists null01;
create table null01(col1 int default null, col2 binary(10));
insert into null01 values (1, '32143');
insert into null01 values (null, '3e');
alter table null01 modify col1 int not null;
show create table null01;
select * from null01;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'null01' and COLUMN_NAME not like '__mo%';
drop table null01;


-- modify column from null to not null, null does not exist in the table
drop table if exists null02;
create table null02(col1 int default null, col2 varbinary(100));
insert into null02 values (1, '32143');
insert into null02 values (2, '3e');
alter table null02 modify col1 int not null;
show create table null02;
insert into null02 values (null, '1');
insert into null02 values (342, 'aesd');
select * from null02;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'null02' and COLUMN_NAME not like '__mo%';
drop table null02;


-- cast int to float、double、decimal
drop table if exists cast01;
create table cast01 (col1 int, col2 smallint, col3 bigint unsigned, col4 tinyint unsigned);
insert into cast01 values (1, -32768, 12352314214243242, 0);
insert into cast01 values (329884234, 32767, 3828493, 21);
insert into cast01 values (-29302423, 32, 324242132321, 10);
insert into cast01 values (null, null, null, null);
select * from cast01;
alter table cast01 modify col1 float;
show create table cast01;
insert into cast01 values (3271.312432, null, 323254324321432, 100);
select * from cast01;
alter table cast01 modify col2 double first;
show create table cast01;
insert into cast01 values (3271834.2134, -3892843.214, 328943232, 255);
select * from cast01;
alter table cast01 modify col3 double;
show create table cast01;
insert into cast01 values (3271834.2134, -3892843.214, 328943232.3234, 255);
select * from cast01;
alter table cast01 modify col4 decimal(28,10) after col2;
show create table cast01;
insert into cast01 values (3271834.2134, -3823243.4324, 328943232.3234, -32423.43243);
select * from cast01;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'cast01' and COLUMN_NAME not like '__mo%';
drop table cast01;


-- cast float、double、decimal to int
drop table if exists cast02;
create table cast02 (col1 float, col2 double, col3 decimal(30,5), col4 decimal(37,1));
insert into cast02 values (1.321341, -32768.32142, 1235231421424.3214242134124324323, 12342.43243242121);
insert into cast02 values (329884234.3242, null, 3828493, 21);
insert into cast02 values (93024232.32324, -32.243142, 324242132321, null);
select * from cast02;
alter table cast02 modify col1 int unsigned;
show create table cast02;
insert into cast02 values (2724.327832, null, 32325432421432, 100.3322142142);
select * from cast02;
alter table cast02 modify col2 bigint;
show create table cast02;
insert into cast02 values (1000, 323421423421342, 328943232.321424, -255.321151234);
select * from cast02;
alter table cast02 modify col3 bigint unsigned;
show create table cast02;
insert into cast02 values (32718, 100, 32894323237289, 234);
select * from cast02;
alter table cast02 modify col4 smallint first;
show create table cast02;
insert into cast02 values (234, 32718, 100, 32894323237289);
select * from cast02;
drop table cast02;


-- numeric type cast to char
drop table if exists cast03;
create table cast03 (col1 smallint unsigned, col2 float, col3 double, col4 decimal);
insert into cast03 values (1, 323242.34242, 23432.3242, 8329498352.32534242323432);
insert into cast03 values (200, -213443.321412, 32424.342424242, 0.382943424324234);
insert into cast03 (col1, col2, col3, col4) values (null, null, null, null);
alter table cast03 modify col1 char(50), modify col2 char(100), modify col3 varchar(50), modify col4 varchar(15) first;
show create table cast03;
insert into cast03 values ('3243342', '3242f()', '4728947234342,', '457328990r3if943i4u9owiuo4ewfr3w4r3fre');
select * from cast03;
drop table cast03;


-- converts a character type in numeric format to a numeric type（负数需要转换为 SIGNED）
drop table if exists cast04;
create table cast04 (col1 char, col2 varchar, col3 text, col4 blob);
insert into cast04 values ('1', '-281321.21312', '328', '327482739.32413');
insert into cast04 values ('0', '3412234321', '-332134324.2432423423423', '-1032412.4324');
insert into cast04 values (null, null, null, null);
alter table cast04 modify col1 int unsigned;
alter table cast04 modify col2 decimal(34,4) after col4;
show create table cast04;
alter table cast04 modify col3 double, modify col4 float;
show create table cast04;
insert into cast04 values ();
select * from cast04;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'cast04' and COLUMN_NAME not like '__mo%';
drop table cast04;


-- column date modify to column char and datetime
drop table if exists cast05;
create table cast05 (col1 date);
insert into cast05 values ('1997-01-13');
insert into cast05 values ('2023-12-12');
insert into cast05 values (null);
select * from cast05;
alter table cast05 modify col1 varchar(100);
alter table cast05 modify col1 datetime first;
select * from cast05;
show create table cast05;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'cast05' and COLUMN_NAME not like '__mo%';
drop table cast05;


-- unsigned modify to signed
drop table if exists abnormal02;
create table abnormal02 (col1 int unsigned not null, col2 smallint unsigned);
insert into abnormal02 values (2147483647, 255);
insert into abnormal02 values (3242334, 10);
select * from abnormal02;
alter table abnormal02 modify col1 int;
alter table abnormal02 modify col2 smallint;
select * from abnormal02;
show create table abnormal02;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'abnormal02' and COLUMN_NAME not like '__mo%';
drop table abnormal02;


-- abnormal test：The data in the modified column exceeds the normal range of the column
drop table if exists abnormal01;
create table abnormal01 (col1 int, col2 bigint, col3 decimal);
insert into abnormal01 values (2147483647, 9223372036854775807, 3291843920.32783);
insert into abnormal01 values (-21843243, 832942343241999999, -2787343243.2343);
insert into abnormal01 values (null, null, null);
select * from abnormal01;
alter table abnormal01 modify col2 int;
alter table abnormal01 modify col1 smallint;
alter table abnormal01 modify col3 decimal(10,0);
select * from abnormal01;
show create table abnormal01;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'abnormal01' and COLUMN_NAME not like '__mo%';
drop table abnormal01;


-- abnormal test:signed modify to unsigned
drop table if exists abnormal02;
create table abnormal02 (col1 int not null, col2 smallint);
insert into abnormal02 values (-2147483647, 255);
insert into abnormal02 values (3242334, -10);
select * from abnormal02;
alter table abnormal02 modify col1 int unsigned;
alter table abnormal02 modify col2 smallint unsigned;
select * from abnormal02;
show create table abnormal02;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'abnormal02' and COLUMN_NAME not like '__mo%';
drop table abnormal02;


-- column datetime modify to varchar, abnormal test:blob column modify to int
drop table if exists abnormal03;
create table abnormal03 (col1 datetime, col2 blob);
insert into abnormal03 values ('1997-01-13 00:00:00', '342ewfyuehcdeiuopwu4jo3lekwdfhiu48woi3jrdnefrbwui34f');
insert into abnormal03 values ('2012-01-13 23:23:59', '63298ufh3jcweuiv4h32jhf432ouy4hu3enjwfnwje4n3bj24f34573');
select * from abnormal03;
alter table abnormal03 modify col1 varchar(100);
alter table abnormal03 modify col2 int;
select * from abnormal03;
show create table abnormal03;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'abnormal03' and COLUMN_NAME not like '__mo%';
drop table abnormal03;


-- abnormal test: column json、enum modify to float,int
drop table if exists abnormal04;
create table abnormal04 (col1 json, col2 enum('a', 'b'));
insert into abnormal04 values (('{"x": 17, "x": "red"}'),'a');
insert into abnormal04 values (('{"x": 17, "x": "red", "x": [3, 5, 7]}'), 'b');
select * from abnormal04;
alter table abnormal04 modify col1 float;
alter table abnormal04 modify col2 int;
select * from abnormal04;
show create table abnormal04;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'abnormal04' and COLUMN_NAME not like '__mo%';
drop table abnormal04;


-- abnormal test: temporary table
drop table if exists abnormal05;
create temporary table abnormal05 (col1 datetime, col2 blob);
insert into abnormal05 values ('1997-01-13 00:00:00', '342ewfyuehcdeiuopwu4jo3lekwdfhiu48woi3jrdnefrbwui34f');
insert into abnormal05 values ('2012-01-13 23:23:59', '63298ufh3jcweuiv4h32jhf432ouy4hu3enjwfnwje4n3bj24f34573');
select * from abnormal05;
alter table abnormal05 modify col1 float;
alter table abnormal05 modify col2 int;
select * from abnormal05;
show create table abnormal05;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'abnormal05' and COLUMN_NAME not like '__mo%';
drop table abnormal05;


-- creating table, creating view, modifying the columns, view the view
drop table if exists view01;
drop table if exists view02;
create table view01 (a int);
insert into view01 values (1),(2);
create table view02 (a int);
insert into view02 values (1);
create view v0 as select view01.a, view02.a as b from view01 left join view02 using(a);
show create view v0;
alter table view01 modify a float;
show create table view01;
show create view v0;
drop table view01;
drop table view02;
drop view v0;


-- cluster by
drop table if exists cluster01;
create table cluster01(col1 int, col2 decimal) cluster by col1;
insert into cluster01 values (1, 389234924);
insert into cluster01 values (2, -2893428);
alter table cluster01 modify column col2 float;
alter table cluster01 modify col1 double after col2;
show create table cluster01;
select * from cluster01;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'cluster01' and COLUMN_NAME not like '__mo%';
drop table cluster01;


-- insert into select
drop table if exists insert01;
create table insert01 (col1 int, col2 decimal(20,10), col3 date);
alter table insert01 modify col1 float;
insert into insert01 values (1, 2378324.324324234, '2020-12-12');
insert into insert01 values (2, -372893894.32234230000, '1970-01-01');
alter table insert01 modify col1 float;
alter table insert01 modify col2 decimal(30,1);
show create table insert01;
insert into insert01 select * from insert01;
select * from insert01;
drop table insert01;


-- cast date, datetime, timestamp, time to int
drop table if exists time01;
create table time01 (col1 date, col2 datetime, col3 timestamp, col4 time);
insert into time01 values ('2020-01-01', '2000-10-10 12:12:12', '1970-01-01 12:23:59.323000', '01:01:29');
insert into time01 values ('1997-01-13', null, '1989-01-01 23:23:59.100000', '23:23:59');
insert into time01 (col1, col2, col3, col4) values ('2030-12-31', '2031-09-09 01:01:01', '2013-12-12 10:10:10.125000', '10:12:12');
select * from time01;
alter table time01 modify col1 int, modify col2 int first, modify col3 int after col1, modify col4 int;
show create table time01;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'time01' and COLUMN_NAME not like '__mo%';
drop table time01;


-- cast date, datetime, timestamp, time to decimal
drop table if exists time02;
create table time02 (col2 datetime, col3 timestamp, col4 time);
insert into time02 values ('2000-10-10 12:12:12', '1970-01-01 12:23:59.323000', '01:01:29');
insert into time02 values ( null, '1889-01-01 23:23:59.125000', '23:23:59');
insert into time02 (col2, col3, col4) values ('2031-09-09 01:01:01', '2013-12-12 10:10:10.125000', '10:12:12');
select * from time02;
alter table time02 modify col2 decimal(20,10) first, modify col3 decimal after col2, modify col4 decimal(38,0);
show create table time02;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'time02' and COLUMN_NAME not like '__mo%';
drop table time02;


-- prepare
drop table if exists prepare01;
create table prepare01(col1 int, col2 char);
insert into prepare01 values (1,'a'),(2,'b'),(3,'c');
prepare s1 from 'alter table prepare01 modify col1 decimal primary key';
execute s1;
show create table prepare01;
prepare s2 from 'alter table prepare01 modify col2 varchar(100) not null';
execute s2;
show create table prepare01;
drop table prepare01;


-- permission
drop role if exists role_r1;
drop user if exists role_u1;
drop table if exists test01;
create role role_r1;
create user role_u1 identified by '111' default role role_r1;
create table test01(col1 int);
insert into test01 values(1);
insert into test01 values(2);
grant create database on account * to role_r1;
grant show databases on account * to role_r1;
grant connect on account * to role_r1;
grant select on table * to role_r1;
grant show tables on database * to role_r1;

-- @session:id=2&user=sys:role_u1:role_r1&password=111
use alter_table_modify_column;
alter table test01 modify col1 int primary key;
-- @session
grant alter table on database * to role_r1;

-- @session:id=2&user=sys:role_u1:role_r1&password=111
use alter_table_modify_column;
alter table test01 modify col1 int primary key;
show create table test01;
-- @session
show create table test01;
drop table test01;
drop role role_r1;
drop user role_u1;


--mixed situation :add/drop column and modify column
drop table if exists mix01;
create table mix01 (col1 int not null , col2 decimal, col3 date, col4 varchar(100));
insert into mix01 values (1, 23849234.324, '2100-01-01', 'qy83uhfbh234y78y&*%^&%$$$E%^&Y*UIJNHBGVFTY^&Y*UJHBGVTF^&*U(OK');
insert into mix01 values (2, 773892.32748000000000, '1997-01-13', '38782yhbf3uhy4iendb32gefdc7y834uh2neujdr2h4f3v43');
insert into mix01 values (3, -82913942.3434, null, null);
select * from mix01;
alter table mix01 add column col1_2 binary after col1;
show create table mix01;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'mix01' and COLUMN_NAME not like '__mo%';
alter table mix01 modify column col1_2 varbinary(10) first;
show create table mix01;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'mix01' and COLUMN_NAME not like '__mo%';
truncate table mix01;
alter table mix01 add column col5 int;
show create table mix01;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'mix01' and COLUMN_NAME not like '__mo%';
drop table mix01;

drop database test;