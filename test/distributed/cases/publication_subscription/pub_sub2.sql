drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';
drop account if exists acc02;
create account acc02 admin_name = 'test_account' identified by '111';
drop account if exists acc03;
create account acc03 admin_name = 'test_account' identified by '111';

-- nonsys account publish db, do not specify table, verify a subscriber subscribes to all tables under db
-- @session:id=1&user=acc01:test_account&password=111
drop database if exists db01;
create database db01;
use db01;
drop table if exists table01;
create table table01 (col1 int unique key, col2 enum ('a','b','c'));
insert into table01 values(1,'a');
insert into table01 values(2, 'b');
drop table if exists table02;
create table table02 (col1 int primary key , col2 enum ('a','b','c'));
insert into table02 values(1,'a');
insert into table02 values(2, 'b');
drop table if exists table03;
create table table03(col1 int auto_increment, key key1(col1));
insert into table03 values (1);
insert into table03 values (2);

drop publication if exists pub01;
create publication pub01 database db01 account acc02 comment 'publish to acc02';
-- @ignore:5,6
show publications;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists sub01;
create database sub01 from acc01 publication pub01;
-- @ignore:5,7
show subscriptions all;
use sub01;
show tables;
select * from table01;
select count(*) from table02;
show create table table03;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
use db01;
drop table table01;
truncate table02;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
use sub01;
show tables;
select * from table02;
select * from table03;
drop database sub01;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
drop publication pub01;
drop database db01;
-- @session




-- nonsys account publish db and specify table, verify a subscriber subscribes to specified tables under db
-- @session:id=1&user=acc01:test_account&password=111
drop database if exists db02;
create database db02;
use db02;
drop table if exists t1;
create table t1 (a text);
insert into t1 values('abcdef'),('_bcdef'),('a_cdef'),('ab_def'),('abcd_f'),('abcde_');
drop table if exists t2;
create table t2 (a datetime(0) not null, primary key(a));
insert into t2 values ('20200101000000'), ('2022-01-02'), ('2022-01-02 00:00:01'), ('2022-01-02 00:00:01.512345');
drop table if exists t3;
create table t3(col1 datetime);
insert into t3 values('2020-01-13 12:20:59.1234586153121');
insert into t3 values('2023-04-17 01:01:45');
insert into t3 values(NULL);
drop table if exists t4;
create table t4(a int unique key, b int, c int, unique key(b,c));
insert into t4 values(1,1,1);
insert into t4 values(2,3,3);
insert into t4 values(10,19,11);
drop publication if exists pub02;
create publication pub02 database db02 table t1,t3 account acc02 comment 'publish t1、t3 to acc02';
-- @ignore:5,6
show publications;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists sub02;
create database sub02 from acc01 publication pub02;
-- @ignore:5,7
show subscriptions;
show databases;
use sub02;
show tables;
select * from t1;
select count(*) from t3;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
use db02;
truncate t1;
insert into t3 values ('2023-04-17 01:01:05');
-- @session

-- @session:id=2&user=acc02:test_account&password=111
use sub02;
select * from t1;
select count(*) from t3;
select * from t3;
drop database sub02;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
drop publication pub02;
drop database db02;
-- @session




-- nonsys account publish all tables, verify subscriber subscribes table is *, delete some table, verify subscriber table is *
-- @session:id=1&user=acc01:test_account&password=111
drop database if exists db03;
create database db03;
use db03;
drop table if exists t1;
create table t1(a int not null primary key, b float, c double, d varchar(30),e decimal(20,10));
insert into t1 values(1, 3214321.213, -8392.3,'woshishei',123456789.12356);
insert into t1 values(2, 0, 38293.3332121,'12345@',-12.365);
insert into t1 values(3, -392.1, 8390232,'3***',0.456984166622488655);
drop table if exists t2;
create table t2 (
 col1 int, col2 varbinary(20) not null,
 index idx(col2)
);
insert into t2 values(1, '11111111101010101');
insert into t2 values(2, '10111111101010101');
insert into t2 values(1, '36217468721382183');
insert into t2 values(2, '22258445222388855');
insert into t2 values(2, '00000000000000000');
select * from t2;
drop table if exists t3;
create table t3(col1 tinyint unsigned, col2 binary(10) not null);
insert into t3 values(0, '2312432112');
insert into t3 values(20, '321313');
insert into t3 values(23, '2312432112');
insert into t3 values(255, '321313');
drop publication if exists pub03;
create publication pub03 database db03 account acc02,acc03 comment 'publish to acc02,acc03';
-- @ignore:5,6
show publications;
-- @session

-- @session:id=3&user=acc03:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists sub03;
create database sub03 from acc01 publication pub03;
-- @ignore:5,7
show subscriptions;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
use db03;
drop table t1;
drop table t2;
-- @ignore:5,6
show publications;
-- @session

-- @session:id=3&user=acc03:test_account&password=111
use sub03;
-- @ignore:5,7
show subscriptions;
select * from t3;
drop database sub03;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
drop publication pub03;
drop database db03;
-- @session




-- nonsys account publish some tables, verify subscriber subscribes table shows valid tables, delete some table,
-- verify subscriber table shows valid tables
-- @session:id=1&user=acc01:test_account&password=111
drop database if exists db04;
create database db04;
use db04;
drop table if exists t1;
create table t1 (a int, b bit(10));
insert into t1 values (0, false);
insert into t1 values (1, true);
insert into t1 values (2, 0x2);
insert into t1 values (3, 0b11);
insert into t1 values (4, x'04');
insert into t1 values (5, b'101');
insert into t1 values (6, 'a');
drop table if exists t2;
create table t2(id int,fl float, dl double);
insert into t2 values(1,123456,123456);
insert into t2 values(2,123.456,123.456);
insert into t2 values(3,1.234567,1.234567);
insert into t2 values(4,1.234567891,1.234567891);
insert into t2 values(5,1.2345678912345678912,1.2345678912345678912);
drop table if exists t3;
create table t3 (col1 enum('red','blue','green'));
insert into t3 values ('red'),('blue'),('green');
insert into t3 values (null);
drop publication if exists pub04;
create publication pub04 database db04 table t1,t2 account acc02 comment 'publish to acc02';
-- @ignore:5,6
show publications;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists sub04;
create database sub04 from acc01 publication pub04;
-- @ignore:5,7
show subscriptions;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
use db04;
show tables;
drop table t1;
drop table t2;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
-- @ignore:5,7
show subscriptions all;
show databases;
use sub04;
show tables;
drop database sub04;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
drop publication pub04;
drop database db04;
-- @session




-- nonsys account publish db to specified account, any account subscribe, then nonsys delete publication
-- @session:id=1&user=acc01:test_account&password=111
drop database if exists db05;
create database db05;
use db05;
drop table if exists t1;
create table t1(a int not null primary key, b float, c double, d varchar(30),e decimal(20,10));
insert into t1 values(1, 3214321.213, -8392.3,'woshishei',123456789.12356);
insert into t1 values(2, 0, 38293.3332121,'12345@',-12.365);
insert into t1 values(3, -392.1, 8390232,'3***',0.456984166622488655);
drop table if exists t2;
create table t2 (
 col1 int, col2 varbinary(20) not null,
 index idx(col2)
);
insert into t2 values(1, '11111111101010101');
insert into t2 values(2, '10111111101010101');
insert into t2 values(1, '36217468721382183');
insert into t2 values(2, '22258445222388855');
insert into t2 values(2, '00000000000000000');
select * from t2;
drop table if exists t3;
create table t3(col1 tinyint unsigned, col2 binary(10) not null);
insert into t3 values(0, '2312432112');
insert into t3 values(20, '321313');
insert into t3 values(23, '2312432112');
insert into t3 values(255, '321313');
drop publication if exists pub05;
drop publication if exists pub06;
create publication pub05 database db05 table t2, t3 account acc03, acc02 comment '发布给acc02和acc03';
create publication pub06 database db05 account acc03 comment 'publish all table to acc03';
-- @ignore:5,6
show publications;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists sub05;
create database sub05 from acc01 publication pub05;
-- @ignore:5,7
show subscriptions;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
drop publication pub05;
-- @ignore:5,6
show publications;
-- @session

-- @session:id=3&user=acc03:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists sub06;
create database sub06 from acc01 publication pub06;
-- @ignore:5,7
show subscriptions;
show databases;
use sub06;
select * from t1;
select count(*) from t2;
show create table t3;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
drop publication pub06;
-- @ignore:5,6
show publications;
-- @session

-- @session:id=3&user=acc03:test_account&password=111
-- @ignore:5,7
show subscriptions;
use sub06;
drop database sub06;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
drop database db05;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
drop database sub05;
-- @session




-- nonsys account publish db to some nonsys, any nonsys subscribe, delete publication, the publisher creates a new
-- publication with the same name, then show subscriptions: status is 0
-- @session:id=1&user=acc01:test_account&password=111
drop database if exists db06;
create database db06;
use db06;
drop table if exists t1;
create table t1 (a int, b bit(10));
insert into t1 values (0, false);
insert into t1 values (1, true);
insert into t1 values (2, 0x2);
insert into t1 values (3, 0b11);
insert into t1 values (4, x'04');
insert into t1 values (5, b'101');
insert into t1 values (6, 'a');
drop table if exists t2;
create table t2(id int,fl float, dl double);
insert into t2 values(1,123456,123456);
insert into t2 values(2,123.456,123.456);
insert into t2 values(3,1.234567,1.234567);
insert into t2 values(4,1.234567891,1.234567891);
insert into t2 values(5,1.2345678912345678912,1.2345678912345678912);
drop table if exists t3;
create table t3 (col1 enum('red','blue','green'));
insert into t3 values ('red'),('blue'),('green');
insert into t3 values (null);
drop publication if exists pub06;
drop publication if exists pub07;
create publication pub06 database db06 account acc02,acc03 comment 'publish to acc02,acc03';
create publication pub07 database db06 table t2, t3 account acc03 comment '发布给acc02';
-- @ignore:5,6
show publications;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists sub06;
create database sub06 from acc01 publication pub06;
-- @ignore:5,7
show subscriptions;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
drop publication pub06;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
-- @ignore:5,7
show subscriptions;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
create publication pub06 database db06 account acc02 comment 'publish to acc02';
-- @session

-- @session:id=2&user=acc02:test_account&password=111
-- @ignore:5,7
show subscriptions;
use sub06;
show tables;
select * from t1;
select count(*) from t2;
show create table t3;
drop database sub06;
-- @session

-- @session:id=3&user=acc03:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists sub07;
create database sub07 from acc01 publication pub07;
-- @ignore:5,7
show subscriptions;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
drop publication pub07;
-- @session

-- @session:id=3&user=acc03:test_account&password=111
-- @ignore:5,7
show subscriptions;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
create publication pub07 database db06 table t1 account acc03;
-- @session

-- @session:id=3&user=acc03:test_account&password=111
-- @ignore:5,7
show subscriptions;
use sub07;
show tables;
select * from t1;
drop database sub07;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
drop publication pub06;
drop publication pub07;
drop database db06;
-- @session





-- nonsys account publish db to account a, a subscribe, nonsys account delete publication, the publisher creates a new
-- publication with the same name and publish to account b, then show subscriptions: status is 1
-- @session:id=1&user=acc01:test_account&password=111
drop database if exists db08;
create database db08;
use db08;
drop table if exists employees;
create table employees (
      emp_no      int             NOT NULL,
      birth_date  date            NOT NULL,
      first_name  varchar(14)     NOT NULL,
      last_name   varchar(16)     NOT NULL,
      gender      varchar(5)      NOT NULL,
      hire_date   date            NOT NULL,
      primary key (emp_no)
) partition by range columns (emp_no)(
    partition p01 values less than (100001),
    partition p02 values less than (200001),
    partition p03 values less than (300001),
    partition p04 values less than (400001)
);

insert into employees values (9001,'1980-12-17', 'SMITH', 'CLERK', 'F', '2008-12-17'),
                          (9002,'1981-02-20', 'ALLEN', 'SALESMAN', 'F', '2008-02-20'),
                          (9003,'1981-02-22', 'WARD', 'SALESMAN', 'M', '2005-02-22'),
                          (9004,'1981-04-02', 'JONES', 'MANAGER', 'M', '2003-04-02'),
                          (9005,'1981-09-28', 'MARTIN', 'SALESMAN', 'F','2003-09-28'),
                          (9006,'1981-05-01', 'BLAKE', 'MANAGER', 'M', '2003-05-01'),
                          (9007,'1981-06-09', 'CLARK', 'MANAGER', 'F', '2005-06-09');

drop table if exists pri01;
create table pri01(
                      deptno int unsigned comment '部门编号',
                      dname varchar(15) comment '部门名称',
                      loc varchar(50)  comment '部门所在位置',
                      primary key(deptno)
) comment='部门表';

insert into pri01 values (10,'ACCOUNTING','NEW YORK');
insert into pri01 values (20,'RESEARCH','DALLAS');
insert into pri01 values (30,'SALES','CHICAGO');
insert into pri01 values (40,'OPERATIONS','BOSTON');

drop table if exists aff01;
create table aff01(
                      empno int unsigned auto_increment COMMENT '雇员编号',
                      ename varchar(15) comment '雇员姓名',
                      job varchar(10) comment '雇员职位',
                      mgr int unsigned comment '雇员对应的领导的编号',
                      hiredate date comment '雇员的雇佣日期',
                      sal decimal(7,2) comment '雇员的基本工资',
                      comm decimal(7,2) comment '奖金',
                      deptno int unsigned comment '所在部门',
                      primary key(empno),
                      constraint `c1` foreign key (deptno) references pri01 (deptno)
);

insert into aff01 values (7369,'SMITH','CLERK',7902,'1980-12-17',800,NULL,20);
insert into aff01 values (7499,'ALLEN','SALESMAN',7698,'1981-02-20',1600,300,30);
insert into aff01 values (7521,'WARD','SALESMAN',7698,'1981-02-22',1250,500,30);
insert into aff01 values (7566,'JONES','MANAGER',7839,'1981-04-02',2975,NULL,20);
insert into aff01 values (7654,'MARTIN','SALESMAN',7698,'1981-09-28',1250,1400,30);
insert into aff01 values (7698,'BLAKE','MANAGER',7839,'1981-05-01',2850,NULL,30);
insert into aff01 values (7782,'CLARK','MANAGER',7839,'1981-06-09',2450,NULL,10);
drop publication if exists pub08;
drop publication if exists pub09;
create publication pub08 database db08 account acc02 comment 'publish to acc02';
create publication pub09 database db08 table pri01, aff01 account acc03 comment '发布给acc02';
-- @ignore:5,6
show publications;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
drop database if exists sub08;
create database sub08 from acc01 publication pub08;
-- @ignore:5,7
show subscriptions;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
drop publication pub08;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
-- @ignore:5,7
show subscriptions;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
create publication pub08 database db08 account acc03 comment 'publish to acc03';
-- @session

-- @session:id=2&user=acc02:test_account&password=111
-- @ignore:5,7
show subscriptions;
use sub08;
drop database sub08;
-- @session

-- @session:id=3&user=acc03:test_account&password=111
drop database if exists sub09;
create database sub09 from acc01 publication pub09;
-- @ignore:5,7
show subscriptions;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
drop publication pub09;
-- @session

-- @session:id=3&user=acc03:test_account&password=111
-- @ignore:5,7
show subscriptions;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
create publication pub09 database db08 account acc02 comment 'publish to acc02';
-- @ignore:5,6
show publications;
-- @session

-- @session:id=3&user=acc03:test_account&password=111
-- @ignore:5,7
show subscriptions;
use sub09;
drop database sub09;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
drop publication pub08;
drop publication pub09;
drop database db08;
-- @session




-- @bvt:issue#18063
-- sys account publish db/table to a, b has no permissions to subscribe, sys account modify pub target from a to b, b can subscribe
-- @session:id=1&user=acc01:test_account&password=111
drop database if exists db09;
create database db09;
use db09;
drop table if exists employees;
create table employees (
      emp_no      int             NOT NULL,
      birth_date  date            NOT NULL,
      first_name  varchar(14)     NOT NULL,
      last_name   varchar(16)     NOT NULL,
      gender      varchar(5)      NOT NULL,
      hire_date   date            NOT NULL,
      primary key (emp_no)
) partition by range columns (emp_no)(
    partition p01 values less than (100001),
    partition p02 values less than (200001),
    partition p03 values less than (300001),
    partition p04 values less than (400001)
);

insert into employees values (9001,'1980-12-17', 'SMITH', 'CLERK', 'F', '2008-12-17'),
                          (9002,'1981-02-20', 'ALLEN', 'SALESMAN', 'F', '2008-02-20'),
                          (9003,'1981-02-22', 'WARD', 'SALESMAN', 'M', '2005-02-22'),
                          (9004,'1981-04-02', 'JONES', 'MANAGER', 'M', '2003-04-02'),
                          (9005,'1981-09-28', 'MARTIN', 'SALESMAN', 'F','2003-09-28'),
                          (9006,'1981-05-01', 'BLAKE', 'MANAGER', 'M', '2003-05-01'),
                          (9007,'1981-06-09', 'CLARK', 'MANAGER', 'F', '2005-06-09');

drop table if exists pri01;
create table pri01(
                      deptno int unsigned comment '部门编号',
                      dname varchar(15) comment '部门名称',
                      loc varchar(50)  comment '部门所在位置',
                      primary key(deptno)
) comment='部门表';

insert into pri01 values (10,'ACCOUNTING','NEW YORK');
insert into pri01 values (20,'RESEARCH','DALLAS');
insert into pri01 values (30,'SALES','CHICAGO');
insert into pri01 values (40,'OPERATIONS','BOSTON');

drop publication if exists pub10;
create publication pub10 database db09 account acc02 comment 'publish to all account';
-- @ignore:5,6
show publications;
-- @session

-- @session:id=3&user=acc03:test_account&password=111
-- @ignore:5,7
show subscriptions all;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
alter publication pub10 account acc03 database db09;
-- @ignore:5,6
show publications;
-- @session

-- @session:id=3&user=acc03:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists sub10;
create database sub10 from acc01 publication pub10;
-- @ignore:5,7
show subscriptions;
use sub10;
show tables;
select * from employees;
select * from pri01;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
drop publication pub10;
drop database db09;
-- @session
-- @bvt:issue

drop account acc01;
drop account acc02;
drop account acc03;