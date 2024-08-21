set global enable_privilege_cache = off;
drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';
drop account if exists acc02;
create account acc02 admin_name = 'test_account' identified by '111';
drop account if exists acc03;
create account acc03 admin_name = 'test_account' identified by '111';

-- restore a subscription db does not restore the data of the subscription data, only restore the
-- subscription relationship, because the original data is in the publishing account
drop database if exists test01;
create database test01;
use test01;
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

drop publication if exists pub01;
create publication pub01 database test01 account all;
-- @ignore:5,6
show publications;

-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists sub01;
create database sub01 from sys publication pub01;
drop snapshot if exists sp01;
create snapshot sp01 for account acc01;
-- @ignore:1
show snapshots;
use sub01;
show tables;
select * from aff01;
select count(*) from pri01;

drop database sub01;
-- @ignore:5,7
show subscriptions;
restore account acc01 from snapshot sp01;
-- @ignore:5,7
show subscriptions;
-- @session

show databases;
use test01;
drop table aff01;
-- @ignore:5,6
show publications;

-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:5,7
show subscriptions;
show databases;
use sub01;
show tables;
select * from aff01;
select count(*) from pri01;
drop database sub01;
drop snapshot sp01;
-- @session

drop publication pub01;
drop database test01;




-- restore a deleted subscribed db, and this subscribed db corresponds tp the publication db has been deleted,
-- do not need to restore the subscription db
drop database if exists test02;
create database test02;
use test02;
drop table if exists rs02;
create table rs02 (col1 int primary key , col2 datetime);
insert into rs02 values (1, '2020-10-13 10:10:10');
insert into rs02 values (2, null);
insert into rs02 values (3, '2021-10-10 00:00:00');
insert into rs02 values (4, '2023-01-01 12:12:12');
insert into rs02 values (5, null);
insert into rs02 values (6, null);
insert into rs02 values (7, '2023-11-27 01:02:03');
select * from rs02;
drop table if exists rs03;
create table rs03 (col1 int, col2 float, col3 decimal, col4 enum('1','2','3','4'));
insert into rs03 values (1, 12.21, 32324.32131, 1);
insert into rs03 values (2, null, null, 2);
insert into rs03 values (2, -12.1, 34738, null);
insert into rs03 values (1, 90.2314, null, 4);
insert into rs03 values (1, 43425.4325, -7483.432, 2);

drop publication if exists pub02;
create publication pub02 database test02 table rs02 account acc01;
-- @ignore:5,6
show publications;

-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists sub02;
create database sub02 from sys publication pub02;
-- @ignore:5,7
show subscriptions;

drop snapshot if exists sp02;
create snapshot sp02 for account acc01;
-- @ignore:1
show snapshots;
drop database sub02;
-- @session

drop publication pub02;

-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:5,7
show subscriptions;
restore account acc01 from snapshot sp02;
-- @ignore:5,7
show subscriptions;
show databases;
drop snapshot sp02;
-- @session

drop database test02;




-- restore data to new account, do not need to restore the subscription table
drop database if exists test03;
create database test03;
use test03;
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

drop publication if exists pub03;
create publication pub03 database test03 account acc01;
-- @ignore:5,6
show publications;

-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:5,7
show subscriptions;
drop database if exists sub03;
create database sub03 from sys publication pub03;
drop database if exists db01;
create database db01;
use db01;
drop table if exists table01;
create table table01(col1 varchar(50), col2 bigint);
insert into table01 values('database',23789324);
insert into table01 values('fhuwehwfw',3829032);
-- @session

drop snapshot if exists sp03;
create snapshot sp03 for account acc01;

restore account acc01 from snapshot sp03 to account acc02;

-- @session:id=2&user=acc02:test_account&password=111
show subscriptions;
show databases;
use db01;
show tables;
select * from table01;
drop table table01;
drop database db01;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
drop database sub03;
-- @session

drop publication pub03;
drop database test03;
drop database test04;
drop snapshot sp03;




-- restore level is db or account, can not restore a subscription table
-- @session:id=1&user=acc01:test_account&password=111
drop database if exists test04;
create database test04;
use test04;
drop table if exists t1;
create table t1 (a int primary key);
insert into t1 values (1);
drop table if exists t2;
create table t2 (a int primary key, b int, FOREIGN KEY (b) REFERENCES t2(a));
insert into t2 values (1, 1);
drop table if exists t3;
create table t3 (a int primary key, b int unique key, FOREIGN KEY (a) REFERENCES t1(a), FOREIGN KEY (b) REFERENCES t2(a));
insert into t3 values (1, 1);
drop table if exists t4;
create table t4 (a int primary key, b int, FOREIGN KEY (b) REFERENCES t3(b));
insert into t4 values (2, 1);
drop table if exists t5;
create table t5 (a int, FOREIGN KEY (a) REFERENCES t4(a));
insert into t5 values (2);
drop table if exists t6;
create table t6 (a int, FOREIGN KEY (a) REFERENCES t4(a));
insert into t6 values (2);

drop publication if exists pub04;
create publication pub04 database test04 account sys comment 'pub to sys account';
-- @ignore:5,6
show publications;
-- @session

drop database if exists sub05;
create database sub05 from acc01 publication pub04;

-- @ignore:5,7
show subscriptions;
drop snapshot if exists sp05;
create snapshot sp05 for account sys;

drop database sub05;
restore account sys database sub05 table t4 from snapshot sp05;
restore account sys database sub05 from snapshot sp05;
show databases;
use sub05;
show tables;
select * from t1;
select count(*) from t2;
-- @ignore:5,7
show subscriptions;
drop database sub05;
drop snapshot sp05;

-- @session:id=1&user=acc01:test_account&password=111
drop publication pub04;
drop database test04;
-- @session




-- acc01 pub to acc02, acc01 creates snapshot, acc02 subscribe, acc01 restore, test acc02 show subscriptions: status = 2
-- @session:id=1&user=acc01:test_account&password=111
drop database if exists test06;
create database test06;
use test06;
drop table if exists table01;
create table table01 (col1 int unique key auto_increment, col2 decimal(6), col3 varchar(30));
insert into table01 values (1, null, 'database');
insert into table01 values (2, 38291.32132, 'database');
insert into table01 values (3, null, 'database management system');
insert into table01 values (4, 10, null);
insert into table01 values (5, -321.321, null);
insert into table01 values (6, -1, null);
select count(*) from table01;

drop snapshot if exists sp06;
create snapshot sp06 for account acc01;

drop publication if exists pub06;
create publication pub06 database test06 account acc02 comment 'acc01 pub to acc02';
-- @ignore:5,6
show publications;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
drop database if exists sub06;
create database sub06 from acc01 publication pub06;
-- @ignore:5,7
show subscriptions;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
restore account acc01 from snapshot sp06;
-- @ignore:5,6
show publications;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
-- @ignore:5,7
show subscriptions;
use sub06;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
drop publication if exists pub06;
create publication pub06 database test06 table table01 account acc02 comment 'acc01 pub to acc02';
-- @ignore:5,6
show publications;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
show databases;
use sub06;
show tables;
select * from table01;
show create table table01;
drop database sub06;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
drop publication pub06;
drop database test06;
drop snapshot sp06;
-- @session




-- sys pub db to acc01, create snapshot for sys, alter pub to acc01 to acc02, restore
drop database if exists test07;
create database test07;
use test07;
create table t1 (a int primary key);
insert into t1 values (1);
select * from t1;

drop database if exists test08;
create database test08;
use test08;
create table t3 (a int primary key, b int, FOREIGN KEY (b) REFERENCES test07.t1(a));
insert into t3 values (1, 1);
select * from t3;
create table t4 (a int primary key);
insert into t4 values (1);
select * from t4;

use test07;
create table t2 (a int primary key, b int, FOREIGN KEY (b) REFERENCES test08.t4(a));
insert into t2 values (1, 1);
select * from t2;

drop publication if exists pub06;
create publication pub06 database test07 account acc01;

-- @session:id=1&user=acc01:test_account&password=111
drop database if exists sub06;
create database sub06 from sys publication pub06;
-- @ignore:5,7
show subscriptions;
-- @session

drop snapshot if exists sp06;
create snapshot sp06 for account sys;

alter publication pub06 account acc02 database test07;

-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:5,7
show subscriptions;
-- @session

restore account sys from snapshot sp06;

-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:5,7
show subscriptions;
drop database sub06;
-- @session

drop publication pub06;
drop table test07.t2;
drop table test08.t3;
drop database test07;
drop database test08;
drop snapshot sp06;

drop account acc01;
drop account acc02;
drop account acc03;