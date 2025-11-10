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
-- @session

drop snapshot if exists sp01;
create snapshot sp01 for cluster;
-- @ignore:1
show snapshots;

-- @session:id=1&user=acc01:test_account&password=111
use sub01;
show tables;
select * from aff01;
select count(*) from pri01;
drop database sub01;
-- @ignore:5,7
show subscriptions;
-- @session

restore account acc01{snapshot="sp01"};

-- @session:id=1&user=acc01:test_account&password=111
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
-- @session
drop snapshot sp01;
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
-- @session

drop snapshot if exists sp02;
create snapshot sp02 for cluster;

-- @ignore:1
show snapshots;

-- @session:id=1&user=acc01:test_account&password=111
drop database sub02;
-- @session

drop publication pub02;

-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:5,7
show subscriptions;
-- @session

restore account acc01{snapshot="sp02"};

-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:5,7
show subscriptions;
show databases;
-- @session

drop snapshot sp02;
drop database test02;




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
-- @session

drop snapshot if exists sp06;
create snapshot sp06 for cluster;

-- @session:id=1&user=acc01:test_account&password=111
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

restore account acc01{snapshot="sp06"};

-- @session:id=1&user=acc01:test_account&password=111
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
-- @session
drop snapshot sp06;




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
create snapshot sp06 for cluster;

alter publication pub06 account acc02 database test07;

-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:5,7
show subscriptions;
-- @session

restore account sys{snapshot="sp06"};

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