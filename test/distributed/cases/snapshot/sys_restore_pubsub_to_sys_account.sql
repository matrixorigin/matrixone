drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';

-- create snapshot, then sys publishes to non-sys account and the database has not been subscribed
drop database if exists republication01;
create database republication01;
use republication01;
create table repub01(col1 int);
insert into repub01 values (1);
insert into repub01 values (2);
insert into repub01 (col1) values (3);

drop snapshot if exists sp01;
create snapshot sp01 for account sys;
-- @ignore:1
drop publication if exists publication01;
create publication publication01 database republication01 account acc01 comment 'publish before creating snapshot';
-- @ignore:2
show publications;
-- @ignore:2,6
select * from mo_catalog.mo_pubs;
drop publication publication01;
drop database republication01;
restore account sys from snapshot sp01;

-- @ignore:2,6
select * from mo_catalog.mo_pubs;
show databases;
use republication01;
select * from repub01;
-- @ignore:2
show publications;

drop snapshot sp01;
drop database republication01;





-- create snapshot, sys publishes to non-sys account and the database has been subscribed, then drop publication and restore
drop database if exists repub02;
create database repub02;
use repub02;
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

drop snapshot if exists sp02;
create snapshot sp02 for account sys;

drop publication if exists pub02;
create publication pub02 database repub02 account acc01 comment 'publish before creating snapshot';
-- @ignore:2
show publications;
-- @ignore:2,6
select * from mo_catalog.mo_pubs;

-- @session:id=1&user=acc01:test_account&password=111
drop database if exists sub01;
create database sub01 from sys publication pub02;
show databases;
use sub01;
show tables;
select * from pri01;
select * from aff01;
-- @session

restore account sys from snapshot sp02;
-- @ignore:2
show publications;
-- @ignore:2,6
select * from mo_catalog.mo_pubs;
show databases;
use repub02;
select * from pri01;
select * from aff01;

-- @session:id=1&user=acc01:test_account&password=111
use sub01;
show tables;
drop database sub01;
-- @session
drop database repub02;





-- sys publishes to non-sys account and the database has been subscribed, then drop publication and restore
drop database if exists repub03;
create database repub03;
use repub03;
drop table if exists rs02;
create table rs02 (col1 int, col2 datetime);
insert into rs02 values (1, '2020-10-13 10:10:10');
insert into rs02 values (2, null);
insert into rs02 values (1, '2021-10-10 00:00:00');
insert into rs02 values (2, '2023-01-01 12:12:12');
insert into rs02 values (2, null);
insert into rs02 values (3, null);
insert into rs02 values (4, '2023-11-27 01:02:03');
select * from rs02;
drop table if exists rs03;
create table rs03 (col1 int, col2 float, col3 decimal, col4 enum('1','2','3','4'));
insert into rs03 values (1, 12.21, 32324.32131, 1);
insert into rs03 values (2, null, null, 2);
insert into rs03 values (2, -12.1, 34738, null);
insert into rs03 values (1, 90.2314, null, 4);
insert into rs03 values (1, 43425.4325, -7483.432, 2);

drop publication if exists pub03;
create publication pub03 database repub03 account acc01 comment 'create repub03';
-- @ignore:2
show publications;

-- @session:id=1&user=acc01:test_account&password=111
drop snapshot if exists sp01;
create snapshot sp01 for account acc01;
drop database if exists sub03;
create database sub03 from sys publication pub03;
-- @ignore:3,5
show subscriptions;
show databases;
use sub03;
show tables;
select * from rs02;
select * from rs03;
restore account acc01 from snapshot sp01;
-- @ignore:3
show subscriptions;
show databases;
drop snapshot sp01;
-- @session
-- @ignore:2,6
select * from mo_catalog.mo_pubs;
show databases;
-- @ignore:2
show publications;
drop publication pub03;
drop database repub03;





-- sys create db and publish, non-sys subscribe, sys create snapshot for non-sys account
drop database if exists db01;
create database db01;
use db01;
drop table if exists table01;
create table table01(col1 int primary key , col2 decimal, col3 char, col4 varchar(20), col5 text, col6 double);
insert into table01 values (1, 2, 'a', '23eiojf', 'r23v324r23rer', 3923.324);
insert into table01 values (2, 3, 'b', '32r32r', 'database', 1111111);
drop table if exists table02;
create table table02 (col1 int unique key, col2 varchar(20));
insert into table02 (col1, col2) values (133, 'database');

drop snapshot if exists sp02;
create snapshot sp02 for account acc01;

drop publication if exists pub03;
create publication pub04 database db01 account acc01 comment 'create pub04';
-- @ignore:2,6
select * from mo_catalog.mo_pubs;
-- @ignore:2
show publications;

-- @session:id=1&user=acc01:test_account&password=111
drop database if exists sub04;
create database sub04 from sys publication pub04;
use sub04;
show create table table01;
select * from table02;
-- @session

restore account acc01 from snapshot sp02;

-- @session:id=1&user=acc01:test_account&password=111
show databases;
use sub04;
-- @session
-- @ignore:2
show publications;
drop publication pub04;
drop database db01;
drop snapshot sp02;





drop account if exists acc02;
create account acc02 admin_name = 'test_account' identified by '111';
-- sys create snapshot for acc01 pub
-- @session:id=1&user=acc01:test_account&password=111
drop database if exists db09;
create database db09;
use db09;
drop table if exists index01;
create table index01(
                        col1 int not null,
                        col2 date not null,
                        col3 varchar(16) not null,
                        col4 int unsigned not null,
                        primary key (col1)
);
insert into index01 values(1, '1980-12-17','Abby', 21);
insert into index01 values(2, '1981-02-20','Bob', 22);
insert into index01 values(3, '1981-02-20','Bob', 22);
select count(*) from index01;

drop table if exists index02;
create table index02(col1 char, col2 int, col3 binary);
insert into index02 values('a', 33, 1);
insert into index02 values('c', 231, 0);
alter table index02 add key pk(col1) comment 'primary key';
select count(*) from index02;

drop database if exists db10;
create database db10;
use db10;
drop table if exists index03;
create table index03 (
                         emp_no      int             not null,
                         birth_date  date            not null,
                         first_name  varchar(14)     not null,
                         last_name   varchar(16)     not null,
                         gender      varchar(5)      not null,
                         hire_date   date            not null,
                         primary key (emp_no)
) partition by range columns (emp_no)(
    partition p01 values less than (100001),
    partition p02 values less than (200001),
    partition p03 values less than (300001),
    partition p04 values less than (400001)
);

insert into index03 values (9001,'1980-12-17', 'SMITH', 'CLERK', 'F', '2008-12-17'),
                           (9002,'1981-02-20', 'ALLEN', 'SALESMAN', 'F', '2008-02-20');
-- @session

drop snapshot if exists sp05;
create snapshot sp05 for account acc01;

-- @session:id=1&user=acc01:test_account&password=111
drop publication if exists pub05;
create publication pub05 database db09 account acc02 comment 'publish db09';
drop publication if exists pub06;
create publication pub06 database db10 account acc02 comment 'publish db10';
-- @ignore:2
show publications;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
drop database if exists sub05;
create database sub05 from acc01 publication pub05;
show databases;
use sub05;
show create table index01;
select * from index02;
-- @session

restore account acc01 from snapshot sp05;

-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:2
show publications;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
-- @ignore:3
show subscriptions;
show databases;
use sub05;
drop database sub05;
-- @session
drop snapshot sp05;
-- @session:id=1&user=acc01:test_account&password=111
drop database db09;
drop database db10;





-- non-sys account create db, create snapshot, create publication, non-sys create subscription, acc01 restore
-- @session:id=1&user=acc01:test_account&password=111
drop database if exists db10;
create database db10;
use db10;
drop table if exists ti1;
drop table if exists tm1;
drop table if exists ti2;
drop table if exists tm2;

create  table ti1(a INT not null, b INT, c INT);
create  table tm1(a INT not null, b INT, c INT);
create  table ti2(a INT primary key AUTO_INCREMENT, b INT, c INT);
create  table tm2(a INT primary key AUTO_INCREMENT, b INT, c INT);
show create table ti1;
show create table tm1;
show create table ti2;
show create table tm2;
drop snapshot if exists sp11;
create snapshot sp11 for account acc01;

drop publication if exists pub06;
create publication pub06 database db10 account acc02 comment 'publish db10';
-- @ignore:2,6
select * from mo_catalog.mo_pubs;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
drop database if exists sub06;
create database sub06 from acc01 publication pub06;
use sub06;
show tables;
select * from ti1;
select * from tm1;
select * from ti2;
select * from tm2;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
restore account acc01 from snapshot sp11;
-- @ignore:2
show publications;
show databases;
drop database db10;
-- @ignore:2,6
select * from mo_catalog.mo_pubs;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
show databases;
use sub06;
drop database sub06;
-- @session
drop snapshot sp11;





-- sys create db, create snapshot1, create publication to acc01, create snapshot2,
-- acc01 subscribe, restore to snapshot1, restore snapshot2
drop database if exists test01;
create database test01;
use test01;
drop table if exists test01;
create table test01 (a int);
insert into test01 (a) values (1), (2), (3), (4), (5),(6), (7), (8), (9), (10), (11), (12),(13), (14), (15), (16), (17), (18), (19), (20),(21), (22), (23), (24), (25), (26), (27), (28), (29), (30),(31), (32), (33), (34), (35), (36), (37), (38), (39), (40),(41), (42), (43), (44), (45), (46), (47), (48), (49), (50),(51), (52), (53), (54), (55), (56), (57), (58), (59), (60),(61), (62), (63), (64), (65), (66), (67), (68), (69), (70),(71), (72), (73), (74), (75), (76), (77), (78), (79), (80), (81), (82), (83), (84), (85), (86), (87), (88), (89), (90),(91), (92), (93), (94), (95), (96), (97), (98), (99), (100);

drop snapshot if exists sp100;
create snapshot sp100 for account sys;

drop publication if exists pub07;
create publication pub07 database test01 account acc01 comment 'publish test01';
-- @ignore:2,6
select * from mo_catalog.mo_pubs;

drop snapshot if exists sp101;
create snapshot sp101 for account sys;

-- @session:id=1&user=acc01:test_account&password=111
drop database if exists sub07;
create database sub07 from sys publication pub07;
use sub07;
select sum(a) from test01;
-- @session

restore account sys from snapshot sp100;

-- @session:id=1&user=acc01:test_account&password=111
show databases;
use sub07;
-- @session

restore account sys from snapshot sp101;
-- @ignore:2,6
select * from mo_catalog.mo_pubs;

-- @session:id=1&user=acc01:test_account&password=111
show databases;
use sub07;
select sum(a) from test01;
drop database sub07;
-- @session
drop publication pub07;
drop database test01;
drop snapshot sp100;
drop snapshot sp101;





-- acc01 create db, sys create snapshot1, acc01 create publication to acc02, sys create snapshot2
-- acc01 subscribe, sys restore to snapshot1, query, sys restore to snapshot 2
-- @session:id=1&user=acc01:test_account&password=111
drop database if exists test02;
create database test02;
use test02;
create table t1(col1 int, col2 decimal key);
insert into t1 values(1,2);
insert into t1 values(2,3);
insert into t1 values(3,4);
-- @session

drop snapshot if exists sp102;
create snapshot sp102 for account acc01;

-- @session:id=1&user=acc01:test_account&password=111
drop publication if exists pub08;
create publication pub08 database test02 account acc02 comment 'publish test02';
-- @session

drop snapshot if exists sp103;
create snapshot sp103 for account acc01;

-- @session:id=2&user=acc02:test_account&password=111
drop database if exists sub07;
create database sub07 from acc01 publication pub08;
show databases;
use sub07;
select * from t1;
-- @session

restore account acc01 from snapshot sp102;

-- @session:id=2&user=acc02:test_account&password=111
show databases;
use sub07;
select * from t1;
-- @session

restore account acc01 from snapshot sp103;

-- @session:id=2&user=acc02:test_account&password=111
show databases;
use sub07;
select * from t1;
drop database sub07;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
drop publication pub08;
drop database test02;
-- @session
drop snapshot sp102;
drop snapshot sp103;





-- restore to new account
drop account if exists acc03;
create account acc03 admin_name = 'test_account' identified by '111';
-- @session:id=1&user=acc01:test_account&password=111
drop database if exists test03;
create database test03;
use test03;
drop table if exists t1;
create table t1(a int primary key,b int, constraint `c1` foreign key fk1(b) references t1(a));
show tables;
show create table t1;
insert into t1 values (1,1);
insert into t1 values (2,1);
insert into t1 values (3,2);
select * from t1;
-- @session

drop snapshot if exists sp104;
create snapshot sp104 for account acc01;

-- @session:id=1&user=acc01:test_account&password=111
drop publication if exists pub09;
create publication pub09 database test03 account acc02 comment 'publish test03';
-- @session

drop snapshot if exists sp104;
create snapshot sp104 for account acc01;

-- @session:id=2&user=acc02:test_account&password=111
drop database if exists sub08;
create database sub08 from acc01 publication pub09;
show databases;
use sub08;
select * from t1;
-- @session

restore account acc01 from snapshot sp104 to account acc03;

-- @session:id=3&user=acc03:test_account&password=111
show databases;
use test03;
-- @ignore:2
show publications;
drop publication pub09;
drop database test03;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
show databases;
use sub08;
select * from t1;
drop database sub08;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
drop publication pub09;
drop database test03;
-- @session
drop snapshot sp104;





-- restore to new account
drop account if exists acc03;
create account acc03 admin_name = 'test_account' identified by '111';
-- @session:id=1&user=acc01:test_account&password=111
drop database if exists test04;
create database test04;
use test04;
drop table if exists t1;
create table t1(a int primary key,b int, constraint `c1` foreign key fk1(b) references t1(a));
show tables;
show create table t1;
insert into t1 values (1,1);
insert into t1 values (2,1);
insert into t1 values (3,2);
select * from t1;
-- @session

drop snapshot if exists sp105;
create snapshot sp105 for account acc02;

-- @session:id=1&user=acc01:test_account&password=111
drop publication if exists pub10;
create publication pub10 database test04 account acc02 comment 'publish test03';
-- @ignore:2,6
select * from mo_catalog.mo_pubs;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
drop database if exists sub09;
create database sub09 from acc01 publication pub10;
show databases;
use sub09;
select * from t1;
-- @session

restore account acc02 from snapshot sp105 to account acc03;
-- @ignore:2,6
select * from mo_catalog.mo_pubs;

-- @session:id=4&user=acc03:test_account&password=111
show databases;
use sub09;
select * from t1;
-- @ignore:2
show publications;
drop database sub09;
-- @ignore:2,6
select * from mo_catalog.mo_pubs;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
show databases;
use sub09;
select * from t1;
drop database sub09;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
drop publication pub10;
drop database test04;
-- @session
drop snapshot sp105;

drop account acc01;
drop account acc02;
drop account acc03;