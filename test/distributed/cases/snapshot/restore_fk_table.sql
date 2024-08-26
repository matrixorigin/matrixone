drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';
drop account if exists acc02;
create account acc02 admin_name = 'test_account' identified by '111';
drop account if exists acc03;
create account acc03 admin_name = 'test_account' identified by '111';

-- the primary table does not exist during the recovery, skip recover the secondary table
-- create snapshot for sys account
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
insert into aff01 values (7788,'SCOTT','ANALYST',7566,'0087-07-13',3000,NULL,20);
insert into aff01 values (7839,'KING','PRESIDENT',NULL,'1981-11-17',5000,NULL,10);
insert into aff01 values (7844,'TURNER','SALESMAN',7698,'1981-09-08',1500,0,30);
insert into aff01 values (7876,'ADAMS','CLERK',7788,'0087-07-13',1100,NULL,20);
insert into aff01 values (7900,'JAMES','CLERK',7698,'1981-12-03',950,NULL,30);
insert into aff01 values (7902,'FORD','ANALYST',7566,'1981-12-03',3000,NULL,20);
insert into aff01 values (7934,'MILLER','CLERK',7782,'1982-01-23',1300,NULL,10);

drop database if exists test02;
create database test02;
use test02;
drop table if exists table01;
create table table01(col1 int primary key , col2 decimal, col3 char, col4 varchar(20), col5 text, col6 double);
insert into table01 values (1, 2, 'a', '23eiojf', 'r23v324r23rer', 3923.324);
insert into table01 values (2, 3, 'b', '32r32r', 'database', 1111111);
create table table02 (col1 int unique key, col2 varchar(20));
insert into table02 (col1, col2) values (133, 'database');
create table table03(a INT primary key AUTO_INCREMENT, b INT, c INT);
create table table04(a INT primary key AUTO_INCREMENT, b INT, c INT);
insert into table03 values (1,1,1), (2,2,2);
insert into table04 values (0,1,2), (2,3,4);

drop snapshot if exists sp01;
create snapshot sp01 for account sys;

use test01;
drop table aff01;
drop table pri01;

restore account sys database test01 table aff01 from snapshot sp01;

show databases;
use test01;
show tables;

restore account sys database test01 table pri01 from snapshot sp01;
show tables;
select * from pri01;

restore account sys database test01 table aff01 from snapshot sp01;
show tables;
select * from aff01;
show create table pri01;
use test02;
select * from table01;
drop database test01;
drop database test02;
drop snapshot sp01;




-- the primary table does not exist during the recovery, skip recover the secondary table
-- create snapshot for nonsys account
-- @session:id=1&user=acc01:test_account&password=111
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
insert into aff01 values (7788,'SCOTT','ANALYST',7566,'0087-07-13',3000,NULL,20);
insert into aff01 values (7839,'KING','PRESIDENT',NULL,'1981-11-17',5000,NULL,10);
insert into aff01 values (7844,'TURNER','SALESMAN',7698,'1981-09-08',1500,0,30);
insert into aff01 values (7876,'ADAMS','CLERK',7788,'0087-07-13',1100,NULL,20);
insert into aff01 values (7900,'JAMES','CLERK',7698,'1981-12-03',950,NULL,30);
insert into aff01 values (7902,'FORD','ANALYST',7566,'1981-12-03',3000,NULL,20);
insert into aff01 values (7934,'MILLER','CLERK',7782,'1982-01-23',1300,NULL,10);

drop database if exists test02;
create database test02;
use test02;
drop table if exists table01;
create table table01(col1 int primary key , col2 decimal, col3 char, col4 varchar(20), col5 text, col6 double);
insert into table01 values (1, 2, 'a', '23eiojf', 'r23v324r23rer', 3923.324);
insert into table01 values (2, 3, 'b', '32r32r', 'database', 1111111);
create table table02 (col1 int unique key, col2 varchar(20));
insert into table02 (col1, col2) values (133, 'database');
create table table03(a INT primary key AUTO_INCREMENT, b INT, c INT);
create table table04(a INT primary key AUTO_INCREMENT, b INT, c INT);
insert into table03 values (1,1,1), (2,2,2);
insert into table04 values (0,1,2), (2,3,4);

drop snapshot if exists sp01;
create snapshot sp01 for account acc01;

use test01;
drop table aff01;
drop table pri01;

restore account acc01 database test01 table aff01 from snapshot sp01;

show databases;
use test01;
show tables;

restore account acc01 database test01 table pri01 from snapshot sp01;
show tables;
select * from pri01;

restore account acc01 database test01 table aff01 from snapshot sp01;
show tables;
select * from aff01;
show create table pri01;
use test02;
select * from table01;
drop database test01;
drop database test02;
-- @session




-- the primary table does not exist during the recovery, skip recover the secondary table
-- sys creates snapshot for nonsys account and restore to newnonsys
-- pri table and aff table are in different db
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

drop database if exists test02;
create database test02;
use test02;
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
                      constraint `c1` foreign key (deptno) references test01.pri01 (deptno)
);

insert into aff01 values (7369,'SMITH','CLERK',7902,'1980-12-17',800,NULL,20);
insert into aff01 values (7499,'ALLEN','SALESMAN',7698,'1981-02-20',1600,300,30);
insert into aff01 values (7521,'WARD','SALESMAN',7698,'1981-02-22',1250,500,30);
insert into aff01 values (7566,'JONES','MANAGER',7839,'1981-04-02',2975,NULL,20);
insert into aff01 values (7654,'MARTIN','SALESMAN',7698,'1981-09-28',1250,1400,30);
insert into aff01 values (7698,'BLAKE','MANAGER',7839,'1981-05-01',2850,NULL,30);
insert into aff01 values (7782,'CLARK','MANAGER',7839,'1981-06-09',2450,NULL,10);
insert into aff01 values (7788,'SCOTT','ANALYST',7566,'0087-07-13',3000,NULL,20);
insert into aff01 values (7839,'KING','PRESIDENT',NULL,'1981-11-17',5000,NULL,10);
insert into aff01 values (7844,'TURNER','SALESMAN',7698,'1981-09-08',1500,0,30);
insert into aff01 values (7876,'ADAMS','CLERK',7788,'0087-07-13',1100,NULL,20);
insert into aff01 values (7900,'JAMES','CLERK',7698,'1981-12-03',950,NULL,30);
insert into aff01 values (7902,'FORD','ANALYST',7566,'1981-12-03',3000,NULL,20);
insert into aff01 values (7934,'MILLER','CLERK',7782,'1982-01-23',1300,NULL,10);

drop snapshot if exists sp03;
create snapshot sp03 for account sys;

drop database test02;
drop database test01;

restore account sys database test02 table aff01 from snapshot sp03;

show databases;
use test02;
show tables;

drop snapshot sp03;
drop database test02;




-- restore pri table, if afflicated table exists, then skip to restore primary table
drop database if exists test04;
create database test04;
use test04;
drop table if exists f1;
drop table if exists c1;
create table f1(fa int primary key, fb int unique key);
create table c1 (ca int, cb int);
alter table c1 add constraint ffa foreign key f_a(ca) references f1(fa);
insert into f1 values (2,2);
insert into c1 values (2,3);
insert into c1 values (2,2);

drop snapshot if exists sp04;
create snapshot sp04 for account sys;

insert into f1 values (3,20);
insert into f1 values (4,600);

restore account sys database test04 table f1 from snapshot sp04;

use test04;
show tables;
select * from c1;
select * from f1;

drop snapshot sp04;
drop database test04;




-- restore pri table, if afflicated table exists, then skip to restore primary table
-- @session:id=1&user=acc01:test_account&password=111
drop database if exists test05;
create database test05;
use test05;
drop table if exists f1;
drop table if exists c1;
create table f1(fa int primary key, fb int unique key);
create table c1 (ca int, cb int);
alter table c1 add constraint ffa foreign key f_a(ca) references f1(fa);
insert into f1 values (2,2);
insert into c1 values (2,3);
insert into c1 values (2,2);

drop snapshot if exists sp05;
create snapshot sp05 for account acc01;

insert into f1 values (3,20);
insert into f1 values (4,600);

restore account acc01 database test05 table f1 from snapshot sp05;

use test04;
show tables;
select * from c1;
select * from f1;

drop snapshot sp05;
drop database test05;
-- @session




-- restore pri table, if afflicated table exists, then skip to restore primary table
-- pri table and aff table are in different db
drop database if exists test06;
create database test06;
use test06;
drop table if exists f1;
create table f1(fa int primary key, fb int unique key);
insert into f1 values (2,2);

drop database if exists test07;
create database test07;
use test07;
drop table if exists c1;
create table c1 (ca int, cb int);
alter table c1 add constraint ffa foreign key f_a(ca) references test06.f1(fa);
insert into c1 values (2,3);
insert into c1 values (2,2);

drop snapshot if exists sp06;
create snapshot sp06 for account acc01;

use test06;
insert into f1 values (3,20);
insert into f1 values (4,600);
use test07;
insert into c1 values (2,9);

restore account acc01 database test06 table f1 from snapshot sp06;

use test06;
show tables;
select * from f1;
use test07;
select * from c1;

drop snapshot sp06;
drop database test07;
drop database test06;





-- the recovered table contains foreign keys or is referenced, only recover this table and
-- the data of its associated table is not recovered
drop database if exists test07;
create database test07;
use test07;
drop table if exists foreign01;
create table foreign01(col1 int primary key auto_increment,
                       col2 varchar(20),
                       col3 int,
                       col4 bigint);
drop table if exists foreign02;
create table foreign02(col1 int,
                       col2 int,
                       col3 int primary key,
                       constraint `c1` foreign key(col1) references foreign01(col1));

insert into foreign01 values(1,'sfhuwe',1,1);
insert into foreign01 values(2,'37829901k3d',2,2);
insert into foreign02 values(1,1,1);
insert into foreign02 values(2,2,2);
select * from foreign01;
select * from foreign02;

drop snapshot if exists sp07;
create snapshot sp07 for account sys;

insert into foreign01 values(3, '323214321321', 32, 1);
insert into foreign02 values(3,2,10);

restore account sys database test07 table foreign02 from snapshot sp07;
select * from foreign01;
select * from foreign02;

drop snapshot sp07;
drop database test07;




-- the recovered table contains foreign keys or is referenced, only recover this table and
-- the data of its associated table is not recovered
-- @session:id=1&user=acc01:test_account&password=111
drop database if exists test08;
create database test08;
use test08;
drop table if exists foreign01;
create table foreign01(col1 int primary key auto_increment,
                       col2 varchar(20),
                       col3 int,
                       col4 bigint);
drop table if exists foreign02;
create table foreign02(col1 int,
                       col2 int,
                       col3 int primary key,
                       constraint `c1` foreign key(col1) references foreign01(col1));

insert into foreign01 values(1,'sfhuwe',1,1);
insert into foreign01 values(2,'37829901k3d',2,2);
insert into foreign02 values(1,1,1);
insert into foreign02 values(2,2,2);
select * from foreign01;
select * from foreign02;

drop snapshot if exists sp08;
create snapshot sp08 for account acc01;

insert into foreign01 values(3, '323214321321', 32, 1);
insert into foreign02 values(3,2,10);

restore account acc01 database test08 table foreign02 from snapshot sp08;
select * from foreign01;
select * from foreign02;

drop snapshot sp08;
drop database test08;
-- @session

drop account acc01;
drop account acc02;
drop account acc03;