drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';
drop account if exists acc02;
create account acc02 admin_name = 'test_account' identified by '111';

-- under account, single db, single table
-- @session:id=1&user=acc01:test_account&password=111
drop database if exists acc_test01;
create database acc_test01;
use acc_test01;
drop table if exists s3t;
create table s3t (col1 int, col2 int, col3 int, primary key(col1, col2));
insert into s3t select result, 2, 12 from generate_series(1, 30000, 1) g;
select count(*) from s3t;
select sum(col1) from s3t;
select avg(col1) from s3t;
select count(col3) from s3t where col1 > 1000;
alter table s3t add column col4 int after col2;
-- @session

drop snapshot if exists sp01;
create snapshot sp01 for account acc01;
-- @ignore:1
show snapshots;

-- @session:id=2&user=acc01:test_account&password=111
use acc_test01;
insert into s3t values (300001, 34, 23, 1);
select count(*) from s3t;
select * from s3t where col1 = 23;
-- @session

restore account acc01 from snapshot sp01 to account acc02;

-- @session:id=3&user=acc02:test_account&password=111
show databases;
use acc_test01;
show tables;
select count(*) from s3t;
select sum(col1) from s3t;
select avg(col1) from s3t;
select count(col3) from s3t where col1 > 1000;
-- @session

-- @session:id=4&user=acc01:test_account&password=111
use acc_test01;
alter table s3t drop column col4;
show create table s3t;
-- @session

drop snapshot if exists sp02;
create snapshot sp02 for account acc01;

restore account acc01 from snapshot sp02 to account acc02;

-- @session:id=5&user=acc02:test_account&password=111
use acc_test01;
show create table s3t;
-- @session

restore account acc01 from snapshot sp01 to account acc02;

-- @session:id=6&user=acc02:test_account&password=111
show databases;
use acc_test01;
show tables;
select count(*) from s3t;
select sum(col1) from s3t;
select avg(col1) from s3t;
select count(col3) from s3t where col1 > 1000;
show create table s3t;
drop database acc_test01;
-- @session

-- @session:id=7&user=acc01:test_account&password=111
drop database acc_test01;
-- @session

drop snapshot sp01;
drop snapshot sp02;




-- under account, multi db, multi table
-- @session:id=8&user=acc01:test_account&password=111
drop database if exists acc_test02;
create database acc_test02;
use acc_test02;
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
select count(*) from pri01;

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
select count(*) from aff01;

drop database if exists acc_test03;
create database acc_test03;
use acc_test03;
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
select count(*) from table01;
select count(*) from table02;
select count(*) from table03;
select count(*) from table04;

drop database if exists acc_test04;
create database acc_test04;
use acc_test04;
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


select count(*) from acc_test02.pri01;
select count(*) from acc_test02.aff01;
select * from acc_test02.pri01;
show create table acc_test02.pri01;
show create table acc_test02.aff01;
select count(*) from acc_test03.table01;
select count(*) from acc_test03.table02;
select count(*) from acc_test03.table03;
select count(*) from acc_test03.table04;
show create table acc_test03.table01;
show create table acc_test03.table02;
show create table acc_test03.table03;
show create table acc_test03.table04;
select count(*) from acc_test04.index03;
show create table acc_test04.index03;
-- @session

drop snapshot if exists sp04;
create snapshot sp04 for account acc01;

-- @session:id=9&user=acc01:test_account&password=111
insert into acc_test02.pri01 values (50,'ACCOUNTING','NEW YORK');
insert into acc_test02.aff01 values (9000,'MILLER','CLERK',7782,'1982-01-23',1300,NULL,50);
truncate table acc_test03.table01;
drop table acc_test03.table02;
delete from acc_test03.table03 where a = 1;
update acc_test03.table04 set a = 6000 where a = 1;
select count(*) from acc_test02.pri01;
select count(*) from acc_test02.aff01;
select * from acc_test03.table01;
select count(*) from acc_test03.table03;
select * from acc_test03.table04;
show create table acc_test04.index03;
-- @session

-- @bvt:issue#15971
restore account acc01 from snapshot sp04 to account acc02;

-- @session:id=10&user=acc01:test_account&password=111
show databases;
select count(*) from acc_test02.pri01;
select count(*) from acc_test02.aff01;
select count(*) from acc_test03.table01;
select count(*) from acc_test03.table02;
select count(*) from acc_test03.table03;
select count(*) from acc_test03.table04;
select count(*) from acc_test03.table04;
drop database acc_test03;
-- @session
-- @bvt:issue
drop snapshot sp04;



-- acc01 create sp01,sp02, restore sp02, restore sp01
-- @session:id=11&user=acc01:test_account&password=111
drop database if exists test01;
create database test01;
use test01;
drop table if exists table01;
create table table01(col1 int primary key , col2 decimal, col3 char, col4 varchar(20), col5 text, col6 double);
insert into table01 values (1, 2, 'a', '23eiojf', 'r23v324r23rer', 3923.324);
insert into table01 values (2, 3, 'b', '32r32r', 'database', 1111111);
drop table if exists table02;
create table table02 (col1 int unique key, col2 varchar(20));
insert into table02 (col1, col2) values (133, 'database');
select * from table01;
select * from table02;
-- @session

drop snapshot if exists sp07;
create snapshot sp07 for account acc01;

-- @session:id=12&user=acc01:test_account&password=111
use test01;
drop table table01;
insert into table02 values(134, 'database');
-- @session

drop snapshot if exists sp08;
create snapshot sp08 for account acc01;
-- @ignore:1
show snapshots;

-- @bvt:issue#15971
restore account acc01 from snapshot sp07 to account acc02;

-- @session:id=14&user=acc02:test_account&password=111
use test01;
show tables;
select * from table01;
select * from table02;
-- @session

restore account acc01 from snapshot sp08 to account acc02;

-- @session:id=15&user=acc02:test_account&password=111
use test01;
show tables;
select * from table01;
select * from table02;
show create table table02;
drop database test01;
-- @session

-- @session:id=16&user=acc01:test_account&password=111
drop database test01;
-- @session
-- @bvt:issue

drop snapshot sp07;
drop snapshot sp08;




-- sys create sp01,sp02, restore sp02, restore sp01
-- @session:id=17&user=acc01:test_account&password=111
drop database if exists test02;
create database test02;
use test02;
drop table if exists table01;
create table table01(col1 int primary key , col2 decimal, col3 char, col4 varchar(20), col5 text, col6 double);
insert into table01 values (1, 2, 'a', '23eiojf', 'r23v324r23rer', 3923.324);
insert into table01 values (2, 3, 'b', '32r32r', 'database', 1111111);
drop table if exists table02;
create table table02 (col1 int unique key, col2 varchar(20));
insert into table02 (col1, col2) values (133, 'database');
select * from table01;
select * from table02;
-- @session

drop snapshot if exists sp09;
create snapshot sp09 for account acc01;

-- @session:id=18&user=acc01:test_account&password=111
use test02;
drop table table01;
insert into table02 values(134, 'database');
alter table table02 add column new decimal after col2;
-- @session

drop snapshot if exists sp10;
create snapshot sp10 for account acc01;

-- @bvt:issue#15971
restore account acc01 from snapshot sp10 to account acc02;

-- @session:id=19&user=acc02:test_account&password=111
use test02;
show create table table01;
show create table table02;
select * from table02;
select * from table01;
-- @session

restore account acc01 from snapshot sp09 to account acc02;

-- @session:id=20&user=acc02:test_account&password=111
use test02;
show create table table01;
show create table table02;
select * from table02;
select * from table01;
drop database test02;
-- @session

-- @session:id=21&user=acc01:test_account&password=111
drop database test02;
-- @session
-- @bvt:issue

drop snapshot sp09;
drop snapshot sp10;




-- abnormal test: sys restore non-sys account:acc01 to sys
-- @session:id=20&user=acc01:test_account&password=111
drop database if exists test01;
create database test01;
use test01;

drop table if exists rs01;
create table rs01 (col1 int, col2 decimal(6), col3 varchar(30));
insert into rs01 values (1, null, 'database');
insert into rs01 values (2, 38291.32132, 'database');
insert into rs01 values (3, null, 'database management system');
insert into rs01 values (4, 10, null);
insert into rs01 values (1, -321.321, null);
insert into rs01 values (2, -1, null);
select count(*) from rs01;
-- @session

drop snapshot if exists sp03;
create snapshot sp03 for account acc01;

-- @session:id=21&user=acc01:test_account&password=111
use test01;
delete from rs01 where col1 = 4;
insert into rs01 values (10, -1, null);
select count(*) from rs01;
-- @session

restore account acc01 from snapshot sp03 to account sys;
drop snapshot sp03;

-- @session:id=22&user=acc01:test_account&password=111
drop database test01;
-- @session

drop account acc01;
drop account acc02;