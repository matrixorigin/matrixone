drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';
drop account if exists acc02;
create account acc02 admin_name = 'test_account' identified by '111';
drop account if exists acc03;
create account acc03 admin_name = 'test_account' identified by '111';
drop account if exists acc04;
create account acc04 admin_name = 'test_account' identified by '111';

drop database if exists db01;
create database db01;
use db01;
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
                           (9002,'1981-02-20', 'ALLEN', 'SALESMAN', 'F', '2008-02-20'),
                           (9003,'1991-02-20', 'Bob', 'TEACHER', 'M', '2008-02-20'),
                           (9004,'1999-02-20', 'MARY', 'PROGRAMMER', 'M', '2008-02-20');
select * from index03;

drop database if exists db02;
create database db02;
use db02;
drop table if exists departments;
create table departments (
                             department_id INT primary key auto_increment,
                             department_name varchar(100)
);
show create table departments;

insert into departments (department_id, department_name)
values (1, 'HR'),(2, 'Engineering');

drop table if exists employees;
create table employees (
                           employee_id INT primary key,
                           first_name varchar(50),
                           last_name varchar(50),
                           department_id INT,
                           FOREIGN KEY (department_id) REFERENCES departments(department_id)
);

insert into employees values
                          (1, 'John', 'Doe', 1),
                          (2, 'Jane', 'Smith', 2),
                          (3, 'Bob', 'Johnson', 1);

drop view if exists employee_view;
create view employee_view as select employee_id, first_name, last_name, department_id from employees;

drop view if exists department_view;
create view department_view as select department_id, department_name from departments;

drop view if exists employee_with_department_view;
create view employee_with_department_view as
select e.employee_id, e.first_name, e.last_name, d.department_name
from employee_view e JOIN department_view d ON e.department_id = d.department_id;

select * from employee_view;
select * from department_view;
select * from employee_with_department_view;

drop database if exists db03;
create database db03;
use db03;
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

select count(*) from pri01;
select count(*) from aff01;

show create table pri01;
show create table aff01;

use mo_catalog;
drop table if exists t1;
create cluster table t1(a int);
insert into t1 values (1,6),(2,6),(3,6);


-- @session:id=1&user=acc01:test_account&password=111
drop database if exists t1;
create database t1;
use t1;
drop table if exists departments;
create table IF NOT EXISTS departments (
                                           department_id INT AUTO_INCREMENT primary key,
                                           department_name varchar(255) NOT NULL,
    INDEX idx_department_name (department_name)
    );

INSERT INTO departments (department_name) values ('HR'), ('Engineering'), ('Sales');
INSERT INTO departments (department_name) values ('PR'), ('Engineering'), ('Sales');

drop table if exists employees;
create table IF NOT EXISTS employees (
                                         employee_id INT AUTO_INCREMENT primary key,
                                         first_name varchar(255) NOT NULL,
    last_name varchar(255) NOT NULL,
    email varchar(255),
    department_id INT,
    INDEX idx_email (email),
    foreign key (department_id) references departments(department_id)
    );

insert into employees (first_name, last_name, email, department_id) values
                                                                        ('John', 'Doe', 'john.doe@company.com', 1),
                                                                        ('Jane', 'Smith', 'jane.smith@company.com', 2);

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

drop publication if exists pub02;
create publication pub02 database repub02 account acc02 comment 'publish before creating snapshot';
-- @ignore:5,6
show publications;
-- @ignore:2,6
select * from mo_catalog.mo_pubs;

drop database if exists procedure_test;
create database procedure_test;
use procedure_test;
drop table if exists tbh1;
drop table if exists tbh2;
drop table if exists tbh2;

create table tbh1(id int primary key, val int);
create table tbh2(id int primary key, val char);
create table tbh3(id int primary key, val float);

insert into tbh1(id, val) values(1,10),(2,20),(3,30);
insert into tbh2(id, val) values(1,'a'),(2,'b'),(3,'c');
insert into tbh3(id, val) values(1,1.5),(2,2.5),(3,3.5);

drop procedure if exists test_if_hit_elseif_first_elseif;
create procedure test_if_hit_elseif_first_elseif() 'begin DECLARE v1 INT; SET v1 = 5; IF v1 > 5 THEN select * from tbh1; ELSEIF v1 = 5 THEN select * from tbh2; ELSEIF v1 = 4 THEN select * from tbh2 limit 1; ELSE select * from tbh3; END IF; end';
call test_if_hit_elseif_first_elseif();

drop procedure if exists test_if_hit_if;
create procedure test_if_hit_if() 'begin DECLARE v1 INT; SET v1 = 5; IF v1 > 5 THEN select * from tbh1; ELSEIF v1 = 5 THEN select * from tbh2; ELSEIF v1 = 4 THEN select * from tbh2 limit 1; ELSE select * from tbh3; END IF; end';
call test_if_hit_if();
-- @ignore:0,7,8
select * from mo_catalog.mo_stored_procedure;
-- @session


-- @session:id=2&user=acc02:test_account&password=111
drop database if exists sub01;
create database sub01 from acc01 publication pub02;
show databases;
use sub01;
show tables;
select * from pri01;
select * from aff01;

drop database if exists udf_db2;
create database udf_db2;
use udf_db2;
create function `addAB`(x int, y int) returns int
    language sql as
'$1 + $2';
-- @ignore:0,9,10
select * from mo_catalog.mo_user_defined_function;

drop stage if exists my_ext_stage;
create stage my_ext_stage URL='s3://load/files/';
drop stage if exists my_ext_stage1;
create stage my_ext_stage1 URL='s3://load/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z'};
-- @ignore:0,5
select * from mo_catalog.mo_stages;
-- @session

-- @session:id=3&user=acc03:test_account&password=111
drop database if exists test01;
drop database if exists test02;
drop database if exists test03;
create database test01;
create database test02;
create database test03;

use test01;
drop table if exists sales;
create table sales (
                       id INT NOT NULL,
                       sale_date DATE NOT NULL,
                       amount DECIMAL(10, 2) NOT NULL,
                       PRIMARY KEY (id, sale_date)
) partition BY RANGE ( YEAR(sale_date) ) (
    partition p0 values LESS THAN (2019),
    partition p1 values LESS THAN (2020),
    partition p2 values LESS THAN (2021),
    partition p3 values LESS THAN (2022)
);

INSERT INTO sales (id, sale_date, amount) values
                                              (1, '2018-12-25', 100.00),
                                              (2, '2019-05-15', 200.00),
                                              (3, '2020-07-22', 150.00),
                                              (4, '2021-08-01', 300.00);

use test02;
create view v01 as select * from test01.sales;
show create view v01;
select * from v01;

use test03;
create view v02 as select * from test02.v01;
show create view v02;
select * from v02;

drop database if exists udf_db;
create database udf_db;
use udf_db;
-- @ignore:0,9,10
select name, db from mo_catalog.mo_user_defined_function;
-- function add
create function `addab`(x int, y int) returns int
    language sql as
'$1 + $2';
select addab(10, 5);
-- @ignore:0,9,10
select name, db from mo_catalog.mo_user_defined_function;

drop database if exists udf_db2;
create database udf_db2;
use udf_db2;
create function `subab`(x int, y int) returns int
    language sql as
'$1 - $2';
select subab(10, 5);
-- @ignore:0,9,10
select name, db from mo_catalog.mo_user_defined_function;
-- @session


-- @session:id=4&user=acc04:test_account&password=111
drop user if exists userx;
create user userx identified by '111';
drop user if exists usery;
create user usery identified by '222';
select user_name, authentication_string, status, login_type, creator, owner, default_role from mo_catalog.mo_user;

drop role if exists role1;
drop role if exists role2;
create role role1;
create role role2;
select role_name, creator, owner from mo_catalog.mo_role;

drop database if exists test;
create database test;
use test;
drop table if exists t1;
create table t1 (col1 int, col2 decimal);
drop role if exists role_r1,role_r2,role_r3;
create role role_r1,role_r2,role_r3;
drop user if exists role_u1, role_u2, role_u3;
create user role_u1 identified by '111', role_u2 identified by '111', role_u3 identified by '111';
grant role_r1,role_r2,role_r3 to role_u1,role_u2,role_u2;
grant role_r1 to role_r2;
grant role_r2 to role_r3;
grant select,insert,update on table test.* to role_r1 with grant option;
select user_name, authentication_string, status, login_type, creator, owner, default_role from mo_catalog.mo_user;
select role_name, creator, owner from mo_catalog.mo_role;
select role_name, privilege_id, with_grant_option from mo_catalog.mo_role_privs where role_name in ('role_r1','role_r2');
select operation_role_id,operation_user_id from mo_catalog.mo_role_grant;
-- @session

drop snapshot if exists cluster_level_snapshot;
create snapshot cluster_level_snapshot for cluster;
-- @ignore:1
show snapshots;

use db02;
alter table departments add column newcolumn int after department_id;
show create table departments;
drop table employees;
select * from employee_view;
select * from department_view;
select * from employee_with_department_view;

use db03;
truncate db03.aff01;
select count(*) from pri01;
select count(*) from aff01;

use mo_catalog;
drop table if exists t3;
create cluster table t3(a int);
insert into t3 values (1,6),(2,6),(3,6);
show tables;

-- @session:id=1&user=acc01:test_account&password=111
drop database t1;
drop database repub02;
use procedure_test;
drop procedure if exists test_if_hit_if;
drop procedure if exists test_if_hit_elseif_first_elseif;
drop publication pub02;
-- @ignore:0,7,8
select * from mo_catalog.mo_stored_procedure;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
drop function udf_db2.`addAB`(x int, y int);
drop stage if exists my_ext_stage;
-- @ignore:0,5
select * from mo_catalog.mo_stages;
use udf_db2;
create function `add`(x int, y int) returns int
    language sql as
'$1 + $2';
-- @ignore:0,9,10
select * from mo_catalog.mo_user_defined_function;
-- @session

-- @session:id=3&user=acc03:test_account&password=111
drop database test01;
use test02;
select * from v01;
use test03;
select * from v02;
use udf_db;
drop function `addab`(x int, y int);
use udf_db2;
drop function `subab`(x int, y int);
-- @ignore:0,9,10
select name, db from mo_catalog.mo_user_defined_function;
-- @session

-- @session:id=4&user=acc04:test_account&password=111
revoke role_r2 from role_r3;
revoke role_r1 from role_r2;
select operation_role_id,operation_user_id from mo_catalog.mo_role_grant;
-- @session

restore cluster from snapshot cluster_level_snapshot;

use db02;
select * from departments;
show create table departments;
select * from employee_view;
select * from department_view;
select * from employee_with_department_view;

use db03;
select count(*) from pri01;
select count(*) from aff01;

use mo_catalog;
select * from t1;
show tables;
drop table t1;

-- @session:id=1&user=acc01:test_account&password=111
select * from repub02.aff01;
select * from repub02.pri01;
-- @ignore:0,7,8
select * from mo_catalog.mo_stored_procedure;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
-- @ignore:0,5
select * from mo_catalog.mo_stages;
-- @ignore:0,9,10
select * from mo_catalog.mo_user_defined_function;
-- @session

-- @session:id=3&user=acc03:test_account&password=111
use test02;
select * from v01;
use test03;
select * from v02;
-- @ignore:0,9,10
select name, db from mo_catalog.mo_user_defined_function;
-- @session

-- @session:id=4&user=acc04:test_account&password=111
select operation_role_id,operation_user_id from mo_catalog.mo_role_grant;
-- @session

drop database db01;
use db02;
drop view employee_view;
drop view department_view;
drop view employee_with_department_view;
drop database db03;
use mo_catalog;
drop table t1;
drop table t3;
drop database db02;

-- @session:id=1&user=acc01:test_account&password=111
drop publication pub02;
drop database repub02;
drop database t1;
drop procedure test_if_hit_elseif_first_elseif;
drop procedure test_if_hit_if;
drop database procedure_test;
-- @ignore:0,7,8
select * from mo_catalog.mo_stored_procedure;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
drop database sub01;
use udf_db2;
drop function `addab`(x int, y int);
drop function `add`(x int, y int)
drop stage my_ext_stage;
drop stage my_ext_stage1;
-- @session

-- @session:id=3&user=acc03:test_account&password=111
drop database test03;
drop database test02;
drop database test01;
drop database udf_db;
drop database udf_db2;
-- @session

-- @session:id=4&user=acc04:test_account&password=111
drop role role_r1, role_r2, role_r3;
drop user role_u1, role_u2, role_u3;
drop database test;
-- @session
drop snapshot cluster_level_snapshot;

drop account acc01;
drop account acc02;
drop account acc03;
drop account acc04;