drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';

-- @session:id=1&user=acc01:test_account&password=111
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
insert into aff01 values (7654,'MARTIN','SALPESMAN',7698,'1981-09-28',1250,1400,30);
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
-- @session

create snapshot snapshot_01 for account acc01;

-- @session:id=2&user=acc01:test_account&password=111
insert into acc_test02.pri01 values (50,'ACCOUNTING','NEW YORK');
insert into acc_test02.aff01 values (9000,'MILLER','CLERK',7782,'1982-01-23',1300,NULL,50);
select count(*) from acc_test02.aff01;
-- @session

restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;
restore account acc01 from snapshot snapshot_01;

drop account acc01;
drop snapshot snapshot_01;
