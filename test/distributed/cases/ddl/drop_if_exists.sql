create account acc101 admin_name='root' identified by '123456';
-- @session:id=1&user=acc101:root&password=123456

-- 如果创建新的session后，没有显式选择数据库，执行drop table 报错
drop table if exists t1;
-- 如果创建新的session后，没有显式选择数据库，执行drop index 报错
drop index if exists idx1 on t1;
--如果创建新的session后，没有显式选择数据库，执行drop view 报错
drop view if exists v1;

create database db1;
use db1;
create table t1(
    empno int unsigned auto_increment,
    ename varchar(15),
    job varchar(10),
    mgr int unsigned,
    hiredate date,
    sal decimal(7,2),
    comm decimal(7,2),
    deptno int unsigned,
    primary key(empno),
    unique index idx1(ename)
);

show index from t1;
create view v1 as select * from t1;
show tables;

-- @session:id=2&user=acc101:root&password=123456

-- 如果创建新的session后，没有显式选择数据库，执行drop index 报错
drop index if exists idx1 on t1;
-- 如果创建新的session后，没有显式选择数据库，执行drop table 报错
drop table if exists t1;
--如果创建新的session后，没有显式选择数据库，执行drop view 报错
drop view if exists v1;


use db1;
drop index if exists idx1 on t1;
show index from t1;
drop view if exists v1;
drop table if exists t1;

show tables;
drop database db1;
-- @session
drop account acc101;


drop database if exists test;
create database test;
begin;
create table test.t1(a int, b varchar);
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'test.t1');
insert into test.t1 select *,"just a test for clean files from the disk" from generate_series(1, 50*10000)g;
select count(*) from test.t1;
drop table test.t1;
select disable_fault_injection();
commit;

drop database test;




