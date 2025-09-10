drop database if exists test;

-- 1. normal clone database within txn
begin;
create database test clone system;
show create database test;
commit;
show create database test;

drop database test;

begin;
create database test clone system;
show create database test;
rollback;
show create database test;


-- 2. err happened when clone database
select enable_fault_injection();
select add_fault_point('fj/cn/clone_fails',':::','echo',40,'test.rawlog');
create database test clone system;
show create database test;
select disable_fault_injection();


-- 3. normal clone table within txn
create database test;
begin;
create table test.t1 clone system.rawlog;
commit;
show tables from test;

drop table test.t1;
begin;
create table test.t1 clone system.rawlog;
rollback;
show tables from test;

-- 4. err happened when clone table
select enable_fault_injection();
select add_fault_point('fj/cn/clone_fails',':::','echo',40,'test.t1');
create table test.t1 clone system.rawlog;
select disable_fault_injection();
show tables from test;

drop database test;
