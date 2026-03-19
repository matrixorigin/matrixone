select enable_fault_injection();

drop database if exists test;

drop database if exists srcdb;
create database srcdb;
create table srcdb.t1(a int);
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'srcdb.t1');
insert into srcdb.t1 select * from generate_series(200000)g;
select disable_fault_injection();

-- 1. normal clone database within txn
begin;
create database test clone srcdb;
show create database test;
commit;
show create database test;

drop database test;

begin;
create database test clone srcdb;
show create database test;
rollback;
show create database test;


-- 2. err happened when clone database
select enable_fault_injection();
select add_fault_point('fj/cn/clone_fails',':::','echo',40,'test.t1');
create database test clone srcdb;
show create database test;
select disable_fault_injection();


-- 3. normal clone table within txn
create database test;
begin;
create table test.t1 clone srcdb.t1;
commit;
show tables from test;

drop table test.t1;
begin;
create table test.t1 clone srcdb.t1;
rollback;
show tables from test;

-- 4. err happened when clone table
select enable_fault_injection();
select add_fault_point('fj/cn/clone_fails',':::','echo',40,'test.t1');
create table test.t1 clone srcdb.t1;
select disable_fault_injection();
show tables from test;

-- 5. ensure the shared files won't be deleted
select count(*) from srcdb.t1 where a mod 100 = 0;

drop database test;
drop database srcdb;
