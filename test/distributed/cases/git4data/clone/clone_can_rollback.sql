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

-- 6. clone and branch a table created earlier in the same transaction
begin;
create table test.txn_src(a int primary key, b int);
insert into test.txn_src values (1, 10), (2, 20);
create table test.txn_clone clone test.txn_src;
data branch create table test.txn_b1 from test.txn_src;
data branch create table test.txn_b2 from test.txn_b1;
select count(*) from test.txn_clone;
select count(*) from test.txn_b2;
commit;

insert into test.txn_b2 values (3, 30);
drop table test.txn_b1;
data branch diff test.txn_b2 against test.txn_src output count;

drop database test;
drop database srcdb;
