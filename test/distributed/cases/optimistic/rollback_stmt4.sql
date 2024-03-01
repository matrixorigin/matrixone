drop database if exists rollbacktest;
create database rollbacktest;
use rollbacktest;
--// workspace内部有与表中数据重复时，commit会报错
create table if not exists t2( id int primary key );
insert into t2 values(1);
select * from t2;
begin;
insert into t2 values(2);
select * from t2;
insert into t2 values(1);
select * from t2;
-- ERROR 1062 (HY000): Duplicate entry '1' for key 'id'
commit;
--1
select * from t2;

--// workspace内部数据重复时，commit不会报错

begin;
insert into t2 values(2);
insert into t2 values(2);
select * from t2;
--no error
commit;
--1
--2
--2
select * from t2;

begin;
insert into t2 values(2);
insert into t2 values(2);
select * from t2;
--Duplicate entry '2' for key 'id'
commit;
--1
--2
--2
select * from t2;

--// workspace内部有与表中数据重复时，commit不会报错？

create table t1(a int primary key );
--no error
insert into t1 values(1);
--error. duplicate key
insert into t1 values(1);
--1
select * from t1;

begin;
delete from t1 where a = 1;
--no error
insert into t1 values(1);
--no error? why? workspace does check duplicate key?
insert into t1 values(1);
--1
--1
select * from t1;

--no error
insert into t1 values(2);
--no error? why? workspace does check duplicate key?
insert into t1 values(2);

--1
--1
--2
--2
select * from t1;
--no error
insert into t1 values(3);
--no error
delete from t1 where a = 3;
--error. no column b in t1
delete from t1 where b = 3;
--no error
insert into t1 values(3);
--error. duplicate key？
update t1 set a = 2;
----------------
--why not error?
----------------
commit ;
--1
--1
--2
--2
--3
select * from t1;

drop database if exists rollbacktest;