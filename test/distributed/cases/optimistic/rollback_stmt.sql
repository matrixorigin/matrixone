drop database if exists rollbacktest;
create database rollbacktest;

set @@autocommit = 1;
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
--error. duplicate key
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

--issue 13678
create table if not exists t2( id int primary key );
insert into t2 values(1);
select * from t2;

set autocommit = 1;
begin;
--no error
insert into t2 values(2);
--1
--2
select * from t2;
--no error? why? workspace does check duplicate key?
insert into t2 values(1);
--1
--1
--2
select * from t2;
--duplicate key
commit;
--1
select * from t2;

drop table t1;
drop table t2;
drop database if exists rollbacktest;