drop database if exists drop_multi;
create database drop_multi;
use drop_multi;

drop table if exists t1,t2,t3;
create table t1(a int);
create table t2(a int);
create table t3(a int);
show tables;

drop table if exists t1,t2,t3;
show tables;

create table t1(a int);
create table t2(a int);
show tables;

drop table if exists drop_multi.t1, t2;
show tables;

-- duplicate table names
create table t1(a int);
show tables;
drop table if exists t1, t1;
show tables;

-- no IF EXISTS, expect error and t1 not dropped
create table t1(a int);
drop table t1, no_such_table;
show tables;
drop table t1;

-- view should be skipped
create table t1(a int);
create view v1 as select * from t1;
drop table if exists v1, t1;
select table_name, table_type from information_schema.tables where table_schema='drop_multi' order by table_name, table_type;
drop view v1;

-- sequence should be skipped
create sequence s1;
create table t1(a int);
drop table if exists s1, t1;
show sequences where names in('s1');
drop sequence s1;

drop database drop_multi;
