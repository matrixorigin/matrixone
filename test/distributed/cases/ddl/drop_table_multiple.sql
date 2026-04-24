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

create table t1(a int);
show tables;

drop table if exists t1, t1;
show tables;

create table t1(a int);
drop table t1, no_such_table;
show tables;
drop table t1;

drop database drop_multi;
