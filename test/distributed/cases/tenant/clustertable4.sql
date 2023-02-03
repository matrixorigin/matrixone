use mo_catalog;
create cluster table t1(a int);
drop table if exists t1;
use information_schema;
create cluster table t1(a int);
use system;
create cluster table t1(a int);
use system_metrics;
create cluster table t1(a int);
use mysql;
create cluster table t1(a int);
use mo_task;
create cluster table t1(a int);

drop database if exists db1;
create database db1;
use db1;
create cluster table t1(a int);
drop database if exists db1;