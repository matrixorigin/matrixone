drop database if exists test_temporary;
create database test_temporary;
use test_temporary;
create temporary table t (a int);
create table t1 (a int);
show tables;
t1