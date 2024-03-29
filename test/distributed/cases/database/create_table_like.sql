create table test(a int);
insert into test values(1);
create table new_test like test;
show create table new_test;
drop table test;
drop table new_test;

create database test1;
use test1;
create table test(a int);
insert into test values(1);
create database test2;
use test2;
create table new_test like test1.test;
show create table new_test;
drop database test1;
drop database test2;