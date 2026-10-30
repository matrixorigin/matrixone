-- A numeric literal DEFAULT whose text does not fit a CHAR/VARCHAR column is
-- rejected with 1067 like an over-long string literal, instead of being
-- silently truncated to the column width.
drop database if exists default_numeric_width;
create database default_numeric_width;
use default_numeric_width;
create table t1(v varchar(3) default 12345);
create table t2(v varchar(3) default 1.25);
create table t3(v char(2) default 123);
create table t4(v varchar(3) default 'abcd');
create table ok1(id int, v varchar(3) default 123, w char(5) default 12345, x varchar(4) default 1.25, y varchar(3) default -12);
-- expression defaults are evaluated per row, not at DDL time
create table ok2(z varchar(3) default (12345));
insert into ok1(id) values (1);
select id, v, w, x, y from ok1;
create table a(id int primary key, v varchar(5), w varchar(3));
alter table a alter column v set default 123456;
alter table a alter column v set default 12345;
alter table a add column n varchar(2) default 100;
alter table a modify w varchar(3) default 1000;
alter table a change w w2 varchar(3) default 99.5;
show create table a;
insert into a(id) values (1);
select * from a;
drop database default_numeric_width;
