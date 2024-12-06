set experimental_fulltext_index=1;
drop database if exists test_fulltext;
create database test_fulltext;
use test_fulltext;

-- char column as fulltext index
drop table if exists fulltext01;
create table fulltext01
(
LastName char(10) primary key,
FirstName char(10),
Gender char(1),
DepartmentName char(20),
Age int
);
insert into fulltext01 VALUES('Gilbert', 'Kevin','M','Tool Design',33);
insert into fulltext01 VALUES('Tamburello', 'Andrea','F','Marketing',45);
insert into fulltext01 VALUES('Johnson', 'David','M','Engineering',66);
insert into fulltext01 VALUES('Sharma', 'Bradley','M','Production',27);
insert into fulltext01 VALUES('Rapier', 'Abigail','F',	'Human Resources',38);
select * from fulltext01;

create fulltext index ftidx on fulltext01 (LastName, FirstName);
alter table fulltext01 add column newcolumn decimal after LastName;
-- @bvt:issue#20613
show create table fulltext01;
-- @bvt:issue
select * from fulltext01;
truncate fulltext01;
drop table fulltext01;



-- varchar column as fulltext index
drop table if exists employees;
create table employees (
  employeeNumber int(11) NOT NULL,
  lastName varchar(50) NOT NULL,
  firstName varchar(50) NOT NULL,
  extension varchar(10) NOT NULL,
  email varchar(100) NOT NULL,
  officeCode varchar(10) NOT NULL,
  reportsTo int(11) DEFAULT NULL,
  jobTitle varchar(50) NOT NULL,
  PRIMARY KEY (employeeNumber)
);
insert into employees(employeeNumber,lastName,firstName,extension,email,officeCode,reportsTo,jobTitle) values
(1002,'Murphy','Diane','x5800','dmurphy@classicmodelcars.com','1',NULL,'President'),
(1056,'Patterson','Mary','x4611','mpatterso@classicmodelcars.com','1',1002,'VP Sales'),
(1076,'Firrelli','Jeff','x9273','jfirrelli@classicmodelcars.com','1',1002,'VP Marketing');
select * from employees;
create fulltext index f01 on employees (LastName, FirstName);
alter table employees drop column LastName;
-- @bvt:issue#20613
show create table employees;
-- @bvt:issue
select * from employees;
select count(*) from employees;
truncate employees;
drop table employees;



-- text column as fulltext index
drop table if exists t1;
create table t1 (col1 int primary key , col2 longtext);
insert into t1 values(1, 'abcdef');
insert into t1 values(2, '_bcdef');
insert into t1 values(3, 'a_cdef');
insert into t1 values(4, 'ab_def');
create fulltext index f02 on t1 (col2);
alter table t1 modify column col2 text;
-- @bvt:issue#20613
show create table t1;
-- @bvt:issue
select * from t1;
drop table t1;



-- json column as fulltext index
drop table if exists t1;
create table t1 (a json, b int primary key);
insert into t1 values ('{"t1":"a"}',1),('{"t1":"b"}',2),('{"t1":"c"}',3),('{"t1":"d"}',4);
select * from t1;
create fulltext index f03 on t1 (a);
insert into t1 values ('{"t1":"c"}',5);
select count(a) from t1;
select * from t1;
drop table t1;



-- datalink column as fulltext index column
drop table if exists table01;
create table table01 (col1 int primary key, col2 datalink);
insert into table01 values (1, 'file://$resources/load_data/test_columnlist_01.csv?offset=5');
insert into table01 values (2, 'file://$resources/load_data/test_columnlist_02.csv?offset=10');
select * from table01;
create fulltext index f06 on table01 (col2);
drop table table01;



-- create fulltext index with
-- abnormal test: single table, create duplicate fulltext index
drop table if exists ab01;
create table ab01(col1 int not null primary key , col2 char, col3 varchar(10));
insert into ab01 values (1,2,'da');
insert into ab01 values (2,3,'e4r34f');
select * from ab01;
create fulltext index f01 on ab01 (col2);
-- @bvt:issue#20213
create fulltext index f02 on ab01 (col2);
-- @bvt:issue
drop table ab01;



-- alter table add/drop fulltext index
drop table if exists char01;
create table char01 (col1 varchar(200) primary key , col2 char(10));
insert into char01 values ('23789178942u1uj3ridjfh2d28u49u4ueji32jf2f32ef32894rjk32nv432f432f', '367283r343');
insert into char01 values ('32jhbfchjecmwd%^&^(*&)UJHFRE%^T&YUHIJKNM', null);
select * from char01;
alter table char01 add fulltext index f01(col1);
alter table char01 add fulltext index f02(col2);
-- @bvt:issue#20613
show create table char01;
-- @bvt:issue
drop table char01;



-- abnormal test: create fulltext index on non-string column
drop table if exists ab02;
create table ab02 (a bigint unsigned not null, primary key(a));
insert into ab02 values (18446744073709551615), (0xFFFFFFFFFFFFFFFE), (18446744073709551613), (18446744073709551612);
select * from ab02;
create fulltext index f03 on ab02 (a);
drop table ab02;

drop table if exists t1;
create table t1(a binary(2) primary key);
insert into t1 values(null);
select * from t1;
insert into t1 values("时");
select * from t1;
insert into t1 values(rpad("1", 500, "1"));
delete from t1 where a="时";
insert into t1 values("6");
insert into t1 values("66");
create fulltext index f04 on t1 (a);
drop table t1;

drop table if exists t1;
create table t1(t time(3) primary key );
insert into t1 values("100:00:20");
insert into t1 values("-800:59:59");
insert into t1 values("2012-12-12 12:00:20");
insert into t1 values("2012-12-12 12:00:20.1234");
insert into t1 values("2012-12-12 12:00:20.1235");
create fulltext index f05 on t1 (t);
drop table t1;



-- create fulltext index with parser ngram
drop table if exists articles;
create table articles (
id int auto_increment primary key,
title varchar(255),
content text,
fulltext(title, content) with parser ngram
);
insert into articles (title, content) values
('MO全文索引示例', '这是一个关于MO全文索引的例子。它展示了如何使用ngram解析器进行全文搜索。'),
('ngram解析器', 'ngram解析器允许MO对中文等语言进行分词，以优化全文搜索。');
show create table articles;
select * from articles where match(title, content) against('全文索引' IN NATURAL LANGUAGE MODE);
select * from articles;
drop table articles;



-- create fulltext index with parser json
drop table if exists products;
create table products (
    id int auto_increment primary key,
    name varchar(255),
    details json,
    fulltext(details) with PARSER json
);
insert into products (name, details) values
('ノートパソコン', '{"brand": "Dell", "specs": "i7, 16GB RAM", "price": 1200}'),
('스마트폰', '{"brand": "Apple", "model": "iPhone 12", "price": 800}');
show create table products;
insert into products (name, details) values('手机', '{"brand": "Apple", "model": "iPhone 12", "price": 800}');
select * from products;
select * from products where match(details) against('Dell' IN NATURAL LANGUAGE MODE);
select id, name from products where match(details) against('Apple' IN NATURAL LANGUAGE MODE);
drop table products;

drop database test_fulltext;
