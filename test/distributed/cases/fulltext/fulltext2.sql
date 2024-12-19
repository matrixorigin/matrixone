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



-- create fulltext in prepare stmt
drop table if exists prepare_fulltext;
create table prepare_fulltext (a char primary key , b varchar(20));
insert into prepare_fulltext values (1, 11), (2, 22), (3, 33);
prepare stmt1 from 'create fulltext index f06 on prepare_fulltext (a)';
execute stmt1;
-- @bvt:issue#20613
show create table prepare_fulltext;
-- @bvt:issue
select * from prepare_fulltext;
drop table prepare_fulltext;



-- alter table add fulltext index in prepare stmt
drop table if exists pro;
create table pro (
    id int auto_increment primary key,
    name varchar(255),
    details json
);
prepare stmt4 from 'alter table pro add fulltext index pro1(details) with PARSER json';
execute stmt4;
prepare stmt3 from 'alter table pro add fulltext index pro2(name)';
execute stmt3;
-- @bvt:issue#20613
show create table pro;
-- @bvt:issue
insert into pro (name, details) values('手机', '{"brand": "Apple", "model": "iPhone 12", "price": 800}');
select * from pro;
drop table pro;



-- create fulltext index with parser default and load data
drop table if exists test_table;
create table test_table(
col1 int auto_increment,
col2 float,
col3 bool,
col4 Date,
col5 varchar(255),
col6 text,
PRIMARY KEY (`col1`),
fulltext(col5)
);
-- @bvt:issue#20613
show create table test_table;
-- @bvt:issue
load data infile '$resources/load_data/test_1.csv' into table test_table fields terminated by ',' parallel 'true';
select * from test_table;
drop table test_table;



-- create fulltext index
drop table if exists jsonline_t2;
create table jsonline_t2(
col1 char(225),
col2 varchar(225) ,
col3 text,
col4 varchar(225) primary key
);
create fulltext index f05 on jsonline_t2(col3);
load data infile{'filepath'='$resources/load_data/char_varchar_2.jl','format'='jsonline','jsondata'='object'}into table jsonline_t2;
select * from jsonline_t2;
drop table jsonline_t2;



-- create fulltext index with parser json
drop table if exists t1;
create table t1(
    col1 bool,
    col2 int primary key,
    col3 varchar(100),
    col4 date,
    col5 datetime,
    col6 timestamp,
    col7 decimal,
    col8 float,
    col9 json,
    col10 text,
    col11 json,
    col12 bool
);
create fulltext index f06 on t1(col9);
load data infile {'filepath'='$resources/load_data/jsonline_object01.jl','format'='jsonline','jsondata'='object'} into table t1;
select * from t1;
-- @bvt:issue#20613
show create table t1;
-- @bvt:issue
drop table t1;




-- create fulltext index on multi column
drop table if exists articles;
create table articles (
    id int unsigned auto_increment not null primary key,
    title varchar(200),
    body text,
    fulltext (title, body)
);
insert into articles (title, body) VALUES
('MO Tutorial', 'DBMS stands for DataBase ...'),
('How To Use MO Well', 'After you went through a ...'),
('Optimizing MO', 'In this tutorial, we show ...'),
('1001 MO Tricks', '1. Never run MOd as root. 2. ...'),
('MO vs. YourSQL', 'In the following database comparison ...'),
('MO Security', 'When configured properly, MO ...');
-- fulltext index query in natural language mode
select * from articles
where match (title, body)
against ('database' in natural language mode);
-- fulltext index query in natural language mode with alias
select id, match (title, body)
against ('Tutorial' in natural language mode) as score
from articles;
-- fulltext index query match multi and calculate score
select id, body, match (title, body)
against ('Security implications of running MO as root' in natural language mode) as score
from articles;
-- filter with where clause and select
select id, body, match (title, body)
against ('Security implications of running MO as root' in natural language mode) as score
from articles
where match (title, body)
against ('Security implications of running MO as root' in natural language mode);
drop table articles;




-- fulltext index query single column in natural language mode
drop table if exists article;
create table article (
    id int unsigned auto_increment not null primary key,
    title varchar(200),
    body text,
    fulltext (body)
);
insert into article (title, body) VALUES
('MO Tutorial', 'DBMS stands for DataBase ...'),
('How To Use MO Well', 'After you went through a ...'),
('Optimizing MO', 'In this tutorial, we show ...'),
('1001 MO Tricks', '1. Never run MOd as root. 2. ...'),
('MO vs. YourSQL', 'In the following database comparison ...'),
('MO Security', 'When configured properly, MO ...');
-- fulltext index query match column in natural language mode
select * from article
where match (body)
against ('database' in natural language mode);
-- fulltext index query match single and calculate score
select id, match (body)
against ('DataBase' in natural language mode) as score
from article;
-- filter with where clause and select
select id, body, match (body)
against ('Security implications of running MO as root' in natural language mode) as score
from article
where match (body)
against ('Security implications of running MO as root' in natural language mode);
drop table article;




-- match with +/-/++/+-/*
drop table if exists example1;
create table example1 (
    id INT auto_increment primary key,
    content text,
    fulltext(content)
);
insert into example1 (content) values
('MO is a database management system.'),
('A database management system is a software that manages databases.'),
('MO is a popular choice for development.'),
('PHP is a popular server-side scripting language for web development.'),
('Python is a high-level programming language used for various applications.');
select * from example1
where match (content)
against ('+MO +database' in boolean mode);

select * from example1
where match(content)
against ('+database' in boolean mode);

select * from example1
where match(content)
against ('-database' in boolean mode);

select * from example1
where match (content)
against ('+web development -MO' in boolean mode);

select * from example1
where match (content)
against ('+MO' in boolean mode);

select * from example1
where match (content)
against ('+MO ~popular' in boolean mode);

select * from example1
where match (content)
against ('MO*' in boolean mode);

select * from example1
where match (content)
against ('+MO +(<popular >database)' in boolean mode);

select * from example1
where match (content)
against ('+MO popular' in boolean mode);

select * from example1
where match (content)
against ('popular' in boolean mode);
drop table example1;




-- match json
drop table if exists example_json;
create table example_json (
    id int auto_increment primary key,
    data json
);
alter table example_json add fulltext index idx_jsondata (data) with parser json;

insert into example_json (data) values
('{"title": "MO Full-Text Search", "content": "Full-text search is a technique for searching text-based content."}'),
('{"title": "Introduction to MO", "content": "MO is an open-source relational database management system."}'),
('{"title": "MO development", "content": "MO history"}');
select * from example_json where match(data) against ('MO development' in boolean mode);
select * from example_json where match(data) against (' ');
select * from example_json where match(data) against ('"MO development"' in boolean mode);
select * from example_json where match(data) against ('+MO -open -source' in boolean mode);
drop table example_json;




-- fulltext index with join
drop table if exists articles;
drop table if exists authors;
create table articles (
    id int auto_increment primary key,
    title varchar(255),
    content text,
    author_id int,
    fulltext(content)
);
create table authors (
    id int auto_increment primary key,
    name varchar(100)
);
insert into authors (name) values ('John Doe'), ('Jane Smith'), ('Alice Johnson');
insert into articles (title, content, author_id) values
('MO全文索引入门', 'MO全文索引是一种强大的工具，可以帮助你快速检索数据库中的文本数据。', 1),
('深入理解全文索引', '全文索引不仅可以提高搜索效率，还可以通过JOIN操作与其他表结合使用。', 2),
('MO性能优化', '本文将探讨如何优化MO数据库的性能，包括索引优化和查询优化。', 3),
('全文索引与JOIN操作', '全文索引可以与JOIN操作结合使用，以实现跨表的全文搜索。', 1);
select * from articles;
select * from authors;
-- @bvt:issue#20687
select a.title, a.content, au.name
from articles a
join authors au on a.author_id = au.id
where match(a.content) against ('MO' IN NATURAL LANGUAGE MODE);
-- @bvt:issue
drop table articles;
drop table authors;



-- fulltext index with group by/union
drop table if exists posts;
drop table if exists comments;
create table posts (
    post_id int auto_increment primary key,
    title varchar(255),
    content text
);
create table comments (
    comment_id int auto_increment primary key,
    post_id int,
    comment_text text,
    foreign key (post_id) references posts(post_id)
);
alter table posts add fulltext(content);
insert into posts (title, content) values
('MO全文索引入门', 'MO全文索引是一种强大的工具，可以帮助你快速检索数据库中的文本数据。'),
('深入理解全文索引', '全文索引不仅可以提高搜索效率，还可以通过JOIN操作与其他表结合使用。');
insert into comments (post_id, comment_text) values
(1, '这篇文章很有用，谢谢分享！'),
(1, '我也在学习全文索引，很有帮助。'),
(2, '全文索引真的很强大，学习了。');
-- @bvt:issue#20687
select count(posts.title), count(comments.comment_id) as comment_count
from posts
left join comments on posts.post_id = comments.post_id
where match(posts.content) against ('全文索引' IN NATURAL LANGUAGE MODE)
group by posts.post_id;

select title, content from articles
where match(content) against ('全文索引' IN NATURAL LANGUAGE MODE)
union
select comment_text as title, comment_text as content from comments
where match(comment_text) AGAINST ('全文索引' IN NATURAL LANGUAGE MODE);
-- @bvt:issue
drop table comments;
drop table posts;



drop database test_fulltext;
