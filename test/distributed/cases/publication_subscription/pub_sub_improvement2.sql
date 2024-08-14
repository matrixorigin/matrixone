-- pub-sub improvement test
drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';
drop account if exists acc02;
create account acc02 admin_name = 'test_account' identified by '111';
drop account if exists acc03;
create account acc03 admin_name = 'test_account' identified by '111';

drop database if exists test_pub_sub;
create database test_pub_sub;
use test_pub_sub;

drop table if exists authors;
create table authors (
    author_id int auto_increment PRIMARY KEY,
    first_name varchar(100) NOT NULL,
    last_name varchar(100) NOT NULL,
    date_of_birth DATE,
    year_of_birth int
);

drop table if exists publishers;
create table publishers (
    publisher_id int auto_increment PRIMARY KEY,
    name varchar(100) NOT NULL,
    address varchar(255),
    city varchar(100),
    country varchar(100)
);

drop table if exists books;
create table books (
    book_id int auto_increment PRIMARY KEY,
    title varchar(255) NOT NULL,
    publisher_id int,
    author_id int,
    year_published int,
    edition int,
    foreign key fk1(publisher_id) references publishers(publisher_id),
    foreign key fk2(author_id) fk2 references authors(author_id)
);

insert into authors (first_name, last_name, date_of_birth, year_of_birth) values
                    ('J.K.', 'Rowling', '1965-07-31', 1965),
                    ('George R.R.', 'Martin', '1948-09-20', 1948);

insert into publishers (name, address, city, country) values
                        ('Penguin Books', '80 Strand, London', 'London', 'UK'),
                        ('HarperCollins', '10 East 53rd Street', 'New York', 'USA');

insert into books (title, publisher_id, author_id, year_published, edition) values
                    ('Harry Potter and the Sorcerer''s Stone', 1, 1, 1997, 1),
                    ('A Game of Thrones', 2, 2, 1996, 1);

-- if no table is entered, all tables are published by default
drop publication if exists p01;
create publication p01 database test_pub_sub account all;
-- @ignore:5,6
show publications;

-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:5,7
show subscriptions all;
create database acc01_sub01 from sys publication p01;
-- @ignore:5,7
show subscriptions;
show databases;
use acc01_sub01;
show tables;
select * from authors;
select count(*) from books;
show create table publishers;
drop database acc01_sub01;
-- @session

-- @ignore:5,6
show publications;

drop publication if exists p02;
create publication p02 database test_pub_sub account acc02;
-- @ignore:5,6
show publications;
-- @session:id=2&user=acc02:test_account&password=111
-- @ignore:5,7
show subscriptions all;
create database sub_acc02 from sys publication p01;
-- @ignore:5,7
show subscriptions all;
-- @ignore:5,7
show subscriptions;
show databases;
use sub_acc02;
show tables;
select count(*) from authors;
show create table books;
select * from publishers;
-- @session

-- @ignore:5,6
show publications;

-- @session:id=1&user=acc01:test_account&password=111
drop database acc01_sub01;
-- @session
-- @session:id=2&user=acc02:test_account&password=111
drop database sub_acc02;
-- @session
drop publication p01;
drop publication p02;
drop database test_pub_sub;




-- system tenant is the publisher, deletes some published tables and verifies that tables in show subscriptions only
-- show tables that are effectively published
drop database if exists test_pub_sub1;
create database test_pub_sub1;
use test_pub_sub1;
drop table if exists table01;
create table table01(col1 int primary key , col2 decimal, col3 char, col4 varchar(20), col5 text, col6 double);
insert into table01 values (1, 2, 'a', '23eiojf', 'r23v324r23rer', 3923.324);
insert into table01 values (2, 3, 'b', '32r32r', 'database', 1111111);
drop table if exists table02;
create table table02 (col1 int unique key, col2 varchar(20));
insert into table02 (col1, col2) values (133, 'database');

drop publication if exists p03;
create publication p03 database test_pub_sub1 account all;
-- @session:id=1&user=acc01:test_account&password=111
drop database if exists acc_sub02;
create database acc_sub02 from sys publication p03;
-- @ignore:5,7
show subscriptions;
use acc_sub02;
show tables;
select * from table01;
select count(*) from table02;
-- @session

alter publication p03 account acc01,acc02 database test_pub_sub1 table table01;
-- @ignore:5,6
show publications;

-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:5,7
show subscriptions;
use acc_sub02;
show tables;
select * from table01;
drop database acc_sub02;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists acc02_sub02;
create database acc02_sub02 from sys publication p03;
use acc02_sub02;
show tables;
select * from table01;
drop database acc02_sub02;
-- @session

drop publication p03;
drop database test_pub_sub1;




-- abnormal test
-- sys publish database/table to current account
drop database if exists sys_db01;
create database sys_db01;
use sys_db01;
drop table if exists t1;
create table t1 (col1 int auto_increment, col2 char(20));
insert into t1 values(1, 'ad');
insert into t1 values(2, 'ew');
drop publication if exists pub_sys;
create publication pub_sys database t1 account sys;
create publication pub_sys database sys_db01 table t1 account sys;
drop database sys_db01;


-- abnormal test
-- sys publish database/db to no-exists account
drop database if exists sys_db02;
create database sys_db02;
use sys_db02;
drop table if exists t2;
create table pri01(
                      deptno int unsigned comment '部门编号',
                      dname varchar(15) comment '部门名称',
                      loc varchar(50)  comment '部门所在位置',
                      primary key(deptno)
) comment='部门表';

insert into pri01 values (10,'ACCOUNTING','NEW YORK');
insert into pri01 values (20,'RESEARCH','DALLAS');
insert into pri01 values (30,'SALES','CHICAGO');
insert into pri01 values (40,'OPERATIONS','BOSTON');
drop publication if exists pub10;
create publication pub10 database sys_db02 account acc10;
create publication pub10 database sys_db02 table pri01 account acc10;
create publication pub10 database sys_db02 account acc01,acc10;
create publication pub10 database sys_db02 table pri01 account acc01,acc10;
drop database sys_db02;


-- abnormal test
-- non-sys publish database/table to current account
-- @session:id=1&user=acc01:test_account&password=111
drop database if exists sys_db01;
create database sys_db01;
use sys_db01;
drop table if exists t1;
create table t1 (col1 int auto_increment, col2 char(20));
insert into t1 values(1, 'ad');
insert into t1 values(2, 'ew');
drop publication if exists pub_sys;
create publication pub_sys database sys_db01 account acc01;
create publication pub_sys database sys_db01 table t1 account acc01;
drop database sys_db01;
-- @session


-- abnormal test
-- non-sys publish db/table to account not exists or some tenant names are incorrect
-- @session:id=1&user=acc01:test_account&password=111
drop database if exists sys_db01;
create database sys_db01;
use sys_db01;
drop table if exists t1;
create table t1 (col1 int auto_increment, col2 char(20));
insert into t1 values(1, 'ad');
insert into t1 values(2, 'ew');
drop publication if exists pub_sys;
create publication pub_sys database sys_db01 account acc01,acc10;
create publication pub_sys database sys_db01 table t1 account acc08,acc10;
drop database sys_db01;
-- @session




-- abnormal test
-- publish cluster table
use mo_catalog;
create cluster table clu01(col1 int, col2 decimal);
insert into clu01 values(1,2,0);
insert into clu01 values(2,3,0);

drop publication if exists p04;
create publication p04 database mo_catalog table clu01 account all;
drop table clu01;


-- abnormal test
-- publish subscribed table
drop database if exists publish_subscribed_table;
create database publish_subscribed_table;
use publish_subscribed_table;
create table t1(col1 int primary key);
insert into t1 values(1),(2),(3);
drop publication if exists pub01;
create publication pub01 database publish_subscribed_table table t1 account acc01;
-- @session:id=1&user=acc01:test_account&password=111
drop database if exists sub01;
create database sub01 from sys publication pub01;
show databases;
drop publication if exists sub_p01;
create publication sub_p01 database sub01 account acc02;
drop database sub01;
-- @session
drop publication pub01;
drop database publish_subscribed_table;


-- abnormal test
-- only table is specified but db is not specified, and publishing fails
-- the account was not specified, report error
drop database if exists test10;
create database test10;
use test10;
create table table01(col1 varchar(50), col2 bigint);
insert into table01 values('database',23789324);
insert into table01 values('fhuwehwfw',3829032);
drop publication pub11;
create publication pub11 table table01 account acc01;
create publication pub11 table table01;
drop database test10;


-- abnormal test
-- multiple subscription
drop database if exists mul01;
create database mul01;
use mul01;
create table table10 (col1 int, col2 float, col3 decimal, col4 enum('1','2','3','4'));
insert into table10 values (1, 12.21, 32324.32131, 1);
insert into table10 values (2, null, null, 2);
insert into table10 values (2, -12.1, 34738, null);
insert into table10 values (1, 90.2314, null, 4);
insert into table10 values (1, 43425.4325, -7483.432, 2);
drop publication if exists pub15;
create publication pub15 database mul01 account acc01;
-- @session:id=1&user=acc01:test_account&password=111
drop database if exists sub15;
create database sub15 from sys publication pub15;
create database sub16 from sys publication pub15;
drop database sub15;
-- @session
drop publication pub15;
drop database mul01;


-- abnormal test
-- Common users do not have permission to create publications or subscriptions
-- @session:id=1&user=acc01:test_account&password=111
drop user if exists u01;
create user u01 identified by '111';
-- @session
-- @session:id=3&user=acc01:u01&password=111
drop database if exists auth01;
create database auth01;
use auth01;
create publication p01 database auth01 account acc01;
-- @session
-- @session:id=1&user=acc01:test_account&password=111
drop user u01;
-- @session




-- abnormal test: create same publication name
drop database if exists dup01;
create database dup01;
use dup01;
create table t01(col1 int, col2 decimal);
insert into t01 values(1,2);
insert into t01 values(2,3);
drop publication if exists p10;
create publication p10 database dup01 account acc01;
-- @pattern
create publication p10 database dup01 account acc02;
-- @session:id=1&user=acc01:test_account&password=111
drop database if exists dup_sub01;
create database dup_sub01 from sys publication p10;
create database dup_sub01 from sys publication p10;
drop database dup_sub01;
-- @session
drop publication p10;
drop database dup01;

drop account acc01;
drop account acc02;
drop account acc03;