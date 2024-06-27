drop database if exists db1;
create database db1;
use db1;

CREATE TABLE t (
a int NOT NULL PRIMARY KEY,
b int NOT NULL DEFAULT 10,
c int NULL DEFAULT 20,
d varchar(20) not null default 'xxx',
e timestamp DEFAULT NULL,
f timestamp NULL DEFAULT CURRENT_TIMESTAMP
);

desc t;
show create table t;

-- 插入数据
INSERT INTO t (a, b, d) VALUES (1, 15, 'Hello');
INSERT INTO t (a, b, d) VALUES (2, 25, 'World');
INSERT INTO t (a, b, d) VALUES (3, 35, 'Dolor Sit');

alter table t add column yy varchar(50) not null default 'foo';
desc t;
show create table t;

--ERROR 1048 (23000): Column 'yy' cannot be null
INSERT INTO t (a, b, d, yy) VALUES (4, 45, 'Lorem Ipsum', null);

alter table t add column zz varchar(50) not null;
desc t;
show create table t;

--ERROR 1364 (HY000): Field 'zz' doesn't have a default value
INSERT INTO t (a, b, d, yy) VALUES (5, 55, 'Horem Ypsum', 'zzz');

-----------------------------------------------------------------------------------------------------------------------
CREATE TABLE t1 (
a int NOT NULL PRIMARY KEY,
b int NOT NULL DEFAULT 10,
c int NULL DEFAULT 20,
d varchar(20) not null default 'xxx',
e timestamp DEFAULT NULL,
f timestamp NULL DEFAULT CURRENT_TIMESTAMP
);

desc t1;
show create table t1;

-- 插入数据
INSERT INTO t1 (a, b, d) VALUES (1, 15, 'Hello');
INSERT INTO t1 (a, b, d) VALUES (2, 25, 'World');
INSERT INTO t1 (a, b, d) VALUES (3, 35, 'Dolor Sit');

alter table t1 modify c bigint not null default 100;

-- ERROR 3819 (HY000): constraint violation: Column 'c' cannot be null
INSERT INTO t1 (a, b, d, c) VALUES (4, 45, 'Lorem Ipsum', null);

desc t1;
show create table t1;

-----------------------------------------------------------------------------------------------------------------------
CREATE TABLE t2 (
a int NOT NULL PRIMARY KEY,
b int NOT NULL DEFAULT 10,
c int NULL DEFAULT 20,
d varchar(20) not null default 'xxx',
e timestamp DEFAULT NULL,
f timestamp NULL DEFAULT CURRENT_TIMESTAMP
);

desc t2;
show create table t2;

INSERT INTO t2 (a, b, d) VALUES (1, 15, 'Hello');
INSERT INTO t2 (a, b, d) VALUES (2, 25, 'World');
INSERT INTO t2 (a, b, d) VALUES (3, 35, 'Dolor Sit');

alter table t2 modify c bigint default 100;
desc t2;
show create table t2;

INSERT INTO t2 (a, b, d, c) VALUES (4, 45, 'Lorem Ipsum', null);

-- ERROR 3819 (HY000): constraint violation: Column 'c' cannot be null
alter table t2 modify c bigint not null default 200;
desc t2;
show create table t2;

-----------------------------------------------------------------------------------------------------------------------
create table t3(id int not null primary key, i int, j int, t text);
desc t3;
show create table t3;

-- 插入一些示例数据
INSERT INTO t3 (id, i, j, t) VALUES (1, 10, 20, 'Example text 1');
INSERT INTO t3 (id, i, j, t) VALUES (2, 30, 40, 'Example text 2');
INSERT INTO t3 (id, i, j, t) VALUES (3, NULL, 50, 'Example text 3');

select * from t3 order by id;

alter table t3 add column e enum('Y', 'N') not null default 'Y';
desc t3;
show create table t3;

--ERROR 3819 (HY000): constraint violation: Column 'e' cannot be null
INSERT INTO t3 (id, i, j, t, e) VALUES (4, NULL, 50, 'Example text 4', null);
select * from t3 order by id;

-----------------------------------------------------------------------------------------------------
CREATE TABLE t4 (
id INT PRIMARY KEY,
a VARCHAR(30) NULL default 'foo',
b VARCHAR(30) NOT NULL default 'foo',
c INT NULL DEFAULT 1000,
d INT NOT NULL DEFAULT 2000
);

desc t4;
show create table t4;
-----------------------------------------------------------------------------------------------------
CREATE TABLE t5 (
y datetime NOT NULL DEFAULT '2023-06-21 00:00:00' PRIMARY KEY,
a int DEFAULT NULL,
b char(10) DEFAULT NULL
);

desc t5;
show create table t5;
-----------------------------------------------------------------------------------------------------
create external table extable1(n1 int)infile{"filepath"='$resources/external_table_file/extable.csv'} ;
desc extable1;
show create table extable1;
-----------------------------------------------------------------------------------------------------
drop database if exists db1;