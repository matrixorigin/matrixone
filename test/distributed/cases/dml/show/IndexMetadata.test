-- @suit
-- @case
-- @desc:index meta data table
-- @label:bvt
DROP DATABASE IF EXISTS test;
CREATE DATABASE test;
USE test;

-- primary key
drop table if exists T1;
create table t1(id int PRIMARY KEY,name VARCHAR(255),age int);
insert into t1 values(1,"Abby", 24);
insert into t1 values(2,"Bob", 25);
insert into t1 values(3,"Carol", 23);
insert into t1 values(4,"Dora", 29);
create unique index Idx on t1(name);
select * from t1;
show index from t1;
drop table t1;

-- unique index
drop table if exists t2;
create table t2 (
col1 bigint primary key,
col2 varchar(25),
col3 float,
col4 varchar(50)
);
create unique index Idx on t2(col2) comment 'create varchar index';
insert into t2 values(1,"Abby", 24,'zbcvdf');
insert into t2 values(2,"Bob", 25,'zbcvdf');
insert into t2 values(3,"Carol", 23,'zbcvdf');
insert into t2 values(4,"Dora", 29,'zbcvdf');
select * from t2;
show index from t2;
drop table t2;

-- unique key and unique index, alter index
drop table if exists t3;
create table t3(a int, b int, unique key(a) comment 'a');
create unique index x ON t3(a) comment 'x';
create index xx ON t3(a) comment 'xx';
show create table t3;
show index from t3;
alter table t3 drop index xx;
show index from t3;
drop table t3;

-- unique index: multicolumn
drop table if exists t4;
create table t4 (
col1 bigint primary key,
col2 varchar(25),
col3 float,
col4 varchar(50)
);
create unique index idx on t4(col2,col3);
insert into t4 values(1,"Abby", 24,'zbcvdf');
insert into t4 values(2,"Bob", 25,'zbcvdf');
insert into t4 values(3,"Carol", 23,'zbcvdf');
insert into t4 values(4,"Dora", 29,'zbcvdf');
select * from t4;
show index from t4;
drop table t4;

-- unique key
drop table if exists t5;
create table t5(a int, b int, unique key(a) comment 'a');
show index from t5;
drop table t5;

-- secondary index
drop table if exists t6;
create table t6(a int, b int, unique key(a));
create index b on t6(b);
show index from t6;
drop index b on t6;
show index from t6;
drop table t6;

drop table if exists t7;
create table t7(a int, b int);
create index x ON t7(a) comment 'x';
show index from t7;
drop table t7;

-- key
drop table if exists t8;
create table t8(a int, b int, key(a) comment 'a');
show index from t8;
drop table t8;

-- unique index on multicolumns
drop table if exists t9;
create table t9(
col1 int unsigned,
col2 varchar(15),
col3 varchar(10),
col4 int unsigned,
col5 date,
col6 decimal(7,2),
col7 decimal(7,2),
col8 int unsigned,
unique index(col1,col2,col3,col6)
);
INSERT INTO t9 VALUES (7369,'SMITH','CLERK',7902,'1980-12-17',800,NULL,20);
INSERT INTO t9 VALUES (7934,'MILLER','CLERK',7782,'1982-01-23',1300,NULL,10);

create unique index idx_1 on t9(col1,col2,col3,col6);
select * from t9;
show index from t9;
alter table t9 drop index idx_1;
show index from t9;
drop table t9;

-- secondary index:multicolumns
drop table if exists t10;
create table t10(a int,b binary,c char,d varchar(20));
create index index01 on t10(a,b,C);
show index from t10;
drop table t10;

-- uppercase and lowercase
drop table if exists t11;
create table t11(col1 int not null, col2 varchar(100), col3 bigint);
create index ABc on t11(COL1);
show index from t11;
alter table t11 drop index ABc;
show index from t11;
drop table t11;

DROP DATABASE test;
