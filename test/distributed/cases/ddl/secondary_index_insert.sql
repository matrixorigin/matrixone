-- 4.a Update PK and SK column (with non nulls)
drop table if exists t1;
create table t1(id int PRIMARY KEY,name VARCHAR(255),age int);
create index idx1 on t1(name);
insert into t1 values(1,"Abby", 24);
insert into t1 values(2,"Deb", 26);
select * from t1;
update t1 set name = "Dora" where id = 2;
select * from t1;
update t1 set id=3 where id=2;
update t1 set name = "Abby" where id = 3;
select * from t1;
show index from t1;
show create table t1;
select name, type, column_name from mo_catalog.mo_indexes mi where name="idx1";

-- 4.b Insert duplicate
drop table if exists t1;
create table t1(id int PRIMARY KEY,name VARCHAR(255),age int);
create index idx2 on t1(name);
insert into t1 values(1,"Abby", 24);
insert into t1 values(2,"Abby", 26);
select * from t1;

-- 4.c Update to Duplicate
drop table if exists t1;
create table t1(id int PRIMARY KEY,name VARCHAR(255),age int);
create index idx3 on t1(name);
insert into t1 values(1,"Abby", 24);
insert into t1 values(2,"Dora", 25);
update t1 set name = "Abby" where id = 2;
select * from t1;

-- 4.d Update to Null
drop table if exists t1;
create table t1(id int PRIMARY KEY,name VARCHAR(255),age int);
create index idx4 on t1(name);
insert into t1 values(1,"Abby", 24);
insert into t1 values(2,"Dora", 25);
update t1 set name = null where id = 2;
select * from t1;

-- 4.e Insert Null
drop table if exists t1;
create table t1(id int PRIMARY KEY,name VARCHAR(255),age int);
create index idx5 on t1(name);
insert into t1 values(1,"Abby", 24);
insert into t1 (id, age) values(2, 25);
select * from t1;

-- 4.f Update to Null and Then Update to Non Null and Then Delete
drop table if exists t1;
create table t1(id VARCHAR(255) PRIMARY KEY,name VARCHAR(255),age VARCHAR(255));
create  index idx6 on t1(name);
insert into t1 values("a","Abby", "twenty four");
insert into t1 values("b","Debby", "twenty six");
select * from t1;
update t1 set name = null where id = "b";
select * from t1;
update t1 set name = "Cia" where id = "b";
select * from t1;
update t1 set name = null where id = "b";
select * from t1;
delete from t1 where id = "b";
select * from t1;