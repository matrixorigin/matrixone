-- 2.a Alter Add new column to PK list [Add a new PK column and then remove it later]
drop table if exists t1;
create table t1(id VARCHAR(20) PRIMARY KEY,name VARCHAR(255),age int);
create index idx1 on t1(name);
insert into t1 values("a","Abby", 24);
insert into t1 values("b","Deb", 26);
select * from t1;
show index from t1;
show create table t1;
select name, type, column_name from mo_catalog.mo_indexes mi where name="idx1";
alter table t1 add column id2 VARCHAR(20);
update t1 set id2 = id;
ALTER TABLE t1 DROP PRIMARY KEY, ADD PRIMARY KEY (id, id2);
show index from t1;
show create table t1;
select name, type, column_name from mo_catalog.mo_indexes mi where name="idx1";
insert into t1 values("d","Abby", 24,"d2");
alter table t1 drop column id2;
desc t1;
show index from t1;
show create table t1;
select name, type, column_name from mo_catalog.mo_indexes mi where name="idx1";

-- 2.b Alter Drop SK [Drop the last standing SK]
drop table if exists t1;
create table t1(id int PRIMARY KEY,name VARCHAR(255),age int);
create index idx2 on t1(name);
insert into t1 values(1,"Abby", 24);
insert into t1 values(2,"Dora", 25);
show index from t1;
select name, type, column_name from mo_catalog.mo_indexes mi where name="idx2";
alter table t1 drop column name;
show index from t1;
select name, type, column_name from mo_catalog.mo_indexes mi where name="idx2";


-- 2.c Alter Drop SK [Drop 1 of 2 SKs]
drop table if exists t1;
create table t1(id int PRIMARY KEY,name VARCHAR(255),age int);
create index idx3 on t1(name,age);
insert into t1 values(1,"Abby", 24);
insert into t1 values(2,"Dora", 25);
show index from t1;
select name, type, column_name from mo_catalog.mo_indexes mi where name="idx3";
alter table t1 drop column name;
show index from t1;
select name, type, column_name from mo_catalog.mo_indexes mi where name="idx3";



-- 2.d Alter Drop user defined PK column, resulting in falling back to __mo_fake_pk
drop table if exists t1;
create table t1(id VARCHAR(255) PRIMARY KEY,name VARCHAR(255),age int);
create index idx4 on t1(id,name);
insert into t1 values("a","Abby", 24);
insert into t1 values("b","Bob", 25);
insert into t1 values("c","Carol", 23);
select * from t1;
show index from t1;
select name, type, column_name from mo_catalog.mo_indexes mi where name="idx4";
alter table t1 drop column id;
show index from t1;
select name, type, column_name from mo_catalog.mo_indexes mi where name="idx4";
insert into t1 values("Dora", 29);
alter table t1 drop column name;
show index from t1;
select name, type, column_name from mo_catalog.mo_indexes mi where name="idx4";
insert into t1 values(29);