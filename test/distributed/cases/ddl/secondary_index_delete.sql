-- 3.a Drop table
drop table if exists t1;
create table t1(id int PRIMARY KEY,name VARCHAR(255),age int);
create index idx1 on t1(name);
insert into t1 values(1,"Abby", 24);
insert into t1 values(2,"Dora", 25);
show index from t1;
drop table t1;
select name, type, column_name from mo_catalog.mo_indexes mi where name="idx1";

-- 3.b Delete a row
drop table if exists t1;
create table t1(id int PRIMARY KEY,name VARCHAR(255),age int);
create index idx2 on t1(name,age);
insert into t1 values(1,"Abby", 24);
insert into t1 values(2,"Dora", 25);
delete from t1 where id = 1;
show index from t1;
select name, type,column_name from mo_catalog.mo_indexes mi where name="idx2";


-- 3.c Drop index
drop table if exists t1;
create table t1(id int PRIMARY KEY,name VARCHAR(255),age int);
create index idx3 on t1(name);
insert into t1 values(1,"Abby", 24);
insert into t1 values(2,"Dora", 25);
show index from t1;
select name, type,column_name from mo_catalog.mo_indexes mi where name="idx3";
show create table t1;
DROP INDEX idx3 ON t1;
show index from t1;
select name, type,column_name from mo_catalog.mo_indexes mi where name="idx3";
show create table t1;
