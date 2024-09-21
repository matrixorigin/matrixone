-- 1.c Create Secondary Index before table population
drop table if exists t1;
create table t1(id int PRIMARY KEY,name VARCHAR(255),age int);
create index idx1 on t1(name);
insert into t1 values(1,"Abby", 24);
insert into t1 values(2,"Bob", 25);
select * from t1;
show index from t1;
show create table t1;
select name, type, column_name from mo_catalog.mo_indexes mi where name="idx1";
-- select a,b, a=b, id from (select serial_full(name,id) as a,id from t1) as v1 inner join (select __mo_index_idx_col as b, __mo_index_pri_col from `__mo_index_secondary_d4ea54e0-6c04-11ee-a57e-723e89f7b972`) as v2 on v1.id = v2.__mo_index_pri_col;


-- 1.a Create Secondary Index after table population [Without User defined PK in SK column list]
drop table if exists t1;
create table t1(id int PRIMARY KEY,name VARCHAR(255),age int);
insert into t1 values(1,"Abby", 24);
insert into t1 values(2,"Bob", 25);
create index idx2 on t1(name);
insert into t1 values(3,"Dora", 30);
select * from t1;
show index from t1;
show create table t1;
select name, type, column_name from mo_catalog.mo_indexes mi where name="idx2";


-- 1.c Create Secondary Index containing the PK column [With user defined PK in SK column list]
drop table if exists t1;
create table t1(id int PRIMARY KEY,name VARCHAR(255),age int);
create index idx3 on t1(id,name);
insert into t1 values(1,"Abby", 24);
insert into t1 values(2,"Bob", 25);
select * from t1;
show index from t1;
show create table t1;
select name, type, column_name from mo_catalog.mo_indexes mi where name="idx3";


-- 1.d Create Secondary Index by using "alter table t1 add key/index"
drop table if exists t1;
create table t1(id VARCHAR(255) PRIMARY KEY,name VARCHAR(255),age int);
insert into t1 values("a","Abby", 24);
insert into t1 values("b","Bob", 25);
alter table t1 add key idx4 (name);
insert into t1 values("c","Danny", 26);
select * from t1;
show index from t1;
show create table t1;
select name, type, column_name from mo_catalog.mo_indexes mi where name="idx4";


-- 1.e Create Table syntax
drop table if exists t1;
create table t1(id VARCHAR(255) PRIMARY KEY,name VARCHAR(255),age int, index idx5(name));
insert into t1 values("a","Abby", 24);
insert into t1 values("b","Bob", 25);
insert into t1 values("c","Carol", 23);
select * from t1;
show index from t1;
show create table t1;
select name, type, column_name from mo_catalog.mo_indexes mi where name="idx5";

-- 1.f Create Secondary Index on PK alone. ie SK = PK.
drop table if exists t1;
create table t1(a double primary key, b int);
insert into t1 values(1.5,100);
create index idx6 on t1(a); -- sk = pk
insert into t1 values(2.6,200);
select * from t1;
show index from t1;
show create table t1;
select name, type, column_name from mo_catalog.mo_indexes mi where name="idx6";

-- 1.g Create Secondary Index with "using BTREE"
drop table if exists t1;
create table t1(id int PRIMARY KEY,name VARCHAR(255),age int);
create index idx7 using BTREE on t1(name);
insert into t1 values(1,"Abby", 24);
show index from t1;
show create table t1;
select name, type, column_name from mo_catalog.mo_indexes mi where name="idx7";