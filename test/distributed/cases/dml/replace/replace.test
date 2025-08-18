-- @suite

-- @case
-- @desc:test for replace data
-- @label:bvt
drop table if exists names;
create table names(id int PRIMARY KEY,name VARCHAR(255),age int);
replace into names(id, name, age) values(1,"Abby", 24);
select name, age from names where id = 1;
replace into names(id, name, age) values(1,"Bobby", 25);
select name, age from names where id = 1;
replace into names set id = 2, name = "Ciro";
select name, age from names where id = 2;
replace into names set id = 2, name = "Ciro", age = 17;
select name, age from names where id = 2;
REPLACE INTO names values (2, "Bob", 19);
select name, age from names where id = 2;
/* comment */ replace into names set id = 2, name = "Dylan";
select name, age from names where id = 2;
-- table without keys
drop table if exists t1;
create table t1(a int, b int, c int);
insert into t1 values (1,1,1), (2,2,2), (3,3,3);
replace t1 values (1,2,3), (2,3,4);
select a, b, c from t1;
create table t4 (a int unique key, b varchar(64));
replace into t4 values (1, 'a');
select * from t4;
replace into t4 values (1, 'b');
select * from t4;

drop table if exists t1;
create table t1(a int primary key, b int unique, c varchar(255), key(c));
insert into t1 values (1,1,"1"), (2,2,"2");
select * from t1;
replace into t1 values (1,4,"4"), (3,3,"3");
select * from t1;

create database replace_db;
use replace_db;
replace into `replace`.`names` values (2, "Dylan", 20);
select name, age from `replace`.`names` where id = 2;

drop table if exists c;
create table c(a int primary key , b int, v vecf32(3));
replace into c values(1,1,'[1,2,3]');
select * from c;
drop table c;
drop table if exists c;
create table c(a int primary key , b int, v vecf32(3));
replace into c values(1,1,'[1,2,3]');
drop table if exists f;
create table f (a int primary key, b int, c int, v vecf32(3));
replace into f with c1 as (select * from c), src as (select * from (values row(1,1, cast('[3,4,5]' as vecf32(3))))) select a, column_0, column_1, column_2 from c1,src where c1.a = src.column_0;
drop table f;
drop table c;

set experimental_ivf_index = 1;
create table bug_22340 (a int not null auto_increment, a1 int default null, b vecf64(3) DEFAULT NULL COMMENT 'document向量化信息', PRIMARY KEY (`a`), KEY `idx_vec` USING ivfflat (`b`) lists = 1 op_type 'vector_l2_ops');
REPLACE INTO bug_22340 VALUES (1,100,'[1, 2, 3]'),(2,100,'[1, 2, 3]'),(3,100,'[1, 2, 3]'),(4,100,'[1, 2, 3]'),(5,100,'[1, 2, 3]'),(6,100,'[1, 2, 3]'),(7,100,'[1, 2, 3]'),(8,100,'[1, 2, 3]'),(9,100,'[1, 2, 3]'),(10,100,'[1, 2, 3]');
-- @separator:table
select mo_ctl('dn','flush','replace_db.bug_22340');
REPLACE INTO bug_22340 VALUES (1,100,'[1, 2, 3]'),(2,100,'[1, 2, 3]'),(3,100,'[1, 2, 3]'),(4,100,'[1, 2, 3]'),(5,100,'[1, 2, 3]'),(6,100,'[1, 2, 3]'),(7,100,'[1, 2, 3]'),(8,100,'[1, 2, 3]'),(9,100,'[1, 2, 3]'),(10,100,'[1, 2, 3]');
drop table bug_22340;
set experimental_ivf_index = 0;

drop database replace_db;
