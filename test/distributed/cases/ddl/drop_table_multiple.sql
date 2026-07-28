drop database if exists drop_multi;
create database drop_multi;
use drop_multi;

drop table if exists t1,t2,t3;
create table t1(a int);
create table t2(a int);
create table t3(a int);
show tables;

drop table if exists t1,t2,t3;
show tables;

create table t1(a int);
create table t2(a int);
show tables;

drop table if exists drop_multi.t1, t2;
show tables;

-- duplicate table names
create table t1(a int);
show tables;
drop table if exists t1, t1;
show tables;

-- no IF EXISTS, expect error and t1 not dropped
create table t1(a int);
drop table t1, no_such_table;
show tables;
drop table t1;

-- view should be skipped
create table t1(a int);
create view v1 as select * from t1;
drop table if exists v1, t1;
select table_name, table_type from information_schema.tables where table_schema='drop_multi' order by table_name, table_type;
drop view v1;

-- sequence should be skipped
create sequence s1;
create table t1(a int);
drop table if exists s1, t1;
show sequences where names in('s1');
drop sequence s1;

-- 1.1 Basic Table Deletion
drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
create table t1(a int);
create table t2(a int);
create table t3(a int);
drop table t1, t2, t3;
-- Clean up (to prevent any remaining failures from above)
drop table if exists t1;
drop table if exists t2;
drop table if exists t3;

-- 1.2 Deletion of Multiple Tables with IF EXISTS (Some Tables Do Not Exist)
create table t1(a int);
drop table if exists t1, t2, t3;
drop table if exists t1;

-- 1.3 Delete 2 tables
create table ta(a int);
create table tb(b int);
drop table ta, tb;
drop table if exists ta;
drop table if exists tb;

-- 1.4 Delete a large number of tables (10 tables)
create table m1(a int);
create table m2(a int);
create table m3(a int);
create table m4(a int);
create table m5(a int);
create table m6(a int);
create table m7(a int);
create table m8(a int);
create table m9(a int);
create table m10(a int);
drop table m1, m2, m3, m4, m5, m6, m7, m8, m9, m10;
drop table if exists m1;
drop table if exists m2;
drop table if exists m3;
drop table if exists m4;
drop table if exists m5;
drop table if exists m6;
drop table if exists m7;
drop table if exists m8;
drop table if exists m9;
drop table if exists m10;

-- 1.5 Deleting Multiple Tables - Tables Contain Data
create table d1(a int, b varchar(100));
create table d2(x int, y datetime);
insert into d1 values (1, 'hello'), (2, 'world');
insert into d2 values (100, '2025-01-01 00:00:00');
select * from d1;
select * from d2;
drop table d1, d2;
drop table if exists d1;
drop table if exists d2;

-- 1.6 Deleting Multiple Tables - Tables have various constraints
create table c1(
    id int primary key auto_increment,
    name varchar(50) not null unique,
    age int default 0
);
create table c2(
    id int primary key,
    val decimal(10,2) not null
);
insert into c1(name, age) values ('alice', 25);
insert into c2 values (1, 99.99);
drop table c1, c2;
drop table if exists c1;
drop table if exists c2;

-- 1.7 Deleting Multiple Tables - Covers Tables of All Data Types
create table type_int(
    a tinyint, b smallint, c int, d bigint,
    e tinyint unsigned, f smallint unsigned, g int unsigned, h bigint unsigned
);
create table type_float(a float, b double, c decimal(20,10));
create table type_str(a char(10), b varchar(255), c text, d tinytext, e mediumtext, f longtext);
create table type_time(a date, b datetime, c timestamp, d time);
create table type_json(a json);
create table type_blob(a binary(10), b varbinary(255), c blob, d tinyblob, e mediumblob, f longblob);
insert into type_int values (127, 32767, 2147483647, 9223372036854775807, 255, 65535, 4294967295, 18446744073709551615);
insert into type_float values (3.14, 2.718281828, 12345678.1234567890);
insert into type_str values ('hello', 'world', 'text_val', 'tiny', 'medium', 'long');
insert into type_time values ('2025-12-31', '2025-12-31 23:59:59', '2025-12-31 23:59:59', '23:59:59');
insert into type_json values ('{"key": "value", "num": 123}');
insert into type_blob values (x'48454C4C4F', x'574F524C44', x'424C4F42', x'54494E59', x'4D454449554D', x'4C4F4E47');
drop table type_int, type_float, type_str, type_time, type_json, type_blob;
drop table if exists type_int;
drop table if exists type_float;
drop table if exists type_str;
drop table if exists type_time;
drop table if exists type_json;
drop table if exists type_blob;

-- 1.8 Multiple Table Deletion - Without IF EXISTS, an error should be reported if the table does not exist
create table err_t1(a int);
drop table if exists err_t1;

-- 1.9 Deleting Multiple Tables - Across Databases
drop database if exists drop_test_db;
create database drop_test_db;
use drop_test_db;
create table x1(a int);
create table x2(a int);
drop table x1, x2;
drop table if exists x1;
drop table if exists x2;
drop database if exists drop_test_db;
-- Return to the test database
use drop_multi;

-- 1.10 Deletion of Multiple Tables - Foreign Key Constraints
drop table if exists child_fk;
drop table if exists parent_fk;
create table parent_fk(id int primary key);
create table child_fk(id int primary key, pid int, foreign key(pid) references parent_fk(id));
insert into parent_fk values (1), (2);
insert into child_fk values (1, 1), (2, 2);
-- Delete the sub-tables first, then delete the parent table
drop table child_fk;
drop table parent_fk;
drop table if exists child_fk;
drop table if exists parent_fk;

drop database drop_multi;
