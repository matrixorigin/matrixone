-- testcase_2. 
-- ENGINE=xxx (InnoDB, MyISAM, ...)
-- DEFAULT CHARSET=xxx (latin1, utf8, ...) COLLATE=utf8mb4_general_ci 
-- CHARACTER SET xxx (utf8, ...) COLLATE utf8mb4_general_ci
-- USING xxx (BTREE, HASH, ...)
-- UNLOCK TABLES and LOCK TABLES
-- AUTO_INCREMENT=xxx, AUTO_INCREMENT xxx
-- ROW_FORMAT=xxx (COMPACT)
-- Combination

create database if not exists mysql_ddl_test_db;
use mysql_ddl_test_db;

create table if not exists mmysql_ddl_test_t21(id int, name varchar(255)) engine = 'InnoDB';
show create table mmysql_ddl_test_t21;

create table if not exists mmysql_ddl_test_t22(id int, name varchar(255)) DEFAULT CHARSET=utf8 COLLATE = utf8mb4_general_ci ;
show create table mmysql_ddl_test_t22;

create table if not exists mmysql_ddl_test_t23(id int, name varchar(255)) DEFAULT CHARSET = utf8;
show create table mmysql_ddl_test_t23;

create table if not exists mmysql_ddl_test_t24(id int, name varchar(255)) DEFAULT CHARSET= utf8;
show create table mmysql_ddl_test_t24;

create table if not exists mmysql_ddl_test_t25(id int, name varchar(255)) DEFAULT CHARSET =utf8;
show create table mmysql_ddl_test_t25;

create table if not exists mmysql_ddl_test_t26(id int, name varchar(255)) DEFAULT CHARSET     =       utf8 COLLATE=utf8mb4_general_ci ;
show create table mmysql_ddl_test_t26;

create table if not exists mmysql_ddl_test_t27(id int, name varchar(255) CHARACTER SET utf8 COLLATE utf8mb4_general_ci);
show create table mmysql_ddl_test_t27;

create table if not exists mmysql_ddl_test_t28(id int, name varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci);
show create table mmysql_ddl_test_t28;

create table if not exists mmysql_ddl_test_t29(id int, name varchar(255),primary key (id)  USING BTREE);
show create table mmysql_ddl_test_t29;

create table mmysql_ddl_test_t210 (id int);
LOCK TABLES t10 WRITE;
alter table mmysql_ddl_test_t210 add column class varchar(50);
UNLOCK TABLES;
show create table mmysql_ddl_test_t210;

create table mmysql_ddl_test_t211 (id int AUTO_INCREMENT);
show create table mmysql_ddl_test_t211;

create table mmysql_ddl_test_t212 (id int) AUTO_INCREMENT = 157472;
show create table mmysql_ddl_test_t212;

create table mmysql_ddl_test_t213 (id int AUTO_INCREMENT) AUTO_INCREMENT = 157472;
show create table mmysql_ddl_test_t213;

create table mmysql_ddl_test_t214 (id int) ROW_FORMAT=DYNAMIC;
show create table mmysql_ddl_test_t214;

create table mmysql_ddl_test_t215 (id int) ROW_FORMAT = COMPACT;
show create table mmysql_ddl_test_t215;

create table if not exists mmysql_ddl_test_t216(id int AUTO_INCREMENT, name varchar(255) CHARACTER SET utf8 COLLATE utf8mb4_general_ci,primary key (id)  USING BTREE) AUTO_INCREMENT=123 engine = 'InnoDB' DEFAULT CHARSET=utf8 COLLATE = utf8mb4_general_ci ROW_FORMAT = COMPACT;
show create table mmysql_ddl_test_t216;
