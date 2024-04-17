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

1. unre
-- create table
DROP TABLE IF EXISTS `projects`;
CREATE TABLE `projects` (
                            `project_id` VARCHAR(64) NOT NULL,
                            `company_id` VARCHAR(64) NOT NULL,
                            `project_no` VARCHAR(255) DEFAULT null,
                            `project_name` VARCHAR(255) NOT NULL,
                            `project_type` VARCHAR(255) NOT NULL,
                            `project_unit` VARCHAR(255) DEFAULT null,
                            `project_leader` VARCHAR(255) NOT NULL,
                            `address` VARCHAR(255) DEFAULT null,
                            `number_of_building` INT DEFAULT null,
                            `project_state` VARCHAR(64) DEFAULT null,
                            `supervisor` VARCHAR(255) DEFAULT null,
                            `build_license` VARCHAR(64) DEFAULT null,
                            `ceiling_normal_plane_distance` VARCHAR(32) DEFAULT null COMMENT '顶板距法面距离',
                            `floor_normal_plane_distance` VARCHAR(32) DEFAULT null COMMENT '底板距法面距离',
                            `ext1` VARCHAR(1024) DEFAULT null COMMENT '扩展字段',
                            `ext2` VARCHAR(1024) DEFAULT null COMMENT '扩展字段',
                            `ext3` VARCHAR(1024) DEFAULT null COMMENT '扩展字段',
                            `sub_company_id` VARCHAR(64) DEFAULT null,
                            `parent_project_id` VARCHAR(64) DEFAULT null,
                            `distinguish_suite` INT DEFAULT '1' COMMENT '是否分户(0,否 1，是)',
                            `draw_switch` INT DEFAULT null,
                            `version` VARCHAR(64) DEFAULT null,
                            `created_by` VARCHAR(64) DEFAULT null,
                            `created_time` DATETIME DEFAULT null,
                            `updated_by` VARCHAR(64) DEFAULT null,
                            `updated_time` DATETIME DEFAULT null,
                            `is_deleted` INT DEFAULT null,
                            `deleted_by` VARCHAR(64) DEFAULT null,
                            `deleted_time` DATETIME DEFAULT null,
                            PRIMARY KEY (`project_id`),
                            UNIQUE KEY `idx_projects_project_id` (`project_id`),
                            KEY `idx_company_id` (`company_id`),
                            KEY `idx_project_state` (`project_state`),
                            KEY `idx_sub_company_id` (`sub_company_id`)
);

-- check table definition
desc `projects`;

-- change table definition
ALTER TABLE `projects`
    MODIFY COLUMN `build_license` varchar (64) NULL DEFAULT NULL AFTER `supervisor`,
    MODIFY COLUMN `created_by` varchar(64) NULL DEFAULT NULL AFTER `distinguish_suite`,
    MODIFY COLUMN `created_time` datetime (0) NULL DEFAULT NULL AFTER `created_by`,
    MODIFY COLUMN `is_deleted` int (11) NULL DEFAULT NULL AFTER `updated_time`,
    MODIFY COLUMN `deleted_by` varchar (64) NULL DEFAULT NULL AFTER `is_deleted`,
    MODIFY COLUMN `deleted_time` datetime(0) NULL DEFAULT NULL AFTER `deleted_by`,
    ADD COLUMN `draw_switch` int(11) NULL AFTER `distinguish_suite`,
    ADD COLUMN `version` varchar (64) NULL AFTER `draw_switch`;

-- change table definition
ALTER TABLE `projects`
    MODIFY COLUMN `build_license` varchar (64) NULL DEFAULT NULL AFTER `supervisor`,
    MODIFY COLUMN `created_by` varchar(64) NULL DEFAULT NULL AFTER `distinguish_suite`,
    MODIFY COLUMN `created_time` datetime (0) NULL DEFAULT NULL AFTER `created_by`,
    MODIFY COLUMN `is_deleted` int (11) NULL DEFAULT NULL AFTER `updated_time`,
    MODIFY COLUMN `deleted_by` varchar (64) NULL DEFAULT NULL AFTER `is_deleted`,
    MODIFY COLUMN `deleted_time` datetime(0) NULL DEFAULT NULL AFTER `deleted_by`,
    ADD COLUMN `draw_switch2` int(11) NULL AFTER `distinguish_suite`,
    ADD COLUMN `version2` varchar (64) NULL AFTER `draw_switch`;
-- check table definition
desc `projects`;

drop database if exists mysql_ddl_test_db;

create database  mysql_ddl_test_db;
 use mysql_ddl_test_db;
CREATE TABLE table_basic_for_alter_100m (
                                            col1 TINYINT DEFAULT NULL,
                                            col2 SMALLINT DEFAULT NULL,
                                            col3 INT NOT NULL,
                                            col4 BIGINT NOT NULL,
                                            col5 TINYINT UNSIGNED DEFAULT NULL,
                                            col6 SMALLINT UNSIGNED DEFAULT NULL,
                                            col7 INT UNSIGNED DEFAULT NULL,
                                            col8 BIGINT UNSIGNED DEFAULT NULL,
                                            col9 FLOAT DEFAULT NULL,
                                            col10 DOUBLE DEFAULT NULL,
                                            col11 VARCHAR(255) DEFAULT NULL,
                                            col12 DATE DEFAULT NULL,
                                            col13 DATETIME DEFAULT NULL,
                                            col14 TIMESTAMP DEFAULT NULL,
                                            col15 BOOL DEFAULT NULL,
                                            col16 DECIMAL(16,6) DEFAULT NULL,
                                            col17 TEXT DEFAULT NULL,
                                            col18 JSON DEFAULT NULL,
                                            col19 BLOB DEFAULT NULL,
                                            col20 BINARY(255) DEFAULT NULL,
                                            col21 VARBINARY(255) DEFAULT NULL,
                                            col22 VECF32(3) DEFAULT NULL,
                                            col23 VECF32(3) DEFAULT NULL,
                                            col24 VECF64(3) DEFAULT NULL,
                                            col25 VECF64(3) DEFAULT NULL,
                                            KEY col3_col4 (col3,col4),
                                            UNIQUE KEY col4 (col4)
) ;
desc table_basic_for_alter_100m;

CREATE TABLE user_table (
                            id INT(11),
                            name VARCHAR(50),
                            UNIQUE KEY (id)
);
desc user_table;

CREATE TABLE users (
                       id INT(11) NOT NULL,
                       name VARCHAR(50),
                       UNIQUE KEY (id)
);
desc users;

 drop database if exists mysql_ddl_test_db;

drop database if exists db1;
create database db1;
use db1;
drop table if exists t1;
CREATE TABLE t1(
                   a INTEGER,
                   b CHAR(10),
                   c date,
                   d decimal(7,2),
                   UNIQUE KEY(a, b)
);
desc t1;

drop table if exists t2;
CREATE TABLE t2(
                   col1 TINYINT DEFAULT NULL,
                   col2 SMALLINT DEFAULT NULL,
                   col3 INT NOT NULL,
                   col4 BIGINT NOT NULL,
                   KEY (col3,col4),
                   UNIQUE KEY (col4)
) ;
desc t2;

drop table if exists t3;
CREATE TABLE t3(
                   a INTEGER,
                   b CHAR(10),
                   c date,
                   d decimal(7,2),
                   KEY (c, d),
                   UNIQUE KEY (d)
);
desc t3;

drop table if exists t4;
CREATE TABLE t4(
                   a INTEGER,
                   b CHAR(10),
                   c date,
                   d decimal(7,2),
                   KEY (a, b),
                   UNIQUE KEY (a)
);
desc t4;

drop database if exists db1;