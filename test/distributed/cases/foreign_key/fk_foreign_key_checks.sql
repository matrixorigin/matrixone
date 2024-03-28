drop database if exists fk_foreign_key_checks;
create database fk_foreign_key_checks;
use fk_foreign_key_checks;

SET FOREIGN_KEY_CHECKS=0;

-- no error
-- fk forward reference
drop table if exists t1;
create table t1( b int, constraint `c1` foreign key `fk1` (b) references t2(a));

drop table if exists t2;
create table t2(a int primary key,b int);

-- no error
insert into t2 values (1,1),(2,2),(3,3);
insert into t1 values (1),(2),(3);

SET FOREIGN_KEY_CHECKS=1;

-- error. 1 is referred
delete from t2 where a = 1;

-- error. 4 does not exist in the t2
insert into t1 values (4);

-- no error
insert into t2 values (4,4),(5,5);

-- no error
insert into t1 values (4);

--error
delete from t2 where a = 4;

--no error
delete from t2 where a = 5;

--no error
drop table if exists t3;
create table t3( b int, constraint `c1` foreign key `fk1` (b) references t2(a));

--no error
insert into t3 values (1),(2),(3),(4);

--error
insert into t3 values (5);

--error
delete from t2 where a = 3;

--error
drop table t2;

SET FOREIGN_KEY_CHECKS=0;

--no error
insert into t1 values (5);
insert into t3 values (5);

--no error
delete from t2 where a = 3;

-- no error
drop table t2;

--no error
insert into t1 values (6);
insert into t3 values (6);

SET FOREIGN_KEY_CHECKS=1;

--no error
insert into t1 values (7);
insert into t3 values (7);

create table t2(a int primary key,b int);

--error
insert into t1 values (8);
insert into t3 values (8);

insert into t2 values (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8);

--no error
insert into t1 values (8);
insert into t3 values (8);

--error
insert into t1 values (9);
insert into t3 values (9);

--error
delete from t2 where a = 3;

--no error
delete from t1 where b = 3;
delete from t3 where b = 3;

--no error
delete from t2 where a = 3;

--error
drop table t2;

--no error
drop table t1;

--error. t3 also refers to t2
drop table t2;

--error
insert into t3 values (9);

--error
insert into t3 values (3);

--no error
insert into t2 values (3,3);

--error
insert into t3 values (9);

--no error
insert into t3 values (3);

--error
drop table t2;

SET FOREIGN_KEY_CHECKS=0;

--no error
drop table t2;

--no error
insert into t3 values (9);

SET FOREIGN_KEY_CHECKS=1;

create table t2(a int primary key,b int);

--error
insert into t3 values (10);

--no error
insert into t2 values (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9);

--no error
insert into t2 values (10,10);

--no error
insert into t3 values (10);

--error
delete from t2 where a = 10;

--error
drop table t2;

drop table t3;

--no error
delete from t2 where a = 10;

--no error
drop table t2;

SET FOREIGN_KEY_CHECKS=0;

create table t2(a int primary key,b int);

SET FOREIGN_KEY_CHECKS=1;

insert into t2 values (1,1),(2,2);

--no error
delete from t2 where a = 2;


drop table if exists t1;
drop table if exists t3;
drop table if exists t2;
drop database if exists fk_foreign_key_checks;

create database if not exists qrtz_fk_testdb;
use qrtz_fk_testdb;
set FOREIGN_KEY_CHECKS=0;

DROP TABLE IF EXISTS `qrtz_simprop_triggers`;
CREATE TABLE `qrtz_simprop_triggers`
(
    `sched_name`    VARCHAR(120) NOT NULL,
    `trigger_name`  VARCHAR(190) NOT NULL,
    `trigger_group` VARCHAR(190) NOT NULL,
    `str_prop_1`    VARCHAR(512)   DEFAULT null,
    `str_prop_2`    VARCHAR(512)   DEFAULT null,
    `str_prop_3`    VARCHAR(512)   DEFAULT null,
    `int_prop_1`    INT            DEFAULT null,
    `int_prop_2`    INT            DEFAULT null,
    `long_prop_1`   BIGINT         DEFAULT null,
    `long_prop_2`   BIGINT         DEFAULT null,
    `dec_prop_1`    DECIMAL(13, 4) DEFAULT null,
    `dec_prop_2`    DECIMAL(13, 4) DEFAULT null,
    `bool_prop_1`   VARCHAR(1)     DEFAULT null,
    `bool_prop_2`   VARCHAR(1)     DEFAULT null,
    PRIMARY KEY (`sched_name`, `trigger_name`, `trigger_group`),
    CONSTRAINT `qrtz_simprop_triggers_ibfk_1` FOREIGN KEY (`sched_name`, `trigger_name`, `trigger_group`) REFERENCES `qrtz_triggers` (`sched_name`, `trigger_name`, `trigger_group`) ON DELETE RESTRICT ON UPDATE RESTRICT
);

DROP TABLE IF EXISTS `qrtz_simple_triggers`;
CREATE TABLE `qrtz_simple_triggers`
(
    `sched_name`      VARCHAR(120) NOT NULL,
    `trigger_name`    VARCHAR(190) NOT NULL,
    `trigger_group`   VARCHAR(190) NOT NULL,
    `repeat_count`    BIGINT       NOT NULL,
    `repeat_interval` BIGINT       NOT NULL,
    `times_triggered` BIGINT       NOT NULL,
    PRIMARY KEY (`sched_name`, `trigger_name`, `trigger_group`),
    CONSTRAINT `qrtz_simple_triggers_ibfk_1` FOREIGN KEY (`sched_name`, `trigger_name`, `trigger_group`) REFERENCES `qrtz_triggers` (`sched_name`, `trigger_name`, `trigger_group`) ON DELETE RESTRICT ON UPDATE RESTRICT
);


DROP TABLE IF EXISTS `qrtz_blob_triggers`;
CREATE TABLE `qrtz_blob_triggers`
(
    `sched_name`    VARCHAR(120) NOT NULL,
    `trigger_name`  VARCHAR(190) NOT NULL,
    `trigger_group` VARCHAR(190) NOT NULL,
    `blob_data`     BLOB DEFAULT NULL,
    PRIMARY KEY (`sched_name`, `trigger_name`, `trigger_group`),
    KEY             `sched_name` (`sched_name`,`trigger_name`,`trigger_group`),
    CONSTRAINT `qrtz_blob_triggers_ibfk_1` FOREIGN KEY (`sched_name`, `trigger_name`, `trigger_group`) REFERENCES `qrtz_triggers` (`sched_name`, `trigger_name`, `trigger_group`) ON DELETE RESTRICT ON UPDATE RESTRICT
);

DROP TABLE IF EXISTS `qrtz_triggers`;
CREATE TABLE `qrtz_triggers`
(
    `sched_name`     VARCHAR(120) NOT NULL,
    `trigger_name`   VARCHAR(190) NOT NULL,
    `trigger_group`  VARCHAR(190) NOT NULL,
    `job_name`       VARCHAR(190) NOT NULL,
    `job_group`      VARCHAR(190) NOT NULL,
    `description`    VARCHAR(250) DEFAULT null,
    `next_fire_time` BIGINT       DEFAULT null,
    `prev_fire_time` BIGINT       DEFAULT null,
    `priority`       INT          DEFAULT null,
    `trigger_state`  VARCHAR(16)  NOT NULL,
    `trigger_type`   VARCHAR(8)   NOT NULL,
    `start_time`     BIGINT       NOT NULL,
    `end_time`       BIGINT       DEFAULT null,
    `calendar_name`  VARCHAR(190) DEFAULT null,
    `misfire_instr`  SMALLINT     DEFAULT null,
    `job_data`       BLOB         DEFAULT NULL,
    PRIMARY KEY (`sched_name`, `trigger_name`, `trigger_group`),
    KEY              `idx_qrtz_t_j` (`sched_name`,`job_name`,`job_group`),
    KEY              `idx_qrtz_t_jg` (`sched_name`,`job_group`),
    KEY              `idx_qrtz_t_c` (`sched_name`,`calendar_name`),
    KEY              `idx_qrtz_t_g` (`sched_name`,`trigger_group`),
    KEY              `idx_qrtz_t_state` (`sched_name`,`trigger_state`),
    KEY              `idx_qrtz_t_n_state` (`sched_name`,`trigger_name`,`trigger_group`,`trigger_state`),
    KEY              `idx_qrtz_t_n_g_state` (`sched_name`,`trigger_group`,`trigger_state`),
    KEY              `idx_qrtz_t_next_fire_time` (`sched_name`,`next_fire_time`),
    KEY              `idx_qrtz_t_nft_st` (`sched_name`,`trigger_state`,`next_fire_time`),
    KEY              `idx_qrtz_t_nft_misfire` (`sched_name`,`misfire_instr`,`next_fire_time`),
    KEY              `idx_qrtz_t_nft_st_misfire` (`sched_name`,`misfire_instr`,`next_fire_time`,`trigger_state`),
    KEY              `idx_qrtz_t_nft_st_misfire_grp` (`sched_name`,`misfire_instr`,`next_fire_time`,`trigger_group`,`trigger_state`),
    CONSTRAINT `qrtz_triggers_ibfk_1` FOREIGN KEY (`sched_name`, `job_name`, `job_group`) REFERENCES `qrtz_job_details` (`sched_name`, `job_name`, `job_group`) ON DELETE RESTRICT ON UPDATE RESTRICT
);

DROP TABLE IF EXISTS `qrtz_job_details`;
CREATE TABLE `qrtz_job_details`
(
    `sched_name`        VARCHAR(120) NOT NULL,
    `job_name`          VARCHAR(190) NOT NULL,
    `job_group`         VARCHAR(190) NOT NULL,
    `description`       VARCHAR(250) DEFAULT null,
    `job_class_name`    VARCHAR(250) NOT NULL,
    `is_durable`        VARCHAR(1)   NOT NULL,
    `is_nonconcurrent`  VARCHAR(1)   NOT NULL,
    `is_update_data`    VARCHAR(1)   NOT NULL,
    `requests_recovery` VARCHAR(1)   NOT NULL,
    `job_data`          BLOB         DEFAULT NULL,
    PRIMARY KEY (`sched_name`, `job_name`, `job_group`),
    KEY                 `idx_qrtz_j_req_recovery` (`sched_name`,`requests_recovery`),
    KEY                 `idx_qrtz_j_grp` (`sched_name`,`job_group`)
);
drop table qrtz_job_details;
drop table qrtz_triggers;
drop table qrtz_blob_triggers;
drop table qrtz_simple_triggers;
drop table qrtz_simprop_triggers;
SET FOREIGN_KEY_CHECKS=1;
drop database qrtz_fk_testdb;