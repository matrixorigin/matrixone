use mo_catalog;
create cluster table t1(a int);
drop table if exists t1;
use information_schema;
create cluster table t1(a int);
use system;
create cluster table t1(a int);
use system_metrics;
create cluster table t1(a int);
use mysql;
create cluster table t1(a int);
use mo_task;
create cluster table t1(a int);

drop database if exists db1;
create database db1;
use db1;
create cluster table t1(a int);
drop database if exists db1;

use mo_catalog;
CREATE CLUSTER TABLE `mo_instance` (`id` varchar(128) NOT NULL,`name` VARCHAR(255) NOT NULL,`account_name` varchar(128) NOT NULL,`provider` longtext NOT NULL,`provider_id` longtext,`region` longtext NOT NULL,`plan_type` longtext NOT NULL,`version` longtext,`status` longtext,`quota` longtext,`network_policy` longtext,`created_by` longtext,`created_at` datetime(3) NULL,PRIMARY KEY (`id`, `account_id`),UNIQUE INDEX `uniq_acc` (`account_name`));
SHOW CREATE TABLE mo_catalog.mo_instance;
drop table mo_instance;
