-- clone db/table in current account with snapshot
set ft_relevancy_algorithm="TF-IDF";
SET experimental_hnsw_index = 1;
drop database if exists test01;
create database test01;
use test01;
drop table if exists src;
create table src (id bigint primary key, body varchar, title text);
insert into src values (0, 'color is red', 't1'), (1, 'car is yellow', 'crazy car'), (2, 'sky is blue', 'no limit'), (3, 'blue is not red', 'colorful'),
                       (4, '遠東兒童中文是針對6到9歲的小朋友精心設計的中文學習教材，共三冊，目前已出版一、二冊。', '遠東兒童中文'),
                       (5, '每冊均採用近百張全幅彩圖及照片，生動活潑、自然真實，加深兒童學習印象，洋溢學習樂趣。', '遠東兒童中文'),
                       (6, '各個單元主題內容涵蓋中華文化及生活應用的介紹。本套教材含課本、教學指引、生字卡、學生作業本與CD，中英對照，精美大字版。本系列有繁體字及簡體字兩種版本印行。', '中文短篇小說'),
                       (7, '59個簡單的英文和中文短篇小說', '適合初學者'),
                       (8, NULL, 'NOT INCLUDED'),
                       (9, 'NOT INCLUDED BODY', NULL),
                       (10, NULL, NULL);

create fulltext index ftidx on src (body, title);
select * from src;
drop table if exists aerr;
create table aerr (uid varchar(120), email varchar(20), lmethod varchar(100));
insert into aerr values ("user1", 'xx@yeah.net', 'basic'),('user2', 'xx@mo.cn','basic'),('user3', '11@tt.cn', 'basic');
select uid,lmethod,email from aerr where lmethod = 'basic' and  email = 'xx@mo.cn';
create unique index mail_method_idx on aerr(email,lmethod);
alter table aerr add primary key (uid);
select uid,lmethod,email from aerr where lmethod = 'basic' and  email = 'xx@mo.cn';
set @idxsql = concat("select count(*) from `", (SELECT index_table_name FROM mo_catalog.mo_indexes WHERE column_name = 'lmethod'), "`");
prepare s1 from @idxsql;
execute s1;
drop snapshot if exists sp01;
create snapshot sp01 for account;

drop database if exists test02;
create database test02;
use test02;
drop table if exists vector_test;
create table `vector_test` (id INT PRIMARY KEY AUTO_INCREMENT, vector vecf32(3));
INSERT INTO `vector_test` VALUES(1,"[1,2,4]");
INSERT INTO `vector_test` VALUES(2,"[1,2,6]");
INSERT INTO `vector_test` VALUES(3,"[1,3,6]");
INSERT INTO `vector_test` VALUES(4,"[1,3,6]");
INSERT INTO `vector_test` VALUES(5,"[1,3,6]");
INSERT INTO `vector_test` VALUES(6,"[1,3,6]");
select * from vector_test;
drop snapshot if exists sp02;
create snapshot sp02 for account;

drop database if exists test03;
create database test03;
use test03;
drop table if exists vector_index_01;
create table vector_index_01(a bigint primary key, b vecf32(128),c int,key c_k(c));
create index idx01 using hnsw on vector_index_01(b) op_type "vector_l2_ops" M 48 EF_CONSTRUCTION 64 EF_SEARCH 64;
insert into vector_index_01 values(9774 ,"[1, 0, 1, 6, 6, 17, 47, 39, 2, 0, 1, 25, 27, 10, 56, 130, 18, 5, 2, 6, 15, 2, 19, 130, 42, 28, 1, 1, 2, 1, 0, 5, 0, 2, 4, 4, 31, 34, 44, 35, 9, 3, 8, 11, 33, 12, 61, 130, 130, 17, 0, 1, 6, 2, 9, 130, 111, 36, 0, 0, 11, 9, 1, 12, 2, 100, 130, 28, 7, 2, 6, 7, 9, 27, 130, 83, 5, 0, 1, 18, 130, 130, 84, 9, 0, 0, 2, 24, 111, 24, 0, 1, 37, 24, 2, 10, 12, 62, 33, 3, 0, 0, 0, 1, 3, 16, 106, 28, 0, 0, 0, 0, 17, 46, 85, 10, 0, 0, 1, 4, 11, 4, 2, 2, 9, 14, 8, 8]",3),(9775,"[0, 1, 1, 3, 0, 3, 46, 20, 1, 4, 17, 9, 1, 17, 108, 15, 0, 3, 37, 17, 6, 15, 116, 16, 6, 1, 4, 7, 7, 7, 9, 6, 0, 8, 10, 4, 26, 129, 27, 9, 0, 0, 5, 2, 11, 129, 129, 12, 103, 4, 0, 0, 2, 31, 129, 129, 94, 4, 0, 0, 0, 3, 13, 42, 0, 15, 38, 2, 70, 129, 1, 0, 5, 10, 40, 12, 74, 129, 6, 1, 129, 39, 6, 1, 2, 22, 9, 33, 122, 13, 0, 0, 0, 0, 5, 23, 4, 11, 9, 12, 45, 38, 1, 0, 0, 4, 36, 38, 57, 32, 0, 0, 82, 22, 9, 5, 13, 11, 3, 94, 35, 3, 0, 0, 0, 1, 16, 97]",5),(9776,"[10, 3, 8, 5, 48, 26, 5, 16, 17, 0, 0, 2, 132, 53, 1, 16, 112, 6, 0, 0, 7, 2, 1, 48, 48, 15, 18, 31, 3, 0, 0, 9, 6, 10, 19, 27, 50, 46, 17, 9, 18, 1, 4, 48, 132, 23, 3, 5, 132, 9, 4, 3, 11, 0, 2, 46, 84, 12, 10, 10, 1, 0, 12, 76, 26, 22, 16, 26, 35, 15, 3, 16, 15, 1, 51, 132, 125, 8, 1, 2, 132, 51, 67, 91, 8, 0, 0, 30, 126, 39, 32, 38, 4, 0, 1, 12, 24, 2, 2, 2, 4, 7, 2, 19, 93, 19, 70, 92, 2, 3, 1, 21, 36, 58, 132, 94, 0, 0, 0, 0, 21, 25, 57, 48, 1, 0, 0, 1]",3);
insert into vector_index_01 values(9777, " [16, 15, 0, 0, 5, 46, 5, 5, 4, 0, 0, 0, 28, 118, 12, 5, 75, 44, 5, 0, 6, 32, 6, 49, 41, 74, 9, 1, 0, 0, 0, 9, 1, 9, 16, 41, 71, 80, 3, 0, 0, 4, 3, 5, 51, 106, 11, 3, 112, 28, 13, 1, 4, 8, 3, 104, 118, 14, 1, 1, 0, 0, 0, 88, 3, 27, 46, 118, 108, 49, 2, 0, 1, 46, 118, 118, 27, 12, 0, 0, 33, 118, 118, 8, 0, 0, 0, 4, 118, 95, 40, 0, 0, 0, 1, 11, 27, 38, 12, 12, 18, 29, 3, 2, 13, 30, 94, 78, 30, 19, 9, 3, 31, 45, 70, 42, 15, 1, 3, 12, 14, 22, 16, 2, 3, 17, 24, 13]",4),(9778,"[41, 0, 0, 7, 1, 1, 20, 67, 9, 0, 0, 0, 0, 31, 120, 61, 25, 0, 0, 0, 0, 10, 120, 90, 32, 0, 0, 1, 13, 11, 22, 50, 4, 0, 2, 93, 40, 15, 37, 18, 12, 2, 2, 19, 8, 44, 120, 25, 120, 5, 0, 0, 0, 2, 48, 97, 102, 14, 3, 3, 11, 9, 34, 41, 0, 0, 4, 120, 56, 3, 4, 5, 6, 15, 37, 116, 28, 0, 0, 3, 120, 120, 24, 6, 2, 0, 1, 28, 53, 90, 51, 11, 11, 2, 12, 14, 8, 6, 4, 30, 9, 1, 4, 22, 25, 79, 120, 66, 5, 0, 0, 6, 42, 120, 91, 43, 15, 2, 4, 39, 12, 9, 9, 12, 15, 5, 24, 36]",4);
select * from vector_index_01;
drop snapshot if exists sp03;
create snapshot sp03 for account;

drop database if exists test04;
create database test04;
use test04;
drop table if exists fk_01;
drop table if exists fk_02;
create table fk_01(col1 varchar(30) not null primary key,col2 int);
create table fk_02(col1 int,col2 varchar(25),col3 tinyint,constraint ck foreign key(col2) REFERENCES fk_01(col1));
show create table fk_02;
insert into fk_01 values ('90',5983),('100',734),('190',50);
insert into fk_02(col2,col3) values ('90',5),('90',4),('100',0),(NULL,80);
drop table if exists enum01;
create table enum01 (col1 enum('red','blue','green'));
insert into enum01 values ('red'),('blue'),('green');
desc enum01;
select * from enum01;
show create table enum01;
drop snapshot if exists sp04;
create snapshot sp04 for account;

-- create database clone in current account
drop database if exists test01_new;
drop database if exists test02_new;
drop database if exists test03_new;
drop database if exists test04_new;
create database test01_new clone test01 {snapshot = 'sp01'};
create database test02_new clone test02 {snapshot = 'sp02'};
create database test03_new clone test03 {snapshot = 'sp03'};
create database test04_new clone test04 {snapshot = 'sp04'};
drop database if exists mo_catalog_new;
drop database if exists mo_catalog_new_new;
create database mo_catalog_new clone mo_catalog {snapshot = 'sp01'};
create table mo_catalog_new.src_new clone mo_catalog.mo_database {snapshot = 'sp01'};
show create table mo_catalog_new.src_new;
select datname,dat_catalog_name,dat_createsql,account_id from mo_catalog_new.mo_database where account_id = 0 and datname = 'test01';
create database mo_catalog_new_new clone mo_catalog {snapshot = 'sp04'};
select datname,dat_catalog_name,dat_createsql,account_id from mo_catalog_new_new.mo_database where account_id = 0 and (datname = 'test01' or datname = 'test02' or datname = 'test03');
drop database if exists mo_catalog_new01;
create database mo_catalog_new01 clone mo_catalog_new;
select datname,dat_catalog_name,dat_createsql,account_id from mo_catalog_new01.mo_database where account_id = 0 and datname = 'test01';
drop database if exists mo_catalog_new_new01;
create database mo_catalog_new_new01 clone mo_catalog_new;
drop database mo_catalog_new;
select datname,dat_catalog_name,dat_createsql,account_id from mo_catalog_new01.mo_database where account_id = 0 and datname = 'test01';
drop database mo_catalog_new_new;
select datname,dat_catalog_name,dat_createsql,account_id from mo_catalog_new_new01.mo_database where account_id = 0 and datname = 'test01';
drop database if exists information_schema_new;
create database information_schema_new clone information_schema;
select table_catalog,table_schema,table_name,table_type,engine,version,row_format from information_schema.tables where table_name = 'mo_columns';
select table_catalog,table_schema,table_name,table_type,engine,version,row_format from information_schema_new.tables where table_name = 'mo_columns';
drop database if exists mo_debug_new;
create database mo_debug_new clone mo_debug;
select * from mo_debug.trace_features;
drop database if exists mo_task_new;
create database mo_task_new clone mo_task;
create snapshot spx for account;
drop database if exists mo_task_new;
create database mo_task_new clone mo_task {snapshot = "spx"};
select
    task_metadata_executor, task_metadata_context, task_metadata_option
from
    mo_task_new.sys_cron_task {snapshot = "spx"}
except
select
    task_metadata_executor, task_metadata_context, task_metadata_option
from
    mo_task_new.sys_cron_task {snapshot = "spx"};
drop database if exists mysql_new;
create database mysql_new clone mysql;
use mysql_new;
drop database if exists system_new;
create database system_new clone system;
drop database if exists system_metrics_new;
create database system_metrics_new clone system_metrics;
use system_metrics_new;
-- @bvt:issue#23182
show tables;
-- @bvt:issue
show databases;
use test01_new;
show tables;
select * from src;
show create table src;
use test02_new;
select count(*) from vector_test;
desc vector_test;
use test03_new;
select * from vector_index_01;
show create table vector_index_01;
use test04_new;
select * from enum01;
desc enum01;
use test01;
drop table src;
use test02;
alter table vector_test drop column vector;
use test03;
truncate vector_index_01;
use test04;
delete from enum01 where col1 = 'blue';
update enum01 set col1 ='blue' where col1 = 'green';
use test01_new;
show tables;
select * from src;
show create table src;
use test02_new;
select count(*) from vector_test;
desc vector_test;
use test03_new;
select * from vector_index_01;
show create table vector_index_01;
use test04_new;
select * from enum01;
select datname,dat_catalog_name,dat_createsql,account_id from mo_catalog_new01.mo_database where account_id = 0 and datname = 'test01';
select datname,dat_catalog_name,dat_createsql,account_id from mo_catalog_new_new01.mo_database where account_id = 0 and datname = 'test01';
select table_catalog,table_schema,table_name,table_type,engine,version,row_format from information_schema.tables where table_name = 'mo_columns';
select table_catalog,table_schema,table_name,table_type,engine,version,row_format from information_schema_new.tables where table_name = 'mo_columns';
select * from mo_debug.trace_features;
show create table mysql_new.columns_priv;
drop snapshot sp01;
drop snapshot sp02;
drop snapshot sp03;
drop snapshot sp04;
drop snapshot spx;
drop database test01;
drop database test02;
drop database test03;
drop database test04;
drop database test01_new;
drop database test02_new;
drop database test03_new;
drop database test04_new;
drop database mo_catalog_new01;
drop database mo_catalog_new_new01;
drop database information_schema_new;
drop database mo_debug_new;
drop database mysql_new;
drop database system_new;
drop database system_metrics_new;
drop database mo_task_new;




-- clone db/table in current account without snapshot
drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';
drop database if exists test05;
create database test05;
use test05;
drop table if exists t1;
drop table if exists t2;
CREATE TABLE t1 (
                    id int,
                    token varchar(100) DEFAULT '' NOT NULL,
                    count int DEFAULT 0 NOT NULL,
                    qty int,
                    phone char(1) DEFAULT '' NOT NULL,
                    times datetime DEFAULT '2000-01-01 00:00:00' NOT NULL
);
INSERT INTO t1 VALUES (21,'e45703b64de71482360de8fec94c3ade',3,7800,'n','1999-12-23 17:22:21');
INSERT INTO t1 VALUES (22,'e45703b64de71482360de8fec94c3ade',4,5000,'y','1999-12-23 17:22:21');
INSERT INTO t1 VALUES (18,'346d1cb63c89285b2351f0ca4de40eda',3,13200,'b','1999-12-23 11:58:04');
INSERT INTO t1 VALUES (17,'ca6ddeb689e1b48a04146b1b5b6f936a',4,15000,'b','1999-12-23 11:36:53');
INSERT INTO t1 VALUES (16,'ca6ddeb689e1b48a04146b1b5b6f936a',3,13200,'b','1999-12-23 11:36:53');
INSERT INTO t1 VALUES (26,'a71250b7ed780f6ef3185bfffe027983',5,1500,'b','1999-12-27 09:44:24');
INSERT INTO t1 VALUES (24,'4d75906f3c37ecff478a1eb56637aa09',3,5400,'y','1999-12-23 17:29:12');
CREATE TABLE t2 (
                    id int,
                    category int DEFAULT 0 NOT NULL,
                    county int DEFAULT 0 NOT NULL,
                    state int DEFAULT 0 NOT NULL,
                    phones int DEFAULT 0 NOT NULL,
                    nophones int DEFAULT 0 NOT NULL
);
INSERT INTO t2 VALUES (3,2,11,12,5400,7800);
INSERT INTO t2 VALUES (4,2,25,12,6500,11200);
INSERT INTO t2 VALUES (5,1,37,6,10000,12000);
drop database if exists test06;
create database test06;
use test06;
create publication publication01 database republication01 account acc01 comment 'republish';
create table repub01(col1 int);
insert into repub01 values (1);
drop database if exists test07;
create database test07;
use test07;
drop table if exists t1;
create table t1(
                   col1 date not null,
                   col2 datetime,
                   col3 timestamp,
                   col4 bool
);
set time_zone = 'SYSTEM';
load data infile '$resources/load_data/time_date_1.csv' into table t1 fields terminated by ',';
select * from t1;
drop database if exists test08;
create database test08;
use test08;
create table vtab32(id int primary key auto_increment,`vecf32_3` vecf32(3),`vecf32_5` vecf32(5));
create table vtab64(id int primary key auto_increment,`vecf64_3` vecf64(3),`vecf64_5` vecf64(5));

insert into vtab32(vecf32_3,vecf32_5) values(NULL,NULL);
insert into vtab32(vecf32_3,vecf32_5) values("[0.8166459,0.66616553,0.4886152]",NULL);
insert into vtab64(vecf64_3,vecf64_5) values(NULL,NULL);
insert into vtab64(vecf64_3,vecf64_5) values("[0.8166459,0.66616553,0.4886152]",NULL);
insert into vtab64(vecf64_3,vecf64_5) values ("[8.5606893,6.7903588,821.977768]","[0.46323407,23.49801546,563.9229458,56.07673508,8732.9583881]");
insert into vtab64(vecf64_3,vecf64_5) values ("[0.9260021,0.26637346,0.06567037]","[0.45756745,65.2996871,321.623636,3.60082066,87.58445764]");
use test05;
show tables;
select * from t1;
select * from t2;
show create table t1;
show create table t2;
desc t1;
desc t2;
use test06;
show tables;
select * from repub01;
use test07;
select * from t1;
use test08;
select * from vtab32;
select * from vtab64;

drop database if exists test05_new;
drop database if exists test06_new;
drop database if exists test07_new;
drop database if exists test08_new;
create database test05_new clone test05;
create table test05_new.t2 clone test05_new.t2;
create table test05_new.t3 clone test05_new.t2;
create database test06_new clone test06;
create table test06_new.temp_test clone test05_new.t2;
show create table test06_new.temp_test;
select * from test06_new.temp_test;
drop table test06_new.temp_test;
create database test07_new clone test07;
create database test08_new clone test08;
create table test07_new.vtab32 clone test08.vtab32;
create table test07_new.vtab64 clone test08.vtab64;
select * from test07_new.vtab32;
select * from test07_new.vtab64;
show databases;
drop database test05;
use test06;
drop publication publication01;
drop table repub01;
drop database test07;
use test08;
truncate vtab32;
show databases;
use test05_new;
show tables;
select * from t1;
select * from t2;
show create table t1;
show create table t2;
desc t1;
desc t2;
use test06_new;
use test07_new;
select * from t1;
use test08_new;
select * from vtab32;
select * from vtab64;
desc vtab32;
show create table vtab64;
drop database test05;
drop database test06;
drop database test07;
drop database test08;
drop database test05_new;
drop database test06_new;
drop database test07_new;
drop database test08_new;




-- clone db/table to new account with snapshot
drop account if exists acc02;
create account acc02 admin_name = 'test_account' identified by '111';
drop account if exists acc03;
create account acc03 admin_name = 'test_account' identified by '111';
drop account if exists acc04;
create account acc04 admin_name = 'test_account' identified by '111';
drop database if exists test09;
create database test09;
use test09;
drop table if exists t1;
create table t1(a int);
create view v1 as select * from t1;
create table t2(a int not null, b varchar(20), c char(20));
create view v2 as select * from t2;
desc v2;
drop snapshot if exists sp09;
create snapshot sp09 for account;
drop database if exists test10;
create database test10;
use test10;
drop table if exists vector_index_07;
create table vector_index_07(a bigint primary key, b vecf32(128),c int,key c_k(c));
insert into vector_index_07 values(9774 ,"[1, 0, 1, 6, 6, 17, 47, 39, 2, 0, 1, 25, 27, 10, 56, 130, 18, 5, 2, 6, 15, 2, 19, 130, 42, 28, 1, 1, 2, 1, 0, 5, 0, 2, 4, 4, 31, 34, 44, 35, 9, 3, 8, 11, 33, 12, 61, 130, 130, 17, 0, 1, 6, 2, 9, 130, 111, 36, 0, 0, 11, 9, 1, 12, 2, 100, 130, 28, 7, 2, 6, 7, 9, 27, 130, 83, 5, 0, 1, 18, 130, 130, 84, 9, 0, 0, 2, 24, 111, 24, 0, 1, 37, 24, 2, 10, 12, 62, 33, 3, 0, 0, 0, 1, 3, 16, 106, 28, 0, 0, 0, 0, 17, 46, 85, 10, 0, 0, 1, 4, 11, 4, 2, 2, 9, 14, 8, 8]",3),(9775,"[0, 1, 1, 3, 0, 3, 46, 20, 1, 4, 17, 9, 1, 17, 108, 15, 0, 3, 37, 17, 6, 15, 116, 16, 6, 1, 4, 7, 7, 7, 9, 6, 0, 8, 10, 4, 26, 129, 27, 9, 0, 0, 5, 2, 11, 129, 129, 12, 103, 4, 0, 0, 2, 31, 129, 129, 94, 4, 0, 0, 0, 3, 13, 42, 0, 15, 38, 2, 70, 129, 1, 0, 5, 10, 40, 12, 74, 129, 6, 1, 129, 39, 6, 1, 2, 22, 9, 33, 122, 13, 0, 0, 0, 0, 5, 23, 4, 11, 9, 12, 45, 38, 1, 0, 0, 4, 36, 38, 57, 32, 0, 0, 82, 22, 9, 5, 13, 11, 3, 94, 35, 3, 0, 0, 0, 1, 16, 97]",5),(9776,"[10, 3, 8, 5, 48, 26, 5, 16, 17, 0, 0, 2, 132, 53, 1, 16, 112, 6, 0, 0, 7, 2, 1, 48, 48, 15, 18, 31, 3, 0, 0, 9, 6, 10, 19, 27, 50, 46, 17, 9, 18, 1, 4, 48, 132, 23, 3, 5, 132, 9, 4, 3, 11, 0, 2, 46, 84, 12, 10, 10, 1, 0, 12, 76, 26, 22, 16, 26, 35, 15, 3, 16, 15, 1, 51, 132, 125, 8, 1, 2, 132, 51, 67, 91, 8, 0, 0, 30, 126, 39, 32, 38, 4, 0, 1, 12, 24, 2, 2, 2, 4, 7, 2, 19, 93, 19, 70, 92, 2, 3, 1, 21, 36, 58, 132, 94, 0, 0, 0, 0, 21, 25, 57, 48, 1, 0, 0, 1]",3);
insert into vector_index_07 values(9777, " [16, 15, 0, 0, 5, 46, 5, 5, 4, 0, 0, 0, 28, 118, 12, 5, 75, 44, 5, 0, 6, 32, 6, 49, 41, 74, 9, 1, 0, 0, 0, 9, 1, 9, 16, 41, 71, 80, 3, 0, 0, 4, 3, 5, 51, 106, 11, 3, 112, 28, 13, 1, 4, 8, 3, 104, 118, 14, 1, 1, 0, 0, 0, 88, 3, 27, 46, 118, 108, 49, 2, 0, 1, 46, 118, 118, 27, 12, 0, 0, 33, 118, 118, 8, 0, 0, 0, 4, 118, 95, 40, 0, 0, 0, 1, 11, 27, 38, 12, 12, 18, 29, 3, 2, 13, 30, 94, 78, 30, 19, 9, 3, 31, 45, 70, 42, 15, 1, 3, 12, 14, 22, 16, 2, 3, 17, 24, 13]",4),(9778,"[41, 0, 0, 7, 1, 1, 20, 67, 9, 0, 0, 0, 0, 31, 120, 61, 25, 0, 0, 0, 0, 10, 120, 90, 32, 0, 0, 1, 13, 11, 22, 50, 4, 0, 2, 93, 40, 15, 37, 18, 12, 2, 2, 19, 8, 44, 120, 25, 120, 5, 0, 0, 0, 2, 48, 97, 102, 14, 3, 3, 11, 9, 34, 41, 0, 0, 4, 120, 56, 3, 4, 5, 6, 15, 37, 116, 28, 0, 0, 3, 120, 120, 24, 6, 2, 0, 1, 28, 53, 90, 51, 11, 11, 2, 12, 14, 8, 6, 4, 30, 9, 1, 4, 22, 25, 79, 120, 66, 5, 0, 0, 6, 42, 120, 91, 43, 15, 2, 4, 39, 12, 9, 9, 12, 15, 5, 24, 36]",4);
create index idx01 using hnsw on vector_index_07(b) op_type "vector_l2_ops";
drop table if exists employees;
create table employees (
                           employeeNumber int(11) NOT NULL,
                           lastName varchar(50) NOT NULL,
                           firstName varchar(50) NOT NULL,
                           extension varchar(10) NOT NULL,
                           email varchar(100) NOT NULL,
                           officeCode varchar(10) NOT NULL,
                           reportsTo int(11) DEFAULT NULL,
                           jobTitle varchar(50) NOT NULL,
                           PRIMARY KEY (employeeNumber)
);
insert into employees(employeeNumber,lastName,firstName,extension,email,officeCode,reportsTo,jobTitle) values
                                                                                                           (1002,'Murphy','Diane','x5800','dmurphy@classicmodelcars.com','1',NULL,'President'),
                                                                                                           (1056,'Patterson','Mary','x4611','mpatterso@classicmodelcars.com','1',1002,'VP Sales'),
                                                                                                           (1076,'Firrelli','Jeff','x9273','jfirrelli@classicmodelcars.com','1',1002,'VP Marketing');
drop table if exists src;
create table src (id bigint primary key, json1 json, json2 json);
insert into src values  (0, '{"a":1, "b":"redredredredredredredredredredrerr"}', '{"d": "happybirthdayhappybirthdayhappybirthday", "f":"winterautumnsummerspring"}'),
                        (1, '{"a":2, "b":"中文學習教材"}', '["apple", "orange", "banana", "指引"]'),
                        (2, '{"a":3, "b":"redbluebrownyelloworange"}', '{"d":"兒童中文"}');
drop snapshot if exists sp10;
create snapshot sp10 for account;
drop database if exists test11;
create database test11;
use test11;
drop table if exists t1;
create table t1(a smallint unsigned auto_increment);
insert into t1 values(null), (3), (null), (6), (null), (65535);
select * from t1;
drop table if exists dense_rank01;
create table dense_rank01 (id integer, sex char(1));
insert into dense_rank01 values (1, 'm');
insert into dense_rank01 values (2, 'f');
insert into dense_rank01 values (3, 'f');
insert into dense_rank01 values (4, 'f');
insert into dense_rank01 values (5, 'm');
drop table if exists text_01;
create table text_01(t1 text,t2 text,t3 text);
insert into text_01 values ('中文123abcd','',NULL);
insert into text_01 values ('yef&&190',' wwww ',983);
insert into text_01 select '',null,'中文';
insert into text_01 select '123','7834','commmmmmment';
insert into text_01 values ('789',' 23:50:00','20');
drop table if exists t1;
create table t1 (a json,b int);
insert into t1 values ('{"t1":"a"}',1),('{"t1":"b"}',2);
select * from t1;
drop table if exists test05;
create table test05 (col1 int, col2 datalink);
insert into test05 values (1, 'file://$resources/load_data/test_columnlist_01.csv?offset=5');
insert into test05 values (2, 'file://$resources/load_data/test_columnlist_02.csv?offset=10');
select col1, load_file(col2) from test05;
drop snapshot if exists sp11;
create snapshot sp11 for account;
show databases;
use test09;
select count(*) from t1;
select * from v1;
show create table v1;
select * from v2;
show tables;
use test10;
show tables;
select * from employees;
desc src;
select count(*) from vector_index_07;
use test11;
select * from dense_rank01;
select * from t1;
select count(*) from test05;
select * from text_01;
show tables;
-- @session:id=2&user=acc02:test_account&password=111
drop database if exists test09_new;
drop database if exists test09_table;
create database test09_table;
-- @session
-- @session:id=3&user=acc03:test_account&password=111
drop database if exists test10_new;
drop database if exists test10_table;
create database test10_table;
-- @session
-- @session:id=4&user=acc04:test_account&password=111
drop database if exists test11_new;
drop database if exists test11_table;
create database test11_table;
-- @session
create database test09_new clone test09 {snapshot = 'sp09'} to account acc02;
create database test10_new clone test10 {snapshot = 'sp10'} to account acc03;
create database test11_new clone test11 {snapshot = 'sp11'} to account acc04;
create table test09_table.t1 clone test09.t1 {snapshot = 'sp09'} to account acc02;
create table test10_table.employees clone test10.employees {snapshot = 'sp10'} to account acc03;
create table test10_table.employees clone test10.employees {snapshot = 'sp10'} to account acc03;
create table test11_table.dense_rank01 clone test11.dense_rank01 {snapshot = 'sp11'} to account acc04;
-- @session:id=2&user=acc02:test_account&password=111
use test09_new;
select count(*) from t1;
select * from v1;
show create table v1;
select * from v2;
use test09_table;
select * from t1;
-- @session
-- @session:id=3&user=acc03:test_account&password=111
use test10_new;
show tables;
select * from employees;
desc src;
select count(*) from vector_index_07;
use test10_table;
select * from employees;
insert into employees(employeeNumber,lastName,firstName,extension,email,officeCode,reportsTo,jobTitle) values
    (1012,'Murphy','Diane','x5800','dmurphy@classicmodelcars.com','1',NULL,'President');
-- @session
-- @session:id=4&user=acc04:test_account&password=111
use test11_new;
select * from dense_rank01;
select * from t1;
select count(*) from test05;
select * from text_01;
use test11_table;
select * from dense_rank01;
delete from dense_rank01 where id < 5;
-- @session
drop database test09;
drop database test10;
drop database test11;
-- @session:id=2&user=acc02:test_account&password=111
use test09_new;
select count(*) from t1;
select * from v1;
show create table v1;
select * from v2;
use test09_table;
select * from t1;
drop database test09_new;
drop database test09_table;
-- @session
-- @session:id=3&user=acc03:test_account&password=111
use test10_new;
show tables;
select * from employees;
desc src;
select count(*) from vector_index_07;
drop database test10_new;
use test10_table;
select * from employees;
drop database test10_table;
-- @session
-- @session:id=4&user=acc04:test_account&password=111
use test11_new;
select * from dense_rank01;
select * from t1;
select count(*) from test05;
select * from text_01;
drop database test11_new;
use test11_table;
select * from dense_rank01;
drop database test11_table;
-- @session
drop account acc04;
drop snapshot sp09;
drop snapshot sp10;
drop snapshot sp11;





-- b clone a, c clone b, d clone c in the same account
-- @bvt:issue#22363
drop account if exists acc05;
create account acc05 admin_name = 'test_account' identified by '111';
-- @session:id=5&user=acc05:test_account&password=111
drop database if exists test12;
create database test12;
use test12;
drop table if exists t1;
create table t1 (
                    a bigint not null,
                    b bigint not null default 0,
                    c bigint not null default 0,
                    d bigint not null default 0,
                    e bigint not null default 0,
                    f bigint not null default 0,
                    g bigint not null default 0,
                    h bigint not null default 0,
                    i bigint not null default 0,
                    j bigint not null default 0,
                    primary key (a));
insert into t1 (a) values (2),(4),(6),(8),(10),(12),(14),(16),(18),(20),(22),(24),(26),(23);
drop table if exists bit01;
create table bit01(id char(1), b binary(10));
insert into bit01 values('a', '111');
insert into bit01 values('a', '110');
insert into bit01 values('a', '143');
insert into bit01 values('a', '000');
insert into bit01 values('b', '001');
insert into bit01 values('b', '011');
insert into bit01 values('c', '010');
insert into bit01 values('d', null);
select * from bit01;
drop table if exists time01;
create table time01 (a datetime(0) not null, primary key(a));
insert into time01 values ('20200101000000'), ('2022-01-02'), ('2022-01-02 00:00:01'), ('2022-01-02 00:00:01.512345');
drop table if exists double01;
create table double01 (a double not null, primary key(a));
insert into double01 values(-1.7976931348623157E+308),(-2.2250738585072014E-308),(0),(2.2250738585072014E-308),(1.7976931348623157E+308);
drop table if exists tinyint01;
create table tinyint01 (a tinyint unsigned not null, primary key(a));
insert into tinyint01 values (255), (0xFC), (254), (253);

SET experimental_hnsw_index = 1;
drop database if exists test13;
create database test13;
use test13;
drop table if exists vector_index_09;
create table vector_index_09(a bigint primary key, b vecf32(128),c int,key c_k(c));
create index idx01 using hnsw on vector_index_09(b) op_type "vector_l2_ops";
insert into vector_index_09 values(9774 ,NULL,3),(9775,NULL,10);
insert into vector_index_09(a,c) values(9777,4),(9778,9);
drop table if exists vector_ip_01;
create table vector_ip_01(a bigint primary key, b vecf32(128), c int, key c_k(c));
insert into vector_ip_01 values(9774 ,normalize_l2("[1, 0, 1, 6, 6, 17, 47, 39, 2, 0, 1, 25, 27, 10, 56, 130, 18, 5, 2, 6, 15, 2, 19, 130, 42, 28, 1, 1, 2, 1, 0, 5, 0, 2, 4, 4, 31, 34, 44, 35, 9, 3, 8, 11, 33, 12, 61, 130, 130, 17, 0, 1, 6, 2, 9, 130, 111, 36, 0, 0, 11, 9, 1, 12, 2, 100, 130, 28, 7, 2, 6, 7, 9, 27, 130, 83, 5, 0, 1, 18, 130, 130, 84, 9, 0, 0, 2, 24, 111, 24, 0, 1, 37, 24, 2, 10, 12, 62, 33, 3, 0, 0, 0, 1, 3, 16, 106, 28, 0, 0, 0, 0, 17, 46, 85, 10, 0, 0, 1, 4, 11, 4, 2, 2, 9, 14, 8, 8]"),3),(9775,normalize_l2("[0, 1, 1, 3, 0, 3, 46, 20, 1, 4, 17, 9, 1, 17, 108, 15, 0, 3, 37, 17, 6, 15, 116, 16, 6, 1, 4, 7, 7, 7, 9, 6, 0, 8, 10, 4, 26, 129, 27, 9, 0, 0, 5, 2, 11, 129, 129, 12, 103, 4, 0, 0, 2, 31, 129, 129, 94, 4, 0, 0, 0, 3, 13, 42, 0, 15, 38, 2, 70, 129, 1, 0, 5, 10, 40, 12, 74, 129, 6, 1, 129, 39, 6, 1, 2, 22, 9, 33, 122, 13, 0, 0, 0, 0, 5, 23, 4, 11, 9, 12, 45, 38, 1, 0, 0, 4, 36, 38, 57, 32, 0, 0, 82, 22, 9, 5, 13, 11, 3, 94, 35, 3, 0, 0, 0, 1, 16, 97]"),5),(9776,normalize_l2("[10, 3, 8, 5, 48, 26, 5, 16, 17, 0, 0, 2, 132, 53, 1, 16, 112, 6, 0, 0, 7, 2, 1, 48, 48, 15, 18, 31, 3, 0, 0, 9, 6, 10, 19, 27, 50, 46, 17, 9, 18, 1, 4, 48, 132, 23, 3, 5, 132, 9, 4, 3, 11, 0, 2, 46, 84, 12, 10, 10, 1, 0, 12, 76, 26, 22, 16, 26, 35, 15, 3, 16, 15, 1, 51, 132, 125, 8, 1, 2, 132, 51, 67, 91, 8, 0, 0, 30, 126, 39, 32, 38, 4, 0, 1, 12, 24, 2, 2, 2, 4, 7, 2, 19, 93, 19, 70, 92, 2, 3, 1, 21, 36, 58, 132, 94, 0, 0, 0, 0, 21, 25, 57, 48, 1, 0, 0, 1]"),3);
insert into vector_ip_01 values(9777, normalize_l2("[16, 15, 0, 0, 5, 46, 5, 5, 4, 0, 0, 0, 28, 118, 12, 5, 75, 44, 5, 0, 6, 32, 6, 49, 41, 74, 9, 1, 0, 0, 0, 9, 1, 9, 16, 41, 71, 80, 3, 0, 0, 4, 3, 5, 51, 106, 11, 3, 112, 28, 13, 1, 4, 8, 3, 104, 118, 14, 1, 1, 0, 0, 0, 88, 3, 27, 46, 118, 108, 49, 2, 0, 1, 46, 118, 118, 27, 12, 0, 0, 33, 118, 118, 8, 0, 0, 0, 4, 118, 95, 40, 0, 0, 0, 1, 11, 27, 38, 12, 12, 18, 29, 3, 2, 13, 30, 94, 78, 30, 19, 9, 3, 31, 45, 70, 42, 15, 1, 3, 12, 14, 22, 16, 2, 3, 17, 24, 13]"),4),(9778,normalize_l2("[41, 0, 0, 7, 1, 1, 20, 67, 9, 0, 0, 0, 0, 31, 120, 61, 25, 0, 0, 0, 0, 10, 120, 90, 32, 0, 0, 1, 13, 11, 22, 50, 4, 0, 2, 93, 40, 15, 37, 18, 12, 2, 2, 19, 8, 44, 120, 25, 120, 5, 0, 0, 0, 2, 48, 97, 102, 14, 3, 3, 11, 9, 34, 41, 0, 0, 4, 120, 56, 3, 4, 5, 6, 15, 37, 116, 28, 0, 0, 3, 120, 120, 24, 6, 2, 0, 1, 28, 53, 90, 51, 11, 11, 2, 12, 14, 8, 6, 4, 30, 9, 1, 4, 22, 25, 79, 120, 66, 5, 0, 0, 6, 42, 120, 91, 43, 15, 2, 4, 39, 12, 9, 9, 12, 15, 5, 24, 36]"),4);
create index idx01 using hnsw on vector_ip_01(b) op_type "vector_ip_ops";
show create table vector_ip_01;

drop database if exists test14;
create database test14;
use test14;
drop table if exists index03;
create table index03 (
                         emp_no      int             not null,
                         birth_date  date            not null,
                         first_name  varchar(14)     not null,
                         last_name   varchar(16)     not null,
                         gender      varchar(5)      not null,
                         hire_date   date            not null,
                         primary key (emp_no)
) partition by range columns (emp_no)(
    partition p01 values less than (100001),
    partition p02 values less than (200001),
    partition p03 values less than (300001),
    partition p04 values less than (400001)
);
insert into index03 values (9001,'1980-12-17', 'SMITH', 'CLERK', 'F', '2008-12-17'),
                           (9002,'1981-02-20', 'ALLEN', 'SALESMAN', 'F', '2008-02-20'),
                           (9003,'1991-02-20', 'Bob', 'TEACHER', 'M', '2008-02-20'),
                           (9004,'1999-02-20', 'MARY', 'PROGRAMMER', 'M', '2008-02-20');
select * from index03;

drop database if exists test15;
create database test15;
use test15;
drop table if exists departments;
create table departments (
                             department_id INT primary key auto_increment,
                             department_name varchar(100)
);
show create table departments;
insert into departments (department_id, department_name)
values (1, 'HR'),(2, 'Engineering');
drop table if exists employees;
create table employees (
                           employee_id INT primary key,
                           first_name varchar(50),
                           last_name varchar(50),
                           department_id INT,
                           FOREIGN KEY (department_id) REFERENCES departments(department_id)
);
insert into employees values
                          (1, 'John', 'Doe', 1),
                          (2, 'Jane', 'Smith', 2),
                          (3, 'Bob', 'Johnson', 1);
drop view if exists employee_view;
create view employee_view as select employee_id, first_name, last_name, department_id from employees;

drop database if exists test12_new;
drop database if exists test13_new;
drop database if exists test14_new;
drop database if exists test15_new;

create database test12_new clone test12;
create database test13_new clone test13;
use test12_new;
drop table t1;
drop table bit01;
drop database if exists test12_new_new;
drop database if exists test13_new_new;
drop database if exists test14_new_new;
drop database if exists test15_new_new;
create database test12_new_new clone test12_new;
create database test13_new_new clone test13_new;
use test12_new_new;
show tables;
select * from double01;
show create table time01;
select * from tinyint01;
create database test14_new clone test14;
create database test15_new clone test15;
drop database test14;
use test15_new;
drop table employees;
create database test14_new_new clone test14_new;
create database test15_new_new clone test15_new;

drop database test12;
use test13;
drop table if exists vector_index_09;
drop table if exists vector_ip_01;

drop database test15;
drop database test12_new;
use test13_new;
drop table if exists vector_index_09;
drop table if exists vector_ip_01;
use test14_new;
truncate index03;
drop database test15_new;
use test12_new_new;
select * from time01;
select count(*) from tinyint01;
use test13_new_new;
select * from vector_index_09;
select * from vector_ip_01;
use test14_new_new;
show create table index03;
use test15_new_new;
select * from departments;
show create table departments;
-- @session
drop account acc05;
-- @bvt:issue
show databases;
