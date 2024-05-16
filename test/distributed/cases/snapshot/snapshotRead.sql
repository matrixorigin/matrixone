-- create/drop
-- A tenant creates snapshot backup, view creation information through show snapshots
drop database if exists test;
create database test;
use test;
drop table if exists snapshot01;
create table snapshot01 (col1 int primary key , col2 decimal, col3 bigint, col4 double, col5 float);
insert into snapshot01 values (1, 10.50, 1234567890, 123.45, 678.90),
                          (2, 20.75, 9876543210, 234.56, 789.01),
                          (3, 30.10, 1122334455, 345.67, 890.12),
                          (4, 40.25, 2233445566, 456.78, 901.23),
                          (5, 50.40, -3344556677, 567.89, 101.24),
                          (6, 60.55, -4455667788, 678.90, 112.35),
                          (7, 70.70, 5566778899, 789.01, 123.46),
                          (8, 80.85, -6677889900, 890.12, 134.57),
                          (9, 90.00, 7788990011, 901.23, 145.68),
                          (10, 100.00, 8899001122, 101.24, 156.79);
select count(*) from snapshot01;
select * from snapshot01;
show create table snapshot01;
drop snapshot if exists sp01;
create snapshot sp01 for account sys;
select * from snapshot01 {snapshot = 'sp01'};
select count(*) from snapshot01 {snapshot = 'sp01'};

-- @ignore:1
show snapshots where SNAPSHOT_NAME = 'sp01';
insert into snapshot01 values(11, 100.00, 8899001122, 101.24, 156.79);
select count(*) from snapshot01;
select count(*) from snapshot01 {snapshot = 'sp01'};
select * from snapshot01 {snapshot = 'sp01'};

update snapshot01 set col1 = 100 where col1 = 243214312;
select count(*) from snapshot01;
select count(*) from snapshot01 {snapshot = 'sp01'};
select * from snapshot01 {snapshot = 'sp01'};

delete from snapshot01 where col1 < 10;
select count(*) from snapshot01;
select count(*) from snapshot01 {snapshot = 'sp01'};
select * from snapshot01 {snapshot = 'sp01'};

alter table snapshot01 add column column1 bigint first;
show create table snapshot01;
select count(*) from snapshot01;
select * from snapshot01;
select count(*) from snapshot01 {snapshot = 'sp01'};
select * from snapshot01 {snapshot = 'sp01'};

truncate table snapshot01;
show create table snapshot01;
select count(*) from snapshot01;
select * from snapshot01;
select count(*) from snapshot01 {snapshot = 'sp01'};
select * from snapshot01 {snapshot = 'sp01'};

drop table snapshot01;
select count(*) from snapshot01;
select count(*) from snapshot01 {snapshot = 'sp01'};
select * from snapshot01 {snapshot = 'sp01'};
select count(*) from mo_catalog.mo_tables{snapshot = 'sp01'} where reldatabase = 'test';
-- @ignore:0,6,7
select * from mo_catalog.mo_database{snapshot = 'sp01'} where datname = 'test';
select attname from mo_catalog.mo_columns{snapshot = 'sp01'} where att_database = 'test';
drop snapshot sp01;
show snapshots;
select count(*) from mo_catalog.mo_tables{snapshot = 'sp01'} where reldatabase = 'test';
-- @ignore:0,6,7
select * from mo_catalog.mo_database{snapshot = 'sp01'} where datname = 'test';
select attname from mo_catalog.mo_columns{snapshot = 'sp01'} where att_database = 'test';

-- create snapshot while snapshot exists
drop table if exists snapshot02;
create table test_snapshot_read (a int);
insert into test_snapshot_read (a) VALUES(1), (2), (3), (4), (5),(6), (7), (8), (9), (10), (11), (12),(13), (14), (15), (16), (17), (18), (19), (20),(21), (22), (23), (24), (25), (26), (27), (28), (29), (30),(31), (32), (33), (34), (35), (36), (37), (38), (39), (40),(41), (42), (43), (44), (45), (46), (47), (48), (49), (50),(51), (52), (53), (54), (55), (56), (57), (58), (59), (60),(61), (62), (63), (64), (65), (66), (67), (68), (69), (70),(71), (72), (73), (74), (75), (76), (77), (78), (79), (80), (81), (82), (83), (84), (85), (86), (87), (88), (89), (90),(91), (92), (93), (94), (95), (96), (97), (98), (99), (100);
select count(*) from test_snapshot_read;
create snapshot snapshot_01 for account sys;
create snapshot snapshot_01 for account sys;
drop snapshot snapshot_01;
drop table test_snapshot_read;

-- create more snapshot
drop table if exists snapshot03;
drop table if exists snapshot04;
create table snapshot03(col1 int unique key,
                       col2 varchar(20),
                       col3 int,
                       col4 bigint);
create table snapshot04(col1 int,
                       col2 int,
                       col3 int primary key,
                       constraint `c1` foreign key(col1) references snapshot03(col1));
show create table snapshot03;
show create table snapshot04;
insert into snapshot03 values(1,'sfhuwe',1,1);
insert into snapshot03 values(2,'37829901k3d',2,2);
insert into snapshot04 values(1,1,1);
insert into snapshot04 values(2,2,2);
select * from snapshot03;
select * from snapshot04;

drop snapshot if exists sp03;
create snapshot sp03 for account sys;
-- @ignore:1
show snapshots where account_name = 'sys';
insert into snapshot03 values(3,'sfhuwe',1,1);
insert into snapshot03 values(4,'37829901k3d',2,2);
drop snapshot if exists sp04;
create snapshot sp04 for account sys;
-- @ignore:1
show snapshots;
select * from snapshot03;
select count(*) from snapshot03 {snapshot = 'sp03'};
select * from snapshot03{snapshot = 'sp03'};
select count(*) from snapshot04 {snapshot = 'sp03'};
select * from snapshot04{snapshot = 'sp03'};
select count(*) from snapshot03 {snapshot = 'sp04'};
select * from snapshot03{snapshot = 'sp04'};

alter table snapshot03 drop column col4;
insert into snapshot03 values(5,'sfhuwe',1);
select * from snapshot03;
select count(*) from snapshot03 {snapshot = 'sp03'};
drop snapshot if exists sp05;
create snapshot sp05 for account sys;
-- @ignore:1
show snapshots;
select count(*) from snapshot03 {snapshot = 'sp05'};
select * from snapshot03{snapshot = 'sp05'};
select count(*) from mo_catalog.mo_tables{snapshot = 'sp03'} where reldatabase = 'test';
-- @ignore:0,6,7
select * from mo_catalog.mo_database{snapshot = 'sp04'} where datname = 'test';
select attname from mo_catalog.mo_columns{snapshot = 'sp05'} where att_database = 'test';

drop snapshot sp05;
drop snapshot sp04;
drop snapshot sp03;
drop table snapshot04;
drop table snapshot03;
drop database test;

-- sys account creates snapshot for non-sys tenant
drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';
-- @session:id=3&user=acc01:test_account&password=111
drop database if exists test01;
create database test01;
use test01;
drop table if exists acc01_snap;
create table acc01_snap(col1 int, col2 char, col3 binary, primary key(col1, col2));
insert into acc01_snap values (1, 'a', '1');
insert into acc01_snap values (2, 'a', '1');
insert into acc01_snap values (10, 'm', null);
select * from acc01_snap;
-- @session
drop snapshot if exists snap01;
create snapshot snap01 for account acc01;
-- @ignore:1
show snapshots;
-- @session:id=4&user=acc01:test_account&password=111
use test01;
show snapshots;
-- @session
select count(*) from acc01_snap{snapshot = 'snap01'};
select count(*) from mo_catalog.mo_tables{snapshot = 'snap01'} where reldatabase = 'test01';
-- @ignore:0,6,7
select * from mo_catalog.mo_database{snapshot = 'snap01'} where datname = 'test01';
select attname from mo_catalog.mo_columns{snapshot = 'snap01'} where att_database = 'test01';
-- @session:id=5&user=acc01:test_account&password=111
drop database test01;
-- @session
select count(*) from mo_catalog.mo_tables{snapshot = 'snap01'} where reldatabase = 'test01';
-- @ignore:0,6,7
select * from mo_catalog.mo_database{snapshot = 'snap01'} where datname = 'test01';
select attname from mo_catalog.mo_columns{snapshot = 'snap01'} where att_database = 'test01';
drop snapshot snap01;
drop account acc01;

-- non-sys account create snapshot for non-sys account
drop account if exists acc02;
create account acc02 admin_name = 'test_account' identified by '111';
-- @session:id=6&user=acc02:test_account&password=111
drop database if exists test02;
create database test02;
use test02;
drop table if exists acc02_test01;
create table acc02_test01 (col1 decimal, col2 char, col3 varchar(30), col4 float);
insert into acc02_test01 values (3242.234234, '1', 'weawf3redwe', 38293.3232);
insert into acc02_test01 values (323.32411, 'a', '3233234213', 323231221);
insert into acc02_test01 values (-32323, 'v', 'wqd3wq', -323232);
select count(*) from acc02_test01;
drop snapshot if exists snap02;
create snapshot snap02 for account acc02;
-- @ignore:1
show snapshots;
-- @ignore:1
show snapshots where account_name = 'acc02';
select count(*) from acc02_test01 {snapshot = 'snap02'};
select * from acc02_test01 {snapshot = 'snap02'};
alter table acc02_test01 add column new int first;
show create table acc02_test01;
select count(*) from acc02_test01 {snapshot = 'snap02'};
select * from acc02_test01 {snapshot = 'snap02'};
truncate acc02_test01;
select count(*) from acc02_test01 {snapshot = 'snap02'};
select * from acc02_test01 {snapshot = 'snap02'};
insert into acc02_test01 values(1,1,2,3,4);
select count(*) from acc02_test01 {snapshot = 'snap02'};
select * from acc02_test01 {snapshot = 'snap02'};
select count(*) from mo_catalog.mo_tables{snapshot = 'snap02'} where reldatabase = 'test02';
-- @ignore:0,6,7
select * from mo_catalog.mo_database{snapshot = 'snap02'} where datname = 'test02';
select attname from mo_catalog.mo_columns{snapshot = 'snap02'} where att_database = 'test02';
drop snapshot snap02;
drop table acc02_test01;
drop database test02;
-- @session
drop account acc02;

-- select where
drop database if exists test03;
create database test03;
use test03;
drop table if exists testsnap_03;
create table testsnap_03 (
       employeeNumber int(11) not null ,
       lastName char(50) not null ,
       firstName char(50) not null ,
       extension char(10) not null ,
       email char(100) not null ,
       officeCode char(10) not null ,
       reportsTo int(11) DEFAULT NULL,
       jobTitle char(50) not null ,
       key (employeeNumber)
);
insert into testsnap_03(employeeNumber,lastName,firstName,extension,email,officeCode,reportsTo,jobTitle) values
                    (1002,'Murphy','Diane','x5800','dmurphy@classicmodelcars.com','1',NULL,'President'),
                    (1056,'Patterson','Mary','x4611','mpatterso@classicmodelcars.com','1',1002,'VP Sales'),
                    (1076,'Firrelli','Jeff','x9273','jfirrelli@classicmodelcars.com','1',1002,'VP Marketing'),
                    (1088,'Patterson','William','x4871','wpatterson@classicmodelcars.com','6',1056,'Sales Manager (APAC)'),
                    (1102,'Bondur','Gerard','x5408','gbondur@classicmodelcars.com','4',1056,'Sale Manager (EMEA)'),
                    (1143,'Bow','Anthony','x5428','abow@classicmodelcars.com','1',1056,'Sales Manager (NA)'),
                    (1165,'Jennings','Leslie','x3291','ljennings@classicmodelcars.com','1',1143,'Sales Rep'),
                    (1166,'Thompson','Leslie','x4065','lthompson@classicmodelcars.com','1',1143,'Sales Rep'),
                    (1188,'Firrelli','Julie','x2173','jfirrelli@classicmodelcars.com','2',1143,'Sales Rep'),
                    (1216,'Patterson','Steve','x4334','spatterson@classicmodelcars.com','2',1143,'Sales Rep');

drop snapshot if exists testsnap_03;
select count(*) from testsnap_03;
create snapshot sp04 for account sys;
select count(*) from testsnap_03 {snapshot = 'sp04'};
insert into testsnap_03 values (1286,'Tseng','Foon Yue','x2248','ftseng@classicmodelcars.com','3',1143,'Sales Rep'),
                               (1323,'Vanauf','George','x4102','gvanauf@classicmodelcars.com','3',1143,'Sales Rep'),
                               (1337,'Bondur','Loui','x6493','lbondur@classicmodelcars.com','4',1102,'Sales Rep'),
                               (1370,'Hernandez','Gerard','x2028','ghernande@classicmodelcars.com','4',1102,'Sales Rep'),
                               (1401,'Castillo','Pamela','x2759','pcastillo@classicmodelcars.com','4',1102,'Sales Rep');
select count(*) from testsnap_03 {snapshot = 'sp04'};
drop snapshot if exists sp05;
create snapshot sp05 for account sys;
select * from testsnap_03;
select * from testsnap_03 {snapshot = 'sp04'} where jobTitle = 'President';
select employeeNumber,lastName,firstName,extension from testsnap_03 {snapshot = 'sp04'} where extension = 'x4611';
select count(*) from testsnap_03 {snapshot = 'sp04'} where employeeNumber > 1000;
drop table if exists testsnap_04;
create table testsnap_04 (
     employeeNumber int(11) not null ,
     lastName char(50) not null ,
     firstName char(50) not null ,
     extension char(10) not null
);
insert into testsnap_04 select employeeNumber,lastName,firstName,extension from testsnap_03{snapshot = 'sp04'};
select * from testsnap_04;
select * from testsnap_03 {snapshot = 'sp04'};
select count(*) from mo_catalog.mo_tables{snapshot = 'sp04'} where reldatabase = 'test03';
-- @ignore:0,6,7
select * from mo_catalog.mo_database{snapshot = 'sp04'} where datname = 'test03';
select attname from mo_catalog.mo_columns{snapshot = 'sp04'} where att_database = 'test03';
drop table testsnap_03;
drop table testsnap_04;
drop snapshot sp04;
drop snapshot sp05;
drop database test03;

-- create snapshot for cluster table
use mo_catalog;
drop table if exists cluster01;
create cluster table cluster01(col1 int,col2 bigint);
insert into cluster01 values(1,2,0);
insert into cluster01 values(2,3,0);
select * from cluster01;
drop snapshot if exists sp06;
create snapshot sp06 for account sys;
-- @bvt:issue#15901
select count(*) from mo_catalog.mo_tables{snapshot = sp06} where reldatabase = 'mo_catalog';
-- @ignore:0,6,7
select * from mo_catalog.mo_database{snapshot = sp06} where datname = 'mo_catalog';
select attname from mo_catalog.mo_columns{snapshot = sp06} where att_database = 'mo_catalog';
-- @bvt:issue
drop table cluster01;
drop snapshot sp06;

-- pub table
drop database if exists test03;
create database test03;
use test03;
create table pub01 (col1 int primary key , col2 decimal, col3 bigint, col4 double, col5 float);
insert into pub01 values (1, 10.50, 1234567890, 123.45, 678.90),
                              (2, 20.75, 9876543210, 234.56, 789.01),
                              (3, 30.10, 1122334455, 345.67, 890.12),
                              (4, 40.25, 2233445566, 456.78, 901.23);
drop account if exists test_tenant_1;
create account test_tenant_1 admin_name 'test_account' identified by '111';
create publication publication01 database test03 account test_tenant_1 comment 'publish database to account01';
-- @session:id=7&user=test_tenant_1:test_account&password=111
create database sub_database01 from sys publication publication01;
show databases;
use sub_database01;
show tables;
-- @session
drop snapshot if exists sp06;
create snapshot sp06 for account sys;
select * from pub01 {snapshot = 'sp06'};
select count(*) from pub01 {snapshot = 'sp06'};
select count(*) from mo_catalog.mo_tables{snapshot = 'sp06'} where reldatabase = 'test03';
-- @ignore:0,6,7
select * from mo_catalog.mo_database{snapshot = 'sp06'} where datname = 'test03';
select attname from mo_catalog.mo_columns{snapshot = 'sp06'} where att_database = 'test03';
-- @session:id=2&user=test_tenant_1:test_account&password=111
use sub_database01;
select count(*) from mo_catalog.mo_tables{snapshot = 'sp06'} where reldatabase = 'sub_database01';
-- @ignore:0,6,7
select * from mo_catalog.mo_database{snapshot = 'sp06'} where datname = 'sub_database01';
select attname from mo_catalog.mo_columns{snapshot = 'sp06'} where att_database = 'sub_database01';
-- @session
drop account test_tenant_1;
drop publication publication01;
drop snapshot sp06;
drop table pub01;

-- table with partition by
drop table if exists pt_table;
create table pt_table(col1 tinyint,col2 smallint,col3 int,clo4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 text)partition by key(col13)partitions 10;
load data infile '$resources/external_table_file/pt_table_data.csv' into table  pt_table fields terminated by ',';
select count(*) from pt_table;
drop snapshot if exists sp07;
create snapshot sp07 for account sys;
-- @ignore:1
show snapshots;
select count(*) from pt_table{snapshot = 'sp07'};
show create table pt_table{snapshot = 'sp07'};
select count(*) from mo_catalog.mo_tables{snapshot = 'sp07'} where reldatabase = 'test03';
-- @ignore:0,6,7
select * from mo_catalog.mo_database{snapshot = 'sp07'} where datname = 'test03';
select attname from mo_catalog.mo_columns{snapshot = 'sp07'} where att_database = 'test03';
drop snapshot sp07;
drop table pt_table;
drop database test03;

-- A database contains multiple tables at the same time, including tables of any type
drop database if exists test04;
create database test04;
use test04;
drop table if exists normal_table01;
create table normal_table01 (col1 enum('a','b','c'), col2 date, col3 binary);
insert into normal_table01 values ('a', '2000-10-01', 'a');
insert into normal_table01 values ('b', '2023-12-12', '1');
insert into normal_table01 values ('c', '1999-11-11', 'c');
select * from normal_table01;
use mo_catalog;
drop table if exists cluster02;
create cluster table cluster02(col1 timestamp, col2 varchar(20));
insert into cluster02 values ('2000-12-12 12:12:12.000', 'database', 0);
insert into cluster02 values ('2024-01-12 11:11:29.000', 'cluster table', 0);
select * from cluster02;
use test04;
drop table if exists test04;
create table test04 (`maxvalue` int);
create table t3 (`maxvalue` int);
insert into t3 values(1);
insert into t3 values(2);
insert into t3 values(100);
insert into t3 values(-1000);
drop account if exists test_tenant_1;
create account test_tenant_1 admin_name 'test_account' identified by '111';
create publication publication01 database test04 account test_tenant_1 comment 'publish database';
-- @session:id=12&user=test_tenant_1:test_account&password=111
create database sub_database02 from sys publication publication01;
use sub_database02;
-- @session
drop snapshot if exists sp08;
create snapshot sp08 for account sys;
use test04;
select count(*) from normal_table01 {snapshot = 'sp08'};
select count(*) from t3 {snapshot = 'sp08'};
use mo_catalog;
select count(*) from cluster02 {snapshot = 'sp08'};
use test04;
delete from normal_table01 where col1 = 'a';
use mo_catalog;
update cluster02 set col2 = 'table';
use test04;
insert into t3 values(218231);
select count(*) from normal_table01 {snapshot = 'sp08'};
use mo_catalog;
select count(*) from cluster02 {snapshot = 'sp08'};
use test04;
select count(*) from t3 {snapshot = 'sp08'};
select count(*) from mo_catalog.mo_tables{snapshot = 'sp08'} where reldatabase = 'test04';
drop account test_tenant_1;
-- @ignore:0,6,7
select * from mo_catalog.mo_database{snapshot = 'sp08'} where datname = 'test04';
select attname from mo_catalog.mo_columns{snapshot = 'sp08'} where att_database = 'test04';
select count(*) from mo_catalog.mo_tables{snapshot = 'sp08'} where reldatabase = 'test04';
-- @ignore:0,6,7
select * from mo_catalog.mo_database{snapshot = 'sp08'} where datname = 'test04';
select attname from mo_catalog.mo_columns{snapshot = 'sp08'} where att_database = 'test04';
drop publication publication01;
drop table normal_table01;
use mo_catalog;
drop table cluster02;
use test04;
drop table t3;
drop database test04;

-- reserved keywords and non-reserved keywords as snapshot name
drop database if exists test05;
create database test05;
use test05;
drop table if exists t1;
create table t1 (a blob);
insert into t1 values('abcdef');
insert into t1 values('_bcdef');
insert into t1 values('a_cdef');
insert into t1 values('ab_def');
insert into t1 values('abc_ef');
insert into t1 values('abcd_f');
insert into t1 values('abcde_');
select count(*) from t1;
drop snapshot if exists `binary`;
create snapshot `binary` for account sys;
-- @bvt:issue#15901
select count(*) from mo_catalog.mo_tables{snapshot = `binary`} where reldatabase = 'test05';
-- @ignore:0,6,7
select * from mo_catalog.mo_database{snapshot = `binary`} where datname = 'test05';
select attname from mo_catalog.mo_columns{snapshot = `binary`} where att_database = 'test05';
-- @bvt:issue
drop snapshot `binary`;
drop table t1;

drop table if exists t1;
create table t1 (
    dvalue  date not null,
    value32  integer not null,
    primary key(dvalue)
);
insert into t1 values('2022-01-01', 1);
insert into t1 values('2022-01-02', 2);
drop snapshot if exists consistent;
create snapshot consistent for account sys;
-- @bvt:issue#15901
select count(*) from mo_catalog.mo_tables{snapshot = consistent} where reldatabase = 'test05';
-- @ignore:0,6,7
select * from mo_catalog.mo_database{snapshot = consistent} where datname = 'test05';
select attname from mo_catalog.mo_columns{snapshot = consistent} where att_database = 'test05';
-- @bvt:issue
drop snapshot consistent;
drop table t1;
drop database if exists test05;

-- create snapshot in explicit transaction
drop database if exists test06;
create database test06;
use test06;
drop table if exists tran01;
start transaction;
create table tran01(col1 enum('red','blue','green'));
insert into tran01 values('red'),('blue'),('green');
create snapshot sp09 for account sys;
-- @ignore:1
show snapshots;
commit;
drop snapshot if exists sp09;
create snapshot sp09 for account sys;
-- @ignore:1
show snapshots;
select count(*) from tran01{snapshot = 'sp09'};
select count(*) from mo_catalog.mo_tables{snapshot = 'sp09'} where reldatabase = 'test06';
-- @ignore:0,6,7
select * from mo_catalog.mo_database{snapshot = 'sp09'} where datname = 'test06';
select attname from mo_catalog.mo_columns{snapshot = 'sp09'} where att_database = 'test06';
drop table tran01;
drop snapshot sp09;
drop database test06;

-- verify that data outside the snapshot is deleted by gc
drop database if exists test07;
create database test07;
use test07;
drop table if exists t1;
create table t1(a int, b char(10), c date, d decimal(7,2), UNIQUE KEY(a, b));
desc t1;
insert into t1 values(1, 'ab', '1980-12-17', 800);
insert into t1 values(2, 'ac', '1981-02-20', 1600);
insert into t1 values(3, 'ad', '1981-02-22', 500);
select * from t1;

drop table if exists t2;
create table t2 (a json,b int);
insert into t2 values ('{"t1":"a"}',1),('{"t1":"b"}',2);
select * from t2;

drop table if exists t3;
create table t3(
                     deptno varchar(20),
                     dname varchar(15),
                     loc varchar(50),
                     primary key(deptno)
);

insert into t3 values (10,'ACCOUNTING','NEW YORK');
insert into t3 values (20,'RESEARCH','DALLAS');
insert into t3 values (30,'SALES','CHICAGO');
insert into t3 values (40,'OPERATIONS','BOSTON');
select * from t3;

drop table t3;
drop table t2;

drop snapshot if exists sp10;
create snapshot sp10 for account sys;

select count(*) from t3{snapshot = 'sp10'};
select count(*) from t2{snapshot = 'sp10'};
select count(*) from t1{snapshot = 'sp10'};
select * from t1{snapshot = 'sp10'};
select * from t2{snapshot = 'sp10'};
select * from t3{snapshot = 'sp10'};

-- @ignore:0
select distinct(object_name) from metadata_scan('test07.t3','a')m;
-- @ignore:0
select distinct(object_name) from metadata_scan('test07.t2','a')m;
-- @ignore:0
select distinct(object_name) from metadata_scan('test07.t1','a')m;
drop table t1;
drop database test07;
drop snapshot sp10;
drop snapshot sp08;





