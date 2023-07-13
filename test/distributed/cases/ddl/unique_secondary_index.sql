-- create unique/secondary index all type
create table index_01 (col1 bigint primary key,col2 varchar(25),col3 float,col4 varchar(50),unique key num_phone(col2),key num_id(col4));
insert into index_01 values (1,'13456789872',20.23,'5678'),(2,'13873458290',100.00,'23'),(3,NULL,100.00,'23');
insert into index_01 values (67834,'13456789872',20.23,'4090'),(56473,'13456789872',100.00,'5678');
insert into index_01 values (4,'13866666666',20.23,'5678'),(5,'13873458290',100.00,'23'),(6,'13777777777',100.00,'23');
select col2,col4 from index_01;
create table index_02 (col1 bigint primary key,col2 char(25) unique key,col3 float,col4 char(50),key num_id(col4));
insert into index_02 values (67834,'13456789872',20.23,'5678'),(56473,'',100.00,'5678');
insert into index_02 values (1,'',20.23,'5678'),(2,'13873458290',100.00,'23');
insert into index_02 values (67834,'13456799878',20.23,'4090'),(56473,NULL,100.00,'');
insert into index_02 values (3,'excel',0.1,'4090'),(4,'中文',0.2,''),(5,'MMEabc$%^123',0.2,'');
select col2,col4 from index_02;
select * from index_02 where col2="MMEabc$%^123";
create table index_03 (col1 bigint auto_increment primary key,col2 int,col3 float,col4 int,unique key id1(col2),key id2(col4));
insert into index_03(col2,col3,col4) values (10,20.23,4090),(10,100.00,5678);
insert into index_03(col2,col3,col4) values (10,20.23,4090),(11,100.00,4090);
insert into index_03(col2,col3,col4) values (67834,20.23,4090),(56473,100.00,5678),(NULL,0.01,NULL);
insert into index_03(col2,col3,col4) values (-2147483648,1.2,100),(2147483647,2.0,5);
select * from index_03;
select * from index_03 where col2=-2147483648;
create table index_04 (col1 bigint,col2 int primary key,col3 float,col4 bigint,unique key id1(col1),key id2(col4));
insert into index_04 values (67834,2,20.23,4090),(67834,4,100.00,4091);
insert into index_04 values (1,2,20.23,4090),(2,4,100.00,4091),(NULL,3,0.01,NULL);
-- @pattern
insert into index_04 values (3,2,20.23,4090),(2,4,100.00,4091),(4,4,100.00,4090);
select * from index_04;
select * from index_04 where col1 between 10 and 1000000;
create table index_05 (col1 smallint unique key,col2 int primary key,col3 float,col4 smallint,key id2(col4));
insert into index_05 values (1,2,20.23,4090),(1,4,100.00,4091);
insert into index_05 values (1,2,20.23,4090),(2,4,100.00,4091),(NULL,3,0.01,NULL);
select * from index_05;
create table index_06 (col1 tinyint,col2 int primary key,col3 float,col4 tinyint,unique key id1(col1),key id2(col4));
insert into index_06 values (1,2,20.23,56),(1,4,100.00,90);
insert into index_06 values (1,2,20.23,56),(2,4,100.00,41),(NULL,3,0.01,NULL);
select * from index_06;
create table index_07 (col1 int unsigned,col2 int primary key,col3 float,col4 int unsigned,unique key id1(col1),key id2(col4));
insert into index_07 values (1,2,20.23,56),(2,4,100.00,41),(NULL,3,0.01,NULL);
-- @pattern
insert into index_07 values (1,2,20.23,56),(1,4,100.00,90);
select * from index_07;
create table index_08 (col1 bigint unsigned,col2 int primary key,col3 float,col4 bigint unsigned,unique key id1(col1),key id2(col4));
insert into index_08 values (1,2,20.23,56),(1,4,100.00,90);
insert into index_08 values (1,2,20.23,56),(2,4,100.00,41),(NULL,3,0.01,NULL);
select * from index_08;
create table index_09 (col1 bigint primary key,col2 decimal(4,2),col3 decimal(4,2),unique key d1(col2),key d2(col3));
insert into index_09 values (1000,20.23,20.00),(1200,20.23,0.10);
insert into index_09 values (1000,20.23,20.00),(1200,0.23,20.10),(1100,NULL,NULL);
select * from index_09;
create table index_10 (col1 bigint primary key,col2 float,col3 float,unique key d1(col2),key d2(col3));
insert into index_10 values (1000,20.23,20.00),(1200,20.23,0.10);
insert into index_10 values (1000,20.23,20.00),(1200,0.23,20.10),(1100,NULL,NULL);
select * from index_10;
create table index_11 (col1 bigint primary key,col2 double,col3 double,unique key d1(col2),key d2(col3));
insert into index_11 values (1000,20.23,20.00),(1200,20.23,0.10);
insert into index_11 values (1000,20.23,20.00),(1200,0.23,20.10),(1100,NULL,NULL);
select * from index_11;
create table index_12 (col1 bigint auto_increment primary key,col2 date,col3 date,unique key d1(col2),key d2(col3));
insert into index_12(col2,col3) values ('2013-01-01','2014-02-01'),('2013-01-01','2014-02-20');
insert into index_12(col2,col3) values (NULL,'2014-02-01'),(NULL,NULL);
select col2 from index_12;
create table index_13 (col1 bigint auto_increment primary key,col2 datetime,col3 datetime,unique key d1(col2),key d2(col3));
insert into index_13(col2,col3) values ('2013-01-01 12:00:00','2014-02-01 10:00:00'),('2013-01-01 12:00:00','2014-02-20 05:00:00');
insert into index_13(col2,col3) values (NULL,'2014-02-01 12:00:0'),(NULL,NULL);
create table index_14 (col1 bigint auto_increment primary key,col2 timestamp,col3 timestamp,unique key d1(col2),key d2(col3));
insert into index_14(col2,col3) values ('2013-01-01 12:00:00','2014-02-01 10:00:00'),('2013-01-01 12:00:00','2014-02-20 05:00:00');
insert into index_14(col2,col3) values (NULL,'2014-02-01 12:00:0'),(NULL,NULL);
create table index_15 (col1 bigint primary key,col2 bool,unique key c2(col2));
-- @pattern
insert into index_15 values (1,TRUE),(2,FALSE),(3,TRUE);
insert into index_15 values (1,TRUE),(2,FALSE),(3,NULL);
select * from index_15;
-- blob/json/text type not support unique index
create table index_16 (col1 bigint primary key,col2 blob,col3 blob,unique key d1(col2),key d2(col3));
create table index_17 (col1 bigint primary key,col2 json,col3 json,unique key d1(col2),key d2(col3));
create table index_18 (col1 bigint primary key,col2 text,col3 text,unique key d1(col2),key d2(col3));

--unique index name test
create table index_name (col1 bigint primary key,col2 varchar(25),col3 float,col4 varchar(50),unique key 123(col2),key num_id(col4));
create table index_name (col1 bigint primary key,col2 varchar(25),col3 float,col4 varchar(50),unique key abs@123.abc(col2),key num_id(col4));
create table index_name (col1 bigint primary key,col2 varchar(25),col3 float,col4 varchar(50),unique key default(col2),key num_id(col4));
create table index_name (col1 bigint primary key,col2 varchar(25),col3 float,col4 varchar(50),unique key `default`(col2),key num_id(col4));
drop table index_name;
create table index_name (col1 bigint primary key,col2 varchar(25),col3 float,col4 varchar(50),unique key `123`(col2),key num_id(col4));
drop table index_name;
create table index_name (col1 bigint primary key,col2 varchar(25),col3 float,col4 varchar(50),unique key `abs@123.abc`(col2),key num_id(col4));
drop table index_name;

--unique index include null and not null,''
create table index_table_01 (col1 bigint auto_increment primary key,col2 varchar(25),col3 float default 87.01,col4 int,unique key num_phone(col2),key num_id(col4));
insert into index_table_01 values (67834,'13456789872',20.23,4090),(56473,'',100.00,5678);
insert into index_table_01 values (34,NULL,4090,1);
insert into index_table_01 values (56478,'',103.00,5670);
select * from index_table_01;
drop table index_table_01;

create table index_table_02 (col1 bigint primary key,col2 varchar(25) not null,col3 float default 87.01,col4 int,unique key num_phone(col2),key num_id(col4));
insert into index_table_02 values (34,NULL,4090,1);
drop table index_table_02;

--unique index default
create table index_table_02 (col1 bigint primary key,col2 int default 0,unique key col2(col2));
insert into index_table_02(col1) values (1);
insert into index_table_02(col1) values (2),(3);
drop table index_table_02;

--only unique key
create table index_table_03 (col1 bigint,col2 int,unique key col2(col2));
insert into index_table_03 values (1,20),(2,NULL),(3,90);
update index_table_03 set col2=10 where col2 is NULL;
select * from index_table_03;
drop table index_table_03;

--load infile ,insert select
CREATE TABLE IF NOT EXISTS `t_code_rule` (
  `code_id` bigint(20) NOT NULL ,
  `code_no` varchar(50) NOT NULL,
  `org_no` varchar(50) NOT NULL,
  `org_name` varchar(50) NOT NULL,
  `ancestors` varchar(255) NOT NULL,
  `code_rule_no` varchar(50) NOT NULL,
  `code_rule_name` varchar(50) NOT NULL,
  `code_name` varchar(50) NOT NULL,
  `code_type` int(11),
  `split` varchar(50) DEFAULT NULL,
  `remark` varchar(255),
  `create_time` datetime NOT NULL,
  `create_user` varchar(50) DEFAULT NULL,
  `last_update_time` datetime DEFAULT NULL,
  `last_update_user` varchar(50) DEFAULT NULL,
  `is_system_code` varchar(20)  NOT NULL DEFAULT 'N',
  PRIMARY KEY (`code_id`),
  UNIQUE KEY `code_type` (`code_type`),
  KEY `code_no` (`code_no`),
  KEY `code_rule_no` (`code_rule_no`),
  KEY `org_no` (`org_no`)
);
show create table t_code_rule;
load data infile  '$resources/load_data/unique_index_file.csv' into table t_code_rule ;
select code_id,code_type,code_no,code_rule_no,org_no from t_code_rule;
truncate table t_code_rule;
-- @bvt:issue#3433
load data infile  '$resources/load_data/unique_index_duplicate.csv' into table t_code_rule;
select code_id,code_type,code_no,code_rule_no,org_no from t_code_rule;
create table index_temp( col1 bigint(20) NOT NULL ,col2 varchar(50) NOT NULL,col3 varchar(50) NOT NULL,col4 varchar(50) NOT NULL,col5 varchar(255) NOT NULL,col6 varchar(50) NOT NULL,col7 varchar(50) NOT NULL,col8 varchar(50) NOT NULL,col9 int(11) ,col10 varchar(50) DEFAULT NULL,col11 varchar(255),col12 datetime NOT NULL,col13 varchar(50) DEFAULT NULL,col14 datetime DEFAULT NULL,col15 varchar(50) DEFAULT NULL,col16 varchar(20)  NOT NULL DEFAULT 'N');
load data infile  '$resources/load_data/unique_index_file.csv' into table index_temp;
insert into t_code_rule select * from index_temp;
select code_id,code_type,code_no,code_rule_no,org_no from t_code_rule;
truncate table index_temp;
load data infile  '$resources/load_data/unique_index_duplicate.csv' into table index_temp;
-- @pattern
insert into t_code_rule select * from index_temp;
-- @bvt:issue

--unique index more column
create table index_table_04 (col1 bigint not null auto_increment,col2 varchar(25),col3 int,col4 varchar(50),primary key (col1),unique key m1(col2,col3),key num_id(col4));
insert into index_table_04(col2,col3,col4)  select 'apple',1,'10';
insert into index_table_04(col2,col3,col4)  select 'apple',2,'11';
insert into index_table_04(col2,col3,col4)  select 'apple',2,'12';
insert into index_table_04(col2,col3,col4)  select NULL,NULL,'13';
select * from index_table_04;
drop table index_table_04;
create table index_table_04 (col1 bigint not null auto_increment,col2 varchar(25),col3 int,col4 varchar(50),primary key (col1),unique key m1(col2),unique key m2(col3),key num_id(col4));
insert into index_table_04(col2,col3,col4)  select 'apple',1,'10';
insert into index_table_04(col2,col3,col4)  select 'apple',2,'11';
insert into index_table_04(col2,col3,col4)  select 'apple',2,'12';
insert into index_table_04(col2,col3,col4)  select NULL,NULL,'13';
select * from index_table_04;

--unique key update/delete/truncate and update duplicate data
create table index_table_05 (col1 bigint not null auto_increment,col2 varchar(25),col3 int,col4 varchar(50),primary key (col1),unique key col2(col2),key num_id(col4));
insert into index_table_05(col2,col3,col4) values ('apple',1,'10'),('store',2,'11'),('bread',3,'12');
select * from index_table_05;
update index_table_05 set col2='chart' where col1=2;
select col2 from index_table_05;
update index_table_05 set col2='bread' where col1=1;
select * from index_table_05;
delete from index_table_05 where col2='apple';
select * from index_table_05;
truncate table index_table_05;
insert into index_table_05(col2,col3,col4) values ('apple',1,'10'),('store',2,'11'),('apple',3,'12');
select * from index_table_05;
drop table index_table_05;

--rename table
create table index_table_05 (col1 bigint not null auto_increment,col2 varchar(25),col3 int,col4 varchar(50),primary key (col1),unique key col2(col2),key num_id(col4));
insert into index_table_05(col2,col3,col4) values ('apple',1,'10'),('store',2,'11'),('bread',3,'12');
--rename table index_table_05 to index_table_05_new;
--insert into index_table_05_new(col2,col3,col4) values ('apple',4,'13');
--select * from index_table_05_new;

--only one/more key column
create table index_table_06 (col1 bigint not null auto_increment,col2 varchar(25),col3 int,col4 varchar(50),primary key (col1),key num_id(col4));
insert into index_table_06(col2,col3,col4) values ('apple',1,'10'),('store',2,'11'),('bread',3,'12');
select * from index_table_06;
drop table index_table_06;
create table index_table_06 (col1 bigint not null auto_increment,col2 varchar(25),col3 int default 10,col4 varchar(50),primary key (col1),key col2(col2),key col3(col3));
insert into index_table_06(col2,col3,col4) values ('apple',1,'10'),('store',2,'11'),('bread',2,'12');
insert into index_table_06(col2,col3,col4) values ('read',1,'10'),('write',2,'11'),('bread',3,'10');
insert into index_table_06(col2,col3,col4) values ('read',NULL,'10'),('write',NULL,NULL);
select col2,col3 from index_table_06;

--abnormal test :create not exists unique/secondary index column
create table index_table_07 (col1 bigint not null auto_increment,col2 varchar(25),col3 int,col4 varchar(50),primary key (col1),unique key col10(col10),key num_id(col4));
create table index_table_07 (col1 bigint not null auto_increment,col2 varchar(25),col3 int,col4 varchar(50),primary key (col1),unique key col2(col2),key num_id(col40));

--create unique index
create table create_index_01 (col1 bigint primary key,col2 varchar(25),col3 float,col4 varchar(50));
create unique index varchar_index on create_index_01(col2) comment 'create varchar index';
insert into create_index_01 values (1,'13456789872',20.23,'5678'),(2,'13873458290',100.00,'23'),(3,NULL,100.00,'23');
insert into create_index_01 values (67834,'13456789872',20.23,'4090'),(56473,'13456789872',100.00,'5678');
select col2,col4 from create_index_01;
select * from create_index_01 where col2 not in ('13873458290');
drop index varchar_index on create_index_01;
create table create_index_02 (col1 bigint,col2 char(25),col3 float,col4 char(50));
create unique index char_index on create_index_02(col2);
insert into create_index_02 values (1,'',20.23,'5678'),(2,'13873458290',100.00,'23');
insert into create_index_02 values (67834,'13456799878',20.23,'4090'),(56473,NULL,100.00,'');
insert into create_index_02 values (3,'excel',0.1,'4090'),(4,'中文',0.2,''),(5,'MMEabc$%^123',0.2,'');
select col2,col4 from create_index_02;
select * from create_index_02 where col2 like "MME%";
drop index char_index on create_index_02;
create table create_index_03 (col1 bigint auto_increment,col2 int,col3 float,col4 int);
create unique index int_index on create_index_03(col2);
insert into create_index_03(col2,col3,col4) values (10,20.23,4090),(10,100.00,5678);
insert into create_index_03(col2,col3,col4) values (10,20.23,4090),(11,100.00,4090);
insert into create_index_03(col2,col3,col4) values (67834,20.23,4090),(56473,100.00,5678),(NULL,0.01,NULL);
select * from create_index_03;
drop index int_index on create_index_03;
create table create_index_04 (col1 bigint,col2 int primary key,col3 float,col4 bigint);
create unique index bigint_index on create_index_04(col1);
insert into create_index_04 values (67834,2,20.23,4090),(67834,4,100.00,4091);
insert into create_index_04 values (1,2,20.23,4090),(2,4,100.00,4091),(NULL,3,0.01,NULL);
insert into create_index_04 values (-9223372036854775808,5,20.23,4090),(9223372036854775807,6,100.00,4091);
select * from create_index_04;
select * from create_index_04 where col1 in (-9223372036854775808,9223372036854775807);
create table create_index_05 (col1 smallint,col2 int primary key,col3 float,col4 smallint);
create unique index smallint_index on create_index_05(col1);
insert into create_index_05 values (1,2,20.23,4090),(1,4,100.00,4091);
insert into create_index_05 values (1,2,20.23,4090),(2,4,100.00,4091),(NULL,3,0.01,NULL);
select * from create_index_05;
create table create_index_06 (col1 tinyint,col2 int primary key,col3 float,col4 tinyint);
create unique index tinyint_index on create_index_06(col1);
insert into create_index_06 values (1,2,20.23,56),(1,4,100.00,90);
insert into create_index_06 values (1,2,20.23,56),(2,4,100.00,41),(NULL,3,0.01,NULL);
select * from create_index_06;
create table create_index_07 (col1 int unsigned,col2 int primary key,col3 float,col4 int unsigned);
create unique index int_unsigned_index on create_index_07(col1);
insert into create_index_07 values (1,2,20.23,56),(2,4,100.00,41),(NULL,3,0.01,NULL);
-- @pattern
insert into create_index_07 values (1,2,20.23,56),(1,4,100.00,90);
select * from create_index_07;
create table create_index_08 (col1 bigint unsigned,col2 int primary key,col3 float,col4 bigint unsigned);
create unique index bigint_unsigned_index on create_index_08(col1);
insert into create_index_08 values (1,2,20.23,56),(1,4,100.00,90);
insert into create_index_08 values (1,2,20.23,56),(2,4,100.00,41),(NULL,3,0.01,NULL);
select * from create_index_08;
create table create_index_09 (col1 bigint primary key,col2 decimal(16,8),col3 decimal(16,8));
create unique index decimal_index on create_index_09(col2);
insert into create_index_09 values (1000,20.23,20.00),(1200,20.23,0.10);
insert into create_index_09 values (1000,20.23,20.00),(1200,0.23,20.10),(1100,NULL,NULL);
select * from create_index_09;
create table create_index_10 (col1 bigint primary key,col2 float,col3 float);
create unique index float_index on create_index_10(col2);
insert into create_index_10 values (1000,20.23,20.00),(1200,20.23,0.10);
insert into create_index_10 values (1000,20.23,20.00),(1200,0.23,20.10),(1100,NULL,NULL);
select * from create_index_10;
create table create_index_11 (col1 bigint primary key,col2 double,col3 double);
create unique index double_index on create_index_11(col2);
insert into create_index_11 values (1000,20.23,20.00),(1200,20.23,0.10);
insert into create_index_11 values (1000,20.23,20.00),(1200,0.23,20.10),(1100,NULL,NULL);
select * from create_index_11;
create table create_index_12(col1 bigint auto_increment primary key,col2 date,col3 date);
create unique index date_index on create_index_12(col2);
insert into create_index_12(col2,col3) values ('2013-01-01','2014-02-01'),('2013-01-01','2014-02-20');
insert into create_index_12(col2,col3) values (NULL,'2014-02-01'),(NULL,NULL);
create table create_index_13 (col1 bigint auto_increment primary key,col2 datetime,col3 datetime);
create unique index datetime_index on create_index_13(col2);
insert into create_index_13(col2,col3) values ('2013-01-01 12:00:00','2014-02-01 10:00:00'),('2013-01-01 12:00:00','2014-02-20 05:00:00');
insert into create_index_13(col2,col3) values (NULL,'2014-02-01 12:00:0'),(NULL,NULL);
create table create_index_14 (col1 bigint auto_increment primary key,col2 timestamp,col3 timestamp);
create unique index timestamp_index on create_index_14(col2);
insert into create_index_14(col2,col3) values ('2013-01-01 12:00:00','2014-02-01 10:00:00'),('2013-01-01 12:00:00','2014-02-20 05:00:00');
insert into create_index_14(col2,col3) values (NULL,'2014-02-01 12:00:0'),(NULL,NULL);
create table create_index_15 (col1 bigint primary key,col2 bool);
create unique index bool_index on create_index_15(col2);
-- @pattern
insert into create_index_15 values (1,TRUE),(2,FALSE),(3,TRUE);
insert into create_index_15 values (1,TRUE),(2,FALSE),(3,NULL);
select * from create_index_15;
-- blob/json/text type not support unique index
create table create_index_16 (col1 bigint primary key,col2 blob,col3 blob);
create unique index blob_index on create_index_16(col2);
drop table create_index_16;
create table create_index_17 (col1 bigint primary key,col2 json,col3 json);
create unique index json_index on create_index_17(col2);
drop table create_index_17;
create table create_index_18 (col1 bigint primary key,col2 text,col3 text);
create unique index text_index on create_index_18(col2);
drop table create_index_18;

--create unique index name test
create table create_index_name (col1 bigint not null auto_increment,col2 varchar(25),col3 int,col4 varchar(50),primary key (col1));
create unique index 123 on create_index_name(col2);
create unique index INdex_123 on create_index_name(col3);
create unique index abs@123.abc on create_index_name(col4);
create unique index index_123 on create_index_name(col3);
create unique index default on create_index_name(col3);
create unique index `default` on create_index_name(col3);

--create unique index more column
create table create_index_18 (col1 bigint not null auto_increment,col2 varchar(25),col3 int,col4 varchar(50),primary key (col1));
create unique index m1_index on create_index_18(col2,col3);
insert into create_index_18(col2,col3,col4)  select 'apple',1,'10';
insert into create_index_18(col2,col3,col4)  select 'apple',2,'11';
insert into create_index_18(col2,col3,col4)  select 'apple',2,'12';
insert into create_index_18(col2,col3,col4)  select NULL,NULL,'13';
select * from create_index_18;
drop index m1_index on create_index_18;
create unique index m2_index on create_index_18(col2,col3,col4);
truncate table create_index_18;
insert into create_index_18(col2,col3,col4)  select 'apple',1,'10';
insert into create_index_18(col2,col3,col4)  select 'apple',2,'11';
insert into create_index_18(col2,col3,col4)  select 'apple',2,'12';
insert into create_index_18(col2,col3,col4)  select NULL,NULL,'13';
insert into create_index_18(col2,col3,col4)  select 'apple',2,'12';
select * from create_index_18;
drop index m2_index on create_index_18;
create unique index m3_index on create_index_18(col2);
create unique index m4_index on create_index_18(col3);
create unique index m5_index on create_index_18(col4);
select * from create_index_18;
show create table create_index_18;
drop index m3_index on create_index_18;
drop index m4_index on create_index_18;
drop index m5_index on create_index_18;
drop table create_index_18;
create table create_index_18(col1 int,col2 char(15));
insert into create_index_18 values(2,'20');
insert into create_index_18 values(3,'20');
create unique index m1_index on create_index_18(col1);
create unique index m2_index on create_index_18(col1);
create unique index m3_index on create_index_18(col1,col2);
drop table create_index_18;
drop index m3_index on create_index_18;

--create index update/delete/truncate
create table create_index_19 (col1 bigint not null auto_increment,col2 varchar(25),col3 int,col4 varchar(50),primary key (col1));
create unique index col2 on create_index_19(col2);
insert into create_index_19(col2,col3,col4) values ('apple',1,'10'),('store',2,'11'),('bread',3,'12');
select * from create_index_19;
update create_index_19 set col2='chart' where col1=2;
select col2 from create_index_19;
update create_index_19 set col2='bread' where col1=1;
select * from create_index_19;
delete from create_index_19 where col2='apple';
select * from create_index_19;
truncate table create_index_19;
insert into create_index_19(col2,col3,col4) values ('apple',1,'10'),('store',2,'11'),('apple',3,'12');
select * from create_index_19;
drop table create_index_19;

--abnormal test: create not exists unique column,drop not exists index
create table create_index_20(col1 int,col2 char(15));
create unique index m3_index on create_index_20(col3);
drop index char_index on create_index_20;

--grant privilege
drop account if exists unique_test_account;
create account unique_test_account admin_name='admin' identified by '123456';
-- @session:id=1&user=unique_test_account:admin&password=123456
create user if not exists user_1 identified by '123456';
create role if not exists 'unique_priv_1';
grant create database,drop database,connect on account *  to unique_priv_1;
grant create table on database *  to unique_priv_1;
grant all on table *.* to unique_priv_1;
grant unique_priv_1 to user_1;
-- @session
-- @session:id=2&user=unique_test_account:user_1:unique_priv_1&password=123456
create database testdb;
use testdb;
create table create_index_01 (col1 bigint primary key,col2 varchar(25),col3 float,col4 varchar(50));
create unique index varchar_index on create_index_01(col2) comment 'create varchar index';
insert into create_index_01 values (1,'13456789872',20.23,'5678'),(2,'13873458290',100.00,'23'),(3,NULL,100.00,'23');
select col2,col4 from create_index_01;
drop index varchar_index on create_index_01;
drop database testdb;
-- @session
drop account if exists unique_test_account;

--transaction conflict
use unique_secondary_index;
create table trans_index_01 (col1 bigint primary key,col2 varchar(25),col3 float,col4 varchar(50));
start transaction;
create unique index varchar_index on trans_index_01(col2) comment 'create varchar index';
insert into trans_index_01 values (1,'13456789872',20.23,'5678'),(2,'13873458290',100.00,'23'),(3,NULL,100.00,'23');
-- @session:id=3{
use unique_secondary_index;
insert into trans_index_01 values (1,'13456789872',20.23,'5678'),(2,'13873458290',100.00,'23');
select * from trans_index_01;
-- @session}
select * from trans_index_01;
commit;
select * from trans_index_01;
drop table trans_index_01;

use unique_secondary_index;
create table trans_index_01 (col1 bigint primary key,col2 varchar(25),col3 float,col4 varchar(50));
create unique index varchar_index on trans_index_01(col2) comment 'create varchar index';
start transaction;
-- @session:id=3{
use unique_secondary_index;
drop index varchar_index on trans_index_01;
insert into trans_index_01 values (1,'13456789872',20.23,'5678'),(2,'13456789872',100.00,'23'),(3,NULL,100.00,'23');
-- @session}
insert into trans_index_01 values (1,'13456789872',20.23,'5678'),(2,'13456789872',100.00,'23'),(3,NULL,100.00,'23');
select * from trans_index_01;
commit;
select * from trans_index_01;
drop table trans_index_01;

-- secondary ddl
create table create_secondary_01 (col1 bigint primary key,col2 varchar(25) unique key,col3 float,col4 varchar(50));
insert into create_secondary_01 values (1,'13456789872',20.23,'5678'),(2,'13873458290',100.00,'23'),(3,NULL,100.00,'23');
create index varchar_second_index on create_secondary_01(col4) comment 'create varchar index';
show create table create_secondary_01;
insert into create_secondary_01 values (4,'13456789899',20.23,'5678'),(5,'13873458255',100.00,'23');
select * from create_secondary_01;
drop index varchar_second_index on create_secondary_01;
show create table create_secondary_01;
create table create_secondary_02 (col1 bigint,col2 char(25),col3 float,col4 char(50));
create index char_second_index on create_secondary_02(col2);
insert into create_secondary_02 values (1,'',20.23,'5678'),(2,'13873458290',100.00,'23');
select col2,col4 from create_secondary_02;
drop index char_second_index on create_secondary_02;
create table create_secondary_03 (col1 bigint auto_increment,col2 int default 1000,col3 float,col4 int);
create index int_second_index on create_secondary_03(col2);
insert into create_secondary_03(col3,col4) values (20.23,4090),(100.00,5678);
select * from create_secondary_03;
drop index int_second_index on create_secondary_03;
create table create_secondary_04 (col1 bigint,col2 int primary key,col3 float,col4 bigint);
create index bigint_index on create_secondary_04(col1);
insert into create_secondary_04 values (1,2,20.23,4090),(2,4,100.00,4091),(NULL,3,0.01,NULL);
select * from create_secondary_04;
create table create_secondary_05 (col1 smallint,col2 int primary key,col3 float,col4 smallint);
create index smallint_second_index on create_secondary_05(col1);
insert into create_secondary_05 values (1,2,20.23,4090),(1,4,100.00,4091);
select * from create_secondary_05;
create table create_secondary_06 (col1 tinyint,col2 int primary key,col3 float,col4 tinyint);
create index tinyint_second_index on create_secondary_06(col1);
insert into create_secondary_06 values (1,2,20.23,56),(1,4,100.00,90);
select * from create_secondary_06;
create table create_secondary_07 (col1 int unsigned,col2 int primary key,col3 float,col4 int unsigned);
create index int_unsigned_index on create_secondary_07(col1);
insert into create_secondary_07 values (1,2,20.23,56),(1,4,100.00,90);
show create table create_secondary_07;
select * from create_secondary_07;
create table create_secondary_08 (col1 bigint unsigned,col2 int primary key,col3 float,col4 bigint unsigned);
create index bigint_unsigned_index on create_secondary_08(col1);
insert into create_secondary_08 values (1,2,20.23,56),(2,4,100.00,41),(NULL,3,0.01,NULL);
select * from create_secondary_08;
create table create_secondary_09 (col1 bigint primary key,col2 decimal(16,8),col3 decimal(16,8));
create index decimal_index on create_secondary_09(col2);
insert into create_secondary_09 values (1000,20.23,20.00),(1200,0.23,20.10),(1100,NULL,NULL);
select * from create_secondary_09;
create table create_secondary_10 (col1 bigint primary key,col2 float,col3 float);
insert into create_secondary_10 values (1000,20.23,20.00),(1200,0.23,20.10),(1100,NULL,NULL);
create index float_index on create_secondary_10(col2);
select * from create_secondary_10;
create table create_secondary_11 (col1 bigint primary key,col2 double,col3 double);
create index double_index on create_secondary_11(col2);
insert into create_secondary_11 values (1000,20.23,20.00),(1200,20.23,0.10);
select * from create_secondary_11;
create table create_secondary_12(col1 bigint auto_increment primary key,col2 date,col3 date);
create index date_index on create_secondary_12(col2);
insert into create_secondary_12(col2,col3) values ('2013-01-01','2014-02-01'),('2013-01-01','2014-02-20');
create table create_secondary_13 (col1 bigint auto_increment primary key,col2 datetime,col3 datetime);
create index datetime_index on create_secondary_13(col2);
insert into create_secondary_13(col2,col3) values (NULL,'2014-02-01 12:00:0'),(NULL,NULL);
create table create_secondary_14 (col1 bigint auto_increment primary key,col2 timestamp,col3 timestamp);
create index timestamp_index on create_secondary_14(col2);
insert into create_secondary_14(col2,col3) values ('2013-01-01 12:00:00','2014-02-01 10:00:00'),('2013-01-01 12:00:00','2014-02-20 05:00:00');
create table create_secondary_15 (col1 bigint primary key,col2 bool);
create index bool_index on create_secondary_15(col2);
insert into create_secondary_15 values (1,TRUE),(2,FALSE),(3,TRUE);
select * from create_secondary_15;
-- blob/json/text type not support unique index
create table create_secondary_16 (col1 bigint primary key,col2 blob,col3 blob);
create index blob_index on create_secondary_16(col2);
drop table create_secondary_16;
create table create_secondary_17 (col1 bigint primary key,col2 json,col3 json);
create index json_index on create_secondary_17(col2);
drop table create_secondary_17;
create table create_secondary_18 (col1 bigint primary key,col2 text,col3 text);
create index text_index on create_secondary_18(col2);
drop table create_secondary_18;

--more column secondary key
drop table create_secondary_01;
create table create_secondary_01 (col1 bigint primary key,col2 varchar(25) unique key,col3 float,col4 varchar(50),col5 int);
create index secondary_key on create_secondary_01(col4,col5);
insert into create_secondary_01 values (1,'13456789872',20.23,'5678',99),(2,'13873458290',100.00,'23',9),(3,NULL,100.00,'23',99);
select * from create_secondary_01;
show create table create_secondary_01;
drop index secondary_key on create_secondary_01;
truncate table create_secondary_01;
show create table create_secondary_01;
drop table create_secondary_01;

--primary key create secondary key
create table create_secondary_01 (col1 bigint primary key,col2 varchar(25) unique key,col3 float,col4 varchar(50),col5 int);
create index secondary_key1 on create_secondary_01(col1);
create index secondary_key2 on create_secondary_01(col1,col2);
show create table create_secondary_01;
drop index secondary_key1 on create_secondary_01;
drop index secondary_key2 on create_secondary_01;
create index secondary_key1 on create_secondary_01(col4,col5);
create index secondary_key1 on create_secondary_01(col4,col5);
drop table create_secondary_01;

--anormal test
create table create_secondary_01 (col1 bigint primary key,col2 varchar(25) unique key,col3 float,col4 varchar(50),col5 int);
drop index secondary_key1 on create_secondary_01;
create unique index secondary_key1 on create_secondary_01(col1);
create index secondary_key1 on create_secondary_01(col1);
show create table create_secondary_01;