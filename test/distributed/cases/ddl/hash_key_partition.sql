-- key partition: char,varchar ; pk/not pk
create table p_table_01(col1 int,col2 varchar(25),col3 decimal(6,2))partition by key(col2)partitions 4;
insert into p_table_01 values (1,'mod',78.9),(2,'proto',0.34),(3,'mod',6.5),(4,'mode',9.0),(5,'make',662.9),(6,'io',88.92);
select * from `%!%p0%!%p_table_01`;
select * from `%!%p1%!%p_table_01`;
select * from `%!%p2%!%p_table_01`;
select * from `%!%p3%!%p_table_01`;
show create table p_table_01;
create table p_table_02(col1 int,col2 char(25),col3 decimal(6,3),primary key(col1,col2))partition by key(col2)partitions 2;
insert into p_table_02 values (1,'mod',78.9),(2,'proto',0.34),(3,'mod',6.5),(4,'mode',9.0),(5,'make',662.9),(6,'io',88.92);
select * from `%!%p0%!%p_table_02`;
select * from `%!%p1%!%p_table_02`;

-- key partition: char include null values
create table p_table_03(col1 int,col2 char(25),col3 decimal(4,2),primary key(col1,col2))partition by key(col2)partitions 4;
insert into p_table_03 values (1,'',78.9),(2,'proto',0.34),(3,'',6.5),(4,'mode',9.0),(5,'make',62.9),(6,'io',88.92);
select * from `%!%p0%!%p_table_03`;
select * from `%!%p1%!%p_table_03`;
select * from `%!%p2%!%p_table_03`;
select * from `%!%p3%!%p_table_03`;
show create table p_table_03;

-- key partition: varchar ; key() and duplicate key
create table p_table_04(col1 int,col2 char(25) primary key,col3 decimal(4,2))partition by key()partitions 8;
insert into p_table_04 values (1,'mod',78.9),(2,'proto',0.34),(3,'mod',6.5),(4,'mode',9.0),(5,'make',62.9),(6,'io',88.92);
insert into p_table_04 values (1,'mod',78.9),(2,'proto',0.34),(3,'rep',6.5),(4,'test',9.0),(5,'make',62.9),(6,'io',88.92);
select * from `%!%p0%!%p_table_04`;
select * from `%!%p1%!%p_table_04`;
select * from `%!%p2%!%p_table_04`;
select * from `%!%p3%!%p_table_04`;
select * from `%!%p4%!%p_table_04`;
select * from `%!%p5%!%p_table_04`;
select * from `%!%p6%!%p_table_04`;
select * from `%!%p7%!%p_table_04`;
create table p_table_05(col1 int,col2 char(25),col3 decimal(4,2),unique key k2(col2))partition by key()partitions 6;

-- key partition: char,  unique key
create table p_table_06(col1 int,col2 char(25),col3 decimal(6,3),unique key k2(col1,col2))partition by key(col2)partitions 4;
insert into p_table_06 values (1,'mod',78.9),(2,'proto',0.34),(3,'mod',6.5),(4,'mode',9.0),(5,'make',662.9),(6,'io',88.92);
select * from `%!%p0%!%p_table_06`;
select * from `%!%p1%!%p_table_06`;
select * from `%!%p2%!%p_table_06`;
select * from `%!%p3%!%p_table_06`;

-- abnormal test
create table p_table_07(col1 int,col2 char(25),col3 decimal(4,2),unique key k2(col1,col2))partition by key()partitions 8;
create table p_table_07(col1 int,col2 char(25),col3 decimal(4,2))partition by key()partitions 8;
create table p_table_07(col1 int,col2 char(25),col3 decimal(4,2),unique key k2(col1))partition by key(col2)partitions 8;
create table p_table_07(col1 int unsigned,col2 date, col3 varchar(25),primary key(col1,col2) ,unique key k1(col3))partition by key(col1)partitions 4;

--key partition: tinyint,tinyint unsigned,smallint,smallint unsigned
create table p_table_08(col1 tinyint,col2 varchar(25),col3 decimal(6,2))partition by key(col1)partitions 4;
insert into p_table_08 values (34,'mod',78.9),(34,'proto',0.34),(-80,'mod',6.5),(-80,'mode',9.0),(59,'make',62.9),(59,'io',88.92);
select * from `%!%p0%!%p_table_08`;
select * from `%!%p1%!%p_table_08`;
select * from `%!%p2%!%p_table_08`;
select * from `%!%p3%!%p_table_08`;
drop table p_table_08;
create table p_table_08(col1 tinyint unsigned,col2 varchar(25),col3 decimal(6,2))partition by key(col1)partitions 4;
insert into p_table_08 values (34,'mod',78.9),(34,'proto',0.34),(80,'mod',6.5),(80,'mode',9.0),(59,'make',62.9),(59,'io',88.92);
select * from `%!%p0%!%p_table_08`;
select * from `%!%p1%!%p_table_08`;
select * from `%!%p2%!%p_table_08`;
select * from `%!%p3%!%p_table_08`;
drop table p_table_08;
create table p_table_08(col1 smallint ,col2 varchar(25),col3 decimal(6,2))partition by key(col1)partitions 4;
insert into p_table_08 values (34,'mod',78.9),(34,'proto',0.34),(80,'mod',6.5),(80,'mode',9.0),(59,'make',62.9),(59,'io',88.92);
select * from `%!%p0%!%p_table_08`;
select * from `%!%p1%!%p_table_08`;
select * from `%!%p2%!%p_table_08`;
select * from `%!%p3%!%p_table_08`;
drop table p_table_08;
create table p_table_08(col1 smallint unsigned ,col2 varchar(25),col3 decimal(6,2))partition by key(col1)partitions 4;
insert into p_table_08 values (34,'mod',78.9),(34,'proto',0.34),(80,'mod',6.5),(80,'mode',9.0),(59,'make',62.9),(59,'io',88.92);
select * from `%!%p0%!%p_table_08`;
select * from `%!%p1%!%p_table_08`;
select * from `%!%p2%!%p_table_08`;
select * from `%!%p3%!%p_table_08`;

--key partition: int, not pk, key more columns
create table p_table_09(col1 int,col2 date, col3 varchar(25))partition by key(col1,col2)partitions 4;
insert into p_table_09 values (72,'1999-09-29','res1'),(60,'1999-10-01','opt1'),(72,'1999-09-29','res2'),(60,'1999-10-01','opt2'), (200,'1999-10-29','oop1'),(200,'1999-10-29','oop1');
select * from `%!%p0%!%p_table_09`;
select * from `%!%p1%!%p_table_09`;
select * from `%!%p2%!%p_table_09`;
select * from `%!%p3%!%p_table_09`;
insert into p_table_09 values (900,'1999-09-29','res1'),(900,'1999-09-29','opt1'),(1000,'1999-10-01','res2'),(1000,'1999-10-01','opt2'), (200,'1999-10-29','oop1'),(200,'1999-10-29','oop1');
select * from `%!%p0%!%p_table_09`;
select * from `%!%p1%!%p_table_09`;
select * from `%!%p2%!%p_table_09`;
select * from `%!%p3%!%p_table_09`;
show create table p_table_09;
--key partition: int unsigned,pk/unique key
create table p_table_10(col1 int unsigned,col2 date, col3 varchar(25),primary key(col1,col2))partition by key(col1)partitions 4;
insert into p_table_10 values (72,'1999-09-29','res1'),(60,'1999-10-01','opt1'),(72,'1999-10-02','res2'),(72,'1999-10-03','opt2'), (60,'1999-10-29','oop1'),(206,'1999-10-30','oop1');
select * from `%!%p0%!%p_table_10`;
select * from `%!%p1%!%p_table_10`;
select * from `%!%p2%!%p_table_10`;
select * from `%!%p3%!%p_table_10`;
select * from p_table_10 where col1<70;
create table p_table_11(col1 int unsigned,col2 date, col3 varchar(25),unique key k1(col1,col2))partition by key(col1)partitions 4;
insert into p_table_11 values (72,'1999-09-29','res1'),(60,'1999-10-01','opt1'),(72,'1999-10-02','res2'),(72,'1999-10-03','opt2'), (60,'1999-10-29','oop1'),(206,'1999-10-30','oop1');
select * from `%!%p0%!%p_table_11`;
select * from `%!%p1%!%p_table_11`;
select * from `%!%p2%!%p_table_11`;
select * from `%!%p3%!%p_table_11`;
select * from p_table_11;
--key partition: int unsigned,null value
create table p_table_12(col1 int unsigned,col2 date, col3 varchar(25),unique key k1(col1,col2))partition by key(col1)partitions 4;
insert into p_table_12 values (72,'1999-09-29','res1'),(NULL,'1999-10-01','opt1'),(72,'1999-10-02','res2'),(72,'1999-10-03','opt2'), (NULL,'1999-10-29','oop1'),(206,'1999-10-30','oop1');
select * from `%!%p0%!%p_table_12`;
select * from `%!%p1%!%p_table_12`;
select * from `%!%p2%!%p_table_12`;
select * from `%!%p3%!%p_table_12`;
select * from p_table_12 where col2='1999-09-29';

-- key partition: int ; key()
create table p_table_13(col1 int primary key auto_increment,col2 char(25),col3 decimal(4,2))partition by key()partitions 2;
insert into p_table_13(col2,col3) values ('mod',78.9),('proto',0.34),('mod',6.5),('mode',9.0),('make',62.9),('io',88.92);
select * from `%!%p0%!%p_table_13`;
select * from `%!%p1%!%p_table_13`;

-- key partition: bigint,more columns
create table p_table_14(col1 bigint,col2 date,col3 varchar(25),col4 decimal(6,4))partition by key(col1,col2,col3)partitions 8;
insert into p_table_14 values (1000,'1999-09-29','res1',0.12),(6000,'1999-10-01','opt1',0.89),(729,'1999-10-02','res2',0.32),(6000,'1999-10-01','opt1',0.64), (6000,'1999-10-01','opt1',0.55),(206,'1999-10-30','oop1',0.87);
insert into p_table_14 values (1000,'1999-09-29','res1',0.12),(6000,'1999-10-01','opt1',0.89),(1000,'1999-09-29','res1',0.32),(206,'1999-10-30','oop1',0.64), (1000,'1999-09-29','res1',0.55),(206,'1999-10-30','oop1',0.87);
select * from `%!%p0%!%p_table_14`;
select * from `%!%p1%!%p_table_14`;
select * from `%!%p2%!%p_table_14`;
select * from `%!%p3%!%p_table_14`;
select * from `%!%p4%!%p_table_14`;
select * from `%!%p5%!%p_table_14`;
select * from `%!%p6%!%p_table_14`;
select * from `%!%p7%!%p_table_14`;
select * from p_table_14 where col3 in ('opt1','res1');
insert into p_table_14 values (30,'1970-01-01','use',5.7), (30,'1970-01-01','kkk',9.8);
select * from `%!%p0%!%p_table_14`;
select * from `%!%p1%!%p_table_14`;
select * from `%!%p2%!%p_table_14`;
select * from `%!%p3%!%p_table_14`;
select * from `%!%p4%!%p_table_14`;
select * from `%!%p5%!%p_table_14`;
select * from `%!%p6%!%p_table_14`;
select * from `%!%p7%!%p_table_14`;
update p_table_14 set col2='1999-01-01' where col2<'1999-10-02';
select * from p_table_14;
select * from `%!%p0%!%p_table_14`;
select * from `%!%p1%!%p_table_14`;
select * from `%!%p2%!%p_table_14`;
select * from `%!%p3%!%p_table_14`;
select * from `%!%p4%!%p_table_14`;
select * from `%!%p5%!%p_table_14`;
select * from `%!%p6%!%p_table_14`;
select * from `%!%p7%!%p_table_14`;
update p_table_14 set col4=0.999 where col1=1000;
select * from p_table_14 where col1>1000 ;
delete from p_table_14 where col3 in ('res1','res2');
select * from p_table_14;
select * from `%!%p0%!%p_table_14`;
select * from `%!%p1%!%p_table_14`;
select * from `%!%p2%!%p_table_14`;
select * from `%!%p3%!%p_table_14`;
select * from `%!%p4%!%p_table_14`;
select * from `%!%p5%!%p_table_14`;
select * from `%!%p6%!%p_table_14`;
select * from `%!%p7%!%p_table_14`;
truncate table p_table_14;

-- key partition: bigint
create table p_table_15(col1 bigint auto_increment,col2 date,col3 varchar(25),col4 decimal(6,4),primary key(col1,col2,col3))partition by key(col2)partitions 4;
insert into p_table_15(col2,col3,col4) values ('1999-09-29','res1',0.12),('1999-10-02','res2',0.32),('1999-10-01','opt1',0.64), ('1999-10-01','opt2',0.55),('1999-09-29','oop1',0.87);
insert into p_table_15(col2,col3,col4) values ('1999-09-29','res1',0.12),('1999-10-02','res2',0.32),('1999-10-01','opt1',0.64), ('1999-10-01','opt2',0.55),('1999-09-29','oop1',0.87);
select * from p_table_15;
select * from `%!%p0%!%p_table_15`;
select * from `%!%p1%!%p_table_15`;
select * from `%!%p2%!%p_table_15`;
select * from `%!%p3%!%p_table_15`;
truncate table p_table_15;
insert into p_table_15(col2,col3,col4) values ('1999-09-29','res1',0.12),('1999-10-02','res2',0.32),('1999-10-01','opt1',0.64), ('1999-10-01','opt2',0.55),('1999-09-29','oop1',0.87);
insert into p_table_15(col2,col3,col4) values ('1999-09-29','res1',0.12),('1999-10-02','res2',0.32),('1999-10-01','opt1',0.64), ('1999-10-01','opt2',0.55),('1999-09-29','oop1',0.87);
select * from p_table_15;
select * from `%!%p0%!%p_table_15`;
select * from `%!%p1%!%p_table_15`;
select * from `%!%p2%!%p_table_15`;
select * from `%!%p3%!%p_table_15`;
update  p_table_15 set col1=100 where col2='1999-09-29';
update  p_table_15 set col2='2022-10-01' where col1>4;
select * from p_table_15;
select * from `%!%p0%!%p_table_15`;
select * from `%!%p1%!%p_table_15`;
select * from `%!%p2%!%p_table_15`;
select * from `%!%p3%!%p_table_15`;
delete from p_table_15 where col2='1999-09-29';
select * from p_table_15;
select * from `%!%p0%!%p_table_15`;
select * from `%!%p1%!%p_table_15`;
select * from `%!%p2%!%p_table_15`;
select * from `%!%p3%!%p_table_15`;
delete from p_table_15;
select * from p_table_15;
select * from `%!%p0%!%p_table_15`;
select * from `%!%p1%!%p_table_15`;
select * from `%!%p2%!%p_table_15`;
select * from `%!%p3%!%p_table_15`;

-- abnormal test: duplicate value, out of range
create table p_table_16(col1 bigint,col2 date,col3 varchar(25),col4 decimal(6,4),primary key(col1,col2,col3))partition by key(col2)partitions 8;
insert into p_table_16 values (1000,'1999-09-29','res1',0.12),(6000,'1999-10-01','opt1',0.89),(729,'1999-10-02','res2',0.32),(6000,'1999-10-01','opt1',0.64), (6000,'1999-10-01','opt1',0.55),(206,'1999-10-30','oop1',0.87);
insert into p_table_16 values (1000,'0001-09-29','res1',0.12),(6000,'1999-10-11','opt1',0.89);
select * from p_table_16;

-- key partition: bigint unsigned
create table p_table_temp(col1 bigint unsigned auto_increment,col2 date,col3 varchar(25),col4 decimal(6,4),primary key(col1,col2,col3));
insert into p_table_temp(col2,col3,col4) values ('1999-09-29','res1',0.12),('1999-10-02','res2',0.32),('1999-10-01','opt1',0.64), ('1999-10-01','opt2',0.55),('1999-09-29','oop1',0.87);
insert into p_table_temp(col2,col3,col4) values ('1999-09-29','res1',0.12),('1999-10-02','res2',0.32),('1999-10-01','opt1',0.64), ('1999-10-01','opt2',0.55),('1999-09-29','oop1',0.87);
create table p_table_17(col1 bigint unsigned auto_increment,col2 date,col3 varchar(25),col4 decimal(6,4),primary key(col1,col2,col3))partition by key(col2)partitions 4;
insert into p_table_17 select col1,col2,col3,col4 from p_table_temp;
select * from p_table_17;
select * from `%!%p0%!%p_table_17`;
select * from `%!%p1%!%p_table_17`;
select * from `%!%p2%!%p_table_17`;
select * from `%!%p3%!%p_table_17`;

-- key partition: decimal,float,double
-- @bvt:issue#10064
create table p_table_18(col1 bigint,col2 varchar(25),col3 decimal(6,4))partition by key(col3)partitions 2;
insert into p_table_18 values(932,'rel',0.98),(76,'opp',8.94),(823,'var',0.98);
select * from p_table_18;
select * from `%!%p0%!%p_table_18`;
select * from `%!%p1%!%p_table_18`;
drop table p_table_18;
create table p_table_18(col1 bigint,col2 varchar(25),col3 float)partition by key(col3)partitions 2;
insert into p_table_18 values(932,'rel',0.98),(76,'opp',8.94),(823,'var',0.98);
select * from p_table_18;
select * from `%!%p0%!%p_table_18`;
select * from `%!%p1%!%p_table_18`;
drop table p_table_18;
create table p_table_18(col1 bigint,col2 varchar(25),col3 double)partition by key(col3)partitions 2;
insert into p_table_18 values(932,'rel',0.98),(76,'opp',8.94),(823,'var',0.98);
select * from p_table_18;
select * from `%!%p0%!%p_table_18`;
select * from `%!%p1%!%p_table_18`;
-- @bvt:issue

-- key partition: date,datetime,timestamp
create table p_table_19(col1 int,col2 date,col3 varchar(25))partition by key(col2)partitions 6;
load data infile '$resources/load_data/key_partition_data.csv' into table p_table_19;
select * from p_table_19 where col1>1500;
select * from `%!%p0%!%p_table_19`;
select * from `%!%p1%!%p_table_19`;
select * from `%!%p2%!%p_table_19`;
select * from `%!%p3%!%p_table_19`;
select * from `%!%p4%!%p_table_19`;
select * from `%!%p5%!%p_table_19`;
select count(*) from p_table_19;
drop table p_table_19;
create table p_table_19(col1 int auto_increment,col2 date,col3 varchar(25),primary key(col1,col2))partition by key(col2)partitions 3;
load data infile '$resources/load_data/key_partition_data.csv' into table p_table_19;
select * from p_table_19 where col1>1500;
select * from `%!%p0%!%p_table_19`;
select * from `%!%p1%!%p_table_19`;
select * from `%!%p2%!%p_table_19`;
drop table p_table_19;
create table p_table_19(col1 int auto_increment,col2 date,col3 varchar(25),primary key(col1,col2,col3))partition by key()partitions 3;
insert into p_table_19(col2,col3) values('2023-01-01','a'),('2023-01-02','b'),('2023-01-03','c'),('2023-01-04','d');
select * from p_table_19;
select * from `%!%p0%!%p_table_19`;
select * from `%!%p1%!%p_table_19`;
select * from `%!%p2%!%p_table_19`;

-- key partition: binary,varbinary,blob
create table p_table_001(col1 int,col2 binary(50))partition by key(col2)partitions 4;
insert into p_table_001 values (12,'var1'),(56,'sstt'),(78,'var2'),(90,'lop');
select * from `%!%p0%!%p_table_001`;
select * from `%!%p1%!%p_table_001`;
select * from `%!%p2%!%p_table_001`;
select * from `%!%p3%!%p_table_001`;
create table p_table_002(col1 int,col2 varbinary(50))partition by key(col2)partitions 4;
insert into p_table_002 values (12,'var1'),(56,'sstt'),(78,'var2'),(90,'lop');
select * from `%!%p0%!%p_table_002`;
select * from `%!%p1%!%p_table_002`;
select * from `%!%p2%!%p_table_002`;
select * from `%!%p3%!%p_table_002`;
create table p_table_003(col1 int,col2 blob)partition by key(col2)partitions 4;
insert into p_table_003 values (12,'var1'),(56,'sstt'),(78,'var2'),(90,'lop');
select * from `%!%p0%!%p_table_003`;
select * from `%!%p1%!%p_table_003`;
select * from `%!%p2%!%p_table_003`;
select * from `%!%p3%!%p_table_003`;
create table p_table_004(col1 int,col2 text)partition by key(col2)partitions 4;
insert into p_table_004 values (12,'var1'),(56,'sstt'),(78,'var2'),(90,'lop');
select * from `%!%p0%!%p_table_004`;
select * from `%!%p1%!%p_table_004`;
select * from `%!%p2%!%p_table_004`;
select * from `%!%p3%!%p_table_004`;

-- key partition abnormal type : json
create table p_table_non(col1 int,col2 json)partition by key(col2)partitions 4;

-- key partition:
-- @bvt:issue#10081
create temporary table p_table_non(col1 int,col2 varchar(25))partition by key(col2)partitions 4;
-- @bvt:issue
create view p_view as select * from p_table_19;
select * from p_view;
drop view p_view;
CREATE TABLE IF NOT EXISTS p_table_20(
    `id` INT,
    `act_name` VARCHAR(20) NOT NULL,
    `spu_id` VARCHAR(30) NOT NULL,
    `uv`  BIGINT NOT NULL,
    `update_time` date default '2020-10-10' COMMENT 'lastest time',
    PRIMARY KEY ( `id`, `act_name`)
)partition by key(`act_name`) partitions 3;
insert into p_table_20 values (1,'beijing','001',1,'2021-01-03'),(2,'beijing','002',2,'2022-09-23'),(3,'guangzhou','003',3,'2022-09-23'),(3,'shanghai','003',3,'2022-09-23');
insert into p_table_20 values (4,'shenzheng','004',4,'2021-05-28'),(1,'beijing','010',5,'2022-10-23') on duplicate key update id=id*10;
select * from p_table_20;
select * from `%!%p0%!%p_table_20`;
select * from `%!%p1%!%p_table_20`;
select * from `%!%p2%!%p_table_20`;
drop table p_table_20;

create table p_table_20(col1 int,col2 date, col3 varchar(25))partition by key(col1,col2)partitions 4;
start transaction;
insert into p_table_20 values (72,'1999-09-29','res1'),(60,'1999-10-01','opt1'),(72,'1999-09-29','res2'),(60,'1999-10-01','opt2'), (200,'1999-10-29','oop1'),(200,'1999-10-29','oop1');
-- @session:id=2{
use hash_key_partition;
select * from p_table_20;
-- @session}
select * from `%!%p0%!%p_table_20`;
select * from `%!%p1%!%p_table_20`;
select * from `%!%p2%!%p_table_20`;
select * from `%!%p3%!%p_table_20`;
rollback;
select * from p_table_20;
select * from `%!%p0%!%p_table_20`;
select * from `%!%p1%!%p_table_20`;
select * from `%!%p2%!%p_table_20`;
select * from `%!%p3%!%p_table_20`;

drop table p_table_20;
create table p_table_20(col1 int,col2 date, col3 varchar(25))partition by key(col1,col2)partitions 4;
begin;
insert into p_table_20 values (72,'1999-09-29','res1'),(60,'1999-10-01','opt1'),(72,'1999-09-29','res2'),(60,'1999-10-01','opt2'), (200,'1999-10-29','oop1'),(200,'1999-10-29','oop1');
-- @session:id=2{
select * from p_table_20;
-- @session}
select * from `%!%p0%!%p_table_20`;
select * from `%!%p1%!%p_table_20`;
select * from `%!%p2%!%p_table_20`;
select * from `%!%p3%!%p_table_20`;
commit;
select * from p_table_20;
select * from `%!%p0%!%p_table_20`;
select * from `%!%p1%!%p_table_20`;
select * from `%!%p2%!%p_table_20`;
select * from `%!%p3%!%p_table_20`;

-- hash partition: int,pk/not pk
create table p_hash_table_01(col1 int not null,col2 varchar(30),col3 date not null default '1970-01-01',col4 int)partition by hash(col4) partitions 4;
insert into p_hash_table_01 values (-120,'78',NULL,90);
insert into p_hash_table_01 values (-120,'78','2020-12-15',90),(84,'334','2021-01-01',34),(20,'55','2021-01-01',72),(-120,'45','2023-10-09',99),(84,'3','2022-11-01',34),(200,'55','2021-08-11',72);
select * from `%!%p0%!%p_hash_table_01`;
select * from `%!%p1%!%p_hash_table_01`;
select * from `%!%p2%!%p_hash_table_01`;
select * from `%!%p3%!%p_hash_table_01`;
show create table p_hash_table_01;
create table p_hash_table_02(col1 int not null,col2 varchar(30),col3 date default '1970-01-01',col4 int,primary key(col1,col2))partition by hash(col1) partitions 4;
insert into p_hash_table_02 values (-120,'78',NULL,90),(84,'334','2021-01-01',34),(20,'55','2021-01-01',72),(-120,'45',NULL,99),(84,'3','2022-11-01',34),(200,'55','2021-08-11',72);
select * from `%!%p0%!%p_hash_table_02`;
select * from `%!%p1%!%p_hash_table_02`;
select * from `%!%p2%!%p_hash_table_02`;
select * from `%!%p3%!%p_hash_table_02`;

-- hash partition: bigint,key（expr）
create table p_hash_table_03(col1 bigint auto_increment,col2 varchar(30),col3 date default '1970-01-01',col4 int,unique key k1(col1,col2))partition by hash(col1) partitions 4;
insert into p_hash_table_03 values (-120,'78',NULL,90),(84,'334','2021-01-01',34),(20,'55','2021-01-01',72),(-120,'45',NULL,99),(84,'3','2022-11-01',34),(200,'55','2021-08-11',72);
select * from `%!%p0%!%p_hash_table_03`;
select * from `%!%p1%!%p_hash_table_03`;
select * from `%!%p2%!%p_hash_table_03`;
select * from `%!%p3%!%p_hash_table_03`;
show create table p_hash_table_03;
create table p_hash_table_04(col1 bigint ,col2 date,col3 varchar(30))partition by hash(year(col2)) partitions 4;
load data infile '$resources/load_data/key_partition_data.csv' into table p_hash_table_04;
select * from `%!%p0%!%p_hash_table_04`;
select * from `%!%p1%!%p_hash_table_04`;
select * from `%!%p2%!%p_hash_table_04`;
select * from `%!%p3%!%p_hash_table_04`;
select * from p_hash_table_04 where col1>1050;
update p_hash_table_04 set col2='2023-09-05' where col1=1000;
update p_hash_table_04 set col1=999 where col2='2010-08-12';
select * from `%!%p0%!%p_hash_table_04`;
select * from `%!%p1%!%p_hash_table_04`;
select * from `%!%p2%!%p_hash_table_04`;
select * from `%!%p3%!%p_hash_table_04`;
delete from p_hash_table_04 where col2='2010-08-12';
select * from `%!%p0%!%p_hash_table_04`;
select * from `%!%p1%!%p_hash_table_04`;
select * from `%!%p2%!%p_hash_table_04`;
select * from `%!%p3%!%p_hash_table_04`;
truncate table p_hash_table_04;
select * from p_hash_table_04;
select * from `%!%p0%!%p_hash_table_04`;
select * from `%!%p1%!%p_hash_table_04`;

-- hash partition: null values
create table p_hash_table_05(col1 bigint unsigned,col2 varchar(30),col3 datetime)partition by hash(year(col3)) partitions 3;
insert into p_hash_table_05 values(1,'a','2023-04-24 23:00:00'), (8,'b','2023-04-24 13:00:00'),(8,'b',NULL);
select * from `%!%p0%!%p_hash_table_05`;
select * from `%!%p1%!%p_hash_table_05`;
select * from `%!%p2%!%p_hash_table_05`;
select * from p_hash_table_05;

-- hash partition: tinyint,tinyint unsigned
create table p_hash_table_06(col1 tinyint,col2 varchar(30))partition by hash(col1) partitions 2;
insert into p_hash_table_06 values (10,'nb'),(10,'bv'),(12,'nb'),(12,'bv'),(13,'nb'),(14,'bv');
select * from `%!%p0%!%p_hash_table_06`;
select * from `%!%p1%!%p_hash_table_06`;
create table p_hash_table_07(col1 tinyint unsigned,col2 varchar(30))partition by hash(col1) partitions 2;
insert into p_hash_table_07 values (10,'nb'),(10,'bv'),(12,'nb'),(12,'bv'),(13,'nb'),(14,'bv');
select * from `%!%p0%!%p_hash_table_07`;
select * from `%!%p1%!%p_hash_table_07`;
select * from p_hash_table_07;

-- hash partition: key(expr)
-- @bvt:issue#10077
create table p_hash_table_08(col1 tinyint,col2 varchar(30),col3 decimal(6,3))partition by hash(ceil(col3)) partitions 2;
insert into p_hash_table_08 values (10,'nb',35.5),(10,'bv',35.45),(12,'nb',30.09),(12,'nb',30.23);
select * from `%!%p0%!%p_hash_table_08`;
select * from `%!%p1%!%p_hash_table_08`;
select * from p_hash_table_08;
-- @bvt:issue
drop table if exists p_hash_table_08;
create table p_hash_table_08(col1 tinyint,col2 varchar(30),col3 decimal(6,3))partition by hash(col1*100)partitions 2;
insert into p_hash_table_08 values (10,'nb',35.5),(10,'bv',35.45),(12,'nb',30.09),(12,'nb',30.23);
select * from `%!%p0%!%p_hash_table_08`;
select * from `%!%p1%!%p_hash_table_08`;
select * from p_hash_table_08;

-- abnormal test
drop table if exists p_hash_table_03;
create table p_hash_table_03(col1 bigint auto_increment,col2 varchar(30),col3 date default '1970-01-01',col4 int,primary key(col1,col2),unique key k1(col4))partition by hash(col1) partitions 4;
-- @bvt:issue#10080
create table p_hash_table_03(col1 bigint ,col2 date default '1970-01-01',col3 varchar(30))partition by hash(year(col3)) partitions 8;
-- @bvt:issue
create table p_hash_table_06(col1 tinyint,col2 varchar(30),col3 decimal(6,3))partition by hash(col3) partitions 2;
create table p_hash_table_06(col1 tinyint,col2 varchar(30),col3 blob)partition by hash(col3) partitions 2;
create table p_hash_table_06(col1 tinyint,col2 varchar(30))partition by hash(col2) partitions 2;
create table p_hash_table_06(col1 tinyint,col2 varchar(30),col3 json)partition by hash(col3) partitions 2;
create table p_hash_table_06(col1 tinyint,col2 varchar(30),col3 Binary)partition by hash(col3) partitions 2;
create table p_hash_table_06(col1 tinyint,col2 varchar(30),col3 varbinary(25))partition by hash(col3) partitions 2;
create table p_hash_table_06(col1 tinyint,col2 varchar(30),col3 date)partition by hash(col3) partitions 2;
create table p_hash_table_06(col1 tinyint,col2 varchar(30),col3 datetime)partition by hash(col3) partitions 2;
create table p_hash_table_06(col1 tinyint,col2 varchar(30),col3 timestamp)partition by hash(col3) partitions 2;
create table p_hash_table_06(col1 tinyint,col2 varchar(30),col3 varchar(25))partition by hash(col3) partitions 2;
create table p_hash_table_06(col1 tinyint,col2 varchar(30),col3 char(25))partition by hash(col3) partitions 2;
create table p_hash_table_06(col1 tinyint,col2 varchar(30),col3 bool)partition by hash(col3) partitions 2;
create table p_hash_table_08(col1 tinyint,col2 varchar(30),col3 decimal(6,3))partition by hash(col1*100/3)partitions 2;

create table p_hash_table_09(col1 bigint auto_increment,col2 varchar(30),col3 date default '1970-01-01',col4 int,unique key k1(col1,col2))partition by hash(col1) partitions 4;
start transaction ;
insert into p_hash_table_09 values (-120,'78',NULL,90),(84,'334','2021-01-01',34),(20,'55','2021-01-01',72),(-120,'45',NULL,99),(84,'3','2022-11-01',34),(200,'55','2021-08-11',72);
-- @session:id=2{
use hash_key_partition;
select * from p_hash_table_09;
-- @session}
select * from p_hash_table_09;
select * from `%!%p0%!%p_hash_table_09`;
select * from `%!%p1%!%p_hash_table_09`;
select * from `%!%p2%!%p_hash_table_09`;
select * from `%!%p3%!%p_hash_table_09`;
rollback;
select * from p_hash_table_09;
select * from `%!%p0%!%p_hash_table_09`;
select * from `%!%p1%!%p_hash_table_09`;
select * from `%!%p2%!%p_hash_table_09`;
select * from `%!%p3%!%p_hash_table_09`;
drop table p_hash_table_09;

create table p_hash_table_09(col1 bigint auto_increment,col2 varchar(30),col3 date default '1970-01-01',col4 int,unique key k1(col1,col2))partition by hash(col1) partitions 4;
start transaction ;
insert into p_hash_table_09 values (-120,'78',NULL,90),(84,'334','2021-01-01',34),(20,'55','2021-01-01',72),(-120,'45',NULL,99),(84,'3','2022-11-01',34),(200,'55','2021-08-11',72);
-- @session:id=2{
use hash_key_partition;
select * from p_hash_table_09;
-- @session}
select * from p_hash_table_09;
select * from `%!%p0%!%p_hash_table_09`;
select * from `%!%p1%!%p_hash_table_09`;
select * from `%!%p2%!%p_hash_table_09`;
select * from `%!%p3%!%p_hash_table_09`;
commit;
select * from p_hash_table_09;
select * from `%!%p0%!%p_hash_table_09`;
select * from `%!%p1%!%p_hash_table_09`;
select * from `%!%p2%!%p_hash_table_09`;
select * from `%!%p3%!%p_hash_table_09`;

create table p_hash_table_10(col1 bigint ,col2 date,col3 varchar(30))partition by hash(to_days(col2)) partitions 4;
load data infile '$resources/load_data/key_partition_data.csv' into table p_hash_table_10;
select * from `%!%p0%!%p_hash_table_10`;
select * from `%!%p1%!%p_hash_table_10`;
select * from `%!%p2%!%p_hash_table_10`;
select * from `%!%p3%!%p_hash_table_10`;
drop table p_hash_table_10;
create table p_hash_table_10(col1 bigint ,col2 date,col3 varchar(30))partition by hash(to_seconds(col2)) partitions 4;
load data infile '$resources/load_data/key_partition_data.csv' into table p_hash_table_10;
select * from `%!%p0%!%p_hash_table_10`;
select * from `%!%p1%!%p_hash_table_10`;
select * from `%!%p2%!%p_hash_table_10`;
select * from `%!%p3%!%p_hash_table_10`;
