-- @skip:issue#8498
CREATE TABLE IF NOT EXISTS indup_00(
    `id` INT UNSIGNED,
    `act_name` VARCHAR(20) NOT NULL,
    `spu_id` VARCHAR(30) NOT NULL,
    `uv`  BIGINT NOT NULL,
    `update_time` date default '2020-10-10' COMMENT 'lastest time',
    unique key idx_act_name_spu_id (act_name,spu_id)
);
insert into indup_00 values (1,'beijing','001',1,'2021-01-03'),(2,'shanghai','002',2,'2022-09-23'),(3,'guangzhou','003',3,'2022-09-23');
select * from indup_00;

-- insert unique index duplicate data part,update value() and insert
insert into indup_00 values (4,'shenzheng','004',4,'2021-05-28'),(5,'beijing','010',5,'2022-10-23') on duplicate key update `act_name`=VALUES(`act_name`), `spu_id`=VALUES(`spu_id`), `uv`=VALUES(`uv`);
select * from indup_00;
-- insert unique index duplicate data all,update
insert into indup_00 values (6,'shanghai','002',21,'1999-09-23'),(7,'guangzhou','003',31,'1999-09-23') on duplicate key update `act_name`=VALUES(`act_name`), `spu_id`=VALUES(`spu_id`), `uv`=VALUES(`uv`);
select * from indup_00;
-- insert unique index duplicate data
insert into indup_00 values (8,'shanghai','002',21,'1999-09-23') on duplicate key update `act_name`=NULL;
select * from indup_00;
-- insert no duplicate data ,insert new data success
insert into indup_00 values (9,'shanxi','005',4,'2022-10-08'),(10,'shandong','006',6,'2022-11-22') on duplicate key update `act_name`='Hongkong';
select * from indup_00;
insert into indup_00 values (10,'xinjiang','008',7,NULL),(11,'hainan','009',8,NULL) on duplicate key update `act_name`='Hongkong';
select * from indup_00;

-- @bvt:issue#8498
CREATE TABLE IF NOT EXISTS indup_01(
    `id` INT UNSIGNED AUTO_INCREMENT,
    `act_name` VARCHAR(20) NOT NULL,
    `spu_id` VARCHAR(30) NOT NULL,
    `uv`  BIGINT NOT NULL,
    `update_time` date default '2020-10-10' COMMENT 'lastest time',
    PRIMARY KEY ( `id` ),
    unique key idx_act_name_spu_id (act_name,spu_id)
);
insert into indup_01(act_name,spu_id,uv,update_time) values ('beijing','001',1,'2021-01-03'),('shanghai','002',2,'2022-09-23'),('guangzhou','003',3,'2022-09-23');
select * from indup_01;

-- insert unique index duplicate data part,update value() and insert
insert into indup_01(act_name,spu_id,uv,update_time)values ('shenzheng','004',4,'2021-05-28'),('beijing','010',5,'2022-10-23') on duplicate key update `act_name`=VALUES(`act_name`), `spu_id`=VALUES(`spu_id`), `uv`=VALUES(`uv`);
select * from indup_01;
-- insert unique index duplicate data all,update
insert into indup_01(act_name,spu_id,uv,update_time)values ('shanghai','002',21,'1999-09-23'),('guangzhou','003',31,'1999-09-23') on duplicate key update `act_name`=VALUES(`act_name`), `spu_id`=VALUES(`spu_id`), `uv`=VALUES(`uv`);
select * from indup_01;
-- insert unique index duplicate data
insert into indup_01(act_name,spu_id,uv,update_time)values ('shanghai','002',21,'1999-09-23') on duplicate key update `act_name`=NULL;
select * from indup_01;
-- insert no duplicate data ,insert new data success
insert into indup_01(act_name,spu_id,uv,update_time)values ('shanxi','005',4,'2022-10-08'),('shandong','006',6,'2022-11-22') on duplicate key update `act_name`='Hongkong';
select * from indup_01;
insert into indup_01 values (10,'xinjiang','008',7,NULL),(11,'hainan','009',8,NULL) on duplicate key update `act_name`='Hongkong';
select * from indup_01;
-- @bvt:issue

CREATE TABLE IF NOT EXISTS indup_02(
    col1 INT ,
    col2 VARCHAR(20) NOT NULL,
    col3 VARCHAR(30) NOT NULL,
    col4  BIGINT default 30,
    PRIMARY KEY ( col1 )
);
insert into indup_02 values (1,'apple','left',NULL),(2,'bear','right',1000);
select * from indup_02;
--insert primary key duplicate data,update col=expression
insert into indup_02 select 1,'banana','lower',NULL on duplicate key update col1=col1*10;
select * from indup_02;
--insert primary key duplicate data part,update and insert
insert into indup_02(col1,col2,col3) values(2,'wechat','tower'),(3,'paper','up') on duplicate key update col1=col1+20,col3=values(col3);
select * from indup_02;
--insert primary key duplicate data,after update data pk conflict old data
insert into indup_02 values(3,'aaa','bbb',30) on duplicate key update col1=col1+7;
select * from indup_02;
--insert primary key duplicate data, update data pk conflict other insert data
insert into indup_02 values(3,'aaa','bbb',30),(30,'abc','abc',10),(11,'a1','b1',300) on duplicate key update col1=col1*10,col4=0;
select * from indup_02;
--insert into select from table duplicate update,update col=function(col) col=constant
create table indup_tmp(col1 int,col2 varchar(20),col3 varchar(20));
insert into indup_tmp values (1,'apple','left'),(2,'bear','right'),(3,'paper','up'),(10,'wine','down'),(300,'box','high');
insert into indup_02(col1,col2,col3) select col1,col2,col3 from  indup_tmp on duplicate key update indup_02.col3=left(indup_02.col3,2),col2='wow';
select * from indup_02;
delete from indup_02;
select * from indup_02;

--insert primary key no duplicate data
insert into indup_02(col1,col2,col3) values(6,'app','uper'),(7,'light','') on duplicate key update col2='';
select * from indup_02;

CREATE TABLE IF NOT EXISTS indup_03(
    col1 varchar(25) ,
    col2 VARCHAR(20) NOT NULL,
    col3 VARCHAR(30) ,
    col4  BIGINT default 30,
    PRIMARY KEY (col1)
);
insert into indup_03 values ('1','apple','left',NULL),('2','bear','right',1000);
-- insert primary key duplicate data and new data
insert into indup_03(col1,col2,col3) values(3,'paper','up'),('2','bear','right',1000),('1','sofa','high',NULL) on duplicate key update col2=values(col2),col3=values(col3);
select * from indup_03;
-- insert and update not null col
insert into indup_03(col1,col2,col3) values(4,NULL,NULL) on duplicate key update col2=values(col2),col3=values(col3);
insert into indup_03(col1,col2,col3) values(3,NULL,NULL) on duplicate key update col2=values(col2),col3=values(col3);
select * from indup_03;
--update pk constraint
insert into indup_03(col1,col2,col3) values(2,'bear','left') on duplicate key update col1=1;
-- update null/''/constant/expression
insert into indup_03(col1,col2,col3) values(1,'apple','') on duplicate key update col3='constant';
select * from indup_03;
insert into indup_03(col1,col2,col3) values(1,'apple','uuuu') on duplicate key update col3=NULL;
select * from indup_03;
insert into indup_03(col1,col2,col3) values(1,'apple','uuuu') on duplicate key update col3='';
select * from indup_03;
insert into indup_03(col1,col2,col3) values(1,'apple','uuuu') on duplicate key update col1=2+3;
select * from indup_03;

CREATE TABLE IF NOT EXISTS indup_04(
    `id` INT,
    `act_name` VARCHAR(20) NOT NULL,
    `spu_id` VARCHAR(30) NOT NULL,
    `uv`  BIGINT NOT NULL,
    `update_time` date default '2020-10-10' COMMENT 'lastest time',
    PRIMARY KEY ( `id`, `act_name`)
);
insert into indup_04 values (1,'beijing','001',1,'2021-01-03'),(2,'shanghai','002',2,'2022-09-23'),(3,'guangzhou','003',3,'2022-09-23');
select * from indup_04;

-- insert comprimary key duplicate data part,update value() and insert
insert into indup_04 values (4,'shenzheng','004',4,'2021-05-28'),(1,'beijing','010',5,'2022-10-23') on duplicate key update `act_name`=VALUES(`act_name`), `spu_id`=VALUES(`spu_id`), `uv`=VALUES(`uv`);
select * from indup_04;
-- insert comprimary key duplicate data all,update
insert into indup_04 values (2,'shanghai','002',21,'1999-09-23'),(3,'guangzhou','003',31,'1999-09-23') on duplicate key update `act_name`=VALUES(`act_name`), `spu_id`=VALUES(`spu_id`), `uv`=VALUES(`uv`);
select * from indup_04;
-- insert comprimary key duplicate data
insert into indup_04 values (2,'shanghai','002',21,'1999-09-23') on duplicate key update `act_name`=NULL;
select * from indup_04;
-- insert no duplicate data ,insert new data success
insert into indup_04 values (5,'shanxi','005',4,'2022-10-08'),(6,'shandong','006',6,'2022-11-22') on duplicate key update `act_name`='Hongkong';
select * from indup_04;
insert into indup_04 values (10,'xinjiang','008',7,NULL),(11,'hainan','009',8,NULL) on duplicate key update `act_name`='Hongkong';
select * from indup_04;

-- foreign key constraint
create table indup_fk1(col1 int primary key,col2 varchar(25),col3 tinyint);
create table indup_fk2(col1 int,col2 varchar(25),col3 tinyint primary key,constraint ck foreign key(col1) REFERENCES indup_fk1(col1) on delete RESTRICT on update RESTRICT);
insert into indup_fk1 values (2,'yellow',20),(10,'apple',50),(11,'opppo',51);
insert into indup_fk2 values(2,'score',1),(2,'student',4),(10,'goods',2);
-- @bvt:issue#8711
insert into indup_fk2 values(10,'food',1)on duplicate key update col1=50;
insert into indup_fk2 values(50,'food',1)on duplicate key update col1=values(col1);
select * from indup_fk1;
select * from indup_fk2;
-- @bvt:issue
drop table indup_fk2;
drop table indup_fk1;

-- without pk and unique index
CREATE TABLE IF NOT EXISTS indup_05(
    col1 INT ,
    col2 VARCHAR(20) NOT NULL,
    col3 VARCHAR(30) NOT NULL,
    col4 BIGINT default 30
);
insert into indup_05 values(22,'11','33',1), (23,'22','55',2),(24,'66','77',1),(25,'99','88',1),(22,'11','33',1) on duplicate key update col1=col1+col2;
insert into indup_05 values(22,'78','30',99) on duplicate key update col1=col1/2;
select * from indup_05;

-- loop update conflict
create table indup_06(col1 int primary key,col2 int);
insert into indup_06 values(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10),(11,11),(12,12),(13,13),(14,14),(15,15),(16,16),(17,17),(18,18),(19,19),(20,20);
insert into indup_06 values(1,10),(2,20),(3,30),(4,40),(5,50),(6,60),(7,70),(8,80),(9,90),(10,100),(11,110),(12,120),(13,130),(14,140),(15,150),(16,160),(17,170),(18,180),(19,190),(20,200)on duplicate key update col1=col1+1;
truncate table  indup_06;
insert into indup_06 values(1,1);
insert into indup_06 values(1,10),(2,20),(3,30),(4,40),(5,50),(6,60),(7,70),(8,80),(9,90),(10,100),(11,110),(12,120),(13,130),(14,140),(15,150),(16,160),(17,170),(18,180),(19,190),(20,200)on duplicate key update col1=col1+1;
insert into indup_06 values(1,10),(2,20),(3,30),(4,40),(5,50),(6,60),(7,70),(8,80),(9,90),(10,100),(11,110),(12,120),(13,130),(14,140),(15,150),(16,160),(17,170),(18,180),(19,190),(20,200)on duplicate key update col1=col1+1,col2=col2*10;
insert into indup_06 values(1,10),(2,20),(3,30),(4,40),(5,50),(6,60),(7,70),(8,80),(9,90),(10,100),(11,110),(12,120),(13,130),(14,140),(15,150),(16,160),(17,170),(18,180),(19,190),(20,200)on duplicate key update col1=col1+1,col2=col2/10;

--insert duplicate data to null table
CREATE TABLE IF NOT EXISTS indup_07(
    col1 INT primary key,
    col2 VARCHAR(20) NOT NULL,
    col3 VARCHAR(30) NOT NULL,
    col4 BIGINT default 30
);
insert into indup_07 values(22,'11','33',1), (23,'22','55',2),(24,'66','77',1),(25,'99','88',1),(22,'11','33',1) on duplicate key update col1=col1+col2;
select * from indup_07;

--update out of date range
insert into indup_07 values(24,'1','1',100) on duplicate key update col1=2147483649;

--transaction
-- @bvt:issue#8713
begin;
insert into indup_07 values(22,'11','33',1), (23,'22','55',2),(33,'66','77',1) on duplicate key update col1=col1+1,col2='888';
select * from indup_07;
rollback ;
select * from indup_07;
start transaction ;
insert into indup_07 values(22,'11','33',1), (23,'22','55',2),(33,'66','77',1) on duplicate key update col1=col1+1,col2='888';
select * from indup_07;
commit;
select * from indup_07;
-- @bvt:issue
--prepare
prepare stmt1 from "insert into indup_07 values(?, '11', '33', 1)on duplicate key update col1=col1*10";
set @a_var = 1;
execute stmt1 using @a_var;
select * from indup_07;
set @a_var = 23;
execute stmt1 using @a_var;
select * from indup_07;
