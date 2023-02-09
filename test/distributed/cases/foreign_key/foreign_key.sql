--foreign key is varchar,default ON DELETE/update option
create table fk_01(col1 varchar(30) not null primary key,col2 int);
create table fk_02(col1 int,col2 varchar(25),col3 tinyint,constraint ck foreign key(col2) REFERENCES fk_01(col1));
-- @bvt:issue#7934
show create table fk_02;
-- @bvt:issue
insert into fk_01 values ('90',5983),('100',734),('190',50);
insert into fk_02(col2,col3) values ('90',5),('90',4),('100',0),(NULL,80);
select * from fk_01;
select * from fk_02;
insert into fk_02(col2,col3) values ('200',5);
update fk_02 set col2='80' where col2='90';
update fk_01 set col1=5 where col2=734;
delete from fk_01 where col1='90';
delete from fk_01 where col1='190';
select * from fk_01;
select * from fk_02;
update fk_01 set col2=500 where col2=734;
delete from fk_02 where col2='100';
select * from fk_01;
select * from fk_02;
drop table fk_02;
drop table fk_01;

--foreign key is char,default ON DELETE/update option
create table fk_01(col1 char(30) primary key,col2 int);
create table fk_02(col1 int,col2 char(25),col3 tinyint,constraint ck foreign key(col2) REFERENCES fk_01(col1));
insert into fk_01 values ('90',5983),('100',734),('190',50);
insert into fk_02(col2,col3) values ('90',5),('90',4),('100',0),('',3);
insert into fk_02(col2,col3) values ('90',5),('90',4),('100',0);
truncate table fk_01;
select * from fk_01;
select * from fk_02;
drop table fk_02;
drop table fk_01;

--foreign key is tinyint references int
create table fk_01(col1 int auto_increment primary key,col2 varchar(25),col3 tinyint);
create table fk_02(col1 int,col2 varchar(25),col3 tinyint,primary key (col1),key col2 (col2) ,constraint ck foreign key(col3) REFERENCES fk_01(col1)on delete RESTRICT on update RESTRICT);
drop table fk_01;

--foreign key is int,on delete/update RESTRICT
create table fk_01(col1 int primary key,col2 varchar(25),col3 tinyint);
create table fk_02(col1 int,col2 varchar(25),col3 tinyint,constraint ck foreign key(col1) REFERENCES fk_01(col1) on delete RESTRICT on update RESTRICT);
insert into fk_01 values (2,'yellow',20),(10,'apple',50),(11,'opppo',51);
insert into fk_02 values(2,'score',1),(2,'student',4),(10,'goods',2);
insert into fk_02 values(NULL,NULL,NULL);
select * from fk_01;
select * from fk_02;
update fk_02 set col1=10 where col3=4;
select * from fk_02;
update fk_02 set col1=20 where col3=4;
insert into fk_02 values(15,'ssss',10);
delete from fk_01 where col1=11;
select * from fk_01;
select * from fk_02;
update fk_01 set col3=110 where col1=10;
delete from fk_01 where col1=2;
truncate table fk_01;
truncate table fk_02;
select * from fk_02;
drop table fk_01;
drop table fk_02;
drop table fk_01;

--foreign key are two column int varchar,on delete/update RESTRICT
-- @bvt:issue#7909
create table fk_01(col1 int,col2 varchar(20),col3 tinyint,primary key(col1,col2));
create table fk_02(col1 int,col2 varchar(25),col3 tinyint,constraint ck foreign key(col1,col2) REFERENCES fk_01(col1,col2) on delete RESTRICT on update RESTRICT);
insert into fk_01 values (2,'yellow',20),(2,'apple',50),(1,'opppo',51);
insert into fk_02 values(2,'apple',1),(2,'apple',4),(1,'opppo',2);
insert into fk_02 values(20,'score',1),(12,'apple',4),(1,'yellow',2);
select * from fk_01;
select * from fk_02;
update fk_02 set col1=3 where col1=2;
delete from fk_01 where col1=2 and col2='apple';
update fk_01 set col1=3 where col1=2;
update fk_01 set col1=1 where col1=2;
select * from fk_01;
select * from fk_02;
delete from fk_01 where col1=1;
delete from fk_02;
select * from fk_01;
select * from fk_02;
drop table fk_02;
drop table fk_01;
-- @bvt:issue

--foreign key is bigint,on delete/update cascade
create table fk_01(col1 bigint primary key,col2 varchar(25),col3 tinyint);
create table fk_02(col1 bigint,col2 varchar(25),col3 tinyint,constraint ck foreign key(col1) REFERENCES fk_01(col1) on delete CASCADE on update CASCADE);
insert into fk_01 values (1,'yellow',20),(2,'apple',50);
insert into fk_02 values(1,'score',1),(2,'student',NULL),(3,'goods',2);
insert into fk_02 values(4,'age',3);
insert into fk_02 values(1,'score',1),(2,'student',NULL);
select * from fk_01;
select * from fk_02;
update fk_02 set col3=4 where col3=1;
select * from fk_01;
delete from fk_01 where col1=1;
select * from fk_02;
update fk_01 set col1=5 where col2='apple';
select * from fk_01;
select * from fk_02;
delete from fk_02 where col1=5;
select * from fk_02;
select * from fk_01;
drop table fk_02;
drop table fk_01;

--foreign key are more column int,decimal,date,on delete /update SET NULL
-- @bvt:issue#7909
create table fk_01(col1 decimal(38,18),col2 char(25),col3 int,col4 date,primary key(col1,col3,col4));
create table fk_02(col1 decimal(38,18),col2 char(25),col3 int,col4 date,constraint ck foreign key(col1,col3,col4) REFERENCES fk_01(col1,col3,col4) on delete SET NULL on update SET NULL);
insert into fk_01 values(23.10,'a',20,'2022-10-01'),(23.10,'a',21,'2022-10-01'),(23.10,'a',20,'2022-10-02');
insert into fk_02 values(23.10,'a',20,'2022-10-01'),(23.10,'a',21,'2022-10-01'),(23.10,'a',20,'2022-10-02');
insert into fk_02 values(23.10,'a',20,'2022-10-01'),(0.1,'b',22,'2022-10-01'),(23.10,'c',30,'2022-10-02');
insert into fk_02 values(0.001,'b',20,'2022-10-01'),(4.5,'a',21,'2022-10-01'),(56,'a',20,'2022-10-02');
select * from fk_01;
select * from fk_02;
update fk_02 set col3=19 where col3=20;
delete from fk_01  where col3=20;
select * from fk_01;
select * from fk_02;
update fk_01 set col3=19 where col2='a';
select * from fk_01;
select * from fk_02;
drop table fk_02;
drop table fk_01;
-- @bvt:issue

--foreign key int,on delete/update NO ACTION
create table fk_01(col1 int primary key auto_increment,col2 varchar(25),col3 varchar(50));
create table fk_02(col1 int primary key auto_increment,col2 varchar(25),col3 char(5) default 'm',col4 int,constraint ck foreign key(col4) REFERENCES fk_01(col1) on delete NO ACTION on update NO ACTION);
insert into fk_01(col2,col3) values ('non-failing','deli'),('safer','prow'),('ultra','strong');
insert into fk_02(col2,col3,col4) values('aa','bb',2),('cc','dd',1),('ee','ff',1);
insert into fk_02(col2,col3,col4) values('aa','bb',4);
update fk_02 set col4=5 where col3='ff';
delete from fk_01 where col1=1;
select * from fk_01;
select * from fk_02;
delete from fk_02 where col4=1;
delete from fk_01 where col1=1;
select * from fk_01;
select * from fk_02;
update fk_01 set col1=8 where col1=2;
select * from fk_01;
select * from fk_02;
truncate table fk_01;
select * from fk_01;
drop table fk_01;
drop table fk_02;
drop table fk_01;

--foreign key int,on delete/update SET DEFAULT
create table fk_01(col1 int primary key auto_increment,col2 varchar(25),col3 varchar(50));
create table fk_02(col1 int primary key auto_increment,col2 varchar(25),col3 char(5) default 'm',col4 int,constraint ck foreign key(col4) REFERENCES fk_01(col1) on delete SET DEFAULT on update SET DEFAULT);
insert into fk_01(col2,col3) values ('non-failing','deli'),('safer','prow'),('ultra','strong'),('aaa','bbb');
insert into fk_02(col2,col3,col4) values('aa','bb',2),('cc','dd',1),('ee','ff',1);
insert into fk_02(col2,col3,col4) values('aa','bb',3);
select * from fk_01;
select * from fk_02;
delete from fk_01 where col1=1;
delete from fk_02 where col4=1;
select * from fk_02;
select * from fk_01;
update fk_01 set col1=8 where col1=2;
update fk_02 set col4=5 where col3='ff';
select * from fk_01;
select * from fk_02;
update fk_01 set col2='window' where col1=1;
delete from fk_01 where col1=3;
select * from fk_01;
select * from fk_02;
truncate table fk_01;
insert into fk_01(col2,col3) values ('zhi','gao'),('er','li');
select * from fk_01;
select * from fk_02;
drop table fk_01;
drop table fk_02;
drop table fk_01;

--foreign key references unique index,on delete/update SET DEFAULT
-- @bvt:issue#7935
create table fk_01(col1 bigint unique key,col2 varchar(25),col3 tinyint);
create table fk_02(col1 bigint,col2 varchar(25),col3 tinyint,constraint ck foreign key(col1) REFERENCES fk_01(col1) on delete CASCADE on update CASCADE);
insert into fk_01 values (1,'yellow',20),(2,'apple',50);
insert into fk_02 values(1,'score',1),(2,'student',NULL),(3,'goods',2);
insert into fk_02 values(4,'age',3);
insert into fk_02 values(1,'score',1),(2,'student',NULL);
select * from fk_01;
select * from fk_02;
update fk_02 set col3=4 where col3=1;
select * from fk_01;
delete from fk_01 where col1=1;
select * from fk_02;
update fk_01 set col1=5 where col2='apple';
select * from fk_01;
select * from fk_02;
delete from fk_02 where col1=5;
select * from fk_02;
select * from fk_01;
drop table fk_02;
drop table fk_01;
-- @bvt:issue

--more foreign key
create table fk_01(id int primary key auto_increment,title varchar(25));
create table fk_02(id int primary key auto_increment,name varchar(25));
create table fk_03(id int primary key auto_increment,book_id int,foreign key(book_id) REFERENCES fk_01(id) on delete cascade on update cascade,author_id int,foreign key(author_id) REFERENCES fk_02(id) on delete cascade on update cascade);
insert into fk_01(title) values ('self'),('method'),('console');
insert into fk_02(name) values ('daisy'),('wulan');
insert into fk_03(book_id,author_id) values (1,2),(2,2),(3,1);
insert into fk_03(book_id,author_id) values (4,3);
update fk_03 set book_id=6 where book_id=2;
update fk_03 set book_id=3 where book_id=2;
select * from fk_03;
update fk_01 set id=5 where title='self';
select * from fk_03;
select * from fk_01;
delete from fk_02 where id=1;
select * from fk_02;
select * from fk_03;
delete from fk_03;
drop table fk_01;
select * from fk_03;
drop table fk_02;
drop table fk_03;
drop table fk_01;
drop table fk_02;

--foreign key is datetime,timestamp
-- @bvt:issue#7909
create table fk_01(col1 int,col2 datetime,col3 timestamp,primary key(col1,col2,col3));
create table fk_02(col1 int,col2 datetime,col3 char(25),col4 timestamp ,constraint ck foreign key(col1,col2,col4) REFERENCES fk_01(col1,col2,col3));
insert into fk_01 values (9,'2001-10-19','2001-10-09 01:00:09'),(10,'2011-12-09','2001-10-09 01:00:09'),(11,'2011-12-09','2001-10-09 01:00:09');
insert into fk_02 values (9,'2001-10-19','left','2001-10-09 01:00:09'),(11,'2011-12-09','right','2001-10-09 01:00:09');
insert into fk_02 values (5,'2001-10-19','left','2001-10-09 01:00:09');
delete from fk_01 where col3='2001-10-09 01:00:09';
select * from fk_01;
select * from fk_02;
drop table fk_02;
drop table fk_01;
-- @bvt:issue

--Abnormal test
--foreign key is not a related data type
create table fk_an_01(col1 int,col2 varchar(25),col3 tinyint,primary key(col2));
create table fk_an_02(col1 bigint,col2 char(25),constraint ck foreign key(col1) REFERENCES fk_an_01(col2));
--foreign key references table not exists
create table fk_an_03(col1 int,col2 char(25),constraint ck foreign key(col1) REFERENCES fk_an_05(col1));
--references not primary key
create table fk_an_04(col1 bigint,col2 varchar(25),col3 tinyint);
create table fk_an_05(col1 bigint,col2 varchar(25),col3 tinyint,constraint ck foreign key(col1) REFERENCES fk_an_04(col1) on delete CASCADE on update CASCADE);