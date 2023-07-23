-- supplement text blob bvt cases
drop table if exists text_01;
drop table if exists text_02;
drop table if exists text_03;
drop table if exists text_04;
drop table if exists blob_01;
drop table if exists blob_02;
drop table if exists blob_03;
drop table if exists blob_04;
drop table if exists blob_05;

-- text
create table text_01(t1 text,t2 text,t3 text);
insert into text_01 values ('中文123abcd','',NULL);
insert into text_01 values ('yef&&190',' wwww ',983);
insert into text_01 select '',null,'中文';
insert into text_01 select '123','7834','commmmmmment';
insert into text_01 values ('789',' 23:50:00','20');
select * from text_01;
create table text_02(t1 text,t2 tinytext,t3 mediumtext,t4 longtext,t5 text);
load data infile '$resources/blob_file/blob1.csv' into table text_02;
select * from text_02;
insert into text_02 values ("123木头人","zt@126.com","1000-01-01","9999-12-31 23:59:59.999999","2038-01-19 03:14:07.999999");
select * from text_02;
create external table text_03(t1 text,t2 tinytext,t3 mediumtext,t4 longtext,t5 text)infile{"filepath"='$resources/blob_file/blob1.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from text_03;
update text_02 set t3='yelllllow' where t1='cat';
update text_02 set t2='786374' where t1='cat';
select t2,t3 from text_02 where t1='cat';
delete from text_02  where t4='0001-01-01 00:00:00.000000';
select * from text_02;
truncate table text_02;
select * from text_02;

--cast text to json/time and to text
select cast(t2 as time), cast(t3 as time)from text_01 where t1='789';
select cast(t2 as blob) from text_01;
select cast(null as text),cast("" as text);
create table text_04(t1 text,t2 time,t3 json,t4 blob);
insert into text_04 values ('ttttt','12:00','{"a":"1","b":"2"}','yes');
select * from text_04;
select cast(t2 as text)from text_04;
select cast(t3 as text)from text_04;
select cast(t4 as text)from text_04;

-- agg function
select count(t1),max(t2),min(t3) from text_01;
select bin(t3) from text_01;
select space(t3) from text_01;
select * from text_01 where t3 is null or length (t1)  >3;
select * from text_01 where t3 between  '100' and '1000';
select * from text_01 where t3 not between  '100' and '1000';
select * from text_01 where t1 in('yef&&190','','789') and t2 not in(' 23:50:00');

-- intersect/minus
create table text_05(t1 text,t2 text,t3 text, t4 varchar(250),t5 char(250));
insert into text_05 select * from text_03;
insert into text_05 values ('789',' 23:50:00','20','12345','noooooo');
select t1,t2 from text_01 intersect select t1,t2 from text_05;
select t1,t2 from text_01 minus select t1,t2 from text_05;

-- subquery
select * from (select * from text_01 where t1 like '%123%');
select * from text_01 where t1 in (select t1 from text_05);
-- @bvt:issue#7589
select * from text_01 where t2 > (select t2 from text_05 where t1='789');
-- @bvt:issue
select t1,t2,t3 from text_01 where t1 < any(select t2 from text_05);
select t1,t2,t3 from text_01 where t1 >= all(select t2 from text_05);
select t1,t2,t3 from text_01 where t1 >= some(select t2 from text_05);
select * from text_01 where exists(select t2 from text_05);
select * from text_01 where not exists(select t2 from text_05);

-- invalid compk
create table text_02(t1 int,t2 text,t3 text,primary key(t1,t2));
create table text_02(t1 int,t2 text primary key,t3 text);

prepare stmt1 from 'select * from text_01';
execute stmt1;

-- blob
create table blob_01(b1 blob,b2 blob,b3 blob);
insert into blob_01 values ('no','中文','89233432234234 ');
insert into blob_01 values ('',' hhhh@126.com','0001-01-01');
insert into blob_01 values ('#$%^&*()',NULL,null);
select * from blob_01;
select length(b1),length(b2),length(b3) from blob_01;
select substring(b3,5),substr(b2,-3,2) from blob_01;
select count(b1) from blob_01;
select max(b2),min(b3) from blob_01;
select b1||b2 from blob_01;

-- ddl and dml
create table blob_02(b1 blob primary key,b2 int);
create table blob_02(b1 blob,b2 int,primary key(b1,b2));
create table blob_02(b1 blob not null,b2 int);
insert into blob_02 values (null,40);
insert into blob_02 values ('12345',43);
insert into blob_02 values ('tennis','0934');
select * from blob_02;
update blob_02 set b1='abc';
select * from blob_02;
delete from blob_02 where b2=43;
select * from blob_02;
truncate table blob_02;
select * from blob_02;
drop table blob_02;
create table blob_02(b1 blob default 'tom',b2 int);
insert into blob_02(b2) values (50);
select * from blob_02;
drop table blob_02;

-- cast type to blob
select cast('苏轼' as blob),cast('alex' as blob),cast('123' as blob);
select cast('@#$%^' as blob),cast('hhh@123'as blob);
create table blob_03(t1 text,t2 time,t3 json,t4 blob,t5 int,t6 varchar(100),t7 double,t8 decimal(6,3));
insert into blob_03 values ('枫叶','0150','{"a":"1","b":"2"}','checkin',30,'zzzow',89.02,23.90);
select cast(t1 as blob),cast(t2 as blob),cast(t5 as blob),cast(t6 as blob),cast(t7 as blob),cast(t8 as blob) from blob_03;
select cast(t3 as blob) from blob_03;
select cast(t4 as varchar(255))from blob_03;

-- load_file
select load_file(null);
select load_file('');
select load_file('/opt/aaa.csv');
select load_file(12);
select load_file('$resources/blob_file/blob1.csv');
select length(load_file('$resources/blob_file/blob3.jpeg'));
select length(load_file('$resources/blob_file/blob1.csv'));
create table blob_04(a int,b varchar(290));
insert into blob_04 values(1,load_file('$resources/blob_file/blob1.csv'));

-- 音频文件，图文文件
create table blob_02(a int,b blob);
insert into blob_02 values(3,load_file('$resources/blob_file/blob1.csv'));
insert into blob_02 values(4,load_file('$resources/blob_file/blob3.jpeg'));
insert into blob_02 values(5,load_file('$resources/blob_file/blob4.gif'));
insert into blob_02 values(6,load_file('$resources/blob_file/blob5.docx'));
insert into blob_02 values(7,load_file('$resources/blob_file/blob6.pptx'));
insert into blob_02 values(8,load_file('$resources/blob_file/blob7.pdf'));
insert into blob_02 values(9,load_file('$resources/blob_file/blob8.xlsx'));
insert into blob_02 values(10,load_file('$resources/blob_file/blob9.xml'));
select a,length(b) from blob_02;
-- @bvt:issue#6302
insert into blob_02 values(1,load_file('$resources/blob_file/blob2.mp3'));
insert into blob_02 select 2,load_file('$resources/blob_file/blob2.mp3');
select a,length(b) from blob_02 where a in (1,2);
-- @bvt:issue
--preparement
prepare stmt2 from 'create table blob_05(a int,b blob)';
execute stmt2 ;
prepare stmt2 from 'insert into blob_05 values(4, load_file("$resources/blob_file/blob3.jpeg"))';
execute stmt2 ;
prepare stmt2 from 'select length(b) from blob_05';
execute stmt2 ;
deallocate prepare stmt1;
deallocate prepare stmt2;