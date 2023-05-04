--sequence name normal case
create sequence seq_01 as int start 30;
create sequence `123` start 30;
select nextval('123');
create sequence SEQ increment 100 start 30;
create sequence sEq increment 100 start 30;
create sequence `中文` maxvalue 6899 cycle;
select nextval('中文');
select nextval('中文'),currval('中文');
create sequence `test@123456`;
select nextval('test@123456');
select nextval('test@123456'),currval('test@123456');
create sequence _acc;
select nextval('_acc');
select nextval('_acc'),currval('_acc');
create sequence `ab.cd` start with 1;
select nextval('ab.cd');
select nextval('ab.cd'),currval('ab.cd');
create sequence `abcddddddfsfafaffsefsfsefljofiseosfjosisssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss24444444444444444444444444222222222@fffffffffffffffffffffffffffffffffffffffffffffffffffff` start 30;
select nextval('abcddddddfsfafaffsefsfsefljofiseosfjosisssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss24444444444444444444444444222222222@fffffffffffffffffffffffffffffffffffffffffffffffffffff') as s1;

--abnormal sequence name include same as table,view,external table,index name
create table if not exists seq_temp(col1 int);
create sequence seq_temp start 10;
drop table seq_temp;
create table if not exists table_temp(col1 int);
create view seq_temp as select * from table_temp;
create sequence seq_temp start 10;
drop view seq_temp;
drop table table_temp;
create external table seq_temp(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double,num_col11 decimal(38,19)) infile{"filepath"='$resources/external_table_file/ex_table_number.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
create sequence seq_temp start 10;
drop table seq_temp;
create sequence seq_temp start 10;
create sequence seq_temp start 10;
create sequence Seq_temp start 10;
create sequence sequence;
create sequence table as bigint;

--if not exists
create sequence seq_01 as tinyint unsigned MINVALUE 26  NO CYCLE;
create sequence if not exists seq_01 as tinyint unsigned MINVALUE 26  NO CYCLE;
create sequence if not exists seq_02 as tinyint CYCLE;

--sequence grammar cover
create sequence seq_03 increment 3;
select nextval('seq_03');
select nextval('seq_03'),currval('seq_03');
select nextval('seq_03');
select nextval('seq_03');
select * from seq_03;
create sequence seq_04 increment by -10;
select nextval('seq_04');
select nextval('seq_04'),currval('seq_04');
select nextval('seq_04');
select nextval('seq_04'),currval('seq_04');
create sequence seq_05 start 10000;
select nextval('seq_05');
select nextval('seq_05'),currval('seq_05');
create table seq_table_01(col1 int);
insert into seq_table_01 values(nextval('seq_05')),(nextval('seq_04')),(nextval('seq_05')),(nextval('seq_05'));
select col1 from seq_table_01;
create sequence seq_06 start with 10000;
select nextval('seq_06');
select nextval('seq_06'),currval('seq_06');
select nextval('seq_06'),currval('seq_06');
select nextval('seq_06');
select nextval('seq_06');
select * from seq_06;
truncate table seq_table_01;
insert into seq_table_01 values(nextval('seq_06')),(nextval('seq_06'));
insert into seq_table_01 values(nextval('seq_06'));
select col1 from seq_table_01;
create sequence seq_07 minvalue 999 maxvalue 1999;
select nextval('seq_07');
select nextval('seq_07'),currval('seq_07');
select nextval('seq_07'),currval('seq_07');
select nextval('seq_07');
select setval('seq_07',1050,false);
select lastval();
truncate table seq_table_01;
insert into seq_table_01 values(nextval('seq_07'));
insert into seq_table_01 select nextval('seq_07');
select * from seq_table_01;
create sequence seq_08;
select nextval('seq_08');
select nextval('seq_08'),currval('seq_08');
select nextval('seq_08'),currval('seq_08');
create sequence seq_09 minvalue 10 maxvalue 12 no cycle;
select nextval('seq_09');
select nextval('seq_09');
select nextval('seq_09');
select nextval('seq_09');
drop sequence seq_09;
create sequence seq_09 increment 2 minvalue 10 maxvalue 12 cycle;
select nextval('seq_09');
select nextval('seq_09');
select nextval('seq_09');
select nextval('seq_09');
select * from seq_09;
create sequence seq_10 minvalue 1000;
select nextval('seq_10');
select nextval('seq_10'),currval('seq_10');
select nextval('seq_10');
select nextval('seq_10');
select nextval('seq_08'),currval('seq_08');
create sequence seq_11 as smallint start 126;
select nextval('seq_11');
select nextval('seq_11'),currval('seq_11');
select nextval('seq_11');
select nextval('seq_11');
create sequence if not exists seq_12 as bigint increment by 10000 minvalue 500  start with 500 cycle;
select nextval('seq_12');
select nextval('seq_12'),currval('seq_12');
select nextval('seq_12'),currval('seq_12');
select nextval('seq_12');
select * from seq_12;
truncate table seq_table_01;
insert into seq_table_01 select nextval('seq_12');
insert into seq_table_01 values(nextval('seq_12'));
select * from seq_table_01;
create sequence seq_13 as int increment -10000  no cycle;
select nextval('seq_13');
select nextval('seq_13'),currval('seq_13');
truncate table seq_table_01;
insert into seq_table_01 select nextval('seq_13');
insert into seq_table_01 values(nextval('seq_13'));
select * from seq_table_01;
create sequence seq_14  increment 50 start with 126 no cycle;
select nextval('seq_14');
select nextval('seq_14'),currval('seq_14');
truncate table seq_table_01;
insert into seq_table_01 select nextval('seq_14');
insert into seq_table_01 values(nextval('seq_14'));
insert into seq_table_01 values(nextval('seq_14'));
insert into seq_table_01 values(nextval('seq_14'));
insert into seq_table_01 values(nextval('seq_14'));
select * from seq_table_01;

--abnormal test: max/min/start value out of range
create sequence seq_an_01 as smallint start -1000;
create sequence if not exists seq_an_01 as bigint increment by 10000 minvalue 500  start with 100;
create sequence seq_an_01 as smallint maxvalue 9999999 start with -10;
create sequence seq_an_01 start with 0;
create sequence seq_an_02 as tinyint unsigned;
create sequence seq_an_02 as tinyint;
create sequence seq_an_03  increment -50 start with 126 no cycle;


--show
show sequences;
show sequences where names in('seq_05','seq_06');
--select * from mo_catalog.mo_sequences;

--drop if exists
drop sequence seq_15;
drop sequence if exists seq_15;
drop sequence seq_15;
drop sequence seq_non;

--prepare
create sequence seq_15;
create sequence seq_16 increment 10 start with 20 no cycle;
truncate table seq_table_01;
prepare stmt1 from 'insert into seq_table_01 values(?)';
-- @bvt:issue#9241
set @a_var = nextval('seq_15');
execute stmt1 using @a_var;
select * from seq_table_01;
execute stmt1 using @a_var;
select * from seq_table_01;
execute stmt1 using @a_var;
select * from seq_table_01;
-- @bvt:issue
drop sequence seq_16;
drop sequence seq_15;
-- @bvt:issue
--lastval and setval
create sequence seq_17 increment 10 start with 20 no cycle;
select lastval();
select nextval('seq_17');
select lastval();
select setval('seq_17',5);
select nextval('seq_17'),currval('seq_17');
select setval('seq_17',8,false);
select nextval('seq_17'),currval('seq_17');
select nextval('seq_17'),currval('seq_17');
select lastval();

--transaction
-- @bvt:issue#8890
begin;
create sequence seq_18 minvalue 1000;
select nextval('seq_18');
-- @session:id=2&user=sys:dump&password=111
select nextval('seq_18');
create sequence seq_18;
-- @session
select nextval('seq_18');
commit;
select nextval('seq_18'),currval('seq_18');
drop sequence seq_18;

begin;
create sequence seq_19 minvalue 1000;
select nextval('seq_19');
-- @session:id=2&user=sys:dump&password=111
select nextval('seq_19');
create sequence seq_19;
-- @session
rollback;
select nextval('seq_19');
drop sequence seq_19;

start transaction ;
create sequence seq_20 increment by -10;
select nextval('seq_20');
rollback;
select nextval('seq_20');
drop sequence seq_20;
-- @bvt:issue