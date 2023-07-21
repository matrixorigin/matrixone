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