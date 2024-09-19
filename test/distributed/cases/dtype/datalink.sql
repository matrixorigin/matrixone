-- call datalink in load_file function
select load_file(cast('file://$resources/load_data/float_1.csv' as datalink));
select load_file(cast('file://$resources/load_data/char_varchar_2.csv' as datalink));
select load_file(cast('file://$resources/load_data/char_varchar_2.csv?offset=1&size=4' as datalink));
select load_file(cast('file://$resources/load_data/char_varchar_2.csv?offset=2&size=5' as datalink));
select load_file(cast('file://$resources/load_data/char_varchar_2.csv?offset=0&size=100' as datalink));
select load_file(cast('file://$resources/load_data/char_varchar_2.csv?offset=1000&size=100' as datalink));
select load_file(cast('file://$resources/load_data/char_varchar_2.csv?offset=100&size=1000' as datalink));



-- error parameter offset or size
select load_file(cast('file://$resources/load_data/time_date_2.csv?offset=-1&size=2' as datalink));
select load_file(cast('file://$resources/load_data/time_date_2.csv?offset=6&size=-2' as datalink));
select load_file(cast('file://$resources/load_data/time_date_1.csv?offset=a&size=b' as datalink));



-- datalink type, insert into datalink, load_file from datalink
drop database if exists test;
create database test;
use test;

drop table if exists test01;
create table test01 (col1 int, col2 datalink);
insert into test01 values (1, 'file://$resources/load_data/time_date_1.csv');
select col1, load_file(col2) from test01;
drop table test01;


drop table if exists test02;
create table test02 (col1 int, col2 datalink);
insert into test02 values (1, 'file://$resources/load_data/time_date_2.csv');
select col1, load_file(col2) from test02;
drop table test02;



-- datalink type, insert into datalink, load_file from datalink
drop table if exists test03;
create table test03 (col1 int, col2 datalink);
insert into test03 values (1, 'file://$resources/load_data/text.csv.tar.gz');
insert into test03 values (2, 'file://$resources/load_data/test_columnlist_01.csv');
select col1, load_file(col2) from test03;
select * from test03;
alter table test03 drop column col2;
show create table test03;
select * from test03;
drop table test03;



-- datalink type, insert into datalink with parameter size, default offset = 0
drop table if exists test04;
create table test04 (col1 int, col2 datalink);
insert into test04 values (1, 'file://$resources/load_data/test_escaped_by01.csv?size=10');
insert into test04 values (2, 'file://$resources/load_data/test_escaped_by04.csv?size=50');
select col1, load_file(col2) from test04;
drop table test04;



-- datalink type, insert into datalink with parameter offset
drop table if exists test05;
create table test05 (col1 int, col2 datalink);
insert into test05 values (1, 'file://$resources/load_data/test_columnlist_01.csv?offset=5');
insert into test05 values (2, 'file://$resources/load_data/test_columnlist_02.csv?offset=10');
select col1, load_file(col2) from test05;
drop table test05;



-- call datalink as stage
drop stage if exists stage_link01;
create stage stage_link01 url = 'file:///$resources/load_data';
select load_file(cast('stage://stage_link01/auto_increment_1.csv' as datalink));
drop stage if exists stage_link02;
create stage stage_link02 url = 'file:///$resources/load_data';
select load_file(cast('stage://stage_link02/auto_increment_2.csv' as datalink));
drop stage stage_link01;
drop stage stage_link02;



-- save_file() function
drop stage if exists outfilestage01;
create stage outfilestage01 URL='file://$resources/into_outfile/';
select save_file(cast('stage://outfilestage01/datalink/test01.csv' as datalink), '测试datalink功能');
select load_file(cast('stage://outfilestage01/datalink/test01.csv?offset=0&size=6' as datalink));
select save_file(cast('stage://outfilestage01/datalink/test02.csv' as datalink), 'test for datalink');
select load_file(cast('stage://outfilestage01/datalink/test02.csv' as datalink));
drop stage stage_link01;



-- save_file() function
drop stage if exists outfilestage02;
create stage outfilestage02 URL='file://$resources/into_outfile/';
select save_file(cast('stage://outfilestage02/datalink/test03.csv' as datalink), '#……&……*（&（）#（）——¥#——+#%（）——%#）（）%）——#（%——#');
select load_file(cast('stage://outfilestage02/datalink/test03.csv' as datalink));
select save_file(cast('stage://outfilestage02/datalink/test04.csv' as datalink), 'DOUBLE(M, D)M 表示的是最大长度，D 表示的显示的小数位数。M 的取值范围为（1=< M <=255）。D 的取值范围为（1=< D <=30），且 M >= D。带精度的浮点数展示出要求精度的位数，在位数不足时，会进行末尾补 0。');
select load_file(cast('stage://outfilestage02/datalink/test04.csv?offset=10&size=1000' as datalink));
drop stage outfilestage02;



-- abnormal test: parameter of load_file() is incorrect
drop stage if exists stage_link03;
drop stage if exists stage_link04;
create stage stage_link03 url = 'file:///$resources/load_data/abc';
select load_file(cast('stage://stage_link03/auto_increment_1.csv' as datalink));
create stage stage_link04 url = 'file:///$resources/load_data';
select load_file(cast('stage://stage_link04/auto_2.csv' as datalink));
drop stage stage_link03;
drop stage stage_link04;



-- abnormal test, insert into wrong datalink type into table
drop table if exists test02;
create table test02 (a int, b datalink);
insert into test02 values (1, "this is a wrong datalink");
insert into test02 values (2, 'https://github.com/matrixorigin/matrixone/pull/');




