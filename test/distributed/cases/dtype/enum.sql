CREATE TABLE shirts (
    name VARCHAR(40),
    size ENUM('x-small', 'small', 'medium', 'large', 'x-large')
);
INSERT INTO shirts (name, size) VALUES ('dress shirt','large'), ('t-shirt','medium'), ('polo shirt','small');
SELECT name, size FROM shirts;
SELECT name, size FROM shirts WHERE size = 'medium';
DELETE FROM shirts where size = 'large';
SELECT name, size FROM shirts;
DROP TABLE shirts;

create table t_enum(a int,b enum('1','2','3','4','5'));
insert into t_enum values(1,1);
select * from t_enum;
drop table t_enum;

-- @suit
-- @case
-- @desc: datatype:enum
-- @label:bvt


-- abnormal create table: non-string type
drop table if exists enum01;
create table enum01(col1 enum(132142,'*&*',6278131));
drop table enum02;


-- abnormal test: insert a column that does not exist in an enumeration type
drop table if exists enum02;
create table enum02(col1 enum('123214','*&*(234','database数据库'));
insert into enum02 values('1232145');
insert into enum02 values('*&*(2344');
drop table enum02;


-- test insert with enum
drop table if exists enum01;
create table enum01 (col1 enum('red','blue','green'));
insert into enum01 values ('red'),('blue'),('green');
desc enum01;
show create table enum01;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'enum01' and COLUMN_NAME not like '__mo%';
select * from enum01;


-- insert null
drop table if exists enum02;
create table enum02 (col1 enum('red','blue','green'));
insert into enum02 values ('red'),('blue'),('green');
insert into enum02 values (null);
insert into enum02 values ('');
select * from enum02;
drop table enum02;


-- update
update enum01 set col1 ='blue' where col1 = 'green';
delete from enum01 where col1 = 'blue';

-- error update
update enum01 set col1 ='yellow' where col1 ='green';

select * from enum01;
drop table enum01;

-- insert into select
drop table if exists enum02;
drop table if exists enum03;
create table enum02(col1 enum('数据库','数据库管理','数据库管理软件'));
insert into enum02 values('数据库');
insert into enum02 values('数据库管理');
insert into enum02 values('数据库管理软件');
insert into enum02 values(null);
select * from enum02;
create table enum03(col1 enum('数据库','数据库管理','数据库管理软件'));
insert into enum03 select * from enum02;
select * from enum03;
drop table enum02;
drop table enum03;


-- create table enums
drop table if exists enum03;
create table enum03 (col1 enum('0','1','2020'), col2 enum('red','green','blue'));
insert into enum03 values('2020','blue');
select * from enum03;
select cast(col1 as year) from enum03;
select cast(col2 as year) from enum03;
select cast(col1 as binary) from enum03;
select cast(col1 as varbinary(10)) from enum03;

select cast(col1 as varchar(20)) from enum03;
select cast(col2 as char) from enum03;

select cast(col1 as text) from enum03;
select cast(col1 as int) from enum03;

select cast(col1 as blob) from enum03;
select cast(col1 as float) from enum03;
select cast(col2 as decimal) from enum03;
select cast(col1 as decimal(20,18)) from enum03;
select cast(col1 as date) from enum03;
select cast(col1 as datetime) from enum03;
select cast(col2 as time) from enum03;
select cast(col2 as timestamp) from enum03;

select cast(col1 as boolean) from enum03;
select cast(col2 as boolean) from enum03;
drop table enum03;

-- the inserted value can be obtained by implicit conversion
drop table if exists enum04;
create table enum04(a int, b enum('1','2','3','4','1.23454342','8392432.24321342'));
insert into enum04 values(1,1);
insert into enum04 values(2,2);
insert into enum04 values(3,3);
insert into enum04 values(4,1.23454342);
insert into enum04 values(5,8392432.24321342);
select * from enum04;

-- implicit conversion(insert the value at the index location)
-- float,decimal,int convert to index
drop table if exists enum05;
create table enum05(col1 enum('ehjwfwe','3822.833333','vhriewjvlrew'));
insert into enum05 values(3.2341221);
insert into enum05 values('ehjwfwe');
insert into enum05 values('vhriewjvlrew');
insert into enum05 values(1);
insert into enum05 values(1.39243218394082492);
select * from enum05;

-- enum convert to time/timestamp/date/datetime
drop table if exists enum06;
create table enum06(col1 enum('2023-01-13','08:00:00','2019-03-05 01:53:55.63','2004-01-22 21:45:33'));
insert into enum06 values('2023-01-13');
insert into enum06 values('08:00:00');
insert into enum06 values('2019-03-05 01:53:55.63');
select cast(col1 as time) from enum06;
select cast(col1 as timestamp) from enum06;
select cast(col1 as date) from enum06;
select cast(col1 as datetime) from enum06;
select * from enum06;

-- if the modified column value conflicts with the list value, the modification fails
drop table if exists enum05;
create table enum05(col1 enum('1','2','3','4','5'));
insert into enum05 values(1);
insert into enum05 values(2);
alter table enum05 col1('2','3','4','5');
drop table enum05;

-- enumeration types support operators
drop table if exists enum04;
create table enum04(col1 int,col2 enum('38921384','abc','','MOMOMO','矩阵起源'));
insert into enum04 values(1,'38921384');
insert into enum04 values(2,'');
insert into enum04 values(3,'矩阵起源');
select * from enum04;

-- =,!=,>,>=,<,<=,between and,not between and,in,not in,like,COALESCE
select * from enum04 where col2 = '';
select * from enum04 where col2 != '';

select * from enum04 where col2 > '38921384';
select * from enum04 where col2 >= '38921384';

select * from enum04 where col2 < '矩阵起源';
select * from enum04 where col2 <= '矩阵起源';

select * from enum04 where col2 between '38921384' and '矩阵起源';
select * from enum04 where col2 not between '38921384' and '矩阵起源';

select * from enum04 where col2 in('38921384','');
select * from enum04 where col2 not in('38921384','');

select * from enum04 where col2 like '%921384';
select coalesce(null,null,col2) from enum04;
drop table enum04;

-- builtin function
drop table if exists enum05;
create table enum05(col1 enum('  云原生数据库  ','存储引擎 TAE','索引元数据表 ','     ') not null primary key,col2 enum(' database','engine ','index meta data'),col3 int, col4 binary(10),col5 varchar(20));
insert into enum05 values('  云原生数据库  ',' database',328904,'&^&*Uef3r','balance!');
insert into enum05 values('存储引擎 TAE','engine ',-38291,'0039293','春天是彩色的');
insert into enum05 values('索引元数据表 ','index meta data',0,'------','8ueiwlvr');
insert into enum05 values('     ','engine ',0,'------','8ueiwlvr');

select * from enum05;
select concat_ws(',,,',col1,col2) from enum05;
select find_in_set('云原生数据库',col1) from enum05;
select oct(col1) from enum05;
select length(col1) as length_col1,length(col2) as length_col2 from enum05;
select char_length(col1),char_length(col2) from enum05;
select ltrim(col1) from enum05;
select rtrim(col2) from enum05;
select lpad(col1,20,'-') from enum05;
select rpad(col2,60,'****') from enum05;
select startswith(col2,'eng') from enum05;
select endswith(col1,'数据表') from enum05;
select reverse(col1),reverse(col2) from enum05;
select substring(col1,4,6),substring(col2,1,6) from enum05;
select * from enum05 where col1 = space(5);
select bit_length(col2) from enum05;
select bin(col1) as bin_col1,bin(col2) as bin_col2 from enum05;
select hex(col1) as hex_col1,hex(col2) as hex_col2 from enum05;
select oct(col1) from enum05;
select empty(col2) from enum05;

-- subquery
select group_concat(col1,col2) from enum05 where col1 = (select col1 from enum05 where col2 = 'index meta data');
select * from enum05 where col2 = (select col2 from enum05 where col1 = '索引元数据表');

-- aggregate:count,max,min,any_value,group_concat
select count(col1) as count_col1 from enum05;
select max(col1),max(col2) from enum05;
select min(col1),min(col2) from enum05;
select any_value(col1) from enum05;
select any_value(col2) from enum05;
select group_concat(col1,col2) from enum05;

drop table enum05;

-- prepare
drop table if exists prepare01;
create table prepare01 (a int, b enum('100','shujuku'));
insert into prepare01 values(1, 'shujuku');
insert into prepare01 values(2, '100');
select * from prepare01;
prepare stmt1 from 'update prepare01 set b=? where a = 1';
set @bin_a= '100';
execute stmt1 using @bin_a;
select * from prepare01;

prepare stmt2 from 'insert into prepare01 values(3,?)';
set @bin_b='shujuku';
execute stmt2 using @bin_b;
select * from prepare01;
drop table prepare01;

-- join
drop table if exists join01;
drop table if exists join02;
create table join01(col1 enum('操作系统','数据库','数据库管理系统','  '), col2 enum('opertion system','db','DBMS'));
insert into join01 values('操作系统','db');
insert into join01 values('操作系统','db');
insert into join01 values('  ','opertion system');
select * from join01;
create table join02(col1 enum('数据结构','数据库','数据库管理系统','  '), col2 enum('opertion system','db','DBMS'));
insert into join02 values('数据结构','db');
insert into join02 values('数据库','db');
insert into join02 values('数据库管理系统','opertion system');
select * from join02;

select join01.col1,join02.col2 from join01,join02 where join01.col2 = join02.col2;
select join01.col1,join02.col2 from join01 left join join02 on join01.col2 = join02.col2;
select join01.col1,join02.col2 from join01 right join join02 on join01.col2 != join02.col2;
select join01.col1,join02.col2 from join01 inner join join02 on join01.col2 = join02.col2;
select join01.col1,join02.col2 from join01 natural join join02;
drop table join01;
drop table join02;

-- cte
drop table if exists cte01;
create table cte01(col1 int, col2 enum('hfjsa','123214321','&**())_'));
insert into cte01 VALUES(1, 'hfjsa');
insert into cte01 VALUES(2, '123214321');
insert into cte01 VALUES(3, '&**())_');
select * from cte01;
with cte_1 as(select * from cte01 where col2 = 'hfjsa') select col2 from cte_1 where col2 = 'hfjsa';
with cte_2 as(select col1,col2 from cte01 where col1 = 3) select col2 from cte_2 where col2 = '&**())_';
drop table cte01;

-- view
drop table if exists view01;
drop table if exists view02;
drop view if exists v0;
create table view01(col1 enum('S','L','M'));
insert into view01 values('S');
insert into view01 values('L');
insert into view01 values('M');
create table view02(col1 enum('W','S','M'));
insert into view02 values('W');
insert into view02 values('S');

create view v0 as select view01.col1, view02.col1 as b from view01 left join view02 using(col1);
show create view v0;
drop view v0;
drop table view01;
drop table view02;

drop database test;

load data infile '/data/mo-load-data/data/100_columns/40000000_100_columns_load_data.csv' into table test.table_100_columns_4000w  FIELDS TERMINATED BY ',' LINES TERMINATED BY '
' parallel 'true';