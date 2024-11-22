SELECT crc32('123');
SELECT crc32(null);
SELECT crc32('123456');
CREATE TABLE crc32test(col1 varchar(100));
INSERT INTO crc32test VALUES ('123'),(NULL),('123456');
SELECT crc32(col1) FROM crc32test;
SELECT CRC32('Hello, World!') AS crc32_hello_world;

-- null string
select CRC32('');

-- simple string
select crc32('hello world!');
select crc32('euwpqeiwqpjvmrwqewq');
select crc32('372913721ojgi132uio3jkel2m1kl3hi21fr41234321421');
select crc32('aaaaaaa');

-- special character
select CRC32('hello!@#$%');
select crc32('$^*(&)(*)($%^$%#%^&(*&(*');

-- multi length string
select crc32('a'), crc32('ab'), CRC32('abc'), CRC32('abcd'), CRC32('abcde');
select crc32('hello'), crc32('hello world'), crc32('hello multicolored world!')

-- number string
select crc32('-12809132123');
select crc32('37291890239130039201840921');

-- mixed case letters
select crc32('TYHQWHJNfjewkefhklwew');
select crc32('huejwkfnewnjfnfe4woipofemwkemvwkTYUJIWfjkqajefiowjvkew');

--Chinese
select crc32('你好世界');
select crc32('你好，数据库');
select crc32('GitHub 上的 MatrixOne 社区充满活力、热情且知识渊博');

drop database if exists test;
create database test;
use test;
drop table if exists t1;
create table t1 (col1 int, col2 char(10));
insert into t1 values(1,'abcdef');
insert into t1 values(2,'_bcdef');
insert into t1 values(3,'a_cdef');
insert into t1 values(4,'ab_def');
insert into t1 values(5,'abc_ef');
insert into t1 values(6,'abcd_f');
insert into t1 values(7,'abcde_');
select col1,crc32(col2) from t1;
drop table t1;


drop table if exists t1;
create table t1 (a json,b int);
insert into t1 values ('{"t1":"a"}',1);
insert into t1 values ('{"t1":"b"}',2);
insert into t1 values ('{"t1":"c"}',3);
select crc32(a),b from t1;


drop table if exists test_table;
create table test_table (
id int auto_increment primary key,
text_column varchar(255) not null
);
insert into test_table (text_column) VALUES ('Hello, World!');
insert into test_table (text_column) VALUES ('Goodbye, World!');
insert into test_table (text_column) VALUES ('FooBar');

select id, text_column, CRC32(text_column) as crc32_checksum
from test_table;

alter table test_table add column crc32_column int unsigned;

update test_table
set crc32_column = CRC32(text_column);

select id, text_column, crc32_column
FROM test_table;

set @a = 'External Data';
select CRC32(@a) as crc32_external_data;

select id, text_column, crc32_column
from test_table;
where crc32_column = CRC32(@a);
drop table test_table;


drop table if exists employees;
create table employees (
employeeNumber int(11) not null,
lastName text not null,
firstName text not null,
extension text not null,
email text not null,
officeCode text not null,
reportsTo int(11) DEFAULT NULL,
jobTitle text not null,
primary key (employeeNumber)
);
insert  into employees(employeeNumber,lastName,firstName,extension,email,officeCode,reportsTo,jobTitle) values
(1002,'Murphy','Diane','x5800','dmurphy@classicmodelcars.com','1',NULL,'President'),
(1056,'Patterson','Mary','x4611','mpatterso@classicmodelcars.com','1',1002,'VP Sales'),
(1076,'Firrelli','Jeff','x9273','jfirrelli@classicmodelcars.com','1',1002,'VP Marketing'),
(1088,'Patterson','William','x4871','wpatterson@classicmodelcars.com','6',1056,'Sales Manager (APAC)'),
(1102,'Bondur','Gerard','x5408','gbondur@classicmodelcars.com','4',1056,'Sale Manager (EMEA)'),
(1143,'Bow','Anthony','x5428','abow@classicmodelcars.com','1',1056,'Sales Manager (NA)'),
(1165,'Jennings','Leslie','x3291','ljennings@classicmodelcars.com','1',1143,'Sales Rep'),
(1166,'Thompson','Leslie','x4065','lthompson@classicmodelcars.com','1',1143,'Sales Rep'),
(1188,'Firrelli','Julie','x2173','jfirrelli@classicmodelcars.com','2',1143,'Sales Rep'),
(1216,'Patterson','Steve','x4334','spatterson@classicmodelcars.com','2',1143,'Sales Rep'),
(1286,'Tseng','Foon Yue','x2248','ftseng@classicmodelcars.com','3',1143,'Sales Rep'),
(1323,'Vanauf','George','x4102','gvanauf@classicmodelcars.com','3',1143,'Sales Rep'),
(1337,'Bondur','Loui','x6493','lbondur@classicmodelcars.com','4',1102,'Sales Rep');
select crc32(lastName),crc32(firstName) from employees;
select (sum(crc32(concat_ws(lastName,firstName,extension)))) as `columns` from employees where 1=1 and reportsTo > 1000;
drop table employees;

drop table if exists jobs;
create table job(
jobid int primary key,
jobTitle varchar(50)
);
insert into job values
(1,'President'),
(2,'VP Sales'),
(3,'VP Marketing'),
(4,'Sales Manager (APAC)'),
(5,'Sale Manager (EMEA)'),
(6,'Sales Manager (NA)'),
(7,'Sales Rep'),
(8,'Marketing');
select crc32(jobTitle) from job;
drop table job;

drop database test;
