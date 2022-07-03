-- @suite

-- @case
-- @desc:test for all the datatype
-- @label:bvt
drop table if exists numtable;
create table numtable(id int,fl float, dl double);
insert into numtable values(1,123456,123456);
insert into numtable values(2,123.456,123.456);
insert into numtable values(3,1.234567,1.234567);
insert into numtable values(4,1.234567891,1.234567891);
insert into numtable values(5,1.2345678912345678912,1.2345678912345678912);
select id,fl,dl from numtable order by id;
drop table if exists numtable;
create table numtable(id int,fl float(5,3));
insert into numtable values(2,99);
insert into numtable values(3,99.123);
insert into numtable values(4,99.1236);
select id,fl from numtable;
drop table if exists numtable;
create table numtable(id int,fl float(23));
insert into numtable values(1,1.2345678901234567890123456789);
select id,fl from numtable;
drop table if exists numtable;
create table numtable(id int,dl double);
insert into numtable values(1,1.2345678901234567890123456789);
select id,dl from numtable;
drop table if exists numtable;
create table numtable(a tinyint unsigned, b smallint unsigned, c int unsigned, d bigint unsigned);
insert into numtable values(255,65535,4294967295,18446744073709551615);
select a,b,c,d from numtable;
drop table if exists numtable;
create table numtable(a tinyint signed, b smallint signed, c int signed, d bigint signed);
insert into numtable values(127,32767,2147483647,9223372036854775807);
insert into numtable values(-128,-32768,-2147483648,-9223372036854775808);
select a,b,c,d from numtable;
drop table if exists names;
create table names(name varchar(255),age char(255));
insert into names(name, age) values('Abby', '24');
insert into names(name, age) values("Bob", '25');
insert into names(name, age) values('Carol', "23");
insert into names(name, age) values("Dora", "29");
select name,age from names;
drop table if exists t4;
create table t4(a int, b date, c datetime);
insert into t4 values(1, '2021-12-13','2021-12-13 13:00:00');
insert into t4 values(2, '20211214','20211213');
insert into t4 values(3,'2021-12-14','2021-12-14');
insert into t4 values(4,'2021-12-15','2021-12-14');
select * from t4 where b>'20211213';
select * from t4 where c>'20211213';
select * from t4 where b>'2021-12-13';
select * from t4 where c>'2021-12-13';
select * from t4 where b between '2021-12-13' and '2021-12-14';
select * from t4 where b not between '2021-12-13' and '2021-12-14';
