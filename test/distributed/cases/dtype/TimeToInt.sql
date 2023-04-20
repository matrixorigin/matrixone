-- @suit
-- @case
-- @desc:type conversion from time types to int / decimal
-- @label:bvt

drop database if exists test;
create database test;
use test;

-- explicit:datetime to int and decimal
drop table if exists test01;
create table test01(col1 datetime);
insert into test01 values('2020-01-13 12:20:59.1234586153121');
insert into test01 values('2023-04-17 01:01:45');
insert into test01 values('9999-01-01 26:25:26');
insert into test01 values(NULL);
select * from test01;

select cast(col1 as int) from test01;
select cast(col1 as decimal(20,10)) from test01;
drop table test01;

-- explicit:datetime with scale to int and decimal
drop table if exists test01;
create table test01(col1 datetime(3));
insert into test01 values('2020-01-13 12:20:59.12343243');
insert into test01 values('2023-04-17 01:01:45');
insert into test01 values(NULL);
select * from test01;

select cast(col1 as int) from test01;
select cast(col1 as decimal(20,10)) from test01;
drop table test01;

-- explicit:date to int
drop table if exists test02;
create table test02(col1 date);
insert into test02 values('2020-12-13');
insert into test02 values('2023-01-01');
insert into test02 values(NULL);
select * from test02;

select cast(col1 as int) from test02;
drop table test02;

-- explicit:time to int and decimal
drop table if exists test04;
create table test04(col1 time);
insert into test04 values('00:01:59');
insert into test04 values('23:59:59');
insert into test04 values(NULL);

select cast(col1 as int) from test04;
select cast(col1 as decimal(20,10)) from test04;

drop table test04;

-- implicit:datetime to int/decimal
drop table if exists test05;
drop table if exists test06;
drop table if exists test07;

create table test05(col1 int);
create table test06(col1 datetime);
create table test07(col1 decimal(38,18));

insert into test06 values('2020-01-13 12:20:59.123');
insert into test06 values('2023-04-17 01:01:45.65');
insert into test06 values(NULL);

insert into test05 select * from test06;
select cast(col1 as decimal(20,18)) from test06;
insert into test07 select * from test06;

select * from test05;
select * from test07;
drop table test05;
drop table test06;
drop table test07;

-- implicit:datetime with scale to int/decimal
drop table if exists test05;
drop table if exists test06;
drop table if exists test07;

create table test05(col1 int);
create table test06(col1 datetime(6));
create table test07(col1 decimal(38,18));

insert into test06 values('2020-01-13 12:20:59.123');
insert into test06 values('2023-04-17 01:01:45.65');
insert into test06 values(NULL);

insert into test05 select * from test06;
select cast(col1 as decimal(20,18)) from test06;
insert into test07 select * from test06;

select * from test05;
select * from test07;
drop table test05;
drop table test06;
drop table test07;

-- implicit:date to int
drop table if exists test07;
drop table if exists test08;
create table test07(col1 int);
create table test08(col1 date);
insert into test08 values('1995-12-12');
insert into test08 values('2023-01-01');
insert into test08 values(NULL);

insert into test07 select * from test08;

select * from test07;
drop table test07;
drop table test08;

-- implicit:time to int/decimal
drop table if exists test11;
drop table if exists test12;
drop table if exists test13;
create table test11(col1 int);
create table test12(col1 time);
create table test13(col1 decimal(10,0));
insert into test12 values('12:20:59');
insert into test12 values('01:01:45');
insert into test12 values(NULL);

insert into test11 select * from test12;
select cast(col1 as decimal(20,15)) from test11;
insert into test13 select * from test12;

select * from test11;
select * from test13;
drop table test11;
drop table test12;
drop table test13;

-- nested with the time function /datediff/timediff
drop table if exists test14;
create table test14(col1 int,col2 datetime,col3 decimal(20,5));
insert into test14 values(1,'2020-01-13 12:20:59',NULL);
insert into test14 values(2,'2023-04-17 01:01:45',-8.89789552);
insert into test14 values(3,NULL,456456.7887994512);
select * from test14;

select cast(date(col2) as int) from test14;
select cast(date_add(col2,interval 45 day) as int) from test14;
select cast(date_sub(col2,interval 45 day) as int) from test14;
select cast(to_date('2022-01-06 10:20:30','%Y-%m-%d %H:%i:%s') as int) as result;

-- date with n: + - * %
drop table if exists test17;
create table test17(col1 date,col2 int);
insert into test17 values('1990-01-01',10);
insert into test17 values('2023-04-18',20);
insert into test17 values(NULL,30);

select col1 + col2 from test17;
select col1 - col2 from test17;
select col1 * 100 from test17;
select col1 % 2 from test17;
drop table test17;

-- date with date: -
drop table if exists test18;
create table test18(col1 date,col2 date);
insert into test18 values('1997-01-13','2005-01-26');

select col1 - col2 from test18;
select col2 - col1 from test18;
drop table test18;

-- date with datetime: -
drop table if exists test19;
create table test19(col1 date,col2 datetime);
insert into test19 values('1997-01-13','2005-01-26 01:05:06.156');
insert into test19 values('2023-04-18','1997-01-01 00:00:00');

select col1 - col2 from test19;
select col2 - col1 from test19;
drop table test19;

-- datetime with n: + - * / %
drop table if exists test14;
create table test14(col1 int,col2 datetime);
insert into test14 values(1,'2020-01-13 12:20:59');
insert into test14 values(2,'2023-04-17 01:01:45');
insert into test14 values(3,NULL);
select * from test14;

select col2 / col1 from test14;
select col1 * col2 from test14;
select col1 + col2 from test14;
select col1 - col2 from test14;
select col2 % col1 from test14;
drop table test14;

-- datetime with n.m
drop table if exists datetime01;
create table datetime01(col1 datetime(3), col2 decimal);
insert into datetime01 values('2000-12-12 12:58:58.123',123.454648);
insert into datetime01 values('1996-04-06 01:01:01.4',-78645312.7894);
select * from datetime01;

select col1 + col2 from datetime01;
select col1 - col2 from datetime01;
select col1 * col2 from datetime01;
select col1 / col2 from datetime01;
select col1 % col2 from datetime01;
select col1 + 100.291024 from datetime01;
select col1 + (-839402143.9320) from datetime01;
select col1 * 798461455511 from datetime01;
drop table datetime01;

-- datetime with datetime:-
drop table if exists datetime01;
create table datetime01(col1 datetime(3), col2 datetime);
insert into datetime01 values('2000-12-12 12:58:58','1969-03-03 01:05:59');
insert into datetime01 values('1996-04-06 01:01:01','2019-05-04 23:12:29');
select * from datetime01;

select col1 - col2 from datetime01;
drop table datetime01;

-- time with n:+ - * / %
drop table if exists time01;
create table time01(col1 int,col2 time);
insert into time01 values(1,'12:20:59');
insert into time01 values(2,'23:59:59');
insert into time01 values(3,NULL);
select * from time01;

select col2 / col1 from time01;
select col1 * col2 from time01;
select col1 + col2 from time01;
select col1 - col2 from time01;
select col2 % col1 from time01;
drop table time01;

-- time with n.m:+ - * / %
drop table if exists time01;
create table time01(col1 decimal,col2 time);
insert into time01 values(1787945645414794854456412,'12:20:59');
insert into time01 values(-789466511231027845,'23:59:59');
select * from time01;

select col2 / col1 from time01;
select col1 * col2 from time01;
select col1 + col2 from time01;
select col1 - col2 from time01;
select col2 % col1 from time01;

drop table time01;

-- modify time_zone to UTC
SET time_zone = '+0:00';

-- explicit:timestamp to int and decimal
drop table if exists test03;
create table test03(col1 timestamp);
insert into test03 values('1997-01-13 12:20:59.898612');
insert into test03 values('1970-01-30 23:59:59');
insert into test03 values(NULL);
select * from test03;

select cast(col1 as int) from test03;
select cast(col1 as decimal(20,18)) from test03;
drop table test03;

-- explicit:timestamp with scale to int and decimal
drop table if exists test03;
create table test03(col1 timestamp(5));
insert into test03 values('1997-01-13 12:20:59.898612');
insert into test03 values('1970-01-30 23:59:59');
insert into test03 values(NULL);
select * from test03;

select cast(col1 as int) from test03;
select cast(col1 as decimal(20,18)) from test03;
drop table test03;

-- implicit:timestamp to int/decimal
drop table if exists test09;
drop table if exists test10;
drop table if exists test11;
create table test09(col1 int);
create table test10(col1 timestamp);
create table test11(col1 decimal(38,18));
insert into test10 values('2022-12-13 12:20:59.2132131');
insert into test10 values('1998-10-10 01:01:45');
insert into test10 values(NULL);

insert into test09 select * from test10;
select cast(col1 as decimal(10,5)) from test10;
insert into test11 select * from test10;

select * from test09;
select * from test11;
drop table test09;
drop table test10;
drop table test11;

-- implicit:and timestamp with scale
drop table if exists test09;
drop table if exists test10;
create table test09(col1 int);
create table test10(col1 timestamp(6));
create table test11(col1 decimal(38,18));
insert into test10 values('2022-12-13 12:20:59.2132131');
insert into test10 values('1998-10-10 01:01:45');
insert into test10 values(NULL);

insert into test09 select * from test10;
select cast(col1 as decimal(10,5)) from test10;
insert into test11 select * from test10;

select * from test09;
select * from test11;
drop table test09;
drop table test10;
drop table test11;

-- timestamp with n:+ - * / %
drop table if exists timestamp01;
create table timestamp01(col1 int,col2 timestamp);
insert into timestamp01 values(10000,'2020-01-13 12:20:59');
insert into timestamp01 values(-22220,'2023-04-17 01:01:45');
insert into timestamp01 values(123654,NULL);
select * from timestamp01;

select col1 + col2 from timestamp01;
select col1 - col2 from timestamp01;
select col1 * col2 from timestamp01;
select col2 / col1 from timestamp01;
select col2 % col1 from timestamp01;
drop table timestamp01;

-- timestamp with n.m:+ - * / %
drop table if exists timestamp01;
create table timestamp01(col1 decimal(38,18),col2 timestamp);
insert into timestamp01 values(NULL,'2020-01-13 12:20:59');
insert into timestamp01 values(-4514531.548451,'2023-04-17 01:01:45');
insert into timestamp01 values(123654.7897948,NULL);
select * from timestamp01;

select col1 + col2 from timestamp01;
select col1 - col2 from timestamp01;
select col1 * col2 from timestamp01;
select col2 / col1 from timestamp01;
select col2 % col1 from timestamp01;
drop table timestamp01;

drop database test;
