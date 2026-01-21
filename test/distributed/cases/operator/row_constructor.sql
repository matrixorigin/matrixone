-- @suit
-- @case
-- @desc: row constructor
-- @label:bvt

create database test;
use test;

select (1,2,3)=(0,null,3);
select (1,2,3)=(1,null,3);
select (1,2,3)=(1,null,0);
select (1,2,3) = (1,null,3);
select (1,2,3) = (1+1, null, 3);
select (1,2,3) = (1,null,3+1);
select (1,2,3) <> (1,null,3);
select (1,2,3) <> (1+1,null,3);
select (1,2,3) <> (1,null,3+1);
select (1,2) > (2,3);
select (1.000, 2.323200) = (1,2.3232);
select (-10, 200) < (100,200);
select (null,null) = (null,null);
select (1,2,2,3) > (2,3,4,5);
select (78415614.7894,789854.0) = (78415614.7894,789854.0);
select (1,null) < (2,null);
select (2,3) >= (1,3);
select (-2,1,3) >= (-1,2,3);
select (-387293.324321,32190391.34134,000) <= (-387293.324321, -123, -1);

-- + - * / % mod
select (1,2,3) > (-1,-3+2,2*3);
select (128093,341231431,0) < (999*22,328932/0.3,null);
select (100 % 3,100) >= (30,100);
select (2739,32,-328392,329) <= (23920000 mod 9999,32,-328392,329);

select ('abc','def') = ('abc','def');
select ('a', null) < ('b', null);
select ('1234dhyecufjwqv','38293r243f') >= ('1211111','38092i4(((');
select ('&') > ('1');

drop table if exists row01;
create table row01(a int, b int, c int);
insert into row01 values (1, 2, 3),
                         (null,2,3), (1,null,3), (1,2,null),
                         (null,2,3+1), (1,null,3+1), (1,2+1,null),
                         (null,2,3-1), (1,null, 3-1), (1,2*1,null);

select (1,2,3) = (1,   null, 3);
select (1,2,3) = (1+1, null, 3);
select (1,2,3) = (1,   null, 3+1);
select * from row01 where (a,b,c) = (1,2,3);

select (1,2,3) <> (1,null,3);
select (1,2,3) <> (1+1,null,3);
select (1,2,3) <> (1,null,3+1);
select * from row01 where (a,b,c) <> (1,2,3);

select (1,2,3) < (null,2,3);
select (1,2,3) < (1,null,3);
select (1,2,3) < (1*1,null,3);
select (1,2,3) < (1+1,null,3);
select * from row01 where (a,b,c) < (1,2,3);

select (1,2,3) <= (null,2,3);
select (1,2,3) <= (1,null,3);
select (1,2,3) <= (1-1,null,3);
select (1,2,3) <= (1+1,null,3);
select * from row01 where (a,b,c) <= (1,2,3);

select (1,2,3) > (null,2,3);
select (1,2,3) > (1,null,3);
select (1,2,3) > (1-1,null,3);
select (1,2,3) > (1+1,null,3);
select * from row01 where (a,b,c) > (1,2,3);

select (1,2,3) >= (null,2,3);
select (1,2,3) >= (1,null,3);
select (1,2,3) >= (1-1,null,3);
select (1,2,3) >= (100/11,null,3);
select * from row01 where (a,b,c) >= (1,2,3);
drop table row01;

-- abnormal test
select (2930,291201) > (2189,1212,3232);
select (1,2,4,5) < (1,2,4);
select (null,null,2893.323) = ()

-- >,=,<,>=,<=,between and,<>,!=
drop table if exists row01;
create table row01(col1 char, col2 varchar(20));
insert into row01 values('a',' uc32qr3');
insert into row01 values('b',' 09920011');
insert into row01 values('c','3ru82q ijf');
insert into row01 values(null,'e&*(&  &&');
select * from row01;
select (col1,col2) >= (col1,col2) from row01;
select (col1,col2) <= (col1,col2) from row01;
select (col1,col2) < (col1,0) from row01;
select (col1,col2,0) > (col1,col2,col1) from row01;
select (col1,col2) = (col1,col2) from row01;
select * from row01 where (col1,col2) between ('a','uc32qr3') and ('c','3ru82qijf');
select col1,(col1,col2) <> (col1,0) from row01;
select col1,(col1,col2) != (col1,0) from row01;

-- columns nested with string functions
select (concat_ws(col1,col2),col2) >= (col1,col2) from row01;
select (col1,ltrim(col2)) <= (col1,col2) from row01;
select (col1,rtrim(col2)) < (col1,'382039i2jfie') from row01;
select (col1,trim(col2)) < (col1,'382039i2jfie') from row01;
select (col1,substring(col2,1,3),col1) > (col1,col2,col1) from row01;
select (col1,lpad(col2,30,'abc')) = (col1,col2) from row01;
select * from row01 where (col1,col2) between ('a','uc32qr3') and ('c','3ru82qijf');
select (col1),(reverse(col1),col2) <> (col1,0) from row01;
select col1,(reverse(col1),col2) != (col1,0) from row01;
drop table row01;

drop table if exists row02;
create table row02(col1 int,col2 tinyint unsigned,col3 smallint,col4 decimal(20,10),col5 float);
insert into row02 values(1,32,38,3829342.3224141,-0.9883283);
insert into row02 values(2,null,-190,-99932.1,9320.32);
insert into row02 values(3,3,832,-39203.83280932,null);
insert into row02 values(null,0,0,1.2,3801432.3213);
select (col1,col2,col3) > (0,0,0) from row02;
select (col1,col2,col3,col4,col5) < (100,100,100,378217493,3218941031) from row02;
select (col1,col2) >= (col2,col1) from row02;
select (col3,col4) <= (372913412.2143,col4) from row02;
select * from row02 where (col1,col2) between (0,10000) and (10,20000);
select (col1,col2) <> (col1,col2) from row02;
select (col1,col2) != (col4,col5) from row02;

select (col1 * col2,col2,col3) > (0,0,0) from row02;
select (col1,col2 / 2,col3,col4,col5) < (100,100,100,378217493,3218941031) from row02;
select (col1,col2 + col3) >= (col2,col1) from row02;
select (col3,col4 - 329302.32) <= (372913412.2143,col4) from row02;
select * from row02 where (col1 % 100,col2) between (0,10000) and (10,20000);
select (col1,col2) <> (col1 mod 10,col2) from row02;
select (col1,col2 + 10000) != (col4,col5) from row02;
drop table row02;

drop table if exists row03;
create table row03(col1 int unsigned,col2 bigint,col3 decimal(20,10),col4 double);
insert into row03 values(1,39438432094940434,838209302.34094043234,3239283092.23830922);
insert into row03 values(2,37394,-93298439024,4830403434.4329043);
insert into row03 values(3,null,null,null);
select (col1,col2,col3) > (-1,32893,32932) from row03;
select (col1,col2,col3) < (col1,col2,12345678989) from row03;
select (col1,col2,100) >= (col1,col2,50) from row03;
select (col1,col2,col3) <= (col1,col2,col3) from row03;
select (col1,col2,col3) <> (1,39438432094940434,838209302.34094043234) from row03;
select (col1,col2,col3) != (1,39438432094940434,838209302.34094043234) from row03;

select (col1 * col2,col2,col3) > (-1,32893,32932) from row03;
select (col1,col2 - 10000,col3) < (col1,col2,12345678989) from row03;
select (col1,col2 + 182901.213123,100) >= (col1,col2,50) from row03;
select (col1,col2,col3 / 45) <= (col1,col2,col3) from row03;
select (col1,col2,col3) <> (1,39438432094940434 + col1,838209302.34094043234) from row03;
select (col1,col2,col3) != (1,39438432094940434 % 20,838209302.34094043234) from row03;
drop table row03;

-- date
drop table if exists row04;
create table row04(col1 date,col2 datetime,col3 timestamp);
insert into row04 values('2017-06-15','2000-01-01 00:00:00','2022-01-02 00:00:01.512345');
insert into row04 values('2023-04-06','2023-04-06 12:12:00','1999-01-02 00:00:09');
insert into row04 values(null,null,null);
select (col1,col2,col3) >= ('2017-06-15','2000-01-01 00:00:00','2022-01-02 00:00:01.512345') from row04;
select (col1,col2,col3) <= (col1,col2,'2023-01-02 00:00:01.512345')from row04;
select (col1,col2) > ('1999-01-01','1999-01-04 00:00:09') from row04;
select (col1,col3) < ('1970-01-01','2055-01-01 10:10:10') from row04;
select * from row04 where (col1,col2) between ('2017-06-15','1000-01-01 00:00:00') and('2025-06-15','2300-01-01 00:00:00');
select (col1,col3) <> ('2017-06-15','2022-01-02 00:00:01.512345') from row04;
select (col1,col3) != ('2017-06-15','2022-01-02 00:00:01.5123-5') from row04;

select (col1,col2,date(col3)) >= ('2017-06-15','2000-01-01 00:00:00','2022-01-02') from row04;
select (col1,col2,to_date(col3,'%Y-%m-%d %H:%i:%s')) <= (col1,col2,'2023-01-02 00:00:01.512345')from row04;
select (date_add(col1,interval 45 day),col2) > ('1999-01-01','1999-01-04 00:00:09') from row04;
select (date_sub(col1,interval 45 day),col3) < ('1970-01-01','2055-01-01 10:10:10') from row04;
select (year(col1),year(col2)) > (1999,2003) from row04;
select (month(col1),month(col2)) <= (12,12) from row04;
select (day(col1),day(col2)) <> (30,30) from row04;
select (weekday(col1),weekday(col2)) != (12,34) from row04;
drop table row04;

-- blob,json,binary
drop table if exists row05;
create table row05(col1 blob,col2 json,col3 binary(10) not null);
insert into row05 values('abcdef','{"t1":"a"}',456);
insert into row05 values('_bcdef','{"t1":"c"}',100000);
insert into row05 values('__cdef',null,0);
select (col1,col2) = ('abcdef','{"t1":"a"}') from row05;
select (col1,col2) != ('abcdef','{"ehyiuwqnve":"ashyiujewv"}') from row05;
drop table row05;

drop database test;
