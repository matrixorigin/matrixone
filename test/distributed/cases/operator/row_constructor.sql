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
select (1,2) in ((1,2),(3,4));
select (1,2) in ((1,3),(3,4));
select (1,2) in ((1,2));
select (1,2) not in ((1,2));
select (1,2) not in ((1,3),(3,4));
select (1,2) in ((1,null));
select (1,2) in ((1,null),(1,2));
select (1,2) not in ((1,null));
select (1,null) in ((1,null));
select (1,2) not in ((1,null),(2,2));
select null in (1, null) as r;
select null not in (1, null) as r;
create sequence seq_null_in;
select null in (nextval('seq_null_in')) as r;
select currval('seq_null_in') as v;
drop sequence seq_null_in;
select null in ((1,2));

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
select (col1,col2) = ('abcdef','{"t1": "a"}') from row05;
select (col1,col2) != ('abcdef','{"ehyiuwqnve": "ashyiujewv"}') from row05;
drop table row05;

-- #28295: direct row constructors against multi-column scalar subqueries.
drop table if exists row_scalar_28295;
create table row_scalar_28295(id int primary key, a int, b int);
insert into row_scalar_28295 values (1,1,5),(2,1,null),(3,2,8),(4,2,10);
select (1,5) = (select a,b from row_scalar_28295 where id=1) as match_v;
select (1,5) = (select a,b from row_scalar_28295 where id=99) as empty_v;
select (1,5) = (select a,b from row_scalar_28295 where id=2) as null_v;
select (1,4) < (select a,b from row_scalar_28295 where id=1) as order_v;
select (1,null) <=> (select a,b from row_scalar_28295 where id=2) as null_safe_v;
select (1,(select 5)) = (select a,b from row_scalar_28295 where id=1) as nested_scalar_v;
select id from row_scalar_28295 outer_row
where (outer_row.a, outer_row.b) =
      (select inner_row.a, inner_row.b from row_scalar_28295 inner_row where inner_row.id=outer_row.id)
order by id;
select id from row_scalar_28295 outer_row
where (outer_row.id, outer_row.a) =
      (select outer_row.id, inner_row.a from row_scalar_28295 inner_row where inner_row.id=outer_row.id)
order by id;
select (1,7) =
       (select outer_row.id,7 from row_scalar_28295 inner_row
        where inner_row.id=outer_row.id limit 1) as outer_limit_v
from row_scalar_28295 outer_row where outer_row.id=1;
select (1,7) =
       (select distinct outer_row.id,7 from row_scalar_28295 inner_row
        where inner_row.id=outer_row.id) as outer_distinct_v
from row_scalar_28295 outer_row where outer_row.id=1;
select (0,null) <=>
       (select count(*),sum(inner_row.b) from row_scalar_28295 inner_row
        where inner_row.id=outer_row.id+100 having count(*)=0) as aggregate_empty_v
from row_scalar_28295 outer_row where outer_row.id=1;
create table row_scalar_outer_28295(k int primary key);
create table row_scalar_inner_28295(k int, v int);
insert into row_scalar_outer_28295 values (1),(2);
insert into row_scalar_inner_28295 values (1,10);
select o.k, (0,null) <=>
       (select count(1),sum(i.v) from row_scalar_inner_28295 i where i.k<o.k) as real_rows_only
from row_scalar_outer_28295 o order by o.k;
select o.k, (0,null) <=>
       (select count(*),sum(i.v) from row_scalar_inner_28295 i where i.k<o.k limit 0) as rejected_limit
from row_scalar_outer_28295 o order by o.k;
select o.k, (0,null) <=>
       (select sum(coalesce(i.v,0)),sum(i.v) from row_scalar_inner_28295 i where i.k<o.k) as rejected_expression
from row_scalar_outer_28295 o order by o.k;
select o.k, (o.k,(select v from row_scalar_inner_28295 where k=1)) =
       (select count(*),sum(i.v) from row_scalar_inner_28295 i where i.k<o.k) as rejected_composition
from row_scalar_outer_28295 o order by o.k;
select 1 as query_after_rejections;
insert into row_scalar_inner_28295 values (1,20);
select o.k, (select group_concat(i.v order by i.v desc separator '~')
             from row_scalar_inner_28295 i where i.k<o.k) as ordered_concat
from row_scalar_outer_28295 o order by o.k;
create table row_scalar_decimal_28295(x decimal(10,2) not null, y int not null);
select (1.001,1) = (select x,y from row_scalar_decimal_28295) as empty_decimal_eq;
select (1.001,1) <> (select x,y from row_scalar_decimal_28295) as empty_decimal_neq;
select (select x,y from row_scalar_decimal_28295) = (1.001,1) as reversed_empty_decimal_eq;
select 1.001 = (select x from row_scalar_decimal_28295) as ordinary_empty_decimal_eq;
insert into row_scalar_decimal_28295 values (1.00,1);
select (1.001,1) <> (select x,y from row_scalar_decimal_28295) as matched_decimal_neq;
select o.k, (1.001,1) <> (select i.x,i.y from row_scalar_decimal_28295 i where i.y=o.k) as correlated_decimal_neq
from row_scalar_outer_28295 o order by o.k;
select o.k, (1.001,0) <> (select min(i.x),count(*) from row_scalar_decimal_28295 i where i.y<o.k) as aggregate_decimal_neq
from row_scalar_outer_28295 o order by o.k;
drop table row_scalar_decimal_28295;
create table row_scalar_ifnull_outer_28295(k int primary key);
insert into row_scalar_ifnull_outer_28295 values (1),(2),(3);
create table row_scalar_ifnull_inner_28295(k int, v int);
insert into row_scalar_ifnull_inner_28295 values (1,10),(1,20),(2,null);
select o.k, ifnull((select min(i.v) from row_scalar_ifnull_inner_28295 i where i.k<o.k),0) as selected_ifnull
from row_scalar_ifnull_outer_28295 o order by o.k;
select o.k, (select min(i.v) from row_scalar_ifnull_inner_28295 i where i.k<o.k) as raw_min
from row_scalar_ifnull_outer_28295 o order by o.k;
drop table row_scalar_ifnull_inner_28295;
drop table row_scalar_ifnull_outer_28295;
drop table row_scalar_inner_28295;
drop table row_scalar_outer_28295;
select (1,5) = (select a,b from row_scalar_28295 where id>0) as multi_v;
select (1,2) = (select 1,2 limit 0) as no_from_empty_limit_v;
select (1,2) = (select 1,2 limit 1 offset 1) as no_from_empty_offset_v;
select (1,2) = (select 1,2 limit 1 offset 0) as no_from_one_v;
select (1,2) <=> (select 1,2 limit 0) as no_from_null_safe_empty_v;
create sequence row_scalar_guard_seq;
select (nextval('row_scalar_guard_seq'),0) < (select 1,1) as volatile_order_v;
select (0,null) <=>
       (select count(*),sum(inner_row.b) from row_scalar_28295 inner_row
        where inner_row.id=outer_row.id+100 having nextval('row_scalar_guard_seq')=1) as volatile_having_v
from row_scalar_28295 outer_row where outer_row.id=1;
select nextval('row_scalar_guard_seq') as first_value_after_rejection;
drop sequence row_scalar_guard_seq;
set @row_scalar_a=1, @row_scalar_b=5;
prepare row_scalar_stmt from 'select (?,?) = (select a,b from row_scalar_28295 where id=1) as prepared_v';
execute row_scalar_stmt using @row_scalar_a, @row_scalar_b;
deallocate prepare row_scalar_stmt;
drop table row_scalar_28295;

drop database test;
