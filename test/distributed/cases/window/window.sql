drop table if exists t1;
create table t1 (a int, b datetime);
insert into t1 values(1, '2020-11-11'), (2, '2020-11-12'), (3, '2020-11-13');
select sum(a) over(partition by a order by b range between interval 1 day preceding and interval 2 day following) from t1;

drop table if exists t1;
create table t1 (a int, b date);
insert into t1 values(1, '2020-11-11'), (2, '2020-11-12'), (3, '2020-11-13'), (1, '2020-11-11'), (2, '2020-11-12'), (3, '2020-11-13'), (1, '2020-11-11'), (2, '2020-11-12'), (3, '2020-11-13');
select max(a) over(order by b range between interval 1 day preceding and interval 2 day following) from t1;

drop table if exists t1;
create table t1 (a int, b time);
insert into t1 values(1, 112233), (2, 122233), (3, 132233), (1, 112233), (2, 122233), (3, 132233), (1, 112233), (2, 122233), (3, 132233);
select min(a) over(order by b range between interval 1 hour preceding and current row) from t1;

drop table if exists t1;
create table t1 (a int, b timestamp);
insert into t1 values(1, '2020-11-11'), (2, '2020-11-12'), (3, '2020-11-13'), (1, '2020-11-11'), (2, '2020-11-12'), (3, '2020-11-13'), (1, '2020-11-11'), (2, '2020-11-12'), (3, '2020-11-13');
select count(*) over(order by b range current row) from t1;

drop table if exists t1;
create table t1 (a int, b int, c int);
insert into t1 values(1, 2, 1), (3, 4, 2), (5, 6, 3), (7, 8, 4), (3, 4, 5), (3, 4, 6), (3, 4, 7);
select a, rank() over (partition by a) from t1 group by a, c;
select a, c, rank() over (partition by a order by c) from t1 group by a, c;
select a, c, rank() over (partition by a order by c) from t1 group by a, c;
select a, c, b, rank() over (partition by a, c, b) from t1;
select a,  b, rank() over (partition by a, b) from t1;
select a, c, sum(a) over (), sum(c) over () from t1;
select a, c, sum(a) over (order by c), sum(c) over (order by a) from t1;
select a, sum(b), sum(sum(b)) over (partition by a), sum(sum(b)) over (partition by c) from t1 group by a, c;
select a, sum(b), rank() over (partition by a +1), rank() over (partition by c), c from t1 group by a, c;
select a, sum(b), sum(sum(b))  over (partition by a) as o from t1 group by a, c;
select a, sum(b), cast(sum(sum(b))  over (partition by a+1 order by a+1 rows between 2  preceding and CURRENT row) as float) as o from t1 group by a, c;
select a, sum(b), sum(sum(b)) over (partition by a rows BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) from
    t1 group by a, c;
select a, sum(a) over (partition by c order by b range BETWEEN 3 preceding and 4 following), c, b from t1;
select a, sum(a) over (order by a) from t1;
select a, rank() over (partition by a) from t1;
select a, rank() over () from t1;
select a, sum(a) over (partition by a rows current row) from t1;
select c, sum(c) over (order by c range between 1 preceding and 1 following) from t1;
select c, sum(100) over (order by c range between 1 preceding and 1 following), a, b from t1;
select c, sum(null) over (order by c range between 1 preceding and 1 following), a, b from t1;
select a, b, c, rank() over (partition by a, b order by c) from t1;
select a, c, rank() over(partition by a order by c rows current row) from t1;
select a, row_number() over (partition by a) from t1 group by a, c;
select a, c, row_number() over (partition by a order by c) from t1 group by a, c;
select a, c, row_number() over (partition by a order by c) from t1 group by a, c;
select a, c, b, row_number() over (partition by a, c, b) from t1;
select a,  b, row_number() over (partition by a, b) from t1;
select a, sum(b), row_number() over (partition by a +1), row_number() over (partition by c), c from t1 group by a, c;
select a, row_number() over (partition by a) from t1;
select a, row_number() over () from t1;
select a, b, c, row_number() over (partition by a, b order by c) from t1;
select a, c, row_number() over(partition by a order by c rows current row) from t1;
select a, dense_rank() over (partition by a) from t1 group by a, c;
select a, c, dense_rank() over (partition by a order by c) from t1 group by a, c;
select a, c, dense_rank() over (partition by a order by c) from t1 group by a, c;
select a, c, b, dense_rank() over (partition by a, c, b) from t1;
select a,  b, dense_rank() over (partition by a, b) from t1;
select a, sum(b), dense_rank() over (partition by a +1), dense_rank() over (partition by c), c from t1 group by a, c;
select a, dense_rank() over (partition by a) from t1;
select a, dense_rank() over () from t1;
select a, b, c, dense_rank() over (partition by a, b order by c) from t1;
select a, c, dense_rank() over(partition by a order by c rows current row) from t1;
select a, c, rank() over(order by a), row_number() over(order by a), dense_rank() over(order by a) from t1;

drop table if exists t1;
create table t1 (a int, b decimal(7, 2));
insert into t1 values(1, 12.12), (2, 123.13), (3, 456.66), (4, 1111.34);
select a, sum(b) over (partition by a order by a) from t1;

drop table if exists wf01;
create table wf01(i int,j int);
insert into wf01 values(1,1);
insert into wf01 values(1,4);
insert into wf01 values(1,2);
insert into wf01 values(1,4);
select * from wf01;
select i, j, sum(i+j) over (order by j desc rows unbounded preceding) foo from wf01;
select i, j, sum(i+j) over (order by j desc rows between 2 preceding and 2 following) as foo from wf01;
select i, j, sum(i+j) over (order by j desc rows unbounded preceding) foo from wf01 order by foo;
select i, j, sum(i+j) over (order by j desc rows unbounded preceding) foo from wf01 order by foo desc;

drop table if exists wf08;
create table wf08(d decimal(10,2), date date);
insert into wf08 values (10.4, '2002-06-09');
insert into wf08 values (20.5, '2002-06-09');
insert into wf08 values (10.4, '2002-06-10');
insert into wf08 values (3,    '2002-06-09');
insert into wf08 values (40.2, '2015-08-01');
insert into wf08 values (40.2, '2002-06-09');
insert into wf08 values (5,    '2015-08-01');
select * from (select rank() over (order by d) as `rank`, d, date from wf08) alias order by `rank`, d, date;
select * from (select dense_rank() over (order by d) as `d_rank`, d, date from wf08) alias order by `d_rank`, d, date;

drop table if exists wf07;
create table wf07 (user_id integer not null, date date);
insert into wf07 values (1, '2002-06-09');
insert into wf07 values (2, '2002-06-09');
insert into wf07 values (1, '2002-06-09');
insert into wf07 values (3, '2002-06-09');
insert into wf07 values (4, '2002-06-09');
insert into wf07 values (4, '2002-06-09');
insert into wf07 values (5, '2002-06-09');
select rank() over () r from wf07;
select dense_rank() over () r from wf07;

drop table if exists wf12;
create table wf12(d double);
insert into wf12 values (1.7976931348623157e+307);
insert into wf12 values (1);
select d, sum(d) over (rows between current row and 1 following) from wf12;

drop table if exists wf06;
create table wf06 (id integer, sex char(1));
insert into wf06 values (1, 'm');
insert into wf06 values (2, 'f');
insert into wf06 values (3, 'f');
insert into wf06 values (4, 'f');
insert into wf06 values (5, 'm');
drop table if exists wf07;
create table wf07 (user_id integer not null, date date);
insert into wf07 values (1, '2002-06-09');
insert into wf07 values (2, '2002-06-09');
insert into wf07 values (1, '2002-06-09');
insert into wf07 values (3, '2002-06-09');
insert into wf07 values (4, '2002-06-09');
insert into wf07 values (4, '2002-06-09');
insert into wf07 values (5, '2002-06-09');
select id value, sum(id) over (rows unbounded preceding) from wf06 inner join wf07 on wf07.user_id = wf06.id;

-- @suit
-- @case
-- @desc:window function
-- @label:bvt

drop database if exists test;
create database test;
use test;

-- partition by follows the bool type
drop table if exists bool01;
create table bool01(col1 int,col2 bool,col3 datetime);
insert into bool01 values(1, true, '2023-05-16 00:12:12');
insert into bool01 values(2, false, '1997-01-13 12:12:00');
insert into bool01 values(3, true, '2000-10-10 11:11:11');
insert into bool01 values(4, false, '1020-10-01 01:01:01');
insert into bool01 values(5, null, null);
insert into bool01 values(6, null, '1997-11-10 10:10:10');
select * from bool01;
select rank() over (partition by col2 order by col1), sum(col1) over (partition by col2 order by col3) from bool01;
select dense_rank() over (partition by col2 order by col1), sum(col1) over (partition by col2 order by col3) from bool01;

drop table bool01;

-- partition by follows char/varchar/text
drop table varchar01 if exists;
create table varchar01(col1 int, col2 varchar(12) primary key);
insert into varchar01 values(1, 'dhwenfewrfew');
insert into varchar01 values(2, 'wyeuijdew');
insert into varchar01 values(3, '数据库');
insert into varchar01 values(4, 'hejwkvrewvre');
insert into varchar01 values(5, '**&');
insert into varchar01 values(6, '12345');
insert into varchar01 values(7, 'database');
select *, rank() over (partition by col2 order by col1) as tmp from varchar01;
select dense_rank() over (partition by col2 order by col1) as tmp from varchar01;
drop table varchar01;

drop table if exists char01;
create table char01 (col1 integer, col2 char(1));
create table char01 (col1 integer, col2 char(1));
insert into char01 values (1, 'm');
insert into char01 values (2, 'f');
insert into char01 values (3, 'f');
insert into char01 values (4, 'f');
insert into char01 values (5, 'm');
select * from char01;
select *, rank() over (partition by col2 order by col1) as tmp from char01;
select dense_rank() over (partition by col2 order by col1) as tmp from char01;
drop table char01;

drop table if exists text01;
create table text01(col1 int, col2 text);
insert into text01 values(1, 'vdjnekwvrewvrjewkrmbew  bkejwkvmekrememwkvrewvrew re');
insert into text01 values(2, 'vdjnekwvrewvrjewkrmbew  bkejwkvmekrememwkvrewvrew re');
insert into text01 values(3, null);
insert into text01 values(4, '数据库，数据库，数据库，mo，mo，mo!');
insert into text01 values(5, null);
insert into text01 values(6, '数据库，数据库，数据库，mo，mo，mo!');
insert into text01 values(7, null);
select * from text01;
select *, rank() over (partition by col2 order by col1) as tmp from text01;
select dense_rank() over (partition by col2 order by col1) as tmp from text01;
drop table text01;

-- partition by and order by follows int
drop table if exists int01;
create table int01(col1 tinyint unsigned, col2 int, col3 timestamp);
insert into int01 values(100, 100, '2023-05-16 00:12:12');
insert into int01 values(98, -10, '2023-05-16 00:12:12');
insert into int01 values(100, null, '1997-05-16 00:12:12');
insert into int01 values(null, 100, '2023-05-16 00:12:12');
insert into int01 values(0, null, '1997-05-16 00:12:12');
insert into int01 values(null, null, null);
select * from int01;
select col1, avg(col2) over (partition by col1 order by col2) as tmp from int01;
select col1, sum(col2) over (partition by col2 order by col1) as tmp from int01;
select col1, max(col1) over (partition by col1 rows between 1 preceding and 1 following) from int01;
select col1, min(col2) over (partition by col3 order by col2) from int01;
drop table int01;

-- partition by and order by follows float
drop table if exists float01;
create table float01(col1 float, col2 date);
insert into float01 values(12434321313.213213,'2020-01-01');
insert into float01 values(null,'1997-01-13');
insert into float01 values(-12434321313.213213,'1000-10-10');
insert into float01 values(null,'2020-01-01');
insert into float01 values(null,null);
insert into float01 values(12434321313.213213,null);
insert into float01 values(0,'1997-01-13');
insert into float01 values(0,'1000-12-12');
insert into float01 values(12434321313.213213,null);
select * from float01;
select col2, avg(col1) over (partition by col1 order by col2) as tmp from float01;
select col2, sum(col1) over (partition by col2 order by col1) as tmp from float01;
select col2, max(col1) over (partition by col1 rows between 1 preceding and 1 following) from float01;
select col2, min(col1) over (partition by col2 order by col2) from float01;
drop table float01;

-- partition by and order by follows double
drop table if exists double01;
create table double01(d double);
insert into double01 values (2);
insert into double01 values (2);
insert into double01 values (3);
insert into double01 values (1);
insert into double01 values (1);
insert into double01 values (1.2);
insert into double01 values (null);
insert into double01 values (null);
select * from double01;
select d, sum(d) over (partition by d order by d), avg(d) over (order by d) from double01;
select d, sum(d) over (partition by d order by d), avg(d) over (order by d rows between 1 preceding and 1 following) from double01;
-- @bvt:issue#10043
select d, max(d) over (partition by d), avg(d) over () from double01;
select d, sum(d) over (partition by d order by d), avg(d) over () from double01;
-- @bvt:issue
truncate double01;
select * from double01;
insert into double01 values (1.7976931348623157e+307);
insert into double01 values (1);
select * from double01;
select d, sum(d) over (rows between current row and 1 following) from double01;
drop table double01;

-- partition by and order by follows decimal128
drop table if exists decimal01;
create table decimal01(d decimal(38,3));
insert into decimal01 values (28888888888888888888888888888888888.1234);
insert into decimal01 values (99999999999999999999999999999999999.83293323);
insert into decimal01 values (0);
insert into decimal01 values (-7841512312154312313158786541.342152121242143);
insert into decimal01 values (-7841512312154312313158786541.342152121242143);
insert into decimal01 values (99999999999999999999999999999999999.83293323);
insert into decimal01 values (null);
insert into decimal01 values (null);
select * from decimal01;

select max(d) over (partition by d order by d) from decimal01;
select min(d) over (partition by d order by d) from decimal01;
select avg(d) over (partition by d) from decimal01;
-- @bvt:issue#10043
select sum(d) over (partition by d order by d rows between 1 preceding and 1 following) from decimal01;
-- @bvt:issue
drop table decimal01;

-- partition by and order by follows date
drop table if exists date01;
create table date01(id date);
insert into date01 values ('2002-06-09');
insert into date01 values ('2002-06-09');
insert into date01 values ('2002-06-10');
insert into date01 values ('2002-06-09');
insert into date01 values ('2015-08-01');
insert into date01 values ('2002-06-09');
insert into date01 values ('2015-08-01');

select id, rank() over () from date01;
select id, dense_rank() over (order by id) from date01;
select id, max(id) over (order by id rows 2 preceding) from date01;
select min(id) over (partition by id order by id range interval 2 day preceding) from date01;
select id, count(id) over (order by id rows between 2 preceding and 1 following) from date01;
select id, count(id) over (order by date_add(id,interval 3 day) rows between 2 preceding and 1 following) from date01;

drop table date01;

-- check that sum stays that same when it sees null values
drop table if exists test01;
create table test01(i int, j int);
insert into test01 values (1,null);
insert into test01 values (1,null);
insert into test01 values (1,1);
insert into test01 values (1,null);
insert into test01 values (1,2);
insert into test01 values (2,1);
insert into test01 values (2,2);
insert into test01 values (2,null);
insert into test01 values (2,null);
select * from test01;
select i, j, sum(j) over (partition by i order by j rows unbounded preceding) from test01;
select i, j, avg(j) over (partition by i order by j rows unbounded preceding) from test01;
select i, j, max(j) over (partition by i order by j rows unbounded preceding) from test01;
-- @bvt:issue#10043
select i, j, min(j) over (partition by i order by j rows unbounded preceding) from test01;
-- @bvt:issue
drop table test01;

-- rows unbounded preceding,rows unbounded following,current row
drop table if exists row01;
create table row01(i int,j int);
insert into row01 values(1,1);
insert into row01 values(1,4);
insert into row01 values(1,2);
insert into row01 values(1,4);
select * from row01;

-- single partition
select i, j, sum(i+j) over (rows unbounded preceding) foo from row01;
select i, j, sum(i+j) over (rows between unbounded preceding and current row) foo from row01;
select i, j, sum(i+j) over (rows unbounded preceding) foo from row01 order by foo;
select i, j, sum(i+j) over (rows unbounded preceding) foo from row01 order by foo desc;

select i, j, sum(i+j) over (order by j desc rows unbounded preceding) foo from row01;
select i, j, sum(i+j) over (order by j desc rows between 2 preceding and 2 following) as foo from row01;
select i, j, sum(i+j) over (order by j desc rows unbounded preceding) foo from row01 order by foo;
select i, j, sum(i+j) over (order by j desc rows unbounded preceding) foo from row01 order by foo desc;

-- with limit
select i, j, sum(i+j) over (rows unbounded preceding) foo from row01 order by foo desc limit 3;
-- with order by
select i, j, sum(i+j) over (order by j rows unbounded preceding) foo from row01;
select i, j, sum(i+j) over (order by j rows unbounded preceding) foo from row01 order by foo;
select i, j, sum(i+j) over (order by j rows unbounded preceding) foo from row01 order by foo desc;

-- @bvt:issue#10043
select i, j, sum(i+j) over (order by j rows between 2 preceding and 1 preceding) foo from row01 order by foo desc;
select i, j, sum(i+j) over (order by j rows between 2 following and 1 following) foo from row01 order by foo desc;
-- @bvt:issue

-- abnormal test
select i, j, sum(i+j) over (order by j rows between -1 following and 1 following) foo from row01 order by foo desc;
select i, j, sum(i+j) over (order by j rows between 2 preceding and -10 following) foo from row01 order by foo desc;
drop table row01;

-- order by i rows between 2 preceding and 2 following
drop table if exists wf02;
create table wf02 (i int) ;
insert into wf02 (i) values (1);
insert into wf02 (i) values (2);
insert into wf02 (i) values (3);
insert into wf02 (i) values (4);
insert into wf02 (i) values (5);
select * from wf02;
select i, sum(i) over (rows between 0 preceding and 2 following) from wf02;
select i, sum(i) over (order by i rows between 2 preceding and 2 following) from wf02 limit 3;
select i, sum(i * 20) over (rows between 2 preceding and 2 following) from wf02 order by i desc limit 3;
select i, avg(i) over (rows between 2 preceding and 2 following) from wf02;
select i, avg(i + 100) over (rows between 2 preceding and 2 following) from wf02;
select i, sum(i) over (rows between 1 preceding and 2 following) from wf02;

drop table wf02;

-- order by and group by
drop table if exists og01;
create table og01(i int, j int, k int);
insert into og01 values (1,1,1);
insert into og01 values (1,4,1);
insert into og01 values (1,2,1);
insert into og01 values (1,4,1);
insert into og01 values (1,1,2);
insert into og01 values (1,4,2);
insert into og01 values (1,2,2);
insert into og01 values (1,4,2);
insert into og01 values (1,1,3);
insert into og01 values (1,4,3);
insert into og01 values (1,2,3);
insert into og01 values (1,4,3);
insert into og01 values (1,1,4);
insert into og01 values (1,4,4);
insert into og01 values (1,2,4);
insert into og01 values (1,4,4);
select * from og01;

select k, sum(k) over (rows unbounded preceding) wf from og01;
-- combined with group by
select k, min(i), sum(j), sum(k) over (rows unbounded preceding) wf from og01 group by (k);
select k, min(i), sum(j), sum(k) over (rows unbounded preceding) wf from og01 group by (k) order by wf desc;

select k, sum(k) over (rows unbounded preceding) foo from og01 group by (k);
select k, avg(distinct j), sum(k) over (rows unbounded preceding) foo from og01 group by (k);

-- expression argument to sum
select k, sum(k+1) over (rows unbounded preceding) foo from og01 group by (k);
select k, sum(k+1) over (order by k desc rows unbounded preceding) foo from og01 group by (k);
drop table og01;

drop table if exists og02;
create table og02 (id integer, sex char(1));
insert into og02 values (1, 'm');
insert into og02 values (2, 'f');
insert into og02 values (3, 'f');
insert into og02 values (4, 'f');
insert into og02 values (5, 'm');
insert into og02 values (10, null);
insert into og02 values (11, null);
select * from og02;

drop table if exists og03;
create table og03(c char(1));
insert into og03 values ('m');
select * from og03;

select sex, avg(id), row_number() over (partition by sex) from og02
group by sex order by sex desc;

select sex, avg(id), row_number() over (partition by sex) from og02
group by sex order by sex desc;

select sex, avg(id), sum(avg(id) + 10) over (rows unbounded preceding) from og02
group by sex order by sex desc;

select sex, avg(id), row_number() over (partition by sex) from og02
group by sex having sex='m' or sex is null order by sex desc;

select sex, avg(id), sum(avg(id)) over (rows unbounded preceding) from og02
group by sex having sex='m' or sex='f' or sex is null
order by sex desc;

-- having using subquery
select sex, avg(id), row_number() over (partition by sex) from og02
group by sex having sex=(select c from og03 limit 1) or sex is null
order by sex desc;

select sex, avg(id), sum(avg(id)) over (rows unbounded preceding) from og02
group by sex having sex=(select c from og03 limit 1) or sex='f' or sex is null
order by sex desc;

-- sum
select sex, avg(id), sum(avg(id)) over (order by sex rows unbounded preceding) from og02
group by sex
order by sex desc;

select sex, avg(id), sum(avg(id)) over (order by sex rows unbounded preceding) from og02
group by sex having sex=(select c from og03 limit 1) or sex='f' or sex is null
order by sex desc;

drop table og02;
drop table og03;

-- The date function in the window is nested with the date column in the table;
drop table if exists date02;
create table date02(col1 date,col2 datetime, col3 time, col4 timestamp);
insert into date02 values ('2002-06-09','1997-01-13 00:00:00','12:00:59','2023-05-16 00:12:12');
insert into date02 values ('2002-06-09','2020-02-20 00:00:00','11:12:12','2023-05-18 12:12:12');
insert into date02 values ('2002-06-10','1997-01-13 00:00:00','12:00:59','2023-05-16 00:12:12');
insert into date02 values ('2002-06-09','2020-02-20 00:00:00','11:12:12','2023-05-16 00:12:12');
insert into date02 values ('2015-08-01',null,null,'2023-05-18 12:12:12');
insert into date02 values ('2002-06-09',null,'01:01:01',null);
insert into date02 values ('2015-08-01','1990-01-01 01:02:03',null,null);
select * from date02;

-- nested with time function in windows:
select dense_rank() over (partition by col1 order by date_format(col1,'%m-%d-%Y')) from date02;
select max(col2) over (partition by col3 order by date(col2) desc) from date02;
select rank() over (order by col1 range interval 2 day preceding) from date02;
select max(col3) over (order by date_add(col2,interval 2 minute) rows  between 2 preceding and 1 following) from date02;
select min(col3) over (partition by col4 order by date_sub(col2,interval 2 minute) rows  between 2 preceding and 1 following) from date02;
select max(col3) over (order by year(col2) rows  between current row and unbounded following) from date02;
select dense_rank() over (order by month(col3)) from date02;
drop table date02;

-- rank,dense_rank
drop table if exists dense_rank01;
create table dense_rank01 (id integer, sex char(1));
insert into dense_rank01 values (1, 'm');
insert into dense_rank01 values (2, 'f');
insert into dense_rank01 values (3, 'f');
insert into dense_rank01 values (4, 'f');
insert into dense_rank01 values (5, 'm');
select * from dense_rank01;

drop table if exists dense_rank02;
create table dense_rank02 (user_id integer not null, date date);
insert into dense_rank02 values (1, '2002-06-09');
insert into dense_rank02 values (2, '2002-06-09');
insert into dense_rank02 values (1, '2002-06-09');
insert into dense_rank02 values (3, '2002-06-09');
insert into dense_rank02 values (4, '2002-06-09');
insert into dense_rank02 values (4, '2002-06-09');
insert into dense_rank02 values (5, '2002-06-09');
select * from dense_rank02;

-- rank, dense_rank
select rank() over (order by user_id) r from dense_rank02;
select dense_rank() over (order by user_id) r from dense_rank02;

-- same, without order by
select rank() over () r from dense_rank02;
select dense_rank() over () r from dense_rank02;

-- with order by
select id, sex, rank() over (order by sex rows unbounded preceding) from dense_rank01 order by id;
select id, sex, dense_rank() over (order by sex rows unbounded preceding) from dense_rank01 order by id;

select sex, rank() over (order by sex desc rows unbounded preceding) `rank`, avg(distinct id) as uids from dense_rank01 u, dense_rank02
where dense_rank02.user_id = u.id group by sex order by sex;
select sex, dense_rank() over (order by sex desc rows unbounded preceding) `rank`, avg(distinct id) as uids from dense_rank01 u, dense_rank02
where dense_rank02.user_id = u.id group by sex order by sex;

-- window desc ordering by group by
select  sex, avg(id) as uids, rank() over (order by avg(id)) `rank` from dense_rank01 u, dense_rank02
where dense_rank02.user_id = u.id group by sex;
select  sex, avg(id) as uids, dense_rank() over (order by avg(id)) `rank` from dense_rank01 u, dense_rank02
where dense_rank02.user_id = u.id group by sex;

-- window ordering by distinct group by
select  sex, avg(distinct id) as uids, rank() over (order by avg(distinct id) desc) `rank` from dense_rank01 u, dense_rank02
where dense_rank02.user_id = u.id group by sex
order by sex;
select  sex, avg(distinct id) as uids, dense_rank() over (order by avg(distinct id) desc) `p_rank` from dense_rank01 u, dense_rank02
where dense_rank02.user_id = u.id group by sex
order by sex;

-- window ordering by group by, final order by
select  sex, avg(id) as uids, rank() over (order by avg(id) desc) `rank` from dense_rank01 u, dense_rank02
where dense_rank02.user_id = u.id group by sex
order by `rank` desc;

-- sorted result
select  sex, avg(id) as uids, dense_rank() over (order by avg(id) desc) `p_rank`
from dense_rank01 u, dense_rank02
where dense_rank02.user_id = u.id group by sex
order by `p_rank` desc;

-- echo with nulls
insert into dense_rank01 values (10, null);
insert into dense_rank01 values (11, null);

select id, sex, rank() over (order by sex rows unbounded preceding)from dense_rank01 order by id;
select id, sex, dense_rank() over (order by sex rows unbounded preceding) from dense_rank01 order by id;
select id, sex, rank() over (order by sex desc rows unbounded preceding) from dense_rank01 order by id;

-- left join, right join, inner join, natural join, full join
select id value,
       sum(id) over (rows unbounded preceding)
from dense_rank01 left join dense_rank02 on dense_rank02.user_id = dense_rank01.id;

select id value,
       sum(id) over (rows unbounded preceding)
from dense_rank01 right join dense_rank02 on dense_rank02.user_id = dense_rank01.id;

select id value,
       sum(id) over (rows unbounded preceding)
from dense_rank01 inner join dense_rank02 on dense_rank02.user_id = dense_rank01.id;

select id value,
       sum(id) over (partition by id order by id rows unbounded preceding)
from dense_rank01 natural join dense_rank02;

select id value,
       sum(id) over (partition by id order by id rows unbounded preceding)
from dense_rank01 full join dense_rank02;


-- aggregate with group by in window's order by clause
select sex, avg(id), rank() over (order by avg(id) desc) from dense_rank01 group by sex order by sex;
select sex, dense_rank() over (order by avg(id) desc) from dense_rank01 group by sex order by sex;
select sex, rank() over (order by avg(id) desc) from dense_rank01 group by sex order by sex;

-- implicit group aggregate arguments to window function and in
-- window's order by clause
select rank() over (order by avg(id)) from dense_rank01;
select dense_rank() over (order by avg(id)) from dense_rank01;
select avg(id), rank() over (order by avg(id)) from dense_rank01;
select avg(id), dense_rank() over (order by avg(id)) from dense_rank01;
select avg(id), sum(avg(id)) over (order by avg(id) rows unbounded preceding) from dense_rank01;

-- echo several partitions, several window functions over the same window
-- @bvt:issue#10043
select sex, id, rank() over (partition by sex order by id desc) from dense_rank01;
select sex, id, dense_rank() over (partition by sex order by id desc) from dense_rank01;
select sex, id, rank() over (partition by sex order by id asc) from dense_rank01;
select sex, id, dense_rank() over (partition by sex order by id asc) from dense_rank01;
-- @bvt:issue
select sex, id, sum(id) over (partition by sex order by id asc rows unbounded preceding) summ,
        rank() over (partition by sex order by id asc rows unbounded preceding) `rank` from dense_rank01;
select sex, id, sum(id) over (partition by sex order by id asc rows unbounded preceding) summ,
        dense_rank() over (partition by sex order by id asc rows unbounded preceding) `d_rank` from dense_rank01;
select sex, id, sum(id) over (partition by sex order by id asc rows unbounded preceding) summ,
        rank() over (partition by sex order by id asc rows unbounded preceding) `rank` from dense_rank01
order by summ;
select sex, id, sum(id) over (partition by sex order by id asc rows unbounded preceding) summ,
        dense_rank() over (partition by sex order by id asc rows unbounded preceding) `p_rank` from dense_rank01
order by summ;

-- error test:window specification's order by or partition by cannot reference select list aliases
select sex, avg(distinct id),rank() over (order by uids desc) `uids`
from dense_rank01 u, dense_rank01 where dense_rank01.user_id = u.id group by sex
order by sex;
select sex, avg(distinct id),rank() over (order by uids desc) `uids`
from dense_rank01 u, dense_rank02 where dense_rank02.user_id = u.id
group by sex order by sex;

drop table dense_rank01;
drop table dense_rank02;

drop table if exists dense_rank03;
create table dense_rank03(d decimal(10,2), date date);
insert into dense_rank03 values (10.4, '2002-06-09');
insert into dense_rank03 values (20.5, '2002-06-09');
insert into dense_rank03 values (10.4, '2002-06-10');
insert into dense_rank03 values (3,    '2002-06-09');
insert into dense_rank03 values (40.2, '2015-08-01');
insert into dense_rank03 values (40.2, '2002-06-09');
insert into dense_rank03 values (5,    '2015-08-01');
select * from dense_rank03;

select * from (select rank() over (order by d) as `rank`, d, date from dense_rank03) alias order by `rank`, d, date;
select * from (select dense_rank() over (order by d) as `d_rank`, d, date from dense_rank03) alias order by `d_rank`, d, date;
select * from (select rank() over (order by date) as `rank`, date, d from dense_rank03) alias order by `rank`, d desc;
select * from (select dense_rank() over (order by date) as `p_rank`, date, d from dense_rank03) alias order by `p_rank`, d desc;
drop table dense_rank03;

-- order by + rank with more than one ordering expression
drop table if exists rank01;
create table rank01(i int, j int, k int);
insert into rank01 values (1,1,1);
insert into rank01 values (1,1,2);
insert into rank01 values (1,1,2);
insert into rank01 values (1,2,1);
insert into rank01 values (1,2,2);
insert into rank01 values (2,1,1);
insert into rank01 values (2,1,1);
insert into rank01 values (2,1,2);
insert into rank01 values (2,2,1);
insert into rank01 values (2,2,2);
select * from rank01;
select *, rank() over (order by i,j,k) as o_ijk,
        rank() over (order by j) as o_j,
        rank() over (order by k,j) as o_kj from rank01 order by i,j,k;
drop table rank01;

-- row_number tests
drop table if exists row_number01;
create table row_number01 (id integer, sex char(1));
insert into row_number01 values (1, 'm');
insert into row_number01 values (2, 'f');
insert into row_number01 values (3, 'f');
insert into row_number01 values (4, 'f');
insert into row_number01 values (5, 'm');
select * from row_number01;
drop table if exists row_number02;
create table row_number02 (user_id integer not null, date date);
insert into row_number02 values (1, '2002-06-09');
insert into row_number02 values (2, '2002-06-09');
insert into row_number02 values (1, '2002-06-09');
insert into row_number02 values (3, '2002-06-09');
insert into row_number02 values (4, '2002-06-09');
insert into row_number02 values (4, '2002-06-09');
insert into row_number02 values (5, '2002-06-09');
select * from row_number02;
select user_id, row_number() over (partition by user_id) from row_number02 row_number01;
select sex, id, date, row_number() over (partition by date order by id) as row_no, rank() over (partition by date order by id) as `rank` from row_number01,row_number02
where row_number01.id=row_number02.user_id;

-- window function in subquery
select  date,id, rank() over (partition by date order by id) as `rank` from row_number01,row_number02;
select * from (select date,id, rank() over (partition by date order by id) as `rank` from row_number01,row_number02) alias;
select * from (select date,id, dense_rank() over (partition by date order by id) as `p_rank` from row_number01,row_number02) t;

-- multiple windows
select row_number01.*, rank() over (order by sex rows unbounded preceding), sum(id) over (order by sex,id rows unbounded preceding) from row_number01;
select row_number01.*, dense_rank() over (order by sex rows unbounded preceding), sum(id) over (order by sex,id rows unbounded preceding) from row_number01;
select * from (select row_number01.*, sum(id) over (rows unbounded preceding), rank() over (order by sex rows unbounded preceding) from row_number01) alias order by id;
select * from (select row_number01.*, sum(id) over (rows unbounded preceding), dense_rank() over (order by sex rows unbounded preceding) from row_number01) alias order by id;

-- sorted results
select row_number01.*, sum(id) over (order by id rows unbounded preceding),
        rank() over (order by sex,id rows between 1 preceding and 2 following),
        row_number() over (order by sex,id rows unbounded preceding)
from row_number01;
select row_number01.*, sum(id) over (order by id rows unbounded preceding),
        dense_rank() over (order by sex,id rows between 1 preceding and 2 following)
from row_number01;

-- sum, avg, count with frames
select sum(id),avg(id) over (partition by sex), count(id) over (partition by sex) from row_number01;
select * from (select id, sum(id) over (partition by sex), count(*) over (partition by sex), sex from row_number01 alias order by id) alias;
select sum(id) over (partition by sex) from row_number01;
select id, sum(id) over (partition by sex order by id
       rows between 2 preceding and 1 following), sex from row_number01;

-- try the same as a view
create view v as select id, sum(id) over (partition by sex order by id rows between 2 preceding and 1 following), sex from row_number01;
show create view v;
select * from v;

drop view v;
drop table row_number01;
drop table row_number02;

-- avg for moving range frame
drop table if exists wf01;
create table wf01(d float);
insert into wf01 values (10);
insert into wf01 values (1);
insert into wf01 values (2);
insert into wf01 values (3);
insert into wf01 values (4);
insert into wf01 values (5);
insert into wf01 values (6);
insert into wf01 values (7);
insert into wf01 values (8);
insert into wf01 values (9);
select * from wf01;

select d, sum(d) over (order by d range between 2 preceding and current row),
        avg(d) over (order by d range between 2 preceding and current row) from wf01;
select d, sum(d) over (order by d range between 2 preceding and 2 following),
        avg(d) over (order by d range between 2 preceding and 2 following) from wf01;
select d, sum(d) over (order by d range between 2 preceding and current row),
        avg(d) over (order by d range between 2 preceding and current row) from wf01;
select d, sum(d) over (order by d range between 2 preceding and 2 following),
        avg(d) over (order by d range between 2 preceding and 2 following) from wf01;
-- @bvt:issue#10043
select d, sum(d) over (order by d range between current row and 2 following),
        avg(d) over (order by d range between current row and 2 following) from wf01;
select d, sum(d) over (order by d range between 2 preceding and 2 following),
        avg(d) over (order by d range between current row and 2 following) from wf01;
-- @bvt:issue

-- get more duplicates and hence peer sets
insert into wf01 select * from wf01;
select * from wf01;
select d, sum(d) over (order by d range between 2 preceding and current row),
        avg(d) over (order by d range between 1 preceding and current row) from wf01;
select d, sum(d) over (order by d range between 2 preceding and 2 following),
        avg(d) over (order by d range between 3 preceding and 2 following) from wf01;
select d, sum(d) over (order by d range between 2 preceding and current row),
        avg(d) over (order by d range between 2 preceding and current row) from wf01;
select d, sum(d) over (order by d range between 1 preceding and 2 following),
        avg(d) over (order by d range between 2 preceding and 2 following) from wf01;
-- @bvt:issue#10043
select d, sum(d) over (order by d range between current row and 2 following),
        avg(d) over (order by d range between current row and 2 following) from wf01;
select d, sum(d) over (order by d range between current row and 2 following),
        avg(d) over (order by d range between current row and 2 following) from wf01;
-- @bvt:issue
drop table wf01;

-- sum with frames in combination with non-framing window functions
-- row_number and rank
drop table if exists wf02;
create table wf02 (id integer, sex varchar(10));
insert into wf02 values (1, 'moolol');
insert into wf02 values (2, 'fdhsajhd');
insert into wf02 values (3, 'fdhsajhd');
insert into wf02 values (4, 'fdhsajhd');
insert into wf02 values (5, 'moolol');
insert into wf02 values (10, null);
insert into wf02 values (11, null);
select * from wf02;

-- @bvt:issue#10043
select row_number() over (partition by sex order by id rows between unbounded preceding and unbounded following), id,
       sum(id) over (partition by sex order by id rows between 1 following and 2 following), sex from wf02;
select row_number() over (partition by sex order by id rows between 1 following and 2 following), sum(id) over (partition by sex order by id
    rows between 1 following and 2 following) from wf02;
-- @bvt:issue

insert into wf02 values (10, null);
select rank() over (partition by sex order by id), id, sum(id) over (partition by sex order by id), sex from wf02;

-- @bvt:issue#10025
select id, sex, sum(id) over (partition by sex order by id rows between 2 preceding and 1 following) as a from wf02;
select id, sex, sum(id) over (partition by sex order by id rows between 2 preceding and 1 following) as a,
        row_number() over (partition by sex order by id rows between 2 preceding and 1 following) as b,
        rank() over (partition by sex order by id rows between 2 preceding and 1 following) as c  from wf02;
select id, sex, sum(id) over (partition by sex order by id rows between 2 preceding and 1 following) as a,
        row_number() over (partition by sex order by id rows between 2 preceding and 1 following) as b from wf02;
select row_number() over (partition by sex order by id rows between unbounded preceding and unbounded following), id,
       sex from wf02;
select row_number() over (partition by sex order by id rows between 1 preceding and 2 following), sum(id) over (partition by sex order by id
    rows between 1 preceding and 2 following) from wf02;
-- @bvt:issue

drop table wf02;
drop database test;
