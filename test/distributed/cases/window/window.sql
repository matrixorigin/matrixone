drop table if exists t1;
create table t1 (a int, b datetime);
insert into t1 values(1, '2020-11-11'), (2, '2020-11-12'), (3, '2020-11-13');
select sum(a) over(partition by a order by b range between interval 1 day preceding and interval 2 day following) from t1;
drop table t1;

drop table if exists t1;
create table t1 (a int, b date);
insert into t1 values(1, '2020-11-11'), (2, '2020-11-12'), (3, '2020-11-13'), (1, '2020-11-11'), (2, '2020-11-12'), (3, '2020-11-13'), (1, '2020-11-11'), (2, '2020-11-12'), (3, '2020-11-13');
select max(a) over(order by b range between interval 1 day preceding and interval 2 day following) from t1;
drop table t1;

drop table if exists t1;
create table t1 (a int, b time);
insert into t1 values(1, 112233), (2, 122233), (3, 132233), (1, 112233), (2, 122233), (3, 132233), (1, 112233), (2, 122233), (3, 132233);
select min(a) over(order by b range between interval 1 hour preceding and current row) from t1;
drop table t1;

drop table if exists t1;
create table t1 (a int, b timestamp);
insert into t1 values(1, '2020-11-11'), (2, '2020-11-12'), (3, '2020-11-13'), (1, '2020-11-11'), (2, '2020-11-12'), (3, '2020-11-13'), (1, '2020-11-11'), (2, '2020-11-12'), (3, '2020-11-13');
select count(*) over(order by b range current row) from t1;
drop table t1;

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
drop table t1;

-- test cume_dist
drop table if exists t1;
create table t1 (a int, b int);
insert into t1 values(1, 1), (1, 2), (2, 1), (2, 2), (2, 3);
select a, b, cume_dist() over (order by a) from t1;
select a, b, cume_dist() over (order by b) from t1;
select a, b, cume_dist() over (partition by a order by b) from t1;
select a, cume_dist() over () from t1;
drop table t1;

-- test cume_dist: edge cases
drop table if exists t2;
create table t2 (a int, b int);
select a, b, cume_dist() over (order by a) from t2;
insert into t2 values(1, 1);
select a, b, cume_dist() over (order by a) from t2;
insert into t2 values(1, 1), (1, 1), (1, 1);
select a, b, cume_dist() over (order by a) from t2;
drop table t2;

-- test cume_dist: NULL handling
drop table if exists t3;
create table t3 (a int, b int);
insert into t3 values(1, 1), (2, NULL), (3, 3), (NULL, 4), (5, NULL);
select a, b, cume_dist() over (order by a) from t3;
select a, b, cume_dist() over (order by b) from t3;
select a, b, cume_dist() over (partition by a order by b) from t3;
drop table t3;

-- test cume_dist: string type
drop table if exists t4;
create table t4 (name varchar(20), score int);
insert into t4 values('Alice', 90), ('Bob', 85), ('Charlie', 90), ('David', 75), ('Eve', 85);
select name, score, cume_dist() over (order by name) from t4;
select name, score, cume_dist() over (order by score) from t4;
select name, score, cume_dist() over (partition by score order by name) from t4;
drop table t4;

-- test cume_dist: date type
drop table if exists t5;
create table t5 (id int, dt date);
insert into t5 values(1, '2024-01-01'), (2, '2024-01-01'), (3, '2024-02-01'), (4, '2024-03-01'), (5, '2024-02-01');
select id, dt, cume_dist() over (order by dt) from t5;
select id, dt, cume_dist() over (partition by dt order by id) from t5;
drop table t5;

-- test cume_dist: DESC order
drop table if exists t6;
create table t6 (a int, b int);
insert into t6 values(1, 10), (2, 20), (3, 30), (4, 20), (5, 10);
select a, b, cume_dist() over (order by b desc) from t6;
select a, b, cume_dist() over (partition by b order by a desc) from t6;
drop table t6;

-- test cume_dist: multiple columns order by
drop table if exists t7;
create table t7 (a int, b int, c int);
insert into t7 values(1, 1, 1), (1, 2, 1), (1, 1, 2), (2, 1, 1), (2, 2, 2);
select a, b, c, cume_dist() over (order by a, b) from t7;
select a, b, c, cume_dist() over (order by a desc, b asc) from t7;
select a, b, c, cume_dist() over (partition by a order by b, c) from t7;
drop table t7;

-- test cume_dist: with other window functions
drop table if exists t8;
create table t8 (a int, b int);
insert into t8 values(1, 10), (1, 20), (2, 10), (2, 20), (2, 30);
select a, b, 
    row_number() over (order by b) as rn,
    rank() over (order by b) as rnk,
    dense_rank() over (order by b) as drnk,
    cume_dist() over (order by b) as cdist
from t8;
select a, b,
    rank() over (partition by a order by b) as rnk,
    cume_dist() over (partition by a order by b) as cdist
from t8;
drop table t8;

-- test cume_dist: larger dataset
drop table if exists t9;
create table t9 (id int, category int, value int);
insert into t9 values
(1, 1, 100), (2, 1, 200), (3, 1, 100), (4, 1, 300),
(5, 2, 150), (6, 2, 150), (7, 2, 250), (8, 2, 150),
(9, 3, 100), (10, 3, 200), (11, 3, 300), (12, 3, 400);
select id, category, value, cume_dist() over (order by value) from t9;
select id, category, value, cume_dist() over (partition by category order by value) from t9;
drop table t9;

-- test cume_dist: float type
drop table if exists t10;
create table t10 (a float, b double);
insert into t10 values(1.1, 2.2), (1.1, 3.3), (2.2, 2.2), (3.3, 1.1);
select a, b, cume_dist() over (order by a) from t10;
select a, b, cume_dist() over (order by b) from t10;
select a, b, cume_dist() over (partition by a order by b) from t10;
drop table t10;

drop table if exists t1;
create table t1 (a int, b decimal(7, 2));
insert into t1 values(1, 12.12), (2, 123.13), (3, 456.66), (4, 1111.34);
select a, sum(b) over (partition by a order by a) from t1;
drop table t1;

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
drop table wf01;

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
drop table wf08;

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
drop table wf07;

drop table if exists wf12;
create table wf12(d double);
insert into wf12 values (1.7976931348623157e+307);
insert into wf12 values (1);
select d, sum(d) over (rows between current row and 1 following) from wf12;
drop table wf12;

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
drop table wf06;
drop table wf07;

drop table if exists row01;
create table row01(i int,j int);
insert into row01 values(1,1);
insert into row01 values(1,4);
insert into row01 values(1,2);
insert into row01 values(1,4);
select i, j, sum(i+j) over (order by j rows between 2 preceding and 1 preceding) foo from row01 order by foo desc;
select i, j, sum(i+j) over (order by j rows between 2 following and 1 following) foo from row01 order by foo desc;
drop table row01;

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
select i, j, min(j) over (partition by i order by j rows unbounded preceding) from test01;
drop table test01;

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
select d, sum(d) over (partition by d order by d), avg(d) over (order by d rows between 1 preceding and 1 following) from double01;
select d, sum(d) over (partition by d order by d), avg(d) over (order by d rows between 2 preceding and 1 following) from double01;
drop table double01;

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
select d, sum(d) over (order by d range between current row and 2 following), avg(d) over (order by d range between current row and 2 following) from wf01;
select d, sum(d) over (order by d range between 2 preceding and 2 following), avg(d) over (order by d range between current row and 2 following) from wf01;
drop table wf01;

drop table if exists dense_rank01;
create table dense_rank01 (id integer, sex char(1));
insert into dense_rank01 values (1, 'm');
insert into dense_rank01 values (2, 'f');
insert into dense_rank01 values (3, 'f');
insert into dense_rank01 values (4, 'f');
insert into dense_rank01 values (5, 'm');
select sex, id, rank() over (partition by sex order by id desc) from dense_rank01;
select sex, id, dense_rank() over (partition by sex order by id desc) from dense_rank01;
drop table dense_rank01;

drop table if exists sales;
create table sales (customer_id varchar(1), order_date date, product_id integer);
insert into sales(customer_id, order_date, product_id) values ('a', '2021-01-01', '1'), ('a', '2021-01-01', '2'), ('a', '2021-01-07', '2'), ('a', '2021-01-10', '3'), ('a', '2021-01-11', '3'), ('a', '2021-01-11', '3'),('b', '2021-01-01', '2'),('b', '2021-01-02', '2'),('b', '2021-01-04', '1'),('b', '2021-01-11', '1'),('b', '2021-01-16', '3'),('b', '2021-02-01', '3'),('c', '2021-01-01', '3'),('c', '2021-01-01', '3'),('c', '2021-01-07', '3');
drop table if exists menu;
create table menu (product_id integer,product_name varchar(5),price integer);
insert into menu(product_id, product_name, price) values ('1', 'sushi', '10'),('2', 'curry', '15'),('3', 'ramen', '12');
with ordered_sales as (select sales.customer_id, sales.order_date, menu.product_name,dense_rank() over (partition by sales.customer_id order by sales.order_date) as `rank` from sales inner join menu on sales.product_id = menu.product_id) select customer_id, product_name from ordered_sales where `rank` = 1 group by customer_id, product_name;
drop table sales;

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
select rank() over (order by t0.a) as b from (select i as a from test01) as t0;
select rank() over(order by j) as col, j from test01;
drop table test01;

drop table if exists wf14;
create table wf14 (id integer, sex char(1));
insert into wf14 values (1, 'm');
insert into wf14 values (2, 'f');
insert into wf14 values (3, 'f');
insert into wf14 values (4, 'f');
insert into wf14 values (5, 'm');
insert into wf14 values (10, null);
insert into wf14 values (11, null);
insert into wf14 values (10, null);
select id, sex, sum(id) over (partition by sex order by id rows between 2 preceding and 1 following) as a from wf14;
select id, sex, sum(id) over (partition by sex order by id rows between 2 preceding and 1 following) as a from wf14;
select id, sex, sum(id) over (partition by sex order by id rows between 2 preceding and 1 following) as a from wf14;
drop table wf14;

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
select d, max(d) over (partition by d) from double01;
select d, sum(d) over (partition by d order by d) from double01;
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
select sum(d) over (partition by d order by d rows between 1 preceding and 1 following) from decimal01;
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
select i, j, min(j) over (partition by i order by j rows unbounded preceding) from test01;
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

select i, j, sum(i+j) over (order by j rows between 2 preceding and 1 preceding) foo from row01 order by foo desc;
select i, j, sum(i+j) over (order by j rows between 2 following and 1 following) foo from row01 order by foo desc;

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
select sex, id, rank() over (partition by sex order by id desc) from dense_rank01;
select sex, id, dense_rank() over (partition by sex order by id desc) from dense_rank01;
select sex, id, rank() over (partition by sex order by id asc) from dense_rank01;
select sex, id, dense_rank() over (partition by sex order by id asc) from dense_rank01;
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
select d, sum(d) over (order by d range between 1 preceding and 2 following),
        avg(d) over (order by d range between 2 preceding and 3 following) from wf01;
select d, sum(d) over (order by d range between 2 preceding and current row),
        avg(d) over (order by d range between 1 preceding and current row) from wf01;
select d, sum(d) over (order by d range between 2 preceding and 2 following),
        avg(d) over (order by d range between 1 preceding and 2 following) from wf01;
select d, sum(d) over (order by d range between current row and 0 following),
        avg(d) over (order by d range between current row and 2 following) from wf01;
select d, sum(d) over (order by d range between 2 preceding and 2 following),
        avg(d) over (order by d range between current row and 2 following) from wf01;

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
select d, sum(d) over (order by d range between current row and 2 following),
        avg(d) over (order by d range between current row and 2 following) from wf01;
select d, sum(d) over (order by d range between current row and 2 following),
        avg(d) over (order by d range between current row and 2 following) from wf01;
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

select row_number() over (partition by sex order by id rows between unbounded preceding and unbounded following), id,
       sum(id) over (partition by sex order by id rows between 1 following and 2 following), sex from wf02;
select row_number() over (partition by sex order by id rows between 1 following and 2 following), sum(id) over (partition by sex order by id
    rows between 1 following and 2 following) from wf02;

insert into wf02 values (10, null);
select rank() over (partition by sex order by id), id, sum(id) over (partition by sex order by id) as abc, sex from wf02;

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

drop table wf02;

-- windows function with cte and referenced in the external select column
drop table if exists cte01;
drop table if exists cte02;

create table cte01 (
                       customer_id varchar(1),
                       order_date date,
                       product_id integer
);

insert into cte01 values('a', '2021-01-01', '1');
insert into cte01 values('a', '2021-01-01', '2');
insert into cte01 values('a', '2021-01-07', '2');
insert into cte01 values('a', '2021-01-10', '3');
insert into cte01 values('a', '2021-01-11', '3');
insert into cte01 values('a', '2021-01-11', '3');
insert into cte01 values('b', '2021-01-01', '2');
insert into cte01 values('b', '2021-01-02', '2');
insert into cte01 values('b', '2021-01-04', '1');
insert into cte01 values('b', '2021-01-11', '1');
insert into cte01 values('b', '2021-01-16', '3');
insert into cte01 values('b', '2021-02-01', '3');
insert into cte01 values('c', '2021-01-01', '3');
insert into cte01 values('c', '2021-01-01', '3');
insert into cte01 values('c', '2021-01-07', '3');
select * from cte01;

create table cte02 (
                       product_id integer,
                       product_name varchar(5),
                       price integer
);

insert into cte02 values('1', 'sushi', '10');
insert into cte02 values('2', 'curry', '15');
insert into cte02 values('3', 'ramen', '12');
select * from cte02;

with test as (
    select cte01.customer_id, cte01.order_date, cte02.product_name, dense_rank() over (partition by cte01.customer_id
      order by cte01.order_date) as `rank` from cte01 inner join cte02 on cte01.product_id = cte02.product_id)
select customer_id, product_name from test where `rank` = 1 group by customer_id, product_name;


with test as (
    select cte01.customer_id, cte01.order_date, cte02.product_name, rank() over (partition by cte01.customer_id
      order by cte01.order_date) as `rank` from cte01 inner join cte02 on cte01.product_id = cte02.product_id)
select customer_id, product_name from test where `rank` = 1 group by customer_id, product_name;

with test as (
    select cte01.customer_id, cte01.order_date, cte02.product_name, row_number() over (partition by cte01.customer_id
      order by cte01.order_date) as `rank` from cte01 inner join cte02 on cte01.product_id = cte02.product_id)
select customer_id, product_name from test where `rank` = 1 group by customer_id, product_name;

drop table cte01;
drop table cte02;

-- Large data volume
drop table if exists td;
create table td(d int);

insert into td(d) values (10),(1),(2),(3),(4),(5),(6),(7),(8),(9);
insert into td(d) select d+10 from td;
insert into td(d) select d+20 from td;
insert into td(d) select d+30 from td;
insert into td(d) select d+40 from td;
insert into td(d) select d+50 from td;
insert into td(d) select d+60 from td;
insert into td(d) select d+70 from td;
insert into td(d) select d+80 from td;
insert into td(d) select d+90 from td;

insert into td(d) select d+100 from td;
insert into td(d) select d+200 from td;
insert into td(d) select d+300 from td;
insert into td(d) select d+400 from td;
insert into td(d) select d+500 from td;
insert into td(d) select d+600 from td;
insert into td(d) select d+700 from td;
insert into td(d) select d+800 from td;
insert into td(d) select d+900 from td;
insert into td(d) select d+1000 from td;
insert into td(d) select d+2000 from td;
insert into td(d) select d+3000 from td;
select count(*) from td;

-- @bvt:issue#13008
select avg(d) over (order by d range between 2 preceding and 2 following) from td limit 10;
-- @bvt:issue
-- @bvt:issue#23427
select sum(d) over (order by d rows between 10 preceding and 10 following) from td limit 10;
-- @bvt:issue
-- @bvt:issue#23427
select d,min(d) over (partition by d%7 order by d rows  between 2 preceding and 1 following) from td limit 10;
-- @bvt:issue
drop table td;

drop table if exists `c`;
create table `c` (
                     `pk` int(11) not null auto_increment,
                     `col_int` int(11) not null,
                     `col_date` date not null,
                     `col_datetime` datetime not null,
                     `col_time` time not null,
                     `col_varchar` varchar(15) not null,
                     primary key (`pk`),
                     unique key `col_date_key` (`col_date`),
                     unique key `col_date_key_2` (`col_date`,`col_datetime`),
                     key `col_int_key_1` (`col_int`,`col_date`),
                     key `col_int_key_2` (`col_int`,`col_time`),
                     key `col_int_key_3` (`col_int`,`col_datetime`)
);

insert into `c` (`pk`, `col_int`, `col_date`, `col_datetime`, `col_time`, `col_varchar`)
values (1, 9, '2009-11-04', '2006-10-12 19:52:02', '18:19:40', 'a'),
       (2, 4, '2009-05-21', '2005-09-13 00:00:00', '07:45:25', 'tef'),
       (3, 0, '1900-01-01', '2002-09-03 04:42:41', '13:17:14', 'efqsd'),
       (4, 149, '2000-11-05', '2007-02-08 07:29:31', '10:38:21', 'fqsdk'),
       (5, 8, '2001-06-12', '2000-11-07 15:28:31', '23:04:47', 'qsdksji'),
       (6, 8, '2002-06-07', '2007-09-19 02:35:12', '07:33:31', 'sdks'),
       (7, 5, '2008-06-02', '1900-01-01 00:00:00', '14:41:02', 'dksjij'),
       (8, 7, '2000-07-26', '2007-11-27 00:19:33', '23:30:25', 'sjijcsz'),
       (9, 8, '2008-09-16', '2004-12-17 11:22:46', '06:11:14', 'i'),
       (10, 104, '2002-03-06', '2007-02-04 13:09:16', '22:24:50', 'jcszxw'),
       (11, 1, '2004-01-10', '2008-03-19 08:36:41', '00:03:00', 'csz'),
       (12, 4, '2002-02-21', '2008-03-27 03:09:30', '06:52:39', 'szxwbjj'),
       (13, 8, '2004-07-01', '2001-10-20 06:42:39', '08:49:41', 'xwb'),
       (14, 7, '2008-08-13', '2002-04-05 00:00:00', '05:52:03', 'wbjjvvk'),
       (15, 8, '2008-12-18', '1900-01-01 00:00:00', '00:00:00', 'bj'),
       (16, 6, '2002-08-03', '2008-04-14 09:20:36', '00:00:00', 'jjvvk'),
       (17, 97, '2001-06-11', '2002-11-07 00:00:00', '13:30:55', 'j');

drop table if exists `dd`;
create table `dd` (
                      `pk` int(11) not null auto_increment,
                      `col_int` int(11) not null,
                      `col_date` date not null,
                      `col_datetime` datetime not null,
                      `col_time` time not null,
                      `col_varchar` varchar(15) not null,
                      primary key (`pk`),
                      unique key `col_date_key` (`col_date`),
                      unique key `col_date_key_1` (`col_date`,`col_time`,`col_datetime`),
                      key `col_int_key` (`col_int`),
                      key `col_time_key` (`col_time`),
                      key `col_datetime_key` (`col_datetime`),
                      key `col_int_key_5` (`col_int`),
                      key `col_int_key_6` (`col_int`),
                      key `col_int_key_7` (`col_int`,`col_date`),
                      key `col_int_key_8` (`col_int`,`col_time`),
                      key `col_int_key_9` (`col_int`,`col_datetime`));

insert into `dd` (`pk`, `col_int`, `col_date`, `col_datetime`, `col_time`, `col_varchar`)
values (10,7,'1992-01-01','2000-02-09 06:46:23','03:56:10','i'),
       (11,5,'2008-12-11','2004-03-07 18:05:11','00:00:00','jrll'),
       (12,7,'2005-11-18','2001-01-18 08:29:29','20:17:57','rllqunt'),
       (13,9,'2009-02-08','2005-10-25 00:00:00','08:09:49','l'),
       (14,3,'2002-05-26','2009-09-01 10:19:05','09:40:42','lq'),
       (15,66,'2002-03-10','2002-09-06 04:43:02','08:28:55','quntp'),
       (16,3,'2003-07-07','2006-04-07 00:00:00','20:12:00','untppi'),
       (17,95,'2006-06-22','2004-05-08 00:00:00','18:50:24','ntppirz'),
       (18,7,'2004-01-21','2000-01-23 03:34:04','17:01:57','tppirzd'),
       (19,5,'2001-05-01','2005-12-26 20:42:01','15:11:27','pirzdp'),
       (20,8,'2008-12-15','1900-01-01 00:00:00','05:49:51','irzd'),
       (21,3,'2000-08-28','2003-02-28 16:30:52','14:58:44','zdphpdu'),
       (22,96,'2008-06-08','2005-09-15 03:55:22','02:20:01','dp'),
       (23,9,'2002-04-02','2001-01-08 10:44:10','19:03:57','p'),
       (24,3,'2005-03-04','2001-03-23 00:00:00','00:27:13','h'),
       (25,8,'2001-01-21','2004-03-02 00:00:00','13:39:32','pduhwq'),
       (26,8,'2006-10-05','1900-01-01 00:00:00','08:06:08','uhwqh'),
       (27,4,'2001-12-26','2006-10-24 05:59:20','16:15:34','hwqh'),
       (28,7,'1900-01-01','2005-06-14 00:00:00','12:04:50','wqhnsm'),
       (29,6,'2007-12-02','2001-08-25 03:00:31','00:00:00','qh'),
       (30,4,'2009-02-06','2001-06-14 19:13:14','06:00:42','nsmu'),
       (31,9,'2007-01-15','2006-12-18 07:54:16','11:18:35','smujjj'),
       (32,5,'2004-11-07','2000-09-18 04:53:37','16:20:06','muj'),
       (33,1,'2003-12-07','2002-08-18 04:47:11','01:41:35','jj'),
       (34,1,'2008-09-07','2000-10-14 16:58:18','17:42:13','jbld'),
       (35,5,'2005-03-08','2008-11-22 16:40:01','00:59:59','bldnki'),
       (36,181,'2006-11-18','1900-01-01 00:00:00','00:00:00','nkiws'),
       (37,5,'2007-01-26','2008-01-21 00:00:00','02:16:04','kiwsr'),
       (38,1,'2003-08-24','1900-01-01 00:00:00','00:00:00','iwsrsx'),
       (39,162,'2001-12-01','2008-05-17 00:00:00','14:34:36','srsxnd'),
       (40,8,'2003-07-02','2000-06-07 00:00:00','23:02:05','r'),
       (41,2,'2007-03-01','2009-01-03 12:22:04','00:00:00','sxndo'),
       (42,7,'2009-08-04','2009-10-05 04:15:15','00:00:00','xndolp'),
       (43,119,'2000-05-03','2002-02-17 23:12:12','23:23:35','olpujd'),
       (44,3,'2001-05-18','2008-03-27 11:51:54','11:26:20','lp'),
       (45,119,'2004-02-22','1900-01-01 00:00:00','00:00:00','pu'),
       (46,8,'2002-07-15','2008-08-24 21:36:28','12:51:37','dnozrhh'),
       (47,2,'2008-04-22','2005-01-12 08:50:22','20:55:45','no'),
       (48,4,'2006-06-01','2000-04-20 00:00:00','13:02:05','ozrhhcx'),
       (49,8,'2009-09-12','2000-02-16 03:57:05','17:04:35','zrhhcxs'),
       (50,6,'2009-01-06','1900-01-01 00:00:00','05:15:45','rhhcxsx'),
       (51,6,'2008-07-13','2002-04-27 14:13:27','00:00:00','hhcxsxw'),
       (52,8,'2002-03-15','2008-01-17 20:30:57','07:09:22','hcxsxw'),
       (53,6,'2007-10-14','2006-10-11 22:48:02','06:11:59','cxs'),
       (54,1,'2008-07-23','2005-09-11 07:19:40','03:05:06','x'),
       (55,1,'2007-05-22','2002-11-24 16:25:27','10:10:42','s'),
       (56,6,'2008-01-08','2005-06-09 01:11:17','06:03:27','w'),
       (57,9,'2006-10-18','1900-01-01 00:00:00','00:00:00','uju'),
       (58,7,'2000-07-22','1900-01-01 00:00:00','00:00:00','ju'),
       (59,6,'2004-07-21','2009-10-25 16:05:29','11:04:39','ul'),
       (60,2,'2001-10-03','2002-06-13 11:41:55','10:20:49','lpjd'),
       (61,8,'2002-08-17','1900-01-01 00:00:00','00:00:00','jdz'),
       (62,0,'2009-11-10','2000-05-04 05:15:19','00:00:00','zvkpaij'),
       (63,6,'2005-06-26','2002-08-19 00:00:00','09:21:09','vkpaij'),
       (64,6,'2000-06-04','2002-03-22 04:37:00','00:00:00','kp'),
       (65,9,'2005-10-02','2009-01-10 09:03:59','04:56:37','paiju'),
       (66,0,'2009-11-13','1900-01-01 00:00:00','00:00:00','aij'),
       (67,0,'2006-11-26','2001-09-21 00:00:00','08:16:28','ijurspr'),
       (68,6,'2007-09-24','2003-08-27 05:11:09','19:55:11','j'),
       (69,0,'2009-01-24','1900-01-01 00:00:00','11:25:58','urspr'),
       (70,5,'2001-06-22','2005-07-07 00:00:00','14:38:03','rsprn'),
       (71,4,'2006-07-18','2000-07-16 06:17:20','15:32:00','sprnw'),
       (72,5,'2009-05-12','2007-07-26 00:00:00','09:25:59','rnwgrp');

drop table if exists `e`;
create table `e` (
                     `pk` int(11) not null auto_increment,
                     `col_int` int(11) not null,
                     `col_date` date not null,
                     `col_datetime` datetime not null,
                     `col_time` time not null,
                     `col_varchar` varchar(15) not null,
                     primary key (`pk`),
                     unique key `col_date` (`col_date`,`col_time`,`col_datetime`),
                     unique key `col_varchar_key_2` (`col_varchar`(5)),
                     unique key `col_int_key_1` (`col_int`,`col_varchar`(5)),
                     unique key `col_int_key_2` (`col_int`,`col_varchar`(5),`col_date`,`col_time`,`col_datetime`),
                     key `col_int_key` (`col_int`),
                     key `col_time_key` (`col_time`),
                     key `col_datetime_key` (`col_datetime`),
                     key `col_int_key_7` (`col_int`,`col_date`),
                     key `col_int_key_8` (`col_int`,`col_time`),
                     key `col_int_key_9` (`col_int`,`col_datetime`));

insert into `e` (`pk`, `col_int`, `col_date`, `col_datetime`, `col_time`, `col_varchar`)
values (1, 202, '1997-01-13', '2008-11-25 09:14:26', '07:23:12', 'en'),
       (2, 4, '2005-07-10', '2005-03-15 22:48:25', '23:28:02', 'nchyhu'),
       (3, 7, '2005-06-09', '2006-11-22 00:00:00', '10:51:23', 'chy'),
       (4, 2, '2007-12-08', '2007-11-01 09:02:50', '01:12:13', 'hyhu'),
       (5, 7, '2007-12-22', '2001-04-08 00:00:00', '06:34:46', 'yhuoo'),
       (6, 1, '1900-01-01', '2001-11-27 19:47:15', '10:16:53', 'huoo'),
       (7, 7, '2002-10-07', '2009-09-15 04:42:26', '07:07:58', 'uoowit'),
       (8, 7, '2005-01-09', '2001-08-12 02:07:43', '06:15:07', 'oo'),
       (9, 3, '2007-10-12', '2009-05-09 17:06:27', '00:00:00', 'ow'),
       (10, 3, '2004-01-22', '1900-01-01 00:00:00', '06:41:21', 'wityzg'),
       (11, 5, '2007-10-11', '2000-03-03 23:40:04', '22:28:00', 'ityzg'),
       (12, 8, '2001-08-19', '2005-10-18 17:41:54', '04:47:49', 'tyz'),
       (13, 9, '2001-02-12', '2000-03-23 23:22:54', '03:24:01', 'gktbkjr'),
       (14, 0, '2000-07-14', '2007-01-25 11:00:51', '14:37:06', 'ktbkjrk'),
       (15, 4, '2007-11-14', '2003-12-21 10:46:23', '05:53:49', 'tbkjrkm'),
       (16, 9, '2004-01-25', '2003-09-02 01:45:27', '00:00:00', 'k'),
       (17, 2, '2003-12-15', '2009-05-28 08:03:38', '23:41:09', 'j'),
       (18, 4, '2002-01-25', '2003-10-23 18:22:15', '09:26:45', 'kmqm'),
       (19, 0, '2009-09-08', '2001-12-28 00:00:00', '17:04:03', 'mq'),
       (20, 7, '2008-03-15', '2005-05-06 19:42:18', '02:15:17', 'mkn'),
       (21, 0, '2005-11-10', '2003-03-05 00:00:00', '00:00:00', 'knbtoe'),
       (22, 1, '2008-11-12', '2001-12-26 16:47:05', '19:09:36', 'n'),
       (23, 2, '2007-11-22', '2003-02-09 00:00:00', '07:55:11', 'btoer'),
       (24, 4, '2002-04-25', '2008-10-13 00:00:00', '11:24:50', 'toe'),
       (25, 4, '2004-02-14', '2001-07-16 16:05:48', '08:46:01', 'oervq'),
       (26, 4, '2004-04-21', '2004-04-23 14:00:22', '20:16:19', 'rvqlzs'),
       (27, 3, '2003-03-26', '2002-11-10 08:15:17', '13:03:14', 'vqlzs'),
       (28, 0, '2007-06-18', '2006-06-24 03:59:58', '06:11:33', 'qlzsva'),
       (29, 5, '2006-12-09', '2008-04-08 18:06:18', '09:40:31', 'lzsvasu'),
       (30, 8, '2001-10-01', '2000-10-12 16:32:35', '03:34:01', 'zsvasu'),
       (31, 6, '2001-01-07', '2005-09-11 10:09:54', '00:00:00', 'svas'),
       (32, 0, '2007-11-02', '2009-09-10 01:44:18', '12:23:27', 'v'),
       (33, 9, '2005-07-23', '2002-10-20 21:55:02', '05:12:10', 'surqdhu'),
       (34, 4, '2003-09-13', '2009-11-03 09:54:42', '20:54:06', 'urqdh'),
       (35, 165, '2001-05-14', '2002-10-19 00:00:00', '00:00:00', 'rqd'),
       (36, 2, '2006-07-04', '2008-10-26 00:00:00', '00:59:06', 'qdhu'),
       (37, 6, '2001-08-15', '2002-08-14 14:52:08', '07:22:34', 'dhu'),
       (38, 5, '2000-04-27', '2007-06-10 00:00:00', '11:27:19', 'hu4332cjx'),
       (39, 9, '2007-10-13', '2002-07-07 04:10:43', '10:03:09', 'uc'),
       (40, 214, '2004-02-06', '2007-08-15 13:56:29', '23:00:35', 'cjxd'),
       (41, 194, '2008-12-27', '1900-01-01 00:00:00', '11:59:05', 'jx'),
       (42, 1, '2002-08-16', '2000-08-11 11:34:38', '21:39:43', 'xdo'),
       (43, 220, '2001-06-17', '1900-01-01 00:00:00', '00:00:00', 'oyg'),
       (44, 9, '2002-10-16', '2008-12-07 23:41:33', '00:00:00', 'gx'),
       (45, 248, '2008-04-06', '1900-01-01 00:00:00', '12:32:24', 'x'),
       (46, 0, '2000-07-08', '2001-12-27 19:38:22', '00:00:00', 'vgqmw'),
       (47, 0, '2005-03-16', '1900-01-01 00:00:00', '06:22:01', 'qmwcid'),
       (48, 4, '2002-06-19', '2007-03-08 02:43:50', '07:00:21', 'mwc'),
       (49, 3, '2005-11-25', '2001-11-14 17:21:32', '17:59:20', 'wcidtu'),
       (50, 7, '2007-07-08', '1900-01-01 00:00:00', '01:58:05', 'cidtum'),
       (51, 7, '2000-06-20', '2004-07-20 11:05:12', '22:24:24', 'dtumxwc'),
       (52, 5, '2006-03-28', '2008-08-15 08:28:18', '04:22:26', 'tumxwc'),
       (53, 1, '2004-03-05', '1900-01-01 00:00:00', '00:00:00', 'umxwcf'),
       (54, 0, '2009-05-10', '2004-01-28 15:16:19', '11:46:32', 'mxwcft'),
       (55, 67, '2004-04-18', '2001-06-23 00:00:00', '20:12:09', 'xwcfted'),
       (56, 204, '2008-01-10', '2009-02-12 07:59:52', '13:58:17', 'wc'),
       (57, 9, '2000-07-12', '2004-12-10 07:32:31', '04:04:48', 'ftedx'),
       (58, 5, '2001-06-16', '2006-09-06 12:15:44', '10:14:16', 't'),
       (59, 6, '2000-02-20', '2003-09-13 14:23:06', '21:22:20', 'dx'),
       (60, 6, '2001-02-07', '2004-01-18 00:00:00', '10:15:21', 'xqyciak'),
       (61, 1, '2008-12-24', '2004-04-02 07:16:01', '16:30:10', 'qy'),
       (62, 1, '2009-12-14', '2000-01-04 14:51:24', '03:57:54', 'y'),
       (63, 5, '2008-03-07', '2001-06-24 00:00:00', '06:41:05', 'ciak'),
       (64, 4, '2005-01-19', '2001-06-02 03:41:12', '00:00:00', 'iakh'),
       (65, 4, '2003-02-10', '1900-01-01 00:00:00', '08:51:25', 'ak'),
       (66, 9, '2005-12-25', '2007-07-13 14:26:05', '14:32:55', 'hxptz'),
       (67, 4, '2003-10-13', '2008-03-20 21:14:50', '00:21:31', 'xptzfp'),
       (68, 3, '2001-08-03', '1900-01-01 00:00:00', '00:00:00', 'ptzfpjw'),
       (69, 0, '2006-04-01', '1900-01-01 00:00:00', '11:26:05', 'tzfpjwr'),
       (70, 2, '2003-12-27', '2002-05-09 18:39:28', '05:28:11', 'wrgeo'),
       (71, 100, '2001-10-25', '2006-01-13 00:00:00', '04:35:51', 'r'),
       (72, 37, '2006-09-12', '2003-12-04 05:20:00', '06:10:43', 'geo'),
       (73, 5, '2003-06-04', '2003-07-21 11:43:03', '17:26:47', 'eozxnby'),
       (74, 6, '2009-11-13', '2006-12-24 00:00:00', '22:34:54', 'oz'),
       (75, 1, '2006-08-13', '2005-08-25 00:00:00', '21:27:38', 'zxnbyc'),
       (76, 7, '2007-07-09', '2003-10-16 01:16:30', '03:14:14', 'xnbycjz'),
       (77, 6, '2000-01-07', '2001-06-22 00:00:00', '00:00:00', 'nby'),
       (78, 5, '2004-12-21', '2004-09-01 18:53:04', '16:06:30', 'bycj'),
       (79, 0, '2003-10-14', '2000-04-13 05:21:03', '19:04:51', 'ycjzxie');

with test01 as (
    select `e`.col_int, `c`.col_varchar, row_number() over (partition by `e`.col_int
      order by `e`.col_date) as `rank` from `e` inner join `c` on `c`.col_int = `e`.col_int)
select col_int as a from test where `rank` = 1 group by col_int;

with test02 as (
    select `dd`.col_int, `c`.col_datetime, rank() over (partition by `dd`.col_int
      order by `dd`.col_date) as `rank` from `dd` left join `c` on `c`.col_int = `dd`.col_int)
select col_int as a from test02 where `rank` = 1 group by col_int;

with test03 as (
    select `dd`.col_int, `e`.col_varchar, dense_rank() over (partition by `dd`.col_int
      order by `dd`.col_datetime) as `rank` from `dd` left join `e` on `e`.col_int = `dd`.col_int)
select col_int as a from test03 where `rank` = 1 group by col_int;

select `c`.col_int,`c`.col_datetime, `dd`.col_time, row_number() over (partition by `c`.col_int
	order by `dd`.col_time) as `rank` from `dd` left join `c` on `c`.col_int = `dd`.col_int;

select `c`.col_int,`c`.col_datetime, `dd`.col_time, sum(`c`.col_int) over (partition by `c`.col_int
	order by `dd`.col_time) as `rank` from `dd` left join `c` on `c`.col_int = `dd`.col_int;

select `c`.col_int,`c`.col_datetime, `dd`.col_time, avg(`dd`.col_int) over (partition by `c`.col_int
	order by `dd`.col_time) as `rank` from `dd` left join `c` on `c`.col_int = `dd`.col_int;

select `c`.col_int,`dd`.col_time, min(`dd`.col_int) over (partition by `c`.col_int
	order by `dd`.col_time) as `rank` from `dd` left join `c` on `c`.col_int = `dd`.col_int;
drop table `c`;
drop table `dd`;
drop table `e`;
drop table if exists t2;
CREATE TABLE t2 (a int, b int);
insert into t2 values(1,1);
insert into t2 values(2,2);
insert into t2 values(3,3);
insert into t2 values(4,4);
insert into t2 values(1,1);
insert into t2 values(5,5);
insert into t2 values(6,6);
select a,min(a) over (partition by a) ,sum(a+1) over (partition by a) from t2;
drop table t2;


-- order by
drop table if exists int_8;
create table int_8 (id tinyint, sex varchar(10));
insert into int_8 values (-1, 'moolol');
insert into int_8 values (-128, 'fdhsajhd');
insert into int_8 values (32, 'fdhsajhd');
insert into int_8 values (-1, 'fdhsajhd');
insert into int_8 values (23, 'moolol');
insert into int_8 values (127, null);
insert into int_8 values (-128, null);
select * from int_8;
select row_number() over (partition by sex order by id rows between unbounded preceding and unbounded following) as a, id,
       sum(id) over (partition by sex order by id rows between 1 following and 2 following) as b from int_8;
select row_number() over (partition by sex order by id rows between 1 following and 2 following) as a, sum(id) over (partition by sex order by id
    rows between 1 following and 2 following) as b from int_8;
drop table int_8;

drop table if exists int_16;
create table int_16(col1 smallint,col2 bool,col3 datetime);
insert into int_16 values(-32768, true, '2023-05-16 00:12:12');
insert into int_16 values(22201, false, '1997-01-13 12:12:00');
insert into int_16 values(-32768, true, '2000-10-10 11:11:11');
insert into int_16 values(4, false, '1020-10-01 01:01:01');
insert into int_16 values(32767, null, null);
select max(col1) over (partition by col2 order by col1 rows between 2 preceding and 3 following) as col1 from int_16;
select dense_rank() over (partition by col2 order by col1 rows between 2 preceding and 3 following) as col1 from int_16;
drop table int_16;

drop table if exists int_32;
create table int_32(i int, j int, k int);
insert into int_32 values (-2147483648, 1, 1);
insert into int_32 values (-2147483648, 1, 2);
insert into int_32 values (2147483647, 1, 2);
insert into int_32 values (2147483647, 2, 1);
insert into int_32 values (13289392, 2, 2);
insert into int_32 values (23289483, 1, 1);
insert into int_32 values (-3824, 1, 1);
insert into int_32 values (2438294, 1, 2);
insert into int_32 values (-3824, 2, 1);
select * from int_32;
select *, rank() over (order by i,j,k rows between 2 preceding and 3 following) as o_ijk,
        min(i) over (order by j rows between 4 preceding and 5 following) as o_j,
        rank() over (order by k,j rows between 1 preceding and 1 following) as o_kj from int_32 order by i,j,k;
drop table int_32;

drop table if exists int_64;
create table int_64(i bigint unsigned, j int, k int);
insert into int_64 values (18446744073709551614, 1, 1);
insert into int_64 values (18446744073709551614, 1, 2);
insert into int_64 values (2147483647, 1, 2);
insert into int_64 values (2147483647, 2, 1);
insert into int_64 values (0, 2, 2);
insert into int_64 values (0, 1, 1);
select * from int_64;
select *, rank() over (order by i,j,k rows between 2 preceding and 3 following) as o_ijk,
        min(i) over (order by j rows between 4 preceding and 5 following) as o_j,
        rank() over (order by k,j rows between 1 preceding and 1 following) as o_kj from int_64 order by i,j,k;
drop table int_64;

drop table if exists uint_8;
create table uint_8 (col1 tinyint unsigned, col2 varchar(10));
insert into uint_8 values (1, 'moolol');
insert into uint_8 values (128, 'fdhsajhd');
insert into uint_8 values (32, 'fdhsajhd');
insert into uint_8 values (1, 'fdhsajhd');
insert into uint_8 values (23, 'moolol');
insert into uint_8 values (255, null);
insert into uint_8 values (128, null);
select * from uint_8;
select row_number() over (partition by col2 order by col1 rows between unbounded preceding and unbounded following) as a,
        sum(col1) over (partition by col2 order by col1 rows between 1 following and 2 following) as b from uint_8;
select dense_rank() over (partition by col2 order by col1) as col1, sum(col1) over (partition by col2 order by col1) as col2 from uint_8;
drop table uint_8;

drop table if exists uint_16;
create table uint_16(col1 smallint unsigned,col2 bool,col3 datetime);
insert into uint_16 values(0, true, '2023-05-16 00:12:12');
insert into uint_16 values(0, false, '1997-01-13 12:12:00');
insert into uint_16 values(65535, true, '2000-10-10 11:11:11');
insert into uint_16 values(4, false, '1020-10-01 01:01:01');
insert into uint_16 values(null, null, null);
insert into uint_16 values(65535, null, '1997-11-10 10:10:10');
select * from uint_16;
select max(col1) over (partition by col2 order by col1 rows between unbounded preceding and unbounded following) as col1 from uint_16;
select dense_rank() over (partition by col2 order by col1 rows between unbounded preceding and unbounded following) as col1 from uint_16;
drop table uint_16;

drop table if exists uint_32;
create table uint_32(i int unsigned, j int, k int);
insert into uint_32 values (4294967295, 1, 1);
insert into uint_32 values (4294967295, 1, 2);
insert into uint_32 values (2147483647, 1, 2);
insert into uint_32 values (2147483647, 2, 1);
insert into uint_32 values (13289392, 2, 2);
insert into uint_32 values (23289483, 1, 1);
insert into uint_32 values (3824, 1, 1);
insert into uint_32 values (2438294, 1, 2);
insert into uint_32 values (3824, 2, 1);
select * from uint_32;
select  max(i) over (order by i,j,k rows between 1 preceding and 2 following) as o_ijk,
        dense_rank() over (order by i rows between unbounded preceding and unbounded following) as o_j,
        rank() over (order by k,j rows between unbounded preceding and unbounded following) as o_kj from uint_32 order by i,j,k;
drop table uint_32;

drop table if exists uint_64;
create table uint_64(i bigint unsigned, j bigint unsigned, k decimal);
insert into uint_64 values (18446744073709551615, 2147483647, 123213.99898);
insert into uint_64 values (4294967295, 2147483647, 2);
insert into uint_64 values (18446744073709551615, 1, 2);
insert into uint_64 values (2147483647, 23289483, 123213.99898);
insert into uint_64 values (13289392, 2, 2);
insert into uint_64 values (18446744073709551615, 23289483, 1);
insert into uint_64 values (3824, 13289392, 123213.99898);
insert into uint_64 values (2438294, 1, 2);
insert into uint_64 values (3824, 13289392, 1);
select * from uint_64;
select rank() over (order by i,j,k rows between unbounded preceding and unbounded following) as o_ijk,
       max(i) over (order by i rows between 10 preceding and 2 following) as o_j,
       rank() over (order by k,j rows between unbounded preceding and unbounded following) as o_kj from uint_64 order by i,j,k;
drop table uint_64;

drop table if exists decimal_64;
create table decimal_64(col1 decimal(18,10), col2 char(10));
insert into decimal_64 values (23189723.2314892238902, 'male');
insert into decimal_64 values (23189723.2314892238902, 'female');
insert into decimal_64 values (3278.3243214124242, 'male');
insert into decimal_64 (col1, col2) values (32134243.2143243242142, 'male');
insert into decimal_64 values (-23189723.2314892238902, 'male');
insert into decimal_64 values (-3278.3243214124242, 'female');
insert into decimal_64 (col1, col2) values (32134243.2143243242142, 'male');
select * from decimal_64;
select max(col1) over (order by col1 rows between 1 preceding and 0 following) as newcol1 from decimal_64;
select min(col1) over (partition by col2 order by col1 rows between unbounded preceding and unbounded following) as newcol from decimal_64 limit 4;
drop table decimal_64;

drop table if exists decimal_128;
create table decimal_128(col1 decimal(38,10), col2 char(10));
insert into decimal_128 values (2318972338274832748378887878.2314892238902, 'male');
insert into decimal_128 values (2318972338274832748378887878.23148922389, 'female');
insert into decimal_128 values (-3278234242342349090943024982.3243214124242, 'male');
insert into decimal_128 (col1, col2) values (32134243.2143243242142, 'male');
insert into decimal_128 values (-23189723.2314892238902, 'male');
insert into decimal_128 values (-3278234242342349090943024982.3243214124242, 'female');
insert into decimal_128 (col1, col2) values (32134243.2143243242142, 'male');
select * from decimal_128;
select min(col1) over (order by col1 rows between 1 preceding and 0 following) as newcol1 from decimal_128;
select dense_rank() over (order by col1 rows between 1 preceding and 2 following) as newcol2 from decimal_128;
select row_number() over (partition by col2 order by col1 rows between unbounded preceding and unbounded following) as newcol from decimal_128;
drop table decimal_128;

drop table if exists time01;
create table time01 (col1 time, col2 timestamp);
insert into time01 values ('12:1b2:12', '2023-05-16 00:12:12');
insert into time01 values ('23:23:59', '2019-05-16 23:23:59');
insert into time01 values ('12:12:12', '1997-01-13 01:02:03');
insert into time01 values ('23:23:59', '2023-05-16 00:12:12');
insert into time01 values ('01:02:03', '2019-05-16 23:23:59');
insert into time01 values (null, null);
select * from time01;
select rank() over (order by col1 rows between 1 preceding and 0 following) as newcol1 from time01;
select dense_rank() over (order by col1 rows between 1 preceding and 2 following) as newcol2 from time01;
select row_number() over (partition by col2 order by col1 rows between unbounded preceding and unbounded following) as newcol from time01;
select rank() over (partition by col1 order by col2 rows between 0 preceding and 0 following) as newcol from time01;
select max(col2) over (partition by col1 order by col2 rows between 3 preceding and 3 following) as newcol from time01;
drop table time01;

drop table if exists window01;
create table window01(col1 int, col2 varchar(20));
insert into window01 values(1,'老师');
insert into window01 values(2,'医生');
insert into window01 values(3,'工人');
insert into window01 values(10,'学生');
insert into window01 values(20,'学生');
insert into window01 values(12,'学生');
insert into window01 values(21,'老师');
insert into window01 values(100,'老师');
insert into window01 values(200,'工人');

select col2, avg(col1) over (partition by col2 order by col1 desc) from window01;
select col2, col1, sum(col1) over (partition by col2 order by col1 desc) from window01;
select col2, col1, min(col1) over (partition by col2 order by col1 desc) from window01;
select col2, col1, max(col1) over (partition by col2 order by col1 desc) from window01;
select col2, col1, count(col1) over (partition by col2 order by col1 desc) from window01;

create table item(i_item_sk integer not null,i_manufact_id  integer , primary key (i_item_sk));
INSERT INTO item (i_item_sk, i_manufact_id) VALUES (1, 100);
INSERT INTO item (i_item_sk, i_manufact_id) VALUES (2, 200);
INSERT INTO item (i_item_sk, i_manufact_id) VALUES (3, 300);
create table store_sales( ss_item_sk       integer     not null, ss_ticket_number       integer    not null, ss_sales_price   decimal(7,2)  , primary key (ss_item_sk, ss_ticket_number));
INSERT INTO store_sales (ss_item_sk, ss_ticket_number, ss_sales_price) VALUES (1, 101, 19.99);
INSERT INTO store_sales (ss_item_sk, ss_ticket_number, ss_sales_price) VALUES (1, 102, 29.99);
INSERT INTO store_sales (ss_item_sk, ss_ticket_number, ss_sales_price) VALUES (2, 201, 39.99);
select * from (select i_manufact_id, sum(ss_sales_price) sum_sales, avg(sum(ss_sales_price)) over (partition by i_manufact_id) avg_quarterly_sales from item, store_sales group by i_manufact_id) tmp1 where abs(sum_sales - avg_quarterly_sales) / avg_quarterly_sales > 0.1 limit 1;

-- information_schema is now a table which is compatible with mysql, it is now an empty table
select group_concat(c.column_name order by ordinal_position) key_columns  from information_schema.key_column_usage c where c.table_schema='test1' and c.table_name='region' and constraint_name='PRIMARY';

-- NTILE window function tests
drop table if exists t_ntile;
create table t_ntile (a int, b int, c int);
insert into t_ntile values(1, 10, 100), (2, 20, 200), (3, 30, 300), (4, 40, 400), (5, 50, 500), (6, 60, 600), (7, 70, 700), (8, 80, 800), (9, 90, 900), (10, 100, 1000);

-- Basic ntile with 3 buckets
select a, ntile(3) over (order by a) as bucket from t_ntile;

-- Ntile with partition
select a, b, ntile(2) over (partition by a % 2 order by b) as bucket from t_ntile;

-- Ntile with different bucket counts
select a, ntile(1) over (order by a) as bucket from t_ntile;
select a, ntile(4) over (order by a) as bucket from t_ntile;
select a, ntile(10) over (order by a) as bucket from t_ntile;

-- Ntile with uneven distribution
select a, ntile(3) over (order by a) as bucket from t_ntile where a <= 5;
select a, ntile(4) over (order by a) as bucket from t_ntile where a <= 9;

-- Ntile combined with other window functions
select a, ntile(3) over (order by a) as bucket, rank() over (order by a) as rnk, row_number() over (order by a) as rn from t_ntile;

-- Ntile with multiple partitions
select a % 3 as grp, a, ntile(2) over (partition by a % 3 order by a) as bucket from t_ntile;

drop table t_ntile;

drop database test;