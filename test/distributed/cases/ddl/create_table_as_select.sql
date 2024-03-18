create database test;
use test;

create table t1(a int default 123, b char(5));
desc t1;
INSERT INTO t1 values (1, '1');
INSERT INTO t1 values (2, '2');
INSERT INTO t1 values (0x7fffffff, 'max');
select * from t1;

CREATE table t2 (c float) as select b, a from t1;
desc t2;
select * from t2;

CREATE table if not exists t2 (d float) as select b, a from t1;
desc t2;

CREATE table t3 (a bigint unsigned not null auto_increment primary key, c float) as select a, b from t1;
desc t3;
select * from t3;

CREATE table t4 (a tinyint) as select * from t1;

CREATE table t5 (a char(10)) as select * from t1;
desc t5;
select * from t5;

insert into t1 values (1, '1_1');
select * from t1;
CREATE table t6 (a int unique) as select * from t1;
drop table t6;

CREATE table t6 as select max(a) from t1;
desc t6;
select * from t6;

CREATE table t7 as select * from (select * from t1) as t;
desc t7;
select * from t7;

CREATE table t8 as select a as alias_a, 1 from t1;
desc t8;
select * from t8;

CREATE table t9 (index (a)) as select * from t1;
desc t9;
select * from t9;

drop table t1;
drop table t2;
drop table t3;
drop table t5;
drop table t6;
drop table t7;
drop table t8;
drop table t9;

-- the columns of the original table cover all data types
drop table if exists table01;
create table table01(a int default 123, b char(5));
desc table01;
insert into table01 values (1, '1');
insert into table01 values (2, '2');
insert into table01 values (0x7fffffff, 'max');
select * from table01;

drop table if exists table02;
create table table02 (c float) as select b, a from table01;
desc table02;
select * from table02;

drop table if exists table02;
create table table02 (d float) as select b, a from table01;
desc table02;
select * from table02;
drop table table01;
drop table table02;

drop table if exists table03;
create table table03(col1 int, col2 char, col3 varchar(10), col4 text, col5 tinyint unsigned, col6 bigint, col7 decimal, col8 float, col9 double);
insert into table03 values (1, 'a', 'database', 'cover all data types', 12, 372743927942, 3232.000, -1489.1231, 72392342);
insert into table03 values (2, 'b', 'table', 'database management system', 1, 324214, 0.0001, 32932.000, -321342.0);
insert into table03 values (null, null, null, null, null, null, null, null, null);
select * from table03;

drop table if exists table04;
create table table04 as select * from table03;
-- @bvt:issue#14792
show create table table04;
-- @bvt:issue
select * from table04;

drop table if exists table05;
create table table05 as select col1, col3, col5, col7 from table03;
show create table table05;
select * from table05;

drop table if exists table06;
create table table06(col10 binary) as select col2, col4, col6, col8, col9 from table03;
-- @bvt:issue#14792
show create table table06;
-- @bvt:issue
desc table06;
select * from table06;

drop table table03;
drop table table04;
drop table table05;
drop table table06;

drop table if exists t1;
create table t1(a int default 123, b char(5));
desc t1;
INSERT INTO t1 values (1, '1');
INSERT INTO t1 values (2, '2');
INSERT INTO t1 values (0x7fffffff, 'max');
select * from t1;

create table t2 (c float) as select b, a from t1;
desc t2;
select * from t2;
-- @bvt:issue#14775
CREATE table if not exists t2 (d float) as select b, a from t1;
-- @bvt:issue
select * from t2;
drop table t1;
drop table t2;

drop table if exists table07;
create table table07(col1 date, col2 datetime, col3 timestamp, col4 blob, col5 json);
insert into table07 values ('2020-10-11', '2023-11-11 10:00:01', '1997-01-13 12:12:12.000', 'abcdef', '{"x": 17, "x": "red"}');
insert into table07 values ('1919-12-01', '1990-10-10 01:01:01', '2001-12-12 01:01:01.000', 'xxxx', '{"t1": "a"}');
insert into table07 values (null, null, null, null, null);
select * from table07;

drop table if exists table08;
create table table08(col6 int, col7 bigint, col8 char) as select * from table07;
-- @bvt:issue#14792
show create table table08;
-- @bvt:issue
select * from table08;
drop table table08;

drop table if exists table09;
create table table09 as select col1, col2, col4 as newCol4 from table07;
-- @bvt:issue#14792
show create table table09;
-- @bvt:issue
select * from table09;
drop table table09;

-- column duplication
drop table if exists table12;
create table table12 (col1 date) as select * from table07;
-- @bvt:issue#14792
show create table table12;
-- @bvt:issue
select * from table12;
drop table table12;
drop table table07;

-- create table as select distinct
drop table if exists distinct01;
create table distinct01 (
    id int,
    first_name varchar(50),
    last_name varchar(50),
    course varchar(100)
);

insert into distinct01 (id, first_name, last_name, course)  values
(1, 'John', 'Doe', 'Computer Science'),
(2, 'Jane', 'Smith', 'Mathematics'),
(3, 'Alice', 'Johnson', 'Computer Science'),
(4, 'Bob', 'Brown', 'Physics'),
(5, 'Charlie', 'Doe', 'Computer Science'),
(5, 'Charlie', 'Doe', 'Computer Science');

drop table if exists unique_courses;
create table unique_courses as select distinct course from distinct01;
show create table unique_courses;
select * from unique_courses;

drop table if exists unique_courses;
create table unique_courses as select distinct * from distinct01;
show create table unique_courses;
select * from unique_courses;
drop table unique_courses;

-- columns that can be casted to each other (float -> double, double -> float)
drop table if exists cast01;
create table cast01 (col1 float, col2 double);
insert into cast01 values (2617481243.2114, 372534.4353);
insert into cast01 values (-3628742.3223252, 0);
insert into cast01 values (null, null);
select * from cast01;

drop table if exists cast02;
create table cast02(col1 double, col2 float) select * from cast01;
show create table cast02;
select * from cast02;
drop table cast01;
drop table cast02;

-- columns that can be casted to each other (value -> char)
drop table if exists cast03;
create table cast03 (col1 int, col2 float, col3 double);
insert into cast03 values (321424, 213412.23142, -100.313);
insert into cast03 values (-1241, 2314321, 0);
insert into cast03 values (0, 0, 0);
select * from cast03;

drop table if exists cast04;
create table cast04(col1 char(10), col2 char(10), col3 char(10)) as select * from cast03;
-- @bvt:issue#14475
select * from cast04;
-- @bvt:issue
drop table cast03;
drop table cast04;

-- columns that can be casted to each other (character type in numeric format -> numeric type)
drop table if exists cast05;
create table cast05 (col1 char, col2 varchar(10));
insert into cast05 values ('9', '-32824');
insert into cast05 values ('0', '32422');
insert into cast05 values (null, null);
select * from cast05;

drop table if exists cast06;
create table cast06(col1 int, col2 bigint) as select * from cast05;
select * from cast06;
show create table cast06;
drop table cast05;
drop table cast06;

-- columns that can be casted to each other (time -> int)
drop table if exists time01;
create table time01 (col1 date, col2 datetime, col3 timestamp, col4 time);
insert into time01 values ('2020-01-01', '2020-12-12 00:00:01', '1997-01-01 10:10:10.000', '12:12:12');
insert into time01 values ('1996-12-11', '1989-12-09 00:01:01', '2000-05-06 01:01:01.000', '00:01:01');
insert into time01 values (null, null, null, null);
select * from time01;

drop table if exists time02;
create table time02 (col1 int, col2 int, col4 int) as select * from time01;
show create table time02;
select * from time02;
drop table time02;

-- columns that can be casted to each other (time -> decimal)
drop table if exists time03;
create table time03 (col2 decimal(38, 0), col4 decimal) as select col2, col3, col4 from time01;
show create table time03;
select * from time03;
drop table time03;

-- the columns of a table have constraints
drop table if exists table01;
create table table01 (col1 int primary key , col2 char default 'c', col3 decimal not null);
insert into table01 values (1, 'a', 3728.424);
insert into table01 values (3131, 'b', -32832.43);
insert into table01 values (-1, '' , 0);
select * from table01;

drop table if exists table02;
create table table02 as select * from table01;
show create table table02;
desc table02;
select * from table02;
drop table table01;
drop table table02;

drop table if exists table04;
drop table if exists table03;
create table table03 (a int primary key, b varchar(5) unique key);
create table table04 (a int ,b varchar(5), c int, foreign key(c) references table03(a));
insert into table03 values (101,'abc'),(102,'def');
insert into table04 values (1,'zs1',101),(2,'zs2',102);

drop table if exists table05;
create table table05 as select * from table04;
show create table table05;
select * from table05;
drop table if exists table06;
create table table06 (d char not null default 'a') as select a from table03;
-- @bvt:issue#14792
show create table table06;
-- @bvt:issue
select * from table06;
drop table table04;
drop table table03;
drop table table05;
drop table table06;

-- ctas combines with aggr functions
drop table if exists math01;
create table math01 (col1 int default 0, col2 decimal, col3 float, col4 double not null);
insert into math01 values (1, 7382.4324, 432453.3243, -2930.321323);
insert into math01 values (-100, 3283.32324, 328932.0, -9392032);
insert into math01 values (22813, -241, 932342.4324, -0.1);
insert into math01 values (null, null, null, 10);

drop table if exists agg01;
-- @bvt:issue#14792
create table agg01 as select avg(col1) as avgCol, sum(col2) as sumcol, count(col3) as countCol, max(col4) as maxCol, min(col4) as minCol from math01;
show create table agg01;
select * from agg01;
drop table agg01;
-- @bvt:issue

drop table if exists bit01;
create table bit01 (col1 char(1), col2 int);
insert into bit01 values ('a',111),('a',110),('a',100),('a',000),('b',001),('b',011);
select * from bit01;

drop table if exists bit02;
create table bit02 as select bit_and(col2), bit_or(col2), bit_xor(col2), stddev_pop(col2) from bit01;
-- @bvt:issue#14792
desc bit02;
show create table bit02;
-- @bvt:issue
select count(*) from bit02;
select * from bit02;
drop table bit02;
drop table bit01;

-- ctas combines with math functions
drop table if exists math01;
create table math01 (col1 int, col2 decimal, col3 bigint, col4 double, col5 float);
insert into math01 values (1, 10.50, 1234567890, 123.45, 678.90),
                            (2, 20.75, 9876543210, 234.56, 789.01),
                            (3, 30.10, 1122334455, 345.67, 890.12),
                            (4, 40.25, 2233445566, 456.78, 901.23),
                            (5, 50.40, -3344556677, 567.89, 101.24),
                            (6, 60.55, -4455667788, 678.90, 112.35),
                            (7, 70.70, 5566778899, 789.01, 123.46),
                            (8, 80.85, -6677889900, 890.12, 134.57),
                            (9, 90.00, 7788990011, 901.23, 145.68),
                            (10, 100.00, 8899001122, 101.24, 156.79);
drop table if exists math02;
create table math02 as select abs(col3), sin(col1), cos(col2), tan(col1), round(col4) from math01;
select * from math02;
drop table if exists math03;
create table math03 as select cot(col1), atan(col1), sinh(col1), floor(col5) from math01;
select * from math02;
drop table if exists math04;
create table math04 as select ceil(col4), power(col5, 2), pi() * col1, log(col2), ln(col2), exp(col1) from math01;
select * from math04;

drop table math01;
drop table math02;
drop table math03;
drop table math04;

-- ctas combines with string functions
drop table if exists string01;
create table string01 (col1 varchar(40), col2 char, col3 text default null);
insert into string01 values ('  database system', '2', '云原生数据库');
insert into string01 values (' string function ', '1', '字符串函数');
insert into string01 values ('test create table as select', '0', null);

drop table if exists string02;
create table string02 as select concat_ws(',', col1, 'abcde') from string01;
-- @bvt:issue#14792
show create table string02;
-- @bvt:issue
select * from string02;
drop table string02;

drop table if exists string03;
create table string03 as select find_in_set(col2, col1) from string01;
-- @bvt:issue#14792
show create table string03;
-- @bvt:issue
select * from string03;
drop table string03;

drop table if exists string04;
create table string04 as select oct(col2), empty(col3), length(col1) from string01;
-- @bvt:issue#14792
show create table string04;
-- @bvt:issue
select * from string04;
drop table string04;

drop table if exists string05;
create table string05 as select trim(col1), ltrim(col1), rtrim(col1) from string01;
show create table string05;
select * from string05;
drop table string05;

drop table if exists string06;
create table string06 as select lpad(col1, 5, '-'), rpad(col1, 1, '-') from string01;
-- @bvt:issue#14792
show create table string06;
-- @bvt:issue
select * from string06;
drop table string06;

drop table if exists string07;
create table string07 as select startswith(col1, ' '), endswith(col1, ' ') from string01;
show create table string07;
select * from string07;
drop table string07;

drop table if exists string08;
create table string08 as select hex(col2) from string01;
show create table string08;
select * from string08;
drop table string08;

drop table if exists string09;
create table string09 as select substring(col1, 3, 4), reverse(col2) from string01;
show create table string09;
select * from string09;
drop table string09;

drop table if exists string10;
create table string10 (col1 bigint);
insert into string10 values (2319318313), (null);
drop table if exists string11;
create table string11 as select bin(col1) from string10;
show create table string11;
select * from string11;
drop table string10;
drop table string11;

drop table if exists string12;
drop table string12;
create table string12 (col1 varchar(100) not null, col2 date not null);
insert into string12 values ('   Deepak Sharma', '2014-12-01'  ), ('   Ankana Jana', '2018-08-17'),('  Shreya Ghosh', '2020-09-10');
select * from string12;
drop table if exists string13;
create table string13 as select * from string12 where col1 = space(5);
show create table string13;
select * from string13;
drop table string12;
drop table string13;
drop table string01;

-- ctas combines with time functions
drop table if exists time01;
create table time01(col1 date, col2 datetime, col3 timestamp, col4 time);
insert into time01 values ('2020-10-11', '2023-11-11 10:00:01', '1997-01-13 12:12:12.000', '12:12:12');
insert into time01 values ('1919-12-01', '1990-10-10 01:01:01', '2001-12-12 01:01:01.000', '10:59:59');
insert into time01 values (null, null, null, null);
select * from time01;

drop table if exists time02;
create table time02 as select date_format(col2, '%W %M %Y') from time01;
-- @bvt:issue#14792
show create table time02;
desc time02;
-- @bvt:issue
select * from time02;
drop table time02;

drop table if exists time03;
create table time03 as select date(col1), date(col2), year(col1), day(col1), weekday(col1), dayofyear(col1) as dya from time01;
-- @bvt:issue#14792
desc time03;
show create table time03;
-- @bvt:issue
select * from time03;
drop table time03;

drop table if exists time04;
create table time04 as select date_add(col2, interval 45 day), date_sub(col2, interval 5 day) from time01;
show create table time04;
select * from time04;
drop table time04;

-- @bvt:issue#14804
drop table if exists time05;
create table time05 as select unix_timestamp(col1) from time01;
show create table time05;
select * from time05;
drop table time05;
-- @bvt:issue

drop table if exists time06;
create table time06 as select datediff('2007-12-31 23:59:59', col1) as timedifferent from time01;
-- @bvt:issue#14792
show create table time06;
-- @bvt:issue
select * from time06;
drop table time06;

drop table if exists time07;
create table time07 as select timediff("22:22:22", col4) as timedifferent from time01;
show create table time07;
select * from time07;
drop table time07;

drop table if exists test01;
create table test01 as select col1 from time01 order by col1 nulls first;
select * from test01;
drop table test01;

drop table if exists test02;
create table test02 as select * from time01 order by col2 desc nulls first;
select * from test02;
drop table test02;

drop table if exists test03;
create table test03 as select * from time01 order by col2 desc nulls last;
select * from test03;
drop table test03;

drop table if exists test04;
create table test04 as select col1 from time01 order by col1 nulls first;
select * from test04;
drop table test04;

insert into time01 values ('2014-10-11', '2021-11-11 10:00:01', '1989-01-13 12:12:12.000', '12:11:12');
insert into time01 values ('2014-12-11', '2021-01-11 10:00:02', '1981-02-13 12:12:12.000', '14:12:12');
insert into time01 values ('2015-10-11', '2021-11-11 10:00:03', '1982-01-13 12:12:12.000', '15:12:12');
insert into time01 values ('2016-10-11', '2021-11-11 10:00:04', '1983-01-13 12:12:12.000', '16:12:12');
insert into time01 values ('2017-10-11', '2021-11-11 10:00:05', '1984-01-13 12:12:12.000', '17:12:12');
insert into time01 values ('2018-10-11', '2021-11-11 10:00:06', '1985-01-13 12:12:12.000', '18:12:12');
insert into time01 values ('2019-10-11', '2021-11-11 10:00:07', '1986-01-13 12:12:12.000', '19:12:12');
insert into time01 values ('2010-10-11', '2021-11-11 10:00:08', '1987-01-13 12:12:12.000', '20:12:12');
insert into time01 values ('2033-10-11', '2021-11-11 10:00:09', '1988-01-13 12:12:12.000', '21:12:12');
insert into time01 values ('2014-10-12', '2021-11-11 10:00:20', '1989-02-13 12:12:12.000', '22:12:12');

drop table if exists new_table;
create table new_table as
select *
from time01
order by col1
limit 5
offset 10;
select * from new_table;
drop table new_table;

drop table if exists new_table01;
create table new_table01 as
select col2, col3
from time01
order by col1 desc
limit 100
offset 10;
select * from new_table01;
drop table new_table01;
drop table time01;

-- cras combines with group by ... having, order by
drop table if exists orders;
create table orders (order_id int primary key , customer_id int, order_date date, total_amount decimal);
insert into orders values (1, 101, '2023-01-01', 100.00),
                          (2, 101, '2023-01-05', 150.00),
                          (3, 102, '2023-01-02', 200.00),
                          (4, 103, '2023-01-03', 50.00),
                          (5, 101, '2023-01-04', 75.00),
                          (6, 104, '2023-01-06', 300.00),
                          (7, 104, '2023-01-07', 200.00),
                          (8, 105, '2023-01-08', 100.00);
select * from orders;

drop table if exists customer_totals;
create table customer_totals as select customer_id, count(order_id) as total_orders, sum(total_amount) as total_amount from orders group by customer_id having count(order_id) > 1 and sum(total_amount) > 150.0;
-- @bvt:issue#14792
show create table customer_totals;
-- @bvt:issue
select * from customer_totals;

drop table if exists max_totals;
create table max_totals as select customer_id, total_orders from customer_totals order by total_orders desc limit 1;
-- @bvt:issue#14792
desc max_totals;
-- @bvt:issue
select * from max_totals;

drop table if exists max_customer;
create table max_customer as select customer_id, total_amount from customer_totals order by total_amount asc limit 1;
show create table max_customer;
select * from max_customer;

drop table orders;
drop table customer_totals;
drop table max_totals;
drop table max_customer;

-- cras combines with filter
drop table if exists original_table;
create table original_table (id int primary key, name varchar(50), age int, salary decimal, hire_date date);
insert into original_table (id, name, age, salary, hire_date) values (1, 'Alice', 30, 5000.00, '2020-01-01'),
                                                                     (2, 'Bob', 35, 6000.00, '2021-05-15'),
                                                                     (3, 'Charlie', 28, 4500.00, '2022-02-20'),
                                                                     (4, 'David', 40, 7000.00, '2021-10-01'),
                                                                     (5, 'Eve', 25, 4000.00, '2020-07-15');

drop table if exists selected_employees;
-- @bvt:issue#14775
create table selected_employees as select * from original_table where
salary >= 5500.00
and salary < 7000.00
and age > 29
and hire_date >= '2021-01-01'
and name not like 'A%'
and id not in (1, 3)
and salary between 5000.00 and 6500.00;
show create table selected_employees;
select * from selected_employees;
drop table selected_employees;
-- @bvt:issue
drop table original_table;

-- after ctas, create view
drop table if exists view01;
drop table if exists view02;
drop view if exists v1;
create table view01 (a int, b int);
insert into view01 values (1,2),(3,4);
create table view02 select * from view01;
create view v1 as select * from view02;
select * from v1;
drop view v1;
drop table view01;
drop table view02;

-- update/insert/delete/truncate/alter
drop table if exists table01;
create table table01 (
    id int auto_increment primary key,
    col1 varchar(255) not null ,
    col2 int,
    col3 decimal(10, 2),
    col4 date,
    col5 boolean,
    col6 enum('apple', 'banana', 'orange'),
    col7 text,
    col8 timestamp,
    col9 blob,
    col10 char,
    unique index(col8, col10)
);
insert into table01 (col1, col2, col3, col4, col5, col6, col7, col8, col9, col10) values
('Value2', 456, 78.90, '2023-10-24', false, 'banana', 'Another text', '2022-01-01 01:01:01.000', 'More binary data', 'D'),
('Value3', 789, 12.34, '2023-10-25', true, 'orange', 'Yet another text', '1979-01-01 01:01:01.123', 'Even more binary data', 'E');
-- @bvt:issue#14970
create table test.table02 as select * from table01;
show create table table02;
select * from table02;
insert into table02 values (12, 'Value1', 123, 45.67, '2023-10-23', TRUE, 'apple', 'This is a text', '2019-01-01 01:01:01.000', 'Some binary data', 'C');
select * from table02;
update table02 set col1 = 'newvalue' where col2 = 456;
delete from table02 where col10 = 'D';
select * from table02;
alter table table02 add column newcolumn int after col3, drop column col4;
show create table table02;
alter table table02 modify column newcolumn bigint;
desc table02;
select * from table02;
truncate table02;
select * from table02;
drop table table02;
-- @bvt:issue
drop table table01;

-- cras combines with join
drop table if exists students;
create table students (student_id int primary key , student_name varchar(20), student_age int);
insert into students values (1, 'Alice', 20);
insert into students values (2, 'Bob', 22);
insert into students values (3, 'Charlie', 21);
insert into students values (4, 'Dave', 23);

drop table if exists courses;
create table courses (course_id int, course_name varchar(10));
insert into courses values (101, 'Math'), (102, 'English'), (103, 'History'), (104, 'Science');

drop table if exists enrollments;
create table enrollments (student_id int, course_id int);
insert into enrollments values (1, 101), (1, 103), (2, 102), (3, 101), (3, 102), (3, 103), (4, 104);

drop table if exists c_enrollments;
create table student_course_enrollments as
select
    s.student_id,
    s.student_name,
    s.student_age,
    c.course_name
from
    students s
left join
    enrollments e ON s.student_id = e.student_id
left join
    courses c ON e.course_id = c.course_id;
select * from student_course_enrollments;
-- @bvt:issue#14792
show create table student_course_enrollments;
-- @bvt:issue

drop table if exists student_course_enrollments_inner;
create table student_course_enrollments_inner AS
select
    s.student_id,
    s.student_name,
    s.student_age,
    c.course_name
from
    students s
inner join
    enrollments e on s.student_id = e.student_id
inner join
    courses c on e.course_id = c.course_id;
-- @bvt:issue#14792
show create table student_course_enrollments_inner;
-- @bvt:issue
select * from student_course_enrollments;

drop table if exists student_course_enrollments_right;
create table test.student_course_enrollments_right AS
select
    s.student_id,
    s.student_name,
    s.student_age,
    c.course_name
from
    students s
right join
    enrollments e on s.student_id = e.student_id
right join
    courses c on e.course_id = c.course_id;
-- @bvt:issue#14792
show create table student_course_enrollments_right;
-- @bvt:issue
select * from student_course_enrollments_right;

drop table if exists student_course_enrollments_full;
create table student_course_enrollments_full AS
select
    s.student_id,
    s.student_name,
    s.student_age,
    c.course_name
from
    students s
right join
    enrollments e on s.student_id = e.student_id
right join
    courses c on e.course_id = c.course_id;
-- @bvt:issue#14792
show create table student_course_enrollments_full;
-- @bvt:issue
select * from student_course_enrollments_full;

drop table if exists outerjoin01;
create table outerjoin01 (col1 int, col2 char(3));
insert into outerjoin01 values(10,'aaa'), (10,null), (10,'bbb'), (20,'zzz');
drop table if exists outerjoin02;
create table outerjoin02(a1 char(3), a2 int, a3 real);
insert into outerjoin02 values('AAA', 10, 0.5);
insert into outerjoin02 values('BBB', 20, 1.0);

drop table if exists oj01;
create table oj01 as select outerjoin01.col1, outerjoin01.col2, outerjoin02.a2 from outerjoin01 left outer join outerjoin02 on outerjoin01.col1=10 limit 3;
show create table oj01;
select * from oj01;

drop table if exists oj02;
create table oj02 as select outerjoin01.col1, outerjoin01.col2, outerjoin02.a2 from outerjoin01 natural join outerjoin02 order by col1 desc;
show create table oj02;
select * from oj02;
drop table oj01;
drop table oj02;
drop table outerjoin01;
drop table outerjoin02;
drop table student_course_enrollments;
drop table student_course_enrollments_full;
drop table student_course_enrollments_inner;
drop table student_course_enrollments_right;

-- subquery
drop table if exists employees;
create table employees (col1 int, col2 bigint);
insert into employees values (1, 50000), (2, 60000), (3, 55000), (4, 70000);
drop table if exists sal;
create table sal as
select
    col1,
    col2,
    col2 * 0.1 as bonus
from
    employees;
select * from sal;

drop table if exists sal;
create table test.sal as
select
    col1,
    col2,
    (select col2 * 0.1 from employees e2 where e2.col1 = e1.col1) as bonus
from
    employees e1;
select * from sal;

drop table if exists sal;
create table sal as
    select
        col1,
        col2,
        (select col2 from employees where col2 = 60000)
    from employees;
select * from sal;
drop table employees;
drop table sal;

-- derived tables have column restrictions
drop table if exists test01;
create table test01 (col1 int, col2 decimal, col3 varchar(50));
insert into test01 values (1, 3242434.423, '3224332r32r');
insert into test01 values (2, 39304.3424, '343234343213124');
insert into test01 values (3, 372.324, '00');

drop table if exists test02;
create table test02 (col1 int primary key ) as select col1 from test01;
show create table test02;
desc test02;
insert into test02 values (2);

drop table if exists test03;
create table test03 (col2 decimal unique key) as select col2 from test01;
show create table test03;
desc test03;
insert into test03 values (372.324);

drop table if exists test04;
create table test04 (col1 int, col2 varchar(50), key(col1, col2)) as select col1, col3 from test01;
show create table test04;
select * from test04;

drop table if exists test05;
create table test05 (col1 int, col2 decimal, primary key (col1, col2)) as select col1, col2 from test01;
show create table test05;
select * from test05;
-- @pattern
insert into test05 values (2, 39304.3424);

alter table test01 rename column col1 to newCol;
-- @bvt:issue#14955
show create table test01;
-- @bvt:issue

drop table if exists test06;
create table test06 (col1 int not null default 100) as select col1 from test01;
create table test06 (col1 int not null default 100) as select newcol from test01;
-- @bvt:issue#14792
show create table test06;
-- @bvt:issue
select * from test06;

drop table test01;
drop table test02;
drop table test03;
drop table test04;
drop table test05;
drop table test06;

-- ctas in prepare statement
drop table if exists prepare01;
create table prepare01(col1 int primary key , col2 char);
insert into prepare01 values (1,'a'),(2,'b'),(3,'c');
show create table prepare01;
show columns from prepare01;
drop table if exists prepare02;
prepare s1 from 'create table prepare02 as select * from prepare01';
execute s1;
show create table prepare02;
select * from prepare02;
drop table if exists prepare03;
prepare s2 from 'create table prepare03(col1 int, col2 char, col3 char) as select col1, col2 from prepare01';
execute s2;
select * from prepare03;
show create table prepare03;
drop table prepare01;
drop table prepare02;
drop table prepare03;

-- cras temporary table
drop table if exists orders;
create table orders (order_id int primary key , customer_id int, order_date date, total_amount decimal);
insert into orders values (1, 101, '2023-01-01', 100.00),
                          (2, 101, '2023-01-05', 150.00),
                          (3, 102, '2023-01-02', 200.00),
                          (4, 103, '2023-01-03', 50.00),
                          (5, 101, '2023-01-04', 75.00),
                          (6, 104, '2023-01-06', 300.00),
                          (7, 104, '2023-01-07', 200.00),
                          (8, 105, '2023-01-08', 100.00);
select * from orders;

drop table if exists customer_totals;
create temporary table customer_totals as select customer_id, count(order_id) as total_orders, sum(total_amount) as total_amount from orders group by customer_id having count(order_id) > 1 and sum(total_amount) > 150.0;
-- @bvt:issue#14792
show create table customer_totals;
-- @bvt:issue
select * from customer_totals;

drop table if exists max_totals;
create temporary table max_totals as select customer_id, total_orders from customer_totals order by total_orders desc limit 1;
-- @bvt:issue#14792
desc max_totals;
-- @bvt:issue
select * from max_totals;

drop table if exists max_customer;
create temporary table max_customer as select customer_id, total_amount from customer_totals order by total_amount asc limit 1;
show create table max_customer;
select * from max_customer;

drop table orders;
drop table customer_totals;
drop table max_totals;
drop table max_customer;

-- abnormal test, column is not exists in origin table
drop table if exists table10;
drop table if exists table11;
create table table10 as select col100 from table07;
create table table11 (col20 decimal, col30 char, col40 varchar) as select col100 from table07;
drop table table07;
drop table table08;
drop table table09;
drop table table12;

-- abnormal test: null column to not null column
drop table if exists abnormal01;
create table abnormal01 (col1 int default null );
insert into abnormal01 values (1), (null);
drop table if exists abnormal02;
create table test.abnormal02 (col1 int not null) as select col1 from abnormal01;
drop table abnormal01;

-- abnormal test: normal column to pk column
drop table if exists abnormal03;
create table abnormal03 (col1 int, col2 bigint);
insert into abnormal03 values (1, 8324824234);
insert into abnormal03 values (1, 8324824234);
select * from abnormal03;
drop table if exists abnormal04;
drop table if exists abnormal05;
create table abnormal04 (col1 int primary key ) as select col1 from abnormal03;
create table abnormal05 (col2 bigint unique key) as select col2 from abnormal03;
drop table abnormal03;

-- abnormal test: data out of range
drop table if exists abnormal06;
create table abnormal06 (col1 bigint, col2 decimal);
insert into abnormal06 values (271928310313092, 32984832.3214214);
drop table if exists abnormal07;
create table abnormal07 (col1 int) as select col1 from abnormal06;
drop table abnormal06;

-- abnormal test: count of column is not the same
drop table if exists abnormal07;
create table abnormal07 (col1 int, col2 bigint, col3 decimal, col4 char);
insert into abnormal07 values (1, 2, 3, 'a');
insert into abnormal07 values (1, 2, 3, 'b');
insert into abnormal07 values (1, 2, 3, 'c');
insert into abnormal07 values (1, 2, 3, 'd');
insert into abnormal07 values (null, null, null, null);

drop table if exists abnormal08;
create table abnormal08 as select col1, col2, col3, col4, col5 from abnormal07;
create table abnormal07 as select * from abnormal07;
drop table abnormal07;

-- the inserted data violates the constraints of the new table
drop table if exists abnormal09;
create table abnormal09 (col1 int, col2 decimal);
insert into abnormal09 values (1, 2);
insert into abnormal09 values (1, 2);
drop table if exists abnormal10;
create table abnormal10(col1 int primary key) as select col1 from abnormal09;
create table abnormal10(col2 decimal unique key) as select col2 from abnormal09;
drop table abnormal09;

-- perform CTAS operations on the metadata table
drop table if exists abnormal10;
create table abnormal10 as select * from mo_catalog.mo_columns;

-- combines with window function
drop table if exists time_window01;
create table time_window01 (ts timestamp primary key , col2 int);
insert into time_window01 values ('2021-01-12 00:00:00.000', 12);
insert into time_window01 values ('2020-01-12 12:00:12.000', 24);
insert into time_window01 values ('2023-01-12 00:00:00.000', 34);
insert into time_window01 values ('2024-01-12 12:00:12.000', 20);
select * from time_window01;
drop table if exists time_window02;
create table time_window02 as select _wstart, _wend, max(col2), min(col2) from time_window01 where ts > '2020-01-11 12:00:12.000' and ts < '2021-01-13 00:00:00.000' interval(ts, 100, day) fill(prev);
select * from time_window02;
drop table time_window01;
drop table time_window02;

drop table if exists time_window03;
create table time_window03 (ts timestamp primary key , col2 bool);
insert into time_window03 values ('2023-10-26 10:00:00.000', false);
insert into time_window03 values ('2023-10-26 10:10:00.000', true);
insert into time_window03 values ('2023-10-26 10:20:00.000', null);
insert into time_window03 values ('2023-10-26 10:30:00.000', true);
select * from time_window03;
drop table if exists time_window04;
create table time_window04 as select _wstart, _wend, max(col2), min(col2) from time_window03 where ts > '2020-01-11 12:00:12.000' and ts < '2024-01-13 00:00:00.000' interval(ts, 10, second) fill(prev);
select * from time_window04;
select * from time_window03;
drop table time_window03;
drop table time_window04;

drop table if exists test.window01;
create table window01 (user_id integer not null, date date);
insert into window01 values (1, '2002-06-09');
insert into window01 values (2, '2002-06-09');
insert into window01 values (1, '2002-06-09');
insert into window01 values (3, '2002-06-09');
insert into window01 values (4, '2002-06-09');
insert into window01 values (4, '2002-06-09');
insert into window01 values (5, '2002-06-09');
drop table if exists window02;
create table window02 as select rank() over () r from window01;
select * from window02;
drop table if exists window03;
create table window03 as select dense_rank() over () r from window01;
select * from window03;
drop table window01;
drop table window02;
drop table window03;

drop table if exists row01;
create table row01(i int,j int);
insert into row01 values(1,1);
insert into row01 values(1,4);
insert into row01 values(1,2);
insert into row01 values(1,4);
drop table if exists row02;
create table row02 as select i, j, sum(i+j) over (order by j rows between 2 preceding and 1 preceding) foo from row01 order by foo desc;
select * from row02;
drop table if exists row03;
create table row03 as select i, j, sum(i+j) over (order by j rows between 2 following and 1 following) foo from row01 order by foo desc;
select * from row03;
drop table row01;
drop table row02;
drop table row03;

drop table if exists dense_rank01;
create table dense_rank01 (id integer, sex char(1));
insert into dense_rank01 values (1, 'm');
insert into dense_rank01 values (2, 'f');
insert into dense_rank01 values (3, 'f');
insert into dense_rank01 values (4, 'f');
insert into dense_rank01 values (5, 'm');
drop table if exists dense_rank02;
create table dense_rank02 as select sex, id, rank() over (partition by sex order by id desc) from dense_rank01;
select * from dense_rank02;
drop table if exists dense_rank03;
create table dense_rank03 as select sex, id, dense_rank() over (partition by sex order by id desc) from dense_rank01;
select * from dense_rank03;
drop table dense_rank01;
drop table dense_rank02;
drop table dense_rank03;

-- combine with pub-sub table
drop table if exists test01;
create table test01(
col1 tinyint,
col2 smallint,
col3 int,
col4 bigint,
col5 tinyint unsigned,
col6 smallint unsigned,
col7 int unsigned,
col8 bigint unsigned,
col9 float,
col10 double
);

insert into test01 values (1,2,3,4,5,6,7,8,10.2131,3824.34324);
insert into test01 values (2,3,4,5,6,7,8,9,2131.3242343,-3824.34324);
show create table test01;
create publication publication01 database test;
-- @ignore:2,3
show publications;
drop table if exists test02;
create table test02 as select * from test01;
select * from test02;

drop publication publication01;
drop table test01;

drop account if exists acc0;
create account acc0 admin_name 'root' identified by '111';
drop table if exists sys_tbl_1;
create table sys_tbl_1(a int primary key, b decimal, c char, d varchar(20) );
insert into sys_tbl_1 values(1,2,'a','database'),(2,3,'b','test publication'),(3, 4, 'c','324243243');
create publication sys_pub_1 database test;
select * from sys_tbl_1;
-- @ignore:2,3
show publications;
select pub_name, database_name, account_list from mo_catalog.mo_pubs;
-- @session:id=2&user=acc0:root&password=111
create database sub1 from sys publication sys_pub_1;
show databases;
use sub1;
drop table if exists test;
create table test as select * from sys_tbl_1;
-- @session

-- @session:id=3&user=acc0:root&password=111
drop database sub1;
-- @session
drop account acc0;
drop publication sys_pub_1;

-- alias
-- @bvt:issue#14955
drop table if exists alias01;
create table alias01 (col1 int, col2 decimal);
insert into alias01 values (1,2);
insert into alias01 values (2,3);
drop table if exists alias02;
create table alias02 (NewCol int) as select * from alias01;
show create table alias02;
select * from alias02;
drop table alias01;
-- @bvt:issue
drop database test;

-- privilege
drop database if exists db1;
create database db1;
use db1;
drop role if exists role_r1;
drop user if exists role_u1;
create role role_r1;
create user role_u1 identified by '111' default role role_r1;
drop table if exists t1;
create table t1(col1 int);
insert into t1 values(1);
insert into t1 values(2);
grant create database, drop database on account * to role_r1;
grant show databases on account * to role_r1;
grant connect on account * to role_r1;
grant create table, drop table on database *.* to role_r1;
grant show tables on database * to role_r1;
-- @session:id=4&user=sys:role_u1:role_r1&password=111
drop table if exists t2;
create table t2 as select * from t1;
-- @session
grant select on table * to role_r1;
grant insert on table * to role_r1;
-- @session:id=5&user=sys:role_u1:role_r1&password=111
drop table if exists t2;
create table t2 as select * from t1;
-- @session
drop table t1;
drop table t2;
drop database db1;

drop database if exists db2;
create database db2;
use db2;
drop role if exists role_r1;
drop role if exists role_r2;
drop user if exists role_u1;
drop user if exists role_u2;
create role role_r1;
create user role_u1 identified by '111' default role role_r1;
create role role_r2;
create user role_u2 identified by '111' default role role_r2;
drop table if exists t1;
create table t1(col1 int);
insert into t1 values(1);
insert into t1 values(2);
grant create database, drop database on account * to role_r1;
grant show databases on account * to role_r1;
grant connect on account * to role_r1;
grant create table, drop table on database *.* to role_r1;
grant show tables on database * to role_r1;
grant select on table * to role_r1;
grant insert on table * to role_r1;
-- @session:id=6&user=sys:role_u1:role_r1&password=111
use db2;
drop table if exists t2;
create table t2 as select * from t1;
-- @session
-- @session:id=7&user=sys:role_u2:role_r2&password=111
use db2;
drop table if exists t3;
create table t3 as select * from t2;
select * from t3;
-- @session
grant create database, drop database on account * to role_r2;
grant show databases on account * to role_r2;
grant connect on account * to role_r2;
grant create table, drop table on database *.* to role_r2;
grant show tables on database * to role_r2;
grant select on table * to role_r2;
grant insert on table * to role_r2;
-- @session:id=8&user=sys:role_u2:role_r2&password=111
use db2;
drop table if exists t3;
create table t3 as select * from t2;
select * from t3;
-- @session
drop table t1;
drop table t2;
drop table t3;
drop role role_r1;
drop role role_r2;
drop user role_u1;
drop user role_u2;
drop database db2;

-- privilege
drop role if exists role_r1;
drop user if exists role_u1;
create role role_r1;
create user role_u1 identified by '111' default role role_r1;
grant show databases on account * to role_r1;
grant connect on account * to role_r1;
grant show tables on database * to role_r1;
grant create database, drop database on account * to role_r1;
-- @session:id=9&user=sys:role_u1:role_r1&password=111
drop database if exists db3;
create database db3;
drop database if exists db4;
create database db4;
-- @session
use db3;
grant create table, drop table on database db3 to role_r1;
grant create table, drop table on database db4 to role_r1;
grant select on table * to role_r1;
grant insert on table * to role_r1;
use db4;
grant select on table * to role_r1;
grant insert on table * to role_r1;
-- @session:id=9&user=sys:role_u1:role_r1&password=111
use db3;
drop table if exists t1;
create table t1(col1 int);
insert into t1 values(1);
insert into t1 values(2);
drop database if exists db4;
create database db4;
use db4;
drop table if exists t2;
create table t2 as select * from db3.t1;
use db3;
drop table t1;
-- @session
use db4;
select * from t2;
drop table t2;
drop role role_r1;
drop user role_u1;
drop database db3;
