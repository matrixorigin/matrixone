-- @suite

-- @case
-- @desc: arithmetic and comparison between DECIMAL and BOOL bind TRUE as 1 and FALSE as 0, both operand orders, all storage widths (issue 31181)
-- @label:bvt

drop database if exists decimal_bool_arithmetic;
create database decimal_bool_arithmetic;
use decimal_bool_arithmetic;

-- DECIMAL64 and DECIMAL128 columns; row 3 pins NULL propagation
create table td(id int primary key, d decimal(10,2), d2 decimal(30,4));
insert into td values (1,1.50,1.50),(2,0,0),(3,null,null);

-- arithmetic, TRUE side
select id, d + true, true + d, d - true, true - d, d * true, true * d from td order by id;
select id, d / true, true / d, d % true, d div true, d div false from td order by id;

-- arithmetic, FALSE side; division/modulo by FALSE stays NULL
select id, d + false, false + d, d - false, d * false, false * d, d / false, d % false, false / d from td order by id;

-- comparisons with TRUE and FALSE, both operand orders
select id, d = true, d <> true, d > true, d < true, d >= true, d <= true from td order by id;
select id, d = false, d <> false, d > false, d < false, d >= false, d <= false from td order by id;
select id, true = d, true <> d, true > d, true < d, true >= d, true <= d from td order by id;

-- DECIMAL128 column, both operators
select id, d2 + true, d2 * false, d2 = true, d2 > false, d2 div true from td order by id;

-- DECIMAL256 through an explicit typed operand
select cast(1.5 as decimal(60,4)) + true, true + cast(1.5 as decimal(60,4)), cast(1.5 as decimal(60,4)) > true;
select cast(2.5 as decimal(60,4)) / true, false % cast(2.5 as decimal(60,4));

-- literals and explicit casts
select 1.5 + true, true + 1.5, 1.5 = true, 0.0 = false, 1.5 div true;
select cast(true as decimal(10,2)), cast(false as decimal(10,2)), cast(true as decimal(30,4)), cast(true as decimal(60,4));
-- explicit cast clamps out of range like the integer-to-decimal cast
select cast(true as decimal(1,1)), cast(false as decimal(1,1));

-- boolean operand in the where clause
select count(*) from td where d > true;
select count(*) from td where d = false;
select count(*) from td where d >= true and d2 < true;

-- high-scale arithmetic widens to DECIMAL256 with a BOOL operand exactly as
-- with an integer: 0.99e38 + 1e38 does not fit the signed Decimal128 domain
select cast('0.99' as decimal(38,38)) + true, true + cast('0.99' as decimal(38,38));
select cast('0.99' as decimal(38,38)) - true, true - cast('0.99' as decimal(38,38));
select cast('0.99' as decimal(38,38)) * true;

-- division derives BOOL's exact 1/0 precision: a wide quotient promotes like
-- with an integer, and div by FALSE stays NULL
select cast('1e37' as decimal(38,0)) / true, cast('1e37' as decimal(38,0)) / false;

-- nondefault div_precision_increment applies to BOOL operands in both orders
set session div_precision_increment = 10;
select true / cast(3 as decimal(10,2)), cast(1.50 as decimal(10,2)) / true;
set session div_precision_increment = 4;

-- assignment into a DECIMAL column stores BOOL exactly like the integer 1/0:
-- FALSE and NULL fit any declared width
create table tb(id int primary key, d decimal(2,1));
insert into tb values (1,true),(2,false),(3,1),(4,0),(5,null);
select id, d from tb order by id;
insert into tb select 6, true;
select id, d from tb order by id;
update tb set d = true where id = 4;
select id, d from tb order by id;
drop table tb;

-- declared-precision enforcement (#30639) applies to TRUE exactly like the
-- integer 1: DECIMAL(p,p) holds at most 0.99..9, so both are rejected
create table ta(id int primary key, d decimal(1,1));
insert into ta values (1,false),(2,0),(3,null);
select id, d from ta order by id;
insert into ta values (4,true);
insert into ta values (4,1);
-- the failing statements stored nothing (failure atomicity)
select id, d from ta order by id;
insert into ta values (4,true),(5,false);
select count(*) from ta;
-- INSERT SELECT and UPDATE share the same admission cast
insert into ta select 4, true from td limit 1;
update ta set d = true where id = 1;
insert into ta values (1,false) on duplicate key update d = true;
select id, d from ta order by id;
drop table ta;

-- the same contract binds across the DECIMAL storage widths
create table ta128(id int primary key, d decimal(19,19));
create table ta256(id int primary key, d decimal(40,40));
insert into ta128 values (1,true);
insert into ta256 values (1,true);
insert into ta128 values (1,false),(2,0),(3,null);
insert into ta256 values (1,false),(2,0),(3,null);
select id, d from ta128 order by id;
select id, d from ta256 order by id;
drop table ta128;
drop table ta256;

-- insert/update of BOOL values into DECIMAL columns uses the same cast
create table ti(id int primary key, d decimal(10,2));
insert into ti values (1,true),(2,false),(3,null);
select id, d from ti order by id;
update ti set d = true where id = 2;
select id, d from ti order by id;
insert into ti select 4, d + true from ti where id = 1;
select id, d from ti order by id;

drop database decimal_bool_arithmetic;
