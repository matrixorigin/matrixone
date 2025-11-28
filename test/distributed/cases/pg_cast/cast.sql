select 1.2::int;
select 1.2::integer;
select 3.7::bigint;
select 3.7::smallint;
select 3.7::tinyint;
select 123.456::float;
select 123.456::double;
select 123.456::decimal;
select 123.456::numeric;

-- String casts
select 123::varchar;
select 123::char(10);
select 123::text;
select '123'::int;
select '123.45'::decimal(10,2);
select '2023-01-01'::date;
select '2023-01-01 12:00:00'::datetime;
select '2023-01-01 12:00:00'::timestamp;
select '12:00:00'::time;

-- Boolean casts
select 1::bool;
select 0::bool;
select 'true'::bool;
select 'false'::bool;

-- Unsigned casts
select 123::unsigned;
select 123::unsigned integer;
select 123::signed;
select 123::signed integer;

-- Complex expressions with cast
select (1.2 + 3.4)::int;
select (10 / 3)::decimal(10,2);
select abs(-123.45)::int;
select round(123.456)::int;

-- Cast in WHERE clause
drop table if exists t1;
create table t1 (a decimal(10,2), b varchar(20));
insert into t1 values (123.45, '456'), (789.12, '999');
select * from t1 where a::int > 100;
select * from t1 where b::int < 500;
drop table t1;

-- Cast in SELECT with alias
select 123.456::int as int_val;
select '2023-01-01'::date as date_val;
select 1::bool as bool_val;

-- Cast in expressions
select 1.2::int + 3.4::int;
select '123'::int * 2;
select (1.5::int)::varchar;

-- Cast with NULL
select null::int;
select null::varchar;
select null::date;

-- Cast in function calls
select cast(123.456::int as varchar);
select length(123::varchar);

-- Multiple casts in one query
select 1.2::int, 3.4::decimal(10,2), '123'::int, true::int;

-- Cast in ORDER BY
drop table if exists t1;
create table t1 (a varchar(10));
insert into t1 values ('100'), ('200'), ('50');
select * from t1 order by a::int;
drop table t1;

-- Cast in GROUP BY
drop table if exists t1;
create table t1 (a decimal(5,2));
insert into t1 values (1.2), (1.2), (3.4), (3.4);
select a::int, count(*) from t1 group by a::int;
drop table t1;

-- Cast with arithmetic operations
select (1.2::int) + (3.4::int);
select ('100'::int) * 2;
select (123.456::decimal(10,2)) / 2;

-- Cast in CASE expression
select case when 1.5::int = 1 then 'truncated' else 'not truncated' end;
select case 1.5::int when 1 then 'one' when 2 then 'two' else 'other' end;

-- Cast with JSON
select '{"key":"value"}'::json;

-- Cast with UUID
select '550e8400-e29b-41d4-a716-446655440000'::uuid;

-- Nested casts
select (1.2::int)::varchar;
select ('123'::int)::decimal(10,2);

-- Cast in subquery
drop table if exists t1;
create table t1 (a decimal(10,2));
insert into t1 values (123.45), (678.90);
select * from (select a::int as b from t1) t where b > 100;
drop table t1;

-- Cast in JOIN
drop table if exists t1;
drop table if exists t2;
create table t1 (a varchar(10));
create table t2 (b int);
insert into t1 values ('100'), ('200');
insert into t2 values (100), (200);
select * from t1 join t2 on t1.a::int = t2.b;
drop table t1;
drop table t2;

-- Cast with precision
select 123.456789::decimal(10,2);
select 123.456789::decimal(10,4);
select '123.456789'::decimal(10,2);

-- Cast date/time types
select '2023-01-01'::date;
select '2023-01-01 12:00:00'::datetime;
select '2023-01-01 12:00:00'::timestamp;
select '12:00:00'::time;

-- Cast in INSERT
drop table if exists t1;
create table t1 (a int, b varchar(20), c decimal(10,2));
insert into t1 values (123.45::int, 456::varchar, '789.12'::decimal(10,2));
select * from t1;
drop table t1;

-- Cast in UPDATE
drop table if exists t1;
create table t1 (a int, b varchar(20));
insert into t1 values (100, '200');
update t1 set a = '300'::int where b::int > 150;
select * from t1;
drop table t1;

-- Cast with comparison operators
select 1.5::int = 1;
select 1.5::int != 2;
select 1.5::int < 2;
select 1.5::int > 0;
select 1.5::int <= 2;
select 1.5::int >= 1;

-- Cast in UNION
select 1.2::int union select 3.4::int;
select '123'::int union select '456'::int;

-- Cast with aggregate functions
drop table if exists t1;
create table t1 (a decimal(10,2));
insert into t1 values (1.2), (3.4), (5.6);
select sum(a::int) from t1;
select avg(a::int) from t1;
select max(a::int) from t1;
select min(a::int) from t1;
select count(a::int) from t1;
drop table t1;

