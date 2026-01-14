-- @suite
-- @case
-- @desc:test for year datatype
-- @label:bvt

-- Test cases of DDL
drop table if exists t1;
create table t1 (a year not null, primary key(a));
desc t1;
drop table t1;

-- YEAR(4) syntax (deprecated but supported for compatibility)
create table t1 (a year(4) not null);
desc t1;
drop table t1;

-- Test cases of INSERT with various formats
drop table if exists t_year;
create table t_year (id int, y year);

-- 4-digit year values
insert into t_year values (1, 2024);
insert into t_year values (2, 1901);
insert into t_year values (3, 2155);
insert into t_year values (4, 1970);
insert into t_year values (5, 2000);

-- 4-digit string year values
insert into t_year values (10, '2024');
insert into t_year values (11, '1901');
insert into t_year values (12, '2155');

-- 2-digit string year values
-- '0'-'69' -> 2000-2069
insert into t_year values (20, '0');
insert into t_year values (21, '00');
insert into t_year values (22, '24');
insert into t_year values (23, '69');
-- '70'-'99' -> 1970-1999
insert into t_year values (24, '70');
insert into t_year values (25, '99');

-- 2-digit numeric year values
-- 1-69 -> 2001-2069
insert into t_year values (30, 1);
insert into t_year values (31, 24);
insert into t_year values (32, 69);
-- 70-99 -> 1970-1999
insert into t_year values (33, 70);
insert into t_year values (34, 99);

-- Special value 0 -> 0000
insert into t_year values (40, 0);

-- NULL value
insert into t_year values (50, NULL);

select * from t_year order by id;

-- Test cases of SELECT and display format
select y from t_year where id = 1;
select y from t_year where id = 40;
select y from t_year where id = 50;

-- Test cases of CAST operations
-- YEAR to INT
select cast(y as signed) from t_year where id = 1;
select cast(y as signed) from t_year where id = 40;
select cast(y as unsigned) from t_year where id = 1;

-- INT to YEAR
select cast(2024 as year);
select cast(0 as year);
select cast(24 as year);
select cast(70 as year);

-- YEAR to VARCHAR
select cast(y as char(4)) from t_year where id = 1;
select cast(y as char(4)) from t_year where id = 40;
select cast(y as varchar(10)) from t_year where id = 1;

-- VARCHAR to YEAR
select cast('2024' as year);
select cast('0' as year);
select cast('00' as year);
select cast('24' as year);
select cast('70' as year);

-- DATE to YEAR conversion
select cast(cast('2024-06-15' as date) as year);
select cast(cast('1999-12-31' as date) as year);

-- DATETIME to YEAR conversion
select cast(cast('2024-06-15 10:30:00' as datetime) as year);

-- TIMESTAMP to YEAR conversion
select cast(cast('2024-06-15 10:30:00' as timestamp) as year);

-- Test cases of comparison operators
select * from t_year where y = 2024 order by id;
select * from t_year where y = '2024' order by id;
select * from t_year where y < 2000 order by id;
select * from t_year where y > 2000 order by id;
select * from t_year where y <= 1970 order by id;
select * from t_year where y >= 2024 order by id;
select * from t_year where y between 1970 and 2000 order by id;
select * from t_year where y in (2024, 1901, 2155) order by id;
select * from t_year where y is null;
select * from t_year where y is not null order by id;

-- Test cases of ORDER BY
select * from t_year where y is not null order by y asc;
select * from t_year where y is not null order by y desc;

-- Test cases of aggregate functions
select min(y), max(y) from t_year where y is not null and y > 0;
select count(y) from t_year;
select count(*) from t_year where y is null;

-- Test cases of UPDATE
update t_year set y = 2025 where id = 1;
select * from t_year where id = 1;
update t_year set y = '75' where id = 2;
select * from t_year where id = 2;

-- Test cases of DELETE
delete from t_year where y = 0;
select * from t_year where id = 40;

-- Test boundary values
select cast(1901 as year);
select cast(2155 as year);

-- Test with primary key
drop table if exists t_year_pk;
create table t_year_pk (y year primary key, name varchar(50));
insert into t_year_pk values (2020, 'year2020');
insert into t_year_pk values (2021, 'year2021');
insert into t_year_pk values (2022, 'year2022');
select * from t_year_pk order by y;
drop table t_year_pk;

-- Test with default value
drop table if exists t_year_default;
create table t_year_default (id int, y year default 2024);
insert into t_year_default (id) values (1);
insert into t_year_default values (2, 2023);
select * from t_year_default order by id;
drop table t_year_default;

-- Test join with year columns
drop table if exists t1;
drop table if exists t2;
create table t1 (y year, val int);
create table t2 (y year, name varchar(20));
insert into t1 values (2020, 1), (2021, 2), (2022, 3);
insert into t2 values (2020, 'a'), (2021, 'b'), (2023, 'c');
select t1.y, t1.val, t2.name from t1 join t2 on t1.y = t2.y order by t1.y;
drop table t1;
drop table t2;

-- =====================================================
-- Additional test cases: boundary and edge cases
-- =====================================================

-- Test boundary values more thoroughly
drop table if exists t_year_boundary;
create table t_year_boundary (id int, y year);

-- Minimum valid year
insert into t_year_boundary values (1, 1901);
-- Maximum valid year
insert into t_year_boundary values (2, 2155);
-- Zero value (special case)
insert into t_year_boundary values (3, 0);
insert into t_year_boundary values (4, '0000');

select * from t_year_boundary order by id;
drop table t_year_boundary;

-- =====================================================
-- Test cases for invalid/error inputs
-- =====================================================

-- Out of range values (should fail)
select cast(1900 as year);
select cast(2156 as year);
select cast(-1 as year);
select cast(10000 as year);

-- Invalid string values (should fail)
select cast('abc' as year);
select cast('20.24' as year);
select cast('2024.5' as year);

-- =====================================================
-- Test cases for arithmetic operations
-- =====================================================
drop table if exists t_year_arith;
create table t_year_arith (y year);
insert into t_year_arith values (2020), (2021), (2022);

-- YEAR + integer
select y, y + 1 from t_year_arith order by y;
select y, y + 10 from t_year_arith order by y;

-- YEAR - integer
select y, y - 1 from t_year_arith order by y;
select y, y - 20 from t_year_arith order by y;

-- YEAR * integer
select y, y * 2 from t_year_arith order by y;

-- YEAR / integer
select y, y / 2 from t_year_arith order by y;
select y, y / 4 from t_year_arith order by y;

-- YEAR - YEAR (difference in years)
select max(y) - min(y) as year_diff from t_year_arith;

drop table t_year_arith;

-- =====================================================
-- Test cases for GROUP BY and DISTINCT
-- =====================================================
drop table if exists t_year_group;
create table t_year_group (id int, y year, category varchar(10));
insert into t_year_group values (1, 2020, 'A');
insert into t_year_group values (2, 2020, 'B');
insert into t_year_group values (3, 2021, 'A');
insert into t_year_group values (4, 2021, 'B');
insert into t_year_group values (5, 2022, 'A');

-- GROUP BY year
select y, count(*) as cnt from t_year_group group by y order by y;

-- DISTINCT year
select distinct y from t_year_group order by y;

-- GROUP BY with HAVING
select y, count(*) as cnt from t_year_group group by y having count(*) > 1 order by y;

drop table t_year_group;

-- =====================================================
-- Test cases for subqueries with YEAR
-- =====================================================
drop table if exists t_year_sub;
create table t_year_sub (id int, y year);
insert into t_year_sub values (1, 2020), (2, 2021), (3, 2022), (4, 2023);

-- Subquery in WHERE
select * from t_year_sub where y = (select max(y) from t_year_sub);
select * from t_year_sub where y in (select y from t_year_sub where y > 2021) order by id;

-- Subquery in FROM
select * from (select y, count(*) as cnt from t_year_sub group by y) as sub order by y;

drop table t_year_sub;

-- =====================================================
-- Test cases for UNION with YEAR
-- =====================================================
drop table if exists t_year_u1;
drop table if exists t_year_u2;
create table t_year_u1 (y year);
create table t_year_u2 (y year);
insert into t_year_u1 values (2020), (2021);
insert into t_year_u2 values (2021), (2022);

-- UNION (removes duplicates)
select y from t_year_u1 union select y from t_year_u2 order by y;

-- UNION ALL (keeps duplicates)
select y from t_year_u1 union all select y from t_year_u2 order by y;

drop table t_year_u1;
drop table t_year_u2;

-- =====================================================
-- Test cases for INDEX on YEAR column
-- =====================================================
drop table if exists t_year_idx;
create table t_year_idx (id int, y year, index idx_year(y));
insert into t_year_idx values (1, 2020), (2, 2021), (3, 2022), (4, 2020);

select * from t_year_idx where y = 2020 order by id;
select * from t_year_idx where y between 2020 and 2021 order by id;

drop table t_year_idx;

-- =====================================================
-- Test cases for UNIQUE constraint on YEAR
-- =====================================================
drop table if exists t_year_uniq;
create table t_year_uniq (y year unique, name varchar(20));
insert into t_year_uniq values (2020, 'first');
insert into t_year_uniq values (2021, 'second');

-- This should fail due to unique constraint
-- @bvt:issue#23408
insert into t_year_uniq values (2020, 'duplicate');
-- @bvt:issue#23408

select * from t_year_uniq order by y;
drop table t_year_uniq;

-- =====================================================
-- Test cases for NOT NULL constraint
-- =====================================================
drop table if exists t_year_notnull;
create table t_year_notnull (id int, y year not null);
insert into t_year_notnull values (1, 2024);

-- This should fail due to NOT NULL constraint
insert into t_year_notnull values (2, null);

select * from t_year_notnull;
drop table t_year_notnull;

-- =====================================================
-- Test cases for CHECK constraint (if supported)
-- =====================================================
-- drop table if exists t_year_check;
-- create table t_year_check (y year check (y >= 2000));

-- =====================================================
-- Test cases for 2-digit year edge cases
-- =====================================================
drop table if exists t_year_2digit;
create table t_year_2digit (id int, y year);

-- Edge case: '69' -> 2069, '70' -> 1970
insert into t_year_2digit values (1, '69');
insert into t_year_2digit values (2, '70');

-- Numeric 0 vs string '0' vs string '00'
insert into t_year_2digit values (3, 0);
insert into t_year_2digit values (4, '0');
insert into t_year_2digit values (5, '00');

-- Single digit strings
insert into t_year_2digit values (6, '1');
insert into t_year_2digit values (7, '9');

select * from t_year_2digit order by id;
drop table t_year_2digit;

-- =====================================================
-- Test cases for COALESCE and IFNULL with YEAR
-- =====================================================
drop table if exists t_year_null;
create table t_year_null (id int, y year);
insert into t_year_null values (1, 2024);
insert into t_year_null values (2, null);

select id, coalesce(y, 1999) as y_val from t_year_null order by id;
select id, ifnull(y, 2000) as y_val from t_year_null order by id;

drop table t_year_null;

-- =====================================================
-- Test cases for CASE WHEN with YEAR
-- =====================================================
drop table if exists t_year_case;
create table t_year_case (id int, y year);
insert into t_year_case values (1, 1990), (2, 2000), (3, 2010), (4, 2020);

select id, y,
  case
    when y < 2000 then '20th century'
    when y >= 2000 and y < 2010 then '2000s'
    when y >= 2010 and y < 2020 then '2010s'
    else '2020s'
  end as era
from t_year_case order by id;

drop table t_year_case;

-- =====================================================
-- Test cases for comparison between YEAR and other types
-- =====================================================
drop table if exists t_year_cmp;
create table t_year_cmp (y year);
insert into t_year_cmp values (2024);

-- Compare with integer
select * from t_year_cmp where y = 2024;
select * from t_year_cmp where y > 2000;

-- Compare with string
select * from t_year_cmp where y = '2024';
select * from t_year_cmp where y > '2000';

drop table t_year_cmp;

-- =====================================================
-- Test cases for multiple YEAR columns in same table
-- =====================================================
drop table if exists t_year_multi;
create table t_year_multi (id int, start_year year, end_year year);
insert into t_year_multi values (1, 2020, 2024);
insert into t_year_multi values (2, 2015, 2020);
insert into t_year_multi values (3, 2010, 2015);

-- Compare two year columns
select * from t_year_multi where end_year > start_year order by id;
select id, end_year - start_year as duration from t_year_multi order by id;

drop table t_year_multi;

-- =====================================================
-- Test cases for YEAR with LIKE (should not work, but test behavior)
-- =====================================================
drop table if exists t_year_like;
create table t_year_like (y year);
insert into t_year_like values (2020), (2021), (2022), (2120);

-- LIKE on year (behavior test)
select * from t_year_like where cast(y as char) like '202%' order by y;
select * from t_year_like where cast(y as char) like '21%' order by y;

drop table t_year_like;

-- =====================================================
-- Test cases for YEAR in expressions
-- =====================================================
select 2024 + 0 as year_expr;
select cast(2024 as year) + 0;
select cast(2024 as year) * 1;

-- =====================================================
-- Test cases for ALTER TABLE with YEAR
-- =====================================================
drop table if exists t_year_alter;
create table t_year_alter (id int);
alter table t_year_alter add column y year;
desc t_year_alter;
insert into t_year_alter values (1, 2024);
select * from t_year_alter;

alter table t_year_alter modify column y year not null default 2000;
desc t_year_alter;

drop table t_year_alter;

-- =====================================================
-- Test cases for CREATE TABLE AS SELECT with YEAR
-- =====================================================
drop table if exists t_year_src;
create table t_year_src (y year);
insert into t_year_src values (2020), (2021), (2022);

drop table if exists t_year_copy;
create table t_year_copy as select * from t_year_src;
desc t_year_copy;
select * from t_year_copy order by y;

drop table t_year_src;
drop table t_year_copy;

-- =====================================================
-- Test cases for INSERT ... SELECT with YEAR
-- =====================================================
drop table if exists t_year_src2;
drop table if exists t_year_dst;
create table t_year_src2 (y year);
create table t_year_dst (y year);
insert into t_year_src2 values (2020), (2021);

insert into t_year_dst select * from t_year_src2;
select * from t_year_dst order by y;

drop table t_year_src2;
drop table t_year_dst;

-- =====================================================
-- Test cases for REPLACE with YEAR primary key
-- =====================================================
drop table if exists t_year_replace;
create table t_year_replace (y year primary key, val int);
insert into t_year_replace values (2020, 1);
insert into t_year_replace values (2021, 2);

replace into t_year_replace values (2020, 100);
select * from t_year_replace order by y;

drop table t_year_replace;

-- =====================================================
-- Test cases for ON DUPLICATE KEY UPDATE with YEAR
-- =====================================================
drop table if exists t_year_dup;
create table t_year_dup (y year primary key, val int);
insert into t_year_dup values (2020, 1);

insert into t_year_dup values (2020, 2) on duplicate key update val = val + 10;
select * from t_year_dup;

insert into t_year_dup values (2021, 3) on duplicate key update val = val + 10;
select * from t_year_dup order by y;

drop table t_year_dup;

-- =====================================================
-- Test cases for YEAR() function - extract year from date
-- =====================================================
select year('2024-06-15');
select year('1999-12-31');
select year('2000-01-01');

-- YEAR() from datetime
select year('2024-06-15 10:30:00');
select year(cast('2024-06-15 10:30:00' as datetime));

-- YEAR() with NULL
select year(null);

-- =====================================================
-- Test cases for EXTRACT(YEAR FROM ...)
-- =====================================================
select extract(year from '2024-06-15');
select extract(year from '1999-12-31');

-- =====================================================
-- Test cases for DATE/DATETIME to YEAR conversion
-- =====================================================
drop table if exists t_year_date;
create table t_year_date (id int, d date, y year);

insert into t_year_date values (1, '2024-06-15', 2024);
insert into t_year_date values (2, '1999-12-31', 1999);
insert into t_year_date values (3, '2000-01-01', 2000);

-- Compare YEAR column with YEAR() from date
select * from t_year_date where y = year(d) order by id;

-- Insert YEAR from date extraction
insert into t_year_date values (4, '2023-03-20', year('2023-03-20'));
select * from t_year_date where id = 4;

drop table t_year_date;

-- =====================================================
-- Test cases for LEFT JOIN / RIGHT JOIN with YEAR
-- =====================================================
drop table if exists t_year_left;
drop table if exists t_year_right;
create table t_year_left (y year, val int);
create table t_year_right (y year, name varchar(20));

insert into t_year_left values (2020, 1), (2021, 2), (2022, 3);
insert into t_year_right values (2021, 'a'), (2022, 'b'), (2023, 'c');

-- LEFT JOIN
select l.y, l.val, r.name from t_year_left l left join t_year_right r on l.y = r.y order by l.y;

-- RIGHT JOIN
select l.y, l.val, r.y as ry, r.name from t_year_left l right join t_year_right r on l.y = r.y order by r.y;

drop table t_year_left;
drop table t_year_right;

-- =====================================================
-- Test cases for multi-table UPDATE with YEAR
-- =====================================================
drop table if exists t_year_upd1;
drop table if exists t_year_upd2;
create table t_year_upd1 (y year, val int);
create table t_year_upd2 (y year, multiplier int);

insert into t_year_upd1 values (2020, 10), (2021, 20), (2022, 30);
insert into t_year_upd2 values (2020, 2), (2021, 3);

-- Multi-table update
update t_year_upd1 t1, t_year_upd2 t2 set t1.val = t1.val * t2.multiplier where t1.y = t2.y;
select * from t_year_upd1 order by y;

drop table t_year_upd1;
drop table t_year_upd2;

-- =====================================================
-- Test cases for multi-table DELETE with YEAR
-- =====================================================
drop table if exists t_year_del1;
drop table if exists t_year_del2;
create table t_year_del1 (y year, val int);
create table t_year_del2 (y year);

insert into t_year_del1 values (2020, 1), (2021, 2), (2022, 3);
insert into t_year_del2 values (2020), (2021);

-- Multi-table delete
delete t1 from t_year_del1 t1, t_year_del2 t2 where t1.y = t2.y;
select * from t_year_del1;

drop table t_year_del1;
drop table t_year_del2;

-- =====================================================
-- Test cases for VIEW with YEAR
-- =====================================================
drop table if exists t_year_view_base;
drop view if exists v_year;

create table t_year_view_base (id int, y year, name varchar(20));
insert into t_year_view_base values (1, 2020, 'a'), (2, 2021, 'b'), (3, 2022, 'c');

create view v_year as select id, y, name from t_year_view_base where y >= 2021;
select * from v_year order by id;

-- Update through view
update v_year set name = 'updated' where y = 2021;
select * from t_year_view_base order by id;

drop view v_year;
drop table t_year_view_base;

-- =====================================================
-- Test cases for AVG/SUM aggregate functions with YEAR
-- =====================================================
drop table if exists t_year_agg;
create table t_year_agg (y year);
insert into t_year_agg values (2020), (2021), (2022), (2023), (2024);

select avg(y) from t_year_agg;
select sum(y) from t_year_agg;
select avg(y), sum(y), min(y), max(y), count(y) from t_year_agg;

drop table t_year_agg;

-- =====================================================
-- Test cases for window functions with YEAR
-- =====================================================
drop table if exists t_year_window;
create table t_year_window (id int, y year, val int);
insert into t_year_window values (1, 2020, 100), (2, 2020, 200), (3, 2021, 150), (4, 2022, 300), (5, 2022, 250);

-- ROW_NUMBER
select id, y, val, row_number() over (order by y, id) as rn from t_year_window;

-- RANK
select id, y, val, rank() over (order by y) as rnk from t_year_window;

-- DENSE_RANK
select id, y, val, dense_rank() over (order by y) as drnk from t_year_window;

-- Partition by YEAR
select id, y, val, row_number() over (partition by y order by val desc) as rn from t_year_window;

-- SUM over window
select id, y, val, sum(val) over (partition by y) as sum_by_year from t_year_window;

drop table t_year_window;

-- =====================================================
-- Test cases for LIMIT/OFFSET with YEAR ORDER BY
-- =====================================================
drop table if exists t_year_limit;
create table t_year_limit (id int, y year);
insert into t_year_limit values (1, 2020), (2, 2021), (3, 2022), (4, 2023), (5, 2024);

select * from t_year_limit order by y limit 3;
select * from t_year_limit order by y limit 2 offset 2;
select * from t_year_limit order by y desc limit 2;

drop table t_year_limit;

-- =====================================================
-- Test cases for composite primary key with YEAR
-- =====================================================
drop table if exists t_year_composite_pk;
create table t_year_composite_pk (y year, category varchar(10), val int, primary key(y, category));

insert into t_year_composite_pk values (2020, 'A', 1);
insert into t_year_composite_pk values (2020, 'B', 2);
insert into t_year_composite_pk values (2021, 'A', 3);

select * from t_year_composite_pk order by y, category;

drop table t_year_composite_pk;

-- =====================================================
-- Test cases for composite index with YEAR
-- =====================================================
drop table if exists t_year_composite_idx;
create table t_year_composite_idx (id int, y year, category varchar(10), index idx_year_cat(y, category));

insert into t_year_composite_idx values (1, 2020, 'A'), (2, 2020, 'B'), (3, 2021, 'A'), (4, 2021, 'B');

select * from t_year_composite_idx where y = 2020 and category = 'A';
select * from t_year_composite_idx where y = 2020 order by id;

drop table t_year_composite_idx;

-- =====================================================
-- Test cases for TRUNCATE TABLE with YEAR
-- =====================================================
drop table if exists t_year_truncate;
create table t_year_truncate (y year);
insert into t_year_truncate values (2020), (2021), (2022);

select count(*) from t_year_truncate;
truncate table t_year_truncate;
select count(*) from t_year_truncate;

-- Re-insert after truncate
insert into t_year_truncate values (2023), (2024);
select * from t_year_truncate order by y;

drop table t_year_truncate;

-- =====================================================
-- Test cases for transactions with YEAR
-- =====================================================
drop table if exists t_year_txn;
create table t_year_txn (y year);

-- Test COMMIT
begin;
insert into t_year_txn values (2020);
insert into t_year_txn values (2021);
commit;
select * from t_year_txn order by y;

-- Test ROLLBACK
begin;
insert into t_year_txn values (2099);
rollback;
select * from t_year_txn order by y;

drop table t_year_txn;

-- =====================================================
-- Test cases for BETWEEN boundary with YEAR
-- =====================================================
drop table if exists t_year_between;
create table t_year_between (y year);
insert into t_year_between values (1901), (1950), (2000), (2050), (2100), (2155);

-- Inclusive boundary test
select * from t_year_between where y between 1901 and 2155 order by y;
select * from t_year_between where y between 2000 and 2000;
select * from t_year_between where y between 2050 and 2100 order by y;

-- NOT BETWEEN
select * from t_year_between where y not between 1950 and 2050 order by y;

drop table t_year_between;

-- =====================================================
-- Test cases for empty string to YEAR conversion
-- =====================================================
select cast('' as year);

drop table if exists t_year_empty;
create table t_year_empty (y year);
insert into t_year_empty values ('');
select * from t_year_empty;
drop table t_year_empty;

-- =====================================================
-- Test cases for whitespace string to YEAR
-- =====================================================
select cast('  ' as year);
select cast(' 2024 ' as year);

-- =====================================================
-- Test cases for leading zeros in YEAR string
-- =====================================================
select cast('0024' as year);
select cast('0070' as year);
select cast('0000' as year);

drop table if exists t_year_leading_zero;
create table t_year_leading_zero (y year);
insert into t_year_leading_zero values ('0024');
insert into t_year_leading_zero values ('0070');
select * from t_year_leading_zero order by y;
drop table t_year_leading_zero;

-- =====================================================
-- Test cases for negative year string
-- =====================================================
select cast('-1' as year);
select cast('-2024' as year);

-- =====================================================
-- Test cases for float/decimal to YEAR conversion
-- =====================================================
select cast(2024.0 as year);
select cast(2024.5 as year);
select cast(2024.9 as year);

-- =====================================================
-- Test cases for very large numbers to YEAR
-- =====================================================
select cast(99999 as year);
select cast(2147483647 as year);

-- =====================================================
-- Test cases for boolean to YEAR
-- =====================================================
select cast(true as year);
select cast(false as year);

-- =====================================================
-- Test cases for YEAR comparison with NULL
-- =====================================================
drop table if exists t_year_null_cmp;
create table t_year_null_cmp (id int, y year);
insert into t_year_null_cmp values (1, 2024), (2, null), (3, 2020);

select * from t_year_null_cmp where y = null;
select * from t_year_null_cmp where y <> null;
select * from t_year_null_cmp where y is null;
select * from t_year_null_cmp where y is not null order by id;

-- NULL safe equal
select * from t_year_null_cmp where y <=> null;
select * from t_year_null_cmp where y <=> 2024;

drop table t_year_null_cmp;

-- =====================================================
-- Test cases for YEAR in GREATEST/LEAST functions
-- =====================================================
select greatest(2020, 2021, 2022);
select least(2020, 2021, 2022);

drop table if exists t_year_greatest;
create table t_year_greatest (y1 year, y2 year, y3 year);
insert into t_year_greatest values (2020, 2025, 2022);

select greatest(y1, y2, y3) from t_year_greatest;
select least(y1, y2, y3) from t_year_greatest;

drop table t_year_greatest;

-- =====================================================
-- Test cases for YEAR with IF function
-- =====================================================
drop table if exists t_year_if;
create table t_year_if (y year);
insert into t_year_if values (1999), (2000), (2001);

select y, if(y < 2000, '20th century', '21st century') as century from t_year_if order by y;

drop table t_year_if;

-- =====================================================
-- Test cases for YEAR with NULLIF function
-- =====================================================
select nullif(2024, 2024);
select nullif(2024, 2025);

drop table if exists t_year_nullif;
create table t_year_nullif (y year);
insert into t_year_nullif values (2020), (0), (2022);

select y, nullif(y, 0) as non_zero_year from t_year_nullif order by y;

drop table t_year_nullif;

-- =====================================================
-- Test cases for YEAR with ABS function
-- =====================================================
select abs(cast(2024 as year));

-- =====================================================
-- Test cases for YEAR with MOD function
-- =====================================================
select mod(2024, 4);
select mod(2024, 100);
select mod(2024, 400);

drop table if exists t_year_mod;
create table t_year_mod (y year);
insert into t_year_mod values (2020), (2021), (2022), (2023), (2024);

-- Check leap years (divisible by 4)
select y, mod(y, 4) as mod4 from t_year_mod order by y;

drop table t_year_mod;

-- =====================================================
-- Test cases for CONCAT with YEAR
-- =====================================================
select concat('Year: ', 2024);
select concat(2020, '-', 2024);

drop table if exists t_year_concat;
create table t_year_concat (y year, name varchar(20));
insert into t_year_concat values (2024, 'Event');

select concat(name, ' (', y, ')') as full_name from t_year_concat;

drop table t_year_concat;

-- =====================================================
-- Test cases for YEAR in prepared statement style
-- =====================================================
drop table if exists t_year_prep;
create table t_year_prep (y year);

set @y = 2024;
insert into t_year_prep values (@y);
select * from t_year_prep;

set @y = 1999;
select * from t_year_prep where y > @y;

drop table t_year_prep;

-- =====================================================
-- Test cases for YEAR with AUTO_INCREMENT
-- =====================================================
drop table if exists t_year_auto;
create table t_year_auto (id int auto_increment primary key, y year);
insert into t_year_auto (y) values (2020);
insert into t_year_auto (y) values (2021);
insert into t_year_auto (y) values (2022);

select * from t_year_auto order by id;

drop table t_year_auto;

-- =====================================================
-- Test cases for temporary table with YEAR
-- =====================================================
drop table if exists t_year_temp;
create temporary table t_year_temp (y year);
insert into t_year_temp values (2020), (2021), (2022);

select * from t_year_temp order by y;

drop table t_year_temp;

-- Cleanup
drop table if exists t_year;
