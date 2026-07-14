-- @suite
-- @case
-- @desc: MySQL compatibility cases for expression type conversion, batch 1
-- @label:bvt

drop database if exists mysql_compat_type_conversion;
create database mysql_compat_type_conversion;
use mysql_compat_type_conversion;
set time_zone = '+00:00';

-- varchar column values used in explicit and arithmetic numeric conversion
drop table if exists t_string_number;
create table t_string_number (id int primary key, s varchar(20));
insert into t_string_number values
  (1, '0007'),
  (2, '15'),
  (3, '-2'),
  (4, '  12');
select id, s, s + 1 as plus_one, cast(s as decimal(10,2)) as as_decimal
from t_string_number order by id;
drop table t_string_number;

-- set-operation result coercion with string, integer, and decimal branches
select value_col
from (select '1' as value_col union all select 2 union all select 3.50) as u
order by cast(value_col as decimal(10,2));

-- JSON-extracted scalar conversion
drop table if exists t_json_compat;
create table t_json_compat (id int primary key, j json);
insert into t_json_compat values
  (1, '{"a": "12", "b": 2}'),
  (2, '{"a": "003", "b": 4}');
select id, json_extract(j, '$.a') as a_json,
       cast(json_unquote(json_extract(j, '$.a')) as signed) as a_signed,
       cast(json_extract(j, '$.b') as unsigned) as b_unsigned
from t_json_compat order by id;
drop table t_json_compat;

-- conditional expression values with mixed string and numeric branches
select if(1, '2', 3) as if_string_number,
       if(0, '2', 3) as if_number_branch,
       if(1, '2', 3) + 1 as if_plus_one;

select case when 1 then '12.50' else 3.25 end as case_string_decimal,
       (case when 1 then '12.50' else 3.25 end) + 0.50 as case_decimal_plus;

select coalesce(null, '8', 9) as coalesce_string_number,
       coalesce(null, '8', 9) + 1 as coalesce_plus_one;

select ifnull(null, '8') as ifnull_string,
       ifnull(null, '8') + 1 as ifnull_plus_one,
       ifnull(7, '8') as ifnull_first_number;

drop table if exists t_string_number_unhappy;
create table t_string_number_unhappy (id int primary key, s varchar(20));
insert into t_string_number_unhappy values
  (1, null),
  (2, '-0'),
  (3, '000');
select id, s, s + 1 as plus_one, cast(s as signed) as as_signed, cast(s as decimal(6,2)) as as_decimal
from t_string_number_unhappy order by id;
drop table t_string_number_unhappy;

drop database mysql_compat_type_conversion;
