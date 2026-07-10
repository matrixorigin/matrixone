-- @suite

-- @case
-- @desc: MySQL-compatible logical operator coercion with strings, numbers, and NULL
-- @label:bvt

drop database if exists mysql_compat_logical_expr;
create database mysql_compat_logical_expr;
use mysql_compat_logical_expr;

select '1' and 2 as str_true_and_num_true,
       '0' and 2 as str_zero_and_true,
       null and 1 as null_and_true,
       null and 0 as null_and_false;

select '1' or 0 as str_true_or_false,
       '0' or 0 as str_zero_or_false,
       null or 1 as null_or_true,
       null or 0 as null_or_false;

select '1' xor 0 as str_true_xor_false,
       '1' xor 1 as str_true_xor_true,
       '0' xor 0 as str_zero_xor_false,
       null xor 1 as null_xor_true;

select not '1' as not_str_true,
       not '0' as not_str_zero,
       not null as not_null;

drop table if exists t_logical_mixed;
create table t_logical_mixed (
  id int primary key,
  s varchar(16),
  n int
);

insert into t_logical_mixed values
  (1, '1', 1),
  (2, '0', 0),
  (3, null, 0),
  (4, '2', null);

select id, s, n,
       s and n as s_and_n,
       s or n as s_or_n,
       s xor n as s_xor_n,
       not s as not_s
from t_logical_mixed
order by id;

drop database mysql_compat_logical_expr;
