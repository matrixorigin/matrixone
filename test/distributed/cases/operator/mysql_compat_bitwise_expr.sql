-- @suite

-- @case
-- @desc: MySQL-compatible bitwise operator coercion with strings, numbers, and NULL
-- @label:bvt

drop database if exists mysql_compat_bitwise_expr;
create database mysql_compat_bitwise_expr;
use mysql_compat_bitwise_expr;

select null & 1 as null_bit_and,
       null | 1 as null_bit_or,
       null ^ 1 as null_bit_xor,
       null << 1 as null_left_shift,
       1 >> null as null_right_shift,
       ~null as null_bit_not;

select '+7' & 3 as signed_str_and,
       '+7' | 8 as signed_str_or,
       '+7' ^ 3 as signed_str_xor,
       '+7' << 2 as signed_str_left_shift,
       '+7' >> 1 as signed_str_right_shift;

drop table if exists t_bitwise_mixed;
create table t_bitwise_mixed (
  id int primary key,
  s varchar(16),
  n int
);

insert into t_bitwise_mixed values
  (1, '7', 3),
  (2, '+7', 2),
  (3, null, 1);

select id, s, n,
       s & n as s_bit_and_n,
       s | n as s_bit_or_n,
       s ^ n as s_bit_xor_n,
       s << n as s_left_shift_n,
       n >> s as n_right_shift_s
from t_bitwise_mixed
order by id;

drop database mysql_compat_bitwise_expr;
