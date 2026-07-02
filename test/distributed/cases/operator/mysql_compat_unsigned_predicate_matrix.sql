-- @suite

-- @case
-- @desc: MySQL compatibility cases for signed and unsigned comparison predicates
-- @label:bvt

drop database if exists mysql_compat_unsigned_predicate_matrix;
create database mysql_compat_unsigned_predicate_matrix;
use mysql_compat_unsigned_predicate_matrix;

select cast(-1 as signed) < cast(1 as unsigned) as neg_signed_lt_unsigned_one,
       cast(-1 as signed) > cast(1 as unsigned) as neg_signed_gt_unsigned_one,
       cast(9223372036854775807 as signed) < cast(18446744073709551615 as unsigned) as signed_max_lt_unsigned_max,
       cast(18446744073709551615 as unsigned) > cast(9223372036854775807 as signed) as unsigned_max_gt_signed_max;

select -1 in (cast(18446744073709551615 as unsigned), 1) as neg_one_in_unsigned_list,
       cast(18446744073709551615 as unsigned) in (-1, 1) as unsigned_max_in_signed_list,
       cast(18446744073709551615 as unsigned) not in (0, 1) as unsigned_max_not_in_small_list;

drop table if exists t_unsigned_pred;
create table t_unsigned_pred (
  id int primary key,
  s bigint,
  u bigint unsigned,
  v varchar(32)
);

insert into t_unsigned_pred values
  (1, -1, 18446744073709551615, '18446744073709551615'),
  (2, 0, 0, '0'),
  (3, 9223372036854775807, 9223372036854775808, '9223372036854775808');

select id,
       s < u as signed_lt_unsigned_col,
       s > u as signed_gt_unsigned_col,
       u = v as unsigned_eq_varchar_col,
       u between '9223372036854775808' and '18446744073709551615' as unsigned_between_varchar_bounds
from t_unsigned_pred
order by id;

drop database mysql_compat_unsigned_predicate_matrix;
