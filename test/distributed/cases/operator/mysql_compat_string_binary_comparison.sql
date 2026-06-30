-- @suite

-- @case
-- @desc: MySQL-compatible string and binary comparison coercion
-- @label:bvt

drop database if exists mysql_compat_string_binary_cmp;
create database mysql_compat_string_binary_cmp;
use mysql_compat_string_binary_cmp;

select binary 'a' = 'A' as bin_left_case_eq,
       'a' = binary 'A' as bin_right_case_eq,
       binary 'a' < binary 'b' as bin_lt,
       binary 'a ' = binary 'a' as bin_trailing_eq;

select 'a ' like 'a' as like_no_space,
       'a ' like 'a ' as like_space,
       'abc' regexp '^a' as regexp_prefix,
       12345 regexp '^123' as regexp_numeric_prefix,
       null regexp 'a' as regexp_null_left,
       'abc' not regexp null as not_regexp_null_pat;

drop table if exists t_string_binary_cmp;
create table t_string_binary_cmp (
  id int primary key,
  vc varchar(8),
  vb varbinary(8),
  b binary(4)
);

insert into t_string_binary_cmp values
  (1, 'a', 'a', 'a'),
  (2, 'a ', 'a ', 'a '),
  (3, 'A', 'A', 'A'),
  (4, 'abc', 'abc', 'abc');

select id, hex(vc) as vc_hex, hex(vb) as vb_hex, hex(b) as b_hex
from t_string_binary_cmp
order by id;

select id,
       binary vc = 'a' as bin_vc_eq_a,
       vc like 'a ' as vc_like_a_space,
       vc like binary 'a%' as vc_like_binary_prefix,
       vb = 'a' as vb_eq_char_a,
       b like binary 'a%' as b_like_binary_prefix
from t_string_binary_cmp
order by id;

select id,
       vc regexp '^[aA]' as vc_regexp_prefix,
       vc rlike 'c$' as vc_rlike_suffix,
       id regexp '^[12]$' as id_regexp_small_set
from t_string_binary_cmp
order by id;

select binary null = 'a' as bin_null_eq_str,
       binary 'a' = null as bin_str_eq_null,
       binary null <=> null as bin_null_safe_null,
       binary null <=> binary 'a' as bin_null_safe_str;

select '' like '' as empty_like_empty,
       '' like '_' as empty_like_one_char,
       'abc' like '' as nonempty_like_empty,
       'abc' not like null as not_like_null_pat,
       null not like 'a%' as null_not_like_pat;

select '' regexp '^$' as empty_regexp_anchor,
       'abc' not regexp null as not_regexp_null_pat,
       null not regexp 'a' as null_not_regexp_pat;

drop table if exists t_string_binary_unhappy;
create table t_string_binary_unhappy (
  id int primary key,
  vc varchar(8),
  vb varbinary(8),
  b binary(4)
);

insert into t_string_binary_unhappy values
  (1, '', '', ''),
  (2, null, null, null),
  (3, 'a', 'a', 'a');

select id, hex(vc) as vc_hex, hex(vb) as vb_hex, hex(b) as b_hex
from t_string_binary_unhappy
order by id;

select id,
       binary vc = 'a' as bin_vc_eq_a,
       vc like '' as vc_like_empty,
       vc not like null as vc_not_like_null,
       vb = 'a' as vb_eq_char_a,
       b like binary '' as b_like_empty
from t_string_binary_unhappy
order by id;

select id,
       vc regexp '^$' as vc_regexp_empty,
       vc not regexp null as vc_not_regexp_null,
       id not regexp '^[12]$' as id_not_regexp_small_set
from t_string_binary_unhappy
order by id;

drop database mysql_compat_string_binary_cmp;
