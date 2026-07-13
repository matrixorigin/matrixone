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

-- VARBINARY/BINARY comparisons must preserve stored 0x00 padding while
-- avoiding implicit padding of the VARBINARY operand.
drop table if exists t_varbinary_binary_cmp;
create table t_varbinary_binary_cmp (
  id int primary key,
  vb varbinary(8),
  b binary(4)
);

insert into t_varbinary_binary_cmp values
  (1, 'a', 'a'),
  (2, 'a ', 'a '),
  (3, x'61000000', 'a');

select id, hex(vb) as vb_hex, hex(b) as b_hex
from t_varbinary_binary_cmp
order by id;

select id,
       vb = binary 'a' as vb_eq_bin_a,
       binary 'a' = vb as bin_a_eq_vb,
       b = vb as b_eq_vb,
       vb = b as vb_eq_b,
       b = x'61' as b_eq_short_hex,
       b = x'61000000' as b_eq_padded_hex,
       vb <=> binary 'a' as vb_null_safe_eq_bin_a,
       vb > binary 'a' as vb_gt_bin_a
from t_varbinary_binary_cmp
order by id;

select id, vb in (binary 'a') as vb_in_bin_a
from t_varbinary_binary_cmp
order by id;

select id,
       vb in (select b from t_varbinary_binary_cmp where id = 1) as vb_in_b_subquery,
       b in (select vb from t_varbinary_binary_cmp where id = 1) as b_in_vb_subquery
from t_varbinary_binary_cmp
order by id;

drop table t_varbinary_binary_cmp;

-- BLOB/TEXT must retain byte semantics when compared with BINARY values.
drop table if exists t_binary_blob_text_cmp;
create table t_binary_blob_text_cmp (
  id int primary key,
  bl blob,
  tx text,
  b binary(4)
);

insert into t_binary_blob_text_cmp values
  (1, 'a', 'a', 'a'),
  (2, x'61000000', x'61000000', 'a');

select id, hex(bl) as bl_hex, hex(tx) as tx_hex, hex(b) as b_hex
from t_binary_blob_text_cmp
order by id;

select id,
       bl = binary 'a' as bl_eq_bin_a,
       binary 'a' = bl as bin_a_eq_bl,
       bl = b as bl_eq_b,
       b = bl as b_eq_bl,
       tx = binary 'a' as tx_eq_bin_a,
       binary 'a' = tx as bin_a_eq_tx,
       tx = b as tx_eq_b,
       b = tx as b_eq_tx
from t_binary_blob_text_cmp
order by id;

-- Follow MySQL Bug #28076's subquery-IN shape. The variable-length value
-- must not be zero-padded merely to compare it with BINARY(4).
select id
from t_binary_blob_text_cmp
where bl in (select b from t_binary_blob_text_cmp)
order by id;

select id
from t_binary_blob_text_cmp
where tx in (select b from t_binary_blob_text_cmp)
order by id;

select id
from t_binary_blob_text_cmp
where b in (select bl from t_binary_blob_text_cmp)
order by id;

select id
from t_binary_blob_text_cmp
where b in (select tx from t_binary_blob_text_cmp)
order by id;

drop table t_binary_blob_text_cmp;

-- MySQL Bug #28076: BINARY(N) padding must remain significant when an IN
-- predicate or a scalar subquery compares it with an unpadded VARBINARY(N).
drop table if exists t_binary_varbinary_subquery_cmp;
create table t_binary_varbinary_subquery_cmp (
  id int primary key,
  b binary(5),
  vb varbinary(5)
);

insert into t_binary_varbinary_subquery_cmp values
  (1, x'41', x'41'),
  (2, x'42', x'42'),
  (3, x'43', x'43');

-- No row matches: b is zero-padded, while vb is not.
select id
from t_binary_varbinary_subquery_cmp
where vb in (select b from t_binary_varbinary_subquery_cmp)
order by id;

select id
from t_binary_varbinary_subquery_cmp
where (vb, 10) in (select b, 10 from t_binary_varbinary_subquery_cmp)
order by id;

-- The direct literal comparison has the same byte-preserving behavior.
select id
from t_binary_varbinary_subquery_cmp
where b = x'41'
order by id;

create index idx_binary_varbinary_subquery_b on t_binary_varbinary_subquery_cmp(b);
create index idx_binary_varbinary_subquery_vb on t_binary_varbinary_subquery_cmp(vb);

-- Index availability must not change the comparison result.
select id
from t_binary_varbinary_subquery_cmp
where vb in (select b from t_binary_varbinary_subquery_cmp)
order by id;

select id
from t_binary_varbinary_subquery_cmp
where (vb, 10) in (select b, 10 from t_binary_varbinary_subquery_cmp)
order by id;

truncate table t_binary_varbinary_subquery_cmp;
insert into t_binary_varbinary_subquery_cmp values (1, x'41', x'41');

select id
from t_binary_varbinary_subquery_cmp
where b = (select vb from t_binary_varbinary_subquery_cmp)
order by id;

drop table t_binary_varbinary_subquery_cmp;

drop database mysql_compat_string_binary_cmp;
