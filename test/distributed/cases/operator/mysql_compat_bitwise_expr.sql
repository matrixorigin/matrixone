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

-- Binary introducers/casts select fixed-width bytewise semantics; bare hex
-- literals and ordinary text remain numeric controls.
select hex(~_binary X'00FF') as binary_not,
       octet_length(~_binary X'00FF') as binary_not_bytes,
       hex(_binary X'0102' << 1) as binary_left_1,
       hex(_binary X'0102' >> 1) as binary_right_1,
       hex(_binary X'0102' << 0) as binary_left_0,
       hex(_binary X'0102' >> 0) as binary_right_0,
       hex(_binary X'0102' << 8) as binary_left_8,
       hex(_binary X'0102' >> 8) as binary_right_8,
       hex(~cast(X'00FF' as binary(2))) as cast_binary_not,
       hex(~X'00FF') as bare_hex_numeric_not,
       hex(X'0102' << 1) as bare_hex_numeric_left;

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

drop table if exists t_bitwise_binary;
create table t_bitwise_binary (
  id int primary key,
  v varbinary(2),
  b blob
);

insert into t_bitwise_binary values
  (1, unhex('0102'), unhex('00FF')),
  (2, unhex('00'), unhex(repeat('FF', 512))),
  (3, null, null);

select id,
       hex(~v) as varbinary_not,
       octet_length(~v) as varbinary_not_bytes,
       hex(v << 1) as varbinary_left_1,
       octet_length(v << 1) as varbinary_left_1_bytes,
       hex(v << 0) as varbinary_left_0,
       hex(v >> 1) as varbinary_right_1,
       octet_length(v >> 1) as varbinary_right_1_bytes,
       hex(v >> 0) as varbinary_right_0,
       hex(v << 8) as varbinary_left_8,
       hex(v >> 8) as varbinary_right_8
from t_bitwise_binary
order by id;

-- Scalar operators are not subject to the aggregate bitwise 511-byte limit.
select id,
       octet_length(b) as blob_bytes,
       octet_length(~b) as blob_not_bytes,
       hex(substring(~b, 1, 2)) as blob_not_prefix,
       hex(right(~b, 1)) as blob_not_suffix,
       octet_length(b << 1) as blob_left_bytes,
       hex(substring(b << 1, 1, 2)) as blob_left_prefix,
       hex(right(b << 1, 1)) as blob_left_suffix,
       octet_length(b >> 1) as blob_right_bytes
from t_bitwise_binary
where id in (1, 2, 3)
order by id;

drop database mysql_compat_bitwise_expr;
