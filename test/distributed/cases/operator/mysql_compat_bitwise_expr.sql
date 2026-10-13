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

-- BIT numeric operators use MySQL's full BIGINT UNSIGNED domain. In
-- particular, unary complement is not masked to BIT(M)'s declared width.
drop table if exists t_bit_numeric;
create table t_bit_numeric (
  id int primary key,
  b1 bit(1),
  b8 bit(8),
  b64 bit(64)
);

insert into t_bit_numeric values
  (1, b'0', b'00000000', b'0000000000000000000000000000000000000000000000000000000000000000'),
  (2, b'1', b'10000000', b'1000000000000000000000000000000000000000000000000000000000000000'),
  (3, b'1', b'11111111', b'1111111111111111111111111111111111111111111111111111111111111111'),
  (4, null, null, null);

select id,
       hex(~b1) as bit1_not,
       hex(~b8) as bit8_not,
       hex(~b64) as bit64_not,
       b1 div 1 as bit1_div_1,
       b8 div 1 as bit8_div_1,
       b64 div 1 as bit64_div_1
from t_bit_numeric
order by id;

select id, b64 div b64 as bit64_self_div
from t_bit_numeric
where id in (2, 3)
order by id;

create table t_bit_div_meta as
select b64 div 1 as quotient from t_bit_numeric limit 0;
select column_name, column_type
from information_schema.columns
where table_schema = database() and table_name = 't_bit_div_meta';
drop table t_bit_div_meta;
drop table t_bit_numeric;

drop database mysql_compat_bitwise_expr;
