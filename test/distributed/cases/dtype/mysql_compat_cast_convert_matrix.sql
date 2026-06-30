-- @suite

-- @case
-- @desc: MySQL compatibility cases for CAST/CONVERT target type matrix
-- @label:bvt

drop database if exists mysql_compat_cast_convert_matrix;
create database mysql_compat_cast_convert_matrix;
use mysql_compat_cast_convert_matrix;
set time_zone = '+00:00';

select cast('  -12' as signed) as signed_ws,
       cast('0012' as unsigned) as unsigned_leading_zero,
       cast('12.75' as decimal(6,2)) as decimal_scale,
       cast('12.75' as double) as double_value,
       cast('-12.75' as float) as float_value;

select cast('12.344' as decimal(6,2)) as decimal_round_down,
       cast('12.345' as decimal(6,2)) as decimal_round_up,
       cast('-12.345' as decimal(6,2)) as decimal_negative_round,
       cast('7e0' as double) as double_exp,
       cast('7e0' as float) as float_exp,
       cast('1e-3' as decimal(8,4)) as decimal_exp;

select convert('42', signed) as convert_signed,
       convert('42', unsigned) as convert_unsigned,
       convert('42.50', decimal(6,2)) as convert_decimal;

select cast(12345 as char) as int_to_char,
       cast(-12.50 as char) as decimal_to_char,
       hex(cast('AZ' as binary)) as binary_hex,
       length(cast('AZ' as binary)) as binary_len;

select hex(cast('AZ' as binary(4))) as binary_fixed_hex,
       length(cast('AZ' as binary(4))) as binary_fixed_len;

select cast('2024-01-02' as date) as cast_date,
       cast('2024-01-02 03:04:05.123456' as datetime(6)) as cast_datetime6,
       cast('03:04:05.123456' as time(6)) as cast_time6;

select cast('20240102' as date) as compact_date,
       cast('20240102030405' as datetime) as compact_datetime,
       cast('030405' as time) as compact_time,
       cast('34:05' as time) as hour_minute_time;

select cast(cast('2024-01-02' as date) as char) as date_to_char,
       cast(cast('2024-01-02 03:04:05.123456' as datetime(6)) as char) as datetime_to_char,
       cast(cast('03:04:05.123456' as time(6)) as char) as time_to_char;

select json_extract(cast('{"a": 1, "b": "2"}' as json), '$.a') as json_a,
       json_unquote(json_extract(cast('{"a": 1, "b": "2"}' as json), '$.b')) as json_b;

select json_type(cast('[1, "2", null]' as json)) as json_array_type,
       json_extract(cast('[1, "2", null]' as json), '$[1]') as json_array_value,
       json_type(cast('true' as json)) as json_bool_type,
       json_type(cast('null' as json)) as json_null_type;

select cast(null as signed) as null_signed,
       cast(null as unsigned) as null_unsigned,
       cast(null as decimal(6,2)) as null_decimal,
       cast(null as char) as null_char,
       cast(null as date) as null_date,
       cast(null as datetime(6)) as null_datetime,
       cast(null as time(6)) as null_time;

select cast('-0' as signed) as signed_neg_zero,
       cast('  +0' as signed) as signed_plus_zero_ws,
       cast('000.00' as decimal(6,2)) as decimal_zero_padded;

select convert(null, signed) as convert_null_signed,
       convert(null, unsigned) as convert_null_unsigned,
       convert(null, decimal(6,2)) as convert_null_decimal;

drop database mysql_compat_cast_convert_matrix;
