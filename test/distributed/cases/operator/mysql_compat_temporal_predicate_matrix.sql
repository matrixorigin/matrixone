-- @suite

-- @case
-- @desc: MySQL compatibility cases for temporal predicate implicit conversion
-- @label:bvt

drop database if exists mysql_compat_temporal_predicate_matrix;
create database mysql_compat_temporal_predicate_matrix;
use mysql_compat_temporal_predicate_matrix;
set time_zone = '+00:00';

select cast('2024-01-02' as date) = '20240102' as date_eq_compact_string,
       cast('2024-01-02 03:04:05.123456' as datetime(6)) = '20240102030405.123456' as datetime_eq_compact_string;

select cast('2024-01-02' as date) between '20240101' and '20240103' as date_between_compact_strings,
       cast('2024-01-02 03:04:05.123456' as datetime(6)) between '20240102030405.123455' and '20240102030405.123457' as datetime_between_microsecond;

select cast('2024-01-02' as date) in ('20240101', '20240102') as date_in_compact_strings,
       cast('2024-01-02' as date) not in ('20240101', '20240103') as date_not_in_compact_strings,
       cast('2024-01-02 03:04:05.123456' as datetime(6)) in ('20240102030405.123456', '20240103000000') as datetime_in_compact_strings;

drop table if exists t_temporal_pred;
create table t_temporal_pred (
  id int primary key,
  d date,
  dt datetime(6),
  tm time(6),
  s varchar(32)
);

insert into t_temporal_pred values
  (1, '2024-01-02', '2024-01-02 03:04:05.123456', '03:04:05.123456', '20240102'),
  (2, '2024-01-03', '2024-01-03 00:00:00.000001', '12:00:00.000001', '20240103'),
  (3, null, null, null, null);

select id,
       d = s as date_eq_varchar_compact,
       d <=> s as date_nseq_varchar_compact,
       dt between '20240102030405.123455' and '20240103000000.000001' as dt_between_compact,
       tm between '030405.123455' and '120000.000001' as tm_between_compact
from t_temporal_pred
order by id;

-- A synthesized Decimal comparison domain must retain the full BIGINT
-- capacity.  In particular, equal physical Decimal64 targets must still be
-- widened before casting either operand.
drop table if exists t_bigint_time_pred;
create table t_bigint_time_pred (i bigint, tm time(0));
insert into t_bigint_time_pred values
  (-1000000000000000000, '00:00:00'),
  (-999999999999999999, '00:00:00'),
  (999999999999999999, '00:00:00'),
  (1000000000000000000, '00:00:00');
select i,
       tm,
       i = tm as bigint_eq_time,
       tm = i as time_eq_bigint,
       i < tm as bigint_lt_time,
       tm < i as time_lt_bigint
from t_bigint_time_pred
order by i;

-- BIT is an unsigned numeric source even though IsIntOrUint excludes it.
-- Comparison coercion must preserve its complete domain without relaxing
-- strict DECIMAL assignment precision checks.
set @temporal_pred_saved_sql_mode = @@session.sql_mode;
set session sql_mode = 'STRICT_TRANS_TABLES';
create table t_bit_time_pred (id int primary key, b bit(64), tm time(0));
insert into t_bit_time_pred values
  (1, 0, '00:00:00'),
  (2, 999999999999999999, '00:00:00'),
  (3, 1000000000000000000, '00:00:00'),
  (4, 18446744073709551615, '00:00:00'),
  (5, null, '00:00:00');
select id,
       b = tm as bit_eq_time,
       tm = b as time_eq_bit,
       b < tm as bit_lt_time,
       tm < b as time_lt_bit,
       b <=> tm as bit_nseq_time
from t_bit_time_pred
order by id;
create table t_bit_decimal_pred (d decimal(18,0));
insert into t_bit_decimal_pred select b from t_bit_time_pred where id = 3;
select count(*) as rejected_rows from t_bit_decimal_pred;
set session sql_mode = @temporal_pred_saved_sql_mode;

drop database mysql_compat_temporal_predicate_matrix;
