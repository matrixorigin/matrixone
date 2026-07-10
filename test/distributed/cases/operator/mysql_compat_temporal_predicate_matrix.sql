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

drop database mysql_compat_temporal_predicate_matrix;
