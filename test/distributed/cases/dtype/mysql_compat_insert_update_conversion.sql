-- @suite

-- @case
-- @desc: MySQL compatibility cases for INSERT/UPDATE implicit column conversion
-- @label:bvt

drop database if exists mysql_compat_insert_update_conversion;
create database mysql_compat_insert_update_conversion;
use mysql_compat_insert_update_conversion;
set time_zone = '+00:00';
set session sql_mode = '';

drop table if exists t_conv;
create table t_conv (
  id int primary key,
  i int,
  u int unsigned,
  d decimal(6,2),
  f double,
  da date,
  dt datetime(6),
  tm time(6),
  vc varchar(4),
  ch char(4)
);

insert into t_conv values
  (1, '0012', '0012', '12.345', '7e0', '20240102', '20240102112233.123456', '11:22:33.123456', 'abcd', 'abcd'),
  (2, '+7', '7', '-0.004', '-1.25e2', '2024-02-29', '2024-02-29 01:02:03.999999', '112233.5', 'xy', 'xy');

select id, i, u, d, cast(f as decimal(8,1)) as f_value, da,
       date_format(dt, '%Y-%m-%d %H:%i:%s.%f') as dt_text,
       time_format(tm, '%H:%i:%s.%f') as tm_text,
       concat('[', vc, ']') as vc_box, char_length(vc) as vc_len,
       concat('[', ch, ']') as ch_box, char_length(ch) as ch_len
from t_conv order by id;

update t_conv
set i='  +13',
    u='13',
    d='9.999',
    f='-125',
    da='20240203',
    dt='20240203112233.654321',
    tm='12:34:56.654321',
    vc='pqrs',
    ch='pqrs'
where id=1;

select id, i, u, d, cast(f as decimal(8,1)) as f_value, da,
       date_format(dt, '%Y-%m-%d %H:%i:%s.%f') as dt_text,
       time_format(tm, '%H:%i:%s.%f') as tm_text,
       concat('[', vc, ']') as vc_box, char_length(vc) as vc_len,
       concat('[', ch, ']') as ch_box, char_length(ch) as ch_len
from t_conv order by id;

drop table t_conv;
drop database mysql_compat_insert_update_conversion;
