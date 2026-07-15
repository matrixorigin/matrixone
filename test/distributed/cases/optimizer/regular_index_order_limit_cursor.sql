-- @suite
-- @case
-- @desc: regression for regular secondary index ORDER BY PK LIMIT with cursor filter
-- @label:bvt

drop database if exists regular_index_order_limit_cursor;
create database regular_index_order_limit_cursor;
use regular_index_order_limit_cursor;

create table t (
  id varchar(64) primary key,
  user_id varchar(64) not null,
  is_active tinyint default 1,
  content text,
  key idx_user(user_id, is_active)
);

insert into t
select
  concat('id_', lpad(result, 8, '0')),
  'u1',
  1,
  concat('content_', result)
from generate_series(1, 50) g;

select mo_ctl('dn', 'flush', 'regular_index_order_limit_cursor.t');
select Sleep(1);

select id
from t
where user_id = 'u1'
  and is_active = 1
  and id < 'id_00000040'
order by id desc
limit 5;

select id
from t
where user_id = 'u1'
  and is_active = 1
  and id <= 'id_00000040'
order by id desc
limit 5;

select id
from t
where user_id = 'u1'
  and is_active = 1
  and id > 'id_00000010'
order by id asc
limit 5;

select id
from t
where user_id = 'u1'
  and is_active = 1
  and id >= 'id_00000010'
order by id asc
limit 5;

select id
from t
where user_id = 'u1'
  and is_active = 1
  and id < 'id_00000005'
order by id desc
limit 10;

drop database regular_index_order_limit_cursor;
