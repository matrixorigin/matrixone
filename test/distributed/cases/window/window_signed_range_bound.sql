-- @suite

-- @case
-- @desc: issue #27574 signed integer RANGE bounds must not wrap at type extrema
-- @label:bvt

drop database if exists window_signed_range_27574;
create database window_signed_range_27574;
use window_signed_range_27574;

create table extrema(
  id int primary key,
  i8 tinyint,
  i16 smallint,
  i32 int,
  i64 bigint,
  v int
);
insert into extrema values
  (1, -128, -32768, -2147483648, -9223372036854775808, 10),
  (2, 0, 0, 0, 0, 20),
  (3, 127, 32767, 2147483647, 9223372036854775807, 30);

select id,
       count(*) over (order by i8 range between current row and 1 following) as f_c,
       sum(v) over (order by i8 range between current row and 1 following) as f_s,
       count(*) over (order by i8 range between 1 preceding and current row) as p_c,
       sum(v) over (order by i8 range between 1 preceding and current row) as p_s
from extrema order by id;

select id,
       count(*) over (order by i16 range between current row and 1 following) as f_c,
       sum(v) over (order by i16 range between current row and 1 following) as f_s,
       count(*) over (order by i16 range between 1 preceding and current row) as p_c,
       sum(v) over (order by i16 range between 1 preceding and current row) as p_s
from extrema order by id;

select id,
       count(*) over (order by i32 range between current row and 1 following) as f_c,
       sum(v) over (order by i32 range between current row and 1 following) as f_s,
       count(*) over (order by i32 range between 1 preceding and current row) as p_c,
       sum(v) over (order by i32 range between 1 preceding and current row) as p_s
from extrema order by id;

select id,
       count(*) over (order by i64 range between current row and 1 following) as f_c,
       sum(v) over (order by i64 range between current row and 1 following) as f_s,
       count(*) over (order by i64 range between 1 preceding and current row) as p_c,
       sum(v) over (order by i64 range between 1 preceding and current row) as p_s
from extrema order by id;

select id,
       count(*) over (order by i64 desc range between current row and 1 following) as f_c,
       sum(v) over (order by i64 desc range between current row and 1 following) as f_s,
       count(*) over (order by i64 desc range between 1 preceding and current row) as p_c,
       sum(v) over (order by i64 desc range between 1 preceding and current row) as p_s
from extrema order by id;

select id,
       count(*) over (order by i64 range between 1 preceding and 1 preceding) as empty_c,
       sum(v) over (order by i64 range between 1 preceding and 1 preceding) as empty_s
from extrema order by id;

create table nullable_t(id int primary key, k bigint, v int);
insert into nullable_t values
  (1, null, 10),
  (2, null, 20),
  (3, -9223372036854775808, 30),
  (4, 9223372036854775807, 40);
select id, if(k is null, 'NULL', cast(k as char)) as k_label,
       count(*) over (order by k range between current row and 1 following) as f_c,
       sum(v) over (order by k range between current row and 1 following) as f_s,
       count(*) over (order by k range between 1 preceding and current row) as p_c,
       sum(v) over (order by k range between 1 preceding and current row) as p_s
from nullable_t order by k is not null, k, id;

drop database window_signed_range_27574;
