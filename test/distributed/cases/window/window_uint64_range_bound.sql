-- issue #27565: uint64 RANGE bounds must not wrap or return the wrong
-- out-of-domain boundary for ASC or DESC order.
drop database if exists window_uint64_range_27565;
create database window_uint64_range_27565;
use window_uint64_range_27565;

create table t(id bigint unsigned primary key);
insert into t select result from generate_series(0,20) g;

select id,
       count(*) over (
         order by id range between current row and 10 following
       ) as c,
       sum(id) over (
         order by id range between current row and 10 following
       ) as s
from t order by id;

select id,
       count(*) over (
         order by id desc range between current row and 10 following
       ) as c,
       sum(id) over (
         order by id desc range between current row and 10 following
       ) as s
from t order by id desc;

select id,
       count(*) over (
         order by id range between current row and 0 following
       ) as zero_c,
       count(*) over (
         order by id range between 10 preceding and 10 preceding
       ) as preceding_c,
       sum(id) over (
         order by id range between 10 preceding and 10 preceding
       ) as preceding_s
from t order by id;

create table max_t(k bigint unsigned primary key, v int);
insert into max_t values
  (18446744073709551613, 1),
  (18446744073709551614, 2),
  (18446744073709551615, 3);
select k,
       count(*) over (
         order by k range between current row and 10 following
       ) as c,
       sum(v) over (
         order by k range between current row and 10 following
       ) as s
from max_t order by k;

create table nullable_t(id int primary key, k bigint unsigned, v int);
insert into nullable_t values
  (1, null, 10),
  (2, null, 20),
  (3, 0, 30),
  (4, 1, 40);
select id, if(k is null, 'NULL', cast(k as char)) as k_label,
       count(*) over (
         order by k range between current row and 10 following
       ) as c,
       sum(v) over (
         order by k range between current row and 10 following
       ) as s
from nullable_t order by k is not null, k, id;

drop database window_uint64_range_27565;
