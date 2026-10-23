-- @case
-- @desc: issue #30563 numeric RANGE bounds use a wider signed arithmetic domain
-- @label:bvt

drop database if exists window_numeric_range_wide_30563;
create database window_numeric_range_wide_30563;
use window_numeric_range_wide_30563;

create table tiny(k tinyint, v int);
insert into tiny values (1, 10), (2, 20), (3, 30);
select k,
       count(*) over (order by k range between 200 preceding and current row) as preceding_c,
       sum(v) over (order by k range between 200 preceding and current row) as preceding_s
from tiny order by k;
select k,
       count(*) over (order by k range between current row and 200 following) as following_c
from tiny order by k;
select k,
       count(*) over (order by k range between 100 preceding and 200 following) as mixed_c
from tiny order by k;

create table small(k smallint);
insert into small values (1), (2), (3);
select k,
       count(*) over (order by k range between 70000 preceding and current row) as preceding_c
from small order by k;

create table integers(k int);
insert into integers values (1), (2), (2), (3), (4), (5);
select k,
       count(*) over (order by k range between 2147483648 preceding and current row) as int32_c,
       count(*) over (order by k range between 9223372036854775807 preceding and current row) as int64_c
from integers order by k;

drop database window_numeric_range_wide_30563;
-- @suite
