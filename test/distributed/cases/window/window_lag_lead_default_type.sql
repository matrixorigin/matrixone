-- @suite

-- @case
-- @desc: LAG/LEAD default values must share one common result type with the value argument instead of being narrowed to it (string defaults are not truncated, decimal defaults are not rounded), and CTAS/View inherit the widened type
-- @label:bvt

drop database if exists window_lag_lead_default_type;
create database window_lag_lead_default_type;
use window_lag_lead_default_type;

create table wdefault (id int primary key, s varchar(10), v int);
insert into wdefault values (1, 'a', 100), (2, 'b', 10), (3, null, 20);

-- A longer string default keeps its full text and widens the result type.
select id,
       lag(s, 1, 'long default') over (order by id) as lg,
       lead(s, 1, 'long default') over (order by id) as ld
from wdefault order by id;

-- An integer input with a decimal default keeps the fractional default.
select id,
       lag(v, 1, 0.5) over (order by id) as lv,
       lead(v, 1, 0.5) over (order by id) as vd
from wdefault order by id;

-- CTAS inherits the common result type and stores untruncated values.
create table wcopy as
select id, lag(s, 1, 'long default') over (order by id) as lg from wdefault;
show columns from wcopy;
select * from wcopy order by id;

-- Views expose the common result type as well.
create view wv as
select id, lag(v, 1, 0.5) over (order by id) as lv from wdefault;
show columns from wv;
select * from wv order by id;

-- A domainless NULL default keeps the value type.
select id, lag(s, 1, null) over (order by id) as lg from wdefault order by id;

-- A shorter or equally typed default does not change the value type.
select id, lag(s, 1, 'def') over (order by id) as lg from wdefault order by id;
select id, lag(v, 1, -1) over (order by id) as lv from wdefault order by id;

-- Prepared statements reuse different default widths without truncation.
prepare st_lag_default from 'select id, lag(s, 1, ?) over (order by id) as lg from wdefault order by id';
set @short_default = 'x';
set @long_default = 'a very long default value';
execute st_lag_default using @short_default;
execute st_lag_default using @long_default;
deallocate prepare st_lag_default;

drop database window_lag_lead_default_type;
