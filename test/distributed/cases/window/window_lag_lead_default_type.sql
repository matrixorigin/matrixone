-- @suite
--- @metacmp(true)

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

-- 公共类型不得缩窄源文本，默认值未被使用时也必须保持完整容量。
create table wtext (v mediumtext);
insert into wtext values (repeat('a', 65536));
select length(v) as original_length,
       length(lag(v, 0) over ()) as control_length,
       length(lag(v, 0, 0) over ()) as lag_length,
       length(lead(v, 0, 0) over ()) as lead_length from wtext;
select length(lag(v, 1, 0) over ()) as default_length from wtext;
select length(lag(0, 1, v) over ()) as text_default_length from wtext;

-- 数值与字符串字面量的窗口身份不同；投影顺序和单独查询不能改变类型。
select lag(7,0,0.5) over () as num, lag(7,0,'0.5') over () as str;
select lag(7,0,'0.5') over () as str, lag(7,0,0.5) over () as num;
select lag(7,0,0.5) over () as num;
select lag(7,0,'0.5') over () as str;
select lead(7,0,0.5) over () as num, lead(7,0,'0.5') over () as str;
select lead(7,0,'0.5') over () as str, lead(7,0,0.5) over () as num;
select lead(7,0,0.5) over () as num;
select lead(7,0,'0.5') over () as str;
select length(lag(7,0,0.5) over ()) as num_length,
       length(lag(7,0,'0.5') over ()) as str_length;

-- 默认整数所需的整数位必须进入 CTAS 元数据，原小数位保持不变。
create table wdecimal as select lag(cast(1 as decimal(2,1)), 1, 1000) over () as v;
show columns from wdecimal;
select v from wdecimal;
create table wdecimal_column as
select id, lag(cast(1 as decimal(2,1)), 1, v) over (order by id) as d from wdefault;
show columns from wdecimal_column;
select * from wdecimal_column order by id;
select lag(cast(1 as decimal(38,30)), 1, 999999999999) over () as lg,
       lead(cast(1 as decimal(38,30)), 1, 999999999999) over () as ld;
select lag(cast(1 as decimal(38,30)), 0, 999999999999) over () as source_value;

drop database window_lag_lead_default_type;
