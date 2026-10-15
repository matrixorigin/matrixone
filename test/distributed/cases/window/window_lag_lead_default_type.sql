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
create table wtext (v mediumtext, j json);
insert into wtext values (repeat('a', 65536), '{}');
select length(v) as original_length,
       length(lag(v, 0) over ()) as control_length,
       length(lag(v, 0, 0) over ()) as lag_length,
       length(lead(v, 0, 0) over ()) as lead_length from wtext;
select length(lag(v, 1, 0) over ()) as default_length from wtext;
select length(lag(0, 1, v) over ()) as text_default_length from wtext;

-- 未知 JSON 显示容量必须对应可持久化的大对象域；两种模式都不能丢失源 MEDIUMTEXT。
set @wdefault_sql_mode = @@session.sql_mode;
set session sql_mode = 'STRICT_TRANS_TABLES';
create table wtext_json_strict as select lag(v,0,j) over () as lg, lead(v,0,j) over () as ld from wtext;
show create table wtext_json_strict;
select length(lg) as lg_length, length(ld) as ld_length,
       lg = repeat('a',65536) as lg_equal, ld = repeat('a',65536) as ld_equal from wtext_json_strict;
set session sql_mode = '';
create table wtext_json_permissive as select lag(v,0,j) over () as lg, lead(v,0,j) over () as ld from wtext;
show create table wtext_json_permissive;
select length(lg) as lg_length, length(ld) as ld_length,
       lg = repeat('a',65536) as lg_equal, ld = repeat('a',65536) as ld_equal from wtext_json_permissive;
create view wtext_json_view as select lag(v,0,j) over () as lg, lead(v,0,j) over () as ld from wtext;
show columns from wtext_json_view;
select length(lg) as lg_length, length(ld) as ld_length from wtext_json_view;
set session sql_mode = @wdefault_sql_mode;

-- BOOL 只贡献数值 0/1，不得把整数、小数或浮点源值转为真假值。
select lag(42,0,true) over () as lg, lead(42,0,false) over () as ld;
select lag(cast(42.5 as double),0,true) over () as lg,
       lead(cast(42.5 as float),0,false) over () as ld;
select lag(42,1,true) over () as lg, lead(42,1,false) over () as ld;
select lag(true,0,42) over () as lg, lead(false,1,42) over () as ld;
select lag(cast(0.1 as decimal(2,2)),1,true) over () as lg,
       lead(cast(0.1 as decimal(65,65)),0,false) over () as ld;
select id, lag(v,0,v>0) over (order by id) as numeric_source,
       lead(v,1,v<10) over (order by id) as bool_default,
       lag(cast(0.1 as decimal(2,2)),1,v>0) over (order by id) as decimal_bool_default,
       lag(v>0,1,42) over (order by id) as bool_source from wdefault order by id;
create table wbool_copy as select lag(42,0,true) over () as lg,
       lead(cast(42.5 as double),0,false) over () as ld;
show columns from wbool_copy;
select * from wbool_copy;
create view wbool_view as select lag(42,0,true) over () as lg,
       lead(cast(42.5 as double),0,false) over () as ld;
show columns from wbool_view;
select * from wbool_view;

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

-- 二进制大对象不得变为超宽 VARCHAR，导出的列定义必须可重建。
create table wblob (v mediumblob);
insert into wblob values (unhex('610062ff'));
create table wblob_copy as select lag(v,0,0) over () as lg, lead(v,0,0) over () as ld from wblob;
show create table wblob_copy;
select hex(lg) as lg, hex(ld) as ld from wblob_copy;
select hex(lag(v,1,0) over ()) as lg, hex(lead(v,1,0) over ()) as ld from wblob;
create view wblob_view as select lag(v,0,0) over () as v from wblob;
show columns from wblob_view;
select hex(v) as v from wblob_view;

-- 默认零无需新增整数位；无默认值与同型默认值是持久化类型的控制查询。
create table wdecimal_max_zero as select lag(cast(0.1 as decimal(65,65)),0,0) over () as v;
show create table wdecimal_max_zero;
select v from wdecimal_max_zero;
select lag(cast(0.1 as decimal(65,65)),1,0) over () as lg,
       lead(cast(0.1 as decimal(65,65)),1,0) over () as ld;
create table wdecimal_max_control as
select lag(cast(0.1 as decimal(65,65)),0) over () as no_default,
       lag(cast(0.1 as decimal(65,65)),0,cast(0 as decimal(65,65))) over () as same_default;
show create table wdecimal_max_control;
select * from wdecimal_max_control;
create view wdecimal_max_view as select lead(cast(0.1 as decimal(65,65)),0,0) over () as v;
show columns from wdecimal_max_view;
select * from wdecimal_max_view;

-- JSON 显示容量未知，默认 VARCHAR 的宽度不能成为源值的截断上界。
create table wjson (j json, d254 varchar(254), d255 varchar(255), d256 varchar(256));
insert into wjson values (json_object('s', repeat('a',300)), '{}', '{}', '{}');
select length(cast(j as char)) as original_length,
       length(cast(lag(j,0) over () as char)) as lag_control,
       length(cast(lead(j,0) over () as char)) as lead_control,
       length(cast(coalesce(j,d255) as char)) as coalesce_control,
       length(cast(lag(j,0,d254) over () as char)) as lag_254,
       length(cast(lag(j,0,d255) over () as char)) as lag_255,
       length(cast(lag(j,0,d256) over () as char)) as lag_256,
       length(cast(lead(j,0,d254) over () as char)) as lead_254,
       length(cast(lead(j,0,d255) over () as char)) as lead_255,
       length(cast(lead(j,0,d256) over () as char)) as lead_256 from wjson;
select cast(lag(j,0,d255) over () as char) = cast(j as char) as lag_equal,
       cast(lead(j,0,d255) over () as char) = cast(j as char) as lead_equal,
       json_valid(lag(j,0,d255) over ()) as lag_valid,
       json_valid(lead(j,0,d255) over ()) as lead_valid from wjson;
select lag(j,1,d255) over () as lg, lead(j,1,d255) over () as ld from wjson;
select length(lag(d255,1,j) over ()) as lag_default_length,
       length(lead(d255,1,j) over ()) as lead_default_length,
       json_valid(lag(d255,1,j) over ()) as lag_default_valid,
       json_valid(lead(d255,1,j) over ()) as lead_default_valid from wjson;
create table wjson_copy as select lag(j,0,d255) over () as lg, lead(j,0,d255) over () as ld from wjson;
show create table wjson_copy;
select length(lg) as lg, length(ld) as ld, json_valid(lg) as lg_valid, json_valid(ld) as ld_valid from wjson_copy;
create view wjson_view as select lag(j,0,d255) over () as lg, lead(j,0,d255) over () as ld from wjson;
show columns from wjson_view;
select length(lg) as lg, length(ld) as ld, json_valid(lg) as lg_valid, json_valid(ld) as ld_valid from wjson_view;

-- BIT 是无符号整数域，不能令 DECIMAL 源值丢失小数；两方向均需覆盖。
create table wbit (d decimal(5,2), b bit(3), u bit(64));
insert into wbit values (12.34, 7, 18446744073709551615);
select d as original_value, lag(d,0) over () as control_value,
       lag(d,0,b) over () as lg, lead(d,0,b) over () as ld from wbit;
select lag(d,1,b) over () as bit_default, lead(d,1,u) over () as uint64_default,
       lag(b,0,d) over () as bit_source, lead(u,0,d) over () as uint64_source from wbit;
create table wbit_copy as select lag(d,0,b) over () as d, lead(b,0,d) over () as b,
       lag(u,0,d) over () as u from wbit;
show create table wbit_copy;
select * from wbit_copy;
create view wbit_view as select lag(d,0,b) over () as d, lead(b,0,d) over () as b from wbit;
show columns from wbit_view;
select * from wbit_view;

-- VARCHAR 容量以字符计，TEXT/BLOB 子类型以字节计；三字节及四字节字符都不能截断。
create table wunicode (id int, v varchar(100), wide varchar(16383), t tinytext, b tinyblob);
insert into wunicode values (1,repeat('汉',100),repeat('😀',16383),'x',x'78'),
       (2,repeat('😀',100),null,'x',x'78');
select id, length(v) as original_length, length(lag(v,0) over (order by id)) as control_length,
       length(lag(v,0,t) over (order by id)) as lg, length(lead(v,0,t) over (order by id)) as ld,
       length(lag(v,0,b) over (order by id)) as blob_control,
       lag(v,0,t) over (order by id) = v as text_equal from wunicode order by id;
select id, length(lag(t,2,v) over (order by id)) as text_default,
       length(lead(b,2,v) over (order by id)) as blob_default from wunicode order by id;
set @wunicode_sql_mode = @@session.sql_mode;
set session sql_mode = 'STRICT_TRANS_TABLES';
create table wunicode_strict as select id, lag(v,0,t) over (order by id) as v,
       lead(v,0,b) over (order by id) as b, lag(wide,0,t) over (order by id) as wide from wunicode;
show create table wunicode_strict;
select id,length(v) as v_length,length(b) as b_length,length(wide) as wide_length from wunicode_strict order by id;
set session sql_mode = '';
create table wunicode_permissive as select id, lag(v,0,t) over (order by id) as v,
       lead(v,0,b) over (order by id) as b from wunicode;
select id,length(v) as v_length,length(b) as b_length from wunicode_permissive order by id;
create view wunicode_view as select id, lag(v,0,t) over (order by id) as v from wunicode;
show columns from wunicode_view;
select id,length(v) as v_length from wunicode_view order by id;
set session sql_mode = @wunicode_sql_mode;

-- 无损公共域超过 SQL 精度上限时拒绝绑定，不留下无法导出的目录类型。
create table wdecimal_rejected as select lag(cast(0.1 as decimal(65,65)),0,1) over () as v;
show tables like 'wdecimal_rejected';
create table wdecimal_rejected as select lead(cast(0.1 as decimal(65,64)),0,cast(10 as decimal(3,1))) over () as v;
show tables like 'wdecimal_rejected';

drop database window_lag_lead_default_type;
