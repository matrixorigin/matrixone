-- NaN 边界可能隐藏普通值，已初始化的摘要也不能证明普通值不存在。
drop database if exists legacy_nan_zonemap;
create database legacy_nan_zonemap;
use legacy_nan_zonemap;
set @saved_nan_hints=@@optimizer_hints;
create table poisoned (id int primary key, v32 float, v64 double);
insert into poisoned values (1,cast('NaN' as float),cast('NaN' as double)),(2,7,7);
select id from poisoned where id=2 and v64=7;
-- @regex("returnStr.*OK",true)
select mo_ctl('dn','flush','legacy_nan_zonemap.poisoned');
set optimizer_hints='';
select id from poisoned where id=2 and v64=7;

-- 会话提示仅用于稳定覆盖已有消费者，不代表默认准入或性能结论。
-- id 条件将断言限定为普通值 7，不依赖 NaN 本身的行比较语义。
set optimizer_hints='blockFilter=1';
-- @regex("Block Filter Cond:[^\r\n]*v64",true)
explain select id from poisoned where id=2 and v64=7;
select id from poisoned where id=2 and v32=7;
select id from poisoned where id=2 and v32>0;
select id from poisoned where id=2 and v32>=7;
select id from poisoned where id=2 and v32<8;
select id from poisoned where id=2 and v32<=7;
select id from poisoned where id=2 and v32 between 6 and 8;
select id from poisoned where id=2 and v64=7;
select id from poisoned where id=2 and v64>0;
select id from poisoned where id=2 and v64>=7;
select id from poisoned where id=2 and v64<8;
select id from poisoned where id=2 and v64<=7;
select id from poisoned where id=2 and v64 between 6 and 8;
select count(*) from poisoned where id=2 and v64>8;

-- 禁用优化必须返回相同普通值，行过滤仍负责排除不匹配数据。
set optimizer_hints='blockFilter=2';
select id from poisoned where id=2 and v32=7;
select id from poisoned where id=2 and v64>0;
select count(*) from poisoned where id=2 and v64>8;
set optimizer_hints=@saved_nan_hints;
drop database legacy_nan_zonemap;
