-- @suite
-- @case
-- @desc: 多键 Top 的首键块过滤保留次键胜者，并支持远程扫描
-- @label:bvt

drop database if exists top_leading_key_filter;
create database top_leading_key_filter;
use top_leading_key_filter;
set @saved_optimizer_hints = @@optimizer_hints;

create table history (id int not null, k int not null, account_id int not null) cluster by(k, account_id);
insert into history values (1, 9, 1), (2, 8, 0), (3, 7, 1);
select mo_ctl('dn', 'flush', 'top_leading_key_filter.history');
insert into history values (9, 9, 0), (8, 9, 1), (4, 10, 0);
select mo_ctl('dn', 'flush', 'top_leading_key_filter.history');

-- 同首键跨对象，混合方向不能丢掉较晚遇到的次键胜者。
set session optimizer_hints = 'execType=1';
select id, k from history order by k desc, id desc limit 3;
select id, k from history order by k desc, id asc limit 3;
select id, k from history order by k asc, id desc limit 3;
select id, k from history order by k desc, id desc limit 2 offset 1;
select id, k from history order by k desc, id desc limit 20;
select id, k from history where k < 0 order by k desc, id desc limit 3;

-- AP/远程 pipeline 必须与单 CN 和禁用优化的参考结果一致。
set session optimizer_hints = 'execType=2';
select id, k from history order by k desc, id desc limit 3;
select id, k from history order by k desc, id asc limit 3;
select id, k from history order by k asc, id desc limit 3;
select id, k from history order by k desc, id desc limit 2 offset 1;
set session optimizer_hints = 'execType=2,sendMessageFromTopToScan=1';
select id, k from history order by k desc, id asc limit 3;

-- NULL 和表达式首键保守回退，不改变排序语义。
create table nullable_keys (id int not null, k int);
insert into nullable_keys values (1, null), (2, 4), (3, null);
select mo_ctl('dn', 'flush', 'top_leading_key_filter.nullable_keys');
set session optimizer_hints = 'execType=2';
select id, k from nullable_keys order by k asc, id desc limit 2;
select id, k from nullable_keys order by k desc, id desc limit 2;
select id, k from history order by -k asc, id desc limit 3;

-- 含点列名与限定名可能相同，必须保守回退，不能误用另一列的 zonemap。
create table quoted_keys (id int not null, k int not null, `quoted_keys.k` int not null);
insert into quoted_keys values (1, 9, 1), (2, 8, 90), (3, 10, 0);
select mo_ctl('dn', 'flush', 'top_leading_key_filter.quoted_keys');
set session optimizer_hints = 'execType=1';
select id, k, `quoted_keys.k` from quoted_keys order by `quoted_keys.k` desc, id desc limit 2;
set session optimizer_hints = 'execType=2';
select id, k, `quoted_keys.k` from quoted_keys order by k desc, id desc limit 2;
select id, k, `quoted_keys.k` from quoted_keys order by `quoted_keys.k` desc, id desc limit 2;
-- main 的低精度标量仅允许有限值，保留其原有数值比较语义。
create table narrow_keys (id int not null, k bf16 not null);
insert into narrow_keys values (1, -2), (2, -1);
select mo_ctl('dn', 'flush', 'top_leading_key_filter.narrow_keys');
insert into narrow_keys values (3, 1), (4, 0);
select mo_ctl('dn', 'flush', 'top_leading_key_filter.narrow_keys');
insert into narrow_keys values (5, 1), (6, 0);
select mo_ctl('dn', 'flush', 'top_leading_key_filter.narrow_keys');
select id, k from narrow_keys order by k desc, id desc limit 2;
set session optimizer_hints = 'execType=2,sendMessageFromTopToScan=1';
select id, k from narrow_keys order by k desc, id desc limit 2;
-- JSON 的语义数值序不等于二进制字节序；跨对象时不得按字节阈值剪枝。
create table json_keys (id int not null, k json not null);
insert into json_keys values (1, '256');
select mo_ctl('dn', 'flush', 'top_leading_key_filter.json_keys');
insert into json_keys values (2, '1');
select mo_ctl('dn', 'flush', 'top_leading_key_filter.json_keys');
set session optimizer_hints = 'execType=1';
select id, k from json_keys order by k asc, id desc limit 1;
select id, k from json_keys order by k desc, id desc limit 1;
select id, k from json_keys order by k asc limit 1;
select id, k from json_keys order by k desc limit 1;
set session optimizer_hints = 'execType=1,sendMessageFromTopToScan=1';
select id, k from json_keys order by k asc, id desc limit 1;
select id, k from json_keys order by k desc, id desc limit 1;
set session optimizer_hints = 'execType=2';
select id, k from json_keys order by k asc, id desc limit 1;
select id, k from json_keys order by k desc, id desc limit 1;
select id, k from json_keys order by k asc limit 1;
select id, k from json_keys order by k desc limit 1;
set session optimizer_hints = 'execType=2,sendMessageFromTopToScan=1';
select id, k from json_keys order by k asc, id desc limit 1;
select id, k from json_keys order by k desc, id desc limit 1;
-- GEOMETRY 可以排序，但 zonemap 尚无比较器；两个持久化对象必须回退。
create table geometry_keys (id int not null, k geometry not null);
insert into geometry_keys values (1, st_geomfromtext('POINT(0 0)'));
select mo_ctl('dn', 'flush', 'top_leading_key_filter.geometry_keys');
insert into geometry_keys values (2, st_geomfromtext('POINT(1 1)'));
select mo_ctl('dn', 'flush', 'top_leading_key_filter.geometry_keys');
set session optimizer_hints = 'execType=1';
select id from geometry_keys order by k asc, id desc limit 1;
select id from geometry_keys order by k desc, id desc limit 1;
select id from geometry_keys order by k asc limit 1;
select id from geometry_keys order by k desc limit 1;
set session optimizer_hints = 'execType=1,sendMessageFromTopToScan=1';
select id from geometry_keys order by k asc, id desc limit 1;
select id from geometry_keys order by k desc, id desc limit 1;
set session optimizer_hints = 'execType=2';
select id from geometry_keys order by k asc, id desc limit 1;
select id from geometry_keys order by k desc, id desc limit 1;
select id from geometry_keys order by k asc limit 1;
select id from geometry_keys order by k desc limit 1;
set session optimizer_hints = 'execType=2,sendMessageFromTopToScan=1';
select id from geometry_keys order by k asc, id desc limit 1;
select id from geometry_keys order by k desc, id desc limit 1;
set session optimizer_hints = @saved_optimizer_hints;
drop database top_leading_key_filter;
