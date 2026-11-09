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

-- A repeated leading key must not override its first direction or the tie breaker.
select id, k from history order by k desc, id asc, k asc limit 3;
select id, k from history order by k asc, id desc, k desc limit 3;
set session optimizer_hints = 'execType=1,sendMessageFromTopToScan=1';
select id, k from history order by k desc, id asc, k asc limit 3;

-- AP/远程 pipeline 必须与单 CN 和禁用优化的参考结果一致。
set session optimizer_hints = 'execType=2';
select id, k from history order by k desc, id desc limit 3;
select id, k from history order by k desc, id asc limit 3;
select id, k from history order by k asc, id desc limit 3;
select id, k from history order by k desc, id desc limit 2 offset 1;
select id, k from history order by k desc, id asc, k asc limit 3;
set session optimizer_hints = 'execType=2,sendMessageFromTopToScan=1';
select id, k from history order by k desc, id asc limit 3;
select id, k from history order by k desc, id asc, k asc limit 3;

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
-- 空间首键的普通 Top 支持字节序，但存储 zonemap 比较器不支持，必须全扫描回退。
-- 同一 geometry 跨两个持久化对象，次键赢家必须保留。
create table spatial_keys (id int not null, g geometry not null);
insert into spatial_keys values (1, st_geomfromtext('POINT(1 1)'));
select mo_ctl('dn', 'flush', 'top_leading_key_filter.spatial_keys');
insert into spatial_keys values (2, st_geomfromtext('POINT(1 1)'));
select mo_ctl('dn', 'flush', 'top_leading_key_filter.spatial_keys');
set session optimizer_hints = 'execType=1';
select id from spatial_keys order by g desc, id desc limit 1;
select id from spatial_keys order by g asc, id asc limit 1;
set session optimizer_hints = 'execType=1,sendMessageFromTopToScan=1';
select id from spatial_keys order by g desc, id desc limit 1;
select id from spatial_keys order by g asc, id asc limit 1;
set session optimizer_hints = 'execType=2';
select id from spatial_keys order by g desc, id desc limit 1;
select id from spatial_keys order by g asc, id asc limit 1;
set session optimizer_hints = 'execType=2,sendMessageFromTopToScan=1';
select id from spatial_keys order by g desc, id desc limit 1;
select id from spatial_keys order by g asc, id asc limit 1;
set session optimizer_hints = @saved_optimizer_hints;
drop database top_leading_key_filter;
