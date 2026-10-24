drop database if exists shuffle_covering_index;
create database shuffle_covering_index;
use shuffle_covering_index;

-- 有序字符串键和足够的 NDV，使真实规划器选择 Range Shuffle。
-- 表达式路由的类型、边界和 NULL 排列由小型 UT 覆盖。
create table voucher_line (
    id bigint primary key,
    racct varchar(16),
    voucher_header_id varchar(255),
    key idx_racct_header (racct, voucher_header_id)
);
create table voucher_plain (
    id bigint primary key,
    racct varchar(16),
    voucher_header_id varchar(255)
);
create table voucher_main (voucher_header_id varchar(255) primary key);
insert into voucher_line
select result, if(result % 10 = 0, 'other', '6608010000'),
       concat(lpad(cast(result as char), 8, '0'), '-voucher')
from generate_series(1, 300000) g;
insert into voucher_line values
(300001, '6608010000', '00000001-voucher'),
(300002, '6608010000', '00000001-voucher'),
(300003, '6608010000', null),
(300004, '6608010000', null),
(300005, '6608010000', '99999999-unmatched');
insert into voucher_plain select * from voucher_line;
insert into voucher_main
select concat(lpad(cast(result as char), 8, '0'), '-voucher')
from generate_series(1, 149000) g;
select mo_ctl('dn', 'flush', 'shuffle_covering_index.voucher_line');
select mo_ctl('dn', 'flush', 'shuffle_covering_index.voucher_plain');
select mo_ctl('dn', 'flush', 'shuffle_covering_index.voucher_main');
-- 同时要求索引改写和表达式 Range，防止普通计划产生假阳性。
-- @regex("Group Key: serial_extract.*shuffle: range",true)
explain verbose
select count(1) from (
    select distinct voucher_header_id from voucher_line where racct = '6608010000'
) q;
select count(1) from (
    select distinct voucher_header_id from voucher_line where racct = '6608010000'
) q;
select count(1) from (
    select voucher_header_id from voucher_line where racct = '6608010000'
    group by voucher_header_id
) q;
select count(1) from (
    select distinct voucher_header_id from voucher_plain where racct = '6608010000'
) q;

-- @regex("Join Cond:.*shuffle: range.*serial_extract",true)
explain verbose
select count(1) from voucher_line vl join voucher_main vm
on vl.voucher_header_id = vm.voucher_header_id where vl.racct = '6608010000';
select count(1) from voucher_line vl join voucher_main vm
on vl.voucher_header_id = vm.voucher_header_id where vl.racct = '6608010000';
select count(1) from voucher_plain vl join voucher_main vm
on vl.voucher_header_id = vm.voucher_header_id where vl.racct = '6608010000';

select count(1) from voucher_main vm join (
    select voucher_header_id from voucher_line where racct = '6608010000'
    group by voucher_header_id
) g on vm.voucher_header_id = g.voucher_header_id;
select count(1) from voucher_main vm join (
    select voucher_header_id from voucher_plain where racct = '6608010000'
    group by voucher_header_id
) g on vm.voucher_header_id = g.voucher_header_id;

-- 重复值和 NULL 必须进入同组，不能只检查总行数。
select frequency, count(*) from (
    select voucher_header_id, count(*) as frequency
    from voucher_line where racct = '6608010000' group by voucher_header_id
) g group by frequency order by frequency;
select frequency, count(*) from (
    select voucher_header_id, count(*) as frequency
    from voucher_plain where racct = '6608010000' group by voucher_header_id
) g group by frequency order by frequency;
select count(1) from (
    select distinct voucher_header_id from voucher_line where racct = 'absent'
) q;
select count(1) from (
    select voucher_header_id from voucher_line where racct = '6608010000'
    group by voucher_header_id having count(*) > 10
) q;
drop database shuffle_covering_index;
