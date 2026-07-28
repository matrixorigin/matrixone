drop database if exists block_or_spk;
create database block_or_spk;
use block_or_spk;

select enable_fault_injection();

-- single pk: int
create table spk_int(a int primary key, b int);
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'block_or_spk.spk_int');
insert into spk_int select result, result * 10 from generate_series(1, 8192) g;
select a from spk_int where a = 10 or a in (20, 30, 40) order by a;
select a from spk_int where a between 100 and 102 or a >= 8190 order by a;
select a from spk_int where a < 4 or a > 8189 order by a;
select a from spk_int where a <= 5 or a >= 8191 order by a;
select a from spk_int where (a = 11 and a = 12) or a = 13 order by a;
select a from spk_int where a < 4 or a between 256 and 258 or a in (500, 600) or a >= 8191 order by a;
drop table spk_int;

-- single pk: bigint unsigned
create table spk_uint(a bigint unsigned primary key, b bigint);
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'block_or_spk.spk_uint');
insert into spk_uint select result, result * 10 from generate_series(1, 8192) g;
select a from spk_uint where a in (1, 2, 3) or a = 10 order by a;
select a from spk_uint where a between 256 and 258 or a > 8190 order by a;
select a from spk_uint where a < 4 or a > 8191 order by a;
select a from spk_uint where (a = 21 and a = 22) or a = 23 order by a;
select a from spk_uint where a <= 3 or a >= 8192 or a in (128, 256) or a between 768 and 769 order by a;
drop table spk_uint;

-- single pk: double
create table spk_double(a double primary key, b double);
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'block_or_spk.spk_double');
insert into spk_double select result + 0.5, result * 1.0 from generate_series(1, 8192) g;
select a from spk_double where a = 10.5 or a in (20.5, 30.5, 40.5) order by a;
select a from spk_double where a between 100.5 and 102.5 or a >= 8190.5 order by a;
select a from spk_double where a < 4.5 or a > 8189.5 order by a;
select a from spk_double where a <= 5.5 or a >= 8191.5 order by a;
select a from spk_double where (a = 50.5 and a = 51.5) or a = 52.5 order by a;
select a from spk_double where a < 4.5 or a between 256.5 and 258.5 or a in (500.5, 600.5) or a >= 8191.5 order by a;
drop table spk_double;

-- single pk: decimal
create table spk_decimal(a decimal(20, 0) primary key, b decimal(20, 0));
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'block_or_spk.spk_decimal');
insert into spk_decimal select result, result * 10 from generate_series(1, 8192) g;
select a from spk_decimal where a = 10 or a in (20, 30, 40) order by a;
select a from spk_decimal where a between 300 and 302 or a >= 8190 order by a;
select a from spk_decimal where a < 4 or a > 8189 order by a;
select a from spk_decimal where a <= 5 or a >= 8192 order by a;
select a from spk_decimal where (a = 60 and a = 61) or a = 62 order by a;
select a from spk_decimal where a < 4 or a between 700 and 702 or a in (900, 1000) or a >= 8191 order by a;
drop table spk_decimal;

-- single pk: date
create table spk_date(a date primary key, b int);
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'block_or_spk.spk_date');
insert into spk_date select date '2024-01-01' + interval result day, result from generate_series(1, 8192) g;
select a from spk_date where a = date '2024-01-10' or a in (date '2024-01-20', date '2024-01-30') order by a;
select a from spk_date where a between date '2024-02-10' and date '2024-02-12' or a >= date '2046-06-01' order by a;
select a from spk_date where a < date '2024-01-05' or a > date '2046-06-01' order by a;
select a from spk_date where a <= date '2024-01-06' or a >= date '2046-06-05' order by a;
select a from spk_date where (a = date '2024-02-01' and a = date '2024-02-02') or a = date '2024-02-03' order by a;
select a from spk_date where a < date '2024-01-05' or a between date '2024-03-01' and date '2024-03-03' or a in (date '2024-06-01', date '2024-07-01') or a >= date '2046-06-05' order by a;
drop table spk_date;

-- single pk: timestamp
create table spk_ts(a timestamp primary key, b int);
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'block_or_spk.spk_ts');
insert into spk_ts select timestamp '2024-01-01 00:00:00' + interval result minute, result from generate_series(1, 8192) g;
select a from spk_ts where a = timestamp '2024-01-01 00:10:00' or a in (timestamp '2024-01-01 00:20:00', timestamp '2024-01-01 00:30:00') order by a;
select a from spk_ts where a between timestamp '2024-01-01 03:00:00' and timestamp '2024-01-01 03:02:00' or a >= timestamp '2024-01-06 16:30:00' order by a;
select a from spk_ts where a < timestamp '2024-01-01 00:05:00' or a > timestamp '2024-01-06 16:30:00' order by a;
select a from spk_ts where a <= timestamp '2024-01-01 00:08:00' or a >= timestamp '2024-01-06 16:31:00' order by a;
select a from spk_ts where (a = timestamp '2024-01-01 01:00:00' and a = timestamp '2024-01-01 01:01:00') or a = timestamp '2024-01-01 01:02:00' order by a;
select a from spk_ts where a < timestamp '2024-01-01 00:04:00' or a between timestamp '2024-01-01 02:00:00' and timestamp '2024-01-01 02:02:00' or a in (timestamp '2024-01-01 08:00:00', timestamp '2024-01-01 09:00:00') or a >= timestamp '2024-01-06 16:31:00' order by a;
drop table spk_ts;

-- single pk: uuid
create table spk_uuid(a uuid primary key, b int);
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'block_or_spk.spk_uuid');
insert into spk_uuid
select cast(concat('00000000-0000-0000-0000-', lpad(lower(hex(result)), 12, '0')) as uuid), result
from generate_series(1, 8192) g;
select a from spk_uuid where a = cast('00000000-0000-0000-0000-00000000000a' as uuid) or a in (
    cast('00000000-0000-0000-0000-000000000014' as uuid),
    cast('00000000-0000-0000-0000-00000000001e' as uuid),
    cast('00000000-0000-0000-0000-000000000028' as uuid)
) order by a;
select a from spk_uuid where a between cast('00000000-0000-0000-0000-000000000064' as uuid) and cast('00000000-0000-0000-0000-000000000066' as uuid) or a >= cast('00000000-0000-0000-0000-000000001ffe' as uuid) order by a;
select a from spk_uuid where (a = cast('00000000-0000-0000-0000-000000000032' as uuid) and a = cast('00000000-0000-0000-0000-000000000033' as uuid)) or a = cast('00000000-0000-0000-0000-000000000034' as uuid) order by a;
select a from spk_uuid where a <= cast('00000000-0000-0000-0000-000000000003' as uuid) or a >= cast('00000000-0000-0000-0000-000000001fff' as uuid) order by a;
select a from spk_uuid where a <= cast('00000000-0000-0000-0000-000000000005' as uuid) or a >= cast('00000000-0000-0000-0000-000000002000' as uuid) order by a;
select a from spk_uuid where a < cast('00000000-0000-0000-0000-000000000004' as uuid) or a between cast('00000000-0000-0000-0000-000000000100' as uuid) and cast('00000000-0000-0000-0000-000000000102' as uuid) or a in (cast('00000000-0000-0000-0000-000000000200' as uuid), cast('00000000-0000-0000-0000-000000000300' as uuid)) or a >= cast('00000000-0000-0000-0000-000000001fff' as uuid) order by a;
drop table spk_uuid;

-- single pk: varchar with non-prefix filters
create table spk_varchar(a varchar(64) primary key, b int);
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'block_or_spk.spk_varchar');
insert into spk_varchar select cast(result as varchar), result from generate_series(1, 8192) g;
select a from spk_varchar where a = '10' or a in ('20', '30', '40') order by cast(a as int);
select a from spk_varchar where a between '8000' and '8003' or a between '900' and '903' order by cast(a as int);
select a from spk_varchar where a in ('5', '15', '25') or a = '512' order by cast(a as int);
select a from spk_varchar where (a = '200' and a = '201') or a = '300' order by cast(a as int);
select a from spk_varchar where a between '2500' and '2502' or a between '6000' and '6001' or a in ('700', '800') or a between '950' and '952' order by cast(a as int);
drop table spk_varchar;

drop database block_or_spk;

select disable_fault_injection();
