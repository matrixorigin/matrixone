----------------
--single value--
----------------
-- bool value
set @b_var1 = 1 = 0,@b_var2 = 0 = 0;
select @b_var1,@b_var2;
-- int8
set @i8_var1 = 127,@i8_var2 = -128;
select @i8_var1,@i8_var2;
-- int16
set @i16_var1 = 32767,@i16_var2 = -32768;
select @i16_var1,@i16_var2;
--int32
set @i32_var1 = 2147483647,@i32_var2 = -2147483648;
select @i32_var1,@i32_var2;
--int64
set @i64_var1 = 9223372036854775807,@i64_var2 = -9223372036854775808;
select @i64_var1,@i64_var2;
-- uint8
set @u8_var1 = 0,@u8_var2 = 255;
select @u8_var1,@u8_var2;
-- uint16
set @u16_var1 = 0,@u16_var2 = 65535;
select @u16_var1,@u16_var2;
--uint32
set @u32_var1 = 0,@u32_var2 = 4294967295;
select @u32_var1,@u32_var2;
--uint64
set @u64_var1 = 0,@u64_var2 = 18446744073709551615;
select @u64_var1,@u64_var2;
--float32
set @f32_var1 = 1.1754943508222875e-38,@f32_var2 = 3.4028234663852886e+38;
select @f32_var1,@f32_var2;
--float64
set @f64_var1 = 2.2250738585072014e-308,@f64_var2 = 1.7976931348623157e+308;
select @f64_var1,@f64_var2;
--char
set @ch_var='abc',@varch_var=cast('def' as varchar),@bin_var=cast('1001' as binary),@varbin_var=cast('1001' as varbinary(6)),@text_var=cast('ghi' as text),@blob_var=cast('1010' as blob);
select @ch_var,@varch_var,@bin_var,@varbin_var,@text_var,@blob_var;
--decimal64
set @d64_var1 = cast(9223372036854775807 as decimal),@d64_var2 = cast(-9223372036854775808 as decimal);
select @d64_var1,@d64_var2;
--decimal128
set @d128_var1 = cast(170141183460469231731687303715884105727 as decimal),@d128_var2 = cast(-170141183460469231731687303715884105728 as decimal);
select @d128_var1,@d128_var2;
--json
set @json_var1 = cast('{"a":1,"b":2}' as json),@json_var2 = cast('[1,2,3]' as json);
select @json_var1,@json_var2;
--uuid
set @uuid_var1 = cast('00000000-0000-0000-0000-000000000000' as uuid),@uuid_var2 = cast('ffffffff-ffff-ffff-ffff-ffffffffffff' as uuid);
select @uuid_var1,@uuid_var2;
--time
set @time_var1 = cast('00:00:00' as time),@time_var2 = cast('23:59:59' as time);
select @time_var1,@time_var2;
--datetime
set @dt_var1 = cast('1000-01-01 00:00:00' as datetime),@dt_var2 = cast('9999-12-31 23:59:59' as datetime);
select @dt_var1,@dt_var2;
--timestamp
set @ts_var1 = cast('1970-01-01 00:00:00' as timestamp),@ts_var2 = cast('2038-01-19 03:14:07' as timestamp);
select @ts_var1,@ts_var2;
---------------------
---compound value----
---------------------
drop table if exists seq_t1;
create table seq_t1(a int, b int);
insert into seq_t1 values (1,2);
insert into seq_t1 values (1,2);

set @single_var1 = (select a from seq_t1 limit 1),@single_var2 = (select b from seq_t1 limit 1);
select @single_var1,@single_var2;

set @single_var2 = (select a from seq_t1 limit 2);
set @single_var3 = (select a,b from seq_t1 limit 1);
select @single_var3;
set @single_var3 = (select b,a from seq_t1 limit 1);
select @single_var3;

set @single_var4 = (select count(a) from seq_t1 limit 1),@single_var5 = (select min(b) from seq_t1 limit 1),@single_var6 = (select max(a) from seq_t1 limit 1);
select @single_var4,@single_var5,@single_var6;

set @single_var7 = (select a,b from seq_t1);
set @single_var8 = (select b,a from seq_t1);

drop table if exists seq_t1;

----sequence function----
create sequence seq1;
set @seq_var1 = nextval('seq1'),@seq_var2 = nextval('seq1'),@seq_var3 = nextval('seq1');
select @seq_var1,@seq_var2,@seq_var3;
select currval('seq1'),nextval('seq1'),lastval(),currval('seq1'),lastval();
select setval('seq1', 50);
select currval('seq1'),nextval('seq1'),lastval(),currval('seq1'),lastval();
drop sequence seq1;

-- @bvt:issue#9847
create table seq_table_01(col1 int);
create sequence seq_14  increment 50 start with 126 no cycle;
--126[176]
select nextval('seq_14');
--176[226]
select nextval('seq_14'),currval('seq_14');
--226[276]
insert into seq_table_01 select nextval('seq_14');
select currval('seq_14');
--276[326]
insert into seq_table_01 values(nextval('seq_14'));
--326[376]
insert into seq_table_01 values(nextval('seq_14'));
--376[426]
insert into seq_table_01 values(nextval('seq_14'));
--426[476]
insert into seq_table_01 values(nextval('seq_14'));
--426

select currval('seq_14');
select * from seq_table_01;

drop sequence seq_14;
drop table seq_table_01;
-- @bvt:issue