drop database if exists starlark_parameter_types;
create database starlark_parameter_types;
use starlark_parameter_types;
-- Original issue reproduction: every invalid output must reject without overwriting any caller variable.
create procedure original_out(out oi int, out ov varchar(3), inout io int) language 'starlark' $$
out_oi = "not-an-int"
out_ov = "too-long"
out_io = "also-not-int"
$$;
set @oi=null, @ov=null, @io=7;
-- @regex("invalid argument cast to int, bad value (not-an-int|also-not-int)|Can't cast 'too-long' to VARCHAR type. Src length 8 is larger than Dest length 3",true)
call original_out(@oi,@ov,@io);
select @oi is null and @ov is null and @io=7 as preserved;
-- OUT tinyint: valid conversion
create or replace procedure typed_value(out v tinyint) language 'starlark' $$out_v = 127$$;
set @v=99;
call typed_value(@v);
select @v=127 as matches;
-- OUT tinyint: valid conversion
create or replace procedure typed_value(out v tinyint) language 'starlark' $$out_v = -128$$;
set @v=99;
call typed_value(@v);
select @v=-128 as matches;
-- OUT tinyint: reject and preserve
create or replace procedure typed_value(out v tinyint) language 'starlark' $$out_v = 128$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- OUT tinyint unsigned: valid conversion
create or replace procedure typed_value(out v tinyint unsigned) language 'starlark' $$out_v = 255$$;
set @v=99;
call typed_value(@v);
select @v=255 as matches;
-- OUT tinyint unsigned: reject and preserve
create or replace procedure typed_value(out v tinyint unsigned) language 'starlark' $$out_v = 256$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- OUT tinyint unsigned: reject and preserve
create or replace procedure typed_value(out v tinyint unsigned) language 'starlark' $$out_v = -1$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- OUT smallint: valid conversion
create or replace procedure typed_value(out v smallint) language 'starlark' $$out_v = 32767$$;
set @v=99;
call typed_value(@v);
select @v=32767 as matches;
-- OUT smallint: reject and preserve
create or replace procedure typed_value(out v smallint) language 'starlark' $$out_v = 32768$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- OUT smallint unsigned: valid conversion
create or replace procedure typed_value(out v smallint unsigned) language 'starlark' $$out_v = 65535$$;
set @v=99;
call typed_value(@v);
select @v=65535 as matches;
-- OUT smallint unsigned: reject and preserve
create or replace procedure typed_value(out v smallint unsigned) language 'starlark' $$out_v = 65536$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- OUT smallint unsigned: reject and preserve
create or replace procedure typed_value(out v smallint unsigned) language 'starlark' $$out_v = -1$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- OUT mediumint: valid conversion
create or replace procedure typed_value(out v mediumint) language 'starlark' $$out_v = 8388607$$;
set @v=99;
call typed_value(@v);
select @v=8388607 as matches;
-- OUT mediumint: reject and preserve
create or replace procedure typed_value(out v mediumint) language 'starlark' $$out_v = 8388608$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- OUT mediumint unsigned: valid conversion
create or replace procedure typed_value(out v mediumint unsigned) language 'starlark' $$out_v = 16777215$$;
set @v=99;
call typed_value(@v);
select @v=16777215 as matches;
-- OUT mediumint unsigned: reject and preserve
create or replace procedure typed_value(out v mediumint unsigned) language 'starlark' $$out_v = 16777216$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- OUT mediumint unsigned: reject and preserve
create or replace procedure typed_value(out v mediumint unsigned) language 'starlark' $$out_v = -1$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- OUT int: valid conversion
create or replace procedure typed_value(out v int) language 'starlark' $$out_v = 2147483647$$;
set @v=99;
call typed_value(@v);
select @v=2147483647 as matches;
-- OUT int: reject and preserve
create or replace procedure typed_value(out v int) language 'starlark' $$out_v = 2147483648$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- OUT int unsigned: valid conversion
create or replace procedure typed_value(out v int unsigned) language 'starlark' $$out_v = 4294967295$$;
set @v=99;
call typed_value(@v);
select @v=4294967295 as matches;
-- OUT int unsigned: reject and preserve
create or replace procedure typed_value(out v int unsigned) language 'starlark' $$out_v = 4294967296$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- OUT int unsigned: reject and preserve
create or replace procedure typed_value(out v int unsigned) language 'starlark' $$out_v = -1$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- OUT bigint: valid conversion
create or replace procedure typed_value(out v bigint) language 'starlark' $$out_v = 9223372036854775807$$;
set @v=99;
call typed_value(@v);
select @v=9223372036854775807 as matches;
-- OUT bigint: reject and preserve
create or replace procedure typed_value(out v bigint) language 'starlark' $$out_v = 9223372036854775808$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- OUT bigint unsigned: valid conversion
create or replace procedure typed_value(out v bigint unsigned) language 'starlark' $$out_v = 18446744073709551615$$;
set @v=99;
call typed_value(@v);
select @v=18446744073709551615 as matches;
-- OUT bigint unsigned: reject and preserve
create or replace procedure typed_value(out v bigint unsigned) language 'starlark' $$out_v = 1 << 64$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- OUT bigint unsigned: reject and preserve
create or replace procedure typed_value(out v bigint unsigned) language 'starlark' $$out_v = -1$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- OUT decimal(30,0): valid conversion
create or replace procedure typed_value(out v decimal(30,0)) language 'starlark' $$out_v = 18446744073709551615$$;
set @v=99;
call typed_value(@v);
select @v='18446744073709551615' as matches;
-- OUT decimal(30,0): valid conversion
create or replace procedure typed_value(out v decimal(30,0)) language 'starlark' $$out_v = 1 << 64$$;
set @v=99;
call typed_value(@v);
select @v='18446744073709551616' as matches;
-- OUT decimal(30,0): valid conversion
create or replace procedure typed_value(out v decimal(30,0)) language 'starlark' $$out_v = -(1 << 64)$$;
set @v=99;
call typed_value(@v);
select @v='-18446744073709551616' as matches;
-- OUT decimal(30,0): reject and preserve
create or replace procedure typed_value(out v decimal(30,0)) language 'starlark' $$out_v = 1000000000000000000000000000000$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- OUT decimal(5,2): valid conversion
create or replace procedure typed_value(out v decimal(5,2)) language 'starlark' $$out_v = "12.345"$$;
set @v=99;
call typed_value(@v);
select @v='12.35' as matches;
-- OUT decimal(5,2): reject and preserve
create or replace procedure typed_value(out v decimal(5,2)) language 'starlark' $$out_v = "1000"$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- OUT float: valid conversion
create or replace procedure typed_value(out v float) language 'starlark' $$out_v = 1.25$$;
set @v=99;
call typed_value(@v);
select @v=1.25 as matches;
-- OUT double: valid conversion
create or replace procedure typed_value(out v double) language 'starlark' $$out_v = 1.25$$;
set @v=99;
call typed_value(@v);
select @v=1.25 as matches;
-- OUT bool: valid conversion
create or replace procedure typed_value(out v bool) language 'starlark' $$out_v = True$$;
set @v=99;
call typed_value(@v);
select @v=true as matches;
-- OUT varchar(3): valid conversion
create or replace procedure typed_value(out v varchar(3)) language 'starlark' $$out_v = "abc"$$;
set @v=99;
call typed_value(@v);
select @v='abc' as matches;
-- OUT varchar(3): reject and preserve
create or replace procedure typed_value(out v varchar(3)) language 'starlark' $$out_v = "too-long"$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- OUT date: valid conversion
create or replace procedure typed_value(out v date) language 'starlark' $$out_v = "2026-02-28"$$;
set @v=99;
call typed_value(@v);
select @v='2026-02-28' as matches;
-- OUT date: reject and preserve
create or replace procedure typed_value(out v date) language 'starlark' $$out_v = "bad"$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- OUT time(3): valid conversion
create or replace procedure typed_value(out v time(3)) language 'starlark' $$out_v = "12:34:56.1234"$$;
set @v=99;
call typed_value(@v);
select @v='12:34:56.123' as matches;
-- OUT time: reject and preserve
create or replace procedure typed_value(out v time) language 'starlark' $$out_v = "bad"$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- OUT datetime(3): valid conversion
create or replace procedure typed_value(out v datetime(3)) language 'starlark' $$out_v = "2026-02-28 12:34:56.1234"$$;
set @v=99;
call typed_value(@v);
select @v='2026-02-28 12:34:56.123' as matches;
-- OUT datetime: reject and preserve
create or replace procedure typed_value(out v datetime) language 'starlark' $$out_v = "bad"$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- OUT timestamp(3): valid conversion
create or replace procedure typed_value(out v timestamp(3)) language 'starlark' $$out_v = "2026-02-28 12:34:56.1234"$$;
set @v=99;
call typed_value(@v);
select @v='2026-02-28 12:34:56.123' as matches;
-- OUT timestamp: reject and preserve
create or replace procedure typed_value(out v timestamp) language 'starlark' $$out_v = "bad"$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- OUT binary(3): valid conversion
create or replace procedure typed_value(out v binary(3)) language 'starlark' $$out_v = "a"$$;
set @v=99;
call typed_value(@v);
select hex(@v)='610000' as matches;
-- OUT binary(3): reject and preserve
create or replace procedure typed_value(out v binary(3)) language 'starlark' $$out_v = "abcd"$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- OUT varbinary(3): valid conversion
create or replace procedure typed_value(out v varbinary(3)) language 'starlark' $$out_v = "abc"$$;
set @v=99;
call typed_value(@v);
select hex(@v)='616263' as matches;
-- OUT varbinary(3): reject and preserve
create or replace procedure typed_value(out v varbinary(3)) language 'starlark' $$out_v = "abcd"$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- OUT vecf32(2): valid conversion
create or replace procedure typed_value(out v vecf32(2)) language 'starlark' $$out_v = "[1,2]"$$;
set @v=99;
call typed_value(@v);
select @v=cast('[1,2]' as vecf32(2)) as matches;
-- OUT vecf64(2): valid conversion
create or replace procedure typed_value(out v vecf64(2)) language 'starlark' $$out_v = "[1,2]"$$;
set @v=99;
call typed_value(@v);
select @v=cast('[1,2]' as vecf64(2)) as matches;
-- OUT vecf32(2): reject and preserve
create or replace procedure typed_value(out v vecf32(2)) language 'starlark' $$out_v = "[1,2,3]"$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- OUT vecf32(2): reject and preserve
create or replace procedure typed_value(out v vecf32(2)) language 'starlark' $$out_v = "bad"$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- INOUT tinyint: valid conversion
create or replace procedure typed_value(inout v tinyint) language 'starlark' $$out_v = 127$$;
set @v=99;
call typed_value(@v);
select @v=127 as matches;
-- INOUT tinyint: valid conversion
create or replace procedure typed_value(inout v tinyint) language 'starlark' $$out_v = -128$$;
set @v=99;
call typed_value(@v);
select @v=-128 as matches;
-- INOUT tinyint: reject and preserve
create or replace procedure typed_value(inout v tinyint) language 'starlark' $$out_v = 128$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- INOUT tinyint unsigned: valid conversion
create or replace procedure typed_value(inout v tinyint unsigned) language 'starlark' $$out_v = 255$$;
set @v=99;
call typed_value(@v);
select @v=255 as matches;
-- INOUT tinyint unsigned: reject and preserve
create or replace procedure typed_value(inout v tinyint unsigned) language 'starlark' $$out_v = 256$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- INOUT tinyint unsigned: reject and preserve
create or replace procedure typed_value(inout v tinyint unsigned) language 'starlark' $$out_v = -1$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- INOUT smallint: valid conversion
create or replace procedure typed_value(inout v smallint) language 'starlark' $$out_v = 32767$$;
set @v=99;
call typed_value(@v);
select @v=32767 as matches;
-- INOUT smallint: reject and preserve
create or replace procedure typed_value(inout v smallint) language 'starlark' $$out_v = 32768$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- INOUT smallint unsigned: valid conversion
create or replace procedure typed_value(inout v smallint unsigned) language 'starlark' $$out_v = 65535$$;
set @v=99;
call typed_value(@v);
select @v=65535 as matches;
-- INOUT smallint unsigned: reject and preserve
create or replace procedure typed_value(inout v smallint unsigned) language 'starlark' $$out_v = 65536$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- INOUT smallint unsigned: reject and preserve
create or replace procedure typed_value(inout v smallint unsigned) language 'starlark' $$out_v = -1$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- INOUT mediumint: valid conversion
create or replace procedure typed_value(inout v mediumint) language 'starlark' $$out_v = 8388607$$;
set @v=99;
call typed_value(@v);
select @v=8388607 as matches;
-- INOUT mediumint: reject and preserve
create or replace procedure typed_value(inout v mediumint) language 'starlark' $$out_v = 8388608$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- INOUT mediumint unsigned: valid conversion
create or replace procedure typed_value(inout v mediumint unsigned) language 'starlark' $$out_v = 16777215$$;
set @v=99;
call typed_value(@v);
select @v=16777215 as matches;
-- INOUT mediumint unsigned: reject and preserve
create or replace procedure typed_value(inout v mediumint unsigned) language 'starlark' $$out_v = 16777216$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- INOUT mediumint unsigned: reject and preserve
create or replace procedure typed_value(inout v mediumint unsigned) language 'starlark' $$out_v = -1$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- INOUT int: valid conversion
create or replace procedure typed_value(inout v int) language 'starlark' $$out_v = 2147483647$$;
set @v=99;
call typed_value(@v);
select @v=2147483647 as matches;
-- INOUT int: reject and preserve
create or replace procedure typed_value(inout v int) language 'starlark' $$out_v = 2147483648$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- INOUT int unsigned: valid conversion
create or replace procedure typed_value(inout v int unsigned) language 'starlark' $$out_v = 4294967295$$;
set @v=99;
call typed_value(@v);
select @v=4294967295 as matches;
-- INOUT int unsigned: reject and preserve
create or replace procedure typed_value(inout v int unsigned) language 'starlark' $$out_v = 4294967296$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- INOUT int unsigned: reject and preserve
create or replace procedure typed_value(inout v int unsigned) language 'starlark' $$out_v = -1$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- INOUT bigint: valid conversion
create or replace procedure typed_value(inout v bigint) language 'starlark' $$out_v = 9223372036854775807$$;
set @v=99;
call typed_value(@v);
select @v=9223372036854775807 as matches;
-- INOUT bigint: reject and preserve
create or replace procedure typed_value(inout v bigint) language 'starlark' $$out_v = 9223372036854775808$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- INOUT bigint unsigned: valid conversion
create or replace procedure typed_value(inout v bigint unsigned) language 'starlark' $$out_v = 18446744073709551615$$;
set @v=99;
call typed_value(@v);
select @v=18446744073709551615 as matches;
-- INOUT bigint unsigned: reject and preserve
create or replace procedure typed_value(inout v bigint unsigned) language 'starlark' $$out_v = 1 << 64$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- INOUT bigint unsigned: reject and preserve
create or replace procedure typed_value(inout v bigint unsigned) language 'starlark' $$out_v = -1$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- INOUT decimal(30,0): valid conversion
create or replace procedure typed_value(inout v decimal(30,0)) language 'starlark' $$out_v = 18446744073709551615$$;
set @v=99;
call typed_value(@v);
select @v='18446744073709551615' as matches;
-- INOUT decimal(30,0): valid conversion
create or replace procedure typed_value(inout v decimal(30,0)) language 'starlark' $$out_v = 1 << 64$$;
set @v=99;
call typed_value(@v);
select @v='18446744073709551616' as matches;
-- INOUT decimal(30,0): valid conversion
create or replace procedure typed_value(inout v decimal(30,0)) language 'starlark' $$out_v = -(1 << 64)$$;
set @v=99;
call typed_value(@v);
select @v='-18446744073709551616' as matches;
-- INOUT decimal(30,0): reject and preserve
create or replace procedure typed_value(inout v decimal(30,0)) language 'starlark' $$out_v = 1000000000000000000000000000000$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- INOUT decimal(5,2): valid conversion
create or replace procedure typed_value(inout v decimal(5,2)) language 'starlark' $$out_v = "12.345"$$;
set @v=99;
call typed_value(@v);
select @v='12.35' as matches;
-- INOUT decimal(5,2): reject and preserve
create or replace procedure typed_value(inout v decimal(5,2)) language 'starlark' $$out_v = "1000"$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- INOUT float: valid conversion
create or replace procedure typed_value(inout v float) language 'starlark' $$out_v = 1.25$$;
set @v=99;
call typed_value(@v);
select @v=1.25 as matches;
-- INOUT double: valid conversion
create or replace procedure typed_value(inout v double) language 'starlark' $$out_v = 1.25$$;
set @v=99;
call typed_value(@v);
select @v=1.25 as matches;
-- INOUT bool: valid conversion
create or replace procedure typed_value(inout v bool) language 'starlark' $$out_v = True$$;
set @v=99;
call typed_value(@v);
select @v=true as matches;
-- INOUT varchar(3): valid conversion
create or replace procedure typed_value(inout v varchar(3)) language 'starlark' $$out_v = "abc"$$;
set @v=99;
call typed_value(@v);
select @v='abc' as matches;
-- INOUT varchar(3): reject and preserve
create or replace procedure typed_value(inout v varchar(3)) language 'starlark' $$out_v = "too-long"$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- INOUT date: valid conversion
create or replace procedure typed_value(inout v date) language 'starlark' $$out_v = "2026-02-28"$$;
set @v=99;
call typed_value(@v);
select @v='2026-02-28' as matches;
-- INOUT date: reject and preserve
create or replace procedure typed_value(inout v date) language 'starlark' $$out_v = "bad"$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- INOUT time(3): valid conversion
create or replace procedure typed_value(inout v time(3)) language 'starlark' $$out_v = "12:34:56.1234"$$;
set @v=99;
call typed_value(@v);
select @v='12:34:56.123' as matches;
-- INOUT time: reject and preserve
create or replace procedure typed_value(inout v time) language 'starlark' $$out_v = "bad"$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- INOUT datetime(3): valid conversion
create or replace procedure typed_value(inout v datetime(3)) language 'starlark' $$out_v = "2026-02-28 12:34:56.1234"$$;
set @v=99;
call typed_value(@v);
select @v='2026-02-28 12:34:56.123' as matches;
-- INOUT datetime: reject and preserve
create or replace procedure typed_value(inout v datetime) language 'starlark' $$out_v = "bad"$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- INOUT timestamp(3): valid conversion
create or replace procedure typed_value(inout v timestamp(3)) language 'starlark' $$out_v = "2026-02-28 12:34:56.1234"$$;
set @v=99;
call typed_value(@v);
select @v='2026-02-28 12:34:56.123' as matches;
-- INOUT timestamp: reject and preserve
create or replace procedure typed_value(inout v timestamp) language 'starlark' $$out_v = "bad"$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- INOUT binary(3): valid conversion
create or replace procedure typed_value(inout v binary(3)) language 'starlark' $$out_v = "a"$$;
set @v=99;
call typed_value(@v);
select hex(@v)='610000' as matches;
-- INOUT binary(3): reject and preserve
create or replace procedure typed_value(inout v binary(3)) language 'starlark' $$out_v = "abcd"$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- INOUT varbinary(3): valid conversion
create or replace procedure typed_value(inout v varbinary(3)) language 'starlark' $$out_v = "abc"$$;
set @v=99;
call typed_value(@v);
select hex(@v)='616263' as matches;
-- INOUT varbinary(3): reject and preserve
create or replace procedure typed_value(inout v varbinary(3)) language 'starlark' $$out_v = "abcd"$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- INOUT vecf32(2): valid conversion
create or replace procedure typed_value(inout v vecf32(2)) language 'starlark' $$out_v = "[1,2]"$$;
set @v=99;
call typed_value(@v);
select @v=cast('[1,2]' as vecf32(2)) as matches;
-- INOUT vecf64(2): valid conversion
create or replace procedure typed_value(inout v vecf64(2)) language 'starlark' $$out_v = "[1,2]"$$;
set @v=99;
call typed_value(@v);
select @v=cast('[1,2]' as vecf64(2)) as matches;
-- INOUT vecf32(2): reject and preserve
create or replace procedure typed_value(inout v vecf32(2)) language 'starlark' $$out_v = "[1,2,3]"$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- INOUT vecf32(2): reject and preserve
create or replace procedure typed_value(inout v vecf32(2)) language 'starlark' $$out_v = "bad"$$;
set @v=99;
call typed_value(@v);
select @v=99 as matches;
-- OUT NULL is independent from initial INOUT NULL support.
create procedure out_null(out v int) language 'starlark' $$out_v = None$$;
set @v=99;
call out_null(@v);
select @v is null as matches;
create procedure missing_outputs(out a int, inout b int) language 'starlark' $$pass$$;
set @a=3,@b=4;
call missing_outputs(@a,@b);
select @a=3 and @b=4 as preserved;
-- Staging checks every alias and preserves all callers on any conversion failure.
create procedure atomic_outputs(out a int, out b varchar(3), inout c int) language 'starlark' $$out_a = 12
out_b = 'too-long'
out_c = 13$$;
set @a=7,@b=7,@c=7;
call atomic_outputs(@a,@b,@c);
select @a=7 and @b=7 and @c=7 as preserved;
set @a=7,@b=7,@c=7;
call atomic_outputs(@a,@a,@a);
select @a=7 and @b=7 and @c=7 as preserved;
create procedure alias_valid(out a int, out b int) language 'starlark' $$out_a = 12
out_b = 13$$;
call alias_valid(@a,@a);
select @a in (12,13) as matches;
create table effects(n int);
-- New output conversion errors must roll back the existing single procedure transaction.
create procedure effect_failure(out v int) language 'starlark' $$rs, err = mo.sql('insert into effects values(1)')
out_v = 'not-an-int'$$;
set @v=99;
call effect_failure(@v);
select @v=99 as preserved;
select count(*) as n from effects;
create procedure after_error(inout v int) language 'starlark' $$out_v = v + 1$$;
call after_error(@v);
select @v=100 as matches;
-- Common-owner vector consumers must retain arrays after evaluator cleanup.
set @vector=cast('[1,2]' as vecf32(2));
select @vector=cast('[1,2]' as vecf32(2)) as matches;
create procedure sql_vector(out v vecf64(2)) 'begin set v=''[1,2]''; end';
call sql_vector(@vector);
select @vector=cast('[1,2]' as vecf64(2)) as matches;
drop database starlark_parameter_types;
