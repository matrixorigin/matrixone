-- @suit
-- @case
-- @desc: unsigned integer subtraction honors NO_UNSIGNED_SUBTRACTION
-- @label:bvt
-- Regression for issue #28134.

set @old_sql_mode = @@session.sql_mode;

drop database if exists test_unsigned_subtraction_mode;
create database test_unsigned_subtraction_mode;
use test_unsigned_subtraction_mode;

set session sql_mode = '';
select cast(0 as unsigned) - 1 as result;
select cast(2 as unsigned) - 1 as result;
select cast(2 as unsigned) - (-1) as result;
select cast('18446744073709551615' as unsigned) - 0 as result;
select cast('18446744073709551615' as unsigned) - (-1) as result;
select (cast(0 as unsigned) + 0) - 1 as result;
select (cast(0 as unsigned) * 1) - 1 as result;
select (cast(0 as unsigned) div 1) - 1 as result;
select (cast(0 as unsigned) % 1) - 1 as result;
select (cast(0 as unsigned) + abs(0)) - 1 as result;
select (cast('18446744073709551615' as unsigned) + 1) - cast('18446744073709551615' as unsigned) as result;

create table t_widths (
    u8 tinyint unsigned,
    u16 smallint unsigned,
    u32 int unsigned,
    u64 bigint unsigned,
    b bit(8)
);
insert into t_widths values (0, 0, 0, 0, b'0');
select u8 - 1 from t_widths;
select u16 - 1 from t_widths;
select u32 - 1 from t_widths;
select u64 - 1 from t_widths;
select b - 1 from t_widths;

set session sql_mode = 'STRICT_TRANS_TABLES';
select cast(0 as unsigned) - 1 as result;

set session sql_mode = 'NO_UNSIGNED_SUBTRACTION';
select cast(0 as unsigned) - 1 as result;
select 1 - cast(2 as unsigned) as result;
select cast(0 as unsigned) - cast(1 as unsigned) as result;
select cast(null as unsigned) - 1 as result;
select cast('18446744073709551615' as unsigned) - cast('18446744073709551615' as unsigned) as result;
select cast('18446744073709551615' as unsigned) - 0 as result;
select (cast(0 as unsigned) + 0) - 1 as result;
select (cast(0 as unsigned) * 1) - 1 as result;
select (cast(0 as unsigned) div 1) - 1 as result;
select (cast(0 as unsigned) % 1) - 1 as result;
select (cast(0 as unsigned) + abs(0)) - 1 as result;
select (cast('18446744073709551615' as unsigned) + 1) - cast('18446744073709551615' as unsigned) as result;
select u8 - 1, u16 - 1, u32 - 1, u64 - 1, b - 1 from t_widths;

set session sql_mode = 'STRICT_TRANS_TABLES,NO_UNSIGNED_SUBTRACTION';
select cast(0 as unsigned) - 1 as result;

create table t_nested (u bigint unsigned, y year);
insert into t_nested values (0, 0);
select (u + 0) - 1, (u * 1) - 1, (u div 1) - 1, (u % 1) - 1, y - 1 from t_nested;

create function stored_signed_sub() returns bigint language sql as 'cast(0 as unsigned) - 1';
set session sql_mode = '';
select stored_signed_sub() as result;
drop function stored_signed_sub();

create function stored_unsigned_sub() returns bigint language sql as 'cast(0 as unsigned) - 1';
set session sql_mode = 'NO_UNSIGNED_SUBTRACTION';
select stored_unsigned_sub() as result;
drop function stored_unsigned_sub();

create table t_ddl_mode (
    u bigint unsigned,
    g bigint generated always as (u - 1) stored,
    d bigint default (cast(0 as unsigned) - 1),
    check (u - 1 < 0)
);
insert into t_ddl_mode (u) values (0);
select u, g, d from t_ddl_mode;
drop table t_ddl_mode;
drop table t_nested;

create table t_update (u bigint unsigned);
insert into t_update values (0);
update t_update set u = u - 1;
select u from t_update;

set session sql_mode = '';
prepare unsigned_sub from 'select cast(? as unsigned) - 1 as result';
set @operand = 0;
set session sql_mode = 'NO_UNSIGNED_SUBTRACTION';
execute unsigned_sub using @operand;
deallocate prepare unsigned_sub;

set session sql_mode = 'NO_UNSIGNED_SUBTRACTION';
prepare unsigned_sub from 'select cast(? as unsigned) - 1 as result';
set @operand = 0;
set session sql_mode = '';
execute unsigned_sub using @operand;
deallocate prepare unsigned_sub;

-- Explicit CASTs fix each marker's domain, so the inner unsigned arithmetic
-- result must be range-checked before an outer subtraction can cancel it.
set @max_uint64 = cast('18446744073709551615' as unsigned);
set session sql_mode = '';
prepare unsigned_typed_add from 'select (cast(? as unsigned) + cast(1 as signed)) - cast(? as unsigned) as result';
execute unsigned_typed_add using @max_uint64, @max_uint64;
deallocate prepare unsigned_typed_add;
prepare unsigned_typed_mul from 'select (cast(? as unsigned) * cast(2 as signed)) - cast(? as unsigned) as result';
execute unsigned_typed_mul using @max_uint64, @max_uint64;
deallocate prepare unsigned_typed_mul;
prepare unsigned_typed_add_ok from 'select (cast(? as unsigned) + cast(0 as signed)) - cast(? as unsigned) as result';
execute unsigned_typed_add_ok using @max_uint64, @max_uint64;
deallocate prepare unsigned_typed_add_ok;
prepare unsigned_typed_mul_ok from 'select (cast(? as unsigned) * cast(1 as signed)) - cast(? as unsigned) as result';
execute unsigned_typed_mul_ok using @max_uint64, @max_uint64;
deallocate prepare unsigned_typed_mul_ok;

set session sql_mode = 'NO_UNSIGNED_SUBTRACTION';
prepare unsigned_typed_add from 'select (cast(? as unsigned) + cast(1 as signed)) - cast(? as unsigned) as result';
execute unsigned_typed_add using @max_uint64, @max_uint64;
deallocate prepare unsigned_typed_add;
prepare unsigned_typed_mul from 'select (cast(? as unsigned) * cast(2 as signed)) - cast(? as unsigned) as result';
execute unsigned_typed_mul using @max_uint64, @max_uint64;
deallocate prepare unsigned_typed_mul;
prepare unsigned_typed_add_ok from 'select (cast(? as unsigned) + cast(0 as signed)) - cast(? as unsigned) as result';
execute unsigned_typed_add_ok using @max_uint64, @max_uint64;
deallocate prepare unsigned_typed_add_ok;
prepare unsigned_typed_mul_ok from 'select (cast(? as unsigned) * cast(1 as signed)) - cast(? as unsigned) as result';
execute unsigned_typed_mul_ok using @max_uint64, @max_uint64;
deallocate prepare unsigned_typed_mul_ok;

-- Nearby prepared controls: bare marker remains runtime-typed, while explicit
-- DECIMAL/FLOAT and NULL boundaries must not be treated as integer overflow.
set session sql_mode = '';
set @negative_peer = -2;
prepare unsigned_bare_peer from 'select (cast(? as unsigned) + ?) is not null as result';
execute unsigned_bare_peer using @max_uint64, @negative_peer;
deallocate prepare unsigned_bare_peer;
set @zero_decimal = cast(0 as decimal(20,1));
prepare unsigned_decimal_peer from 'select (cast(? as unsigned) + cast(? as decimal(20,1))) is not null as result';
execute unsigned_decimal_peer using @max_uint64, @zero_decimal;
deallocate prepare unsigned_decimal_peer;
set @zero_float = cast(0 as double);
prepare unsigned_float_peer from 'select (cast(? as unsigned) + cast(? as double)) is not null as result';
execute unsigned_float_peer using @max_uint64, @zero_float;
deallocate prepare unsigned_float_peer;
set @null_operand = null;
prepare unsigned_null_peer from 'select (cast(? as unsigned) + cast(? as signed)) is null as result';
execute unsigned_null_peer using @null_operand, @negative_peer;
deallocate prepare unsigned_null_peer;
set @max_uint64 = null;
set @negative_peer = null;
set @zero_decimal = null;
set @zero_float = null;
set @null_operand = null;

drop database test_unsigned_subtraction_mode;
set session sql_mode = @old_sql_mode;
set @old_sql_mode = null;
