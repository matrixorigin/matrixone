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
select u8 - 1, u16 - 1, u32 - 1, u64 - 1, b - 1 from t_widths;

set session sql_mode = 'STRICT_TRANS_TABLES,NO_UNSIGNED_SUBTRACTION';
select cast(0 as unsigned) - 1 as result;

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

drop database test_unsigned_subtraction_mode;
set session sql_mode = @old_sql_mode;
set @old_sql_mode = null;
