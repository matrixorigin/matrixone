-- @suite
-- @case

drop database if exists mixed_unsigned_arithmetic_28581;
create database mixed_unsigned_arithmetic_28581;
use mixed_unsigned_arithmetic_28581;

set @mixed_unsigned_saved_sql_mode = @@sql_mode;
set session sql_mode = '';

create table mixed_values (id int primary key, u bigint unsigned, s bigint);
insert into mixed_values values
    (1, 18446744073709551615, 0),
    (2, 18446744073709551615, 1),
    (3, 0, 1);

select u + s from mixed_values where id = 1;
select u + s from mixed_values where id = 2;
select u - s from mixed_values where id = 3;
select u * s from mixed_values where id = 2;
select u * 2 from mixed_values where id = 2;

select (cast(0 as unsigned) + cast(0 as signed)) - 1;
create table typed_result as select u + s as q from mixed_values where id = 1;
select column_type from information_schema.columns
where table_schema = 'mixed_unsigned_arithmetic_28581' and table_name = 'typed_result' and column_name = 'q';
create table rejected_result as select u + s as q from mixed_values where id = 2;
select count(*) from information_schema.tables
where table_schema = 'mixed_unsigned_arithmetic_28581' and table_name = 'rejected_result';

prepare mixed_subtract_stmt from 'select u - s from mixed_values where id = ?';
set @mixed_subtract_id = 3;
execute mixed_subtract_stmt using @mixed_subtract_id;
set session sql_mode = 'NO_UNSIGNED_SUBTRACTION';
select u - s from mixed_values where id = 3;
execute mixed_subtract_stmt using @mixed_subtract_id;
select u - s from mixed_values where id = 1;
select cast(0 as unsigned) - cast(1 as unsigned);
set session sql_mode = '';
execute mixed_subtract_stmt using @mixed_subtract_id;
deallocate prepare mixed_subtract_stmt;

prepare mixed_operand_stmt from 'select cast(0 as unsigned) - ? as q';
set @mixed_operand = 1;
execute mixed_operand_stmt using @mixed_operand;
set session sql_mode = 'NO_UNSIGNED_SUBTRACTION';
execute mixed_operand_stmt using @mixed_operand;
set @mixed_operand = cast(18446744073709551615 as unsigned);
execute mixed_operand_stmt using @mixed_operand;
deallocate prepare mixed_operand_stmt;
set session sql_mode = '';

prepare mixed_unsigned_stmt from 'select u + s from mixed_values where id = ?';
set @mixed_unsigned_id = 1;
execute mixed_unsigned_stmt using @mixed_unsigned_id;
deallocate prepare mixed_unsigned_stmt;

set session sql_mode = @mixed_unsigned_saved_sql_mode;
drop database mixed_unsigned_arithmetic_28581;
