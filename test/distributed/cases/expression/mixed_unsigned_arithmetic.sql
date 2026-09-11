-- @suite
-- @case

drop database if exists mixed_unsigned_arithmetic_28581;
create database mixed_unsigned_arithmetic_28581;
use mixed_unsigned_arithmetic_28581;

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

set @mixed_unsigned_saved_sql_mode = @@sql_mode;
set session sql_mode = 'NO_UNSIGNED_SUBTRACTION';
select u - s from mixed_values where id = 3;
set session sql_mode = @mixed_unsigned_saved_sql_mode;

prepare mixed_unsigned_stmt from 'select u + s from mixed_values where id = ?';
set @mixed_unsigned_id = 1;
execute mixed_unsigned_stmt using @mixed_unsigned_id;
deallocate prepare mixed_unsigned_stmt;

drop database mixed_unsigned_arithmetic_28581;
