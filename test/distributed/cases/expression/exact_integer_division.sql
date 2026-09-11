-- @suite
-- @case

drop database if exists exact_integer_division_28580;
create database exact_integer_division_28580;
use exact_integer_division_28580;

create table integer_values (id int primary key, a bigint, b bigint);
insert into integer_values values
    (1, 9007199254740993, 1),
    (2, 9223372036854775807, 1),
    (3, 10, 3);

select id, a / b from integer_values order by id;
select id from integer_values where a / b = cast(a as decimal(23,4)) / cast(b as decimal(23,4)) order by id;

prepare exact_integer_division_stmt from 'select a / ? from integer_values where id = ?';
set @exact_divisor = 1;
set @exact_id = 1;
execute exact_integer_division_stmt using @exact_divisor, @exact_id;
deallocate prepare exact_integer_division_stmt;

create view exact_integer_division_view as select a / b as quotient from integer_values;
select quotient from exact_integer_division_view order by quotient;

create table exact_integer_division_ctas as select a / b as quotient from integer_values;
select column_type from information_schema.columns
where table_schema = 'exact_integer_division_28580'
  and table_name = 'exact_integer_division_ctas'
  and column_name = 'quotient';

drop database exact_integer_division_28580;
