-- @suite
-- @case
-- @desc: Exact DECIMAL integer arguments must not pass through DOUBLE.
create table exact_integer_arguments(d decimal(20,1), wide decimal(65,1), f double);
insert into exact_integer_arguments values (1.5,1.5,1.5),(-1.5,-1.5,-1.5),(2.5,2.5,2.5);
select d, period_add(202401,d) as p, period_add(202401,wide) as wp, hex(d) as h from exact_integer_arguments order by d;
select period_add(202401.5,0) as p, period_diff(202402.5,202401) as diff;
select hex(cast('9007199254740993' as decimal(20,0))) as h;
select period_add(202401,cast(1.5 as double)) as p;
select hex(cast('9007199254740993' as unsigned)) as h;
select period_add(202401,NULL) as p, period_diff(NULL,202401) as diff, hex(NULL) as h;
prepare exact_args from 'select period_add(202401,?) as p';
set @exact_arg=cast(1.5 as decimal(4,1));
execute exact_args using @exact_arg;
deallocate prepare exact_args;
prepare exact_args from 'select period_diff(?,202401) as diff';
set @exact_arg=cast(202402.5 as decimal(7,1));
execute exact_args using @exact_arg;
deallocate prepare exact_args;
prepare exact_args from 'select hex(?) as h';
set @exact_arg=cast('9007199254740993' as decimal(20,0));
execute exact_args using @exact_arg;
deallocate prepare exact_args;
-- MO intentionally errors on DECIMAL integer-argument overflow instead of saturating.
select hex(cast('9223372036854775808' as decimal(20,0)));
select period_add(202401,cast('-9223372036854775809' as decimal(20,0)));
select period_diff(cast('9223372036854775808' as decimal(20,0)),202401);
drop table exact_integer_arguments;
