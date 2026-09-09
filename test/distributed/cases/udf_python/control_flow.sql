-- Correctness boundaries for scalar invocation, NULL handling, empty input,
-- zero-argument calls, and CASE selection masks.
drop database if exists udf_python_control_bvt;
create database udf_python_control_bvt;
use udf_python_control_bvt;

create function python_bvt_mark_null (x varchar(32)) returns varchar(64) language python as 'def python_bvt_mark_null(ctx, x): return "<NULL>" if x is None else "<" + x + ">"' handler 'python_bvt_mark_null';
create function python_bvt_add_one (x int) returns int language python as 'def python_bvt_add_one(ctx, x): return None if x is None else x + 1' handler 'python_bvt_add_one';
create function python_bvt_add_pair (x int, y int) returns int language python as 'def python_bvt_add_pair(ctx, x, y): return None if x is None or y is None else x + y' handler 'python_bvt_add_pair';
create function python_bvt_zero_arg () returns int language python as 'def python_bvt_zero_arg(ctx): return 7' handler 'python_bvt_zero_arg';
create function python_bvt_must_not_run (x int) returns int language python as 'def python_bvt_must_not_run(ctx, x): raise RuntimeError("unselected Python branch was executed")' handler 'python_bvt_must_not_run';
create function python_bvt_empty_input (x int) returns int language python as 'def python_bvt_empty_input(ctx, x): raise RuntimeError("empty input was executed")' handler 'python_bvt_empty_input';

create table control_values (id int, label varchar(32), value int);
insert into control_values values
    (1, 'empty', 1),
    (2, '中😀', 2),
    (3, null, 3);

-- The default policy passes NULL to the handler and preserves a handler NULL.
select id, python_bvt_mark_null(label) as marked
from control_values order by id;

-- A zero-argument scalar still produces one result per input row.
select id, python_bvt_zero_arg() as constant
from control_values order by id;

-- Multiple arguments preserve row order and NULLs independently.
select id, python_bvt_add_pair(value, id) as pair_sum
from control_values order by id;

-- A CASE branch with no selected rows must not invoke its Python handler.
select id,
       case when value < 0 then python_bvt_must_not_run(value)
            else 99 end as guarded
from control_values order by id;

-- An empty input batch must finish without opening a worker invocation.
select python_bvt_empty_input(value) as never_returned
from control_values where 1 = 0;

drop function python_bvt_mark_null(varchar(32));
drop function python_bvt_add_one(int);
drop function python_bvt_add_pair(int, int);
drop function python_bvt_zero_arg();
drop function python_bvt_must_not_run(int);
drop function python_bvt_empty_input(int);
drop table control_values;
drop database udf_python_control_bvt;
