-- @suite
-- @case
-- @desc: MySQL-compatible user-variable type and SELECT INTO matrix
-- @label:bvt
-- MySQL reference: 8.0.46 (verified against a temporary local server).

-- Matrix A: an unassigned variable is NULL and an explicit NULL remains NULL.
select @uv_matrix_missing;
select @uv_matrix_missing is null;
set @uv_matrix_null = null;
select @uv_matrix_null is null;

-- Matrix B: a variable's assigned numeric type must survive integer contexts.
set @uv_matrix_decimal = 1.5;
select @uv_matrix_decimal + 0;
select abs(@uv_matrix_decimal);
select @uv_matrix_decimal between 1 and 2;
select @uv_matrix_decimal in (1, 2);

-- Matrix C: wide integer values must not be narrowed by a numeric literal.
set @uv_matrix_large = 9007199254740993;
select @uv_matrix_large;
select @uv_matrix_large + 1;

-- Matrix D: string variables convert in numeric context but remain strings in
-- string context.
set @uv_matrix_text = '1.5';
select @uv_matrix_text + 0;
select concat(@uv_matrix_text, ':text');

-- Matrix D2: prepared text variables keep a value-independent floating-point
-- context while each execution applies MySQL's numeric-prefix conversion.
set @uv_matrix_prepared_text = '1';
prepare uv_matrix_text_ps from 'select @uv_matrix_prepared_text + 0';
execute uv_matrix_text_ps;
set @uv_matrix_prepared_text = '1.5x';
execute uv_matrix_text_ps;
set @uv_matrix_prepared_text = '-2.25abc';
execute uv_matrix_text_ps;
deallocate prepare uv_matrix_text_ps;

set @uv_matrix_prepared_text = '1.5';
prepare uv_matrix_text_ps from 'select @uv_matrix_prepared_text + 0';
execute uv_matrix_text_ps;
set @uv_matrix_prepared_text = '2abc';
execute uv_matrix_text_ps;
deallocate prepare uv_matrix_text_ps;

-- Matrix D3: numeric coercion warnings must survive remote-scope folding.
-- The value is folded on the initiating CN, while the numeric cast may run
-- remotely; SHOW WARNINGS must still expose MySQL warning 1292 there.
set @uv_matrix_warn = '12abc';
select @uv_matrix_warn + 0;
show warnings;
set @uv_matrix_warn = '12';
select @uv_matrix_warn + 0;
show warnings;

set @uv_matrix_prepared_warn = '1';
prepare uv_matrix_warn_ps from 'select @uv_matrix_prepared_warn + 0';
set @uv_matrix_prepared_warn = '1.5x';
execute uv_matrix_warn_ps;
show warnings;
set @uv_matrix_prepared_warn = '2';
execute uv_matrix_warn_ps;
show warnings;
deallocate prepare uv_matrix_warn_ps;

-- Matrix E: independent numeric user variables and explicit casts.
set @uv_matrix_a = 1, @uv_matrix_b = 2;
select @uv_matrix_a + @uv_matrix_b;
select @uv_matrix_decimal in (@uv_matrix_a, @uv_matrix_b);
select cast(@uv_matrix_decimal as signed);
select cast(@uv_matrix_decimal as decimal(10, 2));

-- Matrix F: a prepared statement fixes the variable's type at prepare time,
-- while each execution still reads the current value.
set @uv_matrix_prepared = 1.5;
prepare uv_matrix_ps from 'select @uv_matrix_prepared + 0';
execute uv_matrix_ps;
set @uv_matrix_prepared = 2.5;
execute uv_matrix_ps;
deallocate prepare uv_matrix_ps;

drop database if exists mysql_compat_user_variables_matrix;
create database mysql_compat_user_variables_matrix;
use mysql_compat_user_variables_matrix;
create table uv_matrix_src(id int primary key, v varchar(10));
insert into uv_matrix_src values (1, 'one'), (2, 'two');

-- Matrix G: both documented SELECT ... INTO positions are accepted.
select id into @uv_matrix_pre_id from uv_matrix_src where id = 2;
select @uv_matrix_pre_id;
select v from uv_matrix_src where id = 1 into @uv_matrix_terminal_v;
select @uv_matrix_terminal_v;

-- Matrix H: zero rows keep the old value and emit MySQL warning 1329.
set @uv_matrix_keep = 'old';
select v from uv_matrix_src where id = 99 into @uv_matrix_keep;
show warnings;
select @uv_matrix_keep;

-- Matrix I: two SELECT ... INTO statements in one client batch each use a
-- fresh collector.
select 1 into @uv_matrix_batch_a; select 2 into @uv_matrix_batch_b;
select @uv_matrix_batch_a, @uv_matrix_batch_b;

drop database mysql_compat_user_variables_matrix;
