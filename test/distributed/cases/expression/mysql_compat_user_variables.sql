-- MySQL user variables are lazily created: an unassigned variable reads NULL.
select @never_set;
select @never_set is null;

-- SELECT ... INTO @var returns a status packet and stores the single result row.
select abs(-5) into @out;
select @out, @OUT;

-- Multiple expressions map to multiple user variables.
select 7, 'ok', null into @out_number, @out_text, @out_null;
select @out_number, @out_text, @out_null is null;

-- User variables participate in numeric arithmetic like MySQL, even when
-- multiple variables are assigned by the same SET statement.
set @a = 1, @b = 2;
select @a + @b;

-- Prepared arithmetic parameters supplied through user variables use the
-- prepared statement's numeric context instead of TEXT+TEXT overload lookup.
prepare ps_count from 'select ? + ? as sum_val';
set @c1 = 1, @c2 = 2;
execute ps_count using @c1, @c2;
deallocate prepare ps_count;

drop database if exists mysql_compat_user_variables;
create database mysql_compat_user_variables;
use mysql_compat_user_variables;

create table uv_src(id int primary key, v varchar(10));
insert into uv_src values (1, 'one'), (2, 'two'), (3, 'three');

-- SELECT ... FROM ... INTO @var assigns the only result row.
select id, v from uv_src where id = 2 into @row_id, @row_v;
select @row_id, @row_v;

-- ORDER BY/LIMIT still feeds SELECT ... INTO as a single-row statement.
select v from uv_src order by id desc limit 1 into @last_v;
select @last_v;

-- A zero-row SELECT ... INTO leaves the previous variable value unchanged.
set @keep = 'old';
select v from uv_src where id = 100 into @keep;
select @keep;

-- Column/user-variable arity is checked even when the result has zero rows.
set @arity_a = 'old_a', @arity_b = 'old_b';
-- @regex("The used SELECT statements have a different number of columns", true)
select 1 where false into @arity_a, @arity_b;
select @arity_a, @arity_b;

-- Aggregates over empty input still produce one row and assign that value.
select count(*) from uv_src where id = 100 into @empty_count;
select @empty_count;

-- SELECT ... INTO inside a stored procedure uses the background execution path.
drop procedure if exists p_select_into_user_var;
set @proc_out = 'old';
create procedure p_select_into_user_var() 'begin select 123 into @proc_out; end';
call p_select_into_user_var();
select @proc_out;
drop procedure p_select_into_user_var;

-- Binary-string metadata is preserved for EXECUTE ... USING.
create table uv_bin(id int primary key, b binary(4), key idx_b(b));
insert into uv_bin values (1, x'41420000'), (2, x'41420020');
select x'41420000' into @binary_value;
prepare ps_bin from 'select id from uv_bin where b = ? order by id';
execute ps_bin using @binary_value;
deallocate prepare ps_bin;

-- MySQL rejects SELECT ... INTO @var when more than one row is returned.
set @too_many = 'old';
-- @regex("Result consisted of more than one row", true)
select id from uv_src into @too_many;
select @too_many;

drop database mysql_compat_user_variables;
