drop database if exists procedure_test;
create database procedure_test;
use procedure_test;

create table tbh1(id int primary key, val int);
create table tbh2(id int primary key, val char);
create table tbh3(id int primary key, val float);

insert into tbh1(id, val) values(1,10),(2,20),(3,30);
insert into tbh2(id, val) values(1,'a'),(2,'b'),(3,'c');
insert into tbh3(id, val) values(1,1.5),(2,2.5),(3,3.5);

-- procedure creation and deletion test
-- [ignore for now]

-- procedure execution test

-- @case
-- @desc:test if-elseif-else (hit if)
-- @label:bvt
drop procedure if exists test_if_hit_if;
create procedure test_if_hit_if () 'begin DECLARE v1 INT; SET v1 = 10; IF v1 > 5 THEN select * from tbh1; ELSEIF v1 = 5 THEN select * from tbh2; ELSEIF v1 = 4 THEN select * from tbh2 limit 1; ELSE select * from tbh3; END IF; end';
call test_if_hit_if();
drop procedure test_if_hit_if;

-- @case
-- @desc:test if-elseif-else (hit first elseif)
-- @label:bvt
drop procedure if exists test_if_hit_elseif_first_elseif;
create procedure test_if_hit_elseif_first_elseif() 'begin DECLARE v1 INT; SET v1 = 5; IF v1 > 5 THEN select * from tbh1; ELSEIF v1 = 5 THEN select * from tbh2; ELSEIF v1 = 4 THEN select * from tbh2 limit 1; ELSE select * from tbh3; END IF; end';
call test_if_hit_elseif_first_elseif();
drop procedure test_if_hit_elseif_first_elseif;

-- @case
-- @desc:test if-elseif-else (hit second elseif)
-- @label:bvt
drop procedure if exists test_if_hit_second_elseif;
create procedure test_if_hit_second_elseif() 'begin DECLARE v1 INT; SET v1 = 4; IF v1 > 5 THEN select * from tbh1; ELSEIF v1 = 5 THEN select * from tbh2; ELSEIF v1 = 4 THEN select * from tbh2 order by id limit 1; ELSE select * from tbh3; END IF; end';
call test_if_hit_second_elseif();
drop procedure test_if_hit_second_elseif;

-- @case
-- @desc:test if-elseif-else (hit else)
-- @label:bvt
drop procedure if exists test_if_hit_else;
create procedure test_if_hit_else() 'begin DECLARE v1 INT; SET v1 = 3; IF v1 > 5 THEN select * from tbh1; ELSEIF v1 = 5 THEN select * from tbh2; ELSEIF v1 = 4 THEN select * from tbh2 limit 1; ELSE select * from tbh3; END IF; end';
call test_if_hit_else();
drop procedure test_if_hit_else;

-- @case
-- @desc:test while
-- @label:bvt
drop procedure if exists test_while;
create procedure test_while() 'begin DECLARE v1 INT default 10; WHILE v1 < 100 DO insert into tmp(id) values(v1); set v1 = v1 + 10; END WHILE; select * from tmp; end';
create table if not exists tmp(id int); 
call test_while();
drop table if exists tmp; 
drop procedure test_while;

-- @case
-- @desc:test repeat
-- @label:bvt
drop procedure if exists test_repeat;
create procedure test_repeat() 'begin declare p1 int default 10; declare v1 int default 5; repeat set v1 = v1 + 1; until v1 > p1 end repeat; select v1; end';
call test_repeat();
drop procedure test_repeat;

-- @case
-- @desc:test loop
-- @label:bvt
drop procedure if exists test_loop;
create procedure test_loop() 'begin declare p1 int default 5; label1: loop set p1 = p1 + 1; if p1 < 10 THEN iterate label1; end if; leave label1; end loop label1; select p1; end';
call test_loop();
drop procedure test_loop;

-- @case
-- @desc:test inner scope variable access
-- @label:bvt
drop procedure if exists test_var_access;
create procedure test_var_access() 'begin declare v1 int default 10; begin declare v1 int default 5; select v1; end; select v1; end';
call test_var_access();
drop procedure test_var_access;

-- @case
-- @desc:test IN parameter access (both expression and variable passing)
-- @label:bvt
drop procedure if exists test_in_param;
create procedure test_in_param(in sid int) 'begin select val from tbh2 where id = sid; end';
call test_in_param(3);
drop procedure test_in_param;

-- @case
-- @desc:test OUT parameter access
-- @label:bvt
drop procedure if exists test_out_param;
create procedure test_out_param(out sid int) 'begin set sid = 1000; end';
call test_out_param(@id);
select @id;
drop procedure test_out_param;

-- @case
-- @desc:test INOUT parameter access
-- @label:bvt
drop procedure if exists test_inout_param;
create procedure test_inout_param(inout sid int) 'begin select sid; set sid = 1000 end';
set @id = 100;
call test_inout_param(@id);
select @id;
drop procedure test_inout_param;

-- @case
-- @desc:declared DECIMAL type is retained across default, NULL, SET, IN, INOUT, and OUT assignments
-- @label:bvt
drop procedure if exists test_decimal_declared_type;
set @decimal_io = '1.10';
create procedure test_decimal_declared_type(in p1 decimal(10,2), inout io decimal(10,2), out ov decimal(10,2), out ocmp bool) 'begin declare v1 decimal(10,2) default 6; declare n1 decimal(10,2) default null; select v1 > p1 as default_cmp, n1 is null as null_default, v1 as default_value, p1 as in_value, io as inout_value; set v1 = 11; set io = io + 0.25; set ov = v1 + 1.3; set ocmp = v1 > p1; end';
call test_decimal_declared_type(10, @decimal_io, @decimal_out, @decimal_cmp);
select @decimal_io, @decimal_out, @decimal_cmp;
drop procedure test_decimal_declared_type;

-- @case
-- @desc:PREPARE/EXECUTE inside a stored procedure (issue #25413)
-- @label:bvt
drop table if exists t_prepare_inside;
create table t_prepare_inside (id int primary key, v int);
insert into t_prepare_inside values (1, 10), (2, 20), (3, 30);
drop procedure if exists test_prepare_literal;
create procedure test_prepare_literal() 'begin prepare s from ''select sum(v) as prep_sum from t_prepare_inside''; execute s; deallocate prepare s; end';
call test_prepare_literal();
drop procedure test_prepare_literal;
drop procedure if exists test_prepare_user_var;
create procedure test_prepare_user_var() 'begin set @sql = ''select sum(v) as prep_sum from t_prepare_inside''; prepare s from @sql; execute s; deallocate prepare s; end';
call test_prepare_user_var();
drop procedure test_prepare_user_var;
drop procedure if exists test_prepare_using;
create procedure test_prepare_using() 'begin set @left_arg = 20; set @right_arg = 40; prepare s from ''select ? + ? as prep_sum''; execute s using @left_arg, @right_arg; end';
call test_prepare_using();
execute s using @left_arg, @right_arg;
deallocate prepare s;
drop procedure test_prepare_using;
drop table t_prepare_inside;

-- @case
-- @desc:temporary table lifecycle inside a stored procedure
-- @label:bvt
drop procedure if exists test_temp_table_lifecycle;
create procedure test_temp_table_lifecycle() 'begin create temporary table tmp_proc_lifecycle (id int primary key, v int); insert into tmp_proc_lifecycle select id, val from tbh1 where id <= 2; select sum(v) as tmp_sum from tmp_proc_lifecycle; drop table tmp_proc_lifecycle; end';
call test_temp_table_lifecycle();
drop procedure test_temp_table_lifecycle;

-- @case
-- @desc:temporary table created in a stored procedure remains bound to the caller session
-- @label:bvt
drop procedure if exists test_temp_table_session_binding;
create procedure test_temp_table_session_binding() 'begin create temporary table tmp_proc_session (id int primary key, v int); insert into tmp_proc_session select id, val from tbh1 where id <= 2; end';
call test_temp_table_session_binding();
select sum(v) as tmp_sum from tmp_proc_session;
drop table tmp_proc_session;
drop procedure test_temp_table_session_binding;

-- @case
-- @desc:temporary table created in a nested stored procedure remains bound to the caller session
-- @label:bvt
drop procedure if exists test_nested_temp_table_outer;
drop procedure if exists test_nested_temp_table_inner;
create procedure test_nested_temp_table_inner() 'begin create temporary table tmp_nested_proc_session (id int primary key, v int); insert into tmp_nested_proc_session select id, val from tbh1 where id <= 2; end';
create procedure test_nested_temp_table_outer() 'begin call test_nested_temp_table_inner(); end';
call test_nested_temp_table_outer();
select sum(v) as tmp_sum from tmp_nested_proc_session;
drop table tmp_nested_proc_session;
drop procedure test_nested_temp_table_outer;
drop procedure test_nested_temp_table_inner;

-- @case
-- @desc:procedure parser SQL mode is retained after caller mode changes
-- @label:bvt
set sql_mode = 'PIPES_AS_CONCAT';
create procedure test_sql_mode_pipes() 'begin select ''a''||''b'' as c; end';
set sql_mode = '';
call test_sql_mode_pipes();
drop procedure test_sql_mode_pipes;
set sql_mode = default;

-- @case
-- @desc:recursion counts active instances per routine and uses the caller session limit
-- @label:bvt
set @saved_sp_recursion_depth = @@session.max_sp_recursion_depth;
set max_sp_recursion_depth = 0;
create table recursion_effects(n int);
create procedure recursion_self(in n int) 'begin insert into recursion_effects values(n); if n > 0 then call recursion_self(n-1); end if; end';
call recursion_self(0);
select n from recursion_effects order by n;
truncate table recursion_effects;
call recursion_self(1);
-- The rejected child is not entered; the existing top-level CALL transaction rolls back.
select n from recursion_effects order by n;
call recursion_self(0);
select n from recursion_effects order by n;
truncate table recursion_effects;
set max_sp_recursion_depth = 1;
call recursion_self(1);
select n from recursion_effects order by n;
truncate table recursion_effects;
call recursion_self(2);
select n from recursion_effects order by n;
set max_sp_recursion_depth = 2;
call recursion_self(2);
select n from recursion_effects order by n;
truncate table recursion_effects;

-- Different routines can nest at limit zero, and sibling calls do not retain depth.
set max_sp_recursion_depth = 0;
create procedure recursion_leaf() 'begin insert into recursion_effects values(30); end';
create procedure recursion_middle() 'begin call recursion_leaf(); call recursion_leaf(); end';
create procedure recursion_outer() 'begin call recursion_middle(); end';
call recursion_outer();
select n from recursion_effects order by n;
truncate table recursion_effects;

-- Mutual recursion is limited per routine, rather than by total CALL depth.
create procedure recursion_a(in n int) 'begin insert into recursion_effects values(n); if n > 0 then call recursion_b(n-1); end if; end';
create procedure recursion_b(in n int) 'begin insert into recursion_effects values(n); if n > 0 then call recursion_a(n-1); end if; end';
call recursion_a(1);
select n from recursion_effects order by n;
truncate table recursion_effects;
call recursion_a(2);
select n from recursion_effects order by n;
set max_sp_recursion_depth = 1;
call recursion_a(3);
select n from recursion_effects order by n;
truncate table recursion_effects;
call recursion_a(4);
select n from recursion_effects order by n;

-- Identically named routines in different schemas have independent allowances.
drop database if exists procedure_recursion_other;
create database procedure_recursion_other;
create procedure procedure_recursion_other.recursion_same() 'begin insert into procedure_test.recursion_effects values(40); end';
create procedure recursion_same() 'begin call procedure_recursion_other.recursion_same(); end';
set max_sp_recursion_depth = 0;
call recursion_same();
select n from recursion_effects order by n;
truncate table recursion_effects;
drop procedure procedure_recursion_other.recursion_same;
create procedure procedure_recursion_other.recursion_same() 'begin call procedure_test.recursion_same(); end';
call recursion_same();
select n from recursion_effects order by n;
drop database procedure_recursion_other;

-- A second connection has its own default zero limit; failure leaves it reusable.
set max_sp_recursion_depth = 1;
-- @session:id=1{
use procedure_test;
call recursion_self(1);
select n from recursion_effects order by n;
call recursion_self(0);
select n from recursion_effects order by n;
truncate table recursion_effects;
-- @session}
call ReCuRsIoN_SeLf(1);
select n from recursion_effects order by n;
truncate table recursion_effects;
set max_sp_recursion_depth = 0;
call recursion_self(1);
select n from recursion_effects order by n;
call recursion_self(0);
select n from recursion_effects order by n;
-- Starlark CALLs share the per-routine bound, including SQL bridge re-entry.
-- mo.sql reports a rejected child as a value; the script may handle it.
create procedure recursion_starlark(in n int) language 'starlark' $$
mo.setvar("st_entered", n)
rows, err = mo.sql("call recursion_starlark({})".format(n-1)) if n > 0 else [None, None]
code = mo.errno()
state = mo.sqlstate()
mo.setvar("st_errno", code) if err else None
mo.setvar("st_state", state) if err else None
$$;
set @st_entered = -1, @st_errno = 0, @st_state = '';
call recursion_starlark(3);
select @st_entered = 3 as stopped_at_entry, @st_errno = 1456 as recursion_error, @st_state = 'HY000' as recursion_state;
set max_sp_recursion_depth = 1;
set @st_entered = -1, @st_errno = 0, @st_state = '';
call recursion_starlark(3);
select @st_entered = 2 as one_reentry, @st_errno = 1456 as recursion_error;
set @st_entered = -1, @st_errno = 0, @st_state = '';
call recursion_starlark(0);
select @st_entered = 0 as reusable, @st_errno = 0 as no_error;
create procedure recursion_starlark_bridge(in n int) 'begin call recursion_starlark_mixed(n); end';
create procedure recursion_starlark_mixed(in n int) language 'starlark' $$
mo.setvar("st_entered", n)
rows, err = mo.sql("call recursion_starlark_bridge({})".format(n-1)) if n > 0 else [None, None]
code = mo.errno()
mo.setvar("st_errno", code) if err else None
$$;
set max_sp_recursion_depth = 0;
set @st_entered = -1, @st_errno = 0;
call recursion_starlark_mixed(3);
select @st_entered = 3 as mixed_stopped_at_entry, @st_errno = 1456 as recursion_error;
set max_sp_recursion_depth = @saved_sp_recursion_depth;

drop database if exists procedure_test;
