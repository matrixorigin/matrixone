drop database if exists procedure_test;
create database procedure_test;
use procedure_test;

--
-- most basic starlark stored procedure tests.
--

drop procedure if exists sp_empty;
create or replace procedure sp_empty () language 'starlark'
'';

call sp_empty();
call sp_empty('foo');

-- @bvt:issue#22165
create or replace procedure sp_empty () language 'starlark'
'
def add(x, y):
    return x + y

foo = add(1, 2)
';

call sp_empty();
call sp_empty('foo');
-- @bvt:issue


create procedure sp_badparam(in `mo` int) language 'starlark'
'';

create procedure sp_badparam(in `mo.foo` int) language 'starlark'
'';

create procedure sp_badparam(inout out_foo int) language 'starlark'
'';

create or replace procedure sp_x2(in i int, out o int) language 'starlark'
'
out_o = i + i
';

call sp_x2(1, @result);
select @result;

call sp_x2(5, @result);
select @result;

create or replace procedure sp_iox2(inout io int) language 'starlark'
'
out_io = io + io
'
;

set @ioparam = 1;
call sp_iox2(@ioparam);
select @ioparam;
call sp_iox2(@ioparam);
select @ioparam;

create or replace procedure sp_quote(in s varchar, out qs varchar) language 'starlark'
'
qqs, err = mo.quote(s)
out_qs = qqs if not err else "error"
';

call sp_quote('''', @qs);
select @qs;
call sp_quote($$foo'bar'zoo$$, @qs2);
select @qs2;

create or replace procedure sp_jq(in jq varchar, in data varchar, out jqresult varchar, out errstr varchar) language 'starlark'
'   
res, err = mo.jq(jq, data)
out_jqresult = "" if err else res
out_errstr = err 
';

call sp_jq('.0 + .1', '[1, 2]', @res, @err);
select @res;
select @err;
call sp_jq('.[0] + .[1]', '[1, 2]', @res, @err);
select @res;
select @err;
call sp_jq('.[0] + .[1]', '1', @res, @err);
select @res;
select @err;

create or replace procedure sp_var(in varname varchar) language 'starlark'
'   
v, err = mo.getvar(varname)
mo.setvar(varname, v+v)
mo.setvar("err" + varname, "error")
';

set @spvar = 1;
call sp_var('spvar');
select @spvar;
select @errspvar;

drop database if exists procedure_test;
